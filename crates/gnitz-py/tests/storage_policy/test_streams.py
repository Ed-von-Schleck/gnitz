"""Streams: `CREATE TABLE … WITH (stream = true)`.

A stream is a relation with a schema and a primary key that holds no rows. Views
over it are maintained exactly as views over a table are, but nothing it ingests
is recovered: pushed rows exist only as the deltas they produce, so every view
reaching a stream comes back at boot holding the value it would have had if the
stream had never received a row. The stream's *definition* is ordinary durable
catalog state.

A stream's PK is a routing and sort key, not a uniqueness constraint, and its
weights are bag multiplicities. So every assertion here is a weight bag: a row
list reads a duplicated reply frame, a per-worker-replayed broadcast and a
weight-0 ghost all as correct, in the one file whose subject is multiplicity.

What a stream *refuses* is in `admissibility/test_stream_rejections.py`.
"""

import glob
import os

import pytest
import gnitz
from _paths import relation_dir
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _sql import insert

_EVENT_COLS = "id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL"
_HOT = "SELECT kind, SUM(amount) AS total FROM s GROUP BY kind"


def _stream(conn):
    conn.execute_sql(f"CREATE TABLE s ({_EVENT_COLS}) WITH (stream = true)")


def _read(conn, view):
    return bag(rows(conn, f"SELECT * FROM {view}"))


# ---------------------------------------------------------------------------
# Views over a stream
# ---------------------------------------------------------------------------


def test_views_over_a_stream_track_pushes_from_their_creation_on(client):
    """A GROUP BY view tracks every push. A view over that *view* backfills from
    its accumulated output store like over any view, while a second view over the
    stream itself starts empty: a stream's backfill scans a store that holds no
    rows, and the push no read had drained yet is ticked before the view exists.
    Every read verb — SQL, `scan`, `scan_many` — drains the tick the push before
    it left pending."""
    _stream(client)
    client.execute_sql(f"CREATE VIEW hot AS {_HOT}")
    insert(client, "s", [(i, i % 3, i * 10) for i in range(1, 31)])
    client.execute_sql(f"CREATE VIEW late AS {_HOT}")
    assert _read(client, "hot") == {(0, 1650): 1, (1, 1450): 1, (2, 1550): 1}

    client.execute_sql("CREATE VIEW big AS SELECT kind, total FROM hot WHERE total > 1500")
    assert _read(client, "big") == {(0, 1650): 1, (2, 1550): 1}
    assert _read(client, "late") == {}

    vid, schema = client.resolve_table("hot")
    insert(client, "s", [(99, 1, 7)])
    assert bag(client.scan(vid, schema)) == {(0, 1650): 1, (1, 1457): 1, (2, 1550): 1}
    insert(client, "s", [(100, 1, 100)])
    assert bag(client.scan_many([(vid, schema)])[0]) == {(0, 1650): 1, (1, 1557): 1, (2, 1550): 1}
    assert _read(client, "late") == {(1, 107): 1}
    assert _read(client, "big") == {(0, 1650): 1, (1, 1557): 1, (2, 1550): 1}


def test_drop_and_rename_stay_available_on_a_stream(client):
    """DROP of a stream restricts on dependent views exactly as for a table, and
    `RENAME TO` keeps its views fed."""
    _stream(client)
    client.execute_sql("CREATE VIEW hot AS SELECT id, amount FROM s")

    with pytest.raises(gnitz.GnitzRefusedError, match="dependen"):
        client.execute_sql("DROP TABLE s")

    client.execute_sql("ALTER TABLE s RENAME TO events")
    insert(client, "events", [(1, 2, 30)])
    assert _read(client, "hot") == {(1, 30): 1}

    client.execute_sql("DROP VIEW hot")
    client.execute_sql("DROP TABLE events")
    with pytest.raises(gnitz.GnitzNotFoundError):
        client.resolve_table("events")


# ---------------------------------------------------------------------------
# Recovery: a stream-fed view returns to its never-received-a-row value
# ---------------------------------------------------------------------------


# `(view name, body, value once the stream is back to never-having-received-a-row)`.
# The stream carries `kind` 1 and 2; `dim` carries k 1, 2 and 3.
_BOOT_SHAPES = [
    ("agg", _HOT, {}),
    # Reaches the stream only through `agg`.
    ("over_agg", "SELECT kind, total FROM agg WHERE total > 0", {}),
    ("inner_v", "SELECT dim.k, s.amount FROM dim JOIN s ON dim.k = s.kind", {}),
    # Every preserved row comes back null-filled: the null-fill is weight-exact,
    # so a witness clamped to 1 over a matched row would leak an extra one here.
    ("left_v", "SELECT dim.k, s.amount FROM dim LEFT JOIN s ON dim.k = s.kind",
     {(1, None): 1, (2, None): 1, (3, None): 1}),
    # The non-monotone direction: the view *grows* when the stream empties.
    ("grown", "SELECT k FROM dim EXCEPT SELECT kind FROM s", {(1,): 1, (2,): 1, (3,): 1}),
    # Created over a table and retargeted onto the stream by `ALTER VIEW`: the
    # boot decides by the view's sources as they are, not as created.
    ("moved", None, {}),
]
_PUSHED = {
    "agg": {(1, 100): 1, (2, 200): 1},
    "over_agg": {(1, 100): 1, (2, 200): 1},
    "inner_v": {(1, 100): 1, (2, 200): 1},
    "left_v": {(1, 100): 1, (2, 200): 1, (3, None): 1},
    "grown": {(3,): 1},
    "moved": {(1, 100): 1, (2, 200): 1},
}
# `tv` over the table `t` beside them is resumed and published as any view is.
_T_ROWS = {(i, 1, 10): 1 for i in range(1, 11)}
_TV = {(1, 100): 1, (5, 50): 1}


def test_a_view_a_stream_reaches_comes_back_as_if_the_stream_never_received_a_row(own_server):
    """Nothing a stream ingests is recovered: a crash replays its SAL tail into no
    view, and a graceful stop checkpoints no view it reaches — the boot rebuilds
    those whatever their stores hold, so a published one would be state nothing
    reads. Either way each comes back holding what it would if the stream had
    never received a row, which for a set difference is *more* rows than before.

    The base table beside the stream is unaffected, at its own weights: a
    replayed group applied twice is ten rows at weight 2."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql("CREATE TABLE dim (k BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)")
        conn.execute_sql(f"CREATE TABLE t ({_EVENT_COLS})")
        _stream(conn)
        conn.execute_sql("CREATE VIEW tv AS SELECT kind, SUM(amount) AS total FROM t GROUP BY kind")
        conn.execute_sql("CREATE VIEW moved AS SELECT kind, SUM(amount) AS total FROM t GROUP BY kind")
        for name, body, _ in _BOOT_SHAPES:
            if body is not None:
                conn.execute_sql(f"CREATE VIEW {name} AS {body}")
        insert(conn, "dim", [(k, k * 7) for k in (1, 2, 3)])
        insert(conn, "t", [(100, 5, 50)])
        assert _read(conn, "moved") == {(5, 50): 1}
        conn.execute_sql(f"ALTER VIEW moved AS {_HOT}")
        insert(conn, "t", [(i, 1, 10) for i in range(1, 11)])
        ids = {name: conn.resolve_table(name)[0] for name in ["tv", *_PUSHED]}

    def push_and_restart(**how):
        with gnitz.connect(own_server.target) as conn:
            insert(conn, "s", [(1, 1, 100), (2, 2, 200)])
            assert {name: _read(conn, name) for name in _PUSHED} == _PUSHED
        own_server.restart(**how)
        with gnitz.connect(own_server.target) as conn:
            assert {name: _read(conn, name) for name in _PUSHED} == {name: want for name, _, want in _BOOT_SHAPES}
            assert _read(conn, "tv") == _TV
            assert _read(conn, "t") == {**_T_ROWS, (100, 5, 50): 1}

    def published(name):
        return glob.glob(os.path.join(relation_dir(own_server.data_dir, ids[name]), "*", "manifest.bin"))

    push_and_restart()
    push_and_restart(graceful=True)
    assert own_server.rebuilt_view_count() == len(_PUSHED), "the views the stream reaches, and not `tv`"

    # The stream's own definition is durable, and still accepts pushes the
    # rebuilt views track.
    with gnitz.connect(own_server.target) as conn:
        insert(conn, "s", [(1, 1, 42)])
        assert _read(conn, "over_agg") == {(1, 42): 1}
        assert _read(conn, "grown") == {(2,): 1, (3,): 1}
    # The boot's own checkpoint and this stop's publish none of them either.
    own_server.stop_graceful()
    assert published("tv"), "a view over a table is published"
    assert [name for name in _PUSHED if published(name)] == []


# ---------------------------------------------------------------------------
# Append-only bag arithmetic
# ---------------------------------------------------------------------------


def test_a_stream_is_an_append_only_bag(client):
    """The PK is a routing and sort key, not a uniqueness constraint: the same
    row twice is one element at weight 2, two rows sharing a PK but differing in
    payload are two elements, and a push may carry any weight >= 1. A retraction
    is refused, and leaves nothing behind.

    RETURNING projects the batch the client built, so it answers on a stream too.
    """
    _stream(client)
    client.execute_sql("CREATE VIEW pass AS SELECT id, kind, amount FROM s")
    tid, schema = client.resolve_table("s")

    # The same statement twice, where a table would raise a duplicate-key error.
    returned = rows(client, "INSERT INTO s VALUES (1, 4, 100) RETURNING id, amount")
    assert bag(returned) == {(1, 100): 1}
    client.execute_sql("INSERT INTO s VALUES (1, 4, 100)")
    # A second payload under the same PK is a second element, not a replacement.
    client.execute_sql("INSERT INTO s VALUES (1, 4, 999)")
    want = {(1, 4, 100): 2, (1, 4, 999): 1}
    assert _read(client, "pass") == want

    with pytest.raises(gnitz.GnitzRefusedError, match="append-only"):
        client.delete(tid, schema, [1])
    # A batch with one retraction is refused whole, its insert included.
    mixed = gnitz.ZSetBatch(schema).extend([
        {"id": 3, "kind": 3, "amount": 3, "_weight": 1},
        {"id": 1, "kind": 4, "amount": 100, "_weight": -1},
    ])
    with pytest.raises(gnitz.GnitzRefusedError, match="append-only"):
        client.push(tid, mixed)
    assert _read(client, "pass") == want, "a refused push must leave nothing behind"

    # A weight above 1 is bag multiplicity, and lands as that weight.
    assert client.push(tid, gnitz.ZSetBatch(schema).extend([{"id": 2, "kind": 2, "amount": 3, "_weight": 5}])) == 0
    assert _read(client, "pass") == {**want, (2, 2, 3): 5}


# ---------------------------------------------------------------------------
# Placement and durability
# ---------------------------------------------------------------------------


@NEEDS_MULTI
def test_a_cluster_by_stream_routes_by_its_prefix(client):
    """Grouped on exactly its CLUSTER BY prefix, the reduce skips its exchange, so
    a push routed by anything but the prefix would split a group across workers."""
    client.execute_sql(
        "CREATE TABLE cs (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b)) WITH (stream = true) CLUSTER BY a",
    )
    client.execute_sql("CREATE VIEW cv AS SELECT a, SUM(v) AS total FROM cs GROUP BY a")
    insert(client, "cs", [(a, b, a * 100 + b) for a in range(1, 11) for b in range(1, 4)])
    assert _read(client, "cv") == {(a, 300 * a + 6): 1 for a in range(1, 11)}


def test_a_stream_push_replies_lsn_zero(client):
    """An ACK reports an LSN only for a write a restart must recover: a base-table
    push replies its zone LSN, and a stream push, which is not durable, `0`."""
    client.execute_sql(f"CREATE TABLE t ({_EVENT_COLS})")
    _stream(client)
    lsn = {}
    for name in ("t", "s"):
        tid, schema = client.resolve_table(name)
        lsn[name] = client.push(tid, gnitz.ZSetBatch(schema).extend([{"id": 1, "kind": 1, "amount": 10}]))
    assert lsn["t"] > 0 and lsn["s"] == 0, lsn
