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

import pytest
import gnitz
from _read import bag, rows
from _serverproc import NEEDS_MULTI

_EVENT_COLS = "id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL"
_HOT = "SELECT kind, SUM(amount) AS total FROM s GROUP BY kind"


def _stream(conn, sn):
    conn.execute_sql(f"CREATE TABLE s ({_EVENT_COLS}) WITH (stream = true)", schema_name=sn)


def _insert(conn, sn, name, tuples):
    values = ", ".join("(" + ", ".join(str(v) for v in r) + ")" for r in tuples)
    conn.execute_sql(f"INSERT INTO {name} VALUES {values}", schema_name=sn)


def _read(conn, sn, view):
    return bag(rows(conn, sn, f"SELECT * FROM {view}"))


# ---------------------------------------------------------------------------
# Views over a stream
# ---------------------------------------------------------------------------


def test_views_over_a_stream_track_pushes_from_their_creation_on(client, schema_name):
    """A GROUP BY view tracks every push. A view over that *view* backfills from
    its accumulated output store like over any view, while a second view over the
    stream itself starts empty: a stream's backfill scans a store that holds no
    rows. Every read verb — SQL, `scan`, `scan_many` — drains its own pending
    tick, since a stream push never advances the published tick."""
    sn = schema_name
    _stream(client, sn)
    client.execute_sql(f"CREATE VIEW hot AS {_HOT}", schema_name=sn)
    _insert(client, sn, "s", [(i, i % 3, i * 10) for i in range(1, 31)])
    assert _read(client, sn, "hot") == {(0, 1650): 1, (1, 1450): 1, (2, 1550): 1}

    client.execute_sql("CREATE VIEW big AS SELECT kind, total FROM hot WHERE total > 1500", schema_name=sn)
    client.execute_sql(f"CREATE VIEW late AS {_HOT}", schema_name=sn)
    assert _read(client, sn, "big") == {(0, 1650): 1, (2, 1550): 1}
    assert _read(client, sn, "late") == {}

    vid, _ = client.resolve_table(sn, "hot")
    _insert(client, sn, "s", [(99, 1, 7)])
    assert bag(client.scan(vid)) == {(0, 1650): 1, (1, 1457): 1, (2, 1550): 1}
    _insert(client, sn, "s", [(100, 1, 100)])
    assert bag(client.scan_many([vid])[0]) == {(0, 1650): 1, (1, 1557): 1, (2, 1550): 1}
    assert _read(client, sn, "late") == {(1, 107): 1}
    assert _read(client, sn, "big") == {(0, 1650): 1, (1, 1557): 1, (2, 1550): 1}


def test_drop_and_rename_stay_available_on_a_stream(client, schema_name):
    """DROP of a stream restricts on dependent views exactly as for a table, and
    `RENAME TO` keeps its views fed."""
    sn = schema_name
    _stream(client, sn)
    client.execute_sql("CREATE VIEW hot AS SELECT id, amount FROM s", schema_name=sn)

    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("DROP TABLE s", schema_name=sn)

    client.execute_sql("ALTER TABLE s RENAME TO events", schema_name=sn)
    _insert(client, sn, "events", [(1, 2, 30)])
    assert _read(client, sn, "hot") == {(1, 30): 1}

    client.execute_sql("DROP VIEW hot", schema_name=sn)
    client.execute_sql("DROP TABLE events", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE VIEW hot2 AS SELECT id FROM events", schema_name=sn)


# ---------------------------------------------------------------------------
# Recovery: a stream-fed view returns to its never-received-a-row value
# ---------------------------------------------------------------------------


# `(view name, body, value once the stream is back to never-having-received-a-row)`.
# The stream carries `kind` 1 and 2; `dim` carries k 1, 2 and 3.
_BOOT_SHAPES = [
    ("agg", _HOT, {}),
    ("inner_v", "SELECT dim.k, s.amount FROM dim JOIN s ON dim.k = s.kind", {}),
    # Every preserved row comes back null-filled: the null-fill is weight-exact,
    # so a witness clamped to 1 over a matched row would leak an extra one here.
    ("left_v", "SELECT dim.k, s.amount FROM dim LEFT JOIN s ON dim.k = s.kind",
     {(1, None): 1, (2, None): 1, (3, None): 1}),
    # The non-monotone direction: the view *grows* when the stream empties.
    ("grown", "SELECT k FROM dim EXCEPT SELECT kind FROM s", {(1,): 1, (2,): 1, (3,): 1}),
]


def test_a_stream_fed_view_returns_to_its_never_received_a_row_value(own_server):
    """A graceful stop checkpoints each view's state, so the boot must reject that
    state rather than resume onto inputs that no longer exist. The value it comes
    back with is what the view would hold if the stream had never received a row —
    which for a set difference is *more* rows than it held before, not fewer.

    `moved` was created over a table and retargeted onto the stream by `ALTER
    VIEW`: the boot decides by the view's sources as they are, not as created."""
    sn = "sboot"
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema(sn)
        conn.execute_sql("CREATE TABLE dim (k BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)", schema_name=sn)
        conn.execute_sql(f"CREATE TABLE t ({_EVENT_COLS})", schema_name=sn)
        _stream(conn, sn)
        for name, body, _after in _BOOT_SHAPES:
            conn.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=sn)
        conn.execute_sql("CREATE VIEW moved AS SELECT kind, SUM(amount) AS total FROM t GROUP BY kind",
                         schema_name=sn)
        _insert(conn, sn, "dim", [(k, k * 7) for k in (1, 2, 3)])
        _insert(conn, sn, "t", [(1, 5, 50)])
        assert _read(conn, sn, "moved") == {(5, 50): 1}
        conn.execute_sql(f"ALTER VIEW moved AS {_HOT}", schema_name=sn)
        _insert(conn, sn, "s", [(1, 1, 100), (2, 2, 200)])

        names = [name for name, _b, _a in _BOOT_SHAPES] + ["moved"]
        assert {name: _read(conn, sn, name) for name in names} == {
            "agg": {(1, 100): 1, (2, 200): 1},
            "inner_v": {(1, 100): 1, (2, 200): 1},
            "left_v": {(1, 100): 1, (2, 200): 1, (3, None): 1},
            "grown": {(3,): 1},
            "moved": {(1, 100): 1, (2, 200): 1},
        }

    own_server.restart(graceful=True)

    with gnitz.connect(own_server.sock_path) as conn:
        assert {name: _read(conn, sn, name) for name in names} == {
            **{name: want for name, _b, want in _BOOT_SHAPES}, "moved": {},
        }
        # The stream's own definition is durable, and still accepts pushes the
        # rebuilt views track.
        _insert(conn, sn, "s", [(1, 1, 42)])
        assert _read(conn, sn, "agg") == {(1, 42): 1}
        assert _read(conn, sn, "grown") == {(2,): 1, (3,): 1}


def test_a_crash_keeps_the_table_tail_and_drops_the_stream_tail(own_server):
    """A SIGKILL runs no checkpoint, so everything comes back through SAL replay,
    and a stream group in the replayed tail reaches no view. The base table beside
    it must be unaffected — and at its own weights: a replayed group applied twice
    is ten rows at weight 2."""
    sn = "scrash"
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema(sn)
        conn.execute_sql(f"CREATE TABLE t ({_EVENT_COLS})", schema_name=sn)
        _stream(conn, sn)
        for name, src in (("tv", "t"), ("sv", "s")):
            conn.execute_sql(
                f"CREATE VIEW {name} AS SELECT kind, SUM(amount) AS total FROM {src} GROUP BY kind",
                schema_name=sn,
            )
        _insert(conn, sn, "t", [(i, 1, 10) for i in range(1, 11)])
        _insert(conn, sn, "s", [(i, 2, 10) for i in range(1, 11)])
        assert _read(conn, sn, "tv") == {(1, 100): 1}
        assert _read(conn, sn, "sv") == {(2, 100): 1}

    own_server.restart()

    with gnitz.connect(own_server.sock_path) as conn:
        assert _read(conn, sn, "tv") == {(1, 100): 1}, "the base table's tail must still replay into its view"
        assert _read(conn, sn, "sv") == {}, "the stream's tail must reach no view"
        assert _read(conn, sn, "t") == {(i, 1, 10): 1 for i in range(1, 11)}


# ---------------------------------------------------------------------------
# Append-only bag arithmetic
# ---------------------------------------------------------------------------


def test_a_stream_is_an_append_only_bag(client, schema_name):
    """The PK is a routing and sort key, not a uniqueness constraint: the same
    row twice is one element at weight 2, two rows sharing a PK but differing in
    payload are two elements, and a push may carry any weight >= 1. A retraction
    is refused, and leaves nothing behind.

    RETURNING projects the batch the client built, so it answers on a stream too.
    """
    sn = schema_name
    _stream(client, sn)
    client.execute_sql("CREATE VIEW pass AS SELECT id, kind, amount FROM s", schema_name=sn)
    tid, schema = client.resolve_table(sn, "s")

    # The same statement twice, where a table would raise a duplicate-key error.
    returned = rows(client, sn, "INSERT INTO s VALUES (1, 4, 100) RETURNING id, amount")
    assert bag(returned) == {(1, 100): 1}
    client.execute_sql("INSERT INTO s VALUES (1, 4, 100)", schema_name=sn)
    # A second payload under the same PK is a second element, not a replacement.
    client.execute_sql("INSERT INTO s VALUES (1, 4, 999)", schema_name=sn)
    want = {(1, 4, 100): 2, (1, 4, 999): 1}
    assert _read(client, sn, "pass") == want

    with pytest.raises(gnitz.GnitzError, match="append-only"):
        client.delete(tid, schema, [1])
    assert _read(client, sn, "pass") == want, "a refused push must leave nothing behind"

    # A weight above 1 is bag multiplicity, and lands as that weight.
    b = gnitz.ZSetBatch(schema)
    b.append(id=2, kind=2, amount=3, _weight=5)
    client.push(tid, b)
    assert _read(client, sn, "pass") == {**want, (2, 2, 3): 5}


# ---------------------------------------------------------------------------
# Placement and durability
# ---------------------------------------------------------------------------


@NEEDS_MULTI
def test_a_cluster_by_stream_routes_by_its_prefix(client, schema_name):
    """Grouped on exactly its CLUSTER BY prefix, the reduce skips its exchange, so
    a push routed by anything but the prefix would split a group across workers."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE cs (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b)) WITH (stream = true) CLUSTER BY a",
        schema_name=sn,
    )
    client.execute_sql("CREATE VIEW cv AS SELECT a, SUM(v) AS total FROM cs GROUP BY a", schema_name=sn)
    _insert(client, sn, "cs", [(a, b, a * 100 + b) for a in range(1, 11) for b in range(1, 4)])
    assert _read(client, sn, "cv") == {(a, 300 * a + 6): 1 for a in range(1, 11)}


def test_a_stream_push_does_not_advance_the_clients_occ_basis(client, schema_name):
    """A stream push is not durable, so it replies LSN `0` and leaves the client's
    read basis where the last durable write put it. Were it to hand out the zone
    LSN it reserved instead — one that is never published — the read-modify-write
    statement after it would cite a basis above anything published and conflict."""
    sn = schema_name
    client.execute_sql(f"CREATE TABLE t ({_EVENT_COLS})", schema_name=sn)
    _stream(client, sn)
    t_tid, t_schema = client.resolve_table(sn, "t")
    s_tid, s_schema = client.resolve_table(sn, "s")

    tb = gnitz.ZSetBatch(t_schema)
    tb.append(id=1, kind=1, amount=10, _weight=1)
    assert client.push(t_tid, tb) > 0
    sb = gnitz.ZSetBatch(s_schema)
    sb.append(id=1, kind=1, amount=1, _weight=1)
    assert client.push(s_tid, sb) == 0, "a stream push is not durable"

    client.execute_sql("UPDATE t SET amount = 11 WHERE id = 1", schema_name=sn)
    assert bag(client.scan(t_tid)) == {(1, 1, 11): 1}
