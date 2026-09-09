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


def _values(tuples):
    return ", ".join("(" + ", ".join(str(v) for v in r) + ")" for r in tuples)


def _stream(conn, sn, name="s"):
    conn.execute_sql(f"CREATE TABLE {name} ({_EVENT_COLS}) WITH (stream = true)", schema_name=sn)


def _push_events(conn, sn, name, tuples):
    conn.execute_sql(f"INSERT INTO {name} VALUES {_values(tuples)}", schema_name=sn)


def _totals(conn, sn, view):
    return bag(rows(conn, sn, f"SELECT * FROM {view}"), "kind", "total")


# ---------------------------------------------------------------------------
# A view over a stream tracks pushes; the stream itself is unreadable
# ---------------------------------------------------------------------------


def test_groupby_view_over_a_stream_tracks_pushes(client, schema_name):
    sn = schema_name
    _stream(client, sn)
    client.execute_sql(
        "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
        schema_name=sn,
    )
    _push_events(client, sn, "s", [(i, i % 3, i * 10) for i in range(1, 31)])
    expected = {}
    for i in range(1, 31):
        expected[i % 3] = expected.get(i % 3, 0) + i * 10
    assert _totals(client, sn, "hot") == {kv: 1 for kv in expected.items()}


def test_a_stream_is_not_readable_by_sql_or_any_binary_read_verb(client, schema_name):
    """Zero rows would make a live stream indistinguishable from a typo'd empty
    table, so every read path errors instead. The binary verbs each take a
    distinct executor path — `scan` / `seek` / `seek_by_index` through `read_lock`,
    `scan_many` through its own funnel (which without its own check would return
    zero rows from the detached store's empty cursor), and `delta_bootstrap`
    through the feed registry."""
    sn = schema_name
    _stream(client, sn)
    tid, schema = client.resolve_table(sn, "s")

    for sql in (
        "SELECT * FROM s",
        "SELECT kind FROM s WHERE amount > 0",
        "SELECT COUNT(*) FROM s",
        # A CTE body goes through the same `Binder::resolve` funnel.
        "WITH c AS (SELECT id FROM s) SELECT id FROM c",
    ):
        with pytest.raises(gnitz.GnitzError, match="stream"):
            client.execute_sql(sql, schema_name=sn)

    for verb in (
        lambda: list(client.scan(tid)),
        lambda: list(client.seek(tid, 1)),
        lambda: list(client.seek_by_index(tid, [1], [0])),
        lambda: client.scan_many([tid]),
        lambda: client.delta_bootstrap(tid, schema),
    ):
        with pytest.raises(gnitz.GnitzError, match="stream"):
            verb()
    # A poll carries a cursor, and no cursor can name a relation that has no
    # feed — so this one is refused by the tag check before the stream rule is
    # reached, and names that instead.
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        client.delta_poll(tid, gnitz.delta_reply_schema(schema), (0, 0))


def test_a_stream_fed_view_reads_through_scan_and_scan_many(client, schema_name):
    """Both read paths run `read_is_fresh` on their own, and a stream push never
    advances `published()` — so each must drain its own pending tick."""
    sn = schema_name
    _stream(client, sn)
    client.execute_sql(
        "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
        schema_name=sn,
    )
    _push_events(client, sn, "s", [(1, 7, 100), (2, 7, 5)])
    vid, _ = client.resolve_table(sn, "hot")

    assert bag(client.scan(vid), "kind", "total") == {(7, 105): 1}

    # A second push, then read the same view through the other funnel.
    _push_events(client, sn, "s", [(3, 7, 1)])
    assert bag(client.scan_many([vid])[0], "kind", "total") == {(7, 106): 1}


# ---------------------------------------------------------------------------
# Recovery: a stream-fed view returns to its never-received-a-row value
# ---------------------------------------------------------------------------


# `(view name, body, value once the stream is back to never-having-received-a-row)`.
# The stream carries `kind` 1 and 2; `dim` carries k 1, 2 and 3.
_BOOT_SHAPES = [
    ("agg", "SELECT kind, SUM(amount) AS total FROM s GROUP BY kind", {}),
    ("inner_v", "SELECT dim.k, s.amount FROM dim JOIN s ON dim.k = s.kind", {}),
    # Every preserved row comes back null-filled: the null-fill is weight-exact,
    # so a witness clamped to 1 over a matched row would leak an extra one here.
    ("left_v", "SELECT dim.k, s.amount FROM dim LEFT JOIN s ON dim.k = s.kind",
     {(1, None): 1, (2, None): 1, (3, None): 1}),
    # The non-monotone direction: the view *grows* when the stream empties.
    ("grown", "SELECT k FROM dim EXCEPT SELECT kind FROM s", {(1,): 1, (2,): 1, (3,): 1}),
]


def test_a_stream_fed_view_returns_to_its_never_received_a_row_value(own_server):
    """A graceful stop checkpoints each view's output store and operator traces at
    the committed generation, so the boot must reject that state rather than resume
    onto inputs that no longer exist. The value it comes back with is what the view
    would hold if the stream had never received a row — which for a set difference
    is *more* rows than it held before, not fewer."""
    sn = "sboot"
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema(sn)
        conn.execute_sql(
            "CREATE TABLE dim (k BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)", schema_name=sn
        )
        _stream(conn, sn)
        for name, body, _after in _BOOT_SHAPES:
            conn.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=sn)
        conn.execute_sql(f"INSERT INTO dim VALUES {_values([(k, k * 7) for k in (1, 2, 3)])}", schema_name=sn)
        _push_events(conn, sn, "s", [(1, 1, 100), (2, 2, 200)])

        before = {name: bag(rows(conn, sn, f"SELECT * FROM {name}")) for name, _b, _a in _BOOT_SHAPES}
        assert before["agg"] == {(1, 100): 1, (2, 200): 1}
        assert before["inner_v"] == {(1, 100): 1, (2, 200): 1}
        assert before["left_v"] == {(1, 100): 1, (2, 200): 1, (3, None): 1}
        assert before["grown"] == {(3,): 1}

    own_server.restart(graceful=True)

    with gnitz.connect(own_server.sock_path) as conn:
        after = {name: bag(rows(conn, sn, f"SELECT * FROM {name}")) for name, _b, _a in _BOOT_SHAPES}
        assert after == {name: want for name, _b, want in _BOOT_SHAPES}
        # The stream's own definition is durable: still there, still unreadable,
        # and still accepting pushes the rebuilt views track.
        _push_events(conn, sn, "s", [(1, 1, 42)])
        assert bag(rows(conn, sn, "SELECT * FROM agg")) == {(1, 42): 1}
        assert bag(rows(conn, sn, "SELECT * FROM grown")) == {(2,): 1, (3,): 1}


def test_a_crash_keeps_the_table_tail_and_drops_the_stream_tail(own_server):
    """A SIGKILL runs no checkpoint, so everything comes back through SAL replay. A
    stream group in the replayed tail is skipped before it is even decoded, so it
    reaches no view. The base table beside it must be unaffected — and at its own
    weights: a replayed group applied twice is ten rows at weight 2."""
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
        conn.execute_sql(f"INSERT INTO t VALUES {_values([(i, 1, 10) for i in range(1, 11)])}", schema_name=sn)
        _push_events(conn, sn, "s", [(i, 2, 10) for i in range(1, 11)])
        assert _totals(conn, sn, "tv") == {(1, 100): 1}
        assert _totals(conn, sn, "sv") == {(2, 100): 1}

    own_server.restart()

    with gnitz.connect(own_server.sock_path) as conn:
        assert _totals(conn, sn, "tv") == {(1, 100): 1}, "the base table's tail must still replay into its view"
        assert _totals(conn, sn, "sv") == {}, "the stream's tail must reach no view"
        assert bag(rows(conn, sn, "SELECT * FROM t")) == {(i, 1, 10): 1 for i in range(1, 11)}


def test_alter_view_onto_a_stream_makes_it_ephemeral(own_server):
    """The invalidation clause reads the view's sources at boot, so it reaches a
    view that was not stream-fed when it was created."""
    sn = "salter"
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema(sn)
        conn.execute_sql(f"CREATE TABLE t ({_EVENT_COLS})", schema_name=sn)
        _stream(conn, sn)
        conn.execute_sql(
            "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM t GROUP BY kind", schema_name=sn
        )
        conn.execute_sql(f"INSERT INTO t VALUES {_values([(1, 5, 50)])}", schema_name=sn)
        assert _totals(conn, sn, "hot") == {(5, 50): 1}

        conn.execute_sql(
            "ALTER VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind", schema_name=sn
        )
        _push_events(conn, sn, "s", [(1, 8, 80)])
        assert _totals(conn, sn, "hot") == {(8, 80): 1}

    own_server.restart(graceful=True)

    with gnitz.connect(own_server.sock_path) as conn:
        assert _totals(conn, sn, "hot") == {}, "a retargeted view is stream-fed from that point on"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_a_view_over_a_stream_fed_view_backfills_from_its_output(client, schema_name):
    """A view over a *stream-fed view* is not special: it backfills from that
    view's accumulated output store, exactly as over any view."""
    sn = schema_name
    _stream(client, sn)
    client.execute_sql(
        "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind", schema_name=sn
    )
    _push_events(client, sn, "s", [(i, i % 3, 10) for i in range(1, 31)])
    # Created after the pushes, so anything it holds came from `hot`'s store.
    client.execute_sql("CREATE VIEW big AS SELECT kind, total FROM hot WHERE total > 90", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM big"), "kind", "total") == \
        {(0, 100): 1, (1, 100): 1, (2, 100): 1}


def test_a_view_created_after_drained_pushes_holds_none_of_them(client, schema_name):
    """A stream's backfill scans an empty store, so a view starts at zero and
    accumulates from CREATE VIEW onward. The read before the CREATE drains the
    pending delta, so none of those rows are still pending."""
    sn = schema_name
    _stream(client, sn)
    body = "SELECT kind, SUM(amount) AS total FROM s GROUP BY kind"
    client.execute_sql(f"CREATE VIEW early AS {body}", schema_name=sn)
    _push_events(client, sn, "s", [(i, 1, 10) for i in range(1, 11)])
    assert _totals(client, sn, "early") == {(1, 100): 1}, "the pushes are drained"

    client.execute_sql(f"CREATE VIEW late AS {body}", schema_name=sn)
    assert _totals(client, sn, "late") == {}
    _push_events(client, sn, "s", [(99, 1, 7)])
    assert _totals(client, sn, "late") == {(1, 7): 1}
    assert _totals(client, sn, "early") == {(1, 107): 1}


def test_drop_and_rename_stay_available_on_a_stream(client, schema_name):
    """A stream is a TABLE_TAB row, so DROP inherits the kind-agnostic
    dependent-view RESTRICT of the relation-drop precheck with no new engine
    code, and `RENAME TO` is a separate, kind-agnostic metadata-only path."""
    sn = schema_name
    _stream(client, sn)
    client.execute_sql("CREATE VIEW hot AS SELECT id, amount FROM s", schema_name=sn)

    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("DROP TABLE s", schema_name=sn)

    client.execute_sql("ALTER TABLE s RENAME TO events", schema_name=sn)
    _push_events(client, sn, "events", [(1, 2, 30)])
    assert bag(rows(client, sn, "SELECT * FROM hot"), "id", "amount") == {(1, 30): 1}

    client.execute_sql("DROP VIEW hot", schema_name=sn)
    client.execute_sql("DROP TABLE events", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE VIEW hot2 AS SELECT id FROM events", schema_name=sn)


# ---------------------------------------------------------------------------
# Append-only bag arithmetic
# ---------------------------------------------------------------------------


def test_a_stream_is_an_append_only_bag(client, schema_name):
    """The PK is a routing and sort key, not a uniqueness constraint: the same
    row twice is one element at weight 2, two rows sharing a PK but differing in
    payload are two elements, and a push may carry any weight >= 1.

    Nothing clamps a stream's weights the way `enforce_unique_pk` clamps a base
    table's, so one negative accumulated weight in a derived relation would
    fabricate rows through `positive_part`, destroy a linear reduce's accumulator
    and break `LIMIT` — all silently. `delete` builds a retraction batch and is
    the routine way to reach the rejection.
    """
    sn = schema_name
    _stream(client, sn)
    client.execute_sql("CREATE VIEW pass AS SELECT id, kind, amount FROM s", schema_name=sn)
    tid, schema = client.resolve_table(sn, "s")

    # The same statement twice, where a table would raise a duplicate-key error.
    client.execute_sql("INSERT INTO s VALUES (1, 4, 100)", schema_name=sn)
    client.execute_sql("INSERT INTO s VALUES (1, 4, 100)", schema_name=sn)
    # A second payload under the same PK is a second element, not a replacement.
    client.execute_sql("INSERT INTO s VALUES (1, 4, 999)", schema_name=sn)
    want = {(1, 4, 100): 2, (1, 4, 999): 1}
    assert bag(rows(client, sn, "SELECT * FROM pass")) == want

    for weight in (0, -1):
        b = gnitz.ZSetBatch(schema)
        b.append(id=2, kind=2, amount=3, _weight=weight)
        with pytest.raises(gnitz.GnitzError, match="append-only"):
            client.push(tid, b)
    with pytest.raises(gnitz.GnitzError, match="append-only"):
        client.delete(tid, schema, [1])
    assert bag(rows(client, sn, "SELECT * FROM pass")) == want, "a refused push must leave nothing behind"

    # A weight above 1 is legal — bag multiplicity, the same shape `UNION ALL`
    # already produces — and lands as that weight rather than as one row.
    b = gnitz.ZSetBatch(schema)
    b.append(id=2, kind=2, amount=3, _weight=5)
    client.push(tid, b)
    assert bag(rows(client, sn, "SELECT * FROM pass")) == {**want, (2, 2, 3): 5}


def test_insert_returning_works_on_a_stream(client, schema_name):
    """RETURNING on the plain-INSERT path is a projection of the batch the client
    just built, so it reads no store."""
    sn = schema_name
    _stream(client, sn)
    got = rows(client, sn, "INSERT INTO s VALUES (1, 2, 30), (2, 3, 40) RETURNING id, amount")
    assert bag(got) == {(1, 30): 1, (2, 40): 1}


# ---------------------------------------------------------------------------
# Placement
# ---------------------------------------------------------------------------


@NEEDS_MULTI
def test_a_replicated_stream_aggregates_without_a_w_fold_overcount(client, schema_name):
    """A replicated stream must report `replicated = true` in its resolve reply.
    Reported as non-replicated, the client's two-phase reduce becomes eligible and
    every worker's full copy contributes a partial — a silent W-fold overcount,
    which lands as one row at weight W or as a W-times-too-large sum."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE rs2 (" + _EVENT_COLS + ") WITH (stream = true, replicated = true)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW rtot AS SELECT kind, SUM(amount) AS total, COUNT(*) AS c FROM rs2 GROUP BY kind",
        schema_name=sn,
    )
    _push_events(client, sn, "rs2", [(i, 1, 10) for i in range(1, 11)])
    assert bag(rows(client, sn, "SELECT * FROM rtot")) == {(1, 100, 10): 1}


@NEEDS_MULTI
def test_a_cluster_by_stream_routes_by_its_prefix(client, schema_name):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE cs (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b)) WITH (stream = true) CLUSTER BY a",
        schema_name=sn,
    )
    client.execute_sql("CREATE VIEW cv AS SELECT a, SUM(v) AS total FROM cs GROUP BY a", schema_name=sn)
    pushed = [(a, b, a * 100 + b) for a in range(1, 11) for b in range(1, 4)]
    _push_events(client, sn, "cs", pushed)
    expected = {}
    for a, _b, v in pushed:
        expected[a] = expected.get(a, 0) + v
    assert bag(rows(client, sn, "SELECT * FROM cv")) == {kv: 1 for kv in expected.items()}


# ---------------------------------------------------------------------------
# The SAL durability contract
# ---------------------------------------------------------------------------


def test_a_stream_write_inside_a_transaction_is_rejected_atomically(client, schema_name):
    """A transaction promises all-or-nothing recovery and stream data is never
    recovered. `push_txn_body` returns on the first offending family before any
    family is written, so the transaction's other writes are rejected with it rather
    than lost after a partial commit."""
    sn = schema_name
    cols = [
        gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("kind", gnitz.TypeCode.I64),
        gnitz.ColumnDef("amount", gnitz.TypeCode.I64),
    ]
    t_tid = client.create_table(sn, "t", cols)
    _stream(client, sn)
    s_tid, s_schema = client.resolve_table(sn, "s")

    tb = gnitz.ZSetBatch(gnitz.Schema(cols))
    tb.append(id=1, kind=1, amount=1, _weight=1)
    sb = gnitz.ZSetBatch(s_schema)
    sb.append(id=2, kind=2, amount=2, _weight=1)

    with pytest.raises(gnitz.GnitzError, match="stream"):
        with client.transaction() as txn:
            txn.push(t_tid, tb)
            txn.push(s_tid, sb)

    assert bag(client.scan(t_tid)) == {}, "the other family must be rejected with it"


def test_a_stream_push_does_not_advance_the_clients_occ_basis(client, schema_name):
    """A stream push replies LSN `0`, so `track_lsn`'s `max` leaves `last_seen_lsn`
    untouched and the documented "always <= published()" invariant holds. Replying the
    reserved zone LSN instead would hand out a basis naming a zone that was never
    fsynced, and the base-table RMW below would then pass its precondition against it.
    Keyed on the target rather than on whether the batch opened a zone, so a stream
    push the committer coalesces with a base-table push still answers `0`."""
    sn = schema_name
    cols = [
        gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("amount", gnitz.TypeCode.I64),
    ]
    t_tid = client.create_table(sn, "t", cols)
    _stream(client, sn)
    s_tid, s_schema = client.resolve_table(sn, "s")

    before = client.last_seen_lsn
    sb = gnitz.ZSetBatch(s_schema)
    sb.append(id=1, kind=1, amount=1, _weight=1)
    assert client.push(s_tid, sb) == 0, "a stream push is not durable"
    assert client.last_seen_lsn == before

    tb = gnitz.ZSetBatch(gnitz.Schema(cols))
    tb.append(id=1, amount=10, _weight=1)
    assert client.push(t_tid, tb) > 0
    # An RMW statement cites `last_seen_lsn` as its basis; a basis above
    # `published()` would come back as a TxnConflict instead of committing.
    client.execute_sql("UPDATE t SET amount = 11 WHERE id = 1", schema_name=sn)
    assert bag(client.scan(t_tid)) == {(1, 11): 1}
