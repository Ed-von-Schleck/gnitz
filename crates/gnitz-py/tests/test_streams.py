"""E2E tests for streams: `CREATE TABLE … WITH (stream = true)`.

A stream is a relation with a schema and a primary key that holds no rows. Views
over it are maintained exactly as views over a table are, but nothing it ingests is
recovered: pushed rows exist only as the deltas they produce, so every view reaching
a stream comes back at boot holding the value it would have had if the stream had
never received a row. The stream's *definition* is ordinary durable catalog state.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_streams.py -v --tb=short
"""

import pytest
import gnitz
from _serverproc import NUM_WORKERS as _NUM_WORKERS
from _uid import uid as _uid

_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="placement and exchange only matter with GNITZ_WORKERS >= 2"
)




def _rows(conn, sn, q):
    res = conn.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


def _values(rows):
    return ", ".join("(" + ", ".join(str(v) for v in r) + ")" for r in rows)


_EVENT_COLS = "id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL"


def _stream(conn, sn, name="s"):
    conn.execute_sql(f"CREATE TABLE {name} ({_EVENT_COLS}) WITH (stream = true)", schema_name=sn)


def _push_events(conn, sn, name, rows):
    conn.execute_sql(f"INSERT INTO {name} VALUES {_values(rows)}", schema_name=sn)


def _totals(conn, sn, view):
    return sorted((r["kind"], r["total"]) for r in _rows(conn, sn, f"SELECT * FROM {view}"))


# ---------------------------------------------------------------------------
# A view over a stream tracks pushes; the stream itself is unreadable
# ---------------------------------------------------------------------------


def test_groupby_view_over_a_stream_tracks_pushes(client):
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql(
            "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
            schema_name=sn,
        )
        _push_events(client, sn, "s", [(i, i % 3, i * 10) for i in range(1, 31)])
        expected = {}
        for i in range(1, 31):
            expected[i % 3] = expected.get(i % 3, 0) + i * 10
        assert _totals(client, sn, "hot") == sorted(expected.items())
    finally:
        client.drop_schema(sn)


def test_a_stream_is_not_readable_by_sql_or_any_binary_read_verb(client):
    """Zero rows would make a live stream indistinguishable from a typo'd empty
    table, so every read path errors instead. The four binary verbs each take a
    distinct executor path — `scan` / `seek` / `seek_by_index` through `read_lock`,
    and `scan_many` through its own funnel, which without its own check would return
    zero rows from the detached store's empty cursor."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        _push_events(client, sn, "s", [(1, 0, 100)])
        tid, _schema = client.resolve_table(sn, "s")

        for sql in (
            "SELECT * FROM s",
            "SELECT kind FROM s WHERE amount > 0",
            "SELECT COUNT(*) FROM s",
            # A CTE body goes through the same `Binder::resolve` funnel.
            "WITH c AS (SELECT id FROM s) SELECT id FROM c",
        ):
            with pytest.raises(Exception, match="stream"):
                client.execute_sql(sql, schema_name=sn)

        with pytest.raises(Exception, match="stream"):
            list(client.scan(tid))
        with pytest.raises(Exception, match="stream"):
            list(client.seek(tid, 1))
        with pytest.raises(Exception, match="stream"):
            list(client.seek_by_index(tid, [1], [0]))
        with pytest.raises(Exception, match="stream"):
            client.scan_many([tid])
    finally:
        client.drop_schema(sn)


def test_a_stream_fed_view_reads_through_scan_and_scan_many(client):
    """Both read paths run `read_is_fresh` on their own, and a stream push never
    advances `published()` — so each must drain its own pending tick."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql(
            "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
            schema_name=sn,
        )
        _push_events(client, sn, "s", [(1, 7, 100), (2, 7, 5)])
        vid, _ = client.resolve_table(sn, "hot")

        by_scan = sorted((r.kind, r.total) for r in client.scan(vid))
        assert by_scan == [(7, 105)]

        # A second push, then read the same view through the other funnel.
        _push_events(client, sn, "s", [(3, 7, 1)])
        many = client.scan_many([vid])
        assert sorted((r.kind, r.total) for r in many[0]) == [(7, 106)]
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Recovery: a stream-fed view returns to its never-received-a-row value
# ---------------------------------------------------------------------------


def test_a_stream_fed_view_is_empty_after_restart(own_server):
    """A graceful stop checkpoints the view's output store and operator traces at
    the committed generation, so the boot must reject that state rather than resume
    onto inputs that no longer exist."""
    sn = "srst"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    conn.create_schema(sn)
    _stream(conn, sn)
    conn.execute_sql(
        "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
        schema_name=sn,
    )
    _push_events(conn, sn, "s", [(i, i % 3, i * 10) for i in range(1, 31)])
    assert len(_totals(conn, sn, "hot")) == 3
    conn.close()

    own_server.stop_graceful()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    assert _totals(conn, sn, "hot") == [], "a stream-fed view must not resume"
    # The stream's own definition is durable: it is still there, still unreadable,
    # and still accepts pushes that the rebuilt view tracks.
    _push_events(conn, sn, "s", [(1, 9, 42)])
    assert _totals(conn, sn, "hot") == [(9, 42)]
    conn.close()


def test_a_stream_join_view_returns_to_its_no_rows_value_after_restart(own_server):
    """The value a view comes back with is what it would hold if the stream had
    never received a row — zero rows for an inner join, and every preserved row
    null-filled for `table LEFT JOIN stream`."""
    sn = "sjrst"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    conn.create_schema(sn)
    conn.execute_sql(
        "CREATE TABLE dim (k BIGINT UNSIGNED NOT NULL PRIMARY KEY, label BIGINT NOT NULL)",
        schema_name=sn,
    )
    _stream(conn, sn)
    conn.execute_sql(
        "CREATE VIEW inner_v AS SELECT dim.k, dim.label, s.amount FROM dim JOIN s ON dim.k = s.kind",
        schema_name=sn,
    )
    conn.execute_sql(
        "CREATE VIEW left_v AS SELECT dim.k, dim.label, s.amount FROM dim LEFT JOIN s ON dim.k = s.kind",
        schema_name=sn,
    )
    conn.execute_sql(f"INSERT INTO dim VALUES {_values([(k, k * 7) for k in range(1, 4)])}", schema_name=sn)
    _push_events(conn, sn, "s", [(1, 1, 100), (2, 2, 200)])

    assert sorted((r["k"], r["amount"]) for r in _rows(conn, sn, "SELECT * FROM inner_v")) == [(1, 100), (2, 200)]
    left_before = sorted((r["k"], r["amount"]) for r in _rows(conn, sn, "SELECT * FROM left_v"))
    assert left_before == [(1, 100), (2, 200), (3, None)]
    conn.close()

    own_server.stop_graceful()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    assert _rows(conn, sn, "SELECT * FROM inner_v") == []
    assert sorted((r["k"], r["amount"]) for r in _rows(conn, sn, "SELECT * FROM left_v")) == [
        (1, None),
        (2, None),
        (3, None),
    ], "every preserved row comes back null-filled, which is a non-monotone change"
    conn.close()


def test_a_crash_keeps_the_table_tail_and_drops_the_stream_tail(own_server):
    """A SIGKILL runs no checkpoint, so everything comes back through SAL replay. A
    stream group in the replayed tail is skipped before it is even decoded, so it
    reaches no view. The base table beside it must be unaffected."""
    sn = "scrash"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    conn.create_schema(sn)
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn,
    )
    _stream(conn, sn)
    conn.execute_sql(
        "CREATE VIEW tv AS SELECT kind, SUM(amount) AS total FROM t GROUP BY kind", schema_name=sn
    )
    conn.execute_sql(
        "CREATE VIEW sv AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind", schema_name=sn
    )
    conn.execute_sql(f"INSERT INTO t VALUES {_values([(i, 1, 10) for i in range(1, 11)])}", schema_name=sn)
    _push_events(conn, sn, "s", [(i, 2, 10) for i in range(1, 11)])
    assert _totals(conn, sn, "tv") == [(1, 100)]
    assert _totals(conn, sn, "sv") == [(2, 100)]
    conn.close()

    own_server.stop()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    assert _totals(conn, sn, "tv") == [(1, 100)], "the base table's tail must still replay into its view"
    assert _totals(conn, sn, "sv") == [], "the stream's tail must reach no view"
    assert len(_rows(conn, sn, "SELECT * FROM t")) == 10
    conn.close()


def test_alter_view_onto_a_stream_makes_it_ephemeral(own_server):
    """The invalidation clause reads the view's sources at boot, so it reaches a
    view that was not stream-fed when it was created."""
    sn = "salter"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    conn.create_schema(sn)
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn,
    )
    _stream(conn, sn)
    conn.execute_sql(
        "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM t GROUP BY kind",
        schema_name=sn,
    )
    conn.execute_sql(f"INSERT INTO t VALUES {_values([(1, 5, 50)])}", schema_name=sn)
    assert _totals(conn, sn, "hot") == [(5, 50)]

    conn.execute_sql(
        "ALTER VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
        schema_name=sn,
    )
    _push_events(conn, sn, "s", [(1, 8, 80)])
    assert _totals(conn, sn, "hot") == [(8, 80)]
    conn.close()

    own_server.stop_graceful()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    assert _totals(conn, sn, "hot") == [], "a retargeted view is stream-fed from that point on"
    conn.close()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_a_view_over_a_stream_fed_view_backfills_from_its_output(client):
    """A view over a *stream-fed view* is not special: it backfills from that
    view's accumulated output store, exactly as over any view."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql(
            "CREATE VIEW hot AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
            schema_name=sn,
        )
        _push_events(client, sn, "s", [(i, i % 3, 10) for i in range(1, 31)])
        # Created after the pushes, so anything it holds came from `hot`'s store.
        client.execute_sql("CREATE VIEW big AS SELECT kind, total FROM hot WHERE total > 90", schema_name=sn)
        assert sorted((r["kind"], r["total"]) for r in _rows(client, sn, "SELECT * FROM big")) == [
            (0, 100),
            (1, 100),
            (2, 100),
        ]
    finally:
        client.drop_schema(sn)


def test_a_view_created_after_drained_pushes_holds_none_of_them(client):
    """A stream's backfill scans an empty store, so a view starts at zero and
    accumulates from CREATE VIEW onward. The read before the CREATE drains the
    pending delta, so none of those rows are still pending."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql(
            "CREATE VIEW early AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
            schema_name=sn,
        )
        _push_events(client, sn, "s", [(i, 1, 10) for i in range(1, 11)])
        assert _totals(client, sn, "early") == [(1, 100)], "the pushes are drained"

        client.execute_sql(
            "CREATE VIEW late AS SELECT kind, SUM(amount) AS total FROM s GROUP BY kind",
            schema_name=sn,
        )
        assert _totals(client, sn, "late") == []
        _push_events(client, sn, "s", [(99, 1, 7)])
        assert _totals(client, sn, "late") == [(1, 7)]
        assert _totals(client, sn, "early") == [(1, 107)]
    finally:
        client.drop_schema(sn)


def test_drop_table_on_a_stream_is_restricted_by_dependent_views(client):
    """A stream is a TABLE_TAB row, so it inherits the kind-agnostic dependent-view
    RESTRICT of the relation-drop precheck with no new engine code."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql("CREATE VIEW hot AS SELECT id, amount FROM s", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("DROP TABLE s", schema_name=sn)
        client.execute_sql("DROP VIEW hot", schema_name=sn)
        client.execute_sql("DROP TABLE s", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("CREATE VIEW hot2 AS SELECT id FROM s", schema_name=sn)
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# INSERT appends
# ---------------------------------------------------------------------------


def test_insert_into_a_stream_appends_instead_of_conflicting(client):
    """A stream's PK is a routing/sort key, not a uniqueness constraint, so the
    same INSERT twice yields one element at weight 2 — where the same statement
    against a table raises a duplicate-key error."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql("CREATE VIEW n AS SELECT kind, COUNT(*) AS c FROM s GROUP BY kind", schema_name=sn)
        client.execute_sql("INSERT INTO s VALUES (1, 4, 100)", schema_name=sn)
        client.execute_sql("INSERT INTO s VALUES (1, 4, 100)", schema_name=sn)
        assert sorted((r["kind"], r["c"]) for r in _rows(client, sn, "SELECT * FROM n")) == [(4, 2)]
    finally:
        client.drop_schema(sn)


def test_insert_returning_works_on_a_stream(client):
    """RETURNING on the plain-INSERT path is a projection of the batch the client
    just built, so it reads no store."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        rows = _rows(client, sn, "INSERT INTO s VALUES (1, 2, 30), (2, 3, 40) RETURNING id, amount")
        assert sorted((r["id"], r["amount"]) for r in rows) == [(1, 30), (2, 40)]
    finally:
        client.drop_schema(sn)


def test_insert_on_conflict_is_rejected_on_a_stream(client):
    """Both DO NOTHING and DO UPDATE resolve the incoming row against existing rows
    with a client-side seek, and there is no store to seek."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        for action in ("DO NOTHING", "DO UPDATE SET amount = 1"):
            with pytest.raises(Exception, match="stream"):
                client.execute_sql(f"INSERT INTO s VALUES (1, 2, 3) ON CONFLICT (id) {action}", schema_name=sn)
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Placement
# ---------------------------------------------------------------------------


@_NEEDS_MULTI
def test_a_replicated_stream_aggregates_without_a_w_fold_overcount(client):
    """A replicated stream must report `replicated = true` in its resolve reply.
    Reported as non-replicated, the client's two-phase reduce becomes eligible and
    every worker's full copy contributes a partial — a silent W-fold overcount, not
    an error."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE rs2 (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, "
            "amount BIGINT NOT NULL) WITH (stream = true, replicated = true)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW rtot AS SELECT kind, SUM(amount) AS total, COUNT(*) AS c FROM rs2 GROUP BY kind",
            schema_name=sn,
        )
        _push_events(client, sn, "rs2", [(i, 1, 10) for i in range(1, 11)])
        rows = sorted((r["kind"], r["total"], r["c"]) for r in _rows(client, sn, "SELECT * FROM rtot"))
        assert rows == [(1, 100, 10)], f"un-multiplied aggregates expected, got {rows}"
    finally:
        client.drop_schema(sn)


@_NEEDS_MULTI
def test_a_cluster_by_stream_routes_by_its_prefix(client):
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE cs (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, v BIGINT NOT NULL, "
            "PRIMARY KEY (a, b)) WITH (stream = true) CLUSTER BY a",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW cv AS SELECT a, SUM(v) AS total FROM cs GROUP BY a", schema_name=sn)
        rows = [(a, b, a * 100 + b) for a in range(1, 11) for b in range(1, 4)]
        _push_events(client, sn, "cs", rows)
        expected = {}
        for a, _b, v in rows:
            expected[a] = expected.get(a, 0) + v
        got = sorted((r["a"], r["total"]) for r in _rows(client, sn, "SELECT * FROM cv"))
        assert got == sorted(expected.items())
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Append-only, transactions, and the reply LSN
# ---------------------------------------------------------------------------


def test_a_non_positive_weight_is_rejected(client):
    """A stream is append-only: nothing clamps its weights the way
    `enforce_unique_pk` clamps a base table's, so one negative accumulated weight in
    a derived relation would fabricate rows through `positive_part`, destroy a linear
    reduce's accumulator, and break `LIMIT` — all silently. `delete` builds a
    retraction batch and is the routine way to reach the rejection."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        tid, schema = client.resolve_table(sn, "s")

        for weight in (0, -1):
            b = gnitz.ZSetBatch(schema)
            b.append(id=1, kind=2, amount=3, _weight=weight)
            with pytest.raises(Exception, match="append-only"):
                client.push(tid, b)

        with pytest.raises(Exception, match="append-only"):
            client.delete(tid, schema, [1])

        # A positive weight above 1 is legal — bag multiplicity, the same shape
        # `UNION ALL` already produces.
        b = gnitz.ZSetBatch(schema)
        b.append(id=1, kind=2, amount=3, _weight=5)
        client.push(tid, b)
    finally:
        client.drop_schema(sn)


def test_a_stream_write_inside_a_transaction_is_rejected_atomically(client):
    """A transaction promises all-or-nothing recovery and stream data is never
    recovered. `push_txn_body` returns on the first offending family before any
    family is written, so the transaction's other writes are rejected with it rather
    than lost after a partial commit."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        cols = [
            gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("kind", gnitz.TypeCode.I64),
            gnitz.ColumnDef("amount", gnitz.TypeCode.I64),
        ]
        t_tid = client.create_table(sn, "t", cols)
        t_schema = gnitz.Schema(cols)
        _stream(client, sn)
        s_tid, s_schema = client.resolve_table(sn, "s")

        tb = gnitz.ZSetBatch(t_schema)
        tb.append(id=1, kind=1, amount=1, _weight=1)
        sb = gnitz.ZSetBatch(s_schema)
        sb.append(id=2, kind=2, amount=2, _weight=1)

        with pytest.raises(Exception, match="stream"):
            with client.transaction() as txn:
                txn.push(t_tid, tb)
                txn.push(s_tid, sb)

        assert list(client.scan(t_tid)) == [], "the other family must be rejected with it"
    finally:
        client.drop_schema(sn)


def test_a_stream_push_does_not_advance_the_clients_occ_basis(client):
    """A stream push replies LSN `0`, so `track_lsn`'s `max` leaves `last_seen_lsn`
    untouched and the documented "always <= published()" invariant holds. Replying the
    reserved zone LSN instead would hand out a basis naming a zone that was never
    fsynced, and the base-table RMW below would then pass its precondition against it.
    Keyed on the target rather than on whether the batch opened a zone, so a stream
    push the committer coalesces with a base-table push still answers `0`."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        cols = [
            gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("amount", gnitz.TypeCode.I64),
        ]
        t_tid = client.create_table(sn, "t", cols)
        t_schema = gnitz.Schema(cols)
        _stream(client, sn)
        s_tid, s_schema = client.resolve_table(sn, "s")

        before = client.last_seen_lsn
        sb = gnitz.ZSetBatch(s_schema)
        sb.append(id=1, kind=1, amount=1, _weight=1)
        assert client.push(s_tid, sb) == 0, "a stream push is not durable"
        assert client.last_seen_lsn == before

        tb = gnitz.ZSetBatch(t_schema)
        tb.append(id=1, amount=10, _weight=1)
        assert client.push(t_tid, tb) > 0
        # An RMW statement cites `last_seen_lsn` as its basis; a basis above
        # `published()` would come back as a TxnConflict instead of committing.
        client.execute_sql("UPDATE t SET amount = 11 WHERE id = 1", schema_name=sn)
        assert sorted((r.id, r.amount) for r in client.scan(t_tid)) == [(1, 11)]
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Every remaining rejection names the stream
# ---------------------------------------------------------------------------


def test_the_ddl_and_dml_rejections_name_the_stream(client):
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql(
            "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL)",
            schema_name=sn,
        )
        for sql in (
            "UPDATE s SET amount = 1 WHERE id = 1",
            "DELETE FROM s WHERE id = 1",
            "CREATE INDEX s_kind ON s (kind)",
            "CREATE UNIQUE INDEX s_kind_u ON s (kind)",
            "ALTER TABLE s ADD CONSTRAINT s_u UNIQUE (kind)",
            # An inline UNIQUE is refused by the planner's stream rule, which names
            # the column; the engine's index owner check is the backstop, and
            # rejects the whole CREATE bundle.
            "CREATE TABLE su (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, k BIGINT NOT NULL UNIQUE) "
            "WITH (stream = true)",
            "ALTER TABLE s ADD COLUMN extra BIGINT",
            "ALTER TABLE s DROP COLUMN kind",
            "ALTER TABLE s RENAME COLUMN kind TO k",
            "CREATE VIEW b WITH (capacity = '4 MB') AS SELECT id, amount FROM s",
        ):
            with pytest.raises(Exception, match="stream"):
                client.execute_sql(sql, schema_name=sn)

        # A FOREIGN KEY to a stream, and one from a stream.
        with pytest.raises(Exception, match="stream"):
            client.execute_sql(
                "CREATE TABLE child (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "ref BIGINT UNSIGNED NOT NULL REFERENCES s(id))",
                schema_name=sn,
            )
        with pytest.raises(Exception, match="stream"):
            client.execute_sql(
                "CREATE TABLE fs (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "ref BIGINT UNSIGNED NOT NULL REFERENCES t(id)) WITH (stream = true)",
                schema_name=sn,
            )
        # SERIAL draws from a durable sequence, making every push a catalog write.
        with pytest.raises(Exception, match="stream"):
            client.execute_sql(
                "CREATE TABLE ss (id BIGSERIAL PRIMARY KEY, v BIGINT NOT NULL) WITH (stream = true)",
                schema_name=sn,
            )
    finally:
        client.drop_schema(sn)


def test_rename_to_stays_available_on_a_stream(client):
    """`RENAME TO` is a separate, kind-agnostic path that touches only metadata."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _stream(client, sn)
        client.execute_sql("CREATE VIEW hot AS SELECT id, amount FROM s", schema_name=sn)
        client.execute_sql("ALTER TABLE s RENAME TO events", schema_name=sn)
        _push_events(client, sn, "events", [(1, 2, 30)])
        assert sorted((r["id"], r["amount"]) for r in _rows(client, sn, "SELECT * FROM hot")) == [(1, 30)]
    finally:
        client.drop_schema(sn)


def test_options_form_cannot_smuggle_the_stream_flag(client):
    """A non-`WITH` option form carries keys nothing reads, so accepting it would
    silently produce an ordinary durable table with the opposite INSERT semantics --
    and, on CREATE VIEW, an unbounded view. Rejected by form, for both."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(Exception, match="OPTIONS"):
            client.execute_sql(
                "CREATE TABLE o (id BIGINT UNSIGNED NOT NULL PRIMARY KEY) OPTIONS(stream = true)",
                schema_name=sn,
            )
        with pytest.raises(Exception, match="streem"):
            client.execute_sql(
                "CREATE TABLE o (id BIGINT UNSIGNED NOT NULL PRIMARY KEY) WITH (streem = true)",
                schema_name=sn,
            )
        # The same rule on CREATE VIEW, where the discarded key would have been a
        # capacity: an unbounded view where a bounded one was asked for.
        client.execute_sql("CREATE TABLE b (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)", schema_name=sn)
        with pytest.raises(Exception, match="OPTIONS"):
            client.execute_sql(
                "CREATE VIEW bv OPTIONS(capacity = '4 MB') AS SELECT id FROM b",
                schema_name=sn,
            )
    finally:
        client.drop_schema(sn)
