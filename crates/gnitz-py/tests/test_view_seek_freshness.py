"""A SEEK of a view is as fresh as a SCAN of it (server-side read-your-writes).

A view is maintained by an auto-tick that runs the DAG over source-table
pushes. That tick is deadline-batched, and a single-row push stays below the
row-coalesce threshold, so it does not fire an immediate tick. A SCAN drains the
pending-tick set before reading; a SEEK used to fan straight out. So a point
PK-seek of a view derived from a just-ACKed push could miss the row while a scan
of the same view saw it — a silent read-your-writes gap on the primary
materialized-view access pattern (precompute a join/aggregate, then look up one
key). `handle_seek` now drains pending ticks for view targets, exactly as
`handle_scan` does; base-table seeks are unchanged (base state is fresh at
push-apply time, so they never drain).

Each `push`/`INSERT` returns only after its ACK, and the committer bumps the
pending-tick set before it ACKs, so the drain on the following seek covers every
commit the caller has observed — including another client's. The core tests loop
push→seek with no interposed scan, so any lost auto-tick race surfaces as a
missed row (these fail before the drain-on-seek change).

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_view_seek_freshness.py -v --tb=short
"""
import random
import threading

import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _positive(rows):
    return [r for r in rows if r.weight > 0]


def _cleanup(client, sn, tables=("t",), views=("v",)):
    for name in views:
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in tables:
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _make_t_and_v(client, sn):
    """`t` and a keep-everything filter view `v` over it — a real maintained
    circuit (not a passthrough), whose live set equals `t`'s. Returns
    (tid, t_schema, vid). The schema comes from `resolve_table`, not a hand-built
    one: a BIGINT PK is stored signed, so a mis-typed batch would encode a
    different order-preserving key and never consolidate."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t WHERE val >= 0",
        schema_name=sn,
    )
    tid, t_schema = client.resolve_table(sn, "t")
    vid, _ = client.resolve_table(sn, "v")
    return tid, t_schema, vid


def _push_one(client, tid, schema, pk, val, weight=1):
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=pk, val=val, weight=weight)
    client.push(tid, batch)


# ── core: read-your-writes through a raw view SEEK ───────────────────────────

def test_view_seek_reflects_just_pushed_row(client):
    """Push row k, then IMMEDIATELY seek v by pk=k with no interposed scan. The
    just-pushed row's effect on v must be visible on every iteration; before the
    drain-on-seek change the deadline-batched tick loses this race."""
    sn = "vsf" + _uid()
    client.create_schema(sn)
    try:
        tid, t_schema, vid = _make_t_and_v(client, sn)
        for k in range(1, 51):
            _push_one(client, tid, t_schema, pk=k, val=k * 10)
            rows = _positive(client.seek(vid, pk=k))
            assert len(rows) == 1, f"seek(v, {k}) missed the just-pushed row (stale view)"
            assert rows[0].pk == k and rows[0].val == k * 10
    finally:
        _cleanup(client, sn)


# ── SQL surface: SELECT ... WHERE <pk> = k dispatches as a point SEEK ─────────

def test_view_sql_pk_select_reflects_just_inserted_row(client):
    """`SELECT * FROM v WHERE pk = k` fully binds the PK, so it is dispatched as
    a point SEEK, not a scan. It must reflect the row INSERTed on the line
    above."""
    sn = "vsf" + _uid()
    client.create_schema(sn)
    try:
        _make_t_and_v(client, sn)
        for k in range(1, 31):
            client.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=sn)
            res = client.execute_sql(f"SELECT * FROM v WHERE pk = {k}", schema_name=sn)
            assert res[0]["type"] == "Rows"
            rows = list(res[0]["rows"])
            assert len(rows) == 1, f"SELECT ... WHERE pk={k} missed the just-inserted row"
            assert rows[0].val == k * 10
    finally:
        _cleanup(client, sn)


# ── a single drain refreshes a chain: view over a view ───────────────────────

def test_view_over_view_seek_reads_your_writes(client):
    """A view over a view: seeking the TOP view after a push sees the effect,
    confirming one drain cascades the tick transitively down the chain."""
    sn = "vsf" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val >= 0", schema_name=sn)
        client.execute_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val >= 0", schema_name=sn)
        tid, t_schema = client.resolve_table(sn, "t")
        v2id, _ = client.resolve_table(sn, "v2")
        for k in range(1, 31):
            _push_one(client, tid, t_schema, pk=k, val=k * 10)
            rows = _positive(client.seek(v2id, pk=k))
            assert len(rows) == 1, f"seek(v2, {k}) missed row — transitive tick cascade"
            assert rows[0].val == k * 10
    finally:
        _cleanup(client, sn, views=["v2", "v1"])


# ── base-table seeks are unchanged (no drain, always fresh) ───────────────────

def test_base_table_seek_unchanged(client):
    """A base-table seek never drains (base state is fresh at push-apply time)
    and returns the just-pushed row exactly as before — no regression."""
    sn = "vsf" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, t_schema = client.resolve_table(sn, "t")
        for k in range(1, 31):
            _push_one(client, tid, t_schema, pk=k, val=k * 10)
            rows = _positive(client.seek(tid, pk=k))
            assert len(rows) == 1 and rows[0].val == k * 10
    finally:
        _cleanup(client, sn)


# ── empty / never-ticked view: seek matches scan (empty, no error) ────────────

def test_empty_view_seek_matches_scan(client):
    """A view that was never pushed to: a seek returns empty — the same result a
    scan of it returns — with no error. Exercises the watermark early-out /
    empty-drain path on a cold view."""
    sn = "vsf" + _uid()
    client.create_schema(sn)
    try:
        _, _, vid = _make_t_and_v(client, sn)
        assert _positive(client.seek(vid, pk=1)) == []
        assert _positive(client.scan(vid)) == []
    finally:
        _cleanup(client, sn)


# ── cross-client causality: B's seek sees A's ACKed write ─────────────────────

def test_cross_client_seek_sees_other_clients_write(server):
    """Client A pushes and observes the ACK (push returns); then client B seeks
    the view. B must see A's write: the pending-tick set and watermark are shared
    master state, so B's drain covers A's un-ticked tid. Program order across the
    two connections is the out-of-band signal from A to B."""
    sn = "vsf" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        tid, t_schema, vid = _make_t_and_v(setup, sn)
    try:
        with gnitz.connect(server) as a, gnitz.connect(server) as b:
            _, a_schema = a.resolve_table(sn, "t")
            for k in range(1, 31):
                _push_one(a, tid, a_schema, pk=k, val=k * 10)
                rows = _positive(b.seek(vid, pk=k))
                assert len(rows) == 1, f"client B seek(v, {k}) missed client A's ACKed push"
                assert rows[0].val == k * 10
    finally:
        with gnitz.connect(server) as c:
            _cleanup(c, sn)


# ── read-your-writes holds under a concurrent tick storm (slow-path loop) ─────

def test_view_seek_read_your_writes_under_tick_storm(server):
    """A background client hammers a disjoint key range while the main client
    interleaves push→seek on its own keys. Each main seek must still reflect its
    own just-ACKed push, and weights stay conserved. The storm keeps the pending
    set non-empty, so the seek drain iterates its slow path under sustained
    writes. The storm is bounded and self-terminates, so a seek that serializes
    behind it still returns."""
    sn = "vsf" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        tid, _, vid = _make_t_and_v(setup, sn)

    errors = []

    def storm():
        try:
            with gnitz.connect(server) as c:
                _, sch = c.resolve_table(sn, "t")
                for j in range(1, 401):
                    _push_one(c, tid, sch, pk=1_000_000 + j, val=j)
        except Exception as e:  # pragma: no cover - surfaced via assert below
            errors.append(e)

    storm_t = threading.Thread(target=storm)
    storm_t.start()
    try:
        with gnitz.connect(server) as m:
            _, m_schema = m.resolve_table(sn, "t")
            for k in range(1, 61):
                _push_one(m, tid, m_schema, pk=k, val=k * 10)
                rows = _positive(m.seek(vid, pk=k))
                assert len(rows) == 1, f"seek(v, {k}) stale under a concurrent tick storm"
                assert rows[0].val == k * 10
    finally:
        storm_t.join()
    assert not errors, f"storm client errored: {errors}"

    # Weight conservation: every main-thread key is live exactly once.
    with gnitz.connect(server) as c:
        live = {r.pk: r.val for r in _positive(c.scan(vid))}
        for k in range(1, 61):
            assert live.get(k) == k * 10, f"key {k} lost or duplicated in the view"
        _cleanup(c, sn)


# ── no deadlock: the lock-release-drain-reacquire path vs. a live DDL writer ───

def test_view_seek_no_deadlock_under_concurrent_ddl(server):
    """A view seek releases the catalog read lock to drain, then re-acquires it.
    Run seeks (each preceded by a push, so each drains) in a loop while another
    client churns CREATE VIEW / DROP VIEW on an unrelated relation. If the drain
    ever ran under the read lock it would deadlock the writer-preferring lock
    against the DDL writer and the tick loop; this test would then hang."""
    sn = "vsf" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        setup.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        setup.execute_sql(
            "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        setup.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE val >= 0", schema_name=sn)
        tid, t_schema = setup.resolve_table(sn, "t")
        vid, _ = setup.resolve_table(sn, "v")

    stop = threading.Event()
    errors = []

    def ddl_churn():
        try:
            with gnitz.connect(server) as c:
                i = 0
                while not stop.is_set():
                    i += 1
                    name = f"dv{i}"
                    c.execute_sql(
                        f"CREATE VIEW {name} AS SELECT * FROM t2 WHERE val >= 0",
                        schema_name=sn,
                    )
                    c.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception as e:  # pragma: no cover - surfaced via assert below
            errors.append(e)

    churn = threading.Thread(target=ddl_churn)
    churn.start()
    try:
        with gnitz.connect(server) as m:
            for k in range(1, 51):
                _push_one(m, tid, t_schema, pk=k, val=k * 10)
                rows = _positive(m.seek(vid, pk=k))
                assert len(rows) == 1 and rows[0].val == k * 10, \
                    f"view seek {k} stale or lost under concurrent DDL"
    finally:
        stop.set()
        churn.join()
    assert not errors, f"DDL churn client errored: {errors}"
    with gnitz.connect(server) as c:
        _cleanup(c, sn, tables=["t", "t2"], views=["v"])
