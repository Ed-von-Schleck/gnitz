"""A SEEK of a view is as fresh as a SCAN of it (server-side read-your-writes).

A view is maintained by an auto-tick that runs the DAG over source-table pushes.
That tick is deadline-batched, and a single-row push stays below the row-coalesce
threshold, so it does not fire an immediate tick. Every read verb that answers
"what is current" therefore goes through one gate: `read_lock` asks
`read_is_fresh` whether the target's transitive source closure is at or below the
last completed tick, and drains when it is not. A point PK-seek of a view is that
same gate, so it cannot lag a scan of the same view — the primary materialized-view
access pattern (precompute a join or aggregate, then look up one key) reads its
own writes.

A base-table target is vacuously fresh — base state is applied before the push
ACKs — so it never drains; `test_base_table_seek_is_fresh_without_draining` is the
control that keeps the view cases meaningful.

Each `push`/`INSERT` returns only after its ACK, and the committer bumps the
pending-tick set before it ACKs, so the drain on the following seek covers every
commit the caller has observed — including another client's. The core tests loop
push→seek with no interposed scan, so a lost auto-tick surfaces as a missed row.
"""
import threading

import gnitz
from _serverproc import HANG_TIMEOUT, join_or_fail


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


def _push_one(client, tid, schema, pk, val):
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=pk, val=val)
    client.push(tid, batch)


def _assert_live_once(res, k, ctx):
    """The seek found key `k` exactly once, at weight 1 and carrying its payload.

    The weight is the point: a view runs no `enforce_unique_pk`, so a tick applied
    twice leaves the key present at weight 2 — a row count alone reads that as
    correct.
    """
    assert len(res) == 1, f"{ctx}: seek({k}) returned {len(res)} rows, expected 1"
    assert list(res.weights) == [1], f"{ctx}: seek({k}) weight {list(res.weights)}"
    assert res.first().val == k * 10, f"{ctx}: seek({k}) payload {res.first().val}"


# ── core: read-your-writes through a raw view SEEK ───────────────────────────

def test_view_seek_reflects_just_pushed_row(client, schema_name):
    """Push row k, then IMMEDIATELY seek v by pk=k with no interposed scan. The
    just-pushed row's effect on v must be visible on every iteration; a
    deadline-batched tick that the seek did not wait for loses this race."""
    tid, t_schema, vid = _make_t_and_v(client, schema_name)
    for k in range(1, 51):
        _push_one(client, tid, t_schema, pk=k, val=k * 10)
        _assert_live_once(client.seek(vid, pk=k), k, "raw seek")


# ── SQL surface: SELECT ... WHERE <pk> = k dispatches as a point SEEK ─────────

def test_view_sql_pk_select_reflects_just_inserted_row(client, schema_name):
    """`SELECT * FROM v WHERE pk = k` fully binds the PK, so it is dispatched as
    a point SEEK, not a scan. It must reflect the row INSERTed on the line
    above."""
    _make_t_and_v(client, schema_name)
    for k in range(1, 31):
        client.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=schema_name)
        res = client.execute_sql(f"SELECT * FROM v WHERE pk = {k}", schema_name=schema_name)
        assert res[0]["type"] == "Rows"
        rows = list(res[0]["rows"])
        assert len(rows) == 1, f"SELECT ... WHERE pk={k} missed the just-inserted row"
        assert rows[0].val == k * 10 and rows[0].weight == 1


# ── a single drain refreshes a chain: view over a view ───────────────────────

def test_view_over_view_seek_reads_your_writes(client, schema_name):
    """A view over a view: seeking the TOP view after a push sees the effect,
    confirming one drain cascades the tick transitively down the chain."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val >= 0", schema_name=schema_name)
    client.execute_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val >= 0", schema_name=schema_name)
    tid, t_schema = client.resolve_table(schema_name, "t")
    v2id, _ = client.resolve_table(schema_name, "v2")
    for k in range(1, 31):
        _push_one(client, tid, t_schema, pk=k, val=k * 10)
        _assert_live_once(client.seek(v2id, pk=k), k, "transitive tick cascade")


# ── the control: a base-table seek is fresh without draining ─────────────────

def test_base_table_seek_is_fresh_without_draining(client, schema_name):
    """`read_is_fresh` reports any non-view target fresh, so a base-table seek
    takes no drain — and still returns the just-pushed row, because base state is
    applied before the push ACKs. This is what makes the view cases above a claim
    about the drain rather than about pushes being visible at all."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    tid, t_schema = client.resolve_table(schema_name, "t")
    for k in range(1, 31):
        _push_one(client, tid, t_schema, pk=k, val=k * 10)
        _assert_live_once(client.seek(tid, pk=k), k, "base table")


# ── empty / never-ticked view: seek matches scan (empty, no error) ────────────

def test_empty_view_seek_matches_scan(client, schema_name):
    """A view that was never pushed to: a seek returns empty — the same result a
    scan of it returns — with no error. Exercises the watermark early-out /
    empty-drain path on a cold view."""
    _, _, vid = _make_t_and_v(client, schema_name)
    assert len(client.seek(vid, pk=1)) == 0
    assert len(client.scan(vid)) == 0


# ── cross-client causality: B's seek sees A's ACKed write ─────────────────────

def test_cross_client_seek_sees_other_clients_write(client, server, schema_name):
    """Client A pushes and observes the ACK (push returns); then client B seeks
    the view. B must see A's write: the pending-tick set and watermark are shared
    master state, so B's drain covers A's un-ticked tid. Program order across the
    two connections is the out-of-band signal from A to B."""
    tid, _, vid = _make_t_and_v(client, schema_name)
    with gnitz.connect(server) as a, gnitz.connect(server) as b:
        _, a_schema = a.resolve_table(schema_name, "t")
        for k in range(1, 31):
            _push_one(a, tid, a_schema, pk=k, val=k * 10)
            _assert_live_once(b.seek(vid, pk=k), k, "client B")


# ── read-your-writes holds under a concurrent tick storm (slow-path loop) ─────

def test_view_seek_read_your_writes_under_tick_storm(client, server, schema_name):
    """A background client pushes a disjoint key range in step with the main
    client's own push→seek loop, so the pending set is non-empty across the whole
    loop and the seek drain keeps taking its slow path. Each main seek must still
    reflect its own just-ACKed push, and every main key stays live exactly once.

    The two loops are released together each iteration rather than the storm
    running free: an unsynchronised storm finishes in the first fifth of the main
    loop and leaves the rest running against an empty pending set.
    """
    tid, _, vid = _make_t_and_v(client, schema_name)
    iterations = 60
    sync = threading.Barrier(2, timeout=HANG_TIMEOUT)
    errors = []

    def storm():
        try:
            with gnitz.connect(server) as c:
                _, sch = c.resolve_table(schema_name, "t")
                for j in range(iterations):
                    sync.wait()
                    _push_one(c, tid, sch, pk=1_000_000 + j, val=j)
        except Exception as e:  # noqa: BLE001
            errors.append(e)
            sync.abort()

    storm_t = threading.Thread(target=storm)
    storm_t.start()
    try:
        with gnitz.connect(server) as m:
            _, m_schema = m.resolve_table(schema_name, "t")
            for k in range(1, iterations + 1):
                sync.wait()
                _push_one(m, tid, m_schema, pk=k, val=k * 10)
                _assert_live_once(m.seek(vid, pk=k), k, "under tick storm")
    except threading.BrokenBarrierError:
        pass  # the storm failed; `errors` below is the real report
    finally:
        join_or_fail("storm thread hung", storm_t)
    assert not errors, f"storm client errored: {errors}"

    # Weight conservation: every main-thread key is live exactly once, and the
    # storm's own keys are neither lost nor duplicated.
    res = client.scan(vid)
    assert len(res) == 2 * iterations, f"view holds {len(res)} rows"
    live = {r.pk: (r.val, r.weight) for r in res}
    for k in range(1, iterations + 1):
        assert live.get(k) == (k * 10, 1), f"key {k} lost or duplicated in the view"


# ── no deadlock: the lock-release-drain-reacquire path vs. a live DDL writer ───

def test_view_seek_no_deadlock_under_concurrent_ddl(client, server, schema_name):
    """A view seek releases the catalog read lock to drain, then re-acquires it.
    Run seeks (each preceded by a push, so each drains) in a loop while another
    client churns CREATE VIEW / DROP VIEW on an unrelated relation. A drain that
    ran under the read lock would deadlock the writer-preferring lock against the
    DDL writer and the tick loop, and this test would hang."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE val >= 0", schema_name=schema_name)
    tid, t_schema = client.resolve_table(schema_name, "t")
    vid, _ = client.resolve_table(schema_name, "v")

    stop = threading.Event()
    errors = []

    def ddl_churn():
        try:
            with gnitz.connect(server) as c:
                # Capped as well as signalled: an uncapped loop makes the DDL
                # volume a function of machine speed, so what the run covered
                # would not be reproducible.
                for i in range(200):
                    if stop.is_set():
                        return
                    c.execute_sql(
                        f"CREATE VIEW dv{i} AS SELECT * FROM t2 WHERE val >= 0",
                        schema_name=schema_name,
                    )
                    c.execute_sql(f"DROP VIEW dv{i}", schema_name=schema_name)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    churn = threading.Thread(target=ddl_churn)
    churn.start()
    try:
        with gnitz.connect(server) as m:
            for k in range(1, 51):
                _push_one(m, tid, t_schema, pk=k, val=k * 10)
                _assert_live_once(m.seek(vid, pk=k), k, "under concurrent DDL")
    finally:
        stop.set()
        join_or_fail("DDL churn thread hung", churn)
    assert not errors, f"DDL churn client errored: {errors}"
