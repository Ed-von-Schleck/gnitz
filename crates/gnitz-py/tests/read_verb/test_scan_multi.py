"""The consistent multi-relation scan (SCAN_MULTI / scan_many).

`scan_many([(A, schema_a), (B, schema_b), ...])` snapshots every relation at
ONE server-side SAL cut and streams the N reply trains in request order. The headline guarantee: an atomic
multi-table transaction (PUSH_TXN) is either visible in every relation's
result or in none — never torn across the set. This is the read-side completion
of the atomic multi-table write story.

Results are compared as weighted bags throughout: a mis-stitched or repeated
reply frame is a doubled weight, which a row-set comparison reads as correct.

Run with GNITZ_WORKERS=4 — the one-cut fan-out is a distributed path.
"""

import threading

import gnitz
import pytest
from _read import bag
from _serverproc import TINY_REPLY_FRAMES, join_or_fail, spawn
from _schemas import KV


def _kvs(*tids):
    """`tids`, each paired with the `KV` schema its rows are read in."""
    return [(t, KV) for t in tids]


def _batch(values):
    return gnitz.ZSetBatch(KV).extend({"pk": pk, "val": val} for pk, val in values)


def test_the_results_line_up_with_the_requested_tids(client):
    """Request order is reply order, and an empty relation still occupies its
    slot. Such a relation contributes no worker frame at all — the master
    forwards only frames carrying rows — so its train is delimited by its
    master-authored terminal alone. However many relations it names, it is one
    request."""
    a, empty, b = (client.create_table(name, KV) for name in ("a", "empty", "b"))
    client.push(a, _batch([(pk, pk * 10) for pk in range(20)]))
    client.push(b, _batch([(99, 990)]))

    want_a = {(pk, pk * 10): 1 for pk in range(20)}
    before = client.requests_sent
    res = client.scan_many(_kvs(a, empty, b))
    assert client.requests_sent == before + 1, "a scan_many is one request"
    assert [bag(r) for r in res] == [want_a, {}, {(99, 990): 1}]
    # Reversed request order → reversed results.
    assert [bag(r) for r in client.scan_many(_kvs(b, empty, a))] == \
        [{(99, 990): 1}, {}, want_a]


def test_a_base_and_a_view_snapshot_at_one_cut(client):
    """One hash-partitioned base table and one aggregate view (single-row output,
    read via the replicated/unicast path) agree at the same cut."""
    t = client.create_table("t", KV)
    client.push(t, _batch([(i, 1) for i in range(20)]))
    client.execute_sql("CREATE VIEW v AS SELECT COUNT(*) AS c FROM t")
    v = client.resolve_table("v")

    assert [bag(r) for r in client.scan_many([(t, KV), v])] == \
        [{(i, 1): 1 for i in range(20)}, {(20,): 1}]


def test_a_base_read_is_fresh_whatever_its_views_are_doing(client):
    """A base table's own read is fully fresh the moment a push ACKs, whatever
    its dependent views are doing — a pending tick can only change what a VIEW
    sees. Three GROUP BY views ride on `t` so the tick queue is non-empty at read
    time; both read verbs must still return every pushed row at weight 1, and a
    one-relation scan_many is exactly a scan, lsn included."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)")
    for i in range(3):
        client.execute_sql(
            f"CREATE VIEW agg{i} AS SELECT g, SUM(v) AS s FROM t WHERE v > {i} GROUP BY g")
    tid, schema = client.resolve_table("t")
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i % 4}, {i})" for i in range(60)))

    want = {(i, i % 4, i): 1 for i in range(60)}
    single, multi = client.scan(tid, schema), client.scan_many([(tid, schema)])
    assert len(multi) == 1
    assert bag(single) == bag(multi[0]) == want
    assert multi[0].lsn == single.lsn


# ---------------------------------------------------------------------------
# Headline: torn-commit impossibility
# ---------------------------------------------------------------------------


def test_a_commit_is_never_observed_torn(client, server):
    """A writer commits atomic {a: i->i, b: i->i} transactions in a tight loop; a
    reader hammers scan_many([a, b]). Every result set has a's bag == b's bag —
    an atomic commit is never observed torn, and no row is ever observed at a
    weight other than 1, which is what a repeated reply frame would produce. The
    reader must also observe several intermediate sizes, proving it genuinely
    interleaved with the writer (so the 'no tear' result is meaningful, not a
    quiescent artefact)."""
    N = 400
    a, b = client.create_table("a", KV), client.create_table("b", KV)
    stop = threading.Event()
    sizes = set()

    def writer():
        try:
            for i in range(N):
                with client.transaction() as txn:
                    txn.push(a, _batch([(i, i)]))
                    txn.push(b, _batch([(i, i)]))
        finally:
            stop.set()

    def reader():
        with gnitz.connect(server) as rc:
            while not stop.is_set():
                ra, rb = (bag(r) for r in rc.scan_many(_kvs(a, b)))
                sizes.add(len(ra))
                assert ra == rb and set(ra.values()) <= {1}, "torn or mis-weighted snapshot"

    join_or_fail("the writer or reader hung", spawn(reader), spawn(writer))

    final = client.scan_many(_kvs(a, b))
    assert bag(final[0]) == bag(final[1]) == {(i, i): 1 for i in range(N)}
    assert len([s for s in sizes if 0 < s < N]) >= 2, \
        f"reader did not interleave; sizes={sorted(sizes)}"


def test_a_view_never_leads_its_base(client, server):
    """`scan_many([t, v])` with v derived from t (insert-only). Quiescent → they
    agree; under concurrent inserts the invariant is that the view never leads
    the base — every view row is in the base snapshot, at the same weight. The
    reader must observe several intermediate sizes, or it never interleaved."""
    N = 200
    t = client.create_table("t", KV)
    client.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t WHERE val >= 0")
    v = client.resolve_table("v")

    client.push(t, _batch([(i, i) for i in range(10)]))
    res = client.scan_many([(t, KV), v])
    assert bag(res[0]) == bag(res[1]) == {(i, i): 1 for i in range(10)}

    stop = threading.Event()
    sizes = set()

    def writer():
        try:
            for i in range(10, 10 + N):
                client.push(t, _batch([(i, i)]))
        finally:
            stop.set()

    def reader():
        with gnitz.connect(server) as rc:
            while not stop.is_set():
                rt, rv = (bag(r) for r in rc.scan_many([(t, KV), v]))
                sizes.add(len(rt))
                assert rv.items() <= rt.items(), f"view led the base: {len(rt)} < {len(rv)}"

    join_or_fail("the writer or reader hung", spawn(reader), spawn(writer))
    assert len([s for s in sizes if 10 < s < 10 + N]) >= 2, \
        f"reader did not interleave; sizes={sorted(sizes)}"


# ---------------------------------------------------------------------------
# Error paths, FIFO reply ordering
# ---------------------------------------------------------------------------


def test_every_refusal_leaves_the_server_serving(client):
    t = client.create_table("t", KV)
    client.push(t, _batch([(1, 1)]))

    # A repeated tid is legal, so the count alone is what this one crosses.
    with pytest.raises(gnitz.GnitzRefusedError, match="too many items"):
        client.scan_many(_kvs(t) * 1000)
    assert bag(client.scan_many(_kvs(t))[0]) == {(1, 1): 1}, "unhealthy after too many items"

    # An unknown user tid resolves to no table, and the refusal names it.
    missing = gnitz.FIRST_USER_TABLE_ID + 987654
    with pytest.raises(gnitz.GnitzNotFoundError, match=str(missing)):
        client.scan_many(_kvs(t, missing))
    assert bag(client.scan_many(_kvs(t))[0]) == {(1, 1): 1}, "unhealthy after unknown tid"


def test_a_system_family_is_read_beside_user_relations(client):
    """A family is read off the master's own copy, a table off its workers: one
    request answers both, each as its own scan does."""
    t = client.create_table("t", KV)
    client.push(t, _batch([(1, 1)]))
    tables = gnitz.sys_schema(gnitz.TABLE_TAB)
    first, family, last = client.scan_many([(t, KV), (gnitz.TABLE_TAB, tables), (t, KV)])
    assert bag(first) == bag(last) == {(1, 1): 1}
    assert bag(family) == bag(client.scan(gnitz.TABLE_TAB, tables))


def test_a_repeated_tid_is_answered_at_each_position(client):
    t = client.create_table("t", KV)
    client.push(t, _batch([(1, 1), (2, 2)]))
    assert [bag(r) for r in client.scan_many(_kvs(t, t))] == [{(1, 1): 1, (2, 2): 1}] * 2


def test_a_chunked_train_does_not_let_its_siblings_jump_it(own_server):
    """With a 16 KiB reply budget, `big` chunks into a multi-frame train per
    worker while the tiny siblings are one frame each. `scan_many` of `[big, s...]`
    must stream in request order without wedging — the shape that deadlocks
    without the `scan_fifo_reply` flag, where the immediate-emit fast path would jump
    the tiny relations ahead of big's queued chunks. Both orderings, plus a
    heap-bearing (TEXT) sibling whose own train chunks, must complete and be
    correct.

    A wedge is a hang, so the join timeout is the assertion that matters most
    here; the bags below are what rules out a silently reordered or truncated
    train once it does complete.
    """
    c = gnitz.connect(own_server.start(extra_env=TINY_REPLY_FRAMES).target)
    big = c.create_table("big", KV)
    # 4000 rows * 32 B/row wire ≈ 128 KiB total. Even split across 4 workers
    # (~32 KiB each) it exceeds the 16 KiB budget, so every worker's train is
    # genuinely multi-chunk.
    c.push(big, _batch([(i, i) for i in range(4000)]))
    want_big = {(i, i): 1 for i in range(4000)}

    ids = [c.create_table(f"s{k}", KV) for k in range(8)]
    for k, tid in enumerate(ids):
        c.push(tid, _batch([(k, k * 10)]))
    wants = [{(k, k * 10): 1} for k in range(8)]
    assert [bag(r) for r in c.scan_many(_kvs(big, *ids))] == [want_big] + wants
    assert [bag(r) for r in c.scan_many(_kvs(*ids, big))] == wants + [want_big]

    # A blob-bearing sibling: a TEXT dimension must FIFO behind big too, and
    # its own train chunks like any other. The values are past the 12-byte
    # inline threshold, so every row points into the batch's string heap and
    # each frame carries a heap compacted to its own rows: 400 * ~230 B is
    # ~23 KiB per worker against the 16 KiB budget.
    dim_schema = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                               gnitz.ColumnDef("s", gnitz.TypeCode.STRING)], [0])
    dim = c.create_table("dim_text", dim_schema)
    names = [f"name-{i}-" + "z" * 200 for i in range(400)]
    c.push(dim, gnitz.ZSetBatch(dim_schema).extend(
        {"pk": i, "s": nm} for i, nm in enumerate(names)))
    assert [bag(r) for r in c.scan_many([(big, KV), (dim, dim_schema)])] == \
        [want_big, {(i, nm): 1 for i, nm in enumerate(names)}]
