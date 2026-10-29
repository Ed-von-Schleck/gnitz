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

import gnitz
import pytest
from _read import bag
from _serverproc import TINY_REPLY_FRAMES, across
from _schemas import KV, kv


def _kvs(*tids):
    """`tids`, each paired with the `KV` schema its rows are read in."""
    return [(t, KV) for t in tids]



def test_the_results_line_up_with_the_requested_tids(client):
    """Request order is reply order, and an empty relation still occupies its
    slot. Such a relation contributes no worker frame at all — the master
    forwards only frames carrying rows — so its train is delimited by its
    master-authored terminal alone. However many relations it names, it is one
    request."""
    a, empty, b = (client.create_table(name, KV) for name in ("a", "empty", "b"))
    client.push(a, kv(*((pk, pk * 10) for pk in range(20))))
    client.push(b, kv((99, 990)))

    want_a = {(pk, pk * 10): 1 for pk in range(20)}
    before = client.requests_sent
    res = client.scan_many(_kvs(a, empty, b))
    assert client.requests_sent == before + 1, "a scan_many is one request"
    assert [bag(r) for r in res] == [want_a, {}, {(99, 990): 1}]
    # Reversed request order → reversed results.
    assert [bag(r) for r in client.scan_many(_kvs(b, empty, a))] == \
        [{(99, 990): 1}, {}, want_a]


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
# Headline: one cut, under a concurrent writer
# ---------------------------------------------------------------------------


def test_one_cut_never_tears_a_commit_nor_lets_a_view_lead(client, peer):
    """A writer commits atomic {a: i->i, b: i->i} transactions in a tight loop
    while `scan_many([a, b, v, n])` reads at one cut, `v` a projection of `a` and
    `n` its COUNT(*), a single-row view read off one worker. Every result has a's
    bag == b's at weight 1 — a torn commit or a repeated reply frame would
    differ — and neither view ever leads `a`. Quiescent, all four agree. The
    reader must observe several intermediate sizes, or it never interleaved."""
    N = 400
    a, b = client.create_table("a", KV), client.create_table("b", KV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, val FROM a WHERE val >= 0; "
        "CREATE VIEW n AS SELECT COUNT(*) AS c FROM a")
    rels = [(a, KV), (b, KV), client.resolve_table("v"), client.resolve_table("n")]
    rc = peer()
    sizes = set()

    def write():
        for i in range(N):
            with client.transaction() as txn:
                txn.push(a, kv((i, i)))
                txn.push(b, kv((i, i)))

    def read(_):
        ra, rb, rv, rn = (bag(r) for r in rc.scan_many(rels))
        sizes.add(len(ra))
        assert ra == rb and set(ra.values()) <= {1}, "torn or mis-weighted snapshot"
        assert rv.items() <= ra.items(), f"the view led its base: {len(ra)} < {len(rv)}"
        ((count,),) = rn
        assert rn == {(count,): 1} and count <= len(ra), f"the count led its base: {rn}"

    across(write, read)
    whole = {(i, i): 1 for i in range(N)}
    assert [bag(r) for r in client.scan_many(rels)] == [whole, whole, whole, {(N,): 1}]
    assert len([s for s in sizes if 0 < s < N]) >= 2, \
        f"reader did not interleave; sizes={sorted(sizes)}"


# ---------------------------------------------------------------------------
# Error paths, FIFO reply ordering
# ---------------------------------------------------------------------------


def test_every_refusal_leaves_the_server_serving(client):
    t = client.create_table("t", KV)
    client.push(t, kv((1, 1)))

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
    request answers both, each as its own scan does, and a relation named twice
    at each of its positions."""
    t = client.create_table("t", KV)
    client.push(t, kv((1, 1)))
    tables = client.resolve_table("_system.tables")
    first, family, last = client.scan_many([(t, KV), tables, (t, KV)])
    assert bag(first) == bag(last) == {(1, 1): 1}
    assert bag(family) == bag(client.scan(*tables))


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
    with gnitz.connect(own_server.start(extra_env=TINY_REPLY_FRAMES).target) as c:
        big = c.create_table("big", KV)
        # 4000 rows * 32 B/row wire ≈ 128 KiB total. Even split across 4 workers
        # (~32 KiB each) it exceeds the 16 KiB budget, so every worker's train is
        # genuinely multi-chunk.
        c.push(big, kv(*((i, i) for i in range(4000))))
        want_big = {(i, i): 1 for i in range(4000)}

        ids = [c.create_table(f"s{k}", KV) for k in range(8)]
        for k, tid in enumerate(ids):
            c.push(tid, kv((k, k * 10)))
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
