"""The consistent multi-relation scan (SCAN_MULTI / scan_many).

`scan_many([A, B, ...])` snapshots every relation at ONE server-side SAL cut and
streams the N reply trains in request order. The headline guarantee: an atomic
multi-table transaction (FLAG_PUSH_TXN) is either visible in every relation's
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
from _serverproc import HANG_TIMEOUT


def _kv(client, sn, name):
    """(pk U64 PK, val I64) table. Returns (tid, schema)."""
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
    ]
    return client.create_table(sn, name, cols), gnitz.Schema(cols)


def _batch(schema, values):
    b = gnitz.ZSetBatch(schema)
    for pk, val in values:
        b.append(pk=pk, val=val, _weight=1)
    return b


def test_scan_many_of_one_relation_is_a_scan(client, schema_name):
    """A one-relation scan_many is exactly a scan: same rows, same weights, same
    lsn."""
    sn = schema_name
    tid, sch = _kv(client, sn, "t")
    client.push(tid, _batch(sch, [(1, 10), (2, 20), (3, 30)]))

    single, multi = client.scan(tid), client.scan_many([tid])
    assert len(multi) == 1
    assert bag(multi[0]) == bag(single) == {(1, 10): 1, (2, 20): 1, (3, 30): 1}
    assert multi[0].lsn == single.lsn


def test_the_results_line_up_with_the_requested_tids(client, schema_name):
    """Request order is reply order, and an empty relation still occupies its
    slot. Such a relation contributes no worker frame at all — the master
    forwards only frames carrying rows or a schema block — so its train is
    delimited by its master-authored terminal alone."""
    sn = schema_name
    a, a_sch = _kv(client, sn, "a")
    empty, _ = _kv(client, sn, "empty")
    b, b_sch = _kv(client, sn, "b")
    client.push(a, _batch(a_sch, [(pk, pk * 10) for pk in range(20)]))
    client.push(b, _batch(b_sch, [(99, 990)]))

    want_a = {(pk, pk * 10): 1 for pk in range(20)}
    res = client.scan_many([a, empty, b])
    assert [bag(r) for r in res] == [want_a, {}, {(99, 990): 1}]
    # Reversed request order → reversed results.
    assert [bag(r) for r in client.scan_many([b, empty, a])] == \
        [{(99, 990): 1}, {}, want_a]


def test_a_base_and_a_view_snapshot_at_one_cut(client, schema_name):
    """One hash-partitioned base table and one aggregate view (single-row output,
    read via the replicated/unicast path) agree at the same cut."""
    sn = schema_name
    t, sch = _kv(client, sn, "t")
    client.push(t, _batch(sch, [(i, 1) for i in range(20)]))
    client.execute_sql("CREATE VIEW v AS SELECT COUNT(*) AS c FROM t", schema_name=sn)
    v, _ = client.resolve_table(sn, "v")

    res = client.scan_many([t, v])
    assert len(bag(res[0])) == 20
    assert bag(res[1]) == {(20,): 1}, "the aggregate view agrees with the base"


def test_a_base_read_is_fresh_whatever_its_views_are_doing(client, schema_name):
    """A base table's own read is fully fresh the moment a push ACKs, whatever
    its dependent views are doing — a pending tick can only change what a VIEW
    sees. Three GROUP BY views ride on `t` so the tick queue is non-empty at read
    time; both read verbs must still return every pushed row at weight 1, and the
    scan_many/scan lsn equality must hold."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)", schema_name=sn)
    for i in range(3):
        client.execute_sql(
            f"CREATE VIEW agg{i} AS SELECT g, SUM(v) AS s FROM t WHERE v > {i} GROUP BY g",
            schema_name=sn)
    tid = client.resolve_table(sn, "t")[0]
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i % 4}, {i})" for i in range(60)),
        schema_name=sn)

    want = {(i, i % 4, i): 1 for i in range(60)}
    single, multi = client.scan(tid), client.scan_many([tid])
    assert bag(single) == bag(multi[0]) == want
    assert multi[0].lsn == single.lsn


# ---------------------------------------------------------------------------
# Headline: torn-commit impossibility
# ---------------------------------------------------------------------------


def test_a_commit_is_never_observed_torn(server, schema_name):
    """A writer commits atomic {a: i->i, b: i->i} transactions in a tight loop; a
    reader hammers scan_many([a, b]). Every result set has a's bag == b's bag —
    an atomic commit is never observed torn, and no row is ever observed at a
    weight other than 1, which is what a repeated reply frame would produce. The
    reader must also observe several intermediate sizes, proving it genuinely
    interleaved with the writer (so the 'no tear' result is meaningful, not a
    quiescent artefact)."""
    N = 400
    sn = schema_name
    with gnitz.connect(server) as wc:
        a, a_sch = _kv(wc, sn, "a")
        b, b_sch = _kv(wc, sn, "b")

        stop = threading.Event()
        bad, sizes = [], set()

        def writer():
            try:
                for i in range(N):
                    with wc.transaction() as txn:
                        txn.push(a, _batch(a_sch, [(i, i)]))
                        txn.push(b, _batch(b_sch, [(i, i)]))
            finally:
                stop.set()

        def reader():
            with gnitz.connect(server) as rc:
                while not stop.is_set():
                    ra, rb = (bag(r) for r in rc.scan_many([a, b]))
                    sizes.add(len(ra))
                    if ra != rb or set(ra.values()) - {1}:
                        bad.append((ra != rb, sorted(set(ra.values()))))
                        return

        rt, wt = threading.Thread(target=reader), threading.Thread(target=writer)
        rt.start(), wt.start()
        wt.join(timeout=HANG_TIMEOUT)
        rt.join(timeout=HANG_TIMEOUT)

        assert not bad, f"torn or mis-weighted snapshot: {bad}"
        final = wc.scan_many([a, b])
        assert bag(final[0]) == bag(final[1]) == {(i, i): 1 for i in range(N)}
        # Evidence the reader interleaved with the writer (saw the table grow).
        assert len([s for s in sizes if 0 < s < N]) >= 2, \
            f"reader did not interleave; sizes={sorted(sizes)}"


def test_a_view_never_leads_its_base(server, schema_name):
    """`scan_many([t, v])` with v derived from t (insert-only). Quiescent → they
    agree; under concurrent inserts the only asserted invariant is that the view
    never leads the base — every view row's key is present in the base
    snapshot."""
    sn = schema_name
    with gnitz.connect(server) as wc:
        t, sch = _kv(wc, sn, "t")
        wc.execute_sql(
            "CREATE VIEW v AS SELECT pk, val FROM t WHERE val >= 0", schema_name=sn)
        v, _ = wc.resolve_table(sn, "v")

        wc.push(t, _batch(sch, [(i, i) for i in range(10)]))
        res = wc.scan_many([t, v])
        assert bag(res[0]) == bag(res[1]) == {(i, i): 1 for i in range(10)}

        stop = threading.Event()
        violations = []

        def writer():
            try:
                for i in range(10, 210):
                    wc.push(t, _batch(sch, [(i, i)]))
            finally:
                stop.set()

        def reader():
            with gnitz.connect(server) as rc:
                while not stop.is_set():
                    rt_, rv = (bag(r) for r in rc.scan_many([t, v]))
                    if not rv.keys() <= rt_.keys():
                        violations.append((len(rt_), len(rv)))
                        return

        wt, rt = threading.Thread(target=writer), threading.Thread(target=reader)
        rt.start(), wt.start()
        wt.join(timeout=HANG_TIMEOUT)
        rt.join(timeout=HANG_TIMEOUT)
        assert not violations, f"view led the base: {violations}"


# ---------------------------------------------------------------------------
# Schema negotiation, error paths, FIFO reply ordering
# ---------------------------------------------------------------------------


def test_a_cold_and_a_warm_schema_cache_decode_alike(server, schema_name):
    """A fresh connection's first scan_many is cold (schema block absorbed into
    the cache); the second is warm (schema served from cache). Both decode
    identically, and so does a partially-warm call — the per-relation
    schema-version stamping and the optional preliminary schema frame both work
    through the multi path."""
    sn = schema_name
    with gnitz.connect(server) as setup:
        a, a_sch = _kv(setup, sn, "a")
        b, b_sch = _kv(setup, sn, "b")
        setup.push(a, _batch(a_sch, [(1, 1)]))
        setup.push(b, _batch(b_sch, [(2, 2)]))

    want = [{(1, 1): 1}, {(2, 2): 1}]
    with gnitz.connect(server) as rc:
        assert [bag(r) for r in rc.scan_many([a, b])] == want   # cold
        assert [bag(r) for r in rc.scan_many([a, b])] == want   # warm
    with gnitz.connect(server) as rc2:
        rc2.scan(a)                                             # warm only a
        assert [bag(r) for r in rc2.scan_many([a, b])] == want


def test_every_refusal_leaves_the_server_serving(client, schema_name):
    sn = schema_name
    t, sch = _kv(client, sn, "t")
    client.push(t, _batch(sch, [(1, 1)]))

    for why, tids in [
        ("empty list", []),
        ("too many relations", [t + 1 + i for i in range(17)]),
        ("duplicate tid", [t, t]),
        # A fan-out read has no form for a system relation, and an unknown user
        # tid resolves to no table — both refused server-side.
        ("system tid", [gnitz.TABLE_TAB]),
        ("unknown tid", [gnitz.FIRST_USER_TABLE_ID + 987654]),
    ]:
        with pytest.raises(gnitz.GnitzError):
            client.scan_many(tids)
        assert bag(client.scan_many([t])[0]) == {(1, 1): 1}, f"unhealthy after {why}"


def test_a_chunked_train_does_not_let_its_siblings_jump_it(reply_frame_budget_server):
    """With a 16 KiB reply budget, `big` chunks into a multi-frame train per
    worker while the tiny siblings are one frame each. `scan_many([big, s...])`
    must stream in request order without wedging — the shape that deadlocks
    without FLAG_SCAN_FIFO_REPLY, where the immediate-emit fast path would jump
    the tiny relations ahead of big's queued chunks. Both orderings, plus a
    heap-bearing (TEXT) sibling whose own train chunks, must complete and be
    correct.

    A wedge is a hang, so the join timeout is the assertion that matters most
    here; the bags below are what rules out a silently reordered or truncated
    train once it does complete.
    """
    c = reply_frame_budget_server
    sn = "fifo"
    c.create_schema(sn)
    try:
        big, big_sch = _kv(c, sn, "big")
        # 4000 rows * 32 B/row wire ≈ 128 KiB total. Even split across 4 workers
        # (~32 KiB each) it exceeds the 16 KiB budget, so every worker's train is
        # genuinely multi-chunk — exercising the chunked-under-FIFO path — while
        # staying far under any per-ring in-flight concern.
        big_rows = [(i, i) for i in range(4000)]
        c.push(big, _batch(big_sch, big_rows))
        want_big = {(i, i): 1 for i in range(4000)}

        smalls = []
        for k in range(8):
            tid, sch = _kv(c, sn, f"s{k}")
            c.push(tid, _batch(sch, [(k, k * 10)]))
            smalls.append((tid, {(k, k * 10): 1}))

        ids = [t for t, _ in smalls]
        wants = [w for _, w in smalls]
        assert [bag(r) for r in c.scan_many([big] + ids)] == [want_big] + wants
        assert [bag(r) for r in c.scan_many(ids + [big])] == wants + [want_big]

        # A blob-bearing sibling: a TEXT dimension must FIFO behind big too, and
        # its own train chunks like any other. The values are past the 12-byte
        # inline threshold, so every row points into the batch's string heap and
        # each frame carries a heap compacted to its own rows: 400 * ~230 B is
        # ~23 KiB per worker against the 16 KiB budget.
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("s", gnitz.TypeCode.STRING)]
        dim = c.create_table(sn, "dim_text", cols)
        db = gnitz.ZSetBatch(gnitz.Schema(cols))
        names = [f"name-{i}-" + "z" * 200 for i in range(400)]
        for i, nm in enumerate(names):
            db.append(pk=i, s=nm, _weight=1)
        c.push(dim, db)
        assert [bag(r) for r in c.scan_many([big, dim])] == \
            [want_big, {(i, nm): 1 for i, nm in enumerate(names)}]
    finally:
        c.drop_schema(sn)
