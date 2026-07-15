"""E2E tests for the consistent multi-relation scan (SCAN_MULTI / scan_many).

`scan_many([A, B, ...])` snapshots every relation at ONE server-side SAL cut and
streams the N reply trains in request order. The headline guarantee: an atomic
multi-table transaction (FLAG_PUSH_TXN) is either visible in every relation's
result or in none — never torn across the set. This is the read-side completion
of the atomic multi-table write story.

Run with GNITZ_WORKERS=4 — the one-cut fan-out is a distributed path.
"""

import random
import threading

import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _kv_table(client, sn, name):
    """(pk U64 PK, val I64) table. Returns (tid, schema)."""
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
    ]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, name, cols, unique_pk=True)
    return tid, schema


def _batch(schema, rows):
    b = gnitz.ZSetBatch(schema)
    for pk, val in rows:
        b.append(pk=pk, val=val, weight=1)
    return b


def _rows(result):
    """Sorted (pk, val) over the positive-weight rows of one scan result."""
    return sorted((r.pk, r.val) for r in result if r.weight > 0)


def _pks(result):
    return {r.pk for r in result if r.weight > 0}


# ---------------------------------------------------------------------------
# Equivalence: scan_many([t]) == scan(t)
# ---------------------------------------------------------------------------


def test_scan_many_single_equals_scan(client):
    """A one-relation scan_many is exactly a scan: same rows, same lsn."""
    sn = "sm" + _uid()
    client.create_schema(sn)
    try:
        tid, sch = _kv_table(client, sn, "t")
        client.push(tid, _batch(sch, [(1, 10), (2, 20), (3, 30)]))

        single = client.scan(tid)
        multi = client.scan_many([tid])
        assert len(multi) == 1
        assert _rows(multi[0]) == _rows(single)
        assert multi[0].lsn == single.lsn
    finally:
        client.drop_schema(sn)


def test_scan_many_two_tables_quiescent(client):
    """Two independent tables snapshot correctly in one call, in request order."""
    sn = "sm" + _uid()
    client.create_schema(sn)
    try:
        a, a_sch = _kv_table(client, sn, "a")
        b, b_sch = _kv_table(client, sn, "b")
        client.push(a, _batch(a_sch, [(1, 1), (2, 2)]))
        client.push(b, _batch(b_sch, [(9, 90)]))

        res = client.scan_many([a, b])
        assert _rows(res[0]) == [(1, 1), (2, 2)]
        assert _rows(res[1]) == [(9, 90)]

        # Reversed request order → reversed results.
        res = client.scan_many([b, a])
        assert _rows(res[0]) == [(9, 90)]
        assert _rows(res[1]) == [(1, 1), (2, 2)]
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Headline: torn-commit impossibility
# ---------------------------------------------------------------------------


def test_torn_commit_impossible(server):
    """A writer commits atomic {a: i->i, b: i->i} transactions in a tight loop; a
    reader hammers scan_many([a, b]). Every result set has a's rows == b's rows —
    an atomic commit is never observed torn. The reader must also observe several
    intermediate sizes, proving it genuinely interleaved with the writer (so the
    'no tear' result is meaningful, not a quiescent artefact)."""
    N = 400
    with gnitz.connect(server) as wc:
        sn = "sm" + _uid()
        wc.create_schema(sn)
        try:
            a, a_sch = _kv_table(wc, sn, "a")
            b, b_sch = _kv_table(wc, sn, "b")

            stop = threading.Event()
            torn = []
            sizes = set()

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
                        res = rc.scan_many([a, b])
                        ra, rb = _rows(res[0]), _rows(res[1])
                        sizes.add(len(ra))
                        if ra != rb:
                            torn.append((len(ra), len(rb)))
                            return

            rt = threading.Thread(target=reader)
            wt = threading.Thread(target=writer)
            rt.start()
            wt.start()
            wt.join()
            rt.join()

            assert not torn, f"torn commit observed (a vs b row counts): {torn}"
            # The final committed state is complete and consistent.
            final = wc.scan_many([a, b])
            assert _rows(final[0]) == [(i, i) for i in range(N)]
            assert _rows(final[0]) == _rows(final[1])
            # Evidence the reader interleaved with the writer (saw the table grow).
            intermediate = [s for s in sizes if 0 < s < N]
            assert len(intermediate) >= 2, f"reader did not interleave; sizes={sorted(sizes)}"
        finally:
            wc.drop_schema(sn)


def test_concurrent_scan_many_two_readers(server):
    """Two clients running scan_many concurrently both complete correctly — they
    serialize only at the one-cut submit, not the drain."""
    with gnitz.connect(server) as setup:
        sn = "sm" + _uid()
        setup.create_schema(sn)
        try:
            a, a_sch = _kv_table(setup, sn, "a")
            b, b_sch = _kv_table(setup, sn, "b")
            setup.push(a, _batch(a_sch, [(i, i) for i in range(50)]))
            setup.push(b, _batch(b_sch, [(i, i * 2) for i in range(50)]))

            results = {}
            errors = []

            def reader(key):
                try:
                    with gnitz.connect(server) as rc:
                        for _ in range(30):
                            res = rc.scan_many([a, b])
                            results[key] = (_rows(res[0]), _rows(res[1]))
                except Exception as e:  # noqa: BLE001
                    errors.append((key, repr(e)))

            t1 = threading.Thread(target=reader, args=("c1",))
            t2 = threading.Thread(target=reader, args=("c2",))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            assert not errors, errors
            exp_a = [(i, i) for i in range(50)]
            exp_b = [(i, i * 2) for i in range(50)]
            for key in ("c1", "c2"):
                assert results[key][0] == exp_a
                assert results[key][1] == exp_b
        finally:
            setup.drop_schema(sn)


# ---------------------------------------------------------------------------
# Mixed fan-out shapes + views
# ---------------------------------------------------------------------------


def test_mixed_shapes_base_and_view(client):
    """One hash-partitioned base table + one aggregate view (single-row output,
    read via the replicated/unicast path) snapshot consistently in one cut."""
    sn = "sm" + _uid()
    client.create_schema(sn)
    try:
        t, sch = _kv_table(client, sn, "t")
        client.push(t, _batch(sch, [(i, 1) for i in range(20)]))
        client.execute_sql("CREATE VIEW v AS SELECT COUNT(*) AS c FROM t", schema_name=sn)
        v, _ = client.resolve_table(sn, "v")

        res = client.scan_many([t, v])
        t_count = len(_rows(res[0]))
        v_count = next(r.c for r in res[1] if r.weight > 0)
        assert t_count == 20
        assert v_count == t_count, "aggregate view agrees with the base at the same cut"
    finally:
        client.drop_schema(sn)


def test_view_and_base_instant_consistency(server):
    """`scan_many([t, v])` with v derived from t (insert-only). Quiescent → they
    agree; under concurrent inserts the only asserted invariant is that the view
    never leads the base — every view row's key is present in the base snapshot."""
    with gnitz.connect(server) as wc:
        sn = "sm" + _uid()
        wc.create_schema(sn)
        try:
            t, sch = _kv_table(wc, sn, "t")
            wc.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t WHERE val >= 0", schema_name=sn)
            v, _ = wc.resolve_table(sn, "v")

            # Quiescent agreement.
            wc.push(t, _batch(sch, [(i, i) for i in range(10)]))
            res = wc.scan_many([t, v])
            assert _pks(res[0]) == _pks(res[1]) == set(range(10))

            # Under concurrent inserts: v ⊆ t in every snapshot (never leads).
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
                        res = rc.scan_many([t, v])
                        if not _pks(res[1]) <= _pks(res[0]):
                            violations.append((len(_pks(res[0])), len(_pks(res[1]))))
                            return

            wt = threading.Thread(target=writer)
            rt = threading.Thread(target=reader)
            rt.start()
            wt.start()
            wt.join()
            rt.join()
            assert not violations, f"view led the base (v not subset of t): {violations}"
        finally:
            wc.drop_schema(sn)


# ---------------------------------------------------------------------------
# Schema negotiation (cold → warm)
# ---------------------------------------------------------------------------


def test_schema_negotiation_cold_then_warm(server):
    """A fresh connection's first scan_many is cold (schema block absorbed into
    the cache); the second is warm (schema served from cache). Both decode
    correctly and identically — the per-relation schema-version stamping and the
    optional preliminary schema frame both work through the multi path."""
    with gnitz.connect(server) as setup:
        sn = "sm" + _uid()
        setup.create_schema(sn)
        try:
            a, a_sch = _kv_table(setup, sn, "a")
            b, b_sch = _kv_table(setup, sn, "b")
            setup.push(a, _batch(a_sch, [(1, 1)]))
            setup.push(b, _batch(b_sch, [(2, 2)]))

            with gnitz.connect(server) as rc:
                cold = rc.scan_many([a, b])  # schema cache empty → cold
                warm = rc.scan_many([a, b])  # schema cache warm
                assert _rows(cold[0]) == _rows(warm[0]) == [(1, 1)]
                assert _rows(cold[1]) == _rows(warm[1]) == [(2, 2)]
                # A partially-warm call (a cached, b not) still resolves both.
                with gnitz.connect(server) as rc2:
                    rc2.scan(a)  # warm only a's cache entry
                    mixed = rc2.scan_many([a, b])
                    assert _rows(mixed[0]) == [(1, 1)]
                    assert _rows(mixed[1]) == [(2, 2)]
        finally:
            setup.drop_schema(sn)


# ---------------------------------------------------------------------------
# Error paths — server stays healthy after each
# ---------------------------------------------------------------------------


def test_error_paths(client):
    sn = "sm" + _uid()
    client.create_schema(sn)
    try:
        t, sch = _kv_table(client, sn, "t")
        client.push(t, _batch(sch, [(1, 1)]))

        def _healthy():
            assert _rows(client.scan_many([t])[0]) == [(1, 1)]

        # Empty list (rejected client-side).
        with pytest.raises(gnitz.GnitzError):
            client.scan_many([])
        _healthy()

        # Too many relations: 17 distinct ids (rejected client-side by count).
        with pytest.raises(gnitz.GnitzError):
            client.scan_many([t + 1 + i for i in range(17)])
        _healthy()

        # Duplicate tid (rejected client-side).
        with pytest.raises(gnitz.GnitzError):
            client.scan_many([t, t])
        _healthy()

        # System tid (rejected server-side: not a user relation).
        with pytest.raises(gnitz.GnitzError):
            client.scan_many([gnitz.TABLE_TAB])
        _healthy()

        # Unknown user tid (rejected server-side: table not found).
        with pytest.raises(gnitz.GnitzError):
            client.scan_many([gnitz.FIRST_USER_TABLE_ID + 987654])
        _healthy()
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# FIFO reply ordering (the deadlock guard) — chunked big + tiny siblings
# ---------------------------------------------------------------------------


def _text_table(client, sn, name):
    """(pk U64 PK, s TEXT) table — non-wire-safe reply (blob column)."""
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("s", gnitz.TypeCode.STRING),
    ]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, name, cols, unique_pk=True)
    return tid, schema


def test_fifo_ordering_big_then_small(reply_frame_budget_server):
    """With a 16 KiB reply budget, `big` chunks into a multi-frame train per
    worker while the tiny siblings are one frame each. `scan_many([big, s...])`
    must stream in request order without wedging — the shape that deadlocks
    without FLAG_SCAN_FIFO_REPLY (the immediate-emit fast path would jump the
    tiny relations ahead of big's queued chunks). Both orderings, plus a
    non-wire-safe (TEXT) sibling, must complete and be correct."""
    c = reply_frame_budget_server
    sn = "sm" + _uid()
    c.create_schema(sn)
    try:
        big, big_sch = _kv_table(c, sn, "big")
        # 4000 rows * 32 B/row wire ≈ 128 KiB total. Even split across 4 workers
        # (~32 KiB each) it exceeds the 16 KiB budget, so every worker's train is
        # genuinely multi-chunk — exercising the chunked-under-FIFO path — while
        # staying far under any per-ring in-flight concern.
        big_rows = [(i, i) for i in range(4000)]
        c.push(big, _batch(big_sch, big_rows))

        smalls = []
        for k in range(8):
            tid, sch = _kv_table(c, sn, f"s{k}")
            c.push(tid, _batch(sch, [(k, k * 10)]))
            smalls.append((tid, [(k, k * 10)]))

        # big first, then the tiny siblings.
        res = c.scan_many([big] + [t for t, _ in smalls])
        assert _rows(res[0]) == big_rows
        for j, (_, exp) in enumerate(smalls):
            assert _rows(res[1 + j]) == exp

        # Reversed: tiny siblings first, big last.
        res = c.scan_many([t for t, _ in smalls] + [big])
        for j, (_, exp) in enumerate(smalls):
            assert _rows(res[j]) == exp
        assert _rows(res[-1]) == big_rows

        # Non-wire-safe sibling: a TEXT dimension must FIFO behind big too.
        dim, dim_sch = _text_table(c, sn, "dim_text")
        db = gnitz.ZSetBatch(dim_sch)
        for i in range(5):
            db.append(pk=i, s=f"name-{i}", weight=1)
        c.push(dim, db)
        res = c.scan_many([big, dim])
        assert _rows(res[0]) == big_rows
        assert sorted((r.pk, r.s) for r in res[1] if r.weight > 0) == [(i, f"name-{i}") for i in range(5)]
    finally:
        c.drop_schema(sn)
