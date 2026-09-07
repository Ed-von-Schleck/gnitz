"""Tests for gnitz.aio — async client with pipelining."""

import asyncio
import os
import pytest
import pytest_asyncio
import gnitz
from gnitz import aio
from _uid import uid as _uid


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def aconn(server):
    async with aio.connect(server) as conn:
        yield conn


@pytest.fixture
def sync(server):
    with gnitz.connect(server) as conn:
        yield conn


def _make_table(sync, schema_name, table_name, cols):
    sync.create_schema(schema_name)
    tid = sync.create_table(schema_name, table_name, cols)
    return tid


def _drop_table(sync, schema_name, table_name):
    sync.drop_table(schema_name, table_name)
    sync.drop_schema(schema_name)


PK_VAL_COLS = [
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
]


@pytest.fixture
def table(sync):
    """Create a disposable schema + table for async tests."""
    sn = "a" + _uid()
    tid = _make_table(sync, sn, "t", PK_VAL_COLS)
    yield tid, PK_VAL_COLS, sn
    try:
        _drop_table(sync, sn, "t")
    except Exception:
        pass


def _batch(cols, rows):
    schema = gnitz.Schema(cols)
    b = gnitz.ZSetBatch(schema)
    for row in rows:
        b.append(**row)
    return b


# ---------------------------------------------------------------------------
# Basic operations
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_and_scan(aconn, table):
    tid, cols, _ = table
    batch = _batch(cols, [{"pk": 1, "val": 100}, {"pk": 2, "val": 200}])

    lsn = await aconn.push(tid, batch)
    assert isinstance(lsn, int)
    assert lsn > 0

    result = await aconn.scan(tid)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_scan_many_async(aconn, sync):
    """Async scan_many resolves to a list of per-relation results in request
    order, snapshotted at one SAL cut."""
    sn = "am" + _uid()
    sync.create_schema(sn)
    a = sync.create_table(sn, "a", PK_VAL_COLS)
    b = sync.create_table(sn, "b", PK_VAL_COLS)
    try:
        await aconn.push(a, _batch(PK_VAL_COLS, [{"pk": 1, "val": 10}, {"pk": 2, "val": 20}]))
        await aconn.push(b, _batch(PK_VAL_COLS, [{"pk": 5, "val": 50}]))

        results = await aconn.scan_many([a, b])
        assert isinstance(results, list) and len(results) == 2
        assert sorted((r.pk, r.val) for r in results[0]) == [(1, 10), (2, 20)]
        assert sorted((r.pk, r.val) for r in results[1]) == [(5, 50)]

        # A one-relation async scan_many matches an async scan.
        one = await aconn.scan_many([a])
        assert len(one) == 1
        assert sorted((r.pk, r.val) for r in one[0]) == [(1, 10), (2, 20)]
    finally:
        sync.drop_schema(sn)


@pytest.mark.asyncio
async def test_scan_many_malformed_list_does_not_desync(aconn, sync):
    """A malformed async scan_many is rejected client-side (before any frame is
    sent) and leaves the connection fully usable. The empty case is the headline:
    without local validation it sends a count=0 frame whose single server error
    frame the N=0 read loop never consumes, desyncing every later request."""
    sn = "am" + _uid()
    sync.create_schema(sn)
    a = sync.create_table(sn, "a", PK_VAL_COLS)
    b = sync.create_table(sn, "b", PK_VAL_COLS)
    try:
        await aconn.push(a, _batch(PK_VAL_COLS, [{"pk": 1, "val": 10}]))
        await aconn.push(b, _batch(PK_VAL_COLS, [{"pk": 5, "val": 50}]))

        # Empty / over-cap / duplicate all raise locally, and after each the
        # SAME connection still serves a valid scan_many correctly (the desync
        # guard: a stale unread error frame would corrupt this follow-up read).
        for bad in ([], list(range(a, a + 17)), [a, a]):
            with pytest.raises(gnitz.GnitzError):
                await aconn.scan_many(bad)
            ok = await aconn.scan_many([a, b])
            assert sorted((r.pk, r.val) for r in ok[0]) == [(1, 10)]
            assert sorted((r.pk, r.val) for r in ok[1]) == [(5, 50)]
    finally:
        sync.drop_schema(sn)


@pytest.mark.asyncio
async def test_push_many_rows(aconn, table):
    """Push 200 rows and scan back — mirrors test_push_scan_multiworker."""
    tid, cols, _ = table
    n = 200
    batch = _batch(cols, [{"pk": i, "val": i * 10} for i in range(1, n + 1)])
    await aconn.push(tid, batch)

    result = await aconn.scan(tid)
    pks = sorted(row.pk for row in result)
    assert pks == list(range(1, n + 1))


@pytest.mark.asyncio
async def test_push_scan_data_integrity(aconn, table):
    """Verify that column values survive the async round-trip."""
    tid, cols, _ = table
    batch = _batch(cols, [
        {"pk": 10, "val": -999},
        {"pk": 20, "val": 0},
        {"pk": 30, "val": 2**62},
    ])
    await aconn.push(tid, batch)

    result = await aconn.scan(tid)
    rows = {r.pk: r.val for r in result}
    assert rows[10] == -999
    assert rows[20] == 0
    assert rows[30] == 2**62


@pytest.mark.asyncio
async def test_multiple_pushes_accumulate(aconn, table):
    """Multiple sequential pushes to the same table accumulate rows."""
    tid, cols, _ = table
    for i in range(5):
        batch = _batch(cols, [{"pk": 100 + i, "val": i}])
        await aconn.push(tid, batch)

    result = await aconn.scan(tid)
    assert len(result) == 5


@pytest.mark.asyncio
async def test_upsert_via_push(aconn, table):
    """Push same PK twice — second push upserts (replaces val)."""
    tid, cols, _ = table
    await aconn.push(tid, _batch(cols, [{"pk": 1, "val": 100}]))
    await aconn.push(tid, _batch(cols, [{"pk": 1, "val": 200}]))

    result = await aconn.scan(tid)
    rows = list(result)
    assert len(rows) == 1
    assert rows[0].val == 200


@pytest.mark.asyncio
async def test_seek(aconn, table, sync):
    tid, cols, _ = table
    batch = _batch(cols, [{"pk": i, "val": i * 10} for i in range(1, 11)])

    # Push via sync to ensure data is flushed and visible
    sync.push(tid, batch)

    # Seek for pk=5 via sync to establish baseline
    sync_result = sync.seek(tid, pk=5)
    sync_len = len(sync_result) if sync_result is not None else 0

    # Same seek via async
    result = await aconn.seek(tid, pk=5)
    assert result is not None
    assert len(result) == sync_len


@pytest.mark.asyncio
async def test_scan_empty_table(aconn, table):
    """Scan on an empty table returns a result with 0 rows."""
    tid, cols, _ = table
    result = await aconn.scan(tid)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_scan_system_table(aconn):
    """Scan a system table (always exists, always has rows)."""
    result = await aconn.scan(1)  # SCHEMA_TAB
    assert result is not None
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Empty push (regression: the server must ACK an empty push as a no-op push —
# LSN 0 — never mis-route it to a scan whose streamed table dump desyncs the
# one-frame push reply reader; the sync-connection variant lives in test_dml)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_empty_push_interleaved(aconn, table):
    """Interleave an empty push among non-empty pushes in one in-flight group.
    Every push gets exactly one result: the empty one is 0, the rest are
    non-decreasing non-zero LSNs, and a later scan shows only the non-empty
    rows (no frame misalignment across the batch)."""
    tid, cols, _ = table
    schema = gnitz.Schema(cols)

    results = await asyncio.gather(
        aconn.push(tid, _batch(cols, [{"pk": 1, "val": 10}])),
        aconn.push(tid, gnitz.ZSetBatch(schema)),  # empty — the interleaved no-op
        aconn.push(tid, _batch(cols, [{"pk": 2, "val": 20}])),
        aconn.push(tid, _batch(cols, [{"pk": 3, "val": 30}])),
    )
    assert len(results) == 4
    assert results[1] == 0
    nonzero = [results[0], results[2], results[3]]
    assert all(r > 0 for r in nonzero)
    assert nonzero == sorted(nonzero)

    result = await aconn.scan(tid)
    rows = {r.pk: r.val for r in result}
    assert rows == {1: 10, 2: 20, 3: 30}


# ---------------------------------------------------------------------------
# Mixed-kind pipelining
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_mixes_operation_kinds(sync, aconn):
    """Push, scan, seek and scan_many gathered as one in-flight group. The server
    handles one request per connection at a time and replies in request order, so
    each future must resolve to *its own* operation's result — a swap between two
    kinds would hand a scan's rows to a push's future, or one relation's rows to
    another's. Run at W>1: replies leave the workers out of order and only the
    master's serialisation puts them back."""
    sn = "amix" + _uid()
    a = _make_table(sync, sn, "ta", PK_VAL_COLS)
    b = sync.create_table(sn, "tb", PK_VAL_COLS)
    try:
        # Distinct per-relation payloads, so a mis-correlated reply is visible.
        await aconn.push(a, _batch(PK_VAL_COLS, [{"pk": i, "val": 100 + i} for i in range(1, 6)]))
        await aconn.push(b, _batch(PK_VAL_COLS, [{"pk": i, "val": 900 + i} for i in range(1, 4)]))

        push_lsn, scan_a, scan_b, seek_a, seek_b, many = await asyncio.gather(
            aconn.push(a, _batch(PK_VAL_COLS, [{"pk": 42, "val": 4242}])),
            aconn.scan(a),
            aconn.scan(b),
            aconn.seek(a, 3),
            aconn.seek(b, 2),
            aconn.scan_many([b, a]),
        )

        rows_a = {i: 100 + i for i in range(1, 6)}
        rows_b = {i: 900 + i for i in range(1, 4)}
        assert isinstance(push_lsn, int) and push_lsn > 0
        # The push was submitted first, so every read behind it in the batch sees
        # its row: request order is honoured, not just reply order.
        assert {r.pk: r.val for r in scan_a} == {**rows_a, 42: 4242}
        assert {r.pk: r.val for r in scan_b} == rows_b
        assert [(r.pk, r.val) for r in seek_a] == [(3, 103)]
        assert [(r.pk, r.val) for r in seek_b] == [(2, 902)]
        # scan_many keeps request order, which is the reverse of the two scans above.
        assert [{r.pk: r.val for r in res} for res in many] == [rows_b, {**rows_a, 42: 4242}]
    finally:
        try:
            sync.drop_table(sn, "tb")
        except Exception:
            pass
        _drop_table(sync, sn, "ta")


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_context_manager(server):
    async with aio.connect(server) as conn:
        result = await conn.scan(1)
        assert result is not None


@pytest.mark.asyncio
async def test_connect_await(server):
    conn = await aio.connect(server)
    try:
        result = await conn.scan(1)
        assert result is not None
    finally:
        await conn.aclose()


@pytest.mark.asyncio
async def test_close_idempotent(server):
    conn = await aio.connect(server)
    await conn.aclose()
    conn._transport.close()  # second call must not raise


@pytest.mark.asyncio
async def test_multiple_connections(server):
    """Multiple async connections to the same server work independently."""
    async with aio.connect(server) as c1, aio.connect(server) as c2:
        r1 = await c1.scan(1)
        r2 = await c2.scan(1)
        assert len(r1) == len(r2)


# ---------------------------------------------------------------------------
# Pipelining — several operations in flight, gathered
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_push(aconn, table):
    tid, cols, _ = table
    schema = gnitz.Schema(cols)
    n = 50

    futures = []
    for i in range(n):
        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1000 + i, val=i)
        futures.append(aconn.push(tid, batch))
    results = await asyncio.gather(*futures)

    assert len(results) == n
    for r in results:
        assert isinstance(r, int), f"expected int LSN, got {type(r)}: {r}"

    # Verify all rows landed
    result = await aconn.scan(tid)
    assert len(result) >= n


@pytest.mark.asyncio
async def test_pipeline_large(aconn, table):
    """500 concurrent pushes on one connection, all in flight at once."""
    tid, cols, _ = table
    schema = gnitz.Schema(cols)
    n = 500

    futures = []
    for i in range(n):
        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=5000 + i, val=i)
        futures.append(aconn.push(tid, batch))
    results = await asyncio.gather(*futures)

    assert len(results) == n
    errors = [r for r in results if isinstance(r, Exception)]
    assert errors == [], f"push errors: {errors}"


@pytest.mark.asyncio
async def test_pipeline_scan(aconn, table):
    """Concurrent scans — all return the same result."""
    tid, cols, _ = table
    await aconn.push(tid, _batch(cols, [{"pk": 1, "val": 10}]))

    results = await asyncio.gather(*[aconn.scan(tid) for _ in range(10)])

    assert len(results) == 10
    for r in results:
        assert not isinstance(r, Exception)


@pytest.mark.asyncio
async def test_operations_submit_on_call_not_on_await(aconn, table):
    """Each method submits when called and hands back its future, so several can
    be in flight before any is awaited, and each stays independently awaitable."""
    tid, cols, _ = table

    f1 = aconn.push(tid, _batch(cols, [{"pk": 1, "val": 1}]))
    f2 = aconn.push(tid, _batch(cols, [{"pk": 2, "val": 2}]))

    assert isinstance(await f1, int)
    assert isinstance(await f2, int)


# ---------------------------------------------------------------------------
# Concurrent operations (asyncio.gather)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_pushes(aconn, table):
    tid, cols, _ = table

    async def do_push(pk):
        batch = _batch(cols, [{"pk": pk, "val": pk * 10}])
        return await aconn.push(tid, batch)

    results = await asyncio.gather(*[do_push(2000 + i) for i in range(20)])
    assert len(results) == 20
    for r in results:
        assert isinstance(r, int)


@pytest.mark.asyncio
async def test_concurrent_scans(aconn, table):
    """Multiple concurrent scans on the same table."""
    tid, cols, _ = table
    await aconn.push(tid, _batch(cols, [{"pk": i, "val": i} for i in range(1, 6)]))

    results = await asyncio.gather(*[aconn.scan(tid) for _ in range(10)])
    assert len(results) == 10
    for r in results:
        assert len(r) == 5


@pytest.mark.asyncio
async def test_interleaved_push_scan(aconn, table):
    """Push then scan, repeated — each scan should see at least the rows
    from the pushes before it."""
    tid, cols, _ = table
    total = 0
    for i in range(5):
        batch = _batch(cols, [{"pk": 3000 + i, "val": i}])
        await aconn.push(tid, batch)
        total += 1
        result = await aconn.scan(tid)
        assert len(result) >= total


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_push_bad_table(aconn):
    schema = gnitz.Schema([
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    ])
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1)

    with pytest.raises(Exception):
        await aconn.push(0xDEAD_BEEF, batch)


@pytest.mark.asyncio
async def test_error_among_concurrent_pushes(aconn, table):
    """A failure in any one of several in-flight operations must surface when
    they are gathered — the good pushes do not swallow the bad one."""
    tid, cols, _ = table

    with pytest.raises(gnitz.GnitzError):
        await asyncio.gather(
            aconn.push(tid, _batch(cols, [{"pk": 1, "val": 1}])),
            aconn.push(0xDEAD_BEEF, _batch(cols, [{"pk": 2, "val": 2}])),  # bad table
            aconn.push(tid, _batch(cols, [{"pk": 3, "val": 3}])),
        )


@pytest.mark.asyncio
async def test_connection_usable_after_error(aconn, table):
    """After a server error, the connection should still be usable."""
    tid, cols, _ = table
    schema = gnitz.Schema([
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    ])
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1)

    try:
        await aconn.push(0xDEAD_BEEF, batch)
    except Exception:
        pass

    # Connection should still work
    result = await aconn.scan(tid)
    assert result is not None


# ---------------------------------------------------------------------------
# LSN tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lsn_monotonic(aconn, table):
    """LSNs returned by push should be monotonically increasing."""
    tid, cols, _ = table
    lsns = []
    for i in range(10):
        lsn = await aconn.push(tid, _batch(cols, [{"pk": 4000 + i, "val": i}]))
        lsns.append(lsn)

    for i in range(1, len(lsns)):
        assert lsns[i] >= lsns[i - 1], f"LSN not monotonic: {lsns}"


@pytest.mark.asyncio
async def test_pipelined_pushes_lsn_non_strict(aconn, table):
    """Pushes that the committer batches together share one zone LSN, so
    pipelined returns may carry duplicate LSNs; only non-decreasing
    monotonicity is guaranteed, not a distinct LSN per group. Whether
    *this* run actually batches depends on scheduling (committer.try_recv
    timing), so only monotonicity is asserted here. The SAL-level framing
    is unit-tested on the Rust side.
    """
    tid, cols, _ = table
    schema = gnitz.Schema(cols)
    n = 50

    futures = []
    for i in range(n):
        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=6000 + i, val=i)
        futures.append(aconn.push(tid, batch))
    lsns = await asyncio.gather(*futures)

    assert len(lsns) == n
    assert min(lsns) > 0
    for i in range(1, n):
        assert lsns[i] >= lsns[i - 1], (
            f"Phase 6 must preserve non-decreasing LSN monotonicity; "
            f"saw lsns[{i-1}]={lsns[i-1]} > lsns[{i}]={lsns[i]}"
        )


# ---------------------------------------------------------------------------
# ScanResult integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_scan_result_iteration(aconn, table):
    """ScanResult from async scan supports iteration, .all(), .first()."""
    tid, cols, _ = table
    await aconn.push(tid, _batch(cols, [
        {"pk": 1, "val": 10},
        {"pk": 2, "val": 20},
        {"pk": 3, "val": 30},
    ]))
    result = await aconn.scan(tid)

    # Iteration
    rows = list(result)
    assert len(rows) == 3

    # .first()
    first = result.first()
    assert first is not None

    # .all()
    all_rows = result.all()
    assert len(all_rows) == 3


# ---------------------------------------------------------------------------
# API parity — sync and async DML methods must stay in sync
# ---------------------------------------------------------------------------

_DML_METHODS = {"push", "scan", "seek"}


def test_api_parity_async_has_all_dml():
    """AsyncConnection must expose every DML method that Connection does."""
    missing = _DML_METHODS - set(dir(aio.AsyncConnection))
    assert not missing, f"AsyncConnection missing DML methods: {missing}"


def test_api_parity_sync_has_all_dml():
    """Connection must expose every DML method that AsyncConnection does."""
    missing = _DML_METHODS - set(dir(gnitz.GnitzClient))
    assert not missing, f"Connection missing DML methods: {missing}"


# ---------------------------------------------------------------------------
# Transport lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_loss_resolves_every_queued_request(disposable_server):
    """Losing the connection must fail every submitted request.

    All 3000 are submitted before the loop gets a turn, so each holds a future
    and a queued frame. The step that meets the dead peer must resolve all of
    them: a future nobody resolves is a hang, not an error, and the `wait_for`
    is what turns that into a failure.
    """
    target, proc = disposable_server
    conn = await aio.connect(target)
    proc.kill()
    proc.wait()

    futs = [conn.scan(1) for _ in range(3000)]
    results = await asyncio.wait_for(
        asyncio.gather(*futs, return_exceptions=True), timeout=30)
    resolved_ok = [r for r in results if not isinstance(r, BaseException)]
    assert not resolved_ok, f"{len(resolved_ok)}/{len(results)} succeeded against a dead server"

    # A request submitted after the failure was observed must resolve too.
    with pytest.raises(gnitz.GnitzError):
        await asyncio.wait_for(conn.scan(1), timeout=30)

    await conn.aclose()


@pytest.mark.asyncio
async def test_enqueue_after_close_raises(server):
    """Once close() has run, push/scan/seek must raise GnitzError immediately."""
    conn = await aio.connect(server)
    await conn.aclose()

    with pytest.raises(gnitz.GnitzError, match="connection closed"):
        await conn.scan(1)


@pytest.mark.asyncio
async def test_distinct_client_ids(server):
    """Two transports from the same process must have distinct client_ids."""
    async with aio.connect(server) as c1, aio.connect(server) as c2:
        assert c1.client_id != c2.client_id


# ---------------------------------------------------------------------------
# The mismatch a stale stamp draws
#
# That an async push packs warm at all is what makes the mismatch below
# reachable: a cold frame carries its own schema block and never draws one.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_stamp_pushes_fail_and_the_connection_recovers(aconn, sync):
    """A column rename bumps the relation's schema version, so every push
    already encoded at the stale stamp bounces. Each future fails with the
    mismatch, none of their rows is committed, and the connection stays usable:
    the eviction makes the next push cold, and it lands. The sync client holds
    its batch across the round trip, so its own retry is transparent."""
    sn = "am" + _uid()
    sync.create_schema(sn)
    tid = sync.create_table(sn, "t", PK_VAL_COLS)
    try:
        # Warm the async connection's cache under the current version.
        await aconn.push(tid, _batch(PK_VAL_COLS, [{"pk": 1, "val": 1}]))

        sync.execute_sql("ALTER TABLE t RENAME COLUMN val TO amount", schema_name=sn)

        stale = [_batch(PK_VAL_COLS, [{"pk": 10 + i, "val": 100 + i}]) for i in range(3)]
        results = await asyncio.gather(
            *[aconn.push(tid, b) for b in stale], return_exceptions=True)
        assert all(isinstance(r, gnitz.GnitzError) for r in results), results
        assert all("schema version mismatch" in str(r) for r in results), results

        # Nothing they carried was committed — a mismatched push returns before
        # the commit path.
        rows = {r.pk for r in await aconn.scan(tid)}
        assert rows == {1}, rows

        # Same connection, no reconnect: the next push is cold and lands.
        await aconn.push(tid, _batch(PK_VAL_COLS, [{"pk": 20, "val": 200}]))
        rows = {r.pk: r.amount for r in await aconn.scan(tid)}
        assert rows[20] == 200

        # The sync client's own retry makes the same stale batch transparent.
        sync.push(tid, _batch(PK_VAL_COLS, [{"pk": 30, "val": 300}]))
        rows = {r.pk: r.amount for r in await aconn.scan(tid)}
        assert rows[30] == 300
    finally:
        sync.drop_schema(sn)


# ---------------------------------------------------------------------------
# The loop's own costs: no spin, no thread, and a bounded syscall bill
# ---------------------------------------------------------------------------


def _process_cpu_seconds():
    return sum(os.times()[:2])


@pytest.mark.asyncio
async def test_an_idle_connection_consumes_no_cpu(aconn, table):
    """The writer-disarm guard: a writer callback left armed on an
    always-writable fd spins the loop at 100% CPU."""
    tid, cols, _ = table
    await aconn.push(tid, _batch(cols, [{"pk": 1, "val": 1}]))

    before = _process_cpu_seconds()
    await asyncio.sleep(1.0)
    spent = _process_cpu_seconds() - before
    assert spent < 0.05, f"an idle connection spent {spent:.3f}s of CPU over a quiet second"


_SYSCALL_CHILD = '''
import asyncio, sys
from gnitz import aio
import gnitz

target, tid, n, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
schema = gnitz.Schema(cols)

def batch(pk):
    b = gnitz.ZSetBatch(schema)
    b.append(pk=pk, val=pk)
    return b

async def main():
    async with aio.connect(target) as conn:
        # One warm-up push, so the schema cache and the measured region are the
        # same shape for every mode and for the n=0 baseline.
        await conn.push(tid, batch(0))
        if mode == "loop":
            for i in range(n):
                await conn.push(tid, batch(i + 1))
        elif n:
            await asyncio.gather(*[conn.push(tid, batch(i + 1)) for i in range(n)])

asyncio.run(main())
'''

_COUNTED = ("writev", "write", "sendto", "sendmsg", "recvfrom", "read", "recvmsg",
            "epoll_wait", "epoll_pwait", "futex", "poll", "ppoll")


def _strace_counts(script, target, tid, n, mode):
    import subprocess, sys
    out = subprocess.run(
        ["strace", "-f", "-c", "-o", "/dev/stdout",
         sys.executable, script, target, str(tid), str(n), mode],
        capture_output=True, text=True, timeout=180)
    assert out.returncode == 0, f"child exited {out.returncode}\n{out.stdout}\n{out.stderr}"
    counts = {}
    for line in out.stdout.splitlines():
        f = line.split()
        if len(f) >= 5 and f[-1] in _COUNTED:
            counts[f[-1]] = counts.get(f[-1], 0) + int(f[3])
    return counts


@pytest.mark.asyncio
async def test_syscalls_per_operation(server, sync, tmp_path):
    """The acceptance measure: `strace -f -c` over a subprocess, with the n=0
    run subtracted so startup and connect are out.

    `await` in a loop costs 5 syscalls per operation — writev, recvfrom, one
    blocking epoll_wait, and the two zero-timeout epoll_waits asyncio spends on
    the two handles each operation schedules (the deferred flush, and the wake
    `Future.set_result` posts).

    `gather`'s bill is not a per-operation constant: it is set by how many ACKs
    the server has queued when a read runs. What is the client's, and is pinned
    here, is one writev for the whole burst and at most one recvfrom per reply
    frame — never the two a header-then-payload read costs. Near 1.4/op on a
    quiet box. Futex and sendto reach zero on both: no thread left to wake."""
    import shutil
    if shutil.which("strace") is None:
        pytest.skip("strace not installed")
    sn = "as" + _uid()
    sync.create_schema(sn)
    tid = sync.create_table(sn, "t", PK_VAL_COLS)
    script = tmp_path / "syscall_child.py"
    script.write_text(_SYSCALL_CHILD)
    try:
        n = 400
        measured = {}
        for mode in ("loop", "gather"):
            base = _strace_counts(str(script), server, tid, 0, mode)
            full = _strace_counts(str(script), server, tid, n, mode)
            measured[mode] = {k: full.get(k, 0) - base.get(k, 0) for k in _COUNTED}
            assert measured[mode]["futex"] <= 0, f"{mode} still wakes a thread: {measured}"
            assert measured[mode]["sendto"] <= 0, f"{mode} still writes a self-pipe: {measured}"

        loop_total = sum(max(v, 0) for v in measured["loop"].values())
        assert loop_total <= 5 * n + 8, (
            f"await-in-a-loop: {loop_total / n:.2f}/op over a 5/op budget\n{measured}")

        g = measured["gather"]
        assert g["writev"] <= 8, f"a gathered burst must leave in one writev, not {g['writev']}"
        # One read per reply frame at worst, plus the slack the n=0 subtraction
        # does not perfectly cancel; two per frame is what this rules out.
        assert g["recvfrom"] <= n + 8, f"at most one read per reply frame, got {g['recvfrom']} for {n}"
        gather_total = sum(max(v, 0) for v in g.values())
        assert gather_total * 4 <= loop_total * 3, (
            f"gather must cost materially less than awaiting one at a time: "
            f"{gather_total} vs {loop_total}\n{measured}")
    finally:
        sync.drop_schema(sn)


_SHUTDOWN_CHILD = '''
import asyncio, sys
from gnitz import aio

async def main():
    conn = await aio.connect(sys.argv[1])
    # Work in flight when the exception escapes.
    conn.scan(1)
    conn.scan(1)
    raise RuntimeError("boom")

asyncio.run(main())
'''


def test_an_exception_escaping_asyncio_run_exits_cleanly(server, tmp_path):
    """An exception escaping `asyncio.run` with work in flight must exit with
    the exception's status, never an abort."""
    import subprocess, sys
    script = tmp_path / "shutdown_child.py"
    script.write_text(_SHUTDOWN_CHILD)
    out = subprocess.run([sys.executable, str(script), server],
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 1, f"rc={out.returncode}\n{out.stdout}\n{out.stderr}"
    assert "RuntimeError: boom" in out.stderr, out.stderr
