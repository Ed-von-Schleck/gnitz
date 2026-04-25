"""Tests for gnitz.aio — async client with pipelining."""

import asyncio
import os
import pytest
import pytest_asyncio
import gnitz
from gnitz import aio

_uid_counter = 0

def _uid():
    global _uid_counter
    _uid_counter += 1
    return f"{os.getpid()}_{_uid_counter}"


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
async def test_push_many_rows(aconn, table):
    """Push 200 rows and scan back — mirrors test_push_scan_multiworker."""
    tid, cols, _ = table
    n = 200
    batch = _batch(cols, [{"pk": i, "val": i * 10} for i in range(1, n + 1)])
    await aconn.push(tid, batch)

    result = await aconn.scan(tid)
    pks = sorted(row.pk for row in result if row.weight > 0)
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
    rows = [r for r in result if r.weight > 0]
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
# Pipeline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_push(aconn, table):
    tid, cols, _ = table
    schema = gnitz.Schema(cols)
    n = 50

    async with aconn.pipeline() as pipe:
        for i in range(n):
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1000 + i, val=i)
            pipe.push(tid, batch)

    assert len(pipe.results) == n
    for r in pipe.results:
        assert isinstance(r, int), f"expected int LSN, got {type(r)}: {r}"

    # Verify all rows landed
    result = await aconn.scan(tid)
    assert len(result) >= n


@pytest.mark.asyncio
async def test_pipeline_large(aconn, table):
    """Pipeline 500 pushes — exercises natural batching in the I/O thread."""
    tid, cols, _ = table
    schema = gnitz.Schema(cols)
    n = 500

    async with aconn.pipeline() as pipe:
        for i in range(n):
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=5000 + i, val=i)
            pipe.push(tid, batch)

    assert len(pipe.results) == n
    errors = [r for r in pipe.results if isinstance(r, Exception)]
    assert errors == [], f"pipeline errors: {errors}"


@pytest.mark.asyncio
async def test_pipeline_scan(aconn, table):
    """Pipeline of scans — all return the same result."""
    tid, cols, _ = table
    await aconn.push(tid, _batch(cols, [{"pk": 1, "val": 10}]))

    async with aconn.pipeline() as pipe:
        for _ in range(10):
            pipe.scan(tid)

    assert len(pipe.results) == 10
    for r in pipe.results:
        assert not isinstance(r, Exception)


@pytest.mark.asyncio
async def test_pipeline_empty(aconn):
    """Empty pipeline exits cleanly."""
    async with aconn.pipeline() as pipe:
        pass
    assert pipe.results == []


@pytest.mark.asyncio
async def test_pipeline_individual_futures(aconn, table):
    """Futures returned by pipeline methods are independently awaitable."""
    tid, cols, _ = table
    schema = gnitz.Schema(cols)

    async with aconn.pipeline() as pipe:
        f1 = pipe.push(tid, _batch(cols, [{"pk": 1, "val": 1}]))
        f2 = pipe.push(tid, _batch(cols, [{"pk": 2, "val": 2}]))

    # After pipeline exit, futures are resolved
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
async def test_error_in_pipeline(aconn, table):
    """A failure in any pipeline operation must raise from the async-with block."""
    tid, cols, _ = table

    with pytest.raises(gnitz.GnitzError):
        async with aconn.pipeline() as pipe:
            pipe.push(tid, _batch(cols, [{"pk": 1, "val": 1}]))
            pipe.push(0xDEAD_BEEF, _batch(cols, [{"pk": 2, "val": 2}]))  # bad table
            pipe.push(tid, _batch(cols, [{"pk": 3, "val": 3}]))


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
    """Phase 6 (Design 2): pushes that the committer batches together
    share one zone LSN, so pipelined returns may carry duplicate LSNs.
    The strict per-group LSN guarantee is gone — only non-decreasing
    monotonicity is preserved. Whether *this* run actually batches
    depends on scheduling (committer.try_recv timing); we therefore
    only assert LSN monotonicity, not the batching itself. The
    SAL-level invariant is unit-tested in
    `runtime::tests::sal::test_batched_push_shares_zone_lsn`.
    """
    tid, cols, _ = table
    schema = gnitz.Schema(cols)
    n = 50

    async with aconn.pipeline() as pipe:
        for i in range(n):
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=6000 + i, val=i)
            pipe.push(tid, batch)

    lsns = pipe.results
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
    missing = _DML_METHODS - set(dir(gnitz.Connection))
    assert not missing, f"Connection missing DML methods: {missing}"
