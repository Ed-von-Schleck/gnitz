"""`gnitz.aio` per-operation latency: a sequential 1-row `await conn.push` loop,
on an idle interpreter and beside a thread spinning pure-Python arithmetic —
the case where a released GIL costs a switch interval per operation."""

import asyncio
import threading
import time

import pytest

import gnitz
from gnitz import aio

_SCHEMA = gnitz.Schema([
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
], [0])
_OPS = {"quick": 2_000, "full": 10_000}
_WARMUP = 100


def _spin(stop):
    x = 1
    while not stop.is_set():
        x = (x * 3 + 1) & 0xFFFF


async def _push_loop(target, tid, n):
    """Per-op latencies (ms) of `n` awaited pushes, after a warm-up."""
    latencies = []
    async with aio.connect(target) as conn:
        for i in range(_WARMUP + n):
            b = gnitz.ZSetBatch(_SCHEMA)
            b.append(pk=i, val=i)
            start = time.perf_counter()
            await conn.push(tid, b)
            if i >= _WARMUP:
                latencies.append((time.perf_counter() - start) * 1000.0)
    return latencies


@pytest.mark.parametrize("busy", [False, True], ids=["idle", "busy_thread"])
def test_async_push_await_loop(client, socket_path, bench_timer, scale_mode, busy):
    tid = client.create_table("t", _SCHEMA)
    n = _OPS[scale_mode]
    stop = threading.Event()
    spinner = threading.Thread(target=_spin, args=(stop,), daemon=True) if busy else None
    if spinner:
        spinner.start()
    try:
        latencies = asyncio.run(_push_loop(socket_path, tid, n))
    finally:
        stop.set()
        if spinner:
            spinner.join()
    bench_timer.add_latencies(latencies, rows=n)
