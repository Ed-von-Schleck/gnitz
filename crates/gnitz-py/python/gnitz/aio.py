"""Async client for gnitz — native ``await`` support with pipelining.

Usage::

    from gnitz.aio import connect

    async with connect("/var/run/gnitz.sock") as conn:
        lsn = await conn.push(table_id, batch)
        result = await conn.scan(table_id)

Pipeline (batch many pushes, one round-trip)::

    async with conn.pipeline() as pipe:
        pipe.push(table_id, batch1)
        pipe.push(table_id, batch2)
    print(pipe.results)   # [lsn1, lsn2]
"""

import asyncio

from gnitz._native import AsyncTransport, GnitzError  # noqa: F401


# Helpers passed to the Rust I/O thread for resolving futures safely.
def _set_result_safe(future, value):
    if not future.done():
        future.set_result(value)


def _set_exception_safe(future, exc):
    if not future.done():
        future.set_exception(exc)


def connect(socket_path):
    """Connect to a gnitz server.

    Usable as both ``conn = await connect(path)``
    and ``async with connect(path) as conn:``.
    """
    return _ConnectContext(socket_path)


class _ConnectContext:

    __slots__ = ("_path", "_conn")

    def __init__(self, path):
        self._path = path
        self._conn = None

    def __await__(self):
        self._conn = AsyncConnection(self._path)
        # Yield once so asyncio recognises this as a coroutine, then return.
        return _immediate_return(self._conn).__await__()

    async def __aenter__(self):
        self._conn = AsyncConnection(self._path)
        return self._conn

    async def __aexit__(self, *exc):
        if self._conn is not None:
            self._conn._transport.close()
        return False


async def _immediate_return(value):
    return value


class AsyncConnection:
    """Async connection to a gnitz server.

    All I/O runs on a background Rust thread.  ``await`` on any method
    suspends the calling coroutine until the server responds — the event
    loop is free to service other work in the meantime.
    """

    __slots__ = ("_transport",)

    def __init__(self, socket_path):
        loop = asyncio.get_running_loop()
        self._transport = AsyncTransport(
            socket_path, loop, _set_result_safe, _set_exception_safe,
        )

    async def push(self, target_id, batch):
        """Push a batch to a table.  Returns the ingest LSN (int)."""
        raw = batch._raw if hasattr(batch, "_raw") else batch
        return await self._transport.push(target_id, raw)

    async def scan(self, target_id):
        """Scan a table/view.  Returns a ``ScanResult``."""
        return await self._transport.scan(target_id)

    async def seek(self, table_id, pk=0):
        """Point-lookup by primary key.  Returns a ``ScanResult``."""
        pk_lo = pk & 0xFFFFFFFFFFFFFFFF
        pk_hi = (pk >> 64) & 0xFFFFFFFFFFFFFFFF
        return await self._transport.seek(table_id, pk_lo, pk_hi)

    async def aclose(self):
        """Close the connection."""
        self._transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self._transport.close()
        return False

    def pipeline(self):
        """Return a ``Pipeline`` context manager for batching operations.

        Inside the pipeline block, ``push()`` / ``scan()`` fire immediately
        without waiting for individual responses.  All responses are collected
        when the block exits.

        **FIFO ordering caveat:** pipelines are safe for homogeneous batches
        (all pushes to one table, or all reads).  Mixing pushes and reads
        in one pipeline may produce incorrect result ordering in multi-worker
        mode.  Use separate pipelines for different operation types.
        """
        return Pipeline(self)


class Pipeline:
    """Batch multiple operations into a single pipeline.

    Results are available as ``pipe.results`` after the ``async with`` block.
    """

    __slots__ = ("_conn", "_futures", "results")

    def __init__(self, conn):
        self._conn = conn
        self._futures = []
        self.results = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *_):
        if exc_type is None and self._futures:
            self.results = list(await asyncio.gather(
                *self._futures, return_exceptions=True,
            ))
        return False

    def push(self, target_id, batch):
        """Queue a push.  Does not ``await`` — sends immediately."""
        raw = batch._raw if hasattr(batch, "_raw") else batch
        fut = self._conn._transport.push(target_id, raw)
        self._futures.append(fut)
        return fut

    def scan(self, target_id):
        """Queue a scan.  Does not ``await`` — sends immediately."""
        fut = self._conn._transport.scan(target_id)
        self._futures.append(fut)
        return fut

    def seek(self, table_id, pk=0):
        """Queue a seek.  Does not ``await`` — sends immediately."""
        pk_lo = pk & 0xFFFFFFFFFFFFFFFF
        pk_hi = (pk >> 64) & 0xFFFFFFFFFFFFFFFF
        fut = self._conn._transport.seek(table_id, pk_lo, pk_hi)
        self._futures.append(fut)
        return fut
