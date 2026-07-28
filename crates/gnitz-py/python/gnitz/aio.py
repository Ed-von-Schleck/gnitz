"""Async client for gnitz — native ``await`` support with pipelining.

Usage::

    from gnitz.aio import connect

    async with connect("/var/run/gnitz.sock") as conn:
        lsn = await conn.push(table_id, batch)
        result = await conn.scan(table_id)

The target may also be a TLS address: ``tls://HOST:PORT`` with an optional
single param — ``?insecure`` (skip certificate verification; dev/test) or
``?ca=PATH`` (PEM root override). Anything without the ``tls://`` prefix is
an AF_UNIX socket path.

Known limitation: connect + HELLO run synchronously on the calling
(asyncio-loop) thread, so a ``tls://`` connect to a slow or unreachable
remote host blocks the loop for up to the 10 s connect timeout. Harmless
for loopback.

Pipeline (batch many pushes, one round-trip)::

    async with conn.pipeline() as pipe:
        pipe.push(table_id, batch1)
        pipe.push(table_id, batch2)
    print(pipe.results)   # [lsn1, lsn2]
"""

import asyncio

from gnitz._native import AsyncTransport, GnitzError  # noqa: F401


# Passed to the Rust I/O thread, which resolves a whole batch of futures through
# one ``call_soon_threadsafe`` — that call allocates a handle, takes the loop
# lock and writes the self-pipe, so paying it per future cost ~40x more than
# paying it per batch and blocked the loop while the I/O thread held the GIL.
def _resolve_batch(items):
    for future, value, is_exception in items:
        if future.done():
            continue
        if is_exception:
            future.set_exception(value)
        else:
            future.set_result(value)


def connect(socket_path):
    """Connect to a gnitz server.

    Usable as both ``conn = await connect(path)``
    and ``async with connect(path) as conn:``.
    """
    return AsyncConnection(socket_path)


async def _immediate_return(value):
    return value


class AsyncConnection:
    """Async connection to a gnitz server.

    All I/O runs on a background Rust thread.  ``await`` on any method
    suspends the calling coroutine until the server responds — the event
    loop is free to service other work in the meantime.

    Connecting is synchronous, so the object ``connect()`` returns is the
    connection itself: awaiting it yields the same object, and entering it as
    an ``async with`` block closes it on exit.
    """

    __slots__ = ("_transport",)

    def __init__(self, socket_path):
        loop = asyncio.get_running_loop()
        self._transport = AsyncTransport(socket_path, loop, _resolve_batch)

    def __await__(self):
        # Yield once so asyncio recognises `await connect(path)` as awaiting a
        # coroutine, then hand back this same connection.
        return _immediate_return(self).__await__()

    async def push(self, target_id, batch):
        """Push a batch to a table.  Returns the ingest LSN (int)."""
        return await self._transport.push(target_id, batch)

    async def scan(self, target_id, include_hidden=False):
        """Scan a table/view.  Returns a ``ScanResult``."""
        return await self._transport.scan(target_id, include_hidden)

    async def scan_many(self, target_ids, include_hidden=False):
        """Consistent snapshot of N relations at one server-side SAL cut.

        Returns a ``list`` of ``ScanResult`` in request order.  An atomic
        multi-table transaction is never observed torn across the list.
        """
        return await self._transport.scan_many(target_ids, include_hidden)

    async def seek(self, table_id, pk=0, include_hidden=False):
        """Point-lookup by primary key.  Returns a ``ScanResult``."""
        return await self._transport.seek(table_id, pk, include_hidden)

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

    __slots__ = ("_transport", "_futures", "results")

    def __init__(self, conn):
        self._transport = conn._transport
        self._futures = []
        self.results = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *_):
        if self._futures:
            if exc_type is None:
                # Success path: raise on any failure instead of silently
                # placing exception objects in self.results.
                self.results = list(await asyncio.gather(*self._futures))
            else:
                # Failure path: drain futures without masking the user's exception.
                await asyncio.gather(*self._futures, return_exceptions=True)
        return False

    def push(self, target_id, batch):
        """Queue a push.  Does not ``await`` — sends immediately."""
        fut = self._transport.push(target_id, batch)
        self._futures.append(fut)
        return fut

    def scan(self, target_id, include_hidden=False):
        """Queue a scan.  Does not ``await`` — sends immediately."""
        fut = self._transport.scan(target_id, include_hidden)
        self._futures.append(fut)
        return fut

    def scan_many(self, target_ids, include_hidden=False):
        """Queue a consistent multi-relation scan.  Does not ``await``.

        Resolves to a ``list`` of ``ScanResult`` in request order.
        """
        fut = self._transport.scan_many(target_ids, include_hidden)
        self._futures.append(fut)
        return fut
