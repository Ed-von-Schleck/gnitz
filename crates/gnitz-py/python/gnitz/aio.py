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

Every method submits when called and returns its future, so pipelining is
just not awaiting yet — several operations ride one round-trip when they are
started together and gathered::

    lsns = await asyncio.gather(conn.push(table_id, batch1),
                                conn.push(table_id, batch2))

**FIFO ordering caveat:** the transport correlates replies positionally, so
concurrent operations are safe when homogeneous (all pushes to one table, or
all reads). Mixing pushes and reads in one in-flight group may produce
incorrect result ordering in multi-worker mode.
"""

import asyncio

from gnitz._native import AsyncTransport


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

    # Each of these submits the operation and hands back its future, rather
    # than wrapping it in a coroutine: `await conn.push(...)` reads and behaves
    # the same, but the operation reaches the I/O thread when it is called
    # instead of when it is awaited.  That is what lets a caller start several
    # and `asyncio.gather` them onto one round-trip.
    def push(self, target_id, batch):
        """Push a batch to a table.  Awaits to the ingest LSN (int)."""
        return self._transport.push(target_id, batch)

    def scan(self, target_id, include_hidden=False):
        """Scan a table/view.  Awaits to a ``ScanResult``."""
        return self._transport.scan(target_id, include_hidden)

    def scan_many(self, target_ids, include_hidden=False):
        """Consistent snapshot of N relations at one server-side SAL cut.

        Awaits to a ``list`` of ``ScanResult`` in request order.  An atomic
        multi-table transaction is never observed torn across the list.
        """
        return self._transport.scan_many(target_ids, include_hidden)

    def seek(self, table_id, pk=0, include_hidden=False):
        """Point-lookup by primary key.  Awaits to a ``ScanResult``."""
        return self._transport.seek(table_id, pk, include_hidden)

    async def aclose(self):
        """Close the connection."""
        self._transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self._transport.close()
        return False

