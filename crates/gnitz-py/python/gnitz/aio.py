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
remote host blocks the loop for the duration of connect and the TLS
handshake. The 10 s connect timeout is per resolved address, and the
handshake arms a further read timeout after it, so the block is not
bounded by one 10 s wait. Harmless for loopback.

Every method submits when called and returns its future, so pipelining is
just not awaiting yet — several operations ride one round-trip when they are
started together and gathered::

    lsns = await asyncio.gather(conn.push(table_id, batch1),
                                conn.push(table_id, batch2))

Any mix of operations may be pipelined. The server handles one request per
connection at a time and replies in request order, so positional correlation
is always correct — pushes, scans, seeks and ``scan_many`` may be gathered
together, and each future resolves to its own operation's result.
"""

import asyncio

from gnitz._native import AsyncTransport


# Passed to the Rust I/O thread, which resolves a whole batch of futures through
# one ``call_soon_threadsafe`` — that call allocates a handle, takes the loop
# lock and writes the self-pipe, so paying it per future costs far more than
# paying it per batch and blocks the loop while the I/O thread holds the GIL.
#
# Successes and failures arrive as two separate pairs of positionally-aligned
# lists, so the common loop carries no per-item discriminator: an ``isinstance``
# test on the value would cost more per future than the flag it replaced, and a
# single interleaved list would silently drop a trailing element on an odd
# length, leaving a coroutine awaiting a future nobody resolves.
def _resolve_batch(ok_futures, ok_values, err_futures, err_excs):
    for future, value in zip(ok_futures, ok_values):
        if not future.done():
            future.set_result(value)
    for future, exc in zip(err_futures, err_excs):      # usually empty
        if not future.done():
            future.set_exception(exc)


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
        # `connect()` must serve both `await connect(p)` and
        # `async with connect(p)`, so it returns the connection itself rather
        # than a coroutine; this is what makes the object awaitable. Connecting
        # already happened in `__init__`, so there is nothing to wait for — the
        # delegated coroutine returns without ever reaching the event loop.
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

