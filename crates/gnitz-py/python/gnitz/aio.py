"""Async client for gnitz — native ``await`` support with pipelining.

Usage::

    from gnitz.aio import connect

    async with connect("/var/run/gnitz.sock") as conn:
        lsn = await conn.push(table_id, batch)
        result = await conn.scan(table_id)

The target may also be ``tls://HOST:PORT[?QUERY]``, ``QUERY`` being
``&``-separated ``ca=PATH`` (PEM roots, default webpki), ``cert=PATH`` and
``key=PATH`` (mTLS).

Every method submits when called and returns its future, so pipelining is
just not awaiting yet — several operations ride one round-trip when they are
started together and gathered::

    lsns = await asyncio.gather(conn.push(table_id, batch1),
                                conn.push(table_id, batch2))

Any mix of operations may be pipelined. The server handles one request per
connection at a time and replies in request order, so positional correlation
is always correct — pushes, scans, seeks and ``scan_many`` may be gathered
together, and each future resolves to its own operation's result.

Three limitations are choices, not oversights:

* **Connect is synchronous** — connect + HELLO run on the calling thread, so a
  ``tls://`` connect to a slow host blocks the loop: name resolution, then the
  TCP connect, TLS handshake and HELLO under one connect deadline.
  ``asyncio.wait_for`` around
  ``connect`` therefore bounds nothing: the socket is already open by the time
  it sees the object. Harmless for loopback. Moving the connect to a thread
  would fix it and cost a thread — which is the one thing this client does not
  have, and what keeps a gathered burst free of futex traffic.
* **A connection is bound to the loop that constructed it**, because
  ``add_reader`` is a property of that loop.
* **Abandoning an operation is not cancellation.**
  ``asyncio.wait_for(conn.push(...), t)`` still writes the frame and the server
  still commits it; only the future stops being waited on.
"""

import asyncio

from gnitz._native import AsyncTransport


def connect(target):
    """Connect to a gnitz server.

    Usable as both ``conn = await connect(target)``
    and ``async with connect(target) as conn:``.
    """
    return AsyncConnection(target)


async def _immediate_return(value):
    return value


class AsyncConnection:
    """Async connection to a gnitz server.

    All I/O runs **on the event loop**: it calls back on readability, and that
    callback steps the connection and resolves whatever completed — one
    crossing into Rust per event, however many operations are in flight. The
    transport holds the loop and drives its own reader and writer
    registrations, so this class is the public surface over it and nothing
    else.

    Connecting is synchronous, so the object ``connect()`` returns is the
    connection itself: awaiting it yields the same object, and entering it as
    an ``async with`` block closes it on exit.
    """

    __slots__ = ("_transport",)

    def __init__(self, target):
        self._transport = AsyncTransport(target, asyncio.get_running_loop())
        # Two steps: binding the loop callbacks needs the transport object,
        # which its constructor cannot hand itself.
        self._transport.install()

    # -- verbs -------------------------------------------------------------

    def push(self, target_id, batch):
        """Push a batch to a table.  Awaits to the ingest LSN (int)."""
        return self._transport.push(target_id, batch)

    def scan(self, target_id):
        """Scan a table/view.  Awaits to a ``ScanResult``."""
        return self._transport.scan(target_id)

    def scan_many(self, target_ids):
        """Consistent snapshot of N relations at one server-side SAL cut.

        Awaits to a ``list`` of ``ScanResult`` in request order.  An atomic
        multi-table transaction is never observed torn across the list.
        """
        return self._transport.scan_many(target_ids)

    def seek(self, table_id, pk):
        """Point-lookup by primary key.  Awaits to a ``ScanResult``."""
        return self._transport.seek(table_id, pk)

    # -- lifecycle ---------------------------------------------------------

    def __await__(self):
        # `connect()` serves both `await connect(p)` and `async with
        # connect(p)`, so it returns the connection rather than a coroutine;
        # this is what makes that object awaitable. Connecting already happened,
        # so the delegated coroutine never reaches the event loop.
        return _immediate_return(self).__await__()

    async def aclose(self):
        """Close the connection. Deregisters both loop callbacks, abandons every
        outstanding operation, and releases the socket."""
        self._transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
        return False
