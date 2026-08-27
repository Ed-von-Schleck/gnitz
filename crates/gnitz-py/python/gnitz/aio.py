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
  ``tls://`` connect to a slow host blocks the loop for both, and the 10 s
  connect timeout is per resolved address. ``asyncio.wait_for`` around
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

    All I/O runs **on the event loop**: it calls back on readability, and that
    callback steps the connection and resolves whatever completed — one
    crossing into Rust per event, however many operations are in flight.

    Connecting is synchronous, so the object ``connect()`` returns is the
    connection itself: awaiting it yields the same object, and entering it as
    an ``async with`` block closes it on exit.
    """

    __slots__ = ("_transport", "_loop", "_fd", "_writer_armed", "_closed")

    def __init__(self, socket_path):
        self._loop = asyncio.get_running_loop()
        self._transport = AsyncTransport(socket_path, self._loop)
        self._fd = self._transport.fileno()
        self._writer_armed = False
        self._closed = False
        # Armed once, for the life of the connection: a reader on a quiet socket
        # costs nothing, where arming per operation costs two `epoll_ctl`.
        self._loop.add_reader(self._fd, self._on_readable)

    # -- loop callbacks ----------------------------------------------------
    #
    # The loop holds these, not the Rust entry points: `add_reader` discards a
    # callback's return value, and these are what read it.

    def _on_readable(self):
        self._apply(self._transport.on_readable())

    def _on_writable(self):
        self._apply(self._transport.on_writable())

    def _apply(self, wants_write):
        """React to what a step reported: ``None`` is "finished, deregister"."""
        if wants_write is None:
            self._teardown()
        else:
            self._arm_writer(wants_write)

    def _arm_writer(self, on):
        """Add or remove the writable callback, idempotently — the flag is
        exactly "the loop owes us an ``_on_writable``". Disarming matters as
        much as arming: a writer left on an always-writable fd spins the loop
        at 100%."""
        if on == self._writer_armed:
            return
        self._writer_armed = on
        if on:
            self._loop.add_writer(self._fd, self._on_writable)
        else:
            self._loop.remove_writer(self._fd)

    def _teardown(self):
        """Deregister both callbacks. Every caller runs it before the
        transport's own close, so the selector never holds a dead fd."""
        if self._closed:
            return
        self._closed = True
        self._loop.remove_reader(self._fd)
        self._arm_writer(False)

    # -- verbs -------------------------------------------------------------

    def _submit(self, future):
        """Every verb returns through here. A submitted frame is only queued;
        the writer callback is what ships it, and deferring to that callback is
        what keeps a whole turn's submits in one ``writev``."""
        self._arm_writer(True)
        return future

    def push(self, target_id, batch):
        """Push a batch to a table.  Awaits to the ingest LSN (int)."""
        return self._submit(self._transport.push(target_id, batch))

    def scan(self, target_id, include_hidden=False):
        """Scan a table/view.  Awaits to a ``ScanResult``."""
        return self._submit(self._transport.scan(target_id, include_hidden))

    def scan_many(self, target_ids, include_hidden=False):
        """Consistent snapshot of N relations at one server-side SAL cut.

        Awaits to a ``list`` of ``ScanResult`` in request order.  An atomic
        multi-table transaction is never observed torn across the list.
        """
        return self._submit(self._transport.scan_many(target_ids, include_hidden))

    def seek(self, table_id, pk=0, include_hidden=False):
        """Point-lookup by primary key.  Awaits to a ``ScanResult``."""
        return self._submit(self._transport.seek(table_id, pk, include_hidden))

    # -- lifecycle ---------------------------------------------------------

    def __await__(self):
        # `connect()` serves both `await connect(p)` and `async with
        # connect(p)`, so it returns the connection rather than a coroutine;
        # this is what makes that object awaitable. Connecting already happened,
        # so the delegated coroutine never reaches the event loop.
        return _immediate_return(self).__await__()

    async def aclose(self):
        """Close the connection."""
        self._teardown()
        self._transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
        return False
