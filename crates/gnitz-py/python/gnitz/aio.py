"""Async client for gnitz: `gnitz.GnitzClient`'s verbs, awaitable, on asyncio.

    from gnitz.aio import connect

    async with connect("/var/run/gnitz.sock") as conn:
        lsn = await conn.push(table_id, batch)
        await conn.execute_sql("CREATE VIEW v AS SELECT ...")

The target may also be ``tls://HOST:PORT[?QUERY]``, ``QUERY`` being
``&``-separated ``ca=PATH`` (PEM roots, default webpki), ``cert=PATH`` and
``key=PATH`` (mTLS).

A verb submits when called and returns its future; calls run in the order they
were made, one at a time. A verb that is one request — ``push``, ``delete``,
``scan``, ``seek``, ``seek_by_index``, ``scan_many`` — is done with the
connection once the request is sent, so those started together share a round
trip::

    lsns = await asyncio.gather(conn.push(table_id, batch1),
                                conn.push(table_id, batch2))

Socket I/O runs on the loop, and a mirror store's disk work on one thread of
its own. ``connect`` blocks the calling thread. Abandoning a
future is not cancellation: the request is still written and committed.
"""

from gnitz._native import AsyncGnitzClient


async def _immediate_return(value):
    return value


class AsyncConnection(AsyncGnitzClient):
    """A connection on the running event loop. It is connected when built, so
    awaiting it yields itself, and `async with` closes it on exit."""

    __slots__ = ()

    def __await__(self):
        # Lets `await connect(p)` and `async with connect(p)` both work.
        return _immediate_return(self).__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
        return False


def connect(target, schema="public"):
    """Connect to a gnitz server, with names resolving in `schema`."""
    return AsyncConnection(target, schema)
