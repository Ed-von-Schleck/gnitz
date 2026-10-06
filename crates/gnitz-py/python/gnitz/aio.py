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

__all__ = ["AsyncGnitzClient", "connect"]


def connect(target: str, schema: str = "public") -> AsyncGnitzClient:
    """Connect to a gnitz server, with unqualified relation names resolving
    in `schema`; `other.name` names a relation of another schema. The
    connection is made when this returns, so awaiting it yields itself, and
    `async with` closes it on exit."""
    return AsyncGnitzClient(target, schema)
