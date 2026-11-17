"""Async client for gnitz: `gnitz.GnitzClient`'s verbs, awaitable, on asyncio.

    from gnitz.aio import connect

    async with connect("/var/run/gnitz.sock") as conn:
        lsn = await conn.push(table_id, batch)
        await conn.execute_sql("CREATE VIEW v AS SELECT ...")

The target may also be ``tls://HOST:PORT[?QUERY]``, ``QUERY`` being
``&``-separated ``ca=PATH`` (PEM roots, default webpki), ``cert=PATH`` and
``key=PATH`` (mTLS), and ``token_file=PATH`` (the access token a server run
with ``--auth-tokens`` asks for; without it the ``GNITZ_TOKEN`` environment
variable is sent).

A verb submits when called and returns its future; calls run in the order they
were made, one at a time. A verb that is one request — ``push``, ``delete``,
``scan``, ``seek``, ``seek_by_index``, ``scan_many``, ``delta_read`` — is done
with the connection once the request is sent, so those started together share a
round trip::

    lsns = await asyncio.gather(conn.push(table_id, batch1),
                                conn.push(table_id, batch2))

``sync`` submits when called too, and returns a coroutine: the awaiting task
takes the sync's delivery when it resumes, so a task cancelled at any point
loses nothing, and the next sync brings it.

Nothing blocks the loop: socket I/O runs on it, and the connect and a mirror
store's disk work on a thread pool every client in the process shares.
Abandoning a future is not cancellation: the request is still written and
committed.
"""

import asyncio
from typing import Any

from gnitz._native import AsyncGnitzClient, Synced

__all__ = ["AsyncGnitzClient", "connect"]


async def _claimed(delivery: "asyncio.Future[Any]") -> Synced:
    """What `AsyncGnitzClient.sync` returns: its delivery is taken when the
    awaiting task resumes. A task cancelled after the answer arrived resumes
    with `CancelledError` instead, and drops the delivery at once, so the next
    sync reports its reseeds again."""
    try:
        reply = await delivery
    except asyncio.CancelledError:
        if delivery.done() and not delivery.cancelled() and delivery.exception() is None:
            delivery.result().release()
        raise
    taken: Synced = reply.take()
    return taken


def connect(target: str | None = None, schema: str = "public") -> AsyncGnitzClient:
    """A client of the gnitz server at `target`, ``GNITZ_TARGET`` where it is
    not given, with unqualified relation names resolving in `schema`;
    `other.name` names a relation of another schema.

    It returns at once and connects in the background. Awaiting the client,
    or entering it with `async with`, yields it once it is connected, and
    raises if the connect failed; `async with` closes it on exit. A verb
    called before then is queued behind the connect, and raises its failure
    if it fails."""
    return AsyncGnitzClient(target, schema)
