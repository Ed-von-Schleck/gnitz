"""A client keeping pushes in flight faster than the server serves them.

The server receives from a connection only while the requests it has queued
for it weigh less than its share of the inbound budget, so the socket holds a
fast client off. A frame that would pass the budget itself ends the connection
it arrived on, whichever that is.
"""

import asyncio

import gnitz
from gnitz import aio
from _schemas import KV, kv

# The budget's floor: the pushes below together weigh several times it.
INBOUND_BYTES = 64 << 20
PUSHES = 160
ROWS = 50_000  # one push frame is over a megabyte


async def test_a_client_with_pushes_in_flight_is_held_off_not_dropped(own_server):
    """Every push in flight is answered, and a second connection is served
    while they are."""
    target = own_server.start(extra_env={"GNITZ_INBOUND_MEM_BYTES": str(INBOUND_BYTES)}).target
    batch = kv(*((i, i) for i in range(ROWS)))
    async with aio.connect(target) as c:
        with gnitz.connect(target) as other:
            tid = await c.create_table("t", KV)
            # Each push is submitted when called: all of them are queued
            # before the second connection asks.
            pushes = [c.push(tid, batch) for _ in range(PUSHES)]
            assert (await asyncio.to_thread(other.resolve_table, "t"))[0] == tid
            values = await asyncio.gather(*pushes)
            assert values == sorted(values) and len(set(values)) == PUSHES
            assert len(other.scan(tid, KV)) == ROWS
