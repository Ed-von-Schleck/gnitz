"""A client pipelining pushes faster than the server serves them.

The server receives from a connection only while the requests it has queued
for it weigh less than its share of the inbound budget, so the socket holds a
fast client off. A frame that would pass the budget itself ends the connection
it arrived on, whichever that is.
"""

import gnitz
from _schemas import KV, kv

# The budget's floor: the pushes below together weigh several times it.
INBOUND_BYTES = 64 << 20
PUSHES = 160
ROWS = 50_000  # one push frame is over a megabyte


def test_a_pipelining_client_is_held_off_not_dropped(own_server):
    """Every pipelined push is answered, and a second connection is served
    while they are in flight."""
    target = own_server.start(extra_env={"GNITZ_INBOUND_MEM_BYTES": str(INBOUND_BYTES)}).target
    batch = kv(*((i, i) for i in range(ROWS)))
    with gnitz.connect(target) as c, gnitz.connect(target) as other:
        tid = c.create_table("t", KV)
        with c.pipeline():
            lsns = [c.push(tid, batch) for _ in range(PUSHES)]
            # In flight behind the pushes still queued.
            assert other.resolve_table("t")[0] == tid
        values = [p.result() for p in lsns]
        assert values == sorted(values) and len(set(values)) == PUSHES
        assert len(other.scan(tid, KV)) == ROWS
