"""A delta-fed join whose two sources tick in one tick batch, polled while it
ticks.

Each transaction pushes past the tick-coalesce threshold into both sources at
once, so the tick that follows carries both tids in one batch: two rounds, each
with its own exchange rounds. A concurrent poller folds every round the feed
hands it; a round lost between the two — answered before it ran, then skipped
by the advanced cursor — shows as a weight the folded copy lacks.
"""
import threading

import gnitz
from _feedviews import JOIN, Subscriber, fed_view
from _serverproc import MULTI, join_or_fail, spawn

# Past the threshold the server boots with, so each transaction's commit fires
# its own tick, with both sources' tids in it.
_COALESCE = 64
_ROWS = 100
_ROUNDS = 5


def test_a_feed_polled_across_two_source_tick_batches_loses_no_round(own_server):
    own_server.start(workers=MULTI, extra_env={"GNITZ_TICK_COALESCE_ROWS": str(_COALESCE)})
    with gnitz.connect(own_server.target) as client, gnitz.connect(own_server.target) as feed:
        fed_view(client, JOIN)
        t_id, t_schema = client.resolve_table("t")
        u_id, u_schema = client.resolve_table("u")
        sub = Subscriber(feed, "f")
        sub.bootstrap()

        stop = threading.Event()

        def poll():
            while not stop.is_set():
                sub.poll()

        poller = spawn(poll)
        for r in range(_ROUNDS):
            keys = range(r * _ROWS, (r + 1) * _ROWS)
            with client.transaction() as txn:
                txn.push(t_id, gnitz.ZSetBatch(t_schema).extend(
                    [{"id": i, "v": i * 3, "body": f"b{i}"} for i in keys]))
                txn.push(u_id, gnitz.ZSetBatch(u_schema).extend(
                    [{"id": i, "tid": i, "w": i * 7} for i in keys]))
            # Retractions on both sides, so a lost round is a missing -1 as well
            # as a missing +1.
            gone = list(keys)[:_ROWS // 10]
            with client.transaction() as txn:
                txn.delete(t_id, t_schema, gone[:len(gone) // 2])
                txn.delete(u_id, u_schema, gone[len(gone) // 2:])
        stop.set()
        join_or_fail("the poller hung", poller)

        joined = sub.assert_converged("the folded feed must equal the view, weights included")
        assert joined == _ROUNDS * (_ROWS - _ROWS // 10)
