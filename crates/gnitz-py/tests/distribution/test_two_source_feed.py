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
from _feedviews import JOIN, Subscriber, _base_tables, _mk_feed, _zset
from _serverproc import join_or_fail

# Past the committer's tick-coalesce threshold, so each transaction's commit
# fires its own tick, with both sources' tids in it.
_ROWS = 12_000
_ROUNDS = 3


def _poll(target, sn, ready, stop, copy, errors):
    """Bootstrap, signal `ready`, then poll until a poll issued after `stop`
    comes back empty, and leave the folded copy in `copy`."""
    try:
        with gnitz.connect(target) as c:
            sub = Subscriber(c, sn, "f")
            sub.bootstrap()
            ready.set()
            while True:
                stopping = stop.is_set()
                if len(sub.poll().rows) == 0 and stopping:
                    break
            copy.update(sub.copy)
    except Exception as e:
        errors.append(repr(e))
        ready.set()


def test_a_feed_polled_across_two_source_tick_batches_loses_no_round(client, schema_name, server):
    sn = schema_name
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", JOIN)
    t_id, t_schema = client.resolve_table(sn, "t")
    u_id, u_schema = client.resolve_table(sn, "u")
    vid, _ = client.resolve_table(sn, "f")

    copy, errors = {}, []
    ready, stop = threading.Event(), threading.Event()
    poller = threading.Thread(daemon=True, target=_poll,
                              args=(server, sn, ready, stop, copy, errors))
    poller.start()
    assert ready.wait(30), "the poller never bootstrapped"

    for r in range(_ROUNDS):
        keys = range(r * _ROWS, (r + 1) * _ROWS)
        with client.transaction() as txn:
            txn.push(t_id, gnitz.ZSetBatch(t_schema).extend(
                [{"id": i, "v": i * 3, "body": f"b{i}"} for i in keys]))
            txn.push(u_id, gnitz.ZSetBatch(u_schema).extend(
                [{"id": i, "tid": i, "w": i * 7} for i in keys]))
        # Retractions on both sides, so a lost round is a missing -1 as well as
        # a missing +1.
        gone = list(keys)[:_ROWS // 10]
        with client.transaction() as txn:
            txn.delete(t_id, t_schema, gone[:len(gone) // 2])
            txn.delete(u_id, u_schema, gone[len(gone) // 2:])

    # A scan drains every pending tick, so every round exists before the
    # poller's last polls collect it.
    live = _zset(client.scan(vid).including_hidden())
    stop.set()
    join_or_fail("the poller hung", poller)
    assert not errors, errors
    assert live, "the view is empty, so the comparison tests nothing"
    assert copy == live, "the folded feed must equal the view, weights included"
