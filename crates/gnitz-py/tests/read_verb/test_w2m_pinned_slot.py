"""A reply a worker sends ahead of a reply it still owes holds up no ring.

A reader takes the trains it reads in SAL order, so a frame left on a ring
waits only for a reader of an earlier lease. A one-frame reply that passed an
owed train is the one frame out of that order; the master copies it off the
ring unless its reader is already waiting on that worker.
"""

import threading
import time

import gnitz
from _schemas import KV, kv
from _serverproc import NEEDS_MULTI, TINY_REPLY_FRAMES

# A 64 KiB ring holds a handful of 16 KiB frames, and each table's train runs to
# hundreds of them on every worker.
RING = dict(TINY_REPLY_FRAMES, GNITZ_W2M_RING_BYTES=str(64 * 1024))
ROWS = 400_000


def _load(c, name, rows):
    t = c.create_table(name, KV)
    for lo in range(0, rows, 50_000):
        c.push(t, kv(*((i, i) for i in range(lo, min(rows, lo + 50_000)))))
    return t


def _run_all(threads, seconds):
    """Start `threads` and wait for every one, up to `seconds` in all."""
    for t in threads:
        t.start()
    deadline = time.monotonic() + seconds
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))


@NEEDS_MULTI
def test_a_scan_between_two_trains_of_a_multi_scan_does_not_stall_either(own_server):
    """A SCAN_MULTI reads its first train worker by worker while its second
    fills the other worker's ring; small scans land on both rings meanwhile.
    A small scan's frame on the worker it reads last must not hold up the first
    train behind it, or the multi-scan waits on the small scan, which waits on
    the second train."""
    target = own_server.start(workers=2, extra_env=RING).target
    with gnitz.connect(target) as c:
        t1 = _load(c, "t1", ROWS)
        t2 = _load(c, "t2", ROWS)
        small = _load(c, "small", 4)

    out = {}
    go = threading.Event()

    def multi():
        with gnitz.connect(target) as a:
            go.set()
            out["multi"] = [[len(r) for r in a.scan_many([(t1, KV), (t2, KV)])] for _ in range(3)]

    def probes(i):
        with gnitz.connect(target) as b:
            go.wait()
            out[i] = [len(b.scan(small, KV)) for _ in range(200)]

    _run_all([threading.Thread(target=multi, daemon=True)]
             + [threading.Thread(target=probes, args=(i,), daemon=True) for i in range(8)], 120)
    assert out.get("multi") == [[ROWS, ROWS]] * 3, "the multi-scan never finished"
    for i in range(8):
        assert out.get(i) == [4] * 200, f"small scans of client {i} never finished"
