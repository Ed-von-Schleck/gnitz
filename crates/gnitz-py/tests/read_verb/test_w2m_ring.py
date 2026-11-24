"""No reader's pace holds up another's reply on a worker's ring.

A reader takes the trains it reads in SAL order, so a frame left on a ring
waits only for a reader of an earlier lease. A one-frame reply that passed an
owed train is the one frame out of that order; the master copies it off the
ring unless its reader is already waiting on that worker. A reader that stops
or slows holds the ring only until a worker it holds up asks for the ring back
— which a tick exchanging between workers does as much as a read.
"""

import os
import select
import signal
import threading
import time

import _childproc
import gnitz
from _read import bag, rows
from _schemas import KV, kv
from _serverproc import TINY_REPLY_FRAMES, join_or_fail, spawn

# A 64 KiB ring holds a handful of 16 KiB frames, and each table's train runs to
# hundreds of them on every worker.
RING = dict(TINY_REPLY_FRAMES, GNITZ_W2M_RING_BYTES=str(64 * 1024))
ROWS = 400_000


def _load(c, name, rows):
    t = c.create_table(name, KV)
    for lo in range(0, rows, 50_000):
        c.push(t, kv(*((i, i) for i in range(lo, min(rows, lo + 50_000)))))
    return t


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
    go = threading.Event()

    def multi():
        with gnitz.connect(target) as a:
            go.set()
            for _ in range(3):
                assert [len(r) for r in a.scan_many([(t1, KV), (t2, KV)])] == [ROWS, ROWS]

    def probes():
        with gnitz.connect(target) as b:
            assert go.wait(60)
            for _ in range(200):
                assert bag(b.scan(small, KV)) == {(i, i): 1 for i in range(4)}

    join_or_fail("the multi-scan or a small scan stalled",
                 spawn(multi), *(spawn(probes) for _ in range(8)))


def test_a_stopped_reader_holds_up_no_write_and_no_other_read(own_server):
    """A client stopped mid-scan backs its train up into every ring. A view read
    whose GROUP BY tick exchanges between the workers, a push, a SQL INSERT, and
    another client's scans of a one-frame and a many-frame table all finish
    while it stays stopped, and its own scan finishes once it resumes."""
    big_rows = 2_000_000
    target = own_server.start(workers=2, extra_env=RING).target
    with gnitz.connect(target) as c:
        _load(c, "big", big_rows)
        other = _load(c, "other", 1_000)
        wide = _load(c, "wide", 50_000)
        sink = c.create_table("sink", KV)
        c.execute_sql(
            "CREATE TABLE s (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT); "
            "CREATE VIEW agg AS SELECT g, SUM(v) AS t FROM s GROUP BY g")
        # Below the coalesce threshold: pending until the view read drains it.
        c.execute_sql("INSERT INTO s VALUES (1, 1, 1)")

    def write_and_read():
        with gnitz.connect(target) as d:
            assert bag(rows(d, "SELECT * FROM agg")) == {(1, 1): 1}
            d.push(sink, kv(*((i, i) for i in range(1_000))))
            # A PK table's INSERT reads before it writes: its duplicate-key preflight.
            d.execute_sql(
                "INSERT INTO sink VALUES " + ", ".join(f"({i}, {i})" for i in range(1_000, 2_000)))
            assert [len(d.scan(t, KV)) for t in (sink, other, wide)] == [2_000, 1_000, 50_000]

    scanner = _childproc.start("_scanchild", target, "big")
    try:
        assert os.read(scanner.stdout.fileno(), 3) == b"go\n"
        time.sleep(0.15)
        scanner.send_signal(signal.SIGSTOP)
        assert not select.select([scanner.stdout], [], [], 0)[0], "the scan ended before the stop"
        time.sleep(0.3)
        # Well inside the send timeout that evicts a reader making no progress,
        # which would free whatever it held.
        join_or_fail("a write or read waited on the stopped reader", spawn(write_and_read), timeout=15)
        scanner.send_signal(signal.SIGCONT)
        out, _ = scanner.communicate(timeout=60)
        assert int(out) == big_rows
    finally:
        scanner.kill()
