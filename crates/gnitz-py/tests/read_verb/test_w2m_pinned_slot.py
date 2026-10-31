"""No reader's pace holds up another's reply on a worker's ring.

A reader takes the trains it reads in SAL order, so a frame left on a ring
waits only for a reader of an earlier lease. A one-frame reply that passed an
owed train is the one frame out of that order; the master copies it off the
ring unless its reader is already waiting on that worker. A reader that stops
or slows holds the ring only until a worker it holds up asks for the ring back.
"""

import os
import signal
import subprocess
import sys
import threading
import time

import gnitz
import pytest
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


_SCANNER = """
import sys, gnitz
from _schemas import KV
with gnitz.connect(sys.argv[1]) as c:
    tid, _ = c.resolve_table("big")
    print("go", flush=True)
    print(len(c.scan(tid, KV)), flush=True)
"""


def _push(d, sink):
    d.push(sink, kv(*((i, i) for i in range(1_000))))


def _insert(d, sink):
    # A PK table's INSERT reads before it writes: its duplicate-key preflight.
    d.execute_sql("INSERT INTO sink VALUES " + ", ".join(f"({i}, {i})" for i in range(1_000)))


@NEEDS_MULTI
@pytest.mark.parametrize("write", [_push, _insert], ids=["push", "sql_insert"])
def test_a_stopped_reader_holds_up_no_write_and_no_other_read(own_server, write):
    """A client stopped mid-scan: a write and another client's scans, of a
    one-frame table and of a many-frame one, all finish while it stays
    stopped, and its own scan finishes once it resumes."""
    big_rows = 2_000_000
    target = own_server.start(workers=2, extra_env=RING).target
    with gnitz.connect(target) as c:
        _load(c, "big", big_rows)
        other = _load(c, "other", 1_000)
        wide = _load(c, "wide", 50_000)
        sink = c.create_table("sink", KV)

        tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scanner = subprocess.Popen(
            [sys.executable, "-c", _SCANNER, target], stdout=subprocess.PIPE, text=True,
            env=dict(os.environ, PYTHONPATH=tests_dir))
        try:
            assert scanner.stdout.readline().strip() == "go"
            time.sleep(0.15)
            scanner.send_signal(signal.SIGSTOP)
            time.sleep(0.3)
            out = {}

            def write_and_read():
                with gnitz.connect(target) as d:
                    write(d, sink)
                    out["rows"] = (len(d.scan(sink, KV)), len(d.scan(other, KV)), len(d.scan(wide, KV)))

            # Well inside the send timeout that evicts a reader making no
            # progress, which would free whatever it held.
            _run_all([threading.Thread(target=write_and_read, daemon=True)], 15)
            assert out.get("rows") == (1_000, 1_000, 50_000), "a write or scan waited on the stopped reader"
            scanner.send_signal(signal.SIGCONT)
            done, _ = scanner.communicate(timeout=60)
            assert int(done.strip()) == big_rows
        finally:
            scanner.kill()
