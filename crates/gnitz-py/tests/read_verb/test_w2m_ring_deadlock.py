"""A fanned scan's reply train and an exchange tick in flight together.

The master forwards a scan's train worker by worker, so a later worker's
frames wait in its ring until the earlier workers' trains end. A tick with an
exchange published behind the scan needs every worker: a worker whose ring is
full must still take the tick from the SAL, or the earlier worker, inside the
tick's exchange, waits on it while the master waits on the earlier worker.
"""

import os
import signal
import subprocess
import sys
import time

import gnitz
from _read import bag, rows
from _schemas import KV, kv
from _serverproc import NEEDS_MULTI, TINY_REPLY_FRAMES, join_or_fail, spawn

# ~16 MB of reply per worker against a 256 KiB ring: every later worker's ring
# fills while the master forwards the first worker's train.
ROWS = 2_000_000

_SCANNER = """
import sys, gnitz
from _schemas import KV
with gnitz.connect(sys.argv[1]) as c:
    tid, _ = c.resolve_table("big")
    print("go", flush=True)
    print(len(c.scan(tid, KV)), flush=True)
"""


@NEEDS_MULTI
def test_a_full_ring_does_not_stall_a_tick_behind_a_scan(own_server):
    """A client stopped mid-scan backs the train up into every ring; a view read
    then publishes a GROUP BY tick behind it. Once the client resumes, both the
    scan and the view read finish."""
    env = dict(TINY_REPLY_FRAMES, GNITZ_W2M_RING_BYTES=str(256 * 1024))
    target = own_server.start(workers=2, extra_env=env).target
    with gnitz.connect(target) as c:
        big = c.create_table("big", KV)
        for lo in range(0, ROWS, 50_000):
            c.push(big, kv(*((i, i) for i in range(lo, lo + 50_000))))
        c.execute_sql("CREATE TABLE s (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)")
        c.execute_sql("CREATE VIEW agg AS SELECT g, SUM(v) AS t FROM s GROUP BY g")
        # Below the coalesce threshold: pending until the view read drains it.
        c.execute_sql("INSERT INTO s VALUES (1, 1, 1)")

        tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scanner = subprocess.Popen(
            [sys.executable, "-c", _SCANNER, target], stdout=subprocess.PIPE, text=True,
            env=dict(os.environ, PYTHONPATH=tests_dir))
        try:
            assert scanner.stdout.readline().strip() == "go"
            time.sleep(0.15)
            scanner.send_signal(signal.SIGSTOP)
            time.sleep(0.3)
            read = []
            reader = spawn(lambda: read.append(bag(rows(c, "SELECT * FROM agg"))))
            time.sleep(0.3)
            scanner.send_signal(signal.SIGCONT)
            out, _ = scanner.communicate(timeout=60)
            assert int(out.strip()) == ROWS
            join_or_fail("the view read never finished", reader)
            assert read == [{(1, 1): 1}]
        finally:
            scanner.kill()
