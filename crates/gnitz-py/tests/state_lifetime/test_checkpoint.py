"""A checkpoint mid-workload must lose nothing and block nothing.

The checkpoint is the sole shard-durability point, and it lands wherever the SAL
threshold happens to fall — between a committed push and the tick that would
apply it to a view, or under a stream of reads. These tests pin the threshold
low enough that checkpoints fire repeatedly during ordinary activity, assert
that they did, and then assert the two things a checkpoint must not do: strand a
buffered view delta (the view stays permanently diverged until a restart rebuilds
it) and wedge a concurrent operation.

Hang detection is `thread.join` against the shared ceilings in `_serverproc`,
which are deadlock detectors rather than performance budgets.
"""
import threading
import time

import pytest
import gnitz
from _read import bag, scanned
from _schemas import KV, kv
from _serverproc import MULTI, START_TIMEOUT, join_or_fail, spawn
from _sql import insert


_COALESCE = 1_000


@pytest.fixture
def target(own_server):
    """Connect target of a server whose SAL checkpoints at 32 KiB, which every
    test crosses with its own traffic, many times over. A table's pending rows
    auto-tick at `_COALESCE`. Multi-worker, since an exchange is what a
    checkpoint can strand."""
    own_server.start(workers=MULTI, extra_env={
        "GNITZ_CHECKPOINT_BYTES": str(32 * 1024), "GNITZ_TICK_COALESCE_ROWS": str(_COALESCE)})
    armed = own_server.sal_checkpoints()
    yield own_server.target
    assert own_server.sal_checkpoints() > armed, "no checkpoint fired during the test"


def _flood(client, tid, started, n_batches=12, batch_size=400):
    """Push `(pk, pk)` rows into `tid` to trigger checkpoints, setting `started`
    after the first push — and on failure, so waiters wake."""
    try:
        for i in range(n_batches):
            client.push(tid, kv(*((pk, pk) for pk in range(i * batch_size, (i + 1) * batch_size))))
            started.set()
    finally:
        started.set()


_HUNG = "a thread hung during a concurrent checkpoint"


def test_a_view_tracks_its_base_under_sustained_ingest_with_scans(target):
    """Sustained ingest in sub-tick batches crosses many checkpoints without the
    auto-tick firing per batch, so buffered view deltas routinely straddle one,
    while a second connection scans the view throughout — each scan draining
    pending ticks, and each seeing every row at most once. Neither thread may hang,
    and the view must still equal its base: a checkpoint whose flush discarded the
    workers' buffered deltas leaves the rows in the base and no later tick ever
    reaches the view."""
    with gnitz.connect(target) as pusher, gnitz.connect(target) as scanner:
        tid = pusher.create_table("t", KV)
        pusher.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t")
        vid, v_schema = pusher.resolve_table("v")

        n_batches, batch_size = 40, 300   # 300 < _COALESCE: no auto-tick per batch
        started = threading.Event()

        def scan_loop():
            started.wait(timeout=START_TIMEOUT)
            for _ in range(30):
                assert set(bag(scanner.scan(vid, v_schema)).values()) <= {1}, "a delta applied twice"

        join_or_fail(_HUNG, spawn(_flood, pusher, tid, started, n_batches, batch_size), spawn(scan_loop))

        want = {(i, i): 1 for i in range(n_batches * batch_size)}
        assert bag(scanned(pusher, "t"), "pk", "val") == want
        assert bag(scanned(pusher, "v"), "pk", "val") == want


def test_a_unique_constraint_holds_under_checkpoint_pressure(target):
    """The unique filter must hold every span a committed row holds, and no
    checkpoint may reset it. Inserts against a unique indexed column run while
    the SAL floods; afterwards every row must be visible and a duplicate must
    still be rejected — a lost or reset filter accepts it."""
    with gnitz.connect(target) as pusher, gnitz.connect(target) as inserter:
        filler = pusher.create_table("filler", KV)
        inserter.execute_sql(
            "CREATE TABLE unique_t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "CREATE UNIQUE INDEX ON unique_t(val)")
        started = threading.Event()
        n_rows = 50

        def insert_loop():
            started.wait(timeout=START_TIMEOUT)
            for i in range(n_rows):
                inserter.execute_sql(f"INSERT INTO unique_t VALUES ({i}, {i * 10})")

        join_or_fail(_HUNG, spawn(_flood, pusher, filler, started), spawn(insert_loop))

        assert bag(scanned(inserter, "unique_t"), "pk", "val") == {(i, i * 10): 1 for i in range(n_rows)}
        with pytest.raises(gnitz.GnitzIntegrityError, match="Unique index violation"):
            inserter.execute_sql(f"INSERT INTO unique_t VALUES ({n_rows + 1}, 0)")


def test_a_view_created_over_committed_data_survives_a_checkpoint_window(target):
    """Enough rows to cross the checkpoint threshold — draining the unticked deltas —
    and then a CREATE of an exchange GROUP BY view whose only driver is the
    committed store. A flushed-snapshot under-count and a
    checkpoint-strands-exchange regression both surface as a wrong result."""
    with gnitz.connect(target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
            "n BIGINT NOT NULL)")
        n, chunk = 2000, 1000
        for base in range(0, n, chunk):
            insert(conn, "t", [(i, i % 10, i) for i in range(base, base + chunk)])
        conn.execute_sql("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        assert bag(scanned(conn, "v"), "g", "c") == {(g, n // 10): 1 for g in range(10)}


def test_reads_alone_are_reclaimed(own_server, target):
    """Every read verb writes a SAL command group and nothing on a read path
    rewinds the write cursor, so reads alone march it to the end of the mapping.
    The committer tests its threshold only when something is sent to it, which a
    read-only workload never does, so only the watchdog's reclaim wake can start
    the checkpoint that frees it. Four connections read across many thresholds
    with no write between, and the first write afterwards must still commit."""
    with gnitz.connect(target) as client:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        want = {(1, 10): 1, (2, 20): 1, (3, 30): 1}
        rel = client.resolve_table("t")
        armed = own_server.sal_checkpoints()

        def read_loop():
            with gnitz.connect(target) as c:
                for _ in range(2_000):
                    assert bag(c.scan(*rel), "pk", "val") == want

        join_or_fail("a reader hung — the SAL wedged", *[spawn(read_loop) for _ in range(4)])
        assert own_server.sal_checkpoints() > armed, "the reads never crossed the threshold"

        client.execute_sql("INSERT INTO t VALUES (4, 40)")
        assert bag(scanned(client, "t"), "pk", "val") == want | {(4, 40): 1}


def test_no_checkpoint_runs_inside_a_ddl(own_server):
    """A checkpoint asked for while a DDL holds its locks runs only after the DDL.

    `GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT` holds the first DDL, its zone written
    and its locks held, until a task parks on the apply lock it holds. At a
    one-byte threshold the zone alone crosses it, and nothing here asks for a
    tick, so that task is the committer, in the checkpoint the watchdog's
    reclaim wake called for. No checkpoint may finish between the hold's park
    and its release, and that one runs once the DDL ends."""
    own_server.start(extra_env={"GNITZ_CHECKPOINT_BYTES": "1", "GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT": "1"})
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "INSERT INTO t VALUES (1, 10), (2, 20); "
            "CREATE VIEW v AS SELECT pk, val FROM t WHERE val > 10")
        log = own_server.log_text()
        parked = log.index("ddl hold: parked")
        released = log.index("ddl hold: released by its event", parked)
        assert "SAL checkpoint epoch=" not in log[parked:released]
        assert "SAL checkpoint epoch=" in log[released:]
        assert bag(scanned(conn, "v"), "pk", "val") == {(2, 20): 1}


def test_an_idle_server_stops_checkpointing(own_server):
    """A checkpoint leaves its own groups in the SAL, and the threshold counts
    only what is written behind them: at a one-byte threshold every write and
    every read calls for a checkpoint, and a server left alone settles after the
    last one instead of checkpointing what its own checkpoint wrote, forever."""
    own_server.start(extra_env={"GNITZ_CHECKPOINT_BYTES": "1"})
    # The watchdog first looks 100 ms after the server is ready, and a
    # checkpoint is logged a worker round trip later: several of its periods,
    # so one the boot's checkpoint called for is in the log by now.
    time.sleep(0.5)
    assert own_server.sal_checkpoints() == 0, "the boot's checkpoint called for another"
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "INSERT INTO t VALUES (1, 10)")
        assert bag(scanned(conn, "t"), "pk", "val") == {(1, 10): 1}
    armed = own_server.sal_checkpoints()
    assert armed > 0, "the traffic never crossed the threshold"

    # The watchdog polls every 100 ms; the scan's checkpoint may still be due.
    deadline = time.monotonic() + START_TIMEOUT
    while True:
        before = own_server.sal_checkpoints()
        time.sleep(0.5)
        if own_server.sal_checkpoints() == before:
            break
        assert time.monotonic() < deadline, "an idle server keeps checkpointing"
