"""A checkpoint mid-workload must lose nothing and block nothing.

The checkpoint is the sole shard-durability point, and it lands wherever the SAL
threshold happens to fall — between a committed push and the tick that would
apply it to a view, or inside a fan-out read's ACK wait. These tests pin the
threshold low enough that checkpoints fire repeatedly during ordinary activity,
then assert the two things a checkpoint must not do: strand a buffered view delta
(the view stays permanently diverged until a restart rebuilds it) and wedge a
concurrent operation (a fan-out that wrote its SAL group without holding the SAL
writer used the old epoch, was skipped by every worker, and hung).

Hang detection is `thread.join` against the shared ceilings in `_serverproc`,
which are deadlock detectors rather than performance budgets.
"""
import threading

import pytest
import gnitz
from _read import bag, scanned
from _schemas import KV
from _serverproc import MULTI, START_TIMEOUT, join_or_fail, spawn


@pytest.fixture
def target(own_server):
    """Connect target of a server whose SAL checkpoints at 32 KB: a single push
    of ~500 rows encodes to roughly 30-60 KB, so checkpoints fire repeatedly
    during a test's own writes. Multi-worker, since an exchange is what a
    checkpoint can strand."""
    return own_server.start(workers=MULTI, extra_env={"GNITZ_CHECKPOINT_BYTES": str(32 * 1024)}).target


def _flood(client, tid, started, n_batches=12, batch_size=400):
    """Push `(pk, pk)` rows into `tid` to trigger checkpoints, setting `started`
    after the first push — and on failure, so waiters wake."""
    try:
        for i in range(n_batches):
            client.push(tid, gnitz.ZSetBatch(KV).extend(
                {"pk": pk, "val": pk} for pk in range(i * batch_size, (i + 1) * batch_size)))
            started.set()
    finally:
        started.set()


_HUNG = "a thread hung during a concurrent checkpoint"


def test_a_view_tracks_its_base_across_frequent_checkpoints(target):
    """15 000 rows exceeds TICK_COALESCE_ROWS (10 000), so ticks fire mid-sequence
    and a checkpoint lands between committed-but-unticked view deltas and the tick
    that would apply them. If the checkpoint's flush discards the workers'
    buffered deltas the base keeps the rows but no later tick ever reaches the
    view, which stays diverged until a restart rebuilds it. The scan barrier
    drains pending ticks, so both must show every row afterwards."""
    client = gnitz.connect(target)
    tid = client.create_table("t", KV)
    client.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t")

    total = 15_000
    for base in range(0, total, 1_000):
        client.push(tid, gnitz.ZSetBatch(KV).extend({"pk": i, "val": i} for i in range(base, base + 1_000)))

    want = {(i, i): 1 for i in range(total)}
    assert bag(scanned(client, "t"), "pk", "val") == want, "rows lost at a checkpoint"
    assert bag(scanned(client, "v"), "pk", "val") == want, \
        "a checkpoint dropped buffered view deltas"


def test_a_view_tracks_its_base_under_sustained_ingest_with_scans(target):
    """Sustained ingest in sub-tick batches crosses many checkpoints without the
    auto-tick firing per batch, so buffered view deltas routinely straddle one,
    while a second connection scans the view throughout — each scan draining
    pending ticks. Neither thread may hang and the view must still equal its base.
    """
    pusher, scanner = gnitz.connect(target), gnitz.connect(target)
    tid = pusher.create_table("t", KV)
    pusher.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t")
    vid, v_schema = pusher.resolve_table("v")

    n_batches, batch_size = 40, 300   # 300 < TICK_COALESCE_ROWS: no auto-tick
    started = threading.Event()

    def scan_loop():
        started.wait(timeout=START_TIMEOUT)
        for _ in range(30):
            scanner.scan(vid, v_schema)

    join_or_fail(_HUNG, spawn(_flood, pusher, tid, started, n_batches, batch_size), spawn(scan_loop))

    want = {(i, i): 1 for i in range(n_batches * batch_size)}
    assert bag(scanned(pusher, "t"), "pk", "val") == want
    assert bag(scanned(pusher, "v"), "pk", "val") == want


@pytest.mark.parametrize("probe", ["seek", "scan"])
def test_a_fanout_read_does_not_hang_during_a_checkpoint(probe, target):
    """A keyed read (routed to one worker) and a full read (fanned out) each
    write their own SAL group. Writing it without holding the SAL writer lets a request that
    arrives in the Flush ACK-wait window use the old epoch, which every worker
    skips — and the read never returns."""
    pusher, reader = gnitz.connect(target), gnitz.connect(target)
    filler = pusher.create_table("filler", KV)
    pusher.execute_sql(
        "CREATE TABLE read (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "INSERT INTO read VALUES (1, 10), (42, 999)")
    read, read_schema = pusher.resolve_table("read")
    started = threading.Event()

    def read_loop():
        started.wait(timeout=START_TIMEOUT)
        for _ in range(40):
            if probe == "seek":
                assert bag(reader.seek(read, read_schema, pk=42), "pk", "val") == {(42, 999): 1}
            else:
                assert bag(reader.scan(read, read_schema), "pk", "val") == {(1, 10): 1, (42, 999): 1}

    join_or_fail(_HUNG, spawn(_flood, pusher, filler, started), spawn(read_loop))


def test_a_unique_constraint_holds_under_checkpoint_pressure(target):
    """The unique filter must be updated exactly once per durably committed
    batch, and no checkpoint may reset it. Inserts against a unique indexed column
    run while the SAL floods; afterwards every row must be visible and a duplicate
    must still be rejected — a lost or reset filter accepts it."""
    pusher, inserter = gnitz.connect(target), gnitz.connect(target)
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
    with pytest.raises(gnitz.GnitzIntegrityError, match="[Uu]nique index violation"):
        inserter.execute_sql(f"INSERT INTO unique_t VALUES ({n_rows + 1}, 0)")


def test_a_view_created_over_committed_data_survives_a_checkpoint_window(target):
    """Enough rows to cross the checkpoint threshold — draining the unticked deltas —
    and then a CREATE of an exchange GROUP BY view whose only driver is the
    committed store. A flushed-snapshot under-count and a
    checkpoint-strands-exchange regression both surface as a wrong result."""
    conn = gnitz.connect(target)
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "n BIGINT NOT NULL)")
    n, chunk = 2000, 1000
    for base in range(0, n, chunk):
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(
                f"({i}, {i % 10}, {i})" for i in range(base, base + chunk)))
    conn.execute_sql("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g")
    assert bag(scanned(conn, "v"), "g", "c") == {(g, n // 10): 1 for g in range(10)}
