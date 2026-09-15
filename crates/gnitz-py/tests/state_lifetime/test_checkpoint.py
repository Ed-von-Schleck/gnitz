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
import time

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import START_TIMEOUT, join_or_fail


@pytest.fixture
def checkpoint_client(checkpoint_server):
    with gnitz.connect(checkpoint_server) as conn:
        yield conn


def _setup(client, table="t"):
    """A fresh schema holding `(pk, val)`, with the client-side schema for
    pushing to it. Returns `(sn, tid, schema)`."""
    sn = "ck"
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    return sn, client.create_table(sn, table, cols), gnitz.Schema(cols)


def _push_loop(client, tid, schema, errors, started, n_batches=12, batch_size=400):
    """Flood `tid` to trigger checkpoints. Sets `started` after the first push
    (and on failure, so waiters wake) and records any exception in `errors`."""
    try:
        for i in range(n_batches):
            batch = gnitz.ZSetBatch(schema)
            for j in range(batch_size):
                pk = i * batch_size + j
                batch.append(pk=pk, val=pk)
            client.push(tid, batch)
            if i == 0:
                started.set()
    except Exception as exc:
        errors.append(("push", exc))
        started.set()


def _run(errors, *threads):
    """Start every thread, join it against the hang ceiling, fail naming the one
    that did not finish, then re-raise whatever any of them recorded."""
    for t in threads:
        t.start()
    join_or_fail("a thread hung during a concurrent checkpoint", *threads)
    assert not errors, f"a worker thread raised: {errors}"


def test_a_view_tracks_its_base_across_frequent_checkpoints(checkpoint_client):
    """15 000 rows exceeds TICK_COALESCE_ROWS (10 000), so ticks fire mid-sequence
    and a checkpoint lands between committed-but-unticked view deltas and the tick
    that would apply them. If the checkpoint's flush discards the workers'
    buffered deltas the base keeps the rows but no later tick ever reaches the
    view, which stays diverged until a restart rebuilds it. The scan barrier
    drains pending ticks, so both must show every row afterwards."""
    client = checkpoint_client
    sn, tid, schema = _setup(client)
    client.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name=sn)

    total = 15_000
    for base in range(0, total, 1_000):
        batch = gnitz.ZSetBatch(schema)
        for i in range(base, base + 1_000):
            batch.append(pk=i, val=i)
        client.push(tid, batch)

    want = {(i, i): 1 for i in range(total)}
    assert bag(scanned(client, sn, "t"), "pk", "val") == want, "rows lost at a checkpoint"
    assert bag(scanned(client, sn, "v"), "pk", "val") == want, \
        "a checkpoint dropped buffered view deltas"


def test_a_view_tracks_its_base_under_sustained_ingest_with_scans(checkpoint_server):
    """Sustained ingest in sub-tick batches crosses many checkpoints without the
    auto-tick firing per batch, so buffered view deltas routinely straddle one,
    while a second connection scans the view throughout — each scan draining
    pending ticks. Neither thread may hang and the view must still equal its base.
    """
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as scanner:
        sn, tid, schema = _setup(pusher)
        pusher.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name=sn)
        vid, _ = pusher.resolve_table(sn, "v")

        n_batches, batch_size = 40, 300   # 300 < TICK_COALESCE_ROWS: no auto-tick
        errors = []
        started = threading.Event()

        def scan_loop():
            started.wait(timeout=START_TIMEOUT)
            try:
                for _ in range(30):
                    scanner.scan(vid)
            except Exception as exc:
                errors.append(("scan", exc))

        _run(errors,
             threading.Thread(name="push", daemon=True, target=_push_loop,
                              args=(pusher, tid, schema, errors, started),
                              kwargs={"n_batches": n_batches, "batch_size": batch_size}),
             threading.Thread(name="scan", daemon=True, target=scan_loop))

        want = {(i, i): 1 for i in range(n_batches * batch_size)}
        assert bag(scanned(pusher, sn, "t"), "pk", "val") == want
        assert bag(scanned(pusher, sn, "v"), "pk", "val") == want


@pytest.mark.parametrize("probe", ["seek", "scan"])
def test_a_fanout_read_does_not_hang_during_a_checkpoint(probe, checkpoint_server):
    """SEEK (single_worker_async) and SCAN (dispatch_fanout) each write their own
    SAL group. Writing it without holding the SAL writer lets a request that
    arrives in the Flush ACK-wait window use the old epoch, which every worker
    skips — and the read never returns."""
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as reader:
        sn, filler, schema = _setup(pusher, "filler")
        pusher.execute_sql(
            "CREATE TABLE target (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn)
        pusher.execute_sql("INSERT INTO target VALUES (1, 10), (42, 999)",
                           schema_name=sn)
        target, _ = pusher.resolve_table(sn, "target")

        errors = []
        started = threading.Event()

        def read_loop():
            started.wait(timeout=START_TIMEOUT)
            try:
                for _ in range(40):
                    if probe == "seek":
                        assert bag(reader.seek(target, pk=42), "pk", "val") == \
                            {(42, 999): 1}
                    else:
                        assert bag(reader.scan(target), "pk", "val") == \
                            {(1, 10): 1, (42, 999): 1}
            except Exception as exc:
                errors.append((probe, exc))

        _run(errors,
             threading.Thread(name="push", daemon=True, target=_push_loop,
                              args=(pusher, filler, schema, errors, started)),
             threading.Thread(name=probe, daemon=True, target=read_loop))


def test_a_unique_constraint_holds_under_checkpoint_pressure(checkpoint_server):
    """The unique filter must be updated exactly once per durably committed
    batch, and no checkpoint may reset it. Inserts against a unique indexed column
    run while the SAL floods; afterwards every row must be visible and a duplicate
    must still be rejected — a lost or reset filter accepts it."""
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as inserter:
        sn, filler, schema = _setup(pusher, "filler")
        inserter.execute_sql(
            "CREATE TABLE unique_t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn)
        inserter.execute_sql("CREATE UNIQUE INDEX ON unique_t(val)", schema_name=sn)

        errors = []
        started = threading.Event()
        n_rows = 50

        def insert_loop():
            started.wait(timeout=START_TIMEOUT)
            try:
                for i in range(n_rows):
                    inserter.execute_sql(f"INSERT INTO unique_t VALUES ({i}, {i * 10})",
                                         schema_name=sn)
            except Exception as exc:
                errors.append(("insert", exc))

        _run(errors,
             threading.Thread(name="push", daemon=True, target=_push_loop,
                              args=(pusher, filler, schema, errors, started)),
             threading.Thread(name="insert", daemon=True, target=insert_loop))

        assert bag(scanned(inserter, sn, "unique_t"), "pk", "val") == {
            (i, i * 10): 1 for i in range(n_rows)}
        with pytest.raises(gnitz.GnitzError):
            inserter.execute_sql(f"INSERT INTO unique_t VALUES ({n_rows + 1}, 0)",
                                 schema_name=sn)


def test_a_view_created_over_committed_data_survives_a_checkpoint_window(checkpoint_client):
    """Enough rows to cross the checkpoint threshold — draining pending_deltas —
    and then a CREATE of an exchange GROUP BY view whose only driver is the
    committed store. A flushed-snapshot under-count and a
    checkpoint-strands-exchange regression both surface as a wrong result.
    `checkpoint_server` also forces W >= 2; at one worker there is no exchange."""
    conn = checkpoint_client
    conn.create_schema("ckv")
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "n BIGINT NOT NULL)", schema_name="ckv")
    n, chunk = 2000, 1000
    for base in range(0, n, chunk):
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(
                f"({i}, {i % 10}, {i})" for i in range(base, base + chunk)),
            schema_name="ckv")
    conn.execute_sql("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g",
                     schema_name="ckv")
    vid, _ = conn.resolve_table("ckv", "v")
    assert bag(conn.scan(vid), "g", "c") == {(g, n // 10): 1 for g in range(10)}


def test_a_low_space_relay_reclaims_without_aborting_the_master(relay_lowspace_server):
    """When SAL space runs low before an exchange relay, `relay_loop` fires a
    committer barrier to reclaim via a checkpoint, rechecks, and fatally aborts
    the master if space is still low. A barrier-only committer batch that
    short-circuits without checkpointing never bumps the epoch, so the recheck
    still reads low and the master aborts — an availability bug, since workers
    self-exit via getppid().

    The GROUP BY forces an exchange, and a few hundred rows from one serial
    client stay well under TICK_COALESCE_ROWS, so the tick fires from the idle
    timer with no push in flight: the reclaim barrier reaches the committer as a
    barrier-only batch, which is exactly the path that used to abort."""
    target, proc = relay_lowspace_server
    with gnitz.connect(target) as client:
        sn = "rls"
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
            "val BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn)
        vid, _ = client.resolve_table(sn, "v")

        n = 300
        client.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({i}, 1, {i})" for i in range(1, n + 1)),
            schema_name=sn)
        expected = {(1, sum(range(1, n + 1))): 1}

        # The exchange completes only once the relay is delivered, which requires
        # the low-space barrier to have reclaimed via a checkpoint. Poll rather
        # than read once: the injected relay failure is what the recovery here has
        # to work around, so the first drain may legitimately not carry it.
        got = None
        deadline = time.time() + 30
        while time.time() < deadline and proc.poll() is None:
            got = bag(client.scan(vid), "grp", "total")
            if got == expected:
                break
            time.sleep(0.1)

        assert proc.poll() is None, (
            "master aborted on a low-space exchange relay — the barrier-only "
            "committer batch never checkpointed to reclaim SAL space")
        assert got == expected, f"view never converged: {got} != {expected}"
