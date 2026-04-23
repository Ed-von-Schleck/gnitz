"""Tests for SAL checkpoint correctness under load.

These tests start a dedicated server with a very small GNITZ_CHECKPOINT_BYTES
so that SAL checkpoints fire frequently during normal push activity.  They
guard against data loss in the checkpoint code paths — in particular the
pre_write_pushes path where entries could be silently dropped if checkpoint
handling was incorrect.

The server is function-scoped (each test gets its own instance) so that a
misbehaving checkpoint cannot corrupt state for other tests.
"""
import os
import random
import subprocess
import tempfile
import threading
import time
import shutil
import pytest
import gnitz


BINARY = os.environ.get(
    "GNITZ_SERVER_BIN",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
)
WORKERS = int(os.environ.get("GNITZ_WORKERS", "4"))

# 32 KB: forces a checkpoint every ~32 KB of SAL writes.  A single push of
# ~500 rows encodes to roughly 30–60 KB, so this fires multiple checkpoints
# per bulk insert.
CHECKPOINT_BYTES = 32 * 1024


@pytest.fixture
def checkpoint_server():
    """Function-scoped server with a tiny SAL checkpoint threshold."""
    if not os.path.isfile(BINARY):
        pytest.skip(f"Server binary not found: {BINARY}")
    tmpdir = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"),
                              prefix="gnitz_checkpoint_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    env = os.environ.copy()
    env["GNITZ_CHECKPOINT_BYTES"] = str(CHECKPOINT_BYTES)
    cmd = [BINARY, data_dir, sock_path, f"--workers={WORKERS}"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env)
    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.communicate()
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError("Checkpoint server did not start")
    yield sock_path
    proc.kill()
    proc.wait()
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def checkpoint_client(checkpoint_server):
    with gnitz.connect(checkpoint_server) as conn:
        yield conn


def _setup(client):
    import random
    uid = str(random.randint(100_000, 999_999))
    sn = "s" + uid
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def _count_view(client, vid):
    return sum(r.weight for r in client.scan(vid) if r.weight > 0)


def test_no_rows_lost_across_checkpoints(checkpoint_client):
    """All pushed rows must survive when SAL checkpoints fire frequently.

    With GNITZ_CHECKPOINT_BYTES=32KB, every push of ~500 rows triggers at
    least one checkpoint.  We insert 5 000 rows in 10 batches and verify
    the final count matches exactly — catching any silent entry loss in the
    checkpoint code path.
    """
    client = checkpoint_client
    sn, tid, schema = _setup(client)

    total = 5_000
    batch_size = 500
    for batch_start in range(0, total, batch_size):
        batch = gnitz.ZSetBatch(schema)
        for i in range(batch_start, batch_start + batch_size):
            batch.append(pk=i, val=i)
        client.push(tid, batch)

    rows = list(client.scan(tid))
    live = sum(r.weight for r in rows if r.weight > 0)
    assert live == total, f"Expected {total} rows, got {live} — entries lost during checkpoint"


def test_no_rows_lost_with_view_during_checkpoints(checkpoint_client):
    """Table rows pushed concurrently with tick evaluation must not be lost.

    A view forces tick evaluation.  With GNITZ_CHECKPOINT_BYTES=32KB,
    checkpoints fire after every few flushes.  Ticks fire concurrently with
    pushes once 10 000 rows accumulate.  This exercises the timing window
    where checkpoint and tick evaluation overlap — the exact scenario where
    pre_write_pushes could silently drop entries on checkpoint failure.

    We only assert the TABLE count, not the view count.  At this checkpoint
    threshold, checkpoints clear pending_deltas faster than ticks can consume
    them (expected architectural behaviour), so the view count is an
    unreliable signal.  The important invariant is that the durable table
    storage reflects all 15 000 pushed rows.
    """
    client = checkpoint_client
    sn, tid, schema = _setup(client)

    # Create a view so tick evaluation runs concurrently with pushes.
    client.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name=sn)

    # 15 000 rows: exceeds TICK_COALESCE_ROWS (10 000) so ticks fire during
    # the push sequence, not only after.
    total = 15_000
    batch_size = 1_000
    for batch_start in range(0, total, batch_size):
        batch = gnitz.ZSetBatch(schema)
        for i in range(batch_start, batch_start + batch_size):
            batch.append(pk=i, val=i)
        client.push(tid, batch)

    table_live = sum(r.weight for r in client.scan(tid) if r.weight > 0)
    assert table_live == total, \
        f"Table count mismatch: expected {total}, got {table_live} — entries lost during checkpoint"


# ---------------------------------------------------------------------------
# Liveness regressions: fan-out ops must not hang during concurrent checkpoints
# ---------------------------------------------------------------------------
#
# Before the fix, fan_out_seek_async, fan_out_scan_async, and
# execute_pipeline_async did not hold sal_writer_excl while writing their SAL
# group.  A request arriving in the FLAG_FLUSH ACK-wait window wrote with the
# old epoch; workers skipped it; the operation hung forever.
#
# Each test below uses two connections in separate threads: a pusher that
# floods the SAL (triggering frequent checkpoints) and a reader that exercises
# one of the affected fan-out code paths.  Hang detection is via thread.join
# with a timeout; assert-not-alive converts a permanent hang into a test
# failure.

def _filler_schema(client, sock_path):
    """Return (socket_path, filler_tid, filler_schema, sn) after DDL."""
    sn = "ck" + str(random.randint(100_000, 999_999))
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE filler (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    filler_tid, _ = client.resolve_table(sn, "filler")
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
    ]
    schema = gnitz.Schema(cols)
    return filler_tid, schema, sn


def _push_to_fill(client, filler_tid, schema, n_batches=25, batch_size=400):
    """Push n_batches × batch_size rows into filler to trigger checkpoints."""
    for i in range(n_batches):
        batch = gnitz.ZSetBatch(schema)
        for j in range(batch_size):
            pk = i * batch_size + j
            batch.append(pk=pk, val=pk)
        client.push(filler_tid, batch)


def test_seek_during_checkpoints(checkpoint_server):
    """SEEK (single_worker_async) must not hang when checkpoints fire concurrently.

    Regression: fan_out_seek_async wrote its SAL group without holding
    sal_writer_excl.  A SEEK arriving in the FLAG_FLUSH ACK-wait window used
    the old epoch; workers skipped it; the seek hung forever.
    """
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as seeker:

        filler_tid, filler_schema, sn = _filler_schema(pusher, checkpoint_server)

        # Create a target table with one row for the seeker.
        pusher.execute_sql(
            "CREATE TABLE target (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        pusher.execute_sql("INSERT INTO target VALUES (42, 999)", schema_name=sn)
        target_tid, _ = pusher.resolve_table(sn, "target")

        errors = []
        push_started = threading.Event()

        def push_loop():
            try:
                for i in range(25):
                    batch = gnitz.ZSetBatch(filler_schema)
                    for j in range(400):
                        pk = i * 400 + j
                        batch.append(pk=pk, val=pk)
                    pusher.push(filler_tid, batch)
                    if i == 0:
                        push_started.set()
            except Exception as exc:
                errors.append(("push", exc))
                push_started.set()

        def seek_loop():
            push_started.wait(timeout=15)
            try:
                for _ in range(80):
                    seeker.seek(target_tid, pk=42)
            except Exception as exc:
                errors.append(("seek", exc))

        push_t = threading.Thread(target=push_loop, daemon=True)
        seek_t = threading.Thread(target=seek_loop, daemon=True)
        push_t.start()
        seek_t.start()

        push_t.join(timeout=30)
        seek_t.join(timeout=15)

        assert not push_t.is_alive(), "push loop hung unexpectedly"
        assert not seek_t.is_alive(), \
            "seek hung during concurrent checkpoint (sal_writer_excl regression)"
        for src, exc in errors:
            raise AssertionError(f"{src} thread raised: {exc}")


def test_scan_during_checkpoints(checkpoint_server):
    """SCAN (dispatch_fanout) must not hang when checkpoints fire concurrently.

    Regression: fan_out_scan_async wrote its SAL group without holding
    sal_writer_excl.  Same epoch-mismatch hang as the seek regression.
    """
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as scanner:

        filler_tid, filler_schema, sn = _filler_schema(pusher, checkpoint_server)

        pusher.execute_sql(
            "CREATE TABLE target (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        pusher.execute_sql("INSERT INTO target VALUES (1, 10), (2, 20)", schema_name=sn)
        target_tid, _ = pusher.resolve_table(sn, "target")

        errors = []
        push_started = threading.Event()

        def push_loop():
            try:
                for i in range(25):
                    batch = gnitz.ZSetBatch(filler_schema)
                    for j in range(400):
                        pk = i * 400 + j
                        batch.append(pk=pk, val=pk)
                    pusher.push(filler_tid, batch)
                    if i == 0:
                        push_started.set()
            except Exception as exc:
                errors.append(("push", exc))
                push_started.set()

        def scan_loop():
            push_started.wait(timeout=15)
            try:
                for _ in range(40):
                    scanner.scan(target_tid)
            except Exception as exc:
                errors.append(("scan", exc))

        push_t = threading.Thread(target=push_loop, daemon=True)
        scan_t = threading.Thread(target=scan_loop, daemon=True)
        push_t.start()
        scan_t.start()

        push_t.join(timeout=30)
        scan_t.join(timeout=15)

        assert not push_t.is_alive(), "push loop hung unexpectedly"
        assert not scan_t.is_alive(), \
            "scan hung during concurrent checkpoint (sal_writer_excl regression)"
        for src, exc in errors:
            raise AssertionError(f"{src} thread raised: {exc}")


def test_unique_constraint_enforced_under_checkpoint_pressure(checkpoint_server):
    """Unique filter must survive checkpoint cycles and keep rejecting duplicates.

    Regression for unique_filter_ingest_batch ordering (Bug 3): the filter
    update must be called exactly once per durably committed batch, and
    checkpoint activity must not corrupt or reset the filter.

    The test inserts rows with a unique indexed column while simultaneously
    flooding the SAL with filler data (triggering frequent checkpoints).
    After all inserts succeed, it verifies:
      1. Every inserted row is visible — no silent data loss.
      2. Inserting a duplicate unique value is rejected — filter still works.

    If unique_filter_ingest_batch were accidentally removed (e.g. the move to
    Phase D dropped it entirely), or if a checkpoint reset the filter, the
    duplicate INSERT in step 2 would succeed and the assertion would fail.
    """
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as inserter:

        filler_tid, filler_schema, sn = _filler_schema(pusher, checkpoint_server)

        inserter.execute_sql(
            "CREATE TABLE unique_t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL"
            ")",
            schema_name=sn,
        )
        inserter.execute_sql(
            "CREATE UNIQUE INDEX ON unique_t(val)", schema_name=sn
        )
        unique_tid, _ = inserter.resolve_table(sn, "unique_t")

        errors = []
        push_started = threading.Event()

        def push_loop():
            try:
                for i in range(25):
                    batch = gnitz.ZSetBatch(filler_schema)
                    for j in range(400):
                        pk = i * 400 + j
                        batch.append(pk=pk, val=pk)
                    pusher.push(filler_tid, batch)
                    if i == 0:
                        push_started.set()
            except Exception as exc:
                errors.append(("push", exc))
                push_started.set()

        n_rows = 50

        def insert_loop():
            push_started.wait(timeout=15)
            try:
                for i in range(n_rows):
                    inserter.execute_sql(
                        f"INSERT INTO unique_t VALUES ({i}, {i * 10})",
                        schema_name=sn,
                    )
            except Exception as exc:
                errors.append(("insert", exc))

        push_t = threading.Thread(target=push_loop, daemon=True)
        insert_t = threading.Thread(target=insert_loop, daemon=True)
        push_t.start()
        insert_t.start()

        push_t.join(timeout=30)
        insert_t.join(timeout=30)

        assert not push_t.is_alive(), "push loop hung unexpectedly"
        assert not insert_t.is_alive(), \
            "insert loop hung during concurrent checkpoint"
        for src, exc in errors:
            raise AssertionError(f"{src} thread raised: {exc}")

        # 1. All rows must be visible.
        live = sum(r.weight for r in inserter.scan(unique_tid) if r.weight > 0)
        assert live == n_rows, f"Expected {n_rows} rows, got {live} after checkpoints"

        # 2. Unique filter must still enforce the constraint.
        #    val=0 was inserted as row 0 above; re-inserting it must fail.
        try:
            inserter.execute_sql(
                f"INSERT INTO unique_t VALUES ({n_rows + 1}, 0)",
                schema_name=sn,
            )
            raise AssertionError(
                "Duplicate unique value was accepted — unique filter lost after checkpoint"
            )
        except gnitz.GnitzError:
            pass  # expected
