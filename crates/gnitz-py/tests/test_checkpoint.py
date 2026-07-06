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
from _serverproc import server_preexec, HANG_TIMEOUT, START_TIMEOUT


BINARY = os.environ.get(
    "GNITZ_SERVER_BIN",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
)
WORKERS = int(os.environ.get("GNITZ_WORKERS", "4"))

# 32 KB: forces a checkpoint every ~32 KB of SAL writes.  A single push of
# ~500 rows encodes to roughly 30–60 KB, so this fires multiple checkpoints
# per bulk insert.
CHECKPOINT_BYTES = 32 * 1024

# Hang ceilings for the concurrent push/scan/seek/insert tests below — see the
# rationale on the shared constants in _serverproc.py.


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
                            stderr=subprocess.PIPE, env=env,
                            preexec_fn=server_preexec)
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
    uid = str(random.randint(100_000, 999_999))
    sn = "s" + uid
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def _count_live(client, tid):
    """Net live row count of a table or view."""
    return sum(r.weight for r in client.scan(tid) if r.weight > 0)


def _filler_schema(client, table_name="filler"):
    """Create a fresh schema + BIGINT (pk, val) table; return (tid, schema, sn)."""
    sn = "ck" + str(random.randint(100_000, 999_999))
    client.create_schema(sn)
    client.execute_sql(
        f"CREATE TABLE {table_name} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    tid, _ = client.resolve_table(sn, table_name)
    cols = [
        # BIGINT maps to I64 (native PK types: planner no longer coerces).
        gnitz.ColumnDef("pk", gnitz.TypeCode.I64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
    ]
    schema = gnitz.Schema(cols)
    return tid, schema, sn


def _push_loop(client, tid, schema, errors, started, n_batches=25, batch_size=400):
    """Flood `tid` with n_batches × batch_size rows to trigger checkpoints.

    Sets `started` after the first push (and on failure, so waiters wake) and
    records any exception in `errors`.
    """
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

    live = _count_live(client, tid)
    assert live == total, f"Expected {total} rows, got {live} — entries lost during checkpoint"


def test_view_matches_base_across_checkpoints(checkpoint_client):
    """A view must converge to its base table across frequent checkpoints.

    A view forces tick evaluation.  With GNITZ_CHECKPOINT_BYTES=32KB,
    checkpoints fire after every few flushes; with 15 000 rows the auto-tick
    (10 000 rows) fires mid-sequence too — so a checkpoint lands between
    committed-but-unticked view deltas and the tick that would apply them.

    Regression: if a checkpoint's flush discards the workers' buffered
    effective deltas (pending_deltas), the base table keeps the rows but no
    later tick ever applies them to the view, which stays permanently diverged
    until a restart rebuilds it.  Once the scan barrier drains the pending
    ticks, BOTH the durable table AND the derived view must reflect all
    15 000 rows.
    """
    client = checkpoint_client
    sn, tid, schema = _setup(client)

    # Create a view so tick evaluation runs concurrently with pushes.
    client.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name=sn)
    vid, _ = client.resolve_table(sn, "v")

    # 15 000 rows: exceeds TICK_COALESCE_ROWS (10 000) so ticks fire during
    # the push sequence, not only after.
    total = 15_000
    batch_size = 1_000
    for batch_start in range(0, total, batch_size):
        batch = gnitz.ZSetBatch(schema)
        for i in range(batch_start, batch_start + batch_size):
            batch.append(pk=i, val=i)
        client.push(tid, batch)

    table_live = _count_live(client, tid)
    assert table_live == total, \
        f"Table count mismatch: expected {total}, got {table_live} — entries lost during checkpoint"
    # The scan barrier drains any still-pending ticks before reading, so the
    # view must now equal the base — no delta discarded by a checkpoint.
    view_live = _count_live(client, vid)
    assert view_live == total, \
        f"View diverged from base: expected {total}, got {view_live} — a checkpoint dropped buffered view deltas"


def test_views_track_base_under_sustained_ingest_with_scans(checkpoint_server):
    """A view stays equal to its base table across many checkpoints while a
    reader concurrently scans it — no wedge, no divergence.

    Sustained ingest in small (< 10 000-row) batches crosses many 32KB
    checkpoints without the auto-tick firing per batch, so buffered view
    deltas routinely straddle a checkpoint.  A second connection scans the
    view throughout (each scan drains pending ticks via the scan barrier).
    After ingest, both the table and the view must show every row, and neither
    thread may hang.
    """
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as scanner:
        tid, schema, sn = _filler_schema(pusher, table_name="t")
        pusher.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name=sn)
        vid, _ = pusher.resolve_table(sn, "v")

        total = 12_000
        errors = []
        push_started = threading.Event()

        def scan_loop():
            push_started.wait(timeout=START_TIMEOUT)
            try:
                for _ in range(30):
                    # Each scan drains pending ticks and must never hang; the
                    # view weight is monotonic, never exceeding rows pushed.
                    live = _count_live(scanner, vid)
                    assert live <= total, f"view overcounted: {live} > {total}"
            except Exception as exc:
                errors.append(("scan", exc))

        # 300-row batches stay under TICK_COALESCE_ROWS: no per-batch auto-tick.
        push_t = threading.Thread(target=_push_loop, daemon=True,
                                  args=(pusher, tid, schema, errors, push_started),
                                  kwargs={"n_batches": 40, "batch_size": 300})
        scan_t = threading.Thread(target=scan_loop, daemon=True)
        push_t.start()
        scan_t.start()
        push_t.join(timeout=HANG_TIMEOUT)
        scan_t.join(timeout=HANG_TIMEOUT)

        assert not push_t.is_alive(), "push loop hung during concurrent checkpoints"
        assert not scan_t.is_alive(), "scan loop hung during concurrent checkpoints"
        for src, exc in errors:
            raise AssertionError(f"{src} thread raised: {exc}")

        table_live = _count_live(pusher, tid)
        view_live = _count_live(pusher, vid)
        assert table_live == total, f"table: expected {total}, got {table_live}"
        assert view_live == total, \
            f"view diverged from base after sustained checkpointed ingest: expected {total}, got {view_live}"


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

def test_seek_during_checkpoints(checkpoint_server):
    """SEEK (single_worker_async) must not hang when checkpoints fire concurrently.

    Regression: fan_out_seek_async wrote its SAL group without holding
    sal_writer_excl.  A SEEK arriving in the FLAG_FLUSH ACK-wait window used
    the old epoch; workers skipped it; the seek hung forever.
    """
    with gnitz.connect(checkpoint_server) as pusher, \
         gnitz.connect(checkpoint_server) as seeker:

        filler_tid, filler_schema, sn = _filler_schema(pusher)

        # Create a target table with one row for the seeker.
        pusher.execute_sql(
            "CREATE TABLE target (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        pusher.execute_sql("INSERT INTO target VALUES (42, 999)", schema_name=sn)
        target_tid, _ = pusher.resolve_table(sn, "target")

        errors = []
        push_started = threading.Event()

        def seek_loop():
            push_started.wait(timeout=START_TIMEOUT)
            try:
                for _ in range(80):
                    seeker.seek(target_tid, pk=42)
            except Exception as exc:
                errors.append(("seek", exc))

        push_t = threading.Thread(target=_push_loop, daemon=True,
                                  args=(pusher, filler_tid, filler_schema, errors, push_started))
        seek_t = threading.Thread(target=seek_loop, daemon=True)
        push_t.start()
        seek_t.start()

        push_t.join(timeout=HANG_TIMEOUT)
        seek_t.join(timeout=HANG_TIMEOUT)

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

        filler_tid, filler_schema, sn = _filler_schema(pusher)

        pusher.execute_sql(
            "CREATE TABLE target (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        pusher.execute_sql("INSERT INTO target VALUES (1, 10), (2, 20)", schema_name=sn)
        target_tid, _ = pusher.resolve_table(sn, "target")

        errors = []
        push_started = threading.Event()

        def scan_loop():
            push_started.wait(timeout=START_TIMEOUT)
            try:
                for _ in range(40):
                    scanner.scan(target_tid)
            except Exception as exc:
                errors.append(("scan", exc))

        push_t = threading.Thread(target=_push_loop, daemon=True,
                                  args=(pusher, filler_tid, filler_schema, errors, push_started))
        scan_t = threading.Thread(target=scan_loop, daemon=True)
        push_t.start()
        scan_t.start()

        push_t.join(timeout=HANG_TIMEOUT)
        scan_t.join(timeout=HANG_TIMEOUT)

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

        filler_tid, filler_schema, sn = _filler_schema(pusher)

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

        n_rows = 50

        def insert_loop():
            push_started.wait(timeout=START_TIMEOUT)
            try:
                for i in range(n_rows):
                    inserter.execute_sql(
                        f"INSERT INTO unique_t VALUES ({i}, {i * 10})",
                        schema_name=sn,
                    )
            except Exception as exc:
                errors.append(("insert", exc))

        push_t = threading.Thread(target=_push_loop, daemon=True,
                                  args=(pusher, filler_tid, filler_schema, errors, push_started))
        insert_t = threading.Thread(target=insert_loop, daemon=True)
        push_t.start()
        insert_t.start()

        push_t.join(timeout=HANG_TIMEOUT)
        insert_t.join(timeout=HANG_TIMEOUT)

        assert not push_t.is_alive(), "push loop hung unexpectedly"
        assert not insert_t.is_alive(), \
            "insert loop hung during concurrent checkpoint"
        for src, exc in errors:
            raise AssertionError(f"{src} thread raised: {exc}")

        # 1. All rows must be visible.
        live = _count_live(inserter, unique_tid)
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


# ---------------------------------------------------------------------------
# Low-space exchange relay must reclaim SAL space without crashing the master
# ---------------------------------------------------------------------------
#
# When SAL space runs low before an exchange relay, relay_loop fires a
# committer barrier to reclaim space via a checkpoint, rechecks, and fatally
# aborts the master if space is still low.  Before the fix the committer's
# barrier-only batch short-circuited without ever checkpointing: with no push
# in flight the reclaim barrier landed in a barrier-only batch, the epoch never
# bumped, relay_loop's recheck still read low, and the master aborted (an
# availability bug — workers self-exit via getppid()).
#
# Driving real exhaustion needs ~7/8 of the 1 GiB SAL mmap, so a debug-only
# seam (GNITZ_INJECT_RELAY_SPACE_LOW) makes the first exchange relay report
# low space one-shot, armed at the current SAL epoch.  Only a checkpoint (which
# bumps the epoch) disarms it, so the green run delivers the relay exactly when
# the committer checkpoints the barrier-only batch.


def test_low_space_relay_checkpoints_without_aborting_master(relay_lowspace_server):
    """A low-SAL-space exchange relay must reclaim space and keep the master
    alive, even when the reclaim barrier lands in a barrier-only batch.

    The GROUP BY view forces an exchange (and thus a relay) when the tick
    fires.  Inserting a few hundred rows from a single serial client — well
    under TICK_COALESCE_ROWS (10 000) — lets the exchange tick fire from the
    idle timer with no push in flight, so relay_loop's reclaim barrier reaches
    the committer as a barrier-only batch: exactly the path that used to abort.
    """
    sock_path, proc = relay_lowspace_server
    client = gnitz.connect(sock_path)
    try:
        sn = "rls_" + str(random.randint(100_000, 999_999))
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        n = 300
        vals = ", ".join(f"({i}, 1, {i})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        expected = sum(range(1, n + 1))

        # Poll the view until the aggregate matches: the exchange can only
        # complete once the relay is delivered, which requires the low-space
        # barrier to have reclaimed SAL space via a checkpoint.
        got = None
        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:
                break  # master died — assertion below reports it
            try:
                totals = {row[1]: row[2]
                          for row in client.scan(vid) if row.weight > 0}
            except Exception:
                break  # connection broke (likely a dead master)
            if totals.get(1) == expected:
                got = totals[1]
                break
            time.sleep(0.1)

        assert proc.poll() is None, (
            "master aborted on a low-space exchange relay — the barrier-only "
            "committer batch never checkpointed to reclaim SAL space"
        )
        assert got == expected, \
            f"view never converged: SUM(val) = {got} != {expected}"
    finally:
        try:
            client.close()
        except Exception:
            pass
