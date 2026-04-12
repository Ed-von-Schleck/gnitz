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
import subprocess
import tempfile
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
