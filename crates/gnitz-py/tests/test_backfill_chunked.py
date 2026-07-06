"""Chunked distributed view backfill (boot path).

A distributed CREATE-VIEW backfill streams each worker's source partition
through the incremental plan one `ddl_scan_chunk_rows` chunk at a time, issuing
one exchange round per chunk per exchanging view across the cross-worker
barrier. Because partitions are unequal, a worker that has drained its partition
keeps issuing empty *pad* rounds to stay in lockstep until the master — ANDing a
per-chunk pad bit reported by every worker — stamps the collective stop verdict.

These tests drive the **boot rebuild** path (`rebuild_invalid_views` →
`fan_out_backfill` → worker `handle_backfill`): create an exchange view over
populated tables, crash-restart the server (the un-checkpointed view fails the
resume verdict and is rebuilt over the recovered base data), and assert the
rebuilt view is exact. They
run with a shrunk `GNITZ_DDL_SCAN_CHUNK_ROWS` so even small tables span many
chunked rounds — exercising lockstep padding, the pad-bit termination, and (with
the reclaim seam) the per-round SAL checkpoint/reset.

All scenarios force >= 2 workers; the chunked-round barrier only exists in the
distributed plan.
"""

import os
import signal
import subprocess
import tempfile
import time
import shutil

import pytest
import gnitz
from _serverproc import server_preexec, is_debug_build


_BINARY = os.environ.get(
    "GNITZ_SERVER_BIN",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
)
# The chunked-round barrier is distributed-only; force >= 2 workers regardless
# of the ambient GNITZ_WORKERS so a W=1 invocation still tests the real path.
_WORKERS = max(2, int(os.environ.get("GNITZ_WORKERS", "4")))
# Tiny chunk so a handful of rows per worker still produces many chunked rounds
# (unequal partitions => lockstep padding) over a small table.
_CHUNK_ROWS = "3"


def _make_env():
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_bfchunk_")
    return tmpdir, os.path.join(tmpdir, "data"), os.path.join(tmpdir, "gnitz.sock")


def _start(data_dir, sock_path, extra_env):
    if not os.path.isfile(_BINARY):
        pytest.skip(f"Server binary not found: {_BINARY}")
    env = os.environ.copy()
    env["GNITZ_DDL_SCAN_CHUNK_ROWS"] = _CHUNK_ROWS
    env.update(extra_env)
    cmd = [_BINARY, data_dir, sock_path, f"--workers={_WORKERS}"]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, env=env, preexec_fn=server_preexec)
    deadline = time.time() + 10.0
    while time.time() < deadline:
        if os.path.exists(sock_path):
            return proc
        time.sleep(0.05)
    proc.kill()
    proc.communicate()
    raise RuntimeError("server did not start (no socket)")


def _stop(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def _restart(proc, data_dir, sock_path, extra_env):
    """Kill the server and reboot on the SAME data dir — base rows recover from
    the SAL and every ephemeral exchange view re-materializes via the distributed
    chunked boot backfill."""
    _stop(proc)
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    return _start(data_dir, sock_path, extra_env)


def test_groupby_backfill_chunked_across_restart():
    """A GROUP BY (exchange) view over populated tables rebuilds exactly through
    the chunked distributed boot backfill, with unequal per-worker partitions
    forcing pad rounds."""
    tmpdir, data_dir, sock_path = _make_env()
    proc = None
    try:
        proc = _start(data_dir, sock_path, {})
        rows = [(i, i % 6, i * 10) for i in range(1, 41)]
        with gnitz.connect(sock_path) as c:
            c.create_schema("bf")
            c.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, "
                "category BIGINT NOT NULL, amount BIGINT NOT NULL)", schema_name="bf")
            c.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM orders GROUP BY category", schema_name="bf")
            c.execute_sql(
                "INSERT INTO orders VALUES " + ",".join(
                    f"({i},{cat},{amt})" for i, cat, amt in rows), schema_name="bf")

        # Reboot: the ephemeral view is dropped and rebuilt by the chunked
        # distributed backfill over the recovered base partition.
        proc = _restart(proc, data_dir, sock_path, {})

        want = {}
        for _, cat, amt in rows:
            cnt, tot = want.get(cat, (0, 0))
            want[cat] = (cnt + 1, tot + amt)

        with gnitz.connect(sock_path) as c:
            vid = c.resolve_table("bf", "v")[0]
            got = {r["category"]: (r["cnt"], r["total"]) for r in c.scan(vid)}
        assert got == want
    finally:
        if proc is not None:
            _stop(proc)
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_filtered_groupby_backfill_no_early_stop():
    """A WHERE-filtered GROUP BY where a contiguous low-PK range fails the
    predicate on *every* worker's first chunks. The relayed pre-phase payload for
    those rounds is empty (the filter+reindex is pre-relay), so inferring "done"
    from empty relays would stop the backfill early and TRUNCATE the view. The
    worker-reported pad bit tracks the raw drain — not the filtered payload — so
    backfill continues to the later passing rows. Asserts the full, untruncated
    result."""
    tmpdir, data_dir, sock_path = _make_env()
    proc = None
    try:
        proc = _start(data_dir, sock_path, {})
        # ids 1..48 are filtered out (amount <= 1000); only 49..60 pass. Every
        # worker's lowest PKs fall in the filtered range, so its first chunked
        # rounds relay empty — the exact false-stop trap.
        rows = [(i, i % 4, (100 if i <= 48 else 5000)) for i in range(1, 61)]
        with gnitz.connect(sock_path) as c:
            c.create_schema("bf")
            c.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, "
                "category BIGINT NOT NULL, amount BIGINT NOT NULL)", schema_name="bf")
            c.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM orders WHERE amount > 1000 GROUP BY category", schema_name="bf")
            c.execute_sql(
                "INSERT INTO orders VALUES " + ",".join(
                    f"({i},{cat},{amt})" for i, cat, amt in rows), schema_name="bf")

        proc = _restart(proc, data_dir, sock_path, {})

        want = {}
        for _, cat, amt in rows:
            if amt > 1000:
                cnt, tot = want.get(cat, (0, 0))
                want[cat] = (cnt + 1, tot + amt)
        assert want, "test bug: reference is empty"

        with gnitz.connect(sock_path) as c:
            vid = c.resolve_table("bf", "v")[0]
            got = {r["category"]: (r["cnt"], r["total"]) for r in c.scan(vid)}
        assert got == want, f"filtered GROUP BY truncated by false-stop: {got} != {want}"
    finally:
        if proc is not None:
            _stop(proc)
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.skipif(not is_debug_build(), reason="reclaim seam is debug-only")
def test_pure_range_backfill_with_forced_sal_reclaim():
    """Pure range-join (n_eq == 0) broadcast backfill — the worst-case relay — with
    the SAL reclaim seam forcing a per-round checkpoint+reset on every round. The
    workers must re-epoch inline as they consume each relay and the master must
    reset its write side at the next round barrier; if any of that is wrong the
    backfill deadlocks or the view is corrupt. Asserts the rebuilt view equals the
    cross-filter reference, proving reclamation is transparent."""
    tmpdir, data_dir, sock_path = _make_env()
    proc = None
    seam = {"GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW": "1"}
    try:
        proc = _start(data_dir, sock_path, seam)
        a_rows = [(i, (i * 11) % 17) for i in range(1, 16)]
        b_rows = [(i, (i * 13) % 17) for i in range(1, 16)]
        with gnitz.connect(sock_path) as c:
            c.create_schema("bf")
            c.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name="bf")
            c.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name="bf")
            c.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x >= b.y",
                schema_name="bf")
            c.execute_sql("INSERT INTO a VALUES " + ",".join(
                f"({i},{x})" for i, x in a_rows), schema_name="bf")
            c.execute_sql("INSERT INTO b VALUES " + ",".join(
                f"({i},{y})" for i, y in b_rows), schema_name="bf")

        # Reboot with the seam still armed: the boot backfill checkpoints+resets
        # the SAL on every round.
        proc = _restart(proc, data_dir, sock_path, seam)

        want = sorted((a[1], b[1]) for a in a_rows for b in b_rows if a[1] >= b[1])

        with gnitz.connect(sock_path) as c:
            vid = c.resolve_table("bf", "v")[0]
            got = sorted((r["ax"], r["by"]) for r in c.scan(vid) if r.weight > 0)
        assert got == want, "pure-range backfill under forced SAL reclaim mismatched reference"
    finally:
        if proc is not None:
            _stop(proc)
        shutil.rmtree(tmpdir, ignore_errors=True)
