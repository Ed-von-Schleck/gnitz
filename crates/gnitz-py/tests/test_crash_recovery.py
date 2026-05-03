"""
Crash-recovery tests for Design 2 (LSN as the sole atomicity mechanism).

Exercises the new two-pass recovery: a DDL that aborts after broadcasts but
before its commit sentinel is treated as uncommitted on restart, so no
orphan COL_TAB rows survive. Without the sentinel-driven recovery, the
COL_TAB writes that already reached SAL would be replayed at next boot
and a table_id with no matching TABLE_TAB row would persist.

The crash is injected via GNITZ_INJECT_DDL_PANIC=after_broadcasts in a
debug build (executor.rs has #[cfg(debug_assertions)] gate).
"""

import os
import signal
import shutil
import subprocess
import tempfile
import time

import pytest
import gnitz


_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _start_server(data_dir, sock_path, workers=None, extra_env=None,
                  expect_socket=True, timeout_s=10.0):
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if workers:
        cmd += [f"--workers={workers}"]
    env = None
    if extra_env:
        env = os.environ.copy()
        env.update(extra_env)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, env=env,
    )
    if expect_socket:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if os.path.exists(sock_path):
                break
            time.sleep(0.05)
        else:
            proc.kill()
            proc.communicate()
            raise RuntimeError("Server did not start (no socket)")
    return proc


def _stop_server(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def _make_env():
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_crash_",
    )
    return tmpdir, os.path.join(tmpdir, "data"), os.path.join(tmpdir, "gnitz.sock")


def _wait_for_crash(proc, timeout_s=15.0):
    """Wait for the server to abort (libc::abort exits with SIGABRT)."""
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        _stop_server(proc)
        raise RuntimeError("Server did not abort on GNITZ_INJECT_DDL_PANIC")


def _is_debug_build():
    """The injection seam is gated by #[cfg(debug_assertions)]. Cargo's
    default `cargo build` is debug; release builds drop the seam."""
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    # Heuristic: release binaries are stripped/smaller. Use file(1) to
    # check for "not stripped" but this is unreliable. Best signal we
    # have is the GNITZ_RELEASE env that the bench harness sets.
    return os.environ.get("GNITZ_RELEASE", "0") == "0"


def test_crash_after_push_no_tick():
    """SIGKILL after push with no checkpoint: SAL replay must restore all rows.

    Without per-push flush, rows only live in the SAL and memtable at kill
    time.  Recovery re-applies the committed FLAG_PUSH zone so all rows are
    visible after restart.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: create schema and table cleanly. --------------------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("sal_recovery")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="sal_recovery",
        )
        conn.close()
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 2: restart, push rows, kill without checkpoint. --------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="sal_recovery",
        )
        conn.close()
        # Hard kill: no graceful shutdown, no checkpoint.
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 3: restart and verify SAL replay restored all rows. ----
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        tid, _ = conn.resolve_table("sal_recovery", "t")
        rows = list(conn.scan(tid))
        assert len(rows) == 3, f"expected 3 rows after SAL recovery, got {rows}"
        pks = {r["pk"] for r in rows}
        assert pks == {1, 2, 3}, f"wrong PKs after recovery: {pks}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ddl_crash_no_orphan_columns():
    """CREATE TABLE that aborts before its commit sentinel must leave no
    durable trace. Recovery skips the uncommitted zone; the table name is
    free for re-creation."""
    if not _is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_DDL_PANIC seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: create the schema cleanly (no injection). -----------
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)
        conn.create_schema("crash")
        conn.close()
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 2: restart with injection, attempt CREATE TABLE. -------
        # The DDL emits its broadcasts, then libc::abort() fires before
        # commit_zone, leaving the SAL with broadcasts at this LSN but no
        # sentinel.
        proc = _start_server(
            data_dir, sock_path,
            extra_env={"GNITZ_INJECT_DDL_PANIC": "after_broadcasts"},
        )
        conn = gnitz.connect(sock_path)
        # The CREATE TABLE call will hang or error as the server dies
        # mid-request. Catch any exception — the precise error mode
        # depends on whether the abort lands before or after the response
        # was queued.
        try:
            conn.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  val BIGINT NOT NULL)",
                schema_name="crash",
            )
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

        _wait_for_crash(proc)
        assert proc.returncode != 0, (
            f"server should have aborted, got rc={proc.returncode}"
        )
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 3: restart cleanly, verify no orphans. ----------------
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        # The aborted CREATE TABLE must not leave any visible state.
        with pytest.raises(Exception):
            conn.resolve_table("crash", "t")

        # Re-creating with the same name must succeed cleanly. If orphan
        # COL_TAB rows had been replayed for the old (un-committed)
        # table_id, they would either collide or shift column ordering.
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="crash",
        )
        tid, _ = conn.resolve_table("crash", "t")

        # Functional sanity check: insert + scan after the recovered table.
        conn.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name="crash")
        rows = list(conn.scan(tid))
        assert len(rows) == 1
        assert rows[0]["val"] == 100

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ddl_crash_with_workers_no_orphans():
    """Same as test_ddl_crash_no_orphan_columns but multi-worker. Each
    worker's recovery walk independently rejects the uncommitted zone."""
    if not _is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_DDL_PANIC seam)")
    if _NUM_WORKERS < 2:
        pytest.skip("requires GNITZ_WORKERS >= 2")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("crash_mw")
        conn.close()
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_DDL_PANIC": "after_broadcasts"},
        )
        conn = gnitz.connect(sock_path)
        try:
            conn.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  val BIGINT NOT NULL)",
                schema_name="crash_mw",
            )
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        _wait_for_crash(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        with pytest.raises(Exception):
            conn.resolve_table("crash_mw", "t")

        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="crash_mw",
        )
        tid, _ = conn.resolve_table("crash_mw", "t")
        conn.execute_sql("INSERT INTO t VALUES (1, 100), (2, 200)",
                         schema_name="crash_mw")
        rows = list(conn.scan(tid))
        assert len(rows) == 2

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
