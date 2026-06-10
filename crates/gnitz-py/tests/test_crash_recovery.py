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
from _serverproc import server_preexec


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
        start_new_session=True, env=env, preexec_fn=server_preexec,
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


def _restart_server(proc, data_dir, sock_path, **kwargs):
    """SIGKILL the running server, clear the stale socket, and start a fresh
    server on the same data dir — the crash-then-recover restart every test
    performs. Extra kwargs (e.g. workers) pass through to _start_server."""
    _stop_server(proc)
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    return _start_server(data_dir, sock_path, **kwargs)


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

        # ---- Phase 2: restart, push rows, kill without checkpoint. --------
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="sal_recovery",
        )
        conn.close()

        # ---- Phase 3: restart and verify SAL replay restored all rows. ----
        # Hard kill: no graceful shutdown, no checkpoint.
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
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

        # ---- Phase 2: restart with injection, attempt CREATE TABLE. -------
        # The DDL emits its broadcasts, then libc::abort() fires before
        # commit_zone, leaving the SAL with broadcasts at this LSN but no
        # sentinel.
        proc = _restart_server(
            proc, data_dir, sock_path,
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

        proc = _restart_server(
            proc, data_dir, sock_path, workers=_NUM_WORKERS,
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


def _index_on_col(conn, owner_tid, col_idx):
    """True if a live IdxTab row indexes column `col_idx` of table `owner_tid`."""
    from gnitz.core import IDX_TAB
    _, batch_obj, _ = conn._client.scan(IDX_TAB)
    if batch_obj is None:
        return False
    for i in range(len(batch_obj.pks)):
        if batch_obj.weights[i] <= 0:
            continue
        if (batch_obj.columns[1][i] == owner_tid
                and batch_obj.columns[3][i] == col_idx):
            return True
    return False


def test_create_index_after_fk_tables_survives_crash():
    """An fsync-acknowledged CREATE INDEX must survive a crash that happens
    before the next checkpoint.

    FK auto-indices are applied via un-pinned local ingests that bump
    sys_indices' current_lsn once per non-PK FK column, while the CREATE TABLE
    consumes a single zone LSN — so the IDX_TAB recovery watermark drifts ahead
    of the zone allocator. A checkpoint persists the drifted counter, and a
    later CREATE INDEX whose zone LSN sits at or below it is deduped away by
    recovery's `msg.lsn <= flushed` check: the index vanishes despite the ACK.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        # Tiny checkpoint threshold: the DDL broadcasts below push the SAL
        # cursor far past 1 KB, so the first INSERT's committer cycle runs a
        # checkpoint — and completes it before acking the push.
        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_CHECKPOINT_BYTES": "1024"},
        )
        conn = gnitz.connect(sock_path)
        conn.create_schema("idxcrash")
        conn.execute_sql(
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="idxcrash",
        )
        # Three child tables x four non-PK FK columns: each CREATE TABLE
        # consumes one zone LSN but bumps sys_indices' counter by four,
        # drifting the IDX_TAB watermark well past the zone allocator.
        for t in ("c1", "c2", "c3"):
            conn.execute_sql(
                f"CREATE TABLE {t} ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  f1 BIGINT NOT NULL REFERENCES p(id),"
                "  f2 BIGINT NOT NULL REFERENCES p(id),"
                "  f3 BIGINT NOT NULL REFERENCES p(id),"
                "  f4 BIGINT NOT NULL REFERENCES p(id),"
                "  val BIGINT NOT NULL)",
                schema_name="idxcrash",
            )
        # Committer checkpoint fires before this push is acked, persisting
        # the drifted IDX_TAB watermark.
        conn.execute_sql("INSERT INTO p VALUES (1, 100)", schema_name="idxcrash")

        # The acknowledged DDL under test.
        conn.execute_sql("CREATE INDEX ON c1(val)", schema_name="idxcrash")
        c1_tid, _ = conn.resolve_table("idxcrash", "c1")
        VAL_COL = 5  # cid, f1..f4, val
        assert _index_on_col(conn, c1_tid, VAL_COL), "index missing pre-crash"
        conn.close()

        # SIGKILL before any further checkpoint: the CREATE INDEX lives only
        # in the SAL, guarded by the watermark dedup.
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        c1_tid2, _ = conn.resolve_table("idxcrash", "c1")
        assert c1_tid2 == c1_tid
        assert _index_on_col(conn, c1_tid2, VAL_COL), \
            "acknowledged CREATE INDEX lost after crash recovery"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Orphan table/view/index directory reclamation on boot (gc_orphan_directories)
# ---------------------------------------------------------------------------
#
# A DROP's on-disk directory is leaked permanently if the node crashes between
# the commit and the next checkpoint: the deferred-deletion queue is in-memory
# only. Recovery's boot-time sweep (gc_orphan_directories) reclaims it. These
# tests crash the server with SIGKILL before any checkpoint, so the DROPs live
# only in the SAL, and verify the directory is reclaimed on restart while live
# entities (including a SAL-only, not-yet-flushed CREATE) survive intact.


def test_orphan_drop_reclaimed_unflushed_create_survives():
    """A dropped table's directory is reclaimed on recovery while a SAL-only
    CREATE survives — guards that gc_orphan_directories runs *after* SAL replay.

    A sweep that ran before SAL replay would see table B absent from dag.tables
    (its CREATE is committed to the SAL but not yet flushed) and delete its live
    on-disk directory; SAL replay would then re-register B pointing at a missing
    dir. This test fails on that ordering regression.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("gc")
        conn.execute_sql(
            "CREATE TABLE a ("
            "  pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="gc",
        )
        a_tid, _ = conn.resolve_table("gc", "a")

        # Drop A: its directory is moved to the in-memory checkpoint-gated queue
        # (still on disk, not yet removed). Then create B. Both DDLs live only in
        # the SAL because the SIGKILL below precedes any checkpoint.
        conn.drop_table("gc", "a")
        conn.execute_sql(
            "CREATE TABLE b ("
            "  pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="gc",
        )
        b_tid, _ = conn.resolve_table("gc", "b")

        a_dir = os.path.join(data_dir, "gc", f"a_{a_tid}")
        b_dir = os.path.join(data_dir, "gc", f"b_{b_tid}")
        assert os.path.isdir(a_dir), "dropped A's dir is gated (still on disk) pre-crash"
        assert os.path.isdir(b_dir), "B's dir exists pre-crash"

        conn.close()

        # Restart: recovery replays the SAL DDL, then gc_orphan_directories runs.
        # SIGKILL: no graceful shutdown, no checkpoint.
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        # B survived, keeps its directory, and is fully queryable.
        b_tid2, _ = conn.resolve_table("gc", "b")
        assert b_tid2 == b_tid
        assert os.path.isdir(b_dir), "B's SAL-only-created dir must survive recovery"
        conn.execute_sql("INSERT INTO b VALUES (1, 100)", schema_name="gc")
        rows = list(conn.scan(b_tid2))
        assert len(rows) == 1 and rows[0]["v"] == 100

        # A's directory is reclaimed and A is gone.
        assert not os.path.exists(a_dir), "dropped A's dir must be reclaimed on boot"
        with pytest.raises(Exception):
            conn.resolve_table("gc", "a")

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_dropped_schema_subtree_reclaimed():
    """A DROP SCHEMA CASCADE that lives only in the SAL at crash time has its
    whole subtree reclaimed on recovery.

    The schema-scoped scan cannot reach the subtree (the schema is gone from
    schema_by_id), so reclamation depends on the drain of the queue that SAL
    replay re-populated.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("doomed")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="doomed",
        )
        t_tid, _ = conn.resolve_table("doomed", "t")
        schema_dir = os.path.join(data_dir, "doomed")
        table_dir = os.path.join(schema_dir, f"t_{t_tid}")
        assert os.path.isdir(table_dir)

        # DROP SCHEMA CASCADE; SIGKILL before any checkpoint (SAL-only).
        conn.drop_schema("doomed")
        conn.close()

        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        with pytest.raises(Exception):
            conn.resolve_table("doomed", "t")
        assert not os.path.exists(table_dir), "dropped schema's table dir must be gone"
        assert not os.path.exists(schema_dir), "dropped schema dir must be gone"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_recreated_schema_survives_recovery():
    """DROP SCHEMA s + CREATE SCHEMA s (with a fresh table), both SAL-only at
    crash time. The replayed DROP's residue must not wipe the recreated live
    schema.

    A schema's on-disk path is name-based (`<base>/s`), so the replayed DROP and
    CREATE land in the same recovery deletion queue. The cancel_gated_deletion
    fix (which clears pending_dir_deletions, not just the gated queue) removes
    the DROP's residue when the CREATE re-fires its hook, so the boot-time drain
    leaves the live schema alone. Without that fix the drain removes `<base>/s`
    recursively, erasing the recreated schema and its table.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("reborn")
        conn.execute_sql(
            "CREATE TABLE old ("
            "  pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="reborn",
        )

        # Drop then immediately recreate the same-name schema with a fresh table.
        conn.drop_schema("reborn")
        conn.create_schema("reborn")
        conn.execute_sql(
            "CREATE TABLE fresh ("
            "  pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="reborn",
        )
        fresh_tid, _ = conn.resolve_table("reborn", "fresh")
        conn.execute_sql("INSERT INTO fresh VALUES (1, 11), (2, 22)",
                         schema_name="reborn")
        schema_dir = os.path.join(data_dir, "reborn")
        fresh_dir = os.path.join(schema_dir, f"fresh_{fresh_tid}")
        assert os.path.isdir(fresh_dir)

        conn.close()

        # SIGKILL before any checkpoint: both DDLs SAL-only.
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        # The recreated schema and its table survive recovery + boot-time drain.
        fresh_tid2, _ = conn.resolve_table("reborn", "fresh")
        rows = list(conn.scan(fresh_tid2))
        assert len(rows) == 2, f"recreated schema's table lost rows: {rows}"
        assert os.path.isdir(schema_dir), "recreated schema dir must survive"
        assert os.path.isdir(fresh_dir), "recreated table dir must survive"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
