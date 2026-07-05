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
from _serverproc import server_preexec, is_debug_build


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
    if not is_debug_build():
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
    if not is_debug_build():
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


def test_ddl_crash_fk_and_view_no_orphan():
    """A CREATE TABLE child(... REFERENCES parent) and a CREATE VIEW over parent
    that abort after broadcasts but before the commit sentinel must leave no
    durable trace — no orphan COL_TAB/DEP_TAB that would block the parent. The
    pre-fix cross-zone orphan (COL_TAB committed in its own zone before the
    owning TABLE_TAB) is structurally unconstructible once a CREATE is one atomic
    message, so this is the regression proof."""
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_DDL_PANIC seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: clean parent with data. -----------------------------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("crash_fk")
        conn.execute_sql(
            "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
            schema_name="crash_fk",
        )
        conn.execute_sql("INSERT INTO parent VALUES (1), (2), (3)", schema_name="crash_fk")
        conn.close()

        # ---- Phase 2a: crash a CREATE TABLE child with an FK to parent. ----
        proc = _restart_server(
            proc, data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_DDL_PANIC": "after_broadcasts"},
        )
        conn = gnitz.connect(sock_path)
        try:
            conn.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id))",
                schema_name="crash_fk",
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

        # ---- Phase 2b: crash a CREATE VIEW over parent. -------------------
        proc = _restart_server(
            proc, data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_DDL_PANIC": "after_broadcasts"},
        )
        conn = gnitz.connect(sock_path)
        try:
            conn.execute_sql(
                "CREATE VIEW v AS SELECT id FROM parent",
                schema_name="crash_fk",
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

        # ---- Phase 3: restart clean, verify parent is untouched. ----------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        # Neither aborted entity survives.
        with pytest.raises(Exception):
            conn.resolve_table("crash_fk", "child")
        # No phantom FK child blocks a DELETE of the parent's rows...
        conn.execute_sql("DELETE FROM parent WHERE id = 1", schema_name="crash_fk")
        # ...nor an orphan FK child / view dependency blocks DROP TABLE parent.
        conn.execute_sql("DROP TABLE parent", schema_name="crash_fk")
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ddl_crash_unique_index_foldin_rolls_back():
    """CREATE TABLE with an inline UNIQUE index that aborts after broadcasts must
    roll the WHOLE zone back — never a committed table missing its unique index.
    After a clean restart the table does not exist, and a fresh re-create still
    enforces the unique constraint (proving the folded-in index came back)."""
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_DDL_PANIC seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("crash_uidx")
        conn.close()

        proc = _restart_server(
            proc, data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_DDL_PANIC": "after_broadcasts"},
        )
        conn = gnitz.connect(sock_path)
        try:
            conn.execute_sql(
                "CREATE TABLE t ("
                "  a BIGINT NOT NULL PRIMARY KEY,"
                "  b BIGINT NOT NULL UNIQUE)",
                schema_name="crash_uidx",
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
        # The whole bundle (COL_TAB + TABLE_TAB + IDX_TAB) rolled back — no table.
        with pytest.raises(Exception):
            conn.resolve_table("crash_uidx", "t")
        # A clean re-create still enforces the unique constraint.
        conn.execute_sql(
            "CREATE TABLE t ("
            "  a BIGINT NOT NULL PRIMARY KEY,"
            "  b BIGINT NOT NULL UNIQUE)",
            schema_name="crash_uidx",
        )
        conn.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name="crash_uidx")
        with pytest.raises(Exception):
            conn.execute_sql("INSERT INTO t VALUES (2, 10)", schema_name="crash_uidx")
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _index_on_col(conn, owner_tid, col_idx):
    """True if a live IdxTab row indexes column `col_idx` of table `owner_tid`."""
    from gnitz.core import IDX_TAB, unpack_pk_cols

    _, batch_obj, _ = conn._client.scan(IDX_TAB)
    if batch_obj is None:
        return False
    for i in range(len(batch_obj.pks)):
        if batch_obj.weights[i] <= 0:
            continue
        if (batch_obj.columns[1][i] == owner_tid
                and unpack_pk_cols(batch_obj.columns[3][i]) == [col_idx]):
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


def test_worker_boot_flush_failure_preserves_sal():
    """A worker boot-flush failure must abort boot BEFORE the master zeroes the
    SAL's first commit sentinel, so the replayed-but-unflushed rows' only
    durable copy (the SAL) survives for the next boot.

    Without the fix the worker swallows the flush error, acks readiness, and
    the master resets the SAL — destroying the only durable copy of every
    pre-crash row that had not yet been checkpointed.
    """
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_BOOT_FLUSH_ERROR seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: create table, push rows, kill before any checkpoint. -
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("boot_flush")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="boot_flush",
        )
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="boot_flush",
        )
        conn.close()
        # Hard kill: the rows live only in the SAL + memtable, never flushed.
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 2: boot with the injected flush fault must FAIL. --------
        # Every worker reports the boot-flush failure on its startup ACK; the
        # master aborts in collect_acks before resetting the SAL.
        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_BOOT_FLUSH_ERROR": "1"},
            expect_socket=False,
        )
        rc = proc.wait(timeout=15)
        assert rc != 0, f"boot should have aborted on the flush fault, got rc={rc}"
        assert not os.path.exists(sock_path), "failed boot must not accept requests"
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 3: a clean boot recovers every pre-crash row. ----------
        # Proves the failed boot left the SAL intact.
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        tid, _ = conn.resolve_table("boot_flush", "t")
        rows = list(conn.scan(tid))
        assert len(rows) == 3, f"expected 3 rows after the failed boot, got {rows}"
        assert {r["pk"] for r in rows} == {1, 2, 3}, "wrong PKs after recovery"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_master_sys_flush_failure_preserves_sal():
    """A master system-table boot-flush failure must abort boot BEFORE the SAL
    reset, so the replayed-but-unflushed DDL's only durable copy (the SAL)
    survives.

    Without the fix the error is swallowed and the SAL is reset; after a later
    crash the catalog reverts to its pre-DDL state and gc_orphan_directories
    deletes the now-catalog-less entities' directories.
    """
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_SYS_FLUSH_ERROR seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: create a table, kill before any checkpoint. ---------
        # The DDL is committed in the SAL but not yet flushed to the
        # system-table shards.
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("sys_flush")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="sys_flush",
        )
        conn.close()
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 2: boot with the injected sys-flush fault must FAIL. ----
        # The master aborts after recover_system_tables_from_sal, before the
        # fork and long before the SAL reset.
        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_SYS_FLUSH_ERROR": "1"},
            expect_socket=False,
        )
        rc = proc.wait(timeout=15)
        assert rc != 0, f"boot should have aborted on the sys-flush fault, got rc={rc}"
        assert not os.path.exists(sock_path), "failed boot must not accept requests"
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 3: a clean boot recovers the DDL and accepts inserts. ---
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        tid, _ = conn.resolve_table("sys_flush", "t")
        conn.execute_sql("INSERT INTO t VALUES (7, 700)", schema_name="sys_flush")
        rows = list(conn.scan(tid))
        assert len(rows) == 1 and rows[0]["val"] == 700, f"unexpected rows: {rows}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Slice-local secondary indexes: post-restart correctness.
#
# Before slice-local unification, a boot-recovered index was a full copy
# replicated on every worker (plus a dead full copy on the master). Post-restart
# the foreign (W-1)/W fraction of each worker's copy went stale as those rows
# churned on their owning workers, so the distributed unique/FK probes — which
# union raw index hits across workers — saw phantom entries. These four tests
# pin the fixed behavior: every worker's index holds exactly its own base slice,
# so the master's HAS_PK / seek union over workers is exactly global existence.
# They pass at any worker count and would fail pre-fix at GNITZ_WORKERS >= 2.
# ---------------------------------------------------------------------------


def test_unique_reinsert_after_restart():
    """A unique value held at boot, then deleted post-restart, must be
    re-insertable (bugs 1 and 4). Pre-fix, non-owning workers keep the value's
    stale +1 forever in their full boot index copy; the DELETE nets it to 0 only
    on the owning worker, so the re-INSERT's FLAG_HAS_PK union hits a stale
    worker and is falsely rejected as a unique violation."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: table with a UNIQUE column, one holder of u=5. ------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("ureinsert")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  u BIGINT NOT NULL UNIQUE)",
            schema_name="ureinsert",
        )
        conn.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name="ureinsert")
        conn.close()

        # ---- Phase 2: restart (every worker rebuilds its index slice-local),
        # delete the sole holder, then re-insert u=5 under a fresh PK. --------
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.execute_sql("DELETE FROM t WHERE id = 1", schema_name="ureinsert")
        # Must succeed: no live row holds u=5 anywhere after the delete.
        conn.execute_sql("INSERT INTO t VALUES (2, 5)", schema_name="ureinsert")

        tid, _ = conn.resolve_table("ureinsert", "t")
        rows = [r for r in conn.scan(tid) if r.weight > 0]
        assert len(rows) == 1, f"expected exactly one live row, got {rows}"
        assert rows[0]["id"] == 2 and rows[0]["u"] == 5, f"unexpected row: {rows[0]}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_fk_no_orphan_after_restart():
    """An FK child referencing a parent value deleted post-restart must be
    rejected — the orphan must not be admitted (bug 2). The parent-existence
    probe broadcast-seeks the parent's UNIQUE index; pre-fix a stale worker
    still holds the deleted value and answers 'present', admitting the orphan."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: parent P(u UNIQUE), child C.f -> P(u); P holds u=5. --
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("fkorphan")
        conn.execute_sql(
            "CREATE TABLE p ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  u BIGINT NOT NULL UNIQUE)",
            schema_name="fkorphan",
        )
        conn.execute_sql(
            "CREATE TABLE c ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  f BIGINT NOT NULL REFERENCES p(u))",
            schema_name="fkorphan",
        )
        conn.execute_sql("INSERT INTO p VALUES (1, 5)", schema_name="fkorphan")
        conn.close()

        # ---- Phase 2: restart, delete the parent, then try to insert a child
        # referencing the now-absent value — must FAIL. ----------------------
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.execute_sql("DELETE FROM p WHERE id = 1", schema_name="fkorphan")
        with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
            conn.execute_sql("INSERT INTO c VALUES (10, 5)", schema_name="fkorphan")

        # The rejected child must leave no row behind.
        cid, _ = conn.resolve_table("fkorphan", "c")
        rows = [r for r in conn.scan(cid) if r.weight > 0]
        assert len(rows) == 0, f"orphan child was admitted: {rows}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_fk_restrict_update_after_restart():
    """Retargeting a parent's referenced value must succeed once its only child
    is gone, even across a restart (bug 3). The RESTRICT check probes the child's
    FK index for the old value; pre-fix a stale worker still holds the deleted
    child entry and falsely blocks the update."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: P(u UNIQUE), C.f -> P(u); one parent and one child. --
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("fkrestrict")
        conn.execute_sql(
            "CREATE TABLE p ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  u BIGINT NOT NULL UNIQUE)",
            schema_name="fkrestrict",
        )
        conn.execute_sql(
            "CREATE TABLE c ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  f BIGINT NOT NULL REFERENCES p(u))",
            schema_name="fkrestrict",
        )
        conn.execute_sql("INSERT INTO p VALUES (1, 5)", schema_name="fkrestrict")
        conn.execute_sql("INSERT INTO c VALUES (10, 5)", schema_name="fkrestrict")
        # Remove the only child, so the parent value is free to retarget.
        conn.execute_sql("DELETE FROM c WHERE id = 10", schema_name="fkrestrict")
        conn.close()

        # ---- Phase 2: restart, then retarget the parent value — must SUCCEED
        # because no child references u=5 anymore. ---------------------------
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.execute_sql("UPDATE p SET u = 6", schema_name="fkrestrict")

        pid, _ = conn.resolve_table("fkrestrict", "p")
        rows = [r for r in conn.scan(pid) if r.weight > 0]
        assert len(rows) == 1 and rows[0]["u"] == 6, f"update did not take: {rows}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_secondary_index_select_after_restart():
    """A non-unique secondary index spanning partitions must return the correct
    holder set after a restart, and stay correct under post-restart INSERT/DELETE
    — guarding the boot backfill-before-SAL-replay seam (a rebuild after replay
    would double-count the committed tail; before, but skipping replay, would
    drop it). Read through the distributed seek path, which merges every worker's
    slice-local index and resolves hits against the local base slice."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: 30 rows across partitions; every third shares g=7. --
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("sidx")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  g BIGINT NOT NULL)",
            schema_name="sidx",
        )
        values = ", ".join(f"({i}, {7 if i % 3 == 0 else 1000 + i})" for i in range(30))
        conn.execute_sql(f"INSERT INTO t VALUES {values}", schema_name="sidx")
        conn.execute_sql("CREATE INDEX ON t(g)", schema_name="sidx")
        conn.close()

        # ---- Phase 2: restart, then read the index for g=7 via seek. -------
        proc = _restart_server(proc, data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        tid, _ = conn.resolve_table("sidx", "t")

        def seek_g7():
            res = conn.seek_by_index(tid, [1], [7])
            return sorted(res.batch.pks) if res.batch is not None else []

        expected = sorted(i for i in range(30) if i % 3 == 0)  # {0,3,...,27}
        assert seek_g7() == expected, "g=7 holders must survive restart exactly"

        # ---- Phase 3: post-restart mutation flows through the rebuilt index.
        conn.execute_sql("DELETE FROM t WHERE id = 0", schema_name="sidx")
        conn.execute_sql("INSERT INTO t VALUES (100, 7)", schema_name="sidx")
        expected2 = sorted([i for i in expected if i != 0] + [100])
        assert seek_g7() == expected2, "index must track post-restart INSERT/DELETE"

        # Base multiplicity is exactly-once (no doubled weights).
        live = [r for r in conn.scan(tid) if r.weight > 0]
        assert all(r.weight == 1 for r in live), "base weights must be exactly one"
        assert len(live) == 30, f"expected 30 live rows, got {len(live)}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Fail-stop on storage errors while applying committed state.
#
# The GNITZ_INJECT_INGEST_APPLY_ERROR=store|index debug seam substitutes an
# Err for the matching ingest inside DagEngine::ingest_store_and_indices — the
# base-table PUSH apply and the SAL-replay apply. The old code swallowed the
# error, leaving the worker's state diverged from the durable SAL while the
# client held an ACK; the next checkpoint then orphaned the ACKed data forever.
# The fix aborts at the point of detection so restart + SAL replay re-applies it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("inject,with_index", [("store", False), ("index", True)])
def test_ingest_apply_error_aborts_and_replays(inject, with_index):
    """A storage error applying a committed PUSH must abort the cluster, and the
    ACKed rows the old code silently lost must come back via SAL replay.

    Phase 2 arms the seam and INSERTs: the owning worker aborts inside
    ingest_store_and_indices, the master's watchdog turns the dead worker
    into a cluster shutdown. Phase 3 boots clean and every inserted PK is present
    — the load-bearing assertion (the un-flushed PUSH zone stayed above the
    watermark and was replayed)."""
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_INGEST_APPLY_ERROR seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: clean server; CREATE TABLE (+ INDEX for the variant). --
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("apply_err")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="apply_err",
        )
        if with_index:
            conn.execute_sql("CREATE INDEX ON t(val)", schema_name="apply_err")
        conn.close()

        # ---- Phase 2: restart with the seam armed; the INSERT must abort. ----
        proc = _restart_server(
            proc, data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_INGEST_APPLY_ERROR": inject},
        )
        conn = gnitz.connect(sock_path)
        try:
            # The ACK races the abort — the apply aborts before it can be sent,
            # so this raises on a dead socket (or, rarely, returns just before).
            conn.execute_sql(
                "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
                schema_name="apply_err",
            )
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        # The seam aborts the owning worker; the watchdog detects the dead
        # worker, reports it, and fans it into a graceful cluster shutdown (the
        # master then exits 0). The proof the abort fired: the process terminated
        # (not hung) and the master reported the crash.
        proc.wait(timeout=15)
        stderr = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        assert "crashed" in stderr, (
            f"master must report the aborted worker, got stderr tail: {stderr[-500:]}"
        )
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 3: clean boot recovers every ACked/attempted row. ---------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        tid, _ = conn.resolve_table("apply_err", "t")
        rows = list(conn.scan(tid))
        pks = {r["pk"] for r in rows}
        assert pks == {1, 2, 3, 4, 5}, (
            f"SAL replay must restore the rows the swallow would have orphaned: got {pks}"
        )
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_boot_replay_apply_error_aborts_boot():
    """A storage error while replaying a committed PUSH at boot must abort the
    boot BEFORE the master zeroes the SAL sentinel, so the rows' only durable
    copy (the SAL) survives for the next boot.

    Without the fix the worker swallows the replay error, acks readiness, and
    the master resets the SAL — destroying every un-checkpointed pre-crash row."""
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_INGEST_APPLY_ERROR seam)")

    tmpdir, data_dir, sock_path = _make_env()
    try:
        # ---- Phase 1: create + insert, SIGKILL before any checkpoint. --------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("replay_err")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  val BIGINT NOT NULL)",
            schema_name="replay_err",
        )
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="replay_err",
        )
        conn.close()
        # Rows live only in the SAL + memtable, never flushed.
        _stop_server(proc)
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 2: boot with the seam armed must FAIL during SAL replay. --
        # The worker aborts inside ingest_store_and_indices while replaying the
        # committed PUSH zone, before its readiness ACK, so the master fails
        # wait_all_workers before zeroing the SAL sentinel.
        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS,
            extra_env={"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"},
            expect_socket=False,
        )
        rc = proc.wait(timeout=15)
        assert rc != 0, f"boot should have aborted during SAL replay, got rc={rc}"
        assert not os.path.exists(sock_path), "failed boot must not accept requests"
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # ---- Phase 3: a clean boot recovers every pre-crash row. -------------
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        tid, _ = conn.resolve_table("replay_err", "t")
        rows = list(conn.scan(tid))
        assert {r["pk"] for r in rows} == {1, 2, 3}, "SAL must survive the failed boot"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
