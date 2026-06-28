"""
Persistence and recovery: verify that tables, views, and data survive
a full server restart (crash-only shutdown via SIGKILL).

Ports compile_graph_test.py::test_persistence_and_recovery.
"""

import os
import subprocess
import tempfile
import time
import shutil
import signal

import pytest
import gnitz
from _serverproc import server_preexec

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _start_server(data_dir, sock_path, workers=None, extra_env=None):
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
    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.communicate()
        raise RuntimeError("Server did not start")
    return proc


def _stop_server(proc):
    """Kill server and all child workers (entire process group)."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def _crash_and_restart(proc, sock_path, data_dir, workers=None, extra_env=None):
    """SIGKILL the entire process group, clean up socket, restart."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    return _start_server(data_dir, sock_path, workers=workers, extra_env=extra_env)


def test_table_data_survives_restart():
    """Create table + insert rows, stop server, restart, verify rows survive."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_persist_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    try:
        # --- Phase 1: create table and insert data ---
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("test_persist")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="test_persist",
        )
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="test_persist",
        )
        tid, _ = conn.resolve_table("test_persist", "t")

        # Verify data is there before restart
        rows = conn.scan(tid)
        assert len(rows) == 3, f"pre-restart: expected 3 rows, got {len(rows)}"

        conn.close()
        _stop_server(proc)

        # Remove socket so new server can bind
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # --- Phase 2: restart and verify ---
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("test_persist", "t")
        assert tid2 == tid, f"table_id changed after restart: {tid} -> {tid2}"

        rows = conn.scan(tid2)
        assert len(rows) == 3, f"post-restart: expected 3 rows, got {len(rows)}"
        vals = sorted(r["val"] for r in rows)
        assert vals == [100, 200, 300], f"post-restart: unexpected vals {vals}"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_table_data_survives_restart_multiworker():
    """Same as test_table_data_survives_restart but with multi-worker SAL replay."""
    if _NUM_WORKERS < 2:
        pytest.skip("requires GNITZ_WORKERS >= 2")

    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_persist_mw_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        conn.create_schema("test_persist")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="test_persist",
        )
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="test_persist",
        )
        tid, _ = conn.resolve_table("test_persist", "t")
        rows = conn.scan(tid)
        assert len(rows) == 3

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("test_persist", "t")
        assert tid2 == tid
        rows = conn.scan(tid2)
        assert len(rows) == 3
        vals = sorted(r["val"] for r in rows)
        assert vals == [100, 200, 300]

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_view_survives_restart():
    """Create table + view, insert data, restart, verify view still works
    with new data pushed after restart."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_persist_view_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    try:
        # --- Phase 1: create table + view, insert data ---
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("test_persist")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="test_persist",
        )
        conn.execute_sql(
            "CREATE VIEW v AS SELECT pk, val * -1 AS neg_val FROM t",
            schema_name="test_persist",
        )
        tid, _ = conn.resolve_table("test_persist", "t")
        vid, _ = conn.resolve_table("test_persist", "v")

        conn.execute_sql(
            "INSERT INTO t VALUES (1, 42)",
            schema_name="test_persist",
        )
        rows = conn.scan(vid)
        assert len(rows) >= 1, "pre-restart: view should have rows"

        conn.close()
        _stop_server(proc)

        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # --- Phase 2: restart, push new data, verify view processes it ---
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("test_persist", "t")
        vid2, _ = conn.resolve_table("test_persist", "v")
        assert tid2 == tid
        assert vid2 == vid

        # Push new data after restart — view should process it
        conn.execute_sql(
            "INSERT INTO t VALUES (2, 100)",
            schema_name="test_persist",
        )
        rows = {r["pk"]: r for r in conn.scan(vid2) if r.weight > 0}
        assert 2 in rows, f"post-restart: pk=2 missing from view, got {rows}"
        assert rows[2]["neg_val"] == -100, (
            f"post-restart: expected neg_val=-100, got {rows[2]['neg_val']}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_nonexchange_view_retains_unflushed_data_after_restart():
    """A non-exchange (projection) view must reflect ACKed-but-unflushed base
    rows after a crash restart. The INSERT is fdatasync'd to the SAL but never
    checkpointed to a shard, so recovery replays it from the SAL — and the view
    must be rebuilt to include it, exactly as an exchange view (JOIN/GROUP BY)
    would be. The worker post-recovery pass rebuilds this cascade-unreachable
    non-exchange view from the recovered base store."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_persist_nxv_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    try:
        # --- Phase 1: create non-exchange view, insert (no explicit flush) ---
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        conn.create_schema("nxv")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="nxv",
        )
        conn.execute_sql(
            "CREATE VIEW v AS SELECT pk, val + 1 AS plus1 FROM t",
            schema_name="nxv",
        )
        vid, _ = conn.resolve_table("nxv", "v")
        conn.execute_sql("INSERT INTO t VALUES (7, 70)", schema_name="nxv")

        rows = {r["pk"]: r for r in conn.scan(vid) if r.weight > 0}
        assert rows[7]["plus1"] == 71, "pre-restart: view should reflect the insert"

        conn.close()
        _stop_server(proc)  # SIGKILL — the insert is in the SAL, not a shard
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        # --- Phase 2: restart; the unflushed row must survive in the view ---
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        vid2, _ = conn.resolve_table("nxv", "v")
        rows = {r["pk"]: r for r in conn.scan(vid2) if r.weight > 0}
        assert 7 in rows, f"post-restart: pk=7 missing from view, got {rows}"
        assert rows[7]["plus1"] == 71, (
            f"post-restart: expected plus1=71, got {rows[7]['plus1']}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _weights_by(rows, key):
    """Sum scan-row weights per `key` value (robust to consolidation form)."""
    out = {}
    for r in rows:
        out[r[key]] = out.get(r[key], 0) + r.weight
    return out


# Flushes the base table to shards before the crash: a small SAL checkpoint
# threshold makes the bulk INSERT's committer cycle run a checkpoint.
_FLUSH_ENV = {"GNITZ_CHECKPOINT_BYTES": "1024"}


def test_nonexchange_view_flushed_data_after_restart():
    """Flushed-data control for the non-exchange-view rebuild: with the base
    already on shards, the projection view must come back with every row exactly
    once (weight 1) — guarding that removing the inline open-time backfill did
    not regress the already-checkpointed path."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_nxvf_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS,
                             extra_env=_FLUSH_ENV)
        conn = gnitz.connect(sock_path)
        conn.create_schema("nxvf")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="nxvf",
        )
        conn.execute_sql(
            "CREATE VIEW v AS SELECT pk, val + 1 AS plus1 FROM t",
            schema_name="nxvf",
        )
        vals = ", ".join(f"({pk}, {pk * 10})" for pk in range(200))
        conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="nxvf")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS,
                                  extra_env=_FLUSH_ENV)
        conn = gnitz.connect(sock_path)
        vid, _ = conn.resolve_table("nxvf", "v")
        w = _weights_by(conn.scan(vid), "pk")
        assert len(w) == 200, f"expected 200 view rows, got {len(w)}"
        assert all(x == 1 for x in w.values()), \
            f"flushed rows double-counted: weights {sorted(set(w.values()))}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_nested_nonexchange_views_after_restart():
    """Two stacked non-exchange views over one base, no exchange view anywhere —
    both are cascade-unreachable and rebuilt by the worker post-recovery pass.
    The depth sort must fill the inner view first so the outer view's backfill
    reads a populated source. Unflushed data, so the rows live only in the SAL at
    crash time."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_nest_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("nest")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="nest",
        )
        conn.execute_sql(
            "CREATE VIEW v1 AS SELECT pk, val + 1 AS a FROM t",
            schema_name="nest",
        )
        conn.execute_sql(
            "CREATE VIEW v2 AS SELECT pk, a + 1 AS b FROM v1",
            schema_name="nest",
        )
        conn.execute_sql("INSERT INTO t VALUES (3, 30), (4, 40)", schema_name="nest")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        v1, _ = conn.resolve_table("nest", "v1")
        v2, _ = conn.resolve_table("nest", "v2")
        r1 = {r["pk"]: r for r in conn.scan(v1) if r.weight > 0}
        r2 = {r["pk"]: r for r in conn.scan(v2) if r.weight > 0}
        assert _weights_by(conn.scan(v1), "pk") == {3: 1, 4: 1}, "v1 not single-counted"
        assert _weights_by(conn.scan(v2), "pk") == {3: 1, 4: 1}, "v2 not single-counted"
        assert r1[3]["a"] == 31 and r1[4]["a"] == 41, f"v1 wrong: {r1}"
        assert r2[3]["b"] == 32 and r2[4]["b"] == 42, f"v2 wrong: {r2}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_nonexchange_sibling_of_exchange_view_single_counted():
    """Defect-2 regression: a non-exchange view (`vn`) sharing a base with an
    exchange view (`vx`) must be filled exactly once. The exchange cascade drives
    the shared base and re-derives `vn`, so the worker pass must skip it — before
    the fix the flushed rows carried weight 2 (inline open-time backfill plus the
    cascade). Flushed data, asserted at weight 1."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_sib_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS,
                             extra_env=_FLUSH_ENV)
        conn = gnitz.connect(sock_path)
        conn.create_schema("sib")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "g BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name="sib",
        )
        conn.execute_sql(
            "CREATE VIEW vn AS SELECT pk, val + 1 AS plus1 FROM t",
            schema_name="sib",
        )
        conn.execute_sql(
            "CREATE VIEW vx AS SELECT g, SUM(val) AS s FROM t GROUP BY g",
            schema_name="sib",
        )
        rows = [(pk, pk % 4, pk) for pk in range(200)]
        vals = ", ".join(f"({pk}, {g}, {val})" for pk, g, val in rows)
        conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="sib")

        exp_vx = {}
        for pk, g, val in rows:
            exp_vx[g] = exp_vx.get(g, 0) + val
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS,
                                  extra_env=_FLUSH_ENV)
        conn = gnitz.connect(sock_path)
        vn, _ = conn.resolve_table("sib", "vn")
        vx, _ = conn.resolve_table("sib", "vx")

        w = _weights_by(conn.scan(vn), "pk")
        assert len(w) == 200, f"vn missing rows: {len(w)}/200"
        assert all(x == 1 for x in w.values()), \
            f"vn double-counted: weights {sorted(set(w.values()))}"

        got_vx = {r["g"]: r["s"] for r in conn.scan(vx) if r.weight > 0}
        assert got_vx == exp_vx, f"vx wrong: got {got_vx}, want {exp_vx}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_nonexchange_view_over_exchange_view_after_restart():
    """A non-exchange view layered over an exchange view (`vn` over `vx`) is
    cascade-reachable (the cascade traverses base → vx → vn), so the worker pass
    must skip it and the master cascade fills it exactly once. Unflushed data:
    the rows must reach `vn` through the rebuilt `vx`."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_nxoe_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("nxoe")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "g BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name="nxoe",
        )
        conn.execute_sql(
            "CREATE VIEW vx AS SELECT g, SUM(val) AS s FROM t GROUP BY g",
            schema_name="nxoe",
        )
        conn.execute_sql(
            "CREATE VIEW vn AS SELECT g, s + 1 AS s1 FROM vx",
            schema_name="nxoe",
        )
        rows = [(pk, pk % 3, pk + 1) for pk in range(30)]
        vals = ", ".join(f"({pk}, {g}, {val})" for pk, g, val in rows)
        conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="nxoe")

        exp = {}
        for pk, g, val in rows:
            exp[g] = exp.get(g, 0) + val
        exp_vn = {g: s + 1 for g, s in exp.items()}
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        vn, _ = conn.resolve_table("nxoe", "vn")
        got = {r["g"]: r["s1"] for r in conn.scan(vn) if r.weight > 0}
        assert got == exp_vn, f"vn wrong after restart: got {got}, want {exp_vn}"
        # Each group is one row, exactly once.
        assert _weights_by(conn.scan(vn), "g") == {g: 1 for g in exp_vn}, \
            "vn group rows not single-counted"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Durability tests: ACKed operations survive SIGKILL
# ---------------------------------------------------------------------------

def _make_env():
    """Create a fresh tmpdir/data_dir/sock_path for an isolated server."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_dur_",
    )
    return tmpdir, os.path.join(tmpdir, "data"), os.path.join(tmpdir, "gnitz.sock")


def test_dml_insert_survives_crash():
    """ACKed INSERTs survive SIGKILL (WAL + SAL replay)."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        # Insert in multiple batches to exercise WAL across entries
        for i in range(5):
            base = i * 10
            vals = ", ".join(f"({base+j}, {base+j+100}, {base+j+200})" for j in range(10))
            conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="dur")

        rows_before = conn.scan(tid)
        assert len(rows_before) == 50
        conn.close()

        # Crash
        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        assert tid2 == tid
        rows_after = conn.scan(tid2)
        assert len(rows_after) == 50, f"expected 50 rows, got {len(rows_after)}"

        vals_before = sorted((r["pk"], r["a"], r["b"]) for r in rows_before)
        vals_after = sorted((r["pk"], r["a"], r["b"]) for r in rows_after)
        assert vals_before == vals_after, "row data changed after crash recovery"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_dml_update_survives_crash():
    """ACKed UPDATEs (upsert = retract + insert) survive SIGKILL."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="dur",
        )
        # Update pk=2: retract (2,200) + insert (2,999)
        conn.execute_sql("UPDATE t SET val = 999 WHERE pk = 2", schema_name="dur")

        rows_before = {r["pk"]: r["val"] for r in conn.scan(tid) if r.weight > 0}
        assert rows_before == {1: 100, 2: 999, 3: 300}
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = {r["pk"]: r["val"] for r in conn.scan(tid2) if r.weight > 0}
        assert rows_after == {1: 100, 2: 999, 3: 300}, (
            f"update lost after crash: {rows_after}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_dml_delete_survives_crash():
    """ACKed DELETEs survive SIGKILL."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
            schema_name="dur",
        )
        conn.execute_sql("DELETE FROM t WHERE pk = 2", schema_name="dur")

        rows_before = {r["pk"]: r["val"] for r in conn.scan(tid) if r.weight > 0}
        assert rows_before == {1: 100, 3: 300}
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = {r["pk"]: r["val"] for r in conn.scan(tid2) if r.weight > 0}
        assert rows_after == {1: 100, 3: 300}, (
            f"delete lost after crash: {rows_after}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ddl_create_table_survives_crash():
    """ACKed CREATE TABLE survives SIGKILL (SAL DDL_SYNC replay)."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql(
            "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid1, _ = conn.resolve_table("dur", "t1")
        tid2, _ = conn.resolve_table("dur", "t2")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid1_r, _ = conn.resolve_table("dur", "t1")
        tid2_r, _ = conn.resolve_table("dur", "t2")
        assert tid1_r == tid1, f"t1 ID changed: {tid1} -> {tid1_r}"
        assert tid2_r == tid2, f"t2 ID changed: {tid2} -> {tid2_r}"

        # Verify the tables are functional — can insert and scan
        conn.execute_sql("INSERT INTO t1 VALUES (1, 10)", schema_name="dur")
        conn.execute_sql("INSERT INTO t2 VALUES (1, 20)", schema_name="dur")
        assert len(conn.scan(tid1_r)) == 1
        assert len(conn.scan(tid2_r)) == 1

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ddl_create_view_survives_crash():
    """ACKed CREATE VIEW survives SIGKILL. View processes new data after restart."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql(
            "CREATE VIEW v AS SELECT pk, val + 1 AS inc FROM t",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")
        vid, _ = conn.resolve_table("dur", "v")

        # Insert data so view has something before crash
        conn.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name="dur")
        rows = [r for r in conn.scan(vid) if r.weight > 0]
        assert len(rows) == 1 and rows[0]["inc"] == 11
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        vid_r, _ = conn.resolve_table("dur", "v")
        assert vid_r == vid

        # Insert new data — view circuit must be alive
        conn.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name="dur")
        rows = {r["pk"]: r["inc"] for r in conn.scan(vid_r) if r.weight > 0}
        assert rows[2] == 21, f"view not processing after crash: {rows}"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ddl_drop_table_survives_crash():
    """ACKed DROP TABLE stays dropped after SIGKILL (no phantom tables)."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name="dur")
        conn.execute_sql("DROP TABLE dur.t", schema_name="dur")

        with pytest.raises(Exception):
            conn.resolve_table("dur", "t")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        with pytest.raises(Exception):
            conn.resolve_table("dur", "t")

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_multiple_ddl_batch_survives_crash():
    """Multiple DDLs in rapid succession all survive SIGKILL.

    Exercises the DDL response batching path — each DDL is a separate
    SAL broadcast, but fdatasync is deferred to end-of-cycle.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        # Rapid-fire DDLs
        table_ids = {}
        for i in range(10):
            conn.execute_sql(
                f"CREATE TABLE t{i} (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name="dur",
            )
            tid, _ = conn.resolve_table("dur", f"t{i}")
            table_ids[f"t{i}"] = tid
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        for name, expected_tid in table_ids.items():
            tid, _ = conn.resolve_table("dur", name)
            assert tid == expected_tid, f"{name}: ID {expected_tid} -> {tid}"
            # Verify table is functional
            conn.execute_sql(
                f"INSERT INTO {name} VALUES (1, 42)", schema_name="dur",
            )
            rows = conn.scan(tid)
            assert len(rows) == 1

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_interleaved_ddl_dml_survives_crash():
    """Interleaved DDL + DML all survive SIGKILL.

    Tests the interaction between DDL batching (signal-only + deferred sync)
    and DML (sync_and_signal_all on push).
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql("INSERT INTO t1 VALUES (1, 10)", schema_name="dur")

        conn.execute_sql(
            "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql("INSERT INTO t2 VALUES (1, 20)", schema_name="dur")

        conn.execute_sql(
            "CREATE VIEW v1 AS SELECT pk, val * 2 AS doubled FROM t1",
            schema_name="dur",
        )
        conn.execute_sql("INSERT INTO t1 VALUES (2, 30)", schema_name="dur")

        tid1, _ = conn.resolve_table("dur", "t1")
        tid2, _ = conn.resolve_table("dur", "t2")
        vid1, _ = conn.resolve_table("dur", "v1")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        # All DDL survived
        tid1_r, _ = conn.resolve_table("dur", "t1")
        tid2_r, _ = conn.resolve_table("dur", "t2")
        vid1_r, _ = conn.resolve_table("dur", "v1")
        assert tid1_r == tid1
        assert tid2_r == tid2
        assert vid1_r == vid1

        # All DML survived
        t1_rows = {r["pk"]: r["val"] for r in conn.scan(tid1_r) if r.weight > 0}
        assert t1_rows == {1: 10, 2: 30}, f"t1 data lost: {t1_rows}"

        t2_rows = {r["pk"]: r["val"] for r in conn.scan(tid2_r) if r.weight > 0}
        assert t2_rows == {1: 20}, f"t2 data lost: {t2_rows}"

        # View is functional after restart
        conn.execute_sql("INSERT INTO t1 VALUES (3, 50)", schema_name="dur")
        v_rows = {r["pk"]: r["doubled"] for r in conn.scan(vid1_r) if r.weight > 0}
        assert v_rows[3] == 100, f"view broken after crash: {v_rows}"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_no_phantom_data_after_crash():
    """Uncommitted data does not appear after SIGKILL.

    Insert data, crash, restart, insert more data with same PKs.
    If phantom rows from an incomplete write existed, the second insert
    would conflict or produce wrong counts.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        conn.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200)",
            schema_name="dur",
        )
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")

        # Re-insert same PKs with different values via explicit UPSERT
        conn.execute_sql(
            "INSERT INTO t VALUES (1, 111), (2, 222) "
            "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name="dur",
        )
        rows = {r["pk"]: r["val"] for r in conn.scan(tid2) if r.weight > 0}
        assert rows == {1: 111, 2: 222}, f"phantom or stale data: {rows}"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_sequence_monotonicity_after_crash():
    """Table ID allocator does not reuse IDs after crash recovery."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY)",
            schema_name="dur",
        )
        conn.execute_sql(
            "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY)",
            schema_name="dur",
        )
        tid1, _ = conn.resolve_table("dur", "t1")
        tid2, _ = conn.resolve_table("dur", "t2")
        max_tid_before = max(tid1, tid2)
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        # Create a new table — its ID must be strictly greater
        conn.execute_sql(
            "CREATE TABLE t3 (pk BIGINT NOT NULL PRIMARY KEY)",
            schema_name="dur",
        )
        tid3, _ = conn.resolve_table("dur", "t3")
        assert tid3 > max_tid_before, (
            f"ID allocator reused IDs: t1={tid1}, t2={tid2}, t3={tid3}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_double_crash_recovery():
    """Data survives two consecutive crashes without any clean shutdown."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")
        conn.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name="dur")
        conn.close()

        # First crash
        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        conn.execute_sql("INSERT INTO t VALUES (2, 200)", schema_name="dur")
        conn.close()

        # Second crash
        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        assert tid2 == tid
        rows = {r["pk"]: r["val"] for r in conn.scan(tid2) if r.weight > 0}
        assert rows == {1: 100, 2: 200}, f"data lost after double crash: {rows}"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Multi-worker durability (SAL replay across forked workers)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_dml_survives_crash():
    """Multi-worker: ACKed INSERTs across partitions survive SIGKILL."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        # Enough rows to hit multiple partitions
        vals = ", ".join(f"({i}, {i * 10})" for i in range(100))
        conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="dur")
        rows_before = sorted((r["pk"], r["val"]) for r in conn.scan(tid))
        assert len(rows_before) == 100
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = sorted((r["pk"], r["val"]) for r in conn.scan(tid2))
        assert rows_after == rows_before, (
            f"multi-worker data mismatch: {len(rows_after)} rows"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_ddl_batch_survives_crash():
    """Multi-worker: rapid DDL sequence survives SIGKILL.

    Exercises the signal-only broadcast_ddl + deferred fdatasync path
    in the multi-worker configuration where SAL entries are replayed
    into all worker processes on recovery.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        table_ids = {}
        for i in range(8):
            conn.execute_sql(
                f"CREATE TABLE t{i} (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name="dur",
            )
            tid, _ = conn.resolve_table("dur", f"t{i}")
            table_ids[f"t{i}"] = tid
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        for name, expected_tid in table_ids.items():
            tid, _ = conn.resolve_table("dur", name)
            assert tid == expected_tid, f"{name}: {expected_tid} -> {tid}"

        # Verify all tables are functional with multi-worker routing
        for i, (name, tid) in enumerate(table_ids.items()):
            vals = ", ".join(f"({i*100+j}, {j})" for j in range(10))
            conn.execute_sql(f"INSERT INTO {name} VALUES {vals}", schema_name="dur")
            rows = conn.scan(tid)
            assert len(rows) == 10, f"{name}: expected 10 rows, got {len(rows)}"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_view_survives_crash():
    """Multi-worker: view + data survive SIGKILL, view processes new data."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql(
            "CREATE VIEW v AS SELECT pk, val + 1000 AS big FROM t",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")
        vid, _ = conn.resolve_table("dur", "v")

        vals = ", ".join(f"({i}, {i})" for i in range(50))
        conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="dur")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        vid_r, _ = conn.resolve_table("dur", "v")
        assert vid_r == vid

        # Push new data — view must process it across workers
        conn.execute_sql("INSERT INTO t VALUES (999, 1)", schema_name="dur")
        v_rows_raw = conn.scan(vid_r)
        v_rows = {r["pk"]: r["big"] for r in v_rows_raw if r.weight > 0}
        assert v_rows.get(999) == 1001, (
            f"view broken after multi-worker crash: "
            f"v_count={len(v_rows)}, v_has_999={999 in v_rows}, vid={vid_r}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Edge-case durability tests
# ---------------------------------------------------------------------------

def test_string_column_survives_crash():
    """VARCHAR columns (inline + out-of-line blob encoding) survive SIGKILL."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "short_s VARCHAR(100) NOT NULL, long_s VARCHAR(500) NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        # Short string (inline) and long string (out-of-line German blob)
        short = "hello"
        long_s = "A" * 200  # well past the 12-byte inline threshold
        conn.execute_sql(
            f"INSERT INTO t VALUES (1, '{short}', '{long_s}')",
            schema_name="dur",
        )
        conn.execute_sql(
            f"INSERT INTO t VALUES (2, '', '{'B' * 250}')",
            schema_name="dur",
        )
        rows_before = {
            r["pk"]: (r["short_s"], r["long_s"])
            for r in conn.scan(tid)
        }
        assert len(rows_before) == 2
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = {
            r["pk"]: (r["short_s"], r["long_s"])
            for r in conn.scan(tid2)
        }
        assert rows_after == rows_before, (
            f"string data corrupted after crash: {rows_after}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_drop_view_survives_crash():
    """ACKed DROP VIEW stays dropped after SIGKILL (retraction durability)."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql(
            "CREATE VIEW v AS SELECT pk, val FROM t",
            schema_name="dur",
        )
        conn.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name="dur")
        # Verify view works before drop
        vid, _ = conn.resolve_table("dur", "v")
        assert len(conn.scan(vid)) >= 1

        conn.execute_sql("DROP VIEW dur.v", schema_name="dur")
        with pytest.raises(Exception):
            conn.resolve_table("dur", "v")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        # View must stay dropped
        with pytest.raises(Exception):
            conn.resolve_table("dur", "v")

        # Base table unaffected
        tid, _ = conn.resolve_table("dur", "t")
        rows = {r["pk"]: r["val"] for r in conn.scan(tid) if r.weight > 0}
        assert rows == {1: 10}

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_fk_constraint_survives_crash():
    """FK constraints are enforced after crash recovery."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        conn.execute_sql(
            "CREATE TABLE child ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  pid BIGINT NOT NULL REFERENCES parent(id),"
            "  data BIGINT NOT NULL"
            ")",
            schema_name="dur",
        )
        conn.execute_sql("INSERT INTO parent VALUES (10, 100)", schema_name="dur")
        conn.execute_sql("INSERT INTO child VALUES (1, 10, 42)", schema_name="dur")
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        # FK must still be enforced: child row with non-existent parent fails
        with pytest.raises(Exception):
            conn.execute_sql(
                "INSERT INTO child VALUES (2, 999, 0)", schema_name="dur",
            )

        # Child row with existing parent succeeds
        conn.execute_sql(
            "INSERT INTO child VALUES (3, 10, 99)", schema_name="dur",
        )
        ctid, _ = conn.resolve_table("dur", "child")
        rows = {r["pk"]: r["pid"] for r in conn.scan(ctid) if r.weight > 0}
        assert rows == {1: 10, 3: 10}

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_multiple_upserts_same_pk_crash():
    """Multiple updates to the same PK before crash: final value survives."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        conn.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name="dur")
        conn.execute_sql("UPDATE t SET val = 200 WHERE pk = 1", schema_name="dur")
        conn.execute_sql("UPDATE t SET val = 300 WHERE pk = 1", schema_name="dur")
        conn.execute_sql("UPDATE t SET val = 400 WHERE pk = 1", schema_name="dur")

        # Also test insert-delete-reinsert
        conn.execute_sql("INSERT INTO t VALUES (2, 10)", schema_name="dur")
        conn.execute_sql("DELETE FROM t WHERE pk = 2", schema_name="dur")
        conn.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name="dur")

        rows_before = {r["pk"]: r["val"] for r in conn.scan(tid) if r.weight > 0}
        assert rows_before == {1: 400, 2: 20}
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = {r["pk"]: r["val"] for r in conn.scan(tid2) if r.weight > 0}
        assert rows_after == {1: 400, 2: 20}, (
            f"multi-upsert data wrong after crash: {rows_after}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_nullable_columns_survive_crash():
    """Nullable columns with NULL values survive SIGKILL."""
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(data_dir, sock_path)
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  a BIGINT,"
            "  b BIGINT NOT NULL,"
            "  c BIGINT"
            ")",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        conn.execute_sql(
            "INSERT INTO t VALUES (1, NULL, 10, 100)", schema_name="dur",
        )
        conn.execute_sql(
            "INSERT INTO t VALUES (2, 20, 20, NULL)", schema_name="dur",
        )
        conn.execute_sql(
            "INSERT INTO t VALUES (3, NULL, 30, NULL)", schema_name="dur",
        )

        def snapshot(tid):
            return {
                r["pk"]: (r["a"], r["b"], r["c"])
                for r in conn.scan(tid) if r.weight > 0
            }

        rows_before = snapshot(tid)
        assert rows_before[1] == (None, 10, 100)
        assert rows_before[2] == (20, 20, None)
        assert rows_before[3] == (None, 30, None)
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir)
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = snapshot(tid2)
        assert rows_after == rows_before, (
            f"nullable data wrong after crash: {rows_after}"
        )

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# SAL checkpoint boundary durability (multi-worker only)
# ---------------------------------------------------------------------------

_CHECKPOINT_ENV = {"GNITZ_CHECKPOINT_BYTES": "262144"}  # 256 KB


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_crash_across_checkpoint():
    """Data spanning a SAL checkpoint boundary survives SIGKILL.

    Uses GNITZ_CHECKPOINT_BYTES=32KB so a checkpoint triggers after a few
    hundred rows.  Inserts data before AND after the checkpoint, then
    crashes.  Both pre-checkpoint (in shards) and post-checkpoint (in SAL)
    data must survive.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS, extra_env=_CHECKPOINT_ENV,
        )
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        # Insert enough rows in batches to guarantee at least one checkpoint.
        # With 32KB threshold and ~50 bytes/row in SAL wire format, ~700 rows
        # will exceed the threshold.  We insert 2000 to be safe.
        total = 0
        for batch_idx in range(20):
            base = batch_idx * 100
            vals = ", ".join(f"({base + j}, {base + j + 10000})" for j in range(100))
            conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="dur")
            total += 100

        rows_before = conn.scan(tid)
        assert len(rows_before) == total, f"pre-crash: {len(rows_before)} != {total}"
        conn.close()

        proc = _crash_and_restart(
            proc, sock_path, data_dir, workers=_NUM_WORKERS,
            extra_env=_CHECKPOINT_ENV,
        )
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = conn.scan(tid2)
        assert len(rows_after) == total, (
            f"checkpoint-boundary crash lost rows: {len(rows_after)} != {total}"
        )
        vals_before = sorted((r["pk"], r["val"]) for r in rows_before)
        vals_after = sorted((r["pk"], r["val"]) for r in rows_after)
        assert vals_before == vals_after, "data mismatch after checkpoint crash"

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_crash_after_checkpoint():
    """All data in shards (SAL empty after checkpoint) survives SIGKILL.

    Forces a checkpoint, then crashes without writing new data.  Recovery
    must reconstruct everything from shard files alone — no SAL replay.
    """
    tmpdir, data_dir, sock_path = _make_env()
    try:
        proc = _start_server(
            data_dir, sock_path, workers=_NUM_WORKERS, extra_env=_CHECKPOINT_ENV,
        )
        conn = gnitz.connect(sock_path)

        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur",
        )
        tid, _ = conn.resolve_table("dur", "t")

        # Insert enough to trigger checkpoint
        for batch_idx in range(15):
            base = batch_idx * 100
            vals = ", ".join(f"({base + j}, {base + j})" for j in range(100))
            conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="dur")

        # Scan forces pending ticks to fire — view/table state settles
        rows_before = conn.scan(tid)
        n = len(rows_before)
        assert n == 1500

        # Crash — checkpoint already happened, SAL is nearly empty
        conn.close()
        proc = _crash_and_restart(
            proc, sock_path, data_dir, workers=_NUM_WORKERS,
            extra_env=_CHECKPOINT_ENV,
        )
        conn = gnitz.connect(sock_path)

        tid2, _ = conn.resolve_table("dur", "t")
        rows_after = conn.scan(tid2)
        assert len(rows_after) == n, (
            f"post-checkpoint crash lost rows: {len(rows_after)} != {n}"
        )

        # Verify the server is fully functional after shard-only recovery
        conn.execute_sql("INSERT INTO t VALUES (9999, 9999)", schema_name="dur")
        rows_final = conn.scan(tid2)
        assert len(rows_final) == n + 1

        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_global_aggregate_empty_source_survives_restart():
    """A global (ungrouped) aggregate over a never-populated table shows one ground
    row (COUNT=0, SUM=NULL). Views are ephemeral and re-derived through the circuit
    at restart, so the seed must re-fire and exactly one ground row must survive."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_gae_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("gae")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name="gae")
        conn.execute_sql(
            "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
            schema_name="gae")
        vid, _ = conn.resolve_table("gae", "v")
        rows = [r for r in conn.scan(vid) if r.weight > 0]
        assert len(rows) == 1 and rows[0]["cnt"] == 0 and rows[0]["total"] is None
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        vid2, _ = conn.resolve_table("gae", "v")
        rows = [r for r in conn.scan(vid2) if r.weight > 0]
        assert len(rows) == 1, f"post-restart: ground row must survive, got {len(rows)}"
        assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_global_aggregate_emptied_then_restart():
    """A global aggregate populated then fully deleted shows one ground row; after a
    restart (durable base table is empty) the re-derived view keeps exactly one."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_gae2_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("gae2")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name="gae2")
        conn.execute_sql(
            "CREATE VIEW v AS SELECT COUNT(*) AS cnt, MIN(a) AS lo FROM t",
            schema_name="gae2")
        vid, _ = conn.resolve_table("gae2", "v")
        conn.execute_sql("INSERT INTO t VALUES (1, 5), (2, 8)", schema_name="gae2")
        conn.execute_sql("DELETE FROM t", schema_name="gae2")
        rows = [r for r in conn.scan(vid) if r.weight > 0]
        assert len(rows) == 1 and rows[0]["cnt"] == 0 and rows[0]["lo"] is None
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        vid2, _ = conn.resolve_table("gae2", "v")
        rows = [r for r in conn.scan(vid2) if r.weight > 0]
        assert len(rows) == 1, f"post-restart: one ground row, got {len(rows)}"
        assert rows[0]["cnt"] == 0 and rows[0]["lo"] is None
        # And the value returns on a fresh insert after restart.
        conn.execute_sql("INSERT INTO t VALUES (3, 4)", schema_name="gae2")
        r = [r for r in conn.scan(vid2) if r.weight > 0]
        assert len(r) == 1 and r[0]["cnt"] == 1 and r[0]["lo"] == 4
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_global_aggregate_worker_count_change_reseeds():
    """Create a global aggregate at 4 workers, restart at 2: V0's owner partition
    moves to a different worker, so the new owner-bake must re-seed the ground.
    Ephemeral re-derivation + per-process owner-bake must compose to exactly one
    surviving row."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_gawc_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=4)
        conn = gnitz.connect(sock_path)
        conn.create_schema("gawc")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name="gawc")
        conn.execute_sql(
            "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
            schema_name="gawc")
        vid, _ = conn.resolve_table("gawc", "v")
        rows = [r for r in conn.scan(vid) if r.weight > 0]
        assert len(rows) == 1 and rows[0]["cnt"] == 0
        conn.close()

        # Restart at a DIFFERENT worker count: V0 owner moves W3 -> W1.
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=2)
        conn = gnitz.connect(sock_path)
        vid2, _ = conn.resolve_table("gawc", "v")
        rows = [r for r in conn.scan(vid2) if r.weight > 0]
        assert len(rows) == 1, f"new V0 owner must re-seed exactly one row, got {len(rows)}"
        assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_global_aggregate_replicated_survives_restart():
    """A global aggregate over a WITH (replicated=true) empty source shows one
    ground row, and the re-derived view keeps exactly one after a crash restart
    (the backfill empty-epoch + replicated `i_am_owner` disjunct must re-fire)."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_persist_garep_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("garep")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) "
            "WITH (replicated = true)",
            schema_name="garep")
        conn.execute_sql(
            "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
            schema_name="garep")
        vid, _ = conn.resolve_table("garep", "v")
        rows = [r for r in conn.scan(vid) if r.weight > 0]
        assert len(rows) == 1 and rows[0]["cnt"] == 0 and rows[0]["total"] is None
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        vid2, _ = conn.resolve_table("garep", "v")
        rows = [r for r in conn.scan(vid2) if r.weight > 0]
        assert len(rows) == 1, f"replicated ground must survive restart, got {len(rows)}"
        assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
