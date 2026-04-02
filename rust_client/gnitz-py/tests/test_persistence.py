"""
Persistence and recovery: verify that tables, views, and data survive
a full server restart.

Ports compile_graph_test.py::test_persistence_and_recovery.
"""

import os
import subprocess
import tempfile
import time
import shutil

import pytest
import gnitz


def _start_server(data_dir, sock_path, workers=None):
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server-c")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if workers:
        cmd += [f"--workers={workers}"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    proc.kill()
    proc.wait()


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
