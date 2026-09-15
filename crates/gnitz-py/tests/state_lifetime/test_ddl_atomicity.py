"""A DDL zone that aborts before its commit sentinel leaves no durable trace,
and a committed DROP's directory is reclaimed at boot.

The abort is injected with `GNITZ_INJECT_SAL_ZONE_PANIC=ddl`: the SAL's zone
scope fires it between publishing the zone and writing its commit sentinel, and
the tag picks the DDL scope so a push's zone is not the one that aborts. Recovery
treats such a zone as uncommitted, so the writes that already reached the SAL are
never replayed and no table_id survives without its TABLE_TAB row.

The reclamation test is the mirror image: what a committed DROP leaves on disk
must be reclaimed at boot, and nothing else may be. It SIGKILLs before any
checkpoint too, so its DROPs live only in the SAL.
"""

import os
import signal

import pytest
import gnitz
from _read import bag, scanned


def _abort_on(srv, sql, schema):
    """Arm the zone-abort seam for one boot, issue `sql`, and confirm the server
    died on it. The request's error mode depends on whether the abort lands
    before or after the response was queued, so the exit code — not the
    exception — is the observable. A statement the planner rejects never reaches
    the seam and leaves the server up, which `wait_for_exit` reports."""
    srv.restart(extra_env={"GNITZ_INJECT_SAL_ZONE_PANIC": "ddl"})
    try:
        with gnitz.connect(srv.sock_path) as conn:
            conn.execute_sql(sql, schema_name=schema)
    except Exception:
        pass
    rc = srv.wait_for_exit()
    # The seam is a bare `libc::abort()`, so the master dies on SIGABRT. `rc != 0`
    # would also accept a boot failure (1), a worker crash (2) or a fatal abort
    # (134) — i.e. a crash that is not the one being armed.
    assert rc == -signal.SIGABRT, f"the server must abort on `{sql}`, got rc={rc}"


_ABORTED = {
    "t": "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
    # An FK bundle: the pre-fix cross-zone orphan (COL_TAB committed in its own
    # zone before the owning TABLE_TAB) is structurally unconstructible once a
    # CREATE is one atomic message, so this is the regression proof.
    "child": "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, "
             "pid BIGINT NOT NULL REFERENCES parent(id))",
    "v": "CREATE VIEW v AS SELECT id FROM parent",
    # An inline UNIQUE folds an index into the same zone: the whole bundle
    # (COL_TAB + TABLE_TAB + IDX_TAB) must roll back together, never a committed
    # table missing its index.
    "u": "CREATE TABLE u (a BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL UNIQUE)",
}


def test_an_aborted_ddl_leaves_no_durable_trace(own_server):
    """Four DDL bundles, each aborted on its own boot, then one clean boot that
    must find no trace of any of them: nothing resolves, every name is free to
    re-create, a re-created UNIQUE still enforces, and no phantom FK child or view
    dependency blocks the parent's DELETE or its DROP."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("crash")
        conn.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                         schema_name="crash")
        conn.execute_sql("INSERT INTO parent VALUES (1), (2), (3)",
                         schema_name="crash")

    for sql in _ABORTED.values():
        _abort_on(own_server, sql, "crash")

    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        for name in _ABORTED:
            with pytest.raises(gnitz.GnitzNotFoundError):
                conn.resolve_table("crash", name)

        # Re-creating with the same names must succeed cleanly: replayed orphan
        # COL_TAB rows for the un-committed table_ids would collide or shift the
        # column ordering.
        conn.execute_sql(_ABORTED["t"], schema_name="crash")
        conn.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name="crash")
        assert bag(scanned(conn, "crash", "t"), "pk", "val") == {(1, 100): 1}

        conn.execute_sql(_ABORTED["u"], schema_name="crash")
        conn.execute_sql("INSERT INTO u VALUES (1, 10)", schema_name="crash")
        with pytest.raises(gnitz.GnitzError):
            conn.execute_sql("INSERT INTO u VALUES (2, 10)", schema_name="crash")

        assert bag(scanned(conn, "crash", "parent"), "id") == {
            (1,): 1, (2,): 1, (3,): 1}
        conn.execute_sql("DELETE FROM parent WHERE id = 1", schema_name="crash")
        conn.execute_sql("DROP TABLE parent", schema_name="crash")


_TABLE_DDL = "(pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"


def test_the_boot_sweep_reclaims_exactly_the_dropped_directories(own_server):
    """A DROP leaves its directory to the orphan sweep, so a crash before the next
    checkpoint leaks it unless the boot sweep reclaims it — and the sweep must
    reclaim the dropped ones and nothing else."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        for sn in ("after_replay", "dropped"):
            conn.create_schema(sn)

        # Sweeping before SAL replay would see `b` absent from the catalog — its
        # CREATE is committed but not yet flushed — and delete its live dir.
        # Dropped `a` meanwhile waits for a checkpoint's sweep, still on disk.
        conn.execute_sql(f"CREATE TABLE a {_TABLE_DDL}", schema_name="after_replay")
        a_tid, _ = conn.resolve_table("after_replay", "a")
        conn.drop_table("after_replay", "a")
        conn.execute_sql(f"CREATE TABLE b {_TABLE_DDL}", schema_name="after_replay")
        b_tid, _ = conn.resolve_table("after_replay", "b")

        # A dropped schema's member is reclaimed by the same sweep.
        conn.execute_sql(f"CREATE TABLE t {_TABLE_DDL}", schema_name="dropped")
        dropped_tid, _ = conn.resolve_table("dropped", "t")
        conn.drop_schema("dropped")

    def rel_dir(tid):
        # Named by id alone, so a name reused across a drop names a fresh dir.
        return os.path.join(own_server.data_dir, "_relations", f"t_{tid}")

    assert os.path.isdir(rel_dir(a_tid)), \
        "dropped a's dir waits for the sweep (still on disk) pre-crash"

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert conn.resolve_table("after_replay", "b")[0] == b_tid
        assert os.path.isdir(rel_dir(b_tid)), \
            "b's SAL-only-created dir must survive recovery"
        conn.execute_sql("INSERT INTO b VALUES (1, 100)", schema_name="after_replay")
        assert bag(scanned(conn, "after_replay", "b"), "pk", "v") == {(1, 100): 1}
        assert not os.path.exists(rel_dir(a_tid)), \
            "dropped a's dir must be reclaimed on boot"

        # A dropped table in a live schema is resolved and missed client-side;
        # a dropped schema is refused by the server before the name is reached.
        with pytest.raises(gnitz.GnitzNotFoundError):
            conn.resolve_table("after_replay", "a")
        with pytest.raises(gnitz.GnitzError):
            conn.resolve_table("dropped", "t")

    assert not os.path.exists(rel_dir(dropped_tid)), \
        "the dropped schema's table dir must be gone"
