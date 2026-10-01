"""A DDL zone that aborts before its closing member publishes leaves no durable
trace, and a committed DROP's directory is reclaimed at boot.

The abort is injected with `GNITZ_INJECT_SAL_ZONE_PANIC=ddl`: the SAL's zone
scope fires it between publishing the zone and publishing its closing member,
and the tag picks the DDL scope so a push's zone is not the one that aborts. Recovery
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
from _paths import relation_dir
from _read import bag, scanned


_SEAM = {"GNITZ_INJECT_SAL_ZONE_PANIC": "ddl"}


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
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)")
        conn.execute_sql("INSERT INTO parent VALUES (1), (2), (3)")

    for sql in _ABORTED.values():
        # The seam is a bare `libc::abort()`, so the master dies on SIGABRT.
        # `rc != 0` would also accept a boot failure (1), a worker crash (2) or
        # a fatal abort (134) — a crash that is not the one being armed.
        assert own_server.exit_code_on(sql, _SEAM) == -signal.SIGABRT, sql

    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        for name in _ABORTED:
            with pytest.raises(gnitz.GnitzNotFoundError):
                conn.resolve_table(name)

        # Re-creating with the same names must succeed cleanly: replayed orphan
        # COL_TAB rows for the un-committed table_ids would collide or shift the
        # column ordering.
        conn.execute_sql(_ABORTED["t"])
        conn.execute_sql("INSERT INTO t VALUES (1, 100)")
        assert bag(scanned(conn, "t"), "pk", "val") == {(1, 100): 1}

        conn.execute_sql(_ABORTED["u"])
        conn.execute_sql("INSERT INTO u VALUES (1, 10)")
        with pytest.raises(gnitz.GnitzIntegrityError, match="[Uu]nique index violation"):
            conn.execute_sql("INSERT INTO u VALUES (2, 10)")

        assert bag(scanned(conn, "parent"), "id") == {
            (1,): 1, (2,): 1, (3,): 1}
        conn.execute_sql("DELETE FROM parent WHERE id = 1")
        conn.execute_sql("DROP TABLE parent")


_TABLE_DDL = "(pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"


def test_the_boot_sweep_reclaims_exactly_the_dropped_directories(own_server):
    """A DROP leaves its directory to the orphan sweep, so a crash before the next
    checkpoint leaks it unless the boot sweep reclaims it — and the sweep must
    reclaim the dropped ones and nothing else."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        # Sweeping before SAL replay would see `b` absent from the catalog — its
        # CREATE is committed but not yet flushed — and delete its live dir.
        # Dropped `a` meanwhile waits for a checkpoint's sweep, still on disk.
        conn.execute_sql(f"CREATE TABLE a {_TABLE_DDL}")
        a_tid, _ = conn.resolve_table("a")
        conn.drop_table("a")
        conn.execute_sql(f"CREATE TABLE b {_TABLE_DDL}")
        b_tid, _ = conn.resolve_table("b")

        # A dropped schema's member is reclaimed by the same sweep.
        conn.create_schema("dropped")
        conn.schema = "dropped"
        conn.execute_sql(f"CREATE TABLE t {_TABLE_DDL}")
        dropped_tid, _ = conn.resolve_table("t")
        conn.drop_schema("dropped")

    assert os.path.isdir(relation_dir(own_server.data_dir, a_tid)), \
        "dropped a's dir waits for the sweep (still on disk) pre-crash"

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        assert conn.resolve_table("b")[0] == b_tid
        assert os.path.isdir(relation_dir(own_server.data_dir, b_tid)), \
            "b's SAL-only-created dir must survive recovery"
        conn.execute_sql("INSERT INTO b VALUES (1, 100)")
        assert bag(scanned(conn, "b"), "pk", "v") == {(1, 100): 1}
        assert not os.path.exists(relation_dir(own_server.data_dir, a_tid)), \
            "dropped a's dir must be reclaimed on boot"

        with pytest.raises(gnitz.GnitzNotFoundError):
            conn.resolve_table("a")
        conn.schema = "dropped"
        with pytest.raises(gnitz.GnitzNotFoundError, match="schema"):
            conn.resolve_table("t")

    assert not os.path.exists(relation_dir(own_server.data_dir, dropped_tid)), \
        "the dropped schema's table dir must be gone"
