"""A DDL zone that aborts before its commit sentinel leaves no durable trace,
and a committed DROP's directory is reclaimed at boot.

The abort is injected with `GNITZ_INJECT_SAL_ZONE_PANIC=ddl`: the SAL's zone
scope fires it between publishing the zone and writing its commit sentinel, and
the tag picks the DDL scope so a push's zone is not the one that aborts. Recovery
treats such a zone as uncommitted, so the writes that already reached the SAL are
never replayed and no table_id survives without its TABLE_TAB row.

The reclamation tests are the mirror image: a DROP's on-disk directory is queued
for deletion in memory only, so a crash between the commit and the next
checkpoint leaks it permanently unless the boot sweep reclaims it. They SIGKILL
before any checkpoint, so the DROPs live only in the SAL.
"""

import os

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
    assert rc != 0, f"the server must abort on `{sql}`, got rc={rc}"


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
            with pytest.raises(Exception):
                conn.resolve_table("crash", name)

        # Re-creating with the same names must succeed cleanly: replayed orphan
        # COL_TAB rows for the un-committed table_ids would collide or shift the
        # column ordering.
        conn.execute_sql(_ABORTED["t"], schema_name="crash")
        conn.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name="crash")
        assert bag(scanned(conn, "crash", "t"), "pk", "val") == {(1, 100): 1}

        conn.execute_sql(_ABORTED["u"], schema_name="crash")
        conn.execute_sql("INSERT INTO u VALUES (1, 10)", schema_name="crash")
        with pytest.raises(Exception):
            conn.execute_sql("INSERT INTO u VALUES (2, 10)", schema_name="crash")

        assert bag(scanned(conn, "crash", "parent"), "id") == {
            (1,): 1, (2,): 1, (3,): 1}
        conn.execute_sql("DELETE FROM parent WHERE id = 1", schema_name="crash")
        conn.execute_sql("DROP TABLE parent", schema_name="crash")


def test_a_dropped_table_is_reclaimed_while_an_unflushed_create_survives(own_server):
    """The boot sweep must run *after* SAL replay. A sweep that ran before would
    see table B absent from the DAG — its CREATE is committed to the SAL but not
    yet flushed — and delete its live directory; replay would then re-register B
    pointing at a missing dir."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("gc")
        conn.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="gc")
        a_tid, _ = conn.resolve_table("gc", "a")
        # Drop A — its directory moves to the in-memory checkpoint-gated queue,
        # still on disk — then create B. Both DDLs live only in the SAL.
        conn.drop_table("gc", "a")
        conn.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="gc")
        b_tid, _ = conn.resolve_table("gc", "b")

    # Id-only directories (§4): `t_{tid}`, regardless of the table name.
    a_dir = os.path.join(own_server.data_dir, "gc", f"t_{a_tid}")
    b_dir = os.path.join(own_server.data_dir, "gc", f"t_{b_tid}")
    assert os.path.isdir(a_dir), "dropped A's dir is gated (still on disk) pre-crash"

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert conn.resolve_table("gc", "b")[0] == b_tid
        assert os.path.isdir(b_dir), "B's SAL-only-created dir must survive recovery"
        conn.execute_sql("INSERT INTO b VALUES (1, 100)", schema_name="gc")
        assert bag(scanned(conn, "gc", "b"), "pk", "v") == {(1, 100): 1}

        assert not os.path.exists(a_dir), "dropped A's dir must be reclaimed on boot"
        with pytest.raises(Exception):
            conn.resolve_table("gc", "a")


def test_a_dropped_schema_subtree_is_reclaimed(own_server):
    """A DROP SCHEMA CASCADE that lives only in the SAL at crash time has its
    whole subtree reclaimed. The schema-scoped scan cannot reach the subtree — the
    schema is gone from `schema_by_id` — so reclamation depends on the drain of
    the queue SAL replay re-populated."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("doomed")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="doomed")
        t_tid, _ = conn.resolve_table("doomed", "t")
        conn.drop_schema("doomed")

    schema_dir = os.path.join(own_server.data_dir, "doomed")
    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        with pytest.raises(Exception):
            conn.resolve_table("doomed", "t")
    assert not os.path.exists(os.path.join(schema_dir, f"t_{t_tid}"))
    assert not os.path.exists(schema_dir), "dropped schema dir must be gone"


def test_a_recreated_schema_survives_the_replayed_drop(own_server):
    """DROP SCHEMA s + CREATE SCHEMA s, both SAL-only at crash time. A schema's
    on-disk path is name-based, so the replayed DROP and CREATE land in the same
    boot deletion queue; the CREATE's hook must clear the DROP's residue, or the
    drain removes `<base>/s` recursively and erases the live schema."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("reborn")
        conn.execute_sql(
            "CREATE TABLE old (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="reborn")
        conn.drop_schema("reborn")
        conn.create_schema("reborn")
        conn.execute_sql(
            "CREATE TABLE fresh (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="reborn")
        fresh_tid, _ = conn.resolve_table("reborn", "fresh")
        conn.execute_sql("INSERT INTO fresh VALUES (1, 11), (2, 22)",
                         schema_name="reborn")

    schema_dir = os.path.join(own_server.data_dir, "reborn")
    fresh_dir = os.path.join(schema_dir, f"t_{fresh_tid}")
    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "reborn", "fresh"), "pk", "v") == {
            (1, 11): 1, (2, 22): 1}
    assert os.path.isdir(fresh_dir), "the recreated table's dir must survive"
