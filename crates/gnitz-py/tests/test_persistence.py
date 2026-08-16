"""
Persistence and recovery: verify that tables, views, and data survive
a full server restart (crash-only shutdown via SIGKILL).

Ports compile_graph_test.py::test_persistence_and_recovery.
"""

import pytest
import gnitz
from _serverproc import NUM_WORKERS as _NUM_WORKERS, is_debug_build


def test_table_data_survives_restart(own_server):
    """Create table + insert rows, stop server, restart, verify rows survive."""
    sock_path = own_server.sock_path

    # --- Phase 1: create table and insert data ---
    own_server.start()
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

    # Remove socket so new server can bind

    # --- Phase 2: restart and verify ---
    own_server.start()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("test_persist", "t")
    assert tid2 == tid, f"table_id changed after restart: {tid} -> {tid2}"

    rows = conn.scan(tid2)
    assert len(rows) == 3, f"post-restart: expected 3 rows, got {len(rows)}"
    vals = sorted(r["val"] for r in rows)
    assert vals == [100, 200, 300], f"post-restart: unexpected vals {vals}"

    conn.close()


def test_table_data_survives_restart_multiworker(own_server):
    """Same as test_table_data_survives_restart but with multi-worker SAL replay."""
    if _NUM_WORKERS < 2:
        pytest.skip("requires GNITZ_WORKERS >= 2")

    sock_path = own_server.sock_path

    own_server.start()
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
    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("test_persist", "t")
    assert tid2 == tid
    rows = conn.scan(tid2)
    assert len(rows) == 3
    vals = sorted(r["val"] for r in rows)
    assert vals == [100, 200, 300]

    conn.close()


def test_view_survives_restart(own_server):
    """Create table + view, insert data, restart, verify view still works
    with new data pushed after restart."""
    sock_path = own_server.sock_path

    # --- Phase 1: create table + view, insert data ---
    own_server.start()
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


    # --- Phase 2: restart, push new data, verify view processes it ---
    own_server.start()
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
    rows = {r["pk"]: r for r in conn.scan(vid2)}
    assert 2 in rows, f"post-restart: pk=2 missing from view, got {rows}"
    assert rows[2]["neg_val"] == -100, (
        f"post-restart: expected neg_val=-100, got {rows[2]['neg_val']}"
    )

    conn.close()


def test_graceful_shutdown_resumes_without_backfill(own_server):
    """Graceful shutdown (SIGTERM to the master) drives a final checkpoint and
    exits cleanly (rc 0, no hang); the restart then **resumes** every view from
    that checkpoint instead of rebuilding it (commit 3). Asserts both correctness
    (pre-shutdown + post-restart rows) AND that no backfill ran (the master's
    'recovery: rebuilding 0 invalid view(s)' marker)."""
    sock_path = own_server.sock_path

    own_server.start()
    conn = gnitz.connect(sock_path)
    conn.create_schema("gs")
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name="gs",
    )
    conn.execute_sql(
        "CREATE VIEW v AS SELECT pk, val * -1 AS neg_val FROM t",
        schema_name="gs",
    )
    conn.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name="gs")
    vid, _ = conn.resolve_table("gs", "v")
    assert len(conn.scan(vid)) >= 1, "pre-shutdown: view should have rows"
    conn.close()

    # Graceful shutdown: SIGTERM the master. It must exit cleanly without
    # hanging (a hang raises TimeoutExpired from proc.wait). On restart,
    # views must resume (no backfill) and be correct for both the
    # pre-shutdown row and a freshly pushed one.
    own_server.restart(graceful=True)
    conn = gnitz.connect(sock_path)
    vid2, _ = conn.resolve_table("gs", "v")
    conn.execute_sql("INSERT INTO t VALUES (2, 100)", schema_name="gs")
    rows = {r["pk"]: r for r in conn.scan(vid2)}
    assert 1 in rows and rows[1]["neg_val"] == -42, (
        f"post-restart: pre-shutdown row wrong, got {rows}"
    )
    assert 2 in rows and rows[2]["neg_val"] == -100, (
        f"post-restart: new row wrong, got {rows}"
    )
    conn.close()

    # The clean restart must resume from the graceful checkpoint — zero views
    # rebuilt.
    n = own_server.rebuilt_view_count()
    assert n == 0, f"clean restart must resume without backfill, but rebuilt {n} view(s)"


def test_rejected_view_push_leaves_no_trace_across_restart(own_server):
    """A push addressed to a view tid is rejected before anything commits, so
    nothing divergent reaches the view's output store or its checkpoint. The
    graceful shutdown persists every view store and the restart *resumes* from
    that checkpoint (rebuilt count 0) — so the post-restart scan reads the
    checkpointed store itself, which must be byte-for-byte the circuit's own
    output. A rebuild would re-derive the view from base and silently repair
    any corruption, proving nothing."""
    sock_path = own_server.sock_path

    own_server.start()
    conn = gnitz.connect(sock_path)
    conn.create_schema("vguard")
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name="vguard",
    )
    conn.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
        schema_name="vguard",
    )
    conn.execute_sql("INSERT INTO t VALUES (1, 100), (2, 10)", schema_name="vguard")
    vid, v_schema = conn.resolve_table("vguard", "v")
    before = sorted((r.pk, r.val, r.weight) for r in conn.scan(vid))
    assert before == [(1, 100, 1)], f"pre-push view content wrong: {before}"

    batch = gnitz.ZSetBatch(v_schema)
    batch.append(pk=999, val=999)
    with pytest.raises(gnitz.GnitzError, match="not writable"):
        conn.push(vid, batch)
    with pytest.raises(gnitz.GnitzError, match="not writable"):
        conn.delete(vid, v_schema, [1])
    conn.close()

    own_server.restart(graceful=True)
    conn = gnitz.connect(sock_path)
    vid2, _ = conn.resolve_table("vguard", "v")
    after = sorted((r.pk, r.val, r.weight) for r in conn.scan(vid2))
    assert after == before, f"view changed across restart: {before} -> {after}"
    conn.close()

    n = own_server.rebuilt_view_count()
    assert n == 0, f"restart must resume from the checkpoint, but rebuilt {n} view(s)"


def test_nonexchange_view_retains_unflushed_data_after_restart(own_server):
    """A non-exchange (projection) view must reflect ACKed-but-unflushed base
    rows after a crash restart. The INSERT is fdatasync'd to the SAL but never
    checkpointed to a shard, so recovery replays it from the SAL — and the view
    must be rebuilt to include it, exactly as an exchange view (JOIN/GROUP BY)
    would be. The worker post-recovery pass rebuilds this cascade-unreachable
    non-exchange view from the recovered base store."""
    sock_path = own_server.sock_path

    # --- Phase 1: create non-exchange view, insert (no explicit flush) ---
    own_server.start()
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

    rows = {r["pk"]: r for r in conn.scan(vid)}
    assert rows[7]["plus1"] == 71, "pre-restart: view should reflect the insert"

    conn.close()
    own_server.stop()  # SIGKILL — the insert is in the SAL, not a shard

    # --- Phase 2: restart; the unflushed row must survive in the view ---
    own_server.start()
    conn = gnitz.connect(sock_path)

    vid2, _ = conn.resolve_table("nxv", "v")
    rows = {r["pk"]: r for r in conn.scan(vid2)}
    assert 7 in rows, f"post-restart: pk=7 missing from view, got {rows}"
    assert rows[7]["plus1"] == 71, (
        f"post-restart: expected plus1=71, got {rows[7]['plus1']}"
    )

    conn.close()


def _weights_by(rows, key):
    """Sum scan-row weights per `key` value (robust to consolidation form)."""
    out = {}
    for r in rows:
        out[r[key]] = out.get(r[key], 0) + r.weight
    return out


# Flushes the base table to shards before the crash: a small SAL checkpoint
# threshold makes the bulk INSERT's committer cycle run a checkpoint.
_FLUSH_ENV = {"GNITZ_CHECKPOINT_BYTES": "1024"}


def test_nonexchange_view_flushed_data_after_restart(own_server):
    """Flushed-data control for the non-exchange-view rebuild: with the base
    already on shards, the projection view must come back with every row exactly
    once (weight 1) — guarding that removing the inline open-time backfill did
    not regress the already-checkpointed path."""
    sock_path = own_server.sock_path
    own_server.start(extra_env=_FLUSH_ENV)
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

    own_server.restart(extra_env=_FLUSH_ENV)
    conn = gnitz.connect(sock_path)
    vid, _ = conn.resolve_table("nxvf", "v")
    w = _weights_by(conn.scan(vid), "pk")
    assert len(w) == 200, f"expected 200 view rows, got {len(w)}"
    assert all(x == 1 for x in w.values()), \
        f"flushed rows double-counted: weights {sorted(set(w.values()))}"
    conn.close()


def test_nested_nonexchange_views_after_restart(own_server):
    """Two stacked non-exchange views over one base, no exchange view anywhere —
    both are cascade-unreachable and rebuilt by the worker post-recovery pass.
    The depth sort must fill the inner view first so the outer view's backfill
    reads a populated source. Unflushed data, so the rows live only in the SAL at
    crash time."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)
    v1, _ = conn.resolve_table("nest", "v1")
    v2, _ = conn.resolve_table("nest", "v2")
    r1 = {r["pk"]: r for r in conn.scan(v1)}
    r2 = {r["pk"]: r for r in conn.scan(v2)}
    assert _weights_by(conn.scan(v1), "pk") == {3: 1, 4: 1}, "v1 not single-counted"
    assert _weights_by(conn.scan(v2), "pk") == {3: 1, 4: 1}, "v2 not single-counted"
    assert r1[3]["a"] == 31 and r1[4]["a"] == 41, f"v1 wrong: {r1}"
    assert r2[3]["b"] == 32 and r2[4]["b"] == 42, f"v2 wrong: {r2}"
    conn.close()


def test_nonexchange_sibling_of_exchange_view_single_counted(own_server):
    """Defect-2 regression: a non-exchange view (`vn`) sharing a base with an
    exchange view (`vx`) must be filled exactly once. The exchange cascade drives
    the shared base and re-derives `vn`, so the worker pass must skip it — before
    the fix the flushed rows carried weight 2 (inline open-time backfill plus the
    cascade). Flushed data, asserted at weight 1."""
    sock_path = own_server.sock_path
    own_server.start(extra_env=_FLUSH_ENV)
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

    own_server.restart(extra_env=_FLUSH_ENV)
    conn = gnitz.connect(sock_path)
    vn, _ = conn.resolve_table("sib", "vn")
    vx, _ = conn.resolve_table("sib", "vx")

    w = _weights_by(conn.scan(vn), "pk")
    assert len(w) == 200, f"vn missing rows: {len(w)}/200"
    assert all(x == 1 for x in w.values()), \
        f"vn double-counted: weights {sorted(set(w.values()))}"

    got_vx = {r["g"]: r["s"] for r in conn.scan(vx)}
    assert got_vx == exp_vx, f"vx wrong: got {got_vx}, want {exp_vx}"
    conn.close()


def test_nonexchange_view_over_exchange_view_after_restart(own_server):
    """A non-exchange view layered over an exchange view (`vn` over `vx`) is
    cascade-reachable (the cascade traverses base → vx → vn), so the worker pass
    must skip it and the master cascade fills it exactly once. Unflushed data:
    the rows must reach `vn` through the rebuilt `vx`."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)
    vn, _ = conn.resolve_table("nxoe", "vn")
    got = {r["g"]: r["s1"] for r in conn.scan(vn)}
    assert got == exp_vn, f"vn wrong after restart: got {got}, want {exp_vn}"
    # Each group is one row, exactly once.
    assert _weights_by(conn.scan(vn), "g") == {g: 1 for g in exp_vn}, \
        "vn group rows not single-counted"
    conn.close()


# ---------------------------------------------------------------------------
# Durability tests: ACKed operations survive SIGKILL
# ---------------------------------------------------------------------------

def test_dml_insert_survives_crash(own_server):
    """ACKed INSERTs survive SIGKILL (WAL + SAL replay)."""
    sock_path = own_server.sock_path
    own_server.start()
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
    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    assert tid2 == tid
    rows_after = conn.scan(tid2)
    assert len(rows_after) == 50, f"expected 50 rows, got {len(rows_after)}"

    vals_before = sorted((r["pk"], r["a"], r["b"]) for r in rows_before)
    vals_after = sorted((r["pk"], r["a"], r["b"]) for r in rows_after)
    assert vals_before == vals_after, "row data changed after crash recovery"

    conn.close()


def test_dml_update_survives_crash(own_server):
    """ACKed UPDATEs (upsert = retract + insert) survive SIGKILL."""
    sock_path = own_server.sock_path
    own_server.start()
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

    rows_before = {r["pk"]: r["val"] for r in conn.scan(tid)}
    assert rows_before == {1: 100, 2: 999, 3: 300}
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    rows_after = {r["pk"]: r["val"] for r in conn.scan(tid2)}
    assert rows_after == {1: 100, 2: 999, 3: 300}, (
        f"update lost after crash: {rows_after}"
    )

    conn.close()


def test_dml_delete_survives_crash(own_server):
    """ACKed DELETEs survive SIGKILL."""
    sock_path = own_server.sock_path
    own_server.start()
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

    rows_before = {r["pk"]: r["val"] for r in conn.scan(tid)}
    assert rows_before == {1: 100, 3: 300}
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    rows_after = {r["pk"]: r["val"] for r in conn.scan(tid2)}
    assert rows_after == {1: 100, 3: 300}, (
        f"delete lost after crash: {rows_after}"
    )

    conn.close()


def test_ddl_create_table_survives_crash(own_server):
    """ACKed CREATE TABLE survives SIGKILL (SAL DDL_SYNC replay)."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
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


def test_ddl_create_view_survives_crash(own_server):
    """ACKed CREATE VIEW survives SIGKILL. View processes new data after restart."""
    sock_path = own_server.sock_path
    own_server.start()
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
    rows = list(conn.scan(vid))
    assert len(rows) == 1 and rows[0]["inc"] == 11
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)

    vid_r, _ = conn.resolve_table("dur", "v")
    assert vid_r == vid

    # Insert new data — view circuit must be alive
    conn.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name="dur")
    rows = {r["pk"]: r["inc"] for r in conn.scan(vid_r)}
    assert rows[2] == 21, f"view not processing after crash: {rows}"

    conn.close()


def test_ddl_drop_table_survives_crash(own_server):
    """ACKed DROP TABLE stays dropped after SIGKILL (no phantom tables)."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)

    with pytest.raises(Exception):
        conn.resolve_table("dur", "t")

    conn.close()


def test_multiple_ddl_batch_survives_crash(own_server):
    """Multiple DDLs in rapid succession all survive SIGKILL.

    Exercises the DDL response batching path — each DDL is a separate
    SAL broadcast, but fdatasync is deferred to end-of-cycle.
    """
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
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


def test_interleaved_ddl_dml_survives_crash(own_server):
    """Interleaved DDL + DML all survive SIGKILL.

    Tests the interaction between DDL batching (signal-only + deferred sync)
    and DML (sync_and_signal_all on push).
    """
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)

    # All DDL survived
    tid1_r, _ = conn.resolve_table("dur", "t1")
    tid2_r, _ = conn.resolve_table("dur", "t2")
    vid1_r, _ = conn.resolve_table("dur", "v1")
    assert tid1_r == tid1
    assert tid2_r == tid2
    assert vid1_r == vid1

    # All DML survived
    t1_rows = {r["pk"]: r["val"] for r in conn.scan(tid1_r)}
    assert t1_rows == {1: 10, 2: 30}, f"t1 data lost: {t1_rows}"

    t2_rows = {r["pk"]: r["val"] for r in conn.scan(tid2_r)}
    assert t2_rows == {1: 20}, f"t2 data lost: {t2_rows}"

    # View is functional after restart
    conn.execute_sql("INSERT INTO t1 VALUES (3, 50)", schema_name="dur")
    v_rows = {r["pk"]: r["doubled"] for r in conn.scan(vid1_r)}
    assert v_rows[3] == 100, f"view broken after crash: {v_rows}"

    conn.close()


def test_no_phantom_data_after_crash(own_server):
    """Uncommitted data does not appear after SIGKILL.

    Insert data, crash, restart, insert more data with same PKs.
    If phantom rows from an incomplete write existed, the second insert
    would conflict or produce wrong counts.
    """
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")

    # Re-insert same PKs with different values via explicit UPSERT
    conn.execute_sql(
        "INSERT INTO t VALUES (1, 111), (2, 222) "
        "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
        schema_name="dur",
    )
    rows = {r["pk"]: r["val"] for r in conn.scan(tid2)}
    assert rows == {1: 111, 2: 222}, f"phantom or stale data: {rows}"

    conn.close()


def test_sequence_monotonicity_after_crash(own_server):
    """Table ID allocator does not reuse IDs after crash recovery."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
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


def test_schema_id_not_reused_after_crash_before_checkpoint(own_server):
    """CREATE SCHEMA, hard-crash before any checkpoint, CREATE another schema:
    the two schema_ids must differ. Pre-fix, next_schema_id recovers stale from
    the unflushed sys_sequences shard and the second CREATE re-allocates the
    first id. Schema analog of test_sequence_monotonicity_after_crash."""
    sock_path = own_server.sock_path
    own_server.start()
    conn = gnitz.connect(sock_path)
    sid1 = conn.create_schema("crash_s1")
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)
    sid2 = conn.create_schema("crash_s2")
    conn.close()

    assert sid2 > sid1, (
        f"schema_id reused after crash-before-checkpoint: "
        f"crash_s1={sid1}, crash_s2={sid2}"
    )


def test_double_crash_recovery(own_server):
    """Data survives two consecutive crashes without any clean shutdown."""
    sock_path = own_server.sock_path
    own_server.start()
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
    own_server.restart()
    conn = gnitz.connect(sock_path)

    conn.execute_sql("INSERT INTO t VALUES (2, 200)", schema_name="dur")
    conn.close()

    # Second crash
    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    assert tid2 == tid
    rows = {r["pk"]: r["val"] for r in conn.scan(tid2)}
    assert rows == {1: 100, 2: 200}, f"data lost after double crash: {rows}"

    conn.close()


# ---------------------------------------------------------------------------
# Multi-worker durability (SAL replay across forked workers)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_dml_survives_crash(own_server):
    """Multi-worker: ACKed INSERTs across partitions survive SIGKILL."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    rows_after = sorted((r["pk"], r["val"]) for r in conn.scan(tid2))
    assert rows_after == rows_before, (
        f"multi-worker data mismatch: {len(rows_after)} rows"
    )

    conn.close()


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_ddl_batch_survives_crash(own_server):
    """Multi-worker: rapid DDL sequence survives SIGKILL.

    Exercises the signal-only broadcast_ddl + deferred fdatasync path
    in the multi-worker configuration where SAL entries are replayed
    into all worker processes on recovery.
    """
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
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


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_view_survives_crash(own_server):
    """Multi-worker: view + data survive SIGKILL, view processes new data."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)

    vid_r, _ = conn.resolve_table("dur", "v")
    assert vid_r == vid

    # Push new data — view must process it across workers
    conn.execute_sql("INSERT INTO t VALUES (999, 1)", schema_name="dur")
    v_rows_raw = conn.scan(vid_r)
    v_rows = {r["pk"]: r["big"] for r in v_rows_raw}
    assert v_rows.get(999) == 1001, (
        f"view broken after multi-worker crash: "
        f"v_count={len(v_rows)}, v_has_999={999 in v_rows}, vid={vid_r}"
    )

    conn.close()


# ---------------------------------------------------------------------------
# Edge-case durability tests
# ---------------------------------------------------------------------------

def test_string_column_survives_crash(own_server):
    """VARCHAR columns (inline + out-of-line blob encoding) survive SIGKILL."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
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


def test_drop_view_survives_crash(own_server):
    """ACKed DROP VIEW stays dropped after SIGKILL (retraction durability)."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
    conn = gnitz.connect(sock_path)

    # View must stay dropped
    with pytest.raises(Exception):
        conn.resolve_table("dur", "v")

    # Base table unaffected
    tid, _ = conn.resolve_table("dur", "t")
    rows = {r["pk"]: r["val"] for r in conn.scan(tid)}
    assert rows == {1: 10}

    conn.close()


def test_fk_constraint_survives_crash(own_server):
    """FK constraints are enforced after crash recovery."""
    sock_path = own_server.sock_path
    own_server.start()
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

    own_server.restart()
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
    rows = {r["pk"]: r["pid"] for r in conn.scan(ctid)}
    assert rows == {1: 10, 3: 10}

    conn.close()


def test_multiple_upserts_same_pk_crash(own_server):
    """Multiple updates to the same PK before crash: final value survives."""
    sock_path = own_server.sock_path
    own_server.start()
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

    rows_before = {r["pk"]: r["val"] for r in conn.scan(tid)}
    assert rows_before == {1: 400, 2: 20}
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    rows_after = {r["pk"]: r["val"] for r in conn.scan(tid2)}
    assert rows_after == {1: 400, 2: 20}, (
        f"multi-upsert data wrong after crash: {rows_after}"
    )

    conn.close()


def test_nullable_columns_survive_crash(own_server):
    """Nullable columns with NULL values survive SIGKILL."""
    sock_path = own_server.sock_path
    own_server.start()
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
            for r in conn.scan(tid)
        }

    rows_before = snapshot(tid)
    assert rows_before[1] == (None, 10, 100)
    assert rows_before[2] == (20, 20, None)
    assert rows_before[3] == (None, 30, None)
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)

    tid2, _ = conn.resolve_table("dur", "t")
    rows_after = snapshot(tid2)
    assert rows_after == rows_before, (
        f"nullable data wrong after crash: {rows_after}"
    )

    conn.close()


# ---------------------------------------------------------------------------
# SAL checkpoint boundary durability (multi-worker only)
# ---------------------------------------------------------------------------

_CHECKPOINT_ENV = {"GNITZ_CHECKPOINT_BYTES": "262144"}  # 256 KB


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_crash_across_checkpoint(own_server):
    """Data spanning a SAL checkpoint boundary survives SIGKILL.

    Uses GNITZ_CHECKPOINT_BYTES=32KB so a checkpoint triggers after a few
    hundred rows.  Inserts data before AND after the checkpoint, then
    crashes.  Both pre-checkpoint (in shards) and post-checkpoint (in SAL)
    data must survive.
    """
    sock_path = own_server.sock_path
    own_server.start(extra_env=_CHECKPOINT_ENV)
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

    own_server.restart(extra_env=_CHECKPOINT_ENV)
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


@pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)
def test_multiworker_crash_after_checkpoint(own_server):
    """All data in shards (SAL empty after checkpoint) survives SIGKILL.

    Forces a checkpoint, then crashes without writing new data.  Recovery
    must reconstruct everything from shard files alone — no SAL replay.
    """
    sock_path = own_server.sock_path
    own_server.start(extra_env=_CHECKPOINT_ENV)
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
    own_server.restart(extra_env=_CHECKPOINT_ENV)
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


def test_global_aggregate_empty_source_survives_restart(own_server):
    """A global (ungrouped) aggregate over a never-populated table shows one ground
    row (COUNT=0, SUM=NULL). Views are ephemeral and re-derived through the circuit
    at restart, so the seed must re-fire and exactly one ground row must survive."""
    sock_path = own_server.sock_path
    own_server.start()
    conn = gnitz.connect(sock_path)
    conn.create_schema("gae")
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name="gae")
    conn.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
        schema_name="gae")
    vid, _ = conn.resolve_table("gae", "v")
    rows = list(conn.scan(vid))
    assert len(rows) == 1 and rows[0]["cnt"] == 0 and rows[0]["total"] is None
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)
    vid2, _ = conn.resolve_table("gae", "v")
    rows = list(conn.scan(vid2))
    assert len(rows) == 1, f"post-restart: ground row must survive, got {len(rows)}"
    assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
    conn.close()


def test_global_aggregate_emptied_then_restart(own_server):
    """A global aggregate populated then fully deleted shows one ground row; after a
    restart (durable base table is empty) the re-derived view keeps exactly one."""
    sock_path = own_server.sock_path
    own_server.start()
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
    rows = list(conn.scan(vid))
    assert len(rows) == 1 and rows[0]["cnt"] == 0 and rows[0]["lo"] is None
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)
    vid2, _ = conn.resolve_table("gae2", "v")
    rows = list(conn.scan(vid2))
    assert len(rows) == 1, f"post-restart: one ground row, got {len(rows)}"
    assert rows[0]["cnt"] == 0 and rows[0]["lo"] is None
    # And the value returns on a fresh insert after restart.
    conn.execute_sql("INSERT INTO t VALUES (3, 4)", schema_name="gae2")
    r = list(conn.scan(vid2))
    assert len(r) == 1 and r[0]["cnt"] == 1 and r[0]["lo"] == 4
    conn.close()


def test_global_aggregate_worker_count_change_reseeds(own_server):
    """Create a global aggregate at 4 workers, restart at 2: V0's owner partition
    moves to a different worker, so the new owner-bake must re-seed the ground.
    Ephemeral re-derivation + per-process owner-bake must compose to exactly one
    surviving row."""
    sock_path = own_server.sock_path
    own_server.start(workers=4)
    conn = gnitz.connect(sock_path)
    conn.create_schema("gawc")
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name="gawc")
    conn.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
        schema_name="gawc")
    vid, _ = conn.resolve_table("gawc", "v")
    rows = list(conn.scan(vid))
    assert len(rows) == 1 and rows[0]["cnt"] == 0
    conn.close()

    # Restart at a DIFFERENT worker count: V0 owner moves W3 -> W1.
    own_server.restart(workers=2)
    conn = gnitz.connect(sock_path)
    vid2, _ = conn.resolve_table("gawc", "v")
    rows = list(conn.scan(vid2))
    assert len(rows) == 1, f"new V0 owner must re-seed exactly one row, got {len(rows)}"
    assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
    conn.close()


def test_global_aggregate_replicated_survives_restart(own_server):
    """A global aggregate over a WITH (replicated=true) empty source shows one
    ground row, and the re-derived view keeps exactly one after a crash restart
    (the backfill empty-epoch + replicated `i_am_owner` disjunct must re-fire)."""
    sock_path = own_server.sock_path
    own_server.start()
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
    rows = list(conn.scan(vid))
    assert len(rows) == 1 and rows[0]["cnt"] == 0 and rows[0]["total"] is None
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)
    vid2, _ = conn.resolve_table("garep", "v")
    rows = list(conn.scan(vid2))
    assert len(rows) == 1, f"replicated ground must survive restart, got {len(rows)}"
    assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
    conn.close()


def _checkpoint_cut(srv, schema, workers=None):
    """Shared 'cut' the checkpoint-resume tests start from: create <schema>.t
    with view v (dbl = val * 2), insert pks 1..5, then graceful-checkpoint-stop
    the server."""
    srv.start(workers=workers)
    conn = gnitz.connect(srv.sock_path)
    conn.create_schema(schema)
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema,
    )
    conn.execute_sql(
        "CREATE VIEW v AS SELECT pk, val * 2 AS dbl FROM t", schema_name=schema,
    )
    for k in range(1, 6):
        conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=schema)
    conn.close()
    rc = srv.stop_graceful()
    assert rc == 0, f"graceful shutdown must exit rc 0, got {rc}"


def test_checkpoint_cut_plus_sigkill_tail_resumes(own_server):
    """A graceful checkpoint (the 'cut'), then more pushes (the 'tail'), then
    SIGKILL. The restart must resume every view from the checkpoint (0 rebuilt) and
    replay only the un-checkpointed tail from the SAL, so the view reflects
    cut + tail."""
    sock_path = own_server.sock_path
    _checkpoint_cut(own_server, "ct")

    # --- Restart (resumes the cut), then push the tail 6..10, SIGKILL. ---
    own_server.start()
    assert own_server.rebuilt_view_count() == 0, "restart after graceful checkpoint must resume the cut"
    conn = gnitz.connect(sock_path)
    for k in range(6, 11):
        conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name="ct")
    conn.close()
    own_server.restart()

    # --- Resume + tail replay: view = cut + tail, all correct. ---
    conn = gnitz.connect(sock_path)
    vid, _ = conn.resolve_table("ct", "v")
    rows = {r["pk"]: r for r in conn.scan(vid)}
    assert set(rows) == set(range(1, 11)), f"view must reflect cut + tail, got {sorted(rows)}"
    for k in range(1, 11):
        assert rows[k]["dbl"] == k * 20, f"pk={k}: expected dbl={k * 20}, got {rows[k]['dbl']}"
    # The checkpoint was generation-valid, so the tail rode a resume, not a rebuild.
    assert own_server.rebuilt_view_count() == 0, "SIGKILL with a valid checkpoint must resume + replay the tail"
    conn.close()


def test_recovery_reset_injection_forces_correct_rebuild(own_server):
    """The recovery-start generation bump guards the reset→boot_checkpoint window:
    a crash there leaves the base durable at cut+tail (boot-flushed) while the
    views are checkpointed at only the cut and the SAL is reset — a stale-resume
    trap. The bump advances the durable generation, so the next restart's verdict
    rejects the stale view and rebuilds it from the (complete) base. Asserts the
    rebuilt view is CORRECT (cut + tail), not stale (cut only)."""
    if not is_debug_build():
        pytest.skip("requires debug build (GNITZ_INJECT_RECOVERY_PANIC seam)")

    sock_path = own_server.sock_path
    _checkpoint_cut(own_server, "ri")

    # --- Restart, push the un-checkpointed tail 6..10, then SIGKILL (tail is
    #     in the SAL, not any view checkpoint). ---
    own_server.start()
    conn = gnitz.connect(sock_path)
    for k in range(6, 11):
        conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name="ri")
    conn.close()
    own_server.stop()

    # --- Restart with the `reset` injection: recovery boot-flushes base to
    #     cut+tail, resets the SAL, bumps the generation, then crashes before
    #     boot_checkpoint. The un-checkpointed tail is now durable ONLY in the
    #     base — the views' checkpoint is still the stale cut. ---
    rc = own_server.start_expecting_exit(
        extra_env={"GNITZ_INJECT_RECOVERY_PANIC": "reset"})
    assert rc != 0, "injected reset panic must crash boot"

    # --- Clean restart: the bumped durable generation makes the verdict reject
    #     the stale view and rebuild it from the complete base. The result must
    #     be cut + tail (1..10), NOT the stale cut (1..5). ---
    own_server.start()
    conn = gnitz.connect(sock_path)
    vid, _ = conn.resolve_table("ri", "v")
    rows = {r["pk"]: r for r in conn.scan(vid)}
    assert set(rows) == set(range(1, 11)), (
        f"gen bump must force a rebuild to the complete base; got {sorted(rows)} "
        f"(a stale resume would show only 1..5)"
    )
    for k in range(1, 11):
        assert rows[k]["dbl"] == k * 20
    assert own_server.rebuilt_view_count() >= 1, "the stale view must have been rebuilt, not resumed"
    conn.close()


# The SAL floor (16 MiB): `checkpoint_before_backfill` reclaims once the write
# cursor passes 1/8 of it (2 MiB), while the committer's own checkpoint waits for
# 3/4 (12 MiB). The phase-2 push below is sized into that gap — past the backfill
# reclaim, short of any checkpoint that would re-stamp the view.
# `GNITZ_LOG_LEVEL=normal` puts the "SAL checkpoint epoch=" line in the log,
# which is how the test proves the reclaim it depends on actually fired.
_BACKFILL_RECLAIM_ENV = {
    "GNITZ_SAL_BYTES": str(16 * 1024 * 1024),
    "GNITZ_LOG_LEVEL": "normal",
}

# Only the SAL byte volume matters, so buy it with wide rows rather than many:
# ~830 B/row puts ~3.3 MiB through the SAL, inside the (2, 12) MiB gap, while
# keeping view maintenance and the phase-3 scan five times cheaper.
_RECLAIM_ROWS = 4_000
_RECLAIM_PAD = "x" * 800


def _sal_checkpoints(srv):
    """How many SAL checkpoint resets this boot has logged."""
    return srv.log_text().count("SAL checkpoint epoch=")


def test_view_is_not_stale_after_backfill_checkpoint(own_server):
    """A CREATE VIEW whose pre-backfill reclaim fires publishes every base table's
    shards and then resets the SAL — so the rows it made durable exist in the base
    and nowhere else. The first view's checkpoint still names the older cut, and
    its manifests must not stay generation-valid across that: a restart would
    resume a view that is silently short, with no SAL tail left to close the gap.

    Fails without the generation bump inside `do_checkpoint`."""
    sock_path = own_server.sock_path

    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            gnitz.ColumnDef("pad", gnitz.TypeCode.STRING)]
    schema = gnitz.Schema(cols)

    # --- Phase 1: table + view, then a graceful stop so `v` is checkpointed. ---
    own_server.start(extra_env=_BACKFILL_RECLAIM_ENV)
    conn = gnitz.connect(sock_path)
    conn.create_schema("stale")
    tid = conn.create_table("stale", "t", cols)
    conn.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name="stale")
    conn.close()
    rc = own_server.stop_graceful()
    assert rc == 0, f"graceful shutdown must exit rc 0, got {rc}"

    # --- Phase 2: push past the reclaim watermark, then CREATE a second view.
    #     Its backfill reclaims: base shards advance, the SAL is discarded. ---
    own_server.start(extra_env=_BACKFILL_RECLAIM_ENV)
    conn = gnitz.connect(sock_path)
    for lo in range(0, _RECLAIM_ROWS, 2000):
        batch = gnitz.ZSetBatch(schema)
        for i in range(lo, lo + 2000):
            batch.append(pk=i, val=i * 10, pad=_RECLAIM_PAD)
        conn.push(tid, batch)

    before = _sal_checkpoints(own_server)
    conn.execute_sql("CREATE VIEW v2 AS SELECT pk FROM t", schema_name="stale")
    conn.close()
    assert _sal_checkpoints(own_server) > before, (
        "the CREATE VIEW backfill must have reclaimed the SAL — otherwise the "
        "un-checkpointed tail survives and this test proves nothing"
    )

    # --- Phase 3: SIGKILL, restart. `v` must hold every pushed row: either it
    #     was re-stamped at the bumped generation, or the verdict rejected it and
    #     rebuilt it from base. A resume at the stale cut shows nothing. ---
    own_server.stop()
    own_server.start(extra_env=_BACKFILL_RECLAIM_ENV)
    conn = gnitz.connect(sock_path)
    vid, _ = conn.resolve_table("stale", "v")
    rows = conn.scan(vid)
    assert len(rows) == _RECLAIM_ROWS, (
        f"view must reflect every base row after the backfill reclaim, "
        f"got {len(rows)} of {_RECLAIM_ROWS}"
    )
    assert sum(r["val"] for r in rows) == sum(i * 10 for i in range(_RECLAIM_ROWS))
    conn.close()


def _create_replicated_join(conn, schema):
    """A replicated `dim`, a partitioned `fact`, and a view joining them.

    The join is the observable both replicated-copy tests read through, because a
    scan of the replicated table itself is single-sourced to worker 0, whose copy
    is current at every worker count — a damaged copy on workers 1..W-1 is
    invisible to a plain SELECT. `fact JOIN dim` skips the exchange on both sides
    and cogroups against each worker's own `dim`, union-gathering the result, so a
    worker with an empty, stale, or duplicated copy shows up in the gathered rows
    and weights."""
    conn.create_schema(schema)
    conn.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) "
        "WITH (replicated = true)", schema_name=schema)
    conn.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL)",
        schema_name=schema)
    conn.execute_sql(
        "CREATE VIEW j AS SELECT f.pk AS pk, d.v AS v "
        "FROM fact f JOIN dim d ON f.dim_pk = d.pk", schema_name=schema)


def _insert_join_pairs(conn, schema, lo, hi):
    """One `dim` row and one matching `fact` row per key in [lo, hi)."""
    conn.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(lo, hi)),
        schema_name=schema)
    conn.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(f"({i}, {i})" for i in range(lo, hi)),
        schema_name=schema)


def test_replicated_join_survives_worker_count_change(own_server):
    """A replicated dim's per-worker copy is addressed by rank, so every launched
    rank must read a copy that is current after a worker-count change.

    The counts run 2 -> 4 -> 3 -> 4. Widening covers the rebuild (ranks 2 and 3
    have no copy and must get one), narrowing covers reclamation plus the surviving
    ranks keeping their own current copies, and the final re-grow covers a boot
    reading a tail a narrower count wrote: a clean shutdown leaves a live group at
    SAL offset 0 written at W=3, and rank 3 must see its absent slot as empty
    rather than as the earlier W=4 run's leftover at the same offset. Every
    transition is a CLEAN shutdown, so the data under test lives in shards rather
    than the un-checkpointed SAL tail.

    The worker counts are hardcoded, so this runs multi-worker even under
    `make e2e WORKERS=1`.
    """
    sock_path = own_server.sock_path

    def join_rows():
        conn = gnitz.connect(sock_path)
        vid, _ = conn.resolve_table("rj", "j")
        rows = {r["pk"]: r["v"] for r in conn.scan(vid)}
        conn.close()
        return rows

    def insert_pairs(conn, lo, hi):
        _insert_join_pairs(conn, "rj", lo, hi)

    own_server.start(workers=2)
    conn = gnitz.connect(sock_path)
    _create_replicated_join(conn, "rj")
    insert_pairs(conn, 0, 64)
    conn.close()

    expected = {i: i * 10 for i in range(64)}
    assert join_rows() == expected, "W=2: every fact must join its dim"

    # --- Widen to 4: ranks 2 and 3 have no copy at all and must be rebuilt
    #     from rank 0's, which is current at every worker count. Without the
    #     rebuild they join against an empty dim and drop their share. ---
    own_server.restart(graceful=True, workers=4)
    assert join_rows() == expected, "W=4: the new ranks must get a current copy"

    # Mutate `dim` at W=4, so the W=3 leg has something a stale copy would miss.
    conn = gnitz.connect(sock_path)
    conn.execute_sql("DELETE FROM dim WHERE pk < 16", schema_name="rj")
    insert_pairs(conn, 64, 80)
    conn.close()
    expected = {i: i * 10 for i in range(16, 80)}
    assert join_rows() == expected, "W=4: the mutation must reach the join"

    # --- Narrow to 3: rank 3's copy is retired, ranks 0..2 keep their own,
    #     which already carry the mutation. ---
    own_server.restart(graceful=True, workers=3)
    assert join_rows() == expected, (
        "W=3: every surviving rank must read its own copy, mutation included"
    )

    # --- Re-grow to 4: rank 3 reads the W=3 shutdown group at SAL offset 0,
    #     which has no slot 3 — but the earlier W=4 epoch's group at that same
    #     offset did. Reading that leftover aborts the worker's recovery. ---
    own_server.restart(graceful=True, workers=4)
    assert join_rows() == expected, (
        "W=4 again: re-growing past a narrower run must boot and read every copy"
    )



# ---------------------------------------------------------------------------
# Un-checkpointed SAL tail across a worker-count change
#
# The SAL is written pre-sliced per worker at the then-current count and read
# back by slot. A restart at a different count must therefore replay every slot
# the tail carries and re-cut each partitioned group for the launched topology,
# or client-ACKed, fdatasync-durable rows are silently lost. The counts below are
# hardcoded, so these run multi-worker even under `make e2e WORKERS=1`.
# ---------------------------------------------------------------------------


def _assert_reslice_ran(srv):
    """The changed-count replay marker in the current boot's output. Proves the
    boot took the re-slicing path rather than the same-count one."""
    text = srv.log_text()
    assert "SAL tail written by" in text, (
        f"restart at a changed worker count must replay every written slot; "
        f"boot output was:\n{text}"
    )


@pytest.mark.parametrize("wrote,launched", [(4, 2), (1, 4)])
def test_tail_survives_worker_count_change(wrote, launched, own_server):
    """SIGKILL with an un-checkpointed tail at `wrote` workers, restart at
    `launched`. Every launched rank must read all `wrote` slots and keep exactly
    the rows its own slice owns — on the growth leg three of the four
    ranks have no slot of their own at all and depend entirely on slot 0 being
    re-cut. Pre-fix, growth lost 150 of 200 rows.

    The table is `CLUSTER BY` + compound PK, so a re-slice that hashed the full PK
    instead of the distribution prefix would misroute; it carries a UNIQUE column,
    so an over-populated secondary index shows up; and the tail ends in a
    DELETE + re-INSERT, so the replay carries a retraction and the assertions test
    weights rather than mere presence."""
    sock_path = own_server.sock_path
    own_server.start(workers=wrote)
    conn = gnitz.connect(sock_path)
    conn.create_schema("ws")
    conn.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, u BIGINT NOT NULL UNIQUE, "
        "PRIMARY KEY (a, b)) CLUSTER BY a",
        schema_name="ws",
    )
    # A pinned key set — 20 distinct `a`, 10 `b` each — so a changed hash fails
    # loudly instead of silently degrading the partition spread.
    vals = ",".join(f"({a}, {b}, {a * 10 + b})" for a in range(20) for b in range(10))
    conn.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name="ws")
    # Still in the same tail: retract one key and re-insert it.
    conn.execute_sql("DELETE FROM t WHERE a = 7 AND b = 3", schema_name="ws")
    conn.execute_sql("INSERT INTO t VALUES (7, 3, 73)", schema_name="ws")
    conn.close()

    own_server.restart(workers=launched)
    _assert_reslice_ran(own_server)

    conn = gnitz.connect(sock_path)
    tid, _ = conn.resolve_table("ws", "t")
    # The raw row list, not a dict: `scan` concatenates per-worker frames with
    # no cross-worker consolidation, so a row that survived on two workers
    # arrives twice at weight 1 — exactly what a botched re-slice produces, and
    # exactly what keying by PK would hide.
    rows = list(conn.scan(tid))
    assert len(rows) == 200, f"expected 200 rows after {wrote} -> {launched}, got {len(rows)}"
    assert all(r.weight == 1 for r in rows), (
        f"every surviving row must be at weight 1, got {sorted({r.weight for r in rows})}"
    )
    assert {(r["a"], r["b"]) for r in rows} == {(a, b) for a in range(20) for b in range(10)}

    # The only index assertion that discriminates: deleting the sole holder of
    # a `u` value and re-inserting that value must SUCCEED. Rejecting a
    # duplicate and admitting a fresh value both pass with an index
    # over-populated by a replayed foreign slot, so they prove nothing.
    conn.execute_sql("DELETE FROM t WHERE a = 4 AND b = 5", schema_name="ws")
    conn.execute_sql("INSERT INTO t VALUES (4, 5, 45)", schema_name="ws")
    conn.close()


def test_replicated_tail_survives_worker_count_shrink(own_server):
    """A replicated table's rows are broadcast into EVERY SAL slot, not sliced, so
    the changed-count replay must take exactly one slot and never re-cut it:
    re-slicing would cut each worker's copy down to its partition share, replaying
    all four slots would leave every row at weight 4."""
    sock_path = own_server.sock_path
    own_server.start(workers=4)
    conn = gnitz.connect(sock_path)
    _create_replicated_join(conn, "rt")
    _insert_join_pairs(conn, "rt", 0, 64)
    conn.close()

    own_server.restart(workers=2)
    _assert_reslice_ran(own_server)

    conn = gnitz.connect(sock_path)
    vid, _ = conn.resolve_table("rt", "j")
    rows = list(conn.scan(vid))
    assert len(rows) == 64, f"expected 64 joined rows after the shrink, got {len(rows)}"
    assert all(r.weight == 1 for r in rows), (
        f"a duplicated replicated copy shows as weight > 1, got "
        f"{sorted({r.weight for r in rows})}"
    )
    assert {(r["pk"], r["v"]) for r in rows} == {(i, i * 10) for i in range(64)}
    conn.close()


def test_checkpoint_cut_plus_changed_count_tail(own_server):
    """Where re-homed shard state meets the tail re-slice: checkpoint at W=4, push
    a tail, SIGKILL, restart at W=2. The base must hold cut + tail exactly once —
    a double-apply (checkpointed row plus replayed row) would show as weight 2.
    The changed count invalidates every view, so unlike the same-count sibling
    this restart rebuilds rather than resumes."""
    sock_path = own_server.sock_path
    _checkpoint_cut(own_server, "cc", workers=4)

    own_server.start(workers=4)
    conn = gnitz.connect(sock_path)
    for k in range(6, 11):
        conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name="cc")
    conn.close()
    own_server.restart(workers=2)

    _assert_reslice_ran(own_server)
    rebuilt = own_server.rebuilt_view_count()
    assert rebuilt is not None and rebuilt >= 1, (
        f"a changed-count restart invalidates every view, so it must rebuild; got {rebuilt}"
    )

    conn = gnitz.connect(sock_path)
    tid, _ = conn.resolve_table("cc", "t")
    rows = list(conn.scan(tid))
    assert len(rows) == 10, f"base must hold cut + tail exactly once, got {len(rows)} rows"
    assert all(r.weight == 1 for r in rows), (
        f"a checkpointed row re-applied from the tail shows as weight 2, got "
        f"{sorted({r.weight for r in rows})}"
    )
    assert {r["pk"] for r in rows} == set(range(1, 11))
    assert all(r["val"] == r["pk"] * 10 for r in rows)
    conn.close()
