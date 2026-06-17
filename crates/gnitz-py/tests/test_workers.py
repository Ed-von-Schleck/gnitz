import os
import random
import multiprocessing
import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _uid():
    return str(random.randint(100000, 999999))


def test_push_scan_multiworker(client):
    """Push 200 rows and scan back — all must be present."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "big", cols)

    n = 200
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, n + 1):
        batch.append(pk=i, val=i * 10)
    client.push(tid, batch)

    result = client.scan(tid)
    pks = sorted(row.pk for row in result if row.weight > 0)
    assert pks == list(range(1, n + 1))

    client.drop_table(sn, "big")
    client.drop_schema(sn)


def _drop_all(client, sn, tables=(), views=(), indices=()):
    for idx in indices:
        try:
            client.execute_sql(f"DROP INDEX {idx}", schema_name=sn)
        except Exception:
            pass
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)


@_NEEDS_MULTI
def test_index_maintained_on_all_workers(client):
    """
    Stage 3 (index projection) must run on every worker.
    Insert rows designed to spread across partitions (range of PKs),
    then seek_by_index for each key — all must be found.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
        n = 64
        vals = ",".join(f"({i}, {i * 100})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        for i in range(1, n + 1):
            result = client.seek_by_index(tid, [1], [i * 100])
            assert result.batch is not None and len(result.batch.pks) == 1, \
                f"cust_id={i * 100} not found via index"
            assert result.batch.pks[0] == i
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_cust_id"], tables=["t"])


@_NEEDS_MULTI
def test_zset_union_invariant(client):
    """
    ZSet union across partitions is commutative/associative — scan must
    return all inserted rows regardless of how they are partitioned.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        n = 200
        vals = ",".join(f"({i}, {i})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        result = client.scan(tid)
        pks = sorted(row.pk for row in result if row.weight > 0)
        assert len(pks) == n, f"expected {n} rows, got {len(pks)}"
        assert pks == list(range(1, n + 1))
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_seek_routes_to_correct_worker(client):
    """PK seek fan-out returns exactly the one matching row from whichever worker owns it."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (42, 999)", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        result = client.seek(tid, pk=42)
        assert result.batch is not None and len(result.batch.pks) == 1
        assert result.batch.pks[0] == 42
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_seek_by_index_broadcast(client):
    """Index seek broadcast returns the correct single row from whichever worker owns it."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (7, 1234)", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        result = client.seek_by_index(tid, [1], [1234])
        assert result.batch is not None and len(result.batch.pks) == 1
        assert result.batch.pks[0] == 7
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_cust_id"], tables=["t"])


@_NEEDS_MULTI
def test_unique_pk_across_workers(client):
    """
    SQL-standard ON CONFLICT DO UPDATE works correctly across workers:
    re-inserting the same PK via explicit UPSERT replaces the row.
    Scan must return exactly 1 row with the updated value.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 200) "
            "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name=sn,
        )

        tid, _ = client.resolve_table(sn, "t")
        result = client.scan(tid)
        rows = [row for row in result if row.weight > 0]
        assert len(rows) == 1, f"expected 1 row after upsert, got {len(rows)}"
        assert rows[0].pk == 1
        assert rows[0].val == 200
    finally:
        _drop_all(client, sn, tables=["t"])


# ---------------------------------------------------------------------------
# New multi-worker tests
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
def test_workers_ddl_create_table(client):
    """Create table while workers running; push rows; scan → all rows present."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        n = 50
        vals = ",".join(f"({i}, {i * 10})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        pks = sorted(r.pk for r in client.scan(tid) if r.weight > 0)
        assert pks == list(range(1, n + 1))
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_workers_view_passthrough(client):
    """SQL passthrough view; push rows; scan view → all rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        pks = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
        assert pks == [1, 2, 3]
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@_NEEDS_MULTI
def test_workers_view_deletes(client):
    """Push inserts then retractions; passthrough view shows only non-retracted rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        pks = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
        assert pks == [1, 3]
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@_NEEDS_MULTI
def test_workers_view_cascade(client):
    """V1 = filter view, V2 = passthrough of V1; push rows; verify V2 has matching rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
            schema_name=sn,
        )
        v1_id, v1_schema = client.resolve_table(sn, "v1")
        v2_id = client.create_view(sn, "v2", v1_id, v1_schema)

        client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 50), (3, 100)", schema_name=sn)

        v1_pks = sorted(r.pk for r in client.scan(v1_id) if r.weight > 0)
        v2_pks = sorted(r.pk for r in client.scan(v2_id) if r.weight > 0)
        assert v1_pks == [2, 3]
        assert v2_pks == [2, 3]
    finally:
        try:
            client.drop_view(sn, "v2")
        except Exception:
            pass
        _drop_all(client, sn, views=["v1"], tables=["t"])


@_NEEDS_MULTI
def test_workers_view_ddl_then_push(client):
    """Push rows THEN create view; push more rows; scan view → all rows via backfill."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        # Push rows BEFORE view creation — should appear via backfill
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)

        client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)

        # Push rows AFTER view creation — should also appear in view
        client.execute_sql("INSERT INTO t VALUES (2, 20), (3, 30)", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        pks = sorted(r.pk for r in client.scan(vid) if r.weight > 0)
        # Pre-creation row now visible via backfill
        assert pks == [1, 2, 3]
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@_NEEDS_MULTI
def test_workers_view_on_view_backfill(client):
    """Insert rows, then create v1 then v2 (view on view); both get backfilled."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 30), (3, 100)", schema_name=sn)

        client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=sn)
        client.execute_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 50", schema_name=sn)

        v1_id, _ = client.resolve_table(sn, "v1")
        v2_id, _ = client.resolve_table(sn, "v2")

        v1_pks = sorted(r.pk for r in client.scan(v1_id) if r.weight > 0)
        v2_pks = sorted(r.pk for r in client.scan(v2_id) if r.weight > 0)
        assert v1_pks == [2, 3], "v1 backfill: expected [2,3] got %s" % v1_pks
        assert v2_pks == [3], "v2 backfill: expected [3] got %s" % v2_pks
    finally:
        _drop_all(client, sn, views=["v2", "v1"], tables=["t"])


@_NEEDS_MULTI
def test_workers_multiple_views_same_table(client):
    """Two views on same table; both get pushed data."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=sn)
        client.execute_sql("CREATE VIEW v2 AS SELECT * FROM t WHERE val > 50", schema_name=sn)

        client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 30), (3, 100)", schema_name=sn)

        v1_id, _ = client.resolve_table(sn, "v1")
        v2_id, _ = client.resolve_table(sn, "v2")

        v1_pks = sorted(r.pk for r in client.scan(v1_id) if r.weight > 0)
        v2_pks = sorted(r.pk for r in client.scan(v2_id) if r.weight > 0)
        assert v1_pks == [2, 3]
        assert v2_pks == [3]
    finally:
        _drop_all(client, sn, views=["v1", "v2"], tables=["t"])


@_NEEDS_MULTI
def test_workers_upsert(client):
    """Insert PK=1 val=10, then UPSERT PK=1 val=99; scan → one row with val=99."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 99) "
            "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name=sn,
        )

        tid, _ = client.resolve_table(sn, "t")
        rows = [r for r in client.scan(tid) if r.weight > 0]
        assert len(rows) == 1
        assert rows[0].pk == 1
        assert rows[0].val == 99
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_workers_delete_by_pk(client):
    """Push 3 rows; delete PK=2; scan → 2 rows (PK=1,3)."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        pks = sorted(r.pk for r in client.scan(tid) if r.weight > 0)
        assert pks == [1, 3]
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_workers_reduce_sum(client):
    """SQL SUM GROUP BY across partitions produces correct per-group sums."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn,
        )
        # Push 100 rows across two groups
        vals = ",".join(
            f"({i}, {1 if i % 2 == 0 else 2}, {i})" for i in range(1, 101)
        )
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        rows = [r for r in client.scan(vid) if r.weight > 0]
        totals = {r[1]: r[2] for r in rows}

        # group 1: even PKs (2,4,...,100) — sum = 2+4+...+100 = 2550
        # group 2: odd PKs (1,3,...,99) — sum = 1+3+...+99 = 2500
        assert totals[1] == sum(i for i in range(1, 101) if i % 2 == 0)
        assert totals[2] == sum(i for i in range(1, 101) if i % 2 != 0)
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@_NEEDS_MULTI
def test_workers_reduce_incremental(client):
    """7 ticks of inserts/deletes through reduce SUM view: exercises view store L0 compaction."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Tick 1: sum=300
        client.execute_sql(
            "INSERT INTO t VALUES (1, 1, 100), (2, 1, 200)", schema_name=sn
        )
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 300

        # Tick 2: delete pk=2 → sum=100
        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 100

        # Tick 3: insert (3,1,50) → sum=150
        client.execute_sql("INSERT INTO t VALUES (3, 1, 50)", schema_name=sn)
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 150

        # Tick 4: insert (4,1,75) → sum=225
        client.execute_sql("INSERT INTO t VALUES (4, 1, 75)", schema_name=sn)
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 225

        # Tick 5: delete pk=1 → sum=125  [compaction fires here]
        client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 125

        # Tick 6: insert (5,1,30) → sum=155
        client.execute_sql("INSERT INTO t VALUES (5, 1, 30)", schema_name=sn)
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 155

        # Tick 7: delete pk=3 → sum=105
        client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
        assert {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}[1] == 105
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# ---------------------------------------------------------------------------
# Phase 4: Exchange round-trip elimination
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
def test_trivial_preplan_no_exchange(client):
    """
    4b+4c: Trivial non-co-partitioned pre-plan (GROUP BY grp, shard col != pk).
    Master pre-sends FLAG_PRELOADED_EXCHANGE; worker uses stash instead of IPC round-trip.
    Correct COUNT result verifies the preload mechanism works.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp",
            schema_name=sn,
        )
        # Push 100 rows across 2 groups, multiple transactions
        for batch_start in range(0, 100, 20):
            vals = ",".join(
                f"({batch_start + i + 1}, {(batch_start + i) % 2 + 1}, {batch_start + i})"
                for i in range(20)
            )
            client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        rows = {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}
        # 50 rows in each group
        assert rows[1] == 50, f"group 1 count: expected 50, got {rows.get(1)}"
        assert rows[2] == 50, f"group 2 count: expected 50, got {rows.get(2)}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@_NEEDS_MULTI
def test_copartitioned_view_no_exchange(client):
    """
    4d: Co-partitioned view (GROUP BY id where id == pk).
    skip_exchange=True: worker processes pre-result directly without IPC exchange.
    Correct SUM result verifies co-partitioned elimination works.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, SUM(val) AS total FROM t GROUP BY id",
            schema_name=sn,
        )
        n = 50
        vals = ",".join(f"({i}, {i * 10})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        # The group column `id` coincides with the source PK, so it is the view's
        # natural PK column (read by name), not a duplicated payload column.
        rows = {r["id"]: r["total"] for r in client.scan(vid) if r.weight > 0}
        for i in range(1, n + 1):
            assert rows.get(i) == i * 10, \
                f"id={i}: expected {i * 10}, got {rows.get(i)}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@_NEEDS_MULTI
def test_copartitioned_join(client):
    """
    4d: Co-partitioned join (join on PK column).
    co_partitioned_join_sources eliminates exchange for PK-keyed join.
    Correct join results across multiple pushes verifies the optimization.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT a.id, a.x, b.y FROM a JOIN b ON a.id = b.id",
            schema_name=sn,
        )
        n = 30
        a_vals = ",".join(f"({i}, {i * 2})" for i in range(1, n + 1))
        b_vals = ",".join(f"({i}, {i * 3})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        rows = client.scan(vid)
        live_rows = [r for r in rows if r.weight > 0]
        assert len(live_rows) == n, f"expected {n} join rows, got {len(live_rows)}"
        by_id = {r[1]: r for r in live_rows}
        for i in range(1, n + 1):
            r = by_id.get(i)
            assert r is not None, f"id={i} missing from join result"
            assert r[2] == i * 2, f"id={i} x: expected {i * 2}, got {r[2]}"
            assert r[3] == i * 3, f"id={i} y: expected {i * 3}, got {r[3]}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Concurrent push worker (module-level for pickling)
# ---------------------------------------------------------------------------

def _concurrent_push_worker(server_path, tid, pk_start, pk_end, barrier):
    """Worker function: barrier sync then push a batch of rows."""
    import gnitz as _gnitz
    barrier.wait()
    cols = [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
            _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)]
    schema = _gnitz.Schema(cols)
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(pk_start, pk_end):
            batch.append(pk=pk, val=pk)
        c.push(tid, batch)


def _concurrent_multi_table_worker(server_path, tid, pk_start, pk_end, barrier):
    """Worker function for multi-table concurrent test."""
    import gnitz as _gnitz
    barrier.wait()
    cols = [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
            _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)]
    schema = _gnitz.Schema(cols)
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(pk_start, pk_end):
            batch.append(pk=pk, val=pk)
        c.push(tid, batch)


@_NEEDS_MULTI
def test_workers_concurrent_push_same_table(client, server):
    """8 processes barrier-synced push non-overlapping PK ranges; scan → all rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)

        n_procs = 8
        rows_per_proc = 25
        total = n_procs * rows_per_proc
        barrier = multiprocessing.Barrier(n_procs)

        procs = []
        for i in range(n_procs):
            pk_start = i * rows_per_proc + 1
            pk_end = pk_start + rows_per_proc
            p = multiprocessing.Process(
                target=_concurrent_push_worker,
                args=(server, tid, pk_start, pk_end, barrier),
            )
            procs.append(p)

        for p in procs:
            p.start()
        for p in procs:
            p.join()

        for p in procs:
            assert p.exitcode == 0, f"Worker exited with code {p.exitcode}"

        result = client.scan(tid)
        pks = sorted(r.pk for r in result if r.weight > 0)
        assert pks == list(range(1, total + 1))
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_workers_concurrent_push_multi_table(client, server):
    """12 processes push to 4 tables (3 each); scan each → all rows present."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        n_tables = 4
        n_procs_per_table = 3
        rows_per_proc = 20

        tids = []
        for i in range(n_tables):
            tid = client.create_table(sn, f"t{i}", cols)
            tids.append(tid)

        total_per_table = n_procs_per_table * rows_per_proc
        barrier = multiprocessing.Barrier(n_tables * n_procs_per_table)

        procs = []
        for ti, tid in enumerate(tids):
            for pi in range(n_procs_per_table):
                pk_start = pi * rows_per_proc + 1
                pk_end = pk_start + rows_per_proc
                p = multiprocessing.Process(
                    target=_concurrent_multi_table_worker,
                    args=(server, tid, pk_start, pk_end, barrier),
                )
                procs.append(p)

        for p in procs:
            p.start()
        for p in procs:
            p.join()

        for p in procs:
            assert p.exitcode == 0, f"Worker exited with code {p.exitcode}"

        for i, tid in enumerate(tids):
            pks = sorted(r.pk for r in client.scan(tid) if r.weight > 0)
            assert len(pks) == total_per_table, (
                f"table t{i}: expected {total_per_table} rows, got {len(pks)}"
            )
    finally:
        _drop_all(client, sn, tables=[f"t{i}" for i in range(n_tables)])


def test_cross_table_push_scan_isolation(client):
    """Push to table A while scanning table B — scan should not
    cause unnecessary flush of table A's pending pushes.  Per-table
    read barriers ensure cross-table isolation."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        cols = [
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64),
        ]
        schema = gnitz.Schema(cols)
        tid_a = client.create_table(sn, "a", cols)
        tid_b = client.create_table(sn, "b", cols)

        batch_a = gnitz.ZSetBatch(schema)
        for i in range(10):
            batch_a.append(pk=i + 1, val=i * 10)
        client.push(tid_a, batch_a)

        # Scan table B (empty) — must succeed without affecting table A
        result_b = client.scan(tid_b)
        assert len(result_b) == 0

        # Scan table A — all 10 rows must be present
        result_a = client.scan(tid_a)
        pks = sorted(row.pk for row in result_a if row.weight > 0)
        assert len(pks) == 10
    finally:
        _drop_all(client, sn, tables=["a", "b"])


@_NEEDS_MULTI
def test_workers_distinct_view(client):
    """SELECT DISTINCT view with GNITZ_WORKERS=4."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT DISTINCT * FROM t",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        # Insert enough rows to spread across workers
        vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 51))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 50, f"expected 50, got {len(rows)}"

        # Delete a subset and verify retraction
        client.execute_sql("DELETE FROM t WHERE pk = 10", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 25", schema_name=sn)
        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 48, f"expected 48 after deletes, got {len(rows)}"
    finally:
        _drop_all(client, sn, tables=["t"], views=["v"])


@_NEEDS_MULTI
def test_workers_except_multiworker(client):
    """EXCEPT (anti-join) with multi-worker distribution."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]

        # Insert b first (exclusion set)
        b_vals = ", ".join(f"({i}, {i * 10})" for i in [5, 10, 15, 20])
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        # Insert a (full set) — 20 rows, 4 overlap with b
        a_vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 21))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)

        rows = client.scan(vid)
        pks = sorted(r["pk"] for r in rows)
        expected = sorted(set(range(1, 21)) - {5, 10, 15, 20})
        assert pks == expected, f"expected {expected}, got {pks}"
    finally:
        _drop_all(client, sn, tables=["a", "b"], views=["v"])


@_NEEDS_MULTI
def test_empty_batch_barrier(client):
    """With multiple workers, inserting 1 row means all but one worker get an
    empty batch.  The barrier must still complete without deadlock."""
    sn = "eb" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "t", cols)

        batch = gnitz.ZSetBatch(schema)
        batch.append(pk=1, val=10)
        client.push(tid, batch)

        result = client.scan(tid)
        pks = sorted(row.pk for row in result if row.weight > 0)
        assert pks == [1]
    finally:
        _drop_all(client, sn, tables=["t"])


# ---------------------------------------------------------------------------
# Deadlock regression: PK rejection broadcast during active view tick
# ---------------------------------------------------------------------------
#
# These tests pin down a deadlock observed in the full benchmark suite during
# `test_incremental_cost[filter]` after the SQL-standard INSERT rejection
# commit (0eaff28). The new code path runs `validate_all_distributed` →
# `execute_pipeline` synchronously on every INSERT to a unique-PK table in
# Error mode. `execute_pipeline` reads from and resets the shared w2m ring
# cursors. When this happens while an async view-propagation tick is still
# in progress, the tick's worker ACKs are consumed (or their cursor positions
# are clobbered) and the master then waits forever in `poll_tick_progress`.
#
# Symptom: master sleeps in `poll_tick_progress`, all workers idle in poll(),
# client blocked in `unix_stream_data_wait`, no data dir activity.

def _run_with_deadlock_timeout(target, args, seconds, label):
    """Run `target(*args)` in a subprocess; kill and fail on hang.

    `signal.alarm` does not interrupt blocking C-level socket reads in the
    gnitz client, so we isolate the work in a child process and use
    `Process.join(timeout=)` to detect deadlocks.
    """
    proc = multiprocessing.Process(target=target, args=args)
    proc.start()
    proc.join(seconds)
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        if proc.is_alive():
            proc.kill()
            proc.join()
        raise AssertionError(
            f"DEADLOCK: {label} did not complete within {seconds}s"
        )
    assert proc.exitcode == 0, (
        f"{label}: child exited with code {proc.exitcode}"
    )


def _insert_loop_with_view_child(server_path, sn):
    """Child process: INSERT VALUES in a loop on a table that has a view."""
    import gnitz as _gnitz
    with _gnitz.connect(server_path) as c:
        for i in range(20):
            vals = ",".join(
                f"({i * 50 + j}, {j * 10})" for j in range(1, 51)
            )
            c.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)


@_NEEDS_MULTI
def test_insert_sql_with_filter_view_no_deadlock(client, server):
    """Repeated INSERT VALUES on a unique-PK table while a filter view is
    propagating must not deadlock the master.

    Each INSERT triggers the new PK-rejection broadcast in
    `validate_all_distributed`, which calls `execute_pipeline` and resets
    the w2m cursors. The view present on the table forces async tick state.
    """
    sn = "vd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM t WHERE val > 100",
            schema_name=sn,
        )

        _run_with_deadlock_timeout(
            _insert_loop_with_view_child,
            (server, sn),
            seconds=30,
            label="INSERT VALUES loop with view",
        )

        tid, _ = client.resolve_table(sn, "t")
        rows = sorted(r.pk for r in client.scan(tid) if r.weight > 0)
        assert len(rows) == 20 * 50
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


def _push_then_insert_child(server_path, sn, tid, n_bulk):
    """Child process: bulk push (triggers async tick), then INSERT loop."""
    import gnitz as _gnitz
    cols = [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
            _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)]
    schema = _gnitz.Schema(cols)
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(1, n_bulk + 1):
            batch.append(pk=pk, val=pk * 100)
        c.push(tid, batch)

        pk_base = n_bulk + 1
        for i in range(50):
            vals = ",".join(
                f"({pk_base + i * 100 + j}, "
                f"{(pk_base + i * 100 + j) * 100})"
                for j in range(100)
            )
            c.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)


@_NEEDS_MULTI
def test_partition_balance_wide_u64_range(client):
    """Push U64 PKs spanning a wide range; verify all rows are present on scan.

    Uses PKs drawn from low, mid, and high regions of the U64 space to
    exercise the XXH3-based hash partitioning with narrow (8-byte) PK storage.
    The scan result must contain every inserted PK exactly once.
    """
    sn = "pb" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        # PKs from low, mid, and high ranges of u64 (cast to signed BIGINT for SQL)
        import ctypes
        def _to_signed(v):
            return ctypes.c_int64(v).value

        low_pks = list(range(1, 33))
        mid_val = (1 << 32)
        mid_pks = [mid_val + i for i in range(32)]
        high_val = (1 << 62)
        high_pks = [high_val + i for i in range(32)]
        all_pks = low_pks + mid_pks + high_pks

        vals = ", ".join(f"({_to_signed(pk)}, {pk & 0xFFFF})" for pk in all_pks)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        result = client.scan(tid)
        returned_pks = sorted(
            ctypes.c_uint64(r.pk).value for r in result if r.weight > 0
        )
        assert returned_pks == sorted(all_pks), (
            f"expected {len(all_pks)} distinct PKs, got {len(returned_pks)}"
        )
    finally:
        _drop_all(client, sn, tables=["t"])


@_NEEDS_MULTI
def test_push_then_insert_sql_with_view_no_deadlock(client, server):
    """Bulk push (kicks off async tick) immediately followed by INSERT VALUES.

    This is the exact pattern that hung in `test_incremental_cost[filter]`:
    a large `client.push()` triggers view propagation, then a sequence of
    `INSERT VALUES` SQL statements race against the still-running tick. Each
    INSERT goes through the new PK-rejection synchronous broadcast that
    pollutes the shared w2m cursors.

    Scale tuned to mimic the benchmark: 50k bulk push + 50 × 100-row INSERTs.
    """
    sn = "pd" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM t WHERE val > 500000",
            schema_name=sn,
        )

        n_bulk = 50_000
        _run_with_deadlock_timeout(
            _push_then_insert_child,
            (server, sn, tid, n_bulk),
            seconds=60,
            label="bulk push + INSERT loop with view",
        )

        rows = sorted(r.pk for r in client.scan(tid) if r.weight > 0)
        assert len(rows) == n_bulk + 50 * 100
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


def _range_pks(client, sn, sql):
    """Sorted PKs from a direct-SELECT range query (positive weight only)."""
    result = client.execute_sql(sql, schema_name=sn)
    assert result[0]["type"] == "Rows"
    return sorted(row.pk for row in result[0]["rows"] if row.weight > 0)


@_NEEDS_MULTI
def test_range_scan_broadcast_merge(client):
    """A range SELECT over a secondary index must BROADCAST to every worker and
    MERGE the per-worker partials: the index is partitioned by source PK, so a
    range's matches scatter across workers. The merged result must equal a
    full-scan-and-filter reference over the same data."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
            schema_name=sn)
        n = 200
        # pk == x, so PKs (hence index entries) scatter across all partitions and
        # a range on x matches rows owned by many different workers.
        vals = ",".join(f"({i}, {i})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        ref = {row.pk: row.x for row in client.scan(tid) if row.weight > 0}

        def reference(lo, hi, lo_incl, hi_incl):
            return sorted(pk for pk, x in ref.items()
                          if (x >= lo if lo_incl else x > lo)
                          and (x <= hi if hi_incl else x < hi))

        assert _range_pks(client, sn, "SELECT * FROM t WHERE x BETWEEN 40 AND 160") \
            == reference(40, 160, True, True)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x > 40 AND x < 160") \
            == reference(40, 160, False, False)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x >= 150") \
            == reference(150, n, True, True)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x <= 50") \
            == reference(1, 50, True, True)
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])


@_NEEDS_MULTI
def test_range_scan_signed_and_inclusivity_over_wire(client):
    """Boundary inclusivity and a signed range survive the wire round-trip
    (descriptor encode/decode, master verbatim forward, worker OPK encode) across
    workers."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x INT NOT NULL)",
            schema_name=sn)
        # Signed values scattered across PKs/partitions.
        rows = [(i, i - 100) for i in range(1, 201)]   # x from -99 .. 100
        vals = ",".join(f"({pk}, {x})" for pk, x in rows)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

        ref = {pk: x for pk, x in rows}

        def reference(lo, hi, lo_incl, hi_incl):
            return sorted(pk for pk, x in ref.items()
                          if (x >= lo if lo_incl else x > lo)
                          and (x <= hi if hi_incl else x < hi))

        # Signed contiguous interval (the cff7c58 payoff over the wire).
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x BETWEEN -10 AND 10") \
            == reference(-10, 10, True, True)
        # Inclusive vs exclusive boundaries land on exactly the right rows.
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x > -10 AND x < 10") \
            == reference(-10, 10, False, False)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x >= -50 AND x <= -50") \
            == reference(-50, -50, True, True)   # single-value inclusive band
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])


@_NEEDS_MULTI
def test_range_scan_max_arity_descriptor(client):
    """A composite index at PK_LIST_MAX_COLS = 4 with a two-sided range on the
    last column (`a = .. AND b = .. AND c = .. AND d > .. AND d < ..`, n_eq = 3)
    produces an 82-byte descriptor — over the 64-byte seek_pk_extra / 80-byte
    PkTuple cap — exercising the explicit-blob send path end-to-end across
    workers."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL)",
            schema_name=sn)
        # Rows sharing (a,b,c)=(1,2,3) with varying d, plus decoys on (a,b,c).
        rows = []
        pk = 1
        for d in range(0, 60):
            rows.append((pk, 1, 2, 3, d)); pk += 1
        rows.append((pk, 1, 2, 4, 30)); pk += 1   # c differs → excluded
        rows.append((pk, 2, 2, 3, 30)); pk += 1   # a differs → excluded
        vals = ",".join(f"({p},{a},{b},{c},{d})" for p, a, b, c, d in rows)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t(a, b, c, d)", schema_name=sn)

        # a=1 AND b=2 AND c=3 AND 10 < d < 50  → the (1,2,3,d) rows with d in (10,50).
        got = _range_pks(client, sn,
            "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d > 10 AND d < 50")
        expect = sorted(p for p, a, b, c, d in rows
                        if a == 1 and b == 2 and c == 3 and 10 < d < 50)
        assert got == expect, f"got {got}, expect {expect}"
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b_c_d"], tables=["t"])


# ---------------------------------------------------------------------------
# Non-equi (range / band) join — SQL E2E
#
# Both sides reindex onto [eq keys…, range key]. A band join (n_eq ≥ 1) scatters
# its delta by the eq PREFIX, so equal eq-values co-partition both sides and the
# range walk runs partition-local (no PartitionFilter). A pure range join
# (n_eq == 0) has no eq prefix: its delta is broadcast to every worker and probed
# against the other side's PartitionFilter-owned trace by an ordered range walk.
# Either way the output is re-keyed onto the source-PK pair (a.pk, b.pk) and
# exchanged by it, so the view is PK-partitioned like every other view. These run
# at GNITZ_WORKERS=4 under `make e2e`, exercising the eq-prefix scatter (band) and
# broadcast (pure range) input relays, the double exchange, and cross-worker
# retraction cancellation.
# ---------------------------------------------------------------------------

def _range_ref(a_rows, b_rows, pred):
    """Reference: the set of (a_id, b_id) pairs satisfying pred(a_row, b_row).
    Rows are (id, *cols) tuples; the view's pair-PK is exactly (a_id, b_id)."""
    return {(a[0], b[0]) for a in a_rows for b in b_rows if pred(a, b)}


def _view_pairs(client, vid):
    """The live (pair-PK) set of a range-join view: r[0] = a.pk, r[1] = b.pk."""
    return {(r[0], r[1]) for r in client.scan(vid) if r.weight > 0}


def _band_left_ref(a_rows, b_rows, pred):
    """Reference LEFT band-join result as a set of (a_id, b_id_or_None): every left
    row with ≥1 match contributes one (a_id, b_id) per match; every left row with NO
    match contributes its null-fill (a_id, None). Rows are (id, *cols) tuples."""
    out = set()
    for a in a_rows:
        matches = [b for b in b_rows if pred(a, b)]
        if matches:
            out |= {(a[0], b[0]) for b in matches}
        else:
            out.add((a[0], None))
    return out


def _band_left_rows(client, vid):
    """Live LEFT band-join view as {(aid, bid)}, bid None for a null-fill row. The
    view must project `a.id AS aid, b.id AS bid` (b.id is NULL on a null-fill)."""
    return {(r["aid"], r["bid"]) for r in client.scan(vid) if r.weight > 0}


_RANGE_OPS = [
    ("<",  lambda a, b: a < b),
    ("<=", lambda a, b: a <= b),
    (">",  lambda a, b: a > b),
    (">=", lambda a, b: a >= b),
]


class TestRangeJoin:
    """End-to-end coverage of the non-equi (range / band) join feature, driven
    entirely through SQL against the multi-worker cluster."""

    @pytest.mark.parametrize("op,cmp", _RANGE_OPS)
    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_pure_range_all_ops_both_orders(self, client, op, cmp, order):
        """Pure range join `ON a.x OP b.y`, values scattered across partitions,
        both insert orders — the view's pair set equals a python cross-filter."""
        sn = "rj" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                f"CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x {op} b.y",
                schema_name=sn)
            a_rows = [(i, (i * 7) % 23) for i in range(1, 21)]
            b_rows = [(i, (i * 5) % 23) for i in range(1, 21)]
            a_sql = ",".join(f"({i},{x})" for i, x in a_rows)
            b_sql = ",".join(f"({i},{y})" for i, y in b_rows)
            if order == "a_then_b":
                client.execute_sql(f"INSERT INTO a VALUES {a_sql}", schema_name=sn)
                client.execute_sql(f"INSERT INTO b VALUES {b_sql}", schema_name=sn)
            else:
                client.execute_sql(f"INSERT INTO b VALUES {b_sql}", schema_name=sn)
                client.execute_sql(f"INSERT INTO a VALUES {a_sql}", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: cmp(a[1], b[1]))
            assert got == want, f"op {op} order {order}: got {got ^ want} differ"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_join_eq_prefix_plus_range(self, client):
        """Band join `ON a.k = b.k AND a.lo <= b.t` — composite trace key; a
        same-range row in a different equality group must NOT match (group edge).

        The eq key spans dozens of distinct values, so its groups demonstrably
        spread across all four workers, and each group's rows sit on DIFFERENT
        base-table PK partitions (their ids differ by the group count) before the
        eq-prefix scatter re-homes them onto hash(k). A mis-routed scatter (lost
        matches) or a wrongly-retained PartitionFilter (which hashes the full
        [k, lo] key and drops rows whose full-key partition ≠ their [k] partition)
        fails the cross-filter reference.

        Sizing straddles the join's size-adaptive selector: the small `b` side is
        seeded FIRST (one row per group → a thin per-worker trace_b), then the
        larger `a` side is inserted in ONE epoch (three rows per group). On each
        worker the eq-prefix-scattered |ΔA| outnumbers its integrated trace_b, so
        the `join_ab` term takes the trace-driven `range_merge_walk` rather than
        the per-row probe — exercising Strategy 2 end to end through the scatter,
        output exchange, and consolidate pipeline."""
        sn = "bj" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.lo AS alo, b.t AS bt "
                "FROM a JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
            ng = 40                                                       # 40 distinct eq groups
            # Small trace side first (1 row/group), then the wide delta side in one
            # epoch (3 rows/group) so |ΔA_worker| > |trace_b_worker| → merge walk.
            b_rows = [(i, i % ng, (i * 7) % 20) for i in range(1, ng + 1)]       # (id, k, t)
            a_rows = [(i, i % ng, (i * 3) % 20) for i in range(1, 3 * ng + 1)]   # (id, k, lo)
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{k},{t})" for i, k, t in b_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{k},{lo})" for i, k, lo in a_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] <= b[2])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_oversized_delta_merge_walk(self, client):
        """Pure range join (`n_eq = 0`, one whole-trace eq group) where a single
        epoch's broadcast delta outnumbers the per-worker trace, forcing the
        degenerate single-group `range_merge_walk`. The small `b` side is seeded
        first; the larger `a` side is then inserted in one epoch. Because pure
        range broadcasts the delta in full but PartitionFilters the trace to ~1/W
        per worker, the `join_ab` term's |ΔA| (full) exceeds its trace_b slice and
        takes Strategy 2. The merge emits the same pairs as the per-row probe
        (trace-major, re-ordered downstream); the result equals the cross-filter
        reference."""
        sn = "rjm" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            # Thin trace (b) first, then a wide single-epoch delta (a).
            b_rows = [(i, (i * 5) % 23) for i in range(1, 9)]        # 8 trace rows
            a_rows = [(i, (i * 7) % 23) for i in range(1, 49)]       # 48 delta rows ≫ 8
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: a[1] < b[1])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_cross_worker_retraction(self, client):
        """The load-bearing test: a pair's `+1` and later `-1` are emitted by
        OPPOSITE terms on DIFFERENT workers and must cancel through the output
        exchange. Delete each side in turn; UPDATE the range column in/out."""
        sn = "rr" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")

            # One matching pair: a(1, x=10) < b(1, y=20).
            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20)", schema_name=sn)
            assert _view_pairs(client, vid) == {(1, 1)}

            # Delete b → the term-BA `-1` must cancel the term-AB `+1`.
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _view_pairs(client, vid) == set()
            # Re-insert b → the pair returns.
            client.execute_sql("INSERT INTO b VALUES (1, 20)", schema_name=sn)
            assert _view_pairs(client, vid) == {(1, 1)}
            # Delete a → gone again (cancellation from the other side).
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            assert _view_pairs(client, vid) == set()

            # UPDATE moving a pair in/out of range (retract + insert on a.x).
            client.execute_sql("INSERT INTO a VALUES (2, 5)", schema_name=sn)   # 5 < 20 → match
            client.execute_sql("INSERT INTO b VALUES (2, 7)", schema_name=sn)   # a2.x=5 < b2.y=7 → match too
            assert _view_pairs(client, vid) == {(2, 1), (2, 2)}
            # Move a2.x above both b.y values → no matches for a2.
            client.execute_sql("UPDATE a SET x = 99 WHERE id = 2", schema_name=sn)
            assert _view_pairs(client, vid) == set()
            # Move it back below b1.y=20 (but above b2.y=7) → only (2,1).
            client.execute_sql("UPDATE a SET x = 10 WHERE id = 2", schema_name=sn)
            assert _view_pairs(client, vid) == {(2, 1)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_backfill_over_populated_tables(self, client):
        """CREATE VIEW over already-populated tables yields the full match set
        (the backfill replays both sources through the broadcast input relay)."""
        sn = "rb" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            a_rows = [(i, (i * 11) % 17) for i in range(1, 16)]
            b_rows = [(i, (i * 13) % 17) for i in range(1, 16)]
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            # View created AFTER the data is loaded — pure backfill path.
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x >= b.y", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: a[1] >= b[1])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_view_scan_and_pk_seek(self, client):
        """The view is PK-partitioned by the (a.pk, b.pk) pair: a point PK-seek
        returns exactly the matching row (pair-PK routing == GroupKey scatter
        routing). Uses U64 PKs so the OPK pair-key is plain big-endian."""
        sn = "rs" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            a_rows = [(i, i) for i in range(1, 13)]
            b_rows = [(i, i + 5) for i in range(1, 13)]
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            pairs = _view_pairs(client, vid)
            assert pairs == _range_ref(a_rows, b_rows, lambda a, b: a[1] < b[1])
            # Point-seek every live pair by its compound PK. The seek key is the
            # native packed pair (col0 in the low 64 bits, col1 in the high 64);
            # the server OPK-encodes it per the view schema and unicasts to
            # partition_for_pk_bytes(OPK)'s owner. A hit ⟺ the output exchange
            # routed the pair to that same worker — the compound-key alignment
            # invariant. (U64 PK columns: native packing is exact.)
            for (aid, bid) in pairs:
                res = client.seek(vid, int(aid) | (int(bid) << 64))
                assert res.batch is not None and len(res.batch.pks) == 1, \
                    f"PK-seek ({aid},{bid}) should return exactly 1 row"
            # A guaranteed non-pair (a12.x=12 < b1.y=6 is false) seeks to nothing.
            res = client.seek(vid, 12 | (1 << 64))
            assert res.batch is None or len(res.batch.pks) == 0
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_null_key_either_side_matches_nothing(self, client):
        """A NULL in any ON-clause column (eq or range), on EITHER side, matches
        nothing (3VL). Both sides nullable, so this exercises the left (input_a)
        AND the right (input_b) NULL filters."""
        sn = "rn" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NULL, x BIGINT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NULL, y BIGINT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by "
                "FROM a JOIN b ON a.k = b.k AND a.x < b.y", schema_name=sn)
            # b1 fully defined; b2 has NULL eq key; b3 has NULL range key.
            client.execute_sql(
                "INSERT INTO b VALUES (1, 7, 100), (2, NULL, 100), (3, 7, NULL)", schema_name=sn)
            # a1: k NULL; a2: x NULL → no match. a3 fully defined → matches only b1
            # (a3.k=7=b1.k, a3.x=5 < b1.y=100); b2/b3 excluded by their own NULLs.
            client.execute_sql(
                "INSERT INTO a VALUES (1, NULL, 5), (2, 7, NULL), (3, 7, 5)", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            assert _view_pairs(client, vid) == {(3, 1)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_signed_band_over_the_wire(self, client):
        """A signed range pair via cross-sign promotion (U32 vs I64 at T=I64),
        negatives included: the contiguous typed interval survives promotion and
        broadcast with no order inversion."""
        sn = "rsg" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x INT UNSIGNED NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x > b.y", schema_name=sn)
            a_rows = [(i, i) for i in range(0, 10)]            # x: 0..9 (unsigned)
            b_rows = [(i, i - 5) for i in range(1, 11)]        # y: -4..5 (signed, negatives)
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: a[1] > b[1])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_empty_delta_epochs_no_deadlock(self, client):
        """Pushing rows that match nothing still drives both exchanges every
        epoch (empty batches included) — no barrier deadlock with the double
        exchange."""
        sn = "re" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            # All a.x huge, all b.y small → never any match across several epochs.
            for i in range(1, 6):
                client.execute_sql(f"INSERT INTO a VALUES ({i}, 1000000)", schema_name=sn)
            for i in range(1, 6):
                client.execute_sql(f"INSERT INTO b VALUES ({i}, {i})", schema_name=sn)
            assert _view_pairs(client, vid) == set()
            # A subsequent real match still flows (the circuit is alive).
            client.execute_sql("INSERT INTO a VALUES (99, 0)", schema_name=sn)
            assert _view_pairs(client, vid) == {(99, j) for j in range(1, 6)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_view_over_range_join_view(self, client):
        """A GROUP BY view stacked on a range-join view stays consistent under
        retraction — the range-join output deltas are well-formed for downstream
        circuits."""
        sn = "rv" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            # GROUP BY over the range-join view: count matches per a.id.
            client.execute_sql(
                "CREATE VIEW g AS SELECT aid, COUNT(*) AS n FROM v GROUP BY aid", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 5), (2, 50)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (10, 10), (11, 20), (12, 100)", schema_name=sn)
            gid, _ = client.resolve_table(sn, "g")

            # GROUP BY output layout is [_group_pk, group_col, agg]; read the
            # group key and count by name, not positionally. A group whose COUNT
            # falls to 0 (all its range-join rows retracted) is treated as absent
            # — the reduce leaves a count-0 row, which SQL semantics drop.
            def counts():
                return {r["aid"]: r["n"] for r in client.scan(gid)
                        if r.weight > 0 and r["n"] > 0}
            # a1.x=5 < {10,20,100} → 3 matches; a2.x=50 < {100} → 1 match.
            assert counts() == {1: 3, 2: 1}
            # Delete b(12, y=100): a1 loses one (→2), a2 loses its only (→ gone).
            client.execute_sql("DELETE FROM b WHERE id = 12", schema_name=sn)
            assert counts() == {1: 2}
        finally:
            _drop_all(client, sn, views=["g", "v"], tables=["a", "b"])

    def test_branch_routing_regression(self, client):
        """In the same cluster as a range-join view, a plain GROUP BY view and a
        single-sided set-op (DISTINCT) view — both have has_join_shard AND
        has_exchange — must still produce correct results, i.e. they take the
        has_exchange arm, not the range-join broadcast branch (the §7
        discriminator must be exact)."""
        sn = "rbr" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn)
            # Range join (broadcast branch).
            client.execute_sql(
                "CREATE VIEW rj AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            # GROUP BY (has_join_shard via group reindex + has_exchange) — must NOT
            # be diverted into the range-join branch.
            client.execute_sql(
                "CREATE VIEW gb AS SELECT g, COUNT(*) AS n FROM c GROUP BY g", schema_name=sn)
            # Single-sided set-op: SELECT DISTINCT (HashRow → exchange → distinct).
            client.execute_sql(
                "CREATE VIEW dv AS SELECT DISTINCT g FROM c", schema_name=sn)

            client.execute_sql("INSERT INTO a VALUES (1, 1), (2, 2)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 5)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO c VALUES (1, 100), (2, 100), (3, 200), (4, 200), (5, 200)", schema_name=sn)

            rj_id, _ = client.resolve_table(sn, "rj")
            gb_id, _ = client.resolve_table(sn, "gb")
            dv_id, _ = client.resolve_table(sn, "dv")

            assert _view_pairs(client, rj_id) == {(1, 1), (2, 1)}
            # GROUP BY output is [_group_pk, g, n] — read by name, not r[0].
            gb = {r["g"]: r["n"] for r in client.scan(gb_id) if r.weight > 0}
            assert gb == {100: 2, 200: 3}, f"GROUP BY corrupted by branch routing: {gb}"
            dv = sorted(r["g"] for r in client.scan(dv_id) if r.weight > 0)
            assert dv == [100, 200], f"DISTINCT corrupted by branch routing: {dv}"
        finally:
            _drop_all(client, sn, views=["rj", "gb", "dv"], tables=["a", "b", "c"])

    def test_compound_source_pks_wide_pair_pk(self, client):
        """Compound source PKs make a 4-column pair-PK (4×8 = 32 bytes > 16 → the
        wide `partition_for_pk_bytes` xxh routing path on the output exchange).
        The full pair identity is (a.k1, a.k2, b.k1, b.k2)."""
        sn = "rc" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, "
                "PRIMARY KEY (k1, k2))", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, y BIGINT NOT NULL, "
                "PRIMARY KEY (k1, k2))", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            a_rows = [((i, i * 2), (i * 7) % 19) for i in range(1, 13)]   # ((k1,k2), x)
            b_rows = [((i, i * 3), (i * 5) % 19) for i in range(1, 13)]   # ((k1,k2), y)
            client.execute_sql("INSERT INTO a VALUES " +
                ",".join(f"({k1},{k2},{x})" for (k1, k2), x in a_rows), schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES " +
                ",".join(f"({k1},{k2},{y})" for (k1, k2), y in b_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            # pair-PK = (a.k1, a.k2, b.k1, b.k2) at r[0..4]; payload ax=r[4], by=r[5].
            got = {(r[0], r[1], r[2], r[3]) for r in client.scan(vid) if r.weight > 0}
            want = {(ak1, ak2, bk1, bk2)
                    for ((ak1, ak2), ax) in a_rows for ((bk1, bk2), by) in b_rows if ax < by}
            assert got == want, "wide pair-PK routing dropped or duplicated matches"
            # The projected payload values are correct on every emitted row.
            for r in client.scan(vid):
                if r.weight > 0:
                    assert r[4] < r[5], f"projected ax={r[4]} must be < by={r[5]}"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_join_compound_eq_prefix(self, client):
        """Band join with a TWO-column equality prefix (n_eq=2): the trace key is
        [k1, k2, range]; a match needs both equality columns AND the range."""
        sn = "rb2" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k1 BIGINT NOT NULL, "
                "k2 BIGINT NOT NULL, lo BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k1 BIGINT NOT NULL, "
                "k2 BIGINT NOT NULL, t BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.lo AS alo, b.t AS bt "
                "FROM a JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2 AND a.lo <= b.t", schema_name=sn)
            a_rows = [(i, i % 3, i % 2, (i * 3) % 15) for i in range(1, 25)]   # (id,k1,k2,lo)
            b_rows = [(i, i % 3, i % 2, (i * 7) % 15) for i in range(1, 25)]   # (id,k1,k2,t)
            client.execute_sql("INSERT INTO a VALUES " +
                ",".join(f"({i},{k1},{k2},{lo})" for i, k1, k2, lo in a_rows), schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES " +
                ",".join(f"({i},{k1},{k2},{t})" for i, k1, k2, t in b_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows,
                              lambda a, b: a[1] == b[1] and a[2] == b[2] and a[3] <= b[3])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_select_star_projection(self, client):
        """`SELECT *` over a range join: the wildcard projection branch keeps all
        source columns (duplicate names allowed for `*`), and every projected
        value — including the pair-PK twins carried in the payload — is correct."""
        sn = "rw" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.x < b.y", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 30)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (5, 20), (6, 40)", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            # Output layout: [_pair_pk_0=a.id, _pair_pk_1=b.id, a.id, a.x, b.id, b.y].
            got = {(r[0], r[1]): (r[2], r[3], r[4], r[5])
                   for r in client.scan(vid) if r.weight > 0}
            assert set(got.keys()) == {(1, 5), (1, 6), (2, 6)}
            # pair-PK twins (r[2]=a.id, r[4]=b.id) match the PK region; range cols hold.
            assert got[(1, 5)] == (1, 10, 5, 20)
            assert got[(1, 6)] == (1, 10, 6, 40)
            assert got[(2, 6)] == (2, 30, 6, 40)
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_range_column_is_source_pk(self, client):
        """The range column IS the source PK on both sides: the broadcast + probe
        must still run (no co-partition shortcut applies to a range join, even
        when the join key equals the source PK). pair-PK = (a.x, b.y)."""
        sn = "rk" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE a (x BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
            client.execute_sql("CREATE TABLE b (y BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x < b.y", schema_name=sn)
            a_vals = list(range(1, 16))
            b_vals = list(range(5, 20))
            client.execute_sql("INSERT INTO a VALUES " + ",".join(f"({x})" for x in a_vals), schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES " + ",".join(f"({y})" for y in b_vals), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)   # pair-PK = (a.x, b.y)
            want = {(x, y) for x in a_vals for y in b_vals if x < y}
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_join_retraction(self, client):
        """Cross-worker retraction in a BAND join (eq prefix): deleting either
        side cancels the pair; an UPDATE moving the equality key to a different
        group drops the match."""
        sn = "rbr2" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.lo AS alo, b.t AS bt "
                "FROM a JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            client.execute_sql("INSERT INTO a VALUES (1, 100, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 100, 9)", schema_name=sn)  # k=100, 5<=9 → match
            assert _view_pairs(client, vid) == {(1, 1)}
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _view_pairs(client, vid) == set()
            client.execute_sql("INSERT INTO b VALUES (1, 100, 9)", schema_name=sn)
            assert _view_pairs(client, vid) == {(1, 1)}
            # Move a's equality key out of the group → no longer matches.
            client.execute_sql("UPDATE a SET k = 200 WHERE id = 1", schema_name=sn)
            assert _view_pairs(client, vid) == set()
            # Move it back.
            client.execute_sql("UPDATE a SET k = 100 WHERE id = 1", schema_name=sn)
            assert _view_pairs(client, vid) == {(1, 1)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_join_cross_width_eq_key(self, client):
        """Band join whose EQUALITY key is cross-width promoted: `a.k` is INT
        UNSIGNED (U32), `b.k` is BIGINT (I64) → common type T=I64. The U32 side
        routes the scatter through the ReindexPacker (packs at T), while the
        already-I64 side routes through the single-column route_key (OPK-encoded at
        its own width, which IS T). Equal k values must hash to the same partition
        on BOTH legs or the eq-prefix scatter strands matches. No other band test
        promotes the eq key — test_signed_band_over_the_wire promotes the RANGE
        column, not the equality prefix."""
        sn = "bjx" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k INT UNSIGNED NOT NULL, lo BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.lo AS alo, b.t AS bt "
                "FROM a JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
            ng = 16                                                      # k ∈ 0..15 (non-negative: U32 side)
            a_rows = [(i, i % ng, (i * 3) % 20) for i in range(1, 2 * ng + 1)]   # (id, k:U32, lo)
            b_rows = [(i, i % ng, (i * 7) % 20) for i in range(1, 2 * ng + 1)]   # (id, k:I64, t)
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{k},{lo})" for i, k, lo in a_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{k},{t})" for i, k, t in b_rows), schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] <= b[2])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_join_backfill_over_populated_tables(self, client):
        """CREATE VIEW with an eq prefix over ALREADY-POPULATED tables yields the
        full match set via the eq-prefix scatter — confirming the backfill replay
        rides the SAME relay, so each source is PARTITIONED, not replicated. Unlike
        test_backfill_over_populated_tables (a pure `>=` range join that still
        broadcasts), the band case no longer contributes the O(num_workers × N)
        backfill blow-up."""
        sn = "bjb" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn)
            ng = 24
            a_rows = [(i, i % ng, (i * 11) % 17) for i in range(1, 2 * ng + 1)]   # (id, k, lo)
            b_rows = [(i, i % ng, (i * 13) % 17) for i in range(1, 2 * ng + 1)]   # (id, k, t)
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{k},{lo})" for i, k, lo in a_rows), schema_name=sn)
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{k},{t})" for i, k, t in b_rows), schema_name=sn)
            # View created AFTER the data is loaded — pure backfill path.
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.lo AS alo, b.t AS bt "
                "FROM a JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            got = _view_pairs(client, vid)
            want = _range_ref(a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] <= b[2])
            assert got == want
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    @pytest.mark.parametrize("op,want_match", [
        ("<", False), ("<=", True), (">", False), (">=", True)])
    def test_boundary_equal_values(self, client, op, want_match):
        """At x == y, only the inclusive operators (<=, >=) match."""
        sn = "rbd" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                f"CREATE VIEW v AS SELECT a.x AS ax, b.y AS by FROM a JOIN b ON a.x {op} b.y", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 42)", schema_name=sn)   # x == y
            vid, _ = client.resolve_table(sn, "v")
            assert _view_pairs(client, vid) == ({(1, 1)} if want_match else set())
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_sql_rejections(self, client):
        """The unsupported range-join SQL surfaces are rejected at CREATE VIEW
        (also covered by the Rust integration tests; pinned here end-to-end)."""
        sn = "rrej" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                "w BIGINT NOT NULL, s VARCHAR(20) NOT NULL, fx DOUBLE NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL, "
                "z BIGINT NOT NULL, s VARCHAR(20) NOT NULL, fy DOUBLE NOT NULL)", schema_name=sn)
            rejected = {
                # Band LEFT (with an eq conjunct) and PURE-range LEFT (no eq conjunct,
                # via the MAX/MIN threshold null-fill) are BOTH supported now; only
                # RIGHT/FULL outer and the genuinely-unsupported shapes are rejected.
                "two_range":   "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.x < b.y AND a.w > b.z",
                "string_pair": "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.s < b.s",
                "float_pair":  "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.fx < b.fy",
                "self_join":   "CREATE VIEW v AS SELECT * FROM a a1 JOIN a a2 ON a1.x < a2.x",
            }
            for label, sql in rejected.items():
                with pytest.raises(Exception):
                    client.execute_sql(sql, schema_name=sn)
                # No view should have been registered by a rejected CREATE.
                with pytest.raises(Exception):
                    client.resolve_table(sn, "v")
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    # ── Band LEFT OUTER (eq prefix + range): the null-fill set-difference ──────
    #
    # View projects `a.id AS aid, b.id AS bid`; a null-fill row is (aid, None)
    # because its b.id is NULL. The reference is `_band_left_ref`.

    def _mk_band_left(self, client, sn):
        """CREATE a/b/v for the standard band LEFT view and return (vid, pred)."""
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
            "FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
        vid, _ = client.resolve_table(sn, "v")
        return vid, (lambda a, b: a[1] == b[1] and a[2] <= b[2])

    def test_band_left_delta_a_then_b(self, client):
        """ΔA then ΔB, plus retraction and match-multiplicity. An a with no b in its
        k-group satisfying lo<=t null-fills; a ΔB that gives it a first match retracts
        the null-fill and emits the pair; deleting that b restores the null-fill. An a
        with two matches stays matched until the LAST retracts (distinct over summed
        a.pk weights — multiplicity, not a 0-boundary crossing, until the end)."""
        sn = "blab" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_band_left(client, sn)

            # Seed b thin: one row in group k=1 only.
            b_rows = [(1, 1, 50)]
            client.execute_sql("INSERT INTO b VALUES (1, 1, 50)", schema_name=sn)

            # a1 (k=1, lo=10) matches b1; a2 (k=2) and a3 (k=3, lo=999) have no b in
            # their group satisfying the band → null-fills.
            a_rows = [(1, 1, 10), (2, 2, 10), (3, 3, 999)]
            client.execute_sql("INSERT INTO a VALUES (1,1,10), (2,2,10), (3,3,999)", schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            assert _band_left_rows(client, vid) == {(1, 1), (2, None), (3, None)}

            # ΔB: b2 in group k=2 (t=20 ≥ a2.lo=10) → a2 gains its first match.
            b_rows.append((2, 2, 20))
            client.execute_sql("INSERT INTO b VALUES (2, 2, 20)", schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            assert _band_left_rows(client, vid) == {(1, 1), (2, 2), (3, None)}

            # Delete b2 → a2's last (only) match gone → (a2, NULL) returns.
            b_rows = [r for r in b_rows if r[0] != 2]
            client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, 1), (2, None), (3, None)}

            # Multiplicity: add b3 (k=1, t=30) so a1 matches BOTH b1 and b3.
            b_rows.append((3, 1, 30))
            client.execute_sql("INSERT INTO b VALUES (3, 1, 30)", schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert rows == _band_left_ref(a_rows, b_rows, pred)
            assert (1, None) not in rows                      # matched twice — no null-fill
            # Drop one of a1's two matches → still matched.
            b_rows = [r for r in b_rows if r[0] != 1]
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert (1, None) not in rows
            assert rows == {(1, 3), (2, None), (3, None)}
            # Drop a1's last match → (a1, NULL) finally appears.
            b_rows = [r for r in b_rows if r[0] != 3]
            client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, None), (2, None), (3, None)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_left_same_epoch_match(self, client):
        """Same-epoch insert-and-match: b's match already sits in the trace, then a is
        inserted; its match is decided in the SAME epoch as its insertion. The result
        is exactly one (a, b) and NO net (a, NULL) — distinct sees ΔD=+a in this epoch
        (the inner join's AB term) and cancels ΔA=+a. This is the case a key-only
        anti-join cannot get right for a range predicate. A non-matching a inserted in
        the same epoch still null-fills, proving the epoch handles a mixed batch."""
        sn = "blse" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_band_left(client, sn)
            # b's matches live in the trace before a arrives.
            b_rows = [(1, 1, 50)]
            client.execute_sql("INSERT INTO b VALUES (1, 1, 50)", schema_name=sn)
            # One epoch: a1 matches the existing b1; a2 (k=7) has no b.
            a_rows = [(1, 1, 10), (2, 7, 10)]
            client.execute_sql("INSERT INTO a VALUES (1,1,10), (2,7,10)", schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert rows == _band_left_ref(a_rows, b_rows, pred)
            assert rows == {(1, 1), (2, None)}
            assert (1, None) not in rows                      # no same-epoch null-fill for a1
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_left_delete_matched_row(self, client):
        """Delete a matched left row: from steady state (a matched, no null-fill),
        deleting a retracts (a, b) and emits NO (a, NULL) — ΔA=−a and ΔD=−a cancel
        (distinct 1→0 in the same epoch). A broken cancellation would surface a
        spurious (a, NULL) tombstone in the integrated view."""
        sn = "bldm" + _uid()
        client.create_schema(sn)
        try:
            vid, _pred = self._mk_band_left(client, sn)
            client.execute_sql("INSERT INTO b VALUES (1, 1, 50)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 1, 10)", schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert rows == {(1, 1)}
            assert (1, None) not in rows
            # Delete the matched a → fully gone, no null-fill emitted.
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            assert _band_left_rows(client, vid) == set()
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_left_cross_worker_eq_groups(self, client):
        """Many distinct eq-key groups spread across all four workers, some matched
        and some not. The partition-local distinct must be globally correct: never a
        spurious null-fill for a matched a, never a dropped null-fill for an unmatched
        a whose group lives on another worker. b is seeded only in EVEN groups; a is
        inserted in EVERY group, so odd-group a's null-fill and even-group a's match."""
        sn = "blcw" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_band_left(client, sn)
            ng = 40                                            # groups fan out across W=4
            b_rows = [(g, g, 100) for g in range(2, ng + 1, 2)]   # even groups only
            a_rows = [(g, g, 10) for g in range(1, ng + 1)]       # all groups, lo=10 ≤ 100
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{k},{t})" for i, k, t in b_rows),
                schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{k},{lo})" for i, k, lo in a_rows),
                schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            # Sanity: even groups matched, odd groups null-filled.
            rows = _band_left_rows(client, vid)
            assert (2, 2) in rows and (1, None) in rows and (3, None) in rows
            assert (1, 1) not in rows
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_left_null_join_key(self, client):
        """A left row with a NULL eq or NULL range column can never satisfy the
        predicate (SQL 3VL): it is filtered out of the inner match (never in D) yet is
        still a left row (in A, sourced from the UNFILTERED input) → null-filled
        exactly once. No bypass branch exists; the A − D difference subsumes it."""
        sn = "blnk" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, lo BIGINT)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            client.execute_sql("INSERT INTO b VALUES (1, 1, 100)", schema_name=sn)
            # a1: NULL eq key. a2: NULL range key. a3: fully defined, matches b1.
            client.execute_sql(
                "INSERT INTO a VALUES (1, NULL, 10), (2, 1, NULL), (3, 1, 10)", schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert rows == {(1, None), (2, None), (3, 1)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_band_left_payload_fidelity_wide_pk(self, client):
        """Byte-exact (A, D) cancellation under a STRING column, a nullable column
        carrying NULL, and a 2-column (wide) source PK. A matched left row's +1 (from
        A) and −1 (from distinct(π_A(inner))) must be byte-identical so they cancel —
        a stride/encoding mismatch would leave a spurious (a, NULL). Phase 1 inserts
        only matched a's and asserts ZERO null-fills with exact payload; phase 2 adds
        unmatched a's and asserts their null-fills carry the exact A payload."""
        sn = "blpf" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id1 BIGINT NOT NULL, id2 BIGINT NOT NULL, k BIGINT NOT NULL, "
                "lo BIGINT NOT NULL, s VARCHAR(16) NOT NULL, note BIGINT, PRIMARY KEY (id1, id2))",
                schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id1 AS aid1, a.id2 AS aid2, a.s AS asv, "
                "a.note AS anote, b.id AS bid "
                "FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")

            def rowset():
                return {(r["aid1"], r["aid2"], r["asv"], r["anote"], r["bid"])
                        for r in client.scan(vid) if r.weight > 0}

            client.execute_sql("INSERT INTO b VALUES (1, 1, 100), (2, 2, 100)", schema_name=sn)
            # Phase 1 — every a matches (string + nullable note in payload).
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 1, 10, 'alpha', 7), (1, 2, 2, 20, 'beta', NULL)",
                schema_name=sn)
            got = rowset()
            assert got == {(1, 1, "alpha", 7, 1), (1, 2, "beta", None, 2)}
            assert all(r[4] is not None for r in got), "matched rows must not null-fill (clean A − D)"

            # Phase 2 — add unmatched a's (group with no b / lo above every b.t).
            client.execute_sql(
                "INSERT INTO a VALUES (2, 1, 9, 10, 'gamma', 42), (2, 2, 1, 999, 'delta', NULL)",
                schema_name=sn)
            assert rowset() == {
                (1, 1, "alpha", 7, 1), (1, 2, "beta", None, 2),
                (2, 1, "gamma", 42, None), (2, 2, "delta", None, None),
            }
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    # ── Pure-range LEFT OUTER (no eq conjunct): the MAX/MIN threshold null-fill ──
    #
    # `∃b. a.x < b.y  ⟺  a.x < MAX(b.y)`, so the unmatched left rows are exactly
    # those on the null-fill side of a single scalar threshold `m` (MAX for `< <=`,
    # MIN for `> >=`). The null-fill is the subtraction `A − (A ⋈ {m})` against the
    # one-row `m`, computed by an INLINE shard-free reduce over the broadcast `b`
    # (no catalog helpers, no sentinel — an empty `m` simply null-fills all of `A`).
    # The view projects `a.id AS aid, b.id AS bid`; a null-fill row is (aid, None).
    # The reference reuses `_band_left_ref` with a pure-range predicate.

    def _mk_pure_range_left(self, client, sn, op="<",
                            a_x="BIGINT NOT NULL", b_y="BIGINT NOT NULL"):
        """CREATE a/b/v for the standard pure-range LEFT view; return (vid, pred).

        `a_x`/`b_y` are the range column types (default a non-null BIGINT pair) so a
        caller can vary nullability or signedness without re-inlining the DDL."""
        client.execute_sql(
            f"CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x {a_x})", schema_name=sn)
        client.execute_sql(
            f"CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y {b_y})", schema_name=sn)
        client.execute_sql(
            f"CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
            f"FROM a LEFT JOIN b ON a.x {op} b.y", schema_name=sn)
        vid, _ = client.resolve_table(sn, "v")
        cmp = dict(_RANGE_OPS)[op]
        return vid, (lambda a, b: cmp(a[1], b[1]))

    def test_pure_range_left_delta_a_then_b(self, client):
        """ΔA then ΔB, plus retraction across the threshold. An a with x >= MAX(b.y)
        null-fills; a ΔB that raises MAX past it retracts the null-fill and emits the
        pairs; deleting that b restores the null-fill. Matched a's never null-fill."""
        sn = "prab" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_pure_range_left(client, sn, "<")
            # b thin: MAX(b.y) = 50.
            b_rows = [(1, 50)]
            client.execute_sql("INSERT INTO b VALUES (1, 50)", schema_name=sn)
            # a1 (x=10) < 50 → matches b1. a2 (x=50) and a3 (x=999) are >= 50 → null-fill.
            a_rows = [(1, 10), (2, 50), (3, 999)]
            client.execute_sql("INSERT INTO a VALUES (1,10), (2,50), (3,999)", schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            assert _band_left_rows(client, vid) == {(1, 1), (2, None), (3, None)}

            # ΔB: b2 (y=100) raises MAX to 100. a2 (x=50<100) gains a match.
            b_rows.append((2, 100))
            client.execute_sql("INSERT INTO b VALUES (2, 100)", schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            assert _band_left_rows(client, vid) == {(1, 1), (1, 2), (2, 2), (3, None)}

            # Delete b2 → MAX back to 50 → a2 (x=50) flips back to null-fill.
            b_rows = [r for r in b_rows if r[0] != 2]
            client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, 1), (2, None), (3, None)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_same_epoch_match(self, client):
        """Same-epoch insert-and-match: b already covers a's match (m already past
        a.x) before a is inserted. Exactly one pair and NO net (a, NULL) — the
        matched a's passthrough `+a` and its `matched = A ⋈ {m}` term `−a` are
        byte-identical and cancel in-epoch, so no (a, NULL) ever surfaces. A
        non-matching a inserted in the same epoch still null-fills."""
        sn = "prse" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_pure_range_left(client, sn, "<")
            b_rows = [(1, 50)]
            client.execute_sql("INSERT INTO b VALUES (1, 50)", schema_name=sn)
            # One epoch: a1 (x=10 < 50) matches; a2 (x=80 >= 50) does not.
            a_rows = [(1, 10), (2, 80)]
            client.execute_sql("INSERT INTO a VALUES (1,10), (2,80)", schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert rows == _band_left_ref(a_rows, b_rows, pred)
            assert rows == {(1, 1), (2, None)}
            assert (1, None) not in rows                       # no same-epoch null-fill for a1
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_delete_matched_row(self, client):
        """Delete a matched left row: from steady state (a matched, no null-fill),
        deleting a retracts the pair and emits NO (a, NULL) tombstone."""
        sn = "prdm" + _uid()
        client.create_schema(sn)
        try:
            vid, _pred = self._mk_pure_range_left(client, sn, "<")
            client.execute_sql("INSERT INTO b VALUES (1, 50)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, 1)}
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            assert _band_left_rows(client, vid) == set()
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_m_flip_across_workers(self, client):
        """Existence depends ONLY on the extremum m = MAX(b.y). Values scatter
        across all four workers. Deleting the EXTREME b raises the null-fill
        threshold so every a between the old and new m flips to null-filled though
        it was never touched; deleting a NON-extreme b leaves m unchanged → no
        spurious null-fill (the proof existence depends only on the extremum)."""
        sn = "prmf" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_pure_range_left(client, sn, "<")
            b_rows = [(1, 20), (2, 40), (3, 60), (4, 80), (5, 100)]   # MAX = 100
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            a_rows = [(1, 10), (2, 50), (3, 90), (4, 100), (5, 150)]
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            # a4 (x=100) and a5 (x=150) are >= MAX(100) → null-fill.
            rows = _band_left_rows(client, vid)
            assert (4, None) in rows and (5, None) in rows
            assert (4, None) not in {(a, b) for (a, b) in rows if b is not None}

            # Delete the EXTREME b5 (y=100). New MAX = 80 → a3 (x=90 >= 80) flips to
            # null-fill though a3 was untouched.
            client.execute_sql("DELETE FROM b WHERE id = 5", schema_name=sn)
            b_rows = [r for r in b_rows if r[0] != 5]
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            assert (3, None) in _band_left_rows(client, vid)

            # Delete a NON-extreme b1 (y=20). MAX still 80 → NO null-fill flips.
            null_before = {a for (a, b) in _band_left_rows(client, vid) if b is None}
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            b_rows = [r for r in b_rows if r[0] != 1]
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            null_after = {a for (a, b) in _band_left_rows(client, vid) if b is None}
            assert null_before == null_after, "non-extreme b delete must not flip any null-fill"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_unmatched_once_under_broadcast(self, client):
        """A left row matching NO b on ANY worker null-fills EXACTLY ONCE — not W
        times — even though the pure-range relay broadcasts ΔA to every worker. The
        a.x owner produces the single null-fill; the pair-PK shard routes it once."""
        sn = "pru1" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_pure_range_left(client, sn, "<")
            b_rows = [(i, 10 * i) for i in range(1, 9)]            # MAX = 80
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            # Many a's spread across workers, all >= 80 → each null-fills once.
            a_rows = [(i, 80 + i) for i in range(1, 21)]
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred)
            # Raw weights: every null-fill is weight exactly 1 (no W× broadcast dup).
            for r in client.scan(vid):
                if r["bid"] is None:
                    assert r.weight == 1, f"null-fill for aid={r['aid']} has weight {r.weight} != 1"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_empty_b_all_nullfill(self, client):
        """Empty (and re-filled) right side. With NO b the threshold `m` is empty, so
        the subtraction `A − (A ⋈ {m})` is `A − ∅ = A` and EVERY left row null-fills
        — no sentinel needed. The compare type is I64 (a.x:BIGINT vs b.y:INT UNSIGNED
        promote to I64), so a NEGATIVE a.x below b.y's unsigned min orders correctly:
        on refill it matches (−5 < 100 in signed order, not as a huge unsigned). Re-
        inserting b retracts the now-matched a's null-fills."""
        sn = "preb" + _uid()
        client.create_schema(sn)
        try:
            vid, _ = self._mk_pure_range_left(client, sn, "<", b_y="INT UNSIGNED NOT NULL")
            # b EMPTY. a's include a negative x (below b.y's unsigned domain).
            a_rows = [(1, -5), (2, 0), (3, 1000)]
            client.execute_sql("INSERT INTO a VALUES (1,-5), (2,0), (3,1000)", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, None), (2, None), (3, None)}, \
                "empty b must null-fill every a, incl a.x below b.y's type min"

            # Re-fill b: MAX(b.y) = 100. a1(-5) and a2(0) now match; a3(1000) stays null.
            client.execute_sql("INSERT INTO b VALUES (1, 100)", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, 1), (2, 1), (3, None)}

            # Empty b again (delete the last b) → all null-fill returns.
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, None), (2, None), (3, None)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_all_null_range_b(self, client):
        """A b with rows but ALL range values NULL behaves identically to empty b
        (MIN/MAX skip NULL → no value → empty `m` → `A − ∅ = A`) → every a null-fills.
        Nulling the last live b.y re-null-fills the matched a's."""
        sn = "prnb" + _uid()
        client.create_schema(sn)
        try:
            vid, _ = self._mk_pure_range_left(client, sn, "<", b_y="BIGINT")
            client.execute_sql("INSERT INTO b VALUES (1, NULL), (2, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, None), (2, None)}, \
                "all-NULL-range b must null-fill every a (no MAX value)"
            # Add a b with a live value → a's below it match (MAX now defined).
            client.execute_sql("INSERT INTO b VALUES (3, 100)", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, 3), (2, 3)}
            # Delete it → back to all-NULL range → matched a's re-null-fill.
            client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, None), (2, None)}
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_null_a_x(self, client):
        """A left row with a NULL range key can never satisfy the predicate (3VL):
        it is filtered out of the inner match and the threshold compare, yet is
        still a left row → null-filled EXACTLY once (the dedicated nf_null branch
        routed by a.pk, not W times under broadcast)."""
        sn = "prna" + _uid()
        client.create_schema(sn)
        try:
            vid, _ = self._mk_pure_range_left(client, sn, "<", a_x="BIGINT")
            client.execute_sql("INSERT INTO b VALUES (1, 100)", schema_name=sn)
            # a1: NULL range key (null-fills). a2: x=5 < 100 (matches).
            client.execute_sql("INSERT INTO a VALUES (1, NULL), (2, 5)", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, None), (2, 1)}
            for r in client.scan(vid):
                if r["aid"] == 1:
                    assert r.weight == 1, f"NULL-a.x null-fill weight {r.weight} != 1 (W× dup?)"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    @pytest.mark.parametrize("op,cmp", _RANGE_OPS)
    def test_pure_range_left_all_ops_and_boundary(self, client, op, cmp):
        """All four ops with the correct MAX/MIN threshold and the EXACT boundary
        a.x == extremum: `<`/`>` exclude it (null-fill), `<=`/`>=` include it."""
        sn = "prop" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_pure_range_left(client, sn, op)
            b_rows = [(1, 30), (2, 50), (3, 70)]                 # MAX=70, MIN=30
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            # a's straddle both extrema, including exact-boundary rows (x==30, x==70).
            a_rows = [(1, 10), (2, 30), (3, 50), (4, 70), (5, 90)]
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            assert _band_left_rows(client, vid) == _band_left_ref(a_rows, b_rows, pred), \
                f"op {op}: pure-range LEFT differs from the cross-filter reference"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_u64_boundary(self, client):
        """Unsigned U64 range column with an extremum ABOVE I64::MAX. The MIN/MAX
        output is type-labelled I64 by the aggregate contract, but the threshold must
        be encoded in the promoted compare type (U64) — a wrong I64 sign-flip would
        order the high-bit-set extreme at the BOTTOM and flip every match. Pin it:
        small a.x matches a huge b.y; an a.x above the extreme null-fills."""
        sn = "pru64" + _uid()
        client.create_schema(sn)
        try:
            vid, _ = self._mk_pure_range_left(
                client, sn, "<", a_x="BIGINT UNSIGNED NOT NULL", b_y="BIGINT UNSIGNED NOT NULL")
            big = 18_000_000_000_000_000_000               # > I64::MAX (9.22e18), < U64::MAX
            client.execute_sql(f"INSERT INTO b VALUES (1, {big})", schema_name=sn)
            # a1 small (< big → match). a2 just above big (>= big → null-fill).
            client.execute_sql(f"INSERT INTO a VALUES (1, 10), (2, {big + 1})", schema_name=sn)
            assert _band_left_rows(client, vid) == {(1, 1), (2, None)}, \
                "U64 extreme above I64::MAX must order unsigned (no sign-flip threshold bug)"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    def test_pure_range_left_bag_semantics(self, client):
        """Multiplicity-preserving null-fill over a bag-valued (`UNION ALL`) view as
        the LEFT input — exercising the view-source path AND that the threshold form
        never over-fills. The one-row threshold M cannot multiply, so each left row
        contributes its own null-fill exactly once: a MATCHED logical row gets ZERO
        null-fill (summed over its branches) and an UNMATCHED one's null-fills sum to
        its input multiplicity. `u` yields each (id, x) twice (a1 ∪all a2)."""
        sn = "prbag" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a1 (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE a2 (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW u AS SELECT id, x FROM a1 UNION ALL SELECT id, x FROM a2",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT u.id AS aid, b.id AS bid "
                "FROM u LEFT JOIN b ON u.x < b.y", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")
            client.execute_sql("INSERT INTO b VALUES (1, 50)", schema_name=sn)
            # id=1 (x=10) matches b1; id=2 (x=99) does not — each present twice.
            client.execute_sql("INSERT INTO a1 VALUES (1, 10), (2, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a2 VALUES (1, 10), (2, 99)", schema_name=sn)
            # Sum weights per (aid, bid): the two branches carry distinct row PKs, so
            # they are separate rows whose weights add to the input multiplicity (2).
            from collections import defaultdict
            w = defaultdict(int)
            for r in client.scan(vid):
                w[(r["aid"], r["bid"])] += r.weight
            assert w[(1, 1)] == 2, f"matched logical row should pair twice: {dict(w)}"
            assert w[(1, None)] == 0, f"matched row must NOT null-fill: {dict(w)}"
            assert w[(2, None)] == 2, f"unmatched row null-fills once per branch: {dict(w)}"
            assert w[(2, 1)] == 0, f"unmatched row must NOT pair: {dict(w)}"
        finally:
            _drop_all(client, sn, views=["v", "u"], tables=["a1", "a2", "b"])

    def test_pure_range_left_payload_fidelity_wide_pk(self, client):
        """Byte-exact null-fill payload under a STRING column, a nullable column
        carrying NULL, and a 2-column (wide) source PK. Phase 1 inserts only matched
        a's (zero null-fills, exact payload); phase 2 adds unmatched a's and asserts
        their null-fills carry the exact A payload."""
        sn = "prpf" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id1 BIGINT NOT NULL, id2 BIGINT NOT NULL, x BIGINT NOT NULL, "
                "s VARCHAR(16) NOT NULL, note BIGINT, PRIMARY KEY (id1, id2))", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id1 AS aid1, a.id2 AS aid2, a.s AS asv, "
                "a.note AS anote, b.id AS bid "
                "FROM a LEFT JOIN b ON a.x < b.y", schema_name=sn)
            vid, _ = client.resolve_table(sn, "v")

            def rowset():
                return {(r["aid1"], r["aid2"], r["asv"], r["anote"], r["bid"])
                        for r in client.scan(vid) if r.weight > 0}

            client.execute_sql("INSERT INTO b VALUES (1, 100)", schema_name=sn)   # MAX = 100
            # Phase 1 — every a matches (x < 100).
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 10, 'alpha', 7), (1, 2, 20, 'beta', NULL)",
                schema_name=sn)
            assert rowset() == {(1, 1, "alpha", 7, 1), (1, 2, "beta", None, 1)}
            assert all(r[4] is not None for r in rowset()), "matched rows must not null-fill"

            # Phase 2 — add unmatched a's (x >= 100).
            client.execute_sql(
                "INSERT INTO a VALUES (2, 1, 100, 'gamma', 42), (2, 2, 999, 'delta', NULL)",
                schema_name=sn)
            assert rowset() == {
                (1, 1, "alpha", 7, 1), (1, 2, "beta", None, 1),
                (2, 1, "gamma", 42, None), (2, 2, "delta", None, None),
            }
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])

    @pytest.mark.parametrize("ty", ["INT NOT NULL", "INT UNSIGNED NOT NULL", "SMALLINT NOT NULL"])
    def test_pure_range_left_narrow_int_range(self, client, ty):
        """A pure-range LEFT JOIN on a NARROW integer range column (I32/U32/I16)
        null-fills correctly across workers — these were rejected by the old
        `{I64, U64}`-only cap. MIN/MAX preserves the source integer type, so the
        inline threshold reduce emits the narrow type the `reindex_m` consumes
        directly. Matched a's are suppressed, unmatched a's null-fill, and deleting
        the EXTREME b raises the threshold so an untouched a between the old and new
        MAX re-null-fills (values stay within SMALLINT's ±32767)."""
        sn = "prni" + _uid()
        client.create_schema(sn)
        try:
            vid, pred = self._mk_pure_range_left(client, sn, "<", a_x=ty, b_y=ty)
            # MAX(b.y) = 100. a's spread across workers; some below MAX, some at/above.
            b_rows = [(1, 20), (2, 60), (3, 100)]
            client.execute_sql(
                "INSERT INTO b VALUES " + ",".join(f"({i},{y})" for i, y in b_rows), schema_name=sn)
            a_rows = [(1, 10), (2, 70), (3, 100), (4, 200)]
            client.execute_sql(
                "INSERT INTO a VALUES " + ",".join(f"({i},{x})" for i, x in a_rows), schema_name=sn)
            rows = _band_left_rows(client, vid)
            assert rows == _band_left_ref(a_rows, b_rows, pred)
            assert (3, None) in rows and (4, None) in rows    # x >= 100 → null-fill
            assert (1, None) not in rows                      # x=10 < 100 → matched

            # Delete the EXTREME b3 (y=100). New MAX = 60 → a2 (x=70 >= 60) flips to
            # null-fill though a2 was untouched.
            client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=sn)
            b_rows = [r for r in b_rows if r[0] != 3]
            rows = _band_left_rows(client, vid)
            assert rows == _band_left_ref(a_rows, b_rows, pred)
            assert (2, None) in rows
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])
