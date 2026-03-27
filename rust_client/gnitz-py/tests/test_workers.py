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
            result = client.seek_by_index(tid, col_idx=1, key=i * 100)
            assert result.batch is not None and len(result.batch.pk_lo) == 1, \
                f"cust_id={i * 100} not found via index"
            assert result.batch.pk_lo[0] == i
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
        assert result.batch is not None and len(result.batch.pk_lo) == 1
        assert result.batch.pk_lo[0] == 42
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
        result = client.seek_by_index(tid, col_idx=1, key=1234)
        assert result.batch is not None and len(result.batch.pk_lo) == 1
        assert result.batch.pk_lo[0] == 7
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_cust_id"], tables=["t"])


@_NEEDS_MULTI
def test_unique_pk_across_workers(client):
    """
    unique_pk UPSERT semantics work correctly across workers:
    a second insert with the same PK replaces the first row.
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
        client.execute_sql("INSERT INTO t VALUES (1, 200)", schema_name=sn)

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
    """Push PK=1 val=10, then PK=1 val=99 (upsert); scan → one row with val=99."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 99)", schema_name=sn)

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
    """Retract rows through reduce SUM view; verify sum decreases."""
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
        client.execute_sql(
            "INSERT INTO t VALUES (1, 1, 100), (2, 1, 200)", schema_name=sn
        )
        vid, _ = client.resolve_table(sn, "v")

        rows_before = {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}
        assert rows_before[1] == 300

        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
        rows_after = {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}
        assert rows_after[1] == 100
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
        rows = {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}
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
        rows = client.scan(vid)
        assert len(rows) == 50, f"expected 50, got {len(rows)}"

        # Delete a subset and verify retraction
        client.execute_sql("DELETE FROM t WHERE pk = 10", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 25", schema_name=sn)
        rows = client.scan(vid)
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
