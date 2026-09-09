"""How rows land on workers, and what the planner may skip once they have.

A scan must return every partition's rows; a wide U64 PK range must spread
over the workers rather than pile onto one; a single-row insert leaves every
other worker with an empty batch that still has to cross the barrier. The
pre-plan cases are the other direction: a view already co-partitioned with its
source needs no exchange at all, and neither does a join on the PK column.
"""

import os
import pytest
import gnitz
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


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
    pks = sorted(row.pk for row in result)
    assert pks == list(range(1, n + 1))

    client.drop_table(sn, "big")
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
            assert result.schema is not None and len(result.pks) == 1, \
                f"cust_id={i * 100} not found via index"
            assert result.pks[0] == i
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
        pks = sorted(row.pk for row in result)
        assert len(pks) == n, f"expected {n} rows, got {len(pks)}"
        assert pks == list(range(1, n + 1))
    finally:
        _drop_all(client, sn, tables=["t"])

@_NEEDS_MULTI
def test_trivial_preplan_no_exchange(client):
    """
    4b+4c: Trivial non-co-partitioned pre-plan (GROUP BY grp, shard col != pk).
    Correct COUNT result verifies the trivial pre-plan path works.
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
        # Visible layout: row[0]=grp, row[1]=cnt (the synthetic group PK is hidden).
        rows = {r[0]: r[1]
                for r in client.scan(vid)}
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
        rows = {r["id"]: r["total"] for r in client.scan(vid)}
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
        # Visible layout: row[0]=a.id, row[1]=a.x, row[2]=b.y (the synthetic
        # _join_pk is hidden).
        rows = client.scan(vid)
        live_rows = list(rows)
        assert len(live_rows) == n, f"expected {n} join rows, got {len(live_rows)}"
        by_id = {r[0]: r for r in live_rows}
        for i in range(1, n + 1):
            r = by_id.get(i)
            assert r is not None, f"id={i} missing from join result"
            assert r[1] == i * 2, f"id={i} x: expected {i * 2}, got {r[1]}"
            assert r[2] == i * 3, f"id={i} y: expected {i * 3}, got {r[2]}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])

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
        pks = sorted(row.pk for row in result)
        assert pks == [1]
    finally:
        _drop_all(client, sn, tables=["t"])

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
            ctypes.c_uint64(r.pk).value for r in result
        )
        assert returned_pks == sorted(all_pks), (
            f"expected {len(all_pks)} distinct PKs, got {len(returned_pks)}"
        )
    finally:
        _drop_all(client, sn, tables=["t"])
