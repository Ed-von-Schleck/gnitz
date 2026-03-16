import os
import random
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
    client.push(tid, schema, batch)

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
            result = client.seek_by_index(tid, col_idx=1, key_lo=i * 100)
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
        result = client.seek(tid, pk_lo=42)
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
        result = client.seek_by_index(tid, col_idx=1, key_lo=1234)
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
