"""Index benchmarks: CREATE INDEX cost, indexed vs non-indexed INSERT."""

import pytest

import gnitz
from helpers.datagen import DataGen, bulk_load


def test_create_index_on_populated(client, schema_name, bench_timer, scale):
    """Time CREATE INDEX after bulk loading data."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
        gnitz.ColumnDef("cat", gnitz.TypeCode.I64),
    ]
    bulk_load(client, schema_name, "t", cols, scale["rows"])
    bench_timer.measure(
        client.execute_sql, "CREATE INDEX ON t(val)", schema_name,
        rows_per_call=scale["rows"],
    )


def test_insert_with_index(client, schema_name, bench_timer, scale):
    """INSERT throughput on table with an active index."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("CREATE INDEX ON t(val)", schema_name=schema_name)
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_insert_without_index(client, schema_name, bench_timer, scale):
    """INSERT throughput baseline (no secondary index)."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
