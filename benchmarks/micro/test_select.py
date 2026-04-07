"""SELECT throughput benchmarks: full scan, PK seek, index seek, LIMIT."""

import pytest

from helpers.datagen import bulk_load
import gnitz


def _setup_table(client, schema_name, num_rows):
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
    bulk_load(client, schema_name, "t", cols, num_rows)
    return num_rows


def test_full_scan(client, schema_name, bench_timer, scale):
    n = _setup_table(client, schema_name, scale["rows"])
    for _ in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql, "SELECT * FROM t", schema_name,
            rows_per_call=n,
        )


def test_pk_seek(client, schema_name, bench_timer, scale):
    _setup_table(client, schema_name, scale["rows"])
    for i in range(scale["read_iters"]):
        pk = (i % scale["rows"]) + 1
        bench_timer.measure(
            client.execute_sql,
            f"SELECT * FROM t WHERE pk = {pk}", schema_name,
            rows_per_call=1,
        )


def test_index_seek(client, schema_name, bench_timer, scale):
    _setup_table(client, schema_name, scale["rows"])
    client.execute_sql("CREATE INDEX ON t(val)", schema_name=schema_name)
    for i in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql,
            f"SELECT * FROM t WHERE val = {i * 100}", schema_name,
            rows_per_call=1,
        )


def test_limit(client, schema_name, bench_timer, scale):
    _setup_table(client, schema_name, scale["rows"])
    for _ in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql,
            "SELECT * FROM t LIMIT 100", schema_name,
            rows_per_call=100,
        )
