"""View-based WHERE filtering benchmarks.

Views must be created BEFORE data insertion (DBSP: views only process future deltas).
Measured phase: INSERT batches with active view, measuring incremental view maintenance.
"""

import pytest

from helpers.datagen import DataGen


def test_filter_equality(client, schema_name, bench_timer, scale):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t WHERE val = 42",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_filter_range(client, schema_name, bench_timer, scale):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t WHERE val > 100000 AND val < 500000",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_filter_compound(client, schema_name, bench_timer, scale):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t "
        "WHERE (a > 10 AND b < 50) OR c = 0",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "a", "b", "c"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
