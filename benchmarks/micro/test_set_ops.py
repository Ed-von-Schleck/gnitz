"""Set operation benchmarks: UNION ALL, UNION, EXCEPT."""

import pytest

from helpers.datagen import DataGen


def _setup_two_tables(client, schema_name):
    for name in ("t1", "t2"):
        client.execute_sql(
            f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, "
            f"val BIGINT NOT NULL)",
            schema_name=schema_name,
        )


def test_union_all(client, schema_name, bench_timer, scale):
    _setup_two_tables(client, schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t1 UNION ALL SELECT * FROM t2",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t1", ["pk", "val"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_union_distinct(client, schema_name, bench_timer, scale):
    _setup_two_tables(client, schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t1 UNION SELECT * FROM t2",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t1", ["pk", "val"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_except(client, schema_name, bench_timer, scale):
    _setup_two_tables(client, schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t1 EXCEPT SELECT * FROM t2",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t1", ["pk", "val"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
