"""DISTINCT benchmark."""

import pytest

from helpers.datagen import DataGen


def test_distinct_view(client, schema_name, bench_timer, scale):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT DISTINCT val FROM t",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
