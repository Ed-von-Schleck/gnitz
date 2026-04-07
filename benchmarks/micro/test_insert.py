"""INSERT VALUES throughput benchmarks."""

import pytest

from helpers.datagen import DataGen


@pytest.mark.parametrize("batch_size", [1, 10, 100])
def test_insert_values(client, schema_name, bench_timer, scale, batch_size):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], batch_size, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=batch_size,
        )


def test_insert_bulk_throughput(client, schema_name, bench_timer, scale):
    """INSERT many rows in batches of 100, measure sustained throughput."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["insert_batches"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
