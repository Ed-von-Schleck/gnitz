"""Aggregation benchmarks: COUNT/SUM/MIN/MAX/AVG + GROUP BY + HAVING."""

import pytest

from helpers.datagen import DataGen


@pytest.mark.parametrize("agg_fn", [
    "COUNT(*)", "SUM(amount)", "MIN(amount)", "MAX(amount)", "AVG(amount)",
])
def test_group_by(client, schema_name, bench_timer, scale, agg_fn):
    client.execute_sql(
        "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, "
        "category BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        f"CREATE VIEW v AS SELECT category, {agg_fn} AS agg "
        f"FROM orders GROUP BY category",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("orders", ["pk", "category", "amount"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_having(client, schema_name, bench_timer, scale):
    client.execute_sql(
        "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, "
        "category BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt "
        "FROM orders GROUP BY category HAVING COUNT(*) > 5",
        schema_name=schema_name,
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("orders", ["pk", "category", "amount"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
