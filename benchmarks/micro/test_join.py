"""JOIN throughput benchmarks: INNER, LEFT, high fan-out.

Pattern: create tables + view BEFORE data, bulk load dimension table,
then measure INSERT throughput into fact table (triggers incremental join).
"""

import pytest

import gnitz
from helpers.datagen import DataGen


def _setup_join(client, schema_name, join_type, dim_rows, seed=42):
    """Create dim + fact tables and a join view. Bulk-load dim table."""
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
        "label BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
        "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        f"CREATE VIEW v AS SELECT fact.pk, fact.amount, dim.label "
        f"FROM fact {join_type} JOIN dim ON fact.dim_pk = dim.pk",
        schema_name=schema_name,
    )
    # Bulk load dimension table via push
    dim_tid, dim_schema = client.resolve_table(schema_name, "dim")
    batch = gnitz.ZSetBatch(dim_schema)
    import random
    rng = random.Random(seed)
    for i in range(1, dim_rows + 1):
        batch.append(pk=i, label=rng.randint(0, 100))
    client.push(dim_tid, batch)


def test_inner_join(client, schema_name, bench_timer, scale):
    dim_rows = min(scale["rows"] // 10, 1000)
    _setup_join(client, schema_name, "INNER", dim_rows)
    gen = DataGen()
    fk_range = {"dim_pk": (1, dim_rows)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("fact", ["pk", "dim_pk", "amount"], 100, i,
                             col_ranges=fk_range)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_left_join(client, schema_name, bench_timer, scale):
    dim_rows = min(scale["rows"] // 10, 1000)
    _setup_join(client, schema_name, "LEFT", dim_rows)
    gen = DataGen()
    fk_range = {"dim_pk": (1, dim_rows)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("fact", ["pk", "dim_pk", "amount"], 100, i,
                             col_ranges=fk_range)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )


def test_join_high_fanout(client, schema_name, bench_timer, scale):
    """Right table has many matches per left row (small dim, large fact)."""
    _setup_join(client, schema_name, "INNER", dim_rows=10)
    gen = DataGen()
    fk_range = {"dim_pk": (1, 10)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("fact", ["pk", "dim_pk", "amount"], 100, i,
                             col_ranges=fk_range)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
