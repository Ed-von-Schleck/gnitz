"""Combined benchmark: filter -> join -> agg (3-hop view pipeline)."""

import random
import pytest

import gnitz
from helpers.datagen import DataGen


def test_filter_join_agg(client, schema_name, bench_timer, scale):
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
        "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
        "category BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
        "region BIGINT NOT NULL)",
        schema_name=schema_name,
    )

    # 3-hop: filter -> join -> agg
    client.execute_sql(
        "CREATE VIEW v_filtered AS SELECT * FROM fact WHERE amount > 100000",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v_joined AS SELECT v_filtered.pk, v_filtered.amount, "
        "v_filtered.category, dim.region "
        "FROM v_filtered INNER JOIN dim ON v_filtered.dim_pk = dim.pk",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total "
        "FROM v_joined GROUP BY region",
        schema_name=schema_name,
    )

    # Bulk load dim
    rng = random.Random(42)
    dim_tid, dim_schema = client.resolve_table(schema_name, "dim")
    batch = gnitz.ZSetBatch(dim_schema)
    for i in range(1, 101):
        batch.append(pk=i, region=rng.randint(0, 10))
    client.push(dim_tid, batch)

    gen = DataGen()
    fk_range = {"dim_pk": (1, 100), "category": (0, 20)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql(
            "fact", ["pk", "dim_pk", "amount", "category"], 100, i,
            col_ranges=fk_range,
        )
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
