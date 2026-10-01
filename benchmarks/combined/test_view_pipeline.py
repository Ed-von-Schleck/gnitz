"""Combined benchmark: deep view pipeline (5 views)."""

import random

import gnitz
from helpers.datagen import DataGen


def test_deep_pipeline(client, bench_timer, scale):
    """base -> filter -> join -> agg -> filter -> distinct (5 views)."""
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
        "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
        "category BIGINT NOT NULL)",
    )
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
        "label BIGINT NOT NULL)",
    )

    # v1: filter
    client.execute_sql(
        "CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 50000",
    )
    # v2: join
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, v1.category, dim.label "
        "FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk",
    )
    # v3: agg
    client.execute_sql(
        "CREATE VIEW v3 AS SELECT label, SUM(amount) AS total, "
        "COUNT(*) AS cnt FROM v2 GROUP BY label",
    )
    # v4: filter on agg
    client.execute_sql(
        "CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 100000",
    )
    # v5: distinct
    client.execute_sql(
        "CREATE VIEW v5 AS SELECT DISTINCT label FROM v4",
    )

    # Bulk load dim
    rng = random.Random(42)
    dim_tid, dim_schema = client.resolve_table("dim")
    batch = gnitz.ZSetBatch(dim_schema)
    for i in range(1, 51):
        batch.append(pk=i, label=rng.randint(0, 20))
    client.push(dim_tid, batch)

    gen = DataGen()
    fk_range = {"dim_pk": (1, 50), "category": (0, 20)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql(
            "fact", ["pk", "dim_pk", "amount", "category"], 100, i,
            col_ranges=fk_range,
        )
        bench_timer.measure(
            client.execute_sql, sql,
            rows_per_call=100,
        )
