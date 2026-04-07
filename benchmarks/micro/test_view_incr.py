"""Incremental view maintenance cost — the key DBSP benchmark.

Measures delta processing time after bulk base load. Delta processing
should be proportional to delta size, not base data size.
"""

import random
import pytest

import gnitz
from helpers.datagen import DataGen, bulk_load


def _create_base_tables(client, schema_name):
    """Create fact + dim tables for the various view types."""
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
        "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
        "category BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
        "label BIGINT NOT NULL)",
        schema_name=schema_name,
    )


def _create_view(client, schema_name, view_type):
    """Create view(s) matching the view_type BEFORE any data insertion."""
    if view_type == "filter":
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM fact WHERE amount > 500000",
            schema_name=schema_name,
        )
    elif view_type == "join":
        client.execute_sql(
            "CREATE VIEW v AS SELECT fact.pk, fact.amount, dim.label "
            "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
            schema_name=schema_name,
        )
    elif view_type == "agg":
        client.execute_sql(
            "CREATE VIEW v AS SELECT category, SUM(amount) AS total "
            "FROM fact GROUP BY category",
            schema_name=schema_name,
        )
    elif view_type == "join_agg":
        # View-on-view: join first, then aggregate
        client.execute_sql(
            "CREATE VIEW v_joined AS SELECT fact.pk, fact.amount, "
            "fact.category, dim.label "
            "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
            schema_name=schema_name,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT category, SUM(amount) AS total "
            "FROM v_joined GROUP BY category",
            schema_name=schema_name,
        )
    elif view_type == "view_chain":
        # 3-hop: filter -> join -> agg
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 100000",
            schema_name=schema_name,
        )
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, dim.label "
            "FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk",
            schema_name=schema_name,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT label, COUNT(*) AS cnt "
            "FROM v2 GROUP BY label",
            schema_name=schema_name,
        )


def _bulk_load_base(client, schema_name, num_rows, dim_rows=100, seed=42):
    """Bulk load fact + dim tables via push()."""
    rng = random.Random(seed)

    dim_tid, dim_schema = client.resolve_table(schema_name, "dim")
    dim_batch = gnitz.ZSetBatch(dim_schema)
    for i in range(1, dim_rows + 1):
        dim_batch.append(pk=i, label=rng.randint(0, 50))
    client.push(dim_tid, dim_batch)

    fact_tid, fact_schema = client.resolve_table(schema_name, "fact")
    fact_batch = gnitz.ZSetBatch(fact_schema)
    for i in range(1, num_rows + 1):
        fact_batch.append(
            pk=i,
            dim_pk=rng.randint(1, dim_rows),
            amount=rng.randint(0, 1_000_000),
            category=rng.randint(0, 20),
        )
    client.push(fact_tid, fact_batch)
    return num_rows


@pytest.mark.parametrize("view_type", [
    "filter", "join", "agg", "join_agg", "view_chain",
])
def test_incremental_cost(client, schema_name, bench_timer, scale, view_type):
    """INSERT small delta after bulk base load — delta time should scale
    with delta size, not base data size."""
    _create_base_tables(client, schema_name)
    _create_view(client, schema_name, view_type)
    _bulk_load_base(client, schema_name, scale["rows"])

    gen = DataGen(seed=9999)
    pk_base = scale["rows"] + 1
    fk_range = {"dim_pk": (1, 100), "category": (0, 20)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql(
            "fact", ["pk", "dim_pk", "amount", "category"],
            scale["delta_rows"], pk_base + i * scale["delta_rows"],
            col_ranges=fk_range,
        )
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=scale["delta_rows"],
        )
