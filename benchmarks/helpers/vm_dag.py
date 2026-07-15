"""Shared fact/dim view DAG for the flagship view-maintenance benches.

A filter -> join -> grouped SUM/COUNT + grouped MIN/MAX + DISTINCT DAG. Two
benches stream deltas over it: test_view_maintenance (push() — real maintenance
cost) and test_view_maintenance_sql (INSERT VALUES — the SQL-path ceiling).
"""

from __future__ import annotations

from helpers.datagen import push_stream

AMOUNT_MAX = 1_000_000
OPEN_PROB = 0.5            # P(o_status == 1); v_open keeps status == 1 rows
CLOSED_STATUSES = (0, 2, 3, 4)
UPDATES_PER_ITER = 12
DELETES_PER_ITER = 3

# Per-scale sizing. full pushes the dimension, the grouped-reduce cardinality,
# and the DISTINCT domain past the 4 MiB ephemeral-store ceiling so those view
# stores are disk-resident and the base table is heavily sharded; quick stays
# small for a fast functional check.
SIZES = {
    "quick": dict(DIM_ROWS=2_000, REGION_CARD=200, NATION_CARD=50,
                  FACT_SEED=20_000, INSERT_ROWS=2_000, DURATION_S=8.0),
    "full": dict(DIM_ROWS=1_000_000, REGION_CARD=200_000, NATION_CARD=5_000,
                 FACT_SEED=1_000_000, INSERT_ROWS=4_000, DURATION_S=60.0),
}


def setup_views(client, sn):
    client.execute_sql(
        "CREATE TABLE dim_customer (c_id BIGINT NOT NULL PRIMARY KEY, "
        "c_region BIGINT NOT NULL, c_nation BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE fact_orders (o_id BIGINT NOT NULL PRIMARY KEY, "
        "o_customer BIGINT NOT NULL, o_amount BIGINT NOT NULL, "
        "o_status BIGINT NOT NULL)", schema_name=sn)
    # A view processes only deltas applied after it exists, so create the views
    # before loading any data.
    client.execute_sql(
        "CREATE VIEW v_open AS SELECT o_id, o_customer, o_amount "
        "FROM fact_orders WHERE o_status = 1", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_join AS SELECT v_open.o_id AS o_id, "
        "v_open.o_amount AS o_amount, dim_customer.c_region AS c_region, "
        "dim_customer.c_nation AS c_nation FROM v_open "
        "JOIN dim_customer ON v_open.o_customer = dim_customer.c_id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_rev AS SELECT c_region, SUM(o_amount) AS revenue, "
        "COUNT(*) AS cnt FROM v_join GROUP BY c_region", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_ext AS SELECT c_region, MIN(o_amount) AS lo, "
        "MAX(o_amount) AS hi FROM v_join GROUP BY c_region", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_active AS SELECT DISTINCT o_customer FROM v_open",
        schema_name=sn)


def status(rng):
    return 1 if rng.random() < OPEN_PROB else rng.choice(CLOSED_STATUSES)


def seed(client, sn, sz, rng):
    dim_tid, dim_schema = client.resolve_table(sn, "dim_customer")
    push_stream(client, dim_tid, dim_schema,
                lambda b, k: b.append(c_id=k + 1, c_region=(k + 1) % sz["REGION_CARD"],
                                      c_nation=(k + 1) % sz["NATION_CARD"]),
                sz["DIM_ROWS"])
    fact_tid, fact_schema = client.resolve_table(sn, "fact_orders")
    dim_rows = sz["DIM_ROWS"]
    push_stream(client, fact_tid, fact_schema,
                lambda b, k: b.append(o_id=k + 1, o_customer=rng.randint(1, dim_rows),
                                      o_amount=rng.randint(0, AMOUNT_MAX),
                                      o_status=status(rng)),
                sz["FACT_SEED"])
