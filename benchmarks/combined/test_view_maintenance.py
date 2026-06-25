"""Sustained view-maintenance workload, for perf profiling.

A fact/dim schema with a filter -> join -> grouped SUM/COUNT + grouped MIN/MAX +
DISTINCT view DAG. Bulk-loads a base, then runs a continuous INSERT/UPDATE/DELETE
stream for a fixed wall-clock window. Run under perf with `make bench-profile`.
"""

from __future__ import annotations

import random
import time

import gnitz

AMOUNT_MAX = 1_000_000
OPEN_PROB = 0.5            # P(o_status == 1); v_open keeps status == 1 rows
CLOSED_STATUSES = (0, 2, 3, 4)
SEED_CHUNK = 250_000      # rows per seed push (each > 10k trips the auto-tick)
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


def _setup_views(client, sn):
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


def _status(rng):
    return 1 if rng.random() < OPEN_PROB else rng.choice(CLOSED_STATUSES)


def _push_chunked(client, tid, schema, build, total):
    """Push rows 1..total in SEED_CHUNK batches; build(batch, pk) appends one row."""
    pk = 1
    while pk <= total:
        batch = gnitz.ZSetBatch(schema)
        end = min(pk + SEED_CHUNK, total + 1)
        for k in range(pk, end):
            build(batch, k)
        client.push(tid, batch)
        pk = end


def _seed(client, sn, sz, rng):
    dim_tid, dim_schema = client.resolve_table(sn, "dim_customer")
    _push_chunked(client, dim_tid, dim_schema,
                  lambda b, c: b.append(c_id=c, c_region=c % sz["REGION_CARD"],
                                        c_nation=c % sz["NATION_CARD"]),
                  sz["DIM_ROWS"])
    fact_tid, fact_schema = client.resolve_table(sn, "fact_orders")
    dim_rows = sz["DIM_ROWS"]
    _push_chunked(client, fact_tid, fact_schema,
                  lambda b, o: b.append(o_id=o, o_customer=rng.randint(1, dim_rows),
                                        o_amount=rng.randint(0, AMOUNT_MAX),
                                        o_status=_status(rng)),
                  sz["FACT_SEED"])


def test_view_maintenance(client, schema_name, bench_timer, scale_mode):
    """Stream INSERT/UPDATE/DELETE over the view DAG for a wall-clock window."""
    sn = schema_name
    sz = SIZES[scale_mode]
    rng = random.Random(42)
    _setup_views(client, sn)
    _seed(client, sn, sz, rng)

    dim_rows, insert_rows = sz["DIM_ROWS"], sz["INSERT_ROWS"]
    next_pk = sz["FACT_SEED"] + 1
    t_end = time.monotonic() + sz["DURATION_S"]
    while time.monotonic() < t_end:
        rows = []
        for _ in range(insert_rows):
            pk = next_pk
            next_pk += 1
            rows.append(f"({pk},{rng.randint(1, dim_rows)},"
                        f"{rng.randint(0, AMOUNT_MAX)},{_status(rng)})")
        ins = ("INSERT INTO fact_orders (o_id, o_customer, o_amount, o_status) "
               "VALUES " + ",".join(rows))
        bench_timer.measure(client.execute_sql, ins, sn, rows_per_call=insert_rows)

        hi = next_pk - 1
        for _ in range(UPDATES_PER_ITER):
            client.execute_sql(
                f"UPDATE fact_orders SET o_status={_status(rng)}, "
                f"o_amount={rng.randint(0, AMOUNT_MAX)} WHERE o_id={rng.randint(1, hi)}",
                schema_name=sn)
        for _ in range(DELETES_PER_ITER):
            client.execute_sql(
                f"DELETE FROM fact_orders WHERE o_id={rng.randint(1, hi)}",
                schema_name=sn)

    res = client.execute_sql("SELECT * FROM v_rev", schema_name=sn)
    assert len(res[0]["rows"]) > 0, "v_rev empty"
