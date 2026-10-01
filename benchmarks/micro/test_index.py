"""Index benchmarks: CREATE INDEX cost, indexed vs non-indexed INSERT."""


import time

from helpers.datagen import DataGen, bulk_load


def test_create_index_on_populated(client, bench_timer, scale):
    """Time CREATE INDEX after bulk loading data."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
    )
    bulk_load(client, "t", scale["rows"])
    # One sample — `measure`'s warmup gate would discard it.
    start = time.perf_counter()
    client.execute_sql("CREATE INDEX ON t(val)")
    bench_timer.add_latencies([(time.perf_counter() - start) * 1000.0],
                              rows=scale["rows"])


def test_insert_with_index(client, bench_timer, scale):
    """INSERT throughput on table with an active index."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
    )
    client.execute_sql("CREATE INDEX ON t(val)")
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql,
            rows_per_call=100,
        )


def test_insert_without_index(client, bench_timer, scale):
    """INSERT throughput baseline (no secondary index)."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
    )
    gen = DataGen()
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("t", ["pk", "val", "cat"], 100, i)
        bench_timer.measure(
            client.execute_sql, sql,
            rows_per_call=100,
        )
