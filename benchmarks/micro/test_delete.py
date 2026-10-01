"""DELETE benchmarks: PK seek, scan-based."""


from helpers.datagen import bulk_load
from helpers.timing import rows_affected


def _setup(client, num_rows):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
    )
    return bulk_load(client, "t", num_rows)


def test_delete_pk(client, bench_timer, scale):
    pks = _setup(client, scale["rows"])
    for i in range(min(scale["write_iters"], len(pks))):
        bench_timer.measure(
            client.execute_sql,
            f"DELETE FROM t WHERE pk = {pks[i]}",
            rows_per_call=1,
        )


def test_delete_scan(client, bench_timer, scale):
    _setup(client, scale["rows"])
    for i in range(scale["write_iters"]):
        # Delete a narrow slice each iteration to avoid emptying the table
        lo = 900_000 + i * 1000
        hi = lo + 1000
        bench_timer.measure_rows(
            client.execute_sql,
            f"DELETE FROM t WHERE val > {lo} AND val < {hi}",
            rows_fn=rows_affected,
        )
