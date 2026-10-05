"""SELECT throughput benchmarks: full scan, PK seek, index seek, LIMIT, and a cut in key order."""


from helpers.datagen import PROBE_BASE, bulk_load, probe_val, seed_index_probes


def _setup_table(client, num_rows):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
    )
    bulk_load(client, "t", num_rows)
    return num_rows


def test_full_scan(client, bench_timer, scale):
    n = _setup_table(client, scale["rows"])
    for _ in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql, "SELECT * FROM t",
            rows_per_call=n,
        )


def test_pk_seek(client, bench_timer, scale):
    _setup_table(client, scale["rows"])
    for i in range(scale["read_iters"]):
        pk = (i % scale["rows"]) + 1
        bench_timer.measure(
            client.execute_sql,
            f"SELECT * FROM t WHERE pk = {pk}",
            rows_per_call=1,
        )


def test_index_seek(client, bench_timer, scale):
    _setup_table(client, scale["rows"])
    client.execute_sql("CREATE INDEX ON t(val)")
    seed_index_probes(client, "t", scale["rows"] + 1)
    res = client.execute_sql(
        f"SELECT * FROM t WHERE val = {PROBE_BASE}")
    assert len(list(res[0]["rows"])) == 1, "index probe matched no row"
    for i in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql,
            f"SELECT * FROM t WHERE val = {probe_val(i)}",
            rows_per_call=1,
        )


def test_limit(client, bench_timer, scale):
    _setup_table(client, scale["rows"])
    for _ in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql,
            "SELECT * FROM t LIMIT 100",
            rows_per_call=100,
        )


def test_order_by_pk_limit(client, bench_timer, scale):
    _setup_table(client, scale["rows"])
    for _ in range(scale["read_iters"]):
        bench_timer.measure(
            client.execute_sql,
            "SELECT * FROM t ORDER BY pk LIMIT 100",
            rows_per_call=100,
        )


def test_keyset_page(client, bench_timer, scale):
    n = _setup_table(client, scale["rows"])
    for i in range(scale["read_iters"]):
        after = (i * 7919) % max(n - 100, 1)
        bench_timer.measure(
            client.execute_sql,
            f"SELECT * FROM t WHERE pk > {after} ORDER BY pk LIMIT 100",
            rows_per_call=100,
        )
