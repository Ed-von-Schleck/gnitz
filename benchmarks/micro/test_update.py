"""UPDATE benchmarks: PK seek, index seek, full scan."""


from helpers.datagen import PROBE_BASE, DataGen, bulk_load, probe_val, seed_index_probes
from helpers.timing import rows_affected


def _setup(client, num_rows, with_index=False):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
    )
    pks = bulk_load(client, "t", num_rows)
    if with_index:
        client.execute_sql("CREATE INDEX ON t(val)")
    return pks


def test_update_pk_seek(client, bench_timer, scale):
    pks = _setup(client, scale["rows"])
    gen = DataGen()
    for i in range(scale["write_iters"]):
        pk = pks[i % len(pks)]
        sql = gen.update_sql("t", "val", i * 7, pk)
        bench_timer.measure(
            client.execute_sql, sql,
            rows_per_call=1,
        )


def test_update_index_seek(client, bench_timer, scale):
    _setup(client, scale["rows"], with_index=True)
    seed_index_probes(client, "t", scale["rows"] + 1)
    res = client.execute_sql(
        f"UPDATE t SET cat = -1 WHERE val = {PROBE_BASE}")
    assert rows_affected(res) == 1, "index probe matched no row"
    for i in range(scale["write_iters"]):
        bench_timer.measure(
            client.execute_sql,
            f"UPDATE t SET cat = {i} WHERE val = {probe_val(i)}",
            rows_per_call=1,
        )


def test_update_full_scan(client, bench_timer, scale):
    _setup(client, scale["rows"])
    for i in range(scale["write_iters"]):
        bench_timer.measure_rows(
            client.execute_sql,
            f"UPDATE t SET val = val + 1 WHERE val > {900_000 + i}",
            rows_fn=rows_affected,
        )
