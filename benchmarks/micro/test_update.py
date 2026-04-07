"""UPDATE benchmarks: PK seek, index seek, full scan."""

import pytest

import gnitz
from helpers.datagen import DataGen, bulk_load


def _setup(client, schema_name, num_rows, with_index=False):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "val BIGINT NOT NULL, cat BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
        gnitz.ColumnDef("cat", gnitz.TypeCode.I64),
    ]
    pks = bulk_load(client, schema_name, "t", cols, num_rows)
    if with_index:
        client.execute_sql("CREATE INDEX ON t(val)", schema_name=schema_name)
    return pks


def test_update_pk_seek(client, schema_name, bench_timer, scale):
    pks = _setup(client, schema_name, scale["rows"])
    gen = DataGen()
    for i in range(scale["write_iters"]):
        pk = pks[i % len(pks)]
        sql = gen.update_sql("t", "val", i * 7, pk)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=1,
        )


def test_update_index_seek(client, schema_name, bench_timer, scale):
    pks = _setup(client, schema_name, scale["rows"], with_index=True)
    for i in range(scale["write_iters"]):
        bench_timer.measure(
            client.execute_sql,
            f"UPDATE t SET cat = {i} WHERE val = {i * 100}",
            schema_name,
            rows_per_call=1,
        )


def test_update_full_scan(client, schema_name, bench_timer, scale):
    _setup(client, schema_name, scale["rows"])
    for i in range(scale["write_iters"]):
        bench_timer.measure(
            client.execute_sql,
            f"UPDATE t SET val = val + 1 WHERE val > {900_000 + i}",
            schema_name,
            rows_per_call=1,
        )
