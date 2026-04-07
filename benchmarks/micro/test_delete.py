"""DELETE benchmarks: PK seek, scan-based."""

import pytest

import gnitz
from helpers.datagen import bulk_load


def _setup(client, schema_name, num_rows):
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
    return bulk_load(client, schema_name, "t", cols, num_rows)


def test_delete_pk(client, schema_name, bench_timer, scale):
    pks = _setup(client, schema_name, scale["rows"])
    for i in range(min(scale["write_iters"], len(pks))):
        bench_timer.measure(
            client.execute_sql,
            f"DELETE FROM t WHERE pk = {pks[i]}",
            schema_name,
            rows_per_call=1,
        )


def test_delete_scan(client, schema_name, bench_timer, scale):
    _setup(client, schema_name, scale["rows"])
    for i in range(scale["write_iters"]):
        # Delete a narrow slice each iteration to avoid emptying the table
        lo = 900_000 + i * 1000
        hi = lo + 1000
        bench_timer.measure(
            client.execute_sql,
            f"DELETE FROM t WHERE val > {lo} AND val < {hi}",
            schema_name,
            rows_per_call=1,
        )
