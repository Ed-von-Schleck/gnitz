"""FK validation overhead benchmark: FK-checked inserts vs plain inserts."""

import pytest

from helpers.datagen import DataGen


def test_fk_insert_overhead(client, schema_name, bench_timer, scale):
    """Insert N child rows (FK-validated) vs N rows into a plain table."""
    sn = schema_name
    batch_size = 50

    # Create parent + child with FK
    client.execute_sql(
        "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE TABLE child ("
        "  cid BIGINT NOT NULL PRIMARY KEY,"
        "  pid BIGINT NOT NULL REFERENCES parent(id)"
        ")",
        schema_name=sn,
    )

    # Pre-populate parents
    n_parents = scale["write_iters"] * batch_size
    for start in range(0, n_parents, 100):
        end = min(start + 100, n_parents)
        vals = ", ".join(f"({i + 1})" for i in range(start, end))
        client.execute_sql(f"INSERT INTO parent VALUES {vals}", schema_name=sn)

    # Benchmark: insert children in batches (FK-validated via IPC)
    pk = 0
    for i in range(scale["write_iters"]):
        vals = ", ".join(
            f"({pk + j + 1}, {pk + j + 1})" for j in range(batch_size)
        )
        pk += batch_size
        bench_timer.measure(
            client.execute_sql,
            f"INSERT INTO child VALUES {vals}",
            sn,
            rows_per_call=batch_size,
        )
