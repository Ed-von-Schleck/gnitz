"""Deterministic data generators and bulk-load helpers for benchmarks."""

from __future__ import annotations

import random

import gnitz

SCALES = {
    "quick": {
        "rows": 1_000,
        "insert_batches": 10,
        "read_iters": 20,
        "write_iters": 10,
        "delta_rows": 50,
    },
    "full": {
        "rows": 150_000,
        "insert_batches": 1500,
        "read_iters": 50,
        "write_iters": 50,
        "delta_rows": 100,
    },
}


class DataGen:
    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)

    def insert_sql(
        self,
        table: str,
        cols: list[str],
        batch_size: int,
        batch_idx: int,
        col_ranges: dict[str, tuple[int, int]] | None = None,
    ) -> str:
        """Generate INSERT INTO t VALUES (...), ... with deterministic values.

        PKs are monotonic (batch_idx * batch_size + offset) to avoid conflicts.
        col_ranges: optional {col_name: (lo, hi)} to constrain specific columns.
        """
        base_pk = batch_idx * batch_size
        rows = []
        for off in range(batch_size):
            pk = base_pk + off + 1  # 1-based PKs
            vals = [str(pk)]
            for c in cols[1:]:  # skip pk column
                if col_ranges and c in col_ranges:
                    lo, hi = col_ranges[c]
                    vals.append(str(self._rng.randint(lo, hi)))
                else:
                    vals.append(str(self._rng.randint(0, 1_000_000)))
            rows.append(f"({', '.join(vals)})")
        col_list = ", ".join(cols)
        return f"INSERT INTO {table} ({col_list}) VALUES {', '.join(rows)}"

    def insert_stmts(
        self, table: str, cols: list[str], batch_size: int, num_batches: int
    ) -> list[str]:
        return [
            self.insert_sql(table, cols, batch_size, i)
            for i in range(num_batches)
        ]

    def update_sql(
        self, table: str, set_col: str, set_val: int, pk: int
    ) -> str:
        return f"UPDATE {table} SET {set_col} = {set_val} WHERE pk = {pk}"

    def delete_sql(self, table: str, pk: int) -> str:
        return f"DELETE FROM {table} WHERE pk = {pk}"


def bulk_load(
    conn,
    schema_name: str,
    table_name: str,
    columns: list,
    num_rows: int,
    seed: int = 42,
) -> list[int]:
    """Bulk load via push() API (ZSetBatch). Returns list of PKs loaded."""
    tid, schema = conn.resolve_table(schema_name, table_name)
    rng = random.Random(seed)
    batch = gnitz.ZSetBatch(schema)
    pks = []
    for i in range(1, num_rows + 1):
        row = {"pk": i}
        for col in columns:
            if col.name == "pk":
                continue
            tc = col.type_code
            if tc in (gnitz.TypeCode.I64, gnitz.TypeCode.U64):
                row[col.name] = rng.randint(0, 1_000_000)
            elif tc in (gnitz.TypeCode.I32, gnitz.TypeCode.U32):
                row[col.name] = rng.randint(0, 100_000)
            elif tc == gnitz.TypeCode.F64:
                row[col.name] = rng.random() * 1_000_000
            else:
                row[col.name] = rng.randint(0, 1_000_000)
        batch.append(**row)
        pks.append(i)
    conn.push(tid, batch)
    return pks


def bulk_load_pair(
    conn,
    schema_name: str,
    left_table: str,
    left_cols: list,
    right_table: str,
    right_cols: list,
    left_rows: int,
    right_rows: int,
    fk_column: str = "fk",
    seed: int = 42,
) -> tuple[list[int], list[int]]:
    """Load two related tables for join benchmarks (FK relationship)."""
    rng = random.Random(seed)

    # Load left (dimension) table
    left_tid, left_schema = conn.resolve_table(schema_name, left_table)
    left_batch = gnitz.ZSetBatch(left_schema)
    left_pks = []
    for i in range(1, left_rows + 1):
        row = {"pk": i}
        for col in left_cols:
            if col.name == "pk":
                continue
            row[col.name] = rng.randint(0, 100_000)
        left_batch.append(**row)
        left_pks.append(i)
    conn.push(left_tid, left_batch)

    # Load right (fact) table with FK pointing to left PKs
    right_tid, right_schema = conn.resolve_table(schema_name, right_table)
    right_batch = gnitz.ZSetBatch(right_schema)
    right_pks = []
    for i in range(1, right_rows + 1):
        row = {"pk": i, fk_column: rng.randint(1, left_rows)}
        for col in right_cols:
            if col.name in ("pk", fk_column):
                continue
            row[col.name] = rng.randint(0, 1_000_000)
        right_batch.append(**row)
        right_pks.append(i)
    conn.push(right_tid, right_batch)

    return left_pks, right_pks
