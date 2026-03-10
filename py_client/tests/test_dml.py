"""Test DML operations: insert, delete, scan."""

from gnitz_client import GnitzClient, TypeCode, ColumnDef
from gnitz_client.batch import ZSetBatch


def _make_table(client):
    """Helper: create a fresh schema + table with (U64 pk, I64 value)."""
    import random
    suffix = random.randint(1000, 99999)
    schema_name = f"dml_{suffix}"
    client.create_schema(schema_name)
    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("value", TypeCode.I64),
    ]
    tid = client.create_table(schema_name, f"t_{suffix}", columns, pk_col_idx=0)
    from gnitz_client.types import Schema
    tbl_schema = Schema(columns=columns, pk_index=0)
    return tid, tbl_schema


def test_insert_and_scan(client):
    """Insert 3 rows, scan, verify all values."""
    tid, tbl_schema = _make_table(client)

    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[1, 2, 3],
        pk_hi=[0, 0, 0],
        weights=[1, 1, 1],
        nulls=[0, 0, 0],
        columns=[[], [100, 200, 300]],
    )
    client.push(tid, tbl_schema, batch)

    # Scan
    schema, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 3

    # Verify values (sorted by PK)
    pks = list(result.pk_lo)
    vals = list(result.columns[1])  # value column (index 1, payload)
    rows = sorted(zip(pks, vals))
    assert rows == [(1, 100), (2, 200), (3, 300)]


def test_delete_and_scan(client):
    """Insert 3 rows, delete 1, scan, verify 2 remain."""
    tid, tbl_schema = _make_table(client)

    # Insert 3
    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[1, 2, 3],
        pk_hi=[0, 0, 0],
        weights=[1, 1, 1],
        nulls=[0, 0, 0],
        columns=[[], [100, 200, 300]],
    )
    client.push(tid, tbl_schema, batch)

    # Delete pk=2 with weight=-1
    del_batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[2],
        pk_hi=[0],
        weights=[-1],
        nulls=[0],
        columns=[[], [200]],
    )
    client.push(tid, tbl_schema, del_batch)

    # Scan
    schema, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 2
    pks = sorted(result.pk_lo)
    assert pks == [1, 3]


def test_scan_empty_table(client):
    """Create table, scan immediately, verify 0 data rows but schema present."""
    tid, tbl_schema = _make_table(client)

    schema, result = client.scan(tid)
    assert schema is not None
    assert len(schema.columns) == 2
    # result may be None or empty
    if result is not None:
        assert len(result.pk_lo) == 0
