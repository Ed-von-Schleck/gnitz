"""Test string edge cases: empty, short, long, NULL."""

from gnitz_client import GnitzClient, TypeCode, ColumnDef
from gnitz_client.types import Schema
from gnitz_client.batch import ZSetBatch


def test_string_edge_cases(client):
    """Insert rows with various string values and verify round-trip."""
    import random
    suffix = random.randint(1000, 99999)
    schema_name = f"str_{suffix}"
    client.create_schema(schema_name)

    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("label", TypeCode.STRING, is_nullable=True),
    ]
    tid = client.create_table(schema_name, f"t_{suffix}", columns, pk_col_idx=0)
    tbl_schema = Schema(columns=columns, pk_index=0)

    # Test values: empty string, short, 12-byte boundary, long, NULL
    test_strings = ["", "short", "exactly12byt", "this is a longer string that exceeds twelve bytes", None]

    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[1, 2, 3, 4, 5],
        pk_hi=[0, 0, 0, 0, 0],
        weights=[1, 1, 1, 1, 1],
        nulls=[0, 0, 0, 0, 1 << 0],  # null bitmap: payload col 0 is null for row 4 (pk=5)
        columns=[[], test_strings],
    )
    client.push(tid, tbl_schema, batch)

    # Scan and verify
    schema, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 5

    # Build pk -> label map
    rows = {result.pk_lo[i]: result.columns[1][i] for i in range(len(result.pk_lo))}
    assert rows[1] == ""
    assert rows[2] == "short"
    assert rows[3] == "exactly12byt"
    assert rows[4] == "this is a longer string that exceeds twelve bytes"
    assert rows[5] is None
