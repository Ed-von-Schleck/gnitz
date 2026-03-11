"""Test DDL operations: create schema, create table, scan system tables."""

from gnitz_client import GnitzClient, TypeCode, ColumnDef
from gnitz_client.types import SCHEMA_TAB, COL_TAB


def test_create_schema(client):
    """Create a schema and verify it appears in SchemaTab scan."""
    sid = client.create_schema("testapp")
    assert sid > 0

    # Scan SchemaTab to verify
    schema, batch = client.scan(SCHEMA_TAB)
    assert batch is not None
    names = [batch.columns[1][i] for i in range(len(batch.pk_lo)) if batch.weights[i] > 0]
    assert "testapp" in names


def test_create_table(client):
    """Create a table with columns and verify via system table scans."""
    # Ensure schema exists
    client.create_schema("testapp2")

    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("value", TypeCode.I64),
        ColumnDef("label", TypeCode.STRING),
    ]
    tid = client.create_table("testapp2", "items", columns, pk_col_idx=0)
    assert tid > 0

    # Scan ColTab to verify columns exist for this table
    schema, batch = client.scan(COL_TAB)
    assert batch is not None
    # Find columns belonging to our table
    table_cols = []
    for i in range(len(batch.pk_lo)):
        if batch.weights[i] > 0 and batch.columns[1][i] == tid:
            table_cols.append(batch.columns[4][i])  # name column
    assert len(table_cols) == 3
    assert "pk" in table_cols
    assert "value" in table_cols
    assert "label" in table_cols

    # Scan empty table - should get schema but no data
    tbl_schema, tbl_batch = client.scan(tid)
    assert tbl_schema is not None
    assert len(tbl_schema.columns) == 3


def test_create_table_long_name(client):
    """Create a table with a name > 12 chars (German String heap path)."""
    client.create_schema("longname_schema")
    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("value", TypeCode.I64),
    ]
    # 13-char name — previously crashed the server (string > SHORT_STRING_THRESHOLD)
    tid = client.create_table("longname_schema", "long_name_table", columns)
    assert tid > 0

    # 20-char name
    tid2 = client.create_table("longname_schema", "very_long_table_name", columns)
    assert tid2 > 0

    # Verify tables are scannable
    schema, batch = client.scan(tid)
    assert schema is not None
    schema2, batch2 = client.scan(tid2)
    assert schema2 is not None


def test_scan_system_tables(client):
    """Scan SchemaTab, verify at least _system and public schemas exist."""
    schema, batch = client.scan(SCHEMA_TAB)
    assert batch is not None
    names = [batch.columns[1][i] for i in range(len(batch.pk_lo)) if batch.weights[i] > 0]
    assert "_system" in names
    assert "public" in names
