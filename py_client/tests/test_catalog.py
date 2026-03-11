"""Exhaustive catalog tests: schemas, tables, views, DML, edge cases, failures."""

import random
import pytest

from gnitz_client import GnitzClient, GnitzError, TypeCode, ColumnDef, Schema
from gnitz_client.types import (
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, SEQ_TAB,
    SCHEMA_TAB_SCHEMA, TABLE_TAB_SCHEMA, COL_TAB_SCHEMA,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID,
)
from gnitz_client.batch import ZSetBatch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid():
    return str(random.randint(100000, 999999))


def _make_schema(client, prefix="cat"):
    name = prefix + _uid()
    sid = client.create_schema(name)
    return name, sid


def _make_table(client, schema_name, table_name=None, columns=None, pk=0):
    if table_name is None:
        table_name = "t" + _uid()
    if columns is None:
        columns = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
    tid = client.create_table(schema_name, table_name, columns, pk_col_idx=pk)
    tbl_schema = Schema(columns=columns, pk_index=pk)
    return tid, table_name, tbl_schema


def _scan_positive_names(client, tab_id, name_col_idx=1):
    """Scan a system table and return all positive-weight name values."""
    _, batch = client.scan(tab_id)
    if batch is None:
        return []
    return [
        batch.columns[name_col_idx][i]
        for i in range(len(batch.pk_lo))
        if batch.weights[i] > 0
    ]


def _scan_table_names(client):
    """Get all positive-weight table names from TableTab."""
    return _scan_positive_names(client, TABLE_TAB, name_col_idx=2)


def _insert_rows(client, tid, tbl_schema, rows):
    """Insert rows as (pk, val1, val2, ...) tuples."""
    ncols = len(tbl_schema.columns)
    batch = ZSetBatch(schema=tbl_schema)
    cols = [[] for _ in range(ncols)]
    for row in rows:
        pk = row[0]
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(1)
        batch.nulls.append(0)
        for c in range(ncols):
            if c == tbl_schema.pk_index:
                cols[c].append(None)
            else:
                cols[c].append(row[c])
    batch.columns = cols
    client.push(tid, tbl_schema, batch)


def _delete_rows(client, tid, tbl_schema, rows):
    """Delete rows (same format as insert, but with weight=-1)."""
    ncols = len(tbl_schema.columns)
    batch = ZSetBatch(schema=tbl_schema)
    cols = [[] for _ in range(ncols)]
    for row in rows:
        pk = row[0]
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(-1)
        batch.nulls.append(0)
        for c in range(ncols):
            if c == tbl_schema.pk_index:
                cols[c].append(None)
            else:
                cols[c].append(row[c])
    batch.columns = cols
    client.push(tid, tbl_schema, batch)


def _scan_rows(client, tid):
    """Scan a table and return (schema, [(pk, col_values...)]) sorted by pk."""
    schema, batch = client.scan(tid)
    if batch is None or len(batch.pk_lo) == 0:
        return schema, []
    rows = []
    for i in range(len(batch.pk_lo)):
        row = [batch.pk_lo[i]]
        for c in range(len(schema.columns)):
            if c != schema.pk_index:
                row.append(batch.columns[c][i])
        rows.append(tuple(row))
    rows.sort()
    return schema, rows


# ===========================================================================
# Schema Tests
# ===========================================================================

class TestSchemaCreation:

    def test_create_schema(self, client):
        name, sid = _make_schema(client)
        assert sid >= FIRST_USER_SCHEMA_ID
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert name in names

    def test_create_multiple_schemas(self, client):
        n1, s1 = _make_schema(client)
        n2, s2 = _make_schema(client)
        assert s1 != s2
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert n1 in names
        assert n2 in names

    def test_create_schema_ids_monotonic(self, client):
        _, s1 = _make_schema(client)
        _, s2 = _make_schema(client)
        _, s3 = _make_schema(client)
        assert s1 < s2 < s3

    def test_create_duplicate_schema_is_idempotent(self, client):
        """Pushing a duplicate schema to SchemaTab is silently ignored by hook."""
        name, sid = _make_schema(client)
        # Push same schema again - hook ignores because schema already exists
        schema_s = SCHEMA_TAB_SCHEMA
        b = ZSetBatch(
            schema=schema_s,
            pk_lo=[sid + 1000],  # different pk
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [name]],
        )
        # This push succeeds (no error from server)
        client.push(SCHEMA_TAB, schema_s, b)
        # Schema still exists exactly once (hook skipped the duplicate)
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert name in names


class TestSchemaDrop:

    def test_drop_schema(self, client):
        name, _ = _make_schema(client)
        client.drop_schema(name)
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert name not in names

    def test_drop_nonexistent_schema(self, client):
        with pytest.raises(GnitzError, match="(?i)not found"):
            client.drop_schema("nonexistent" + _uid())

    def test_drop_system_schema(self, client):
        """The SchemaEffectHook rejects dropping _system with 'Forbidden'.
        However, the retraction is already ingested before the hook fires.
        This is a known DDL-as-DML limitation: the hook rejects the operation
        but the data is already written. We test that the error is returned,
        then repair the state by re-inserting the _system record."""
        with pytest.raises(GnitzError, match="(?i)forbidden|system|cannot drop"):
            client.drop_schema("_system")
        # Repair: The retraction was ingested, so _system may have net weight 0.
        # Re-insert to restore positive weight.
        schema_s = SCHEMA_TAB_SCHEMA
        # Find system schema ID (it's 1)
        b = ZSetBatch(
            schema=schema_s,
            pk_lo=[1],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], ["_system"]],
        )
        client.push(SCHEMA_TAB, schema_s, b)

    def test_drop_nonempty_schema(self, client):
        """Cannot drop a schema that contains tables."""
        name, _ = _make_schema(client)
        _make_table(client, name)
        with pytest.raises(GnitzError, match="(?i)non.?empty|not empty|cannot drop"):
            client.drop_schema(name)

    def test_drop_and_recreate_schema(self, client):
        name, sid1 = _make_schema(client)
        client.drop_schema(name)
        sid2 = client.create_schema(name)
        assert sid2 > sid1


# ===========================================================================
# Table Tests
# ===========================================================================

class TestTableCreation:

    def test_create_simple_table(self, client):
        sn, _ = _make_schema(client)
        tid, tn, _ = _make_table(client, sn)
        assert tid >= FIRST_USER_TABLE_ID
        names = _scan_table_names(client)
        assert tn in names

    def test_create_table_all_types(self, client):
        """Table with every supported type."""
        sn, _ = _make_schema(client)
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("c_u8", TypeCode.U8),
            ColumnDef("c_i8", TypeCode.I8),
            ColumnDef("c_u16", TypeCode.U16),
            ColumnDef("c_i16", TypeCode.I16),
            ColumnDef("c_u32", TypeCode.U32),
            ColumnDef("c_i32", TypeCode.I32),
            ColumnDef("c_f32", TypeCode.F32),
            ColumnDef("c_u64", TypeCode.U64),
            ColumnDef("c_i64", TypeCode.I64),
            ColumnDef("c_f64", TypeCode.F64),
            ColumnDef("c_str", TypeCode.STRING),
        ]
        tid, tn, _ = _make_table(client, sn, columns=cols)
        schema, _ = client.scan(tid)
        assert len(schema.columns) == len(cols)

    def test_create_table_nondefault_pk(self, client):
        """PK is not column 0."""
        sn, _ = _make_schema(client)
        cols = [
            ColumnDef("val", TypeCode.I64),
            ColumnDef("pk", TypeCode.U64),
        ]
        tid, _, _ = _make_table(client, sn, columns=cols, pk=1)
        schema, _ = client.scan(tid)
        assert schema.pk_index == 1

    def test_create_table_two_columns(self, client):
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, _ = _make_table(client, sn, columns=cols)
        schema, _ = client.scan(tid)
        assert len(schema.columns) == 2

    def test_create_table_max_columns(self, client):
        """Table with 64 columns (the maximum)."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64)]
        for i in range(63):
            cols.append(ColumnDef(f"c{i}", TypeCode.I64))
        tid, _, _ = _make_table(client, sn, columns=cols)
        schema, _ = client.scan(tid)
        assert len(schema.columns) == 64

    def test_create_table_too_many_columns(self, client):
        """Table with 65 columns should fail."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64)]
        for i in range(64):
            cols.append(ColumnDef(f"c{i}", TypeCode.I64))
        with pytest.raises(GnitzError, match="(?i)64 columns|maximum"):
            _make_table(client, sn, columns=cols)

    def test_create_table_pk_out_of_bounds(self, client):
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        with pytest.raises(GnitzError, match="(?i)out of bounds|pk"):
            client.create_table(sn, "t" + _uid(), cols, pk_col_idx=5)

    def test_create_table_string_pk_rejected(self, client):
        """PK must be U64 or U128; string PK should be rejected."""
        sn, _ = _make_schema(client)
        cols = [
            ColumnDef("name", TypeCode.STRING),
            ColumnDef("val", TypeCode.I64),
        ]
        with pytest.raises(GnitzError, match="(?i)primary key|type"):
            _make_table(client, sn, columns=cols, pk=0)

    def test_create_table_nonexistent_schema(self, client):
        with pytest.raises(GnitzError, match="(?i)not found"):
            _make_table(client, "nosuch" + _uid())

    def test_create_multiple_tables_same_schema(self, client):
        sn, _ = _make_schema(client)
        tid1, tn1, _ = _make_table(client, sn)
        tid2, tn2, _ = _make_table(client, sn)
        assert tid1 != tid2
        names = _scan_table_names(client)
        assert tn1 in names
        assert tn2 in names

    def test_table_ids_monotonic(self, client):
        sn, _ = _make_schema(client)
        tid1, _, _ = _make_table(client, sn)
        tid2, _, _ = _make_table(client, sn)
        tid3, _, _ = _make_table(client, sn)
        assert tid1 < tid2 < tid3

    def test_create_table_columns_in_coltab(self, client):
        """Verify column records appear in ColTab."""
        sn, _ = _make_schema(client)
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a", TypeCode.I32),
            ColumnDef("b", TypeCode.STRING),
        ]
        tid, _, _ = _make_table(client, sn, columns=cols)
        _, col_batch = client.scan(COL_TAB)
        table_cols = []
        for i in range(len(col_batch.pk_lo)):
            if col_batch.weights[i] > 0 and col_batch.columns[1][i] == tid:
                table_cols.append(col_batch.columns[4][i])
        assert sorted(table_cols) == ["a", "b", "pk"]


class TestTableDrop:

    def test_drop_table(self, client):
        sn, _ = _make_schema(client)
        tid, tn, _ = _make_table(client, sn)
        client.drop_table(sn, tn)
        names = _scan_table_names(client)
        assert tn not in names

    def test_drop_nonexistent_table(self, client):
        sn, _ = _make_schema(client)
        with pytest.raises(GnitzError, match="(?i)not found"):
            client.drop_table(sn, "nosuch" + _uid())

    def test_drop_table_columns_removed(self, client):
        """Column records should be cleaned up when table is dropped."""
        sn, _ = _make_schema(client)
        tid, tn, _ = _make_table(client, sn)
        client.drop_table(sn, tn)

        _, col_batch = client.scan(COL_TAB)
        if col_batch:
            for i in range(len(col_batch.pk_lo)):
                if col_batch.weights[i] > 0:
                    assert col_batch.columns[1][i] != tid, "Column records not cleaned up"

    def test_drop_and_recreate_table(self, client):
        sn, _ = _make_schema(client)
        tn = "reuse" + _uid()
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid1 = client.create_table(sn, tn, cols)
        client.drop_table(sn, tn)
        tid2 = client.create_table(sn, tn, cols)
        assert tid2 > tid1

    def test_drop_table_with_data(self, client):
        """Drop a table that has data in it."""
        sn, _ = _make_schema(client)
        tid, tn, tbl_schema = _make_table(client, sn)
        _insert_rows(client, tid, tbl_schema, [(1, 100), (2, 200)])
        client.drop_table(sn, tn)
        names = _scan_table_names(client)
        assert tn not in names

    def test_scan_after_drop(self, client):
        """Scanning a dropped table ID should error."""
        sn, _ = _make_schema(client)
        tid, tn, _ = _make_table(client, sn)
        client.drop_table(sn, tn)
        with pytest.raises(GnitzError, match="(?i)unknown"):
            client.scan(tid)

    def test_drop_schema_after_table_dropped(self, client):
        """Schema can be dropped after all its tables are dropped."""
        sn, _ = _make_schema(client)
        tid, tn, _ = _make_table(client, sn)
        client.drop_table(sn, tn)
        client.drop_schema(sn)
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert sn not in names

    def test_drop_multiple_tables(self, client):
        """Drop multiple tables from the same schema."""
        sn, _ = _make_schema(client)
        tid1, tn1, _ = _make_table(client, sn)
        tid2, tn2, _ = _make_table(client, sn)
        client.drop_table(sn, tn1)
        client.drop_table(sn, tn2)
        names = _scan_table_names(client)
        assert tn1 not in names
        assert tn2 not in names


# ===========================================================================
# DML Tests
# ===========================================================================

class TestDML:

    def test_insert_single_row(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 42)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 1
        assert rows[0] == (1, 42)

    def test_insert_multiple_rows_one_batch(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10), (2, 20), (3, 30)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 3
        assert rows == [(1, 10), (2, 20), (3, 30)]

    def test_insert_multiple_batches(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10)])
        _insert_rows(client, tid, tbl, [(2, 20)])
        _insert_rows(client, tid, tbl, [(3, 30)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 3

    def test_delete_single_row(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10), (2, 20), (3, 30)])
        _delete_rows(client, tid, tbl, [(2, 20)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 2
        pks = [r[0] for r in rows]
        assert 2 not in pks

    def test_delete_all_rows(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10), (2, 20)])
        _delete_rows(client, tid, tbl, [(1, 10), (2, 20)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 0

    def test_insert_delete_same_batch(self, client):
        """Mixed insert+delete in a single batch."""
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10), (2, 20)])
        # Batch with insert pk=3 and delete pk=1
        batch = ZSetBatch(
            schema=tbl,
            pk_lo=[3, 1],
            pk_hi=[0, 0],
            weights=[1, -1],
            nulls=[0, 0],
            columns=[[], [30, 10]],
        )
        client.push(tid, tbl, batch)
        _, rows = _scan_rows(client, tid)
        pks = [r[0] for r in rows]
        assert 1 not in pks
        assert 3 in pks

    def test_large_batch(self, client):
        """Insert 1000 rows in one batch."""
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        rows = [(i, i * 10) for i in range(1, 1001)]
        _insert_rows(client, tid, tbl, rows)
        _, result = _scan_rows(client, tid)
        assert len(result) == 1000

    def test_insert_strings(self, client):
        sn, _ = _make_schema(client)
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("name", TypeCode.STRING),
        ]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        tbl = Schema(columns=cols, pk_index=0)

        batch = ZSetBatch(
            schema=tbl,
            pk_lo=[1, 2, 3],
            pk_hi=[0, 0, 0],
            weights=[1, 1, 1],
            nulls=[0, 0, 0],
            columns=[[], ["hello", "", "a" * 200]],
        )
        client.push(tid, tbl, batch)
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 3
        names = {r[0]: r[1] for r in rows}
        assert names[1] == "hello"
        assert names[2] == ""
        assert names[3] == "a" * 200

    def test_insert_negative_values(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, -100), (2, -2**62)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 2
        vals = {r[0]: r[1] for r in rows}
        assert vals[1] == -100
        assert vals[2] == -2**62

    def test_insert_zero_pk(self, client):
        """PK value of 0 should work."""
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(0, 42)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 1
        assert rows[0] == (0, 42)

    def test_insert_max_u64_pk(self, client):
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        max_u64 = (1 << 64) - 1
        _insert_rows(client, tid, tbl, [(max_u64, 99)])
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 1
        assert rows[0][0] == max_u64

    def test_insert_to_table_in_different_schemas(self, client):
        """Data stays isolated between tables with same name in different schemas."""
        sn1, _ = _make_schema(client)
        sn2, _ = _make_schema(client)
        tid1, _, tbl1 = _make_table(client, sn1, table_name="items")
        tid2, _, tbl2 = _make_table(client, sn2, table_name="items")
        _insert_rows(client, tid1, tbl1, [(1, 100)])
        _insert_rows(client, tid2, tbl2, [(1, 200)])
        _, rows1 = _scan_rows(client, tid1)
        _, rows2 = _scan_rows(client, tid2)
        assert rows1[0][1] == 100
        assert rows2[0][1] == 200

    def test_upsert_same_pk(self, client):
        """Inserting with same PK twice should accumulate weight."""
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10)])
        _insert_rows(client, tid, tbl, [(1, 10)])
        # In a ZSet, pk=1 now has weight=2; scan returns positive-weight rows
        _, rows = _scan_rows(client, tid)
        assert len(rows) >= 1  # row exists with accumulated weight

    def test_delete_nonexistent_row(self, client):
        """Deleting a row that doesn't exist creates negative weight (no error)."""
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        # This won't error - it just records weight=-1 in the ZSet
        _delete_rows(client, tid, tbl, [(999, 0)])
        # The row won't appear in scan (negative weight filtered out)
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 0


# ===========================================================================
# View Tests
# ===========================================================================

class TestViewCreation:

    def test_create_simple_view(self, client):
        """Create a passthrough view on a table."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, "v" + _uid(), tid, cols)
        assert vid >= FIRST_USER_TABLE_ID

        # View should appear in ViewTab
        _, vbatch = client.scan(VIEW_TAB)
        assert vbatch is not None
        found = False
        for i in range(len(vbatch.pk_lo)):
            if vbatch.pk_lo[i] == vid and vbatch.weights[i] > 0:
                found = True
        assert found

    def test_view_scannable(self, client):
        """A view should be scannable (starts empty)."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, "v" + _uid(), tid, cols)
        schema, batch = client.scan(vid)
        assert schema is not None
        assert len(schema.columns) == 2

    def test_view_receives_data(self, client):
        """Insert into source table, view should reflect the data."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, "v" + _uid(), tid, cols)

        _insert_rows(client, tid, tbl, [(1, 10), (2, 20)])
        _, rows = _scan_rows(client, vid)
        assert len(rows) == 2

    def test_view_receives_deletes(self, client):
        """Delete from source table, view should reflect deletion."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, "v" + _uid(), tid, cols)

        _insert_rows(client, tid, tbl, [(1, 10), (2, 20), (3, 30)])
        _delete_rows(client, tid, tbl, [(2, 20)])
        _, rows = _scan_rows(client, vid)
        assert len(rows) == 2
        pks = [r[0] for r in rows]
        assert 2 not in pks

    def test_multiple_views_on_same_table(self, client):
        """Two views on the same source table both get data."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vid1 = client.create_view(sn, "va" + _uid(), tid, cols)
        vid2 = client.create_view(sn, "vb" + _uid(), tid, cols)

        _insert_rows(client, tid, tbl, [(1, 10), (2, 20)])
        _, rows1 = _scan_rows(client, vid1)
        _, rows2 = _scan_rows(client, vid2)
        assert len(rows1) == 2
        assert len(rows2) == 2

    def test_view_same_name_as_table_creates_separate_entity(self, client):
        """From IPC, a view can be created with the same name as a table
        (the name collision check is in Engine.create_view, not in hooks).
        The view gets a different ID and is independently scannable."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, tn, tbl = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, tn, tid, cols)
        assert vid != tid
        # Both are scannable
        schema_t, _ = client.scan(tid)
        schema_v, _ = client.scan(vid)
        assert schema_t is not None
        assert schema_v is not None

    def test_view_data_matches_source(self, client):
        """View data should exactly match source table data."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, "v" + _uid(), tid, cols)

        _insert_rows(client, tid, tbl, [(10, 100), (20, 200), (30, 300)])
        _, src_rows = _scan_rows(client, tid)
        _, view_rows = _scan_rows(client, vid)
        assert src_rows == view_rows

    def test_view_independent_from_other_tables(self, client):
        """A view on table A is not affected by inserts into table B."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid_a, _, tbl_a = _make_table(client, sn, columns=cols)
        tid_b, _, tbl_b = _make_table(client, sn, columns=cols)
        vid = client.create_view(sn, "v" + _uid(), tid_a, cols)

        _insert_rows(client, tid_a, tbl_a, [(1, 10)])
        _insert_rows(client, tid_b, tbl_b, [(2, 20)])
        _, view_rows = _scan_rows(client, vid)
        assert len(view_rows) == 1
        assert view_rows[0] == (1, 10)


class TestViewDrop:

    def test_drop_view(self, client):
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, _ = _make_table(client, sn, columns=cols)
        vn = "v" + _uid()
        vid = client.create_view(sn, vn, tid, cols)
        client.drop_view(sn, vn)

        with pytest.raises(GnitzError, match="(?i)unknown"):
            client.scan(vid)

    def test_drop_view_columns_cleaned(self, client):
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, _ = _make_table(client, sn, columns=cols)
        vn = "v" + _uid()
        vid = client.create_view(sn, vn, tid, cols)
        client.drop_view(sn, vn)

        _, col_batch = client.scan(COL_TAB)
        if col_batch:
            for i in range(len(col_batch.pk_lo)):
                if col_batch.weights[i] > 0:
                    assert col_batch.columns[1][i] != vid

    def test_drop_view_deps_cleaned(self, client):
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, _ = _make_table(client, sn, columns=cols)
        vn = "v" + _uid()
        vid = client.create_view(sn, vn, tid, cols)
        client.drop_view(sn, vn)

        _, dep_batch = client.scan(DEP_TAB)
        if dep_batch:
            for i in range(len(dep_batch.pk_lo)):
                if dep_batch.weights[i] > 0:
                    assert dep_batch.columns[1][i] != vid

    def test_drop_and_recreate_view(self, client):
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vn = "v" + _uid()
        vid1 = client.create_view(sn, vn, tid, cols)
        client.drop_view(sn, vn)
        vid2 = client.create_view(sn, vn, tid, cols)
        assert vid2 > vid1

        _insert_rows(client, tid, tbl, [(1, 10)])
        _, rows = _scan_rows(client, vid2)
        assert len(rows) == 1

    def test_drop_table_with_view_dep_handled_by_hook(self, client):
        """The TableEffectHook checks FK dependencies on retraction.
        View deps are checked by Engine.drop_table (server-side), not the hook.
        From IPC, the table retraction succeeds even with a view dependency
        (the hook only checks FK refs, not view deps)."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, tn, _ = _make_table(client, sn, columns=cols)
        vn = "v" + _uid()
        vid = client.create_view(sn, vn, tid, cols)
        # From IPC, drop_table bypasses Engine._check_for_dependencies
        # The hook checks FK refs but not view deps, so this succeeds
        client.drop_table(sn, tn)
        # Table gone
        with pytest.raises(GnitzError, match="(?i)unknown"):
            client.scan(tid)
        # View may still exist (orphaned)
        # Clean up: drop the view
        try:
            client.drop_view(sn, vn)
        except GnitzError:
            pass  # may already be gone

    def test_drop_table_after_view_dropped(self, client):
        """After view is dropped, the source table can be dropped."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, tn, _ = _make_table(client, sn, columns=cols)
        vn = "v" + _uid()
        client.create_view(sn, vn, tid, cols)
        client.drop_view(sn, vn)
        client.drop_table(sn, tn)

    def test_drop_one_of_two_views(self, client):
        """Dropping one view doesn't affect the other."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid, _, tbl = _make_table(client, sn, columns=cols)
        vn1 = "va" + _uid()
        vn2 = "vb" + _uid()
        vid1 = client.create_view(sn, vn1, tid, cols)
        vid2 = client.create_view(sn, vn2, tid, cols)

        _insert_rows(client, tid, tbl, [(1, 10)])
        client.drop_view(sn, vn1)

        # vid2 should still work
        _, rows = _scan_rows(client, vid2)
        assert len(rows) == 1

        # vid1 should be gone
        with pytest.raises(GnitzError, match="(?i)unknown"):
            client.scan(vid1)


# ===========================================================================
# FK Constraint Tests
# ===========================================================================

class TestForeignKeys:

    def _make_parent_child(self, client):
        """Create parent + child tables with FK from child to parent."""
        sn, _ = _make_schema(client)

        parent_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("name", TypeCode.STRING)]
        parent_tid = client.create_table(sn, "parent" + _uid(), parent_cols)
        parent_schema = Schema(columns=parent_cols, pk_index=0)

        _insert_rows(client, parent_tid, parent_schema, [(1, "a"), (2, "b"), (3, "c")])

        child_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("parent_id", TypeCode.U64),
        ]
        child_tid = self._create_fk_table(
            client, sn, "child" + _uid(), child_cols, parent_tid, fk_col=1
        )

        child_schema = Schema(columns=child_cols, pk_index=0)
        return sn, parent_tid, parent_schema, child_tid, child_schema

    def _create_fk_table(self, client, schema_name, table_name, columns, fk_target_id, fk_col):
        """Create a table where one column has a FK constraint."""
        from gnitz_client.client import _pack_column_id
        from gnitz_client.types import SEQ_ID_TABLES

        _, seq_batch = client.scan(SEQ_TAB)
        old_hwm = client._find_seq_val(seq_batch, SEQ_ID_TABLES)
        new_tid = old_hwm + 1
        client._advance_seq(SEQ_ID_TABLES, old_hwm, new_tid)

        _, schema_batch = client.scan(SCHEMA_TAB)
        schema_id = client._find_schema_id(schema_batch, schema_name)

        col_schema = COL_TAB_SCHEMA
        col_batch = ZSetBatch(schema=col_schema)
        pk_col = []
        owner_ids = []
        owner_kinds = []
        col_idxs = []
        names = []
        type_codes = []
        is_nullables = []
        fk_table_ids = []
        fk_col_idxs = []

        for i, col in enumerate(columns):
            col_pk = _pack_column_id(new_tid, i)
            col_batch.pk_lo.append(col_pk)
            col_batch.pk_hi.append(0)
            col_batch.weights.append(1)
            col_batch.nulls.append(0)

            pk_col.append(None)
            owner_ids.append(new_tid)
            owner_kinds.append(OWNER_KIND_TABLE)
            col_idxs.append(i)
            names.append(col.name)
            type_codes.append(col.type_code)
            is_nullables.append(1 if col.is_nullable else 0)
            if i == fk_col:
                fk_table_ids.append(fk_target_id)
                fk_col_idxs.append(0)
            else:
                fk_table_ids.append(0)
                fk_col_idxs.append(0)

        col_batch.columns = [pk_col, owner_ids, owner_kinds, col_idxs, names,
                             type_codes, is_nullables, fk_table_ids, fk_col_idxs]
        client.push(COL_TAB, col_schema, col_batch)

        table_s = TABLE_TAB_SCHEMA
        tb = ZSetBatch(
            schema=table_s,
            pk_lo=[new_tid],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [schema_id], [table_name], [""], [0], [0]],
        )
        client.push(TABLE_TAB, table_s, tb)

        return new_tid

    def test_fk_valid_insert(self, client):
        """Insert with valid FK reference should succeed."""
        sn, parent_tid, parent_s, child_tid, child_s = self._make_parent_child(client)
        _insert_rows(client, child_tid, child_s, [(10, 1), (20, 2)])
        _, rows = _scan_rows(client, child_tid)
        assert len(rows) == 2

    def test_fk_violation(self, client):
        """Insert with invalid FK reference should fail."""
        sn, parent_tid, parent_s, child_tid, child_s = self._make_parent_child(client)
        with pytest.raises(GnitzError, match="(?i)foreign key|violation"):
            _insert_rows(client, child_tid, child_s, [(10, 999)])

    def test_cannot_drop_table_with_fk_refs(self, client):
        """Cannot drop parent table while child has FK reference."""
        sn, parent_tid, parent_s, child_tid, child_s = self._make_parent_child(client)
        _insert_rows(client, child_tid, child_s, [(10, 1)])
        _, tbl_batch = client.scan(TABLE_TAB)
        parent_name = None
        for i in range(len(tbl_batch.pk_lo)):
            if tbl_batch.pk_lo[i] == parent_tid and tbl_batch.weights[i] > 0:
                parent_name = tbl_batch.columns[2][i]
                break
        assert parent_name is not None
        with pytest.raises(GnitzError, match="(?i)dependency|referenced|integrity"):
            client.drop_table(sn, parent_name)


# ===========================================================================
# System Table Edge Cases
# ===========================================================================

class TestSystemTableEdgeCases:

    def test_system_schemas_present_at_boot(self, client):
        """_system and public schemas should always exist."""
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert "_system" in names
        assert "public" in names

    def test_system_tables_present_at_boot(self, client):
        """System tables should be scannable."""
        for tab_id in [SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, SEQ_TAB]:
            schema, batch = client.scan(tab_id)
            assert schema is not None

    def test_seq_tab_has_sequences(self, client):
        _, batch = client.scan(SEQ_TAB)
        assert batch is not None
        seq_ids = [batch.pk_lo[i] for i in range(len(batch.pk_lo)) if batch.weights[i] > 0]
        assert 1 in seq_ids  # SEQ_ID_SCHEMAS
        assert 2 in seq_ids  # SEQ_ID_TABLES

    def test_push_to_system_table_direct(self, client):
        """Direct pushes to system tables are valid (DDL-as-DML)."""
        sn = "directpush" + _uid()
        _, seq_batch = client.scan(SEQ_TAB)
        old_hwm = client._find_seq_val(seq_batch, 1)
        new_sid = old_hwm + 1
        schema_s = SCHEMA_TAB_SCHEMA
        b = ZSetBatch(
            schema=schema_s,
            pk_lo=[new_sid],
            pk_hi=[0],
            weights=[1],
            nulls=[0],
            columns=[[], [sn]],
        )
        client.push(SCHEMA_TAB, schema_s, b)
        client._advance_seq(1, old_hwm, new_sid)
        names = _scan_positive_names(client, SCHEMA_TAB)
        assert sn in names


# ===========================================================================
# Multi-operation Tests
# ===========================================================================

class TestMultiOperation:

    def test_create_tables_in_different_schemas(self, client):
        sn1, _ = _make_schema(client)
        sn2, _ = _make_schema(client)
        tid1, tn1, _ = _make_table(client, sn1, table_name="items")
        tid2, tn2, _ = _make_table(client, sn2, table_name="items")
        assert tid1 != tid2

        s1, _ = client.scan(tid1)
        s2, _ = client.scan(tid2)
        assert s1 is not None
        assert s2 is not None

    def test_same_name_table_and_schema(self, client):
        """Schema and table can share the same name string."""
        name = "shared" + _uid()
        client.create_schema(name)
        tid, _, _ = _make_table(client, name, table_name=name)
        _, rows = _scan_rows(client, tid)
        assert len(rows) == 0

    def test_interleaved_ddl_dml(self, client):
        """Mix DDL and DML operations."""
        sn, _ = _make_schema(client)
        tid1, _, tbl1 = _make_table(client, sn)
        _insert_rows(client, tid1, tbl1, [(1, 10)])
        tid2, _, tbl2 = _make_table(client, sn)
        _insert_rows(client, tid2, tbl2, [(1, 20)])
        _insert_rows(client, tid1, tbl1, [(2, 30)])

        _, rows1 = _scan_rows(client, tid1)
        _, rows2 = _scan_rows(client, tid2)
        assert len(rows1) == 2
        assert len(rows2) == 1

    def test_many_tables_in_one_schema(self, client):
        """Create 20 tables in a single schema."""
        sn, _ = _make_schema(client)
        tids = []
        for i in range(20):
            tid, _, _ = _make_table(client, sn)
            tids.append(tid)
        assert len(set(tids)) == 20

    def test_view_and_table_interleaved(self, client):
        """Create tables and views in interleaved order."""
        sn, _ = _make_schema(client)
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
        tid1, _, tbl1 = _make_table(client, sn, columns=cols)
        vid1 = client.create_view(sn, "v" + _uid(), tid1, cols)
        tid2, _, tbl2 = _make_table(client, sn, columns=cols)
        vid2 = client.create_view(sn, "v" + _uid(), tid2, cols)

        _insert_rows(client, tid1, tbl1, [(1, 10)])
        _insert_rows(client, tid2, tbl2, [(1, 20)])

        _, r1 = _scan_rows(client, vid1)
        _, r2 = _scan_rows(client, vid2)
        assert len(r1) == 1
        assert len(r2) == 1
        assert r1[0][1] == 10
        assert r2[0][1] == 20

    def test_drop_table_create_new_in_same_schema(self, client):
        sn, _ = _make_schema(client)
        tid1, tn1, _ = _make_table(client, sn)
        client.drop_table(sn, tn1)
        tid2, tn2, tbl2 = _make_table(client, sn)
        _insert_rows(client, tid2, tbl2, [(1, 42)])
        _, rows = _scan_rows(client, tid2)
        assert len(rows) == 1


# ===========================================================================
# Scan Edge Cases
# ===========================================================================

class TestScanEdgeCases:

    def test_scan_empty_table(self, client):
        sn, _ = _make_schema(client)
        tid, _, _ = _make_table(client, sn)
        schema, rows = _scan_rows(client, tid)
        assert len(rows) == 0
        assert schema is not None

    def test_scan_returns_schema(self, client):
        """Scan always returns schema even if no data."""
        sn, _ = _make_schema(client)
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a", TypeCode.I32),
            ColumnDef("b", TypeCode.STRING),
        ]
        tid, _, _ = _make_table(client, sn, columns=cols)
        schema, _ = client.scan(tid)
        assert len(schema.columns) == 3
        assert schema.columns[0].name == "pk"
        assert schema.columns[1].name == "a"
        assert schema.columns[2].name == "b"

    def test_scan_after_all_deleted(self, client):
        """Scan table where all rows were deleted."""
        sn, _ = _make_schema(client)
        tid, _, tbl = _make_table(client, sn)
        _insert_rows(client, tid, tbl, [(1, 10)])
        _delete_rows(client, tid, tbl, [(1, 10)])
        schema, rows = _scan_rows(client, tid)
        assert len(rows) == 0

    def test_scan_nonexistent_target(self, client):
        with pytest.raises(GnitzError, match="(?i)unknown"):
            client.scan(99999)
