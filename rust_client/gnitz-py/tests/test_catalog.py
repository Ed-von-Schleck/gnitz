"""Catalog tests: DDL lifecycle, DML edge cases, view lifecycle.

Ported from py_client/tests/test_catalog.py — only the tests not already
covered by test_ddl.py, test_dml.py, and test_views.py.

Skipped from py_client: raw system-table push tests (SCHEMA_TAB, COL_TAB,
DEP_TAB), FK constraint tests (require allocate_table_id + raw COL_TAB push),
and ID-allocation sequence tests (allocate_table_id/schema_id not in gnitz-py
API).
"""

import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _scan_positive(client, tid):
    return [r for r in client.scan(tid) if r.weight > 0]


# ===========================================================================
# Schema DDL
# ===========================================================================

class TestSchemaDDL:

    def test_create_multiple_schemas_distinct_ids(self, client):
        n1, n2 = "s" + _uid(), "s" + _uid()
        s1 = client.create_schema(n1)
        s2 = client.create_schema(n2)
        assert s1 != s2
        client.drop_schema(n1)
        client.drop_schema(n2)

    def test_schema_ids_monotonic(self, client):
        names = ["s" + _uid() for _ in range(3)]
        ids = [client.create_schema(n) for n in names]
        assert ids[0] < ids[1] < ids[2]
        for n in names:
            client.drop_schema(n)

    def test_drop_nonempty_schema(self, client):
        """Dropping a non-empty schema raises GnitzError.

        Due to DDL-as-DML semantics the schema retraction may be committed
        before the hook fires, so the schema may already be gone from the
        catalog after the error is returned.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tn = "t" + _uid()
        client.create_table(sn, tn, cols)
        with pytest.raises(gnitz.GnitzError):
            client.drop_schema(sn)
        # Forgiving cleanup: schema may already be gone
        try:
            client.drop_table(sn, tn)
        except Exception:
            pass
        try:
            client.drop_schema(sn)
        except Exception:
            pass

    def test_drop_and_recreate_schema(self, client):
        """Recreated schema gets a strictly higher ID."""
        sn = "s" + _uid()
        s1 = client.create_schema(sn)
        client.drop_schema(sn)
        s2 = client.create_schema(sn)
        assert s2 > s1
        client.drop_schema(sn)


# ===========================================================================
# Table DDL
# ===========================================================================

class TestTableDDL:

    def test_create_table_all_types(self, client):
        """Create a table with every supported TypeCode."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [
            gnitz.ColumnDef("pk",    gnitz.TypeCode.U64,    primary_key=True),
            gnitz.ColumnDef("c_u8",  gnitz.TypeCode.U8),
            gnitz.ColumnDef("c_i8",  gnitz.TypeCode.I8),
            gnitz.ColumnDef("c_u16", gnitz.TypeCode.U16),
            gnitz.ColumnDef("c_i16", gnitz.TypeCode.I16),
            gnitz.ColumnDef("c_u32", gnitz.TypeCode.U32),
            gnitz.ColumnDef("c_i32", gnitz.TypeCode.I32),
            gnitz.ColumnDef("c_f32", gnitz.TypeCode.F32),
            gnitz.ColumnDef("c_u64", gnitz.TypeCode.U64),
            gnitz.ColumnDef("c_i64", gnitz.TypeCode.I64),
            gnitz.ColumnDef("c_f64", gnitz.TypeCode.F64),
            gnitz.ColumnDef("c_str", gnitz.TypeCode.STRING, is_nullable=True),
        ]
        tn = "t" + _uid()
        try:
            tid = client.create_table(sn, tn, cols)
            resolved_tid, schema = client.resolve_table_id(sn, tn)
            assert resolved_tid == tid
            assert len(schema.columns) == len(cols)
        finally:
            try:
                client.drop_table(sn, tn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_create_table_nondefault_pk(self, client):
        """PK column at index 1 (not column 0)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [
            gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            gnitz.ColumnDef("pk",  gnitz.TypeCode.U64, primary_key=True),
        ]
        tn = "t" + _uid()
        try:
            client.create_table(sn, tn, cols)
            _, schema = client.resolve_table_id(sn, tn)
            assert schema.pk_index == 1
        finally:
            try:
                client.drop_table(sn, tn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_create_table_max_columns(self, client):
        """64 columns (the server maximum) is accepted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        for i in range(63):
            cols.append(gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64))
        tn = "t" + _uid()
        try:
            client.create_table(sn, tn, cols)
            _, schema = client.resolve_table_id(sn, tn)
            assert len(schema.columns) == 64
        finally:
            try:
                client.drop_table(sn, tn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_create_table_too_many_columns(self, client):
        """65 columns exceeds the server limit and raises GnitzError."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        for i in range(64):
            cols.append(gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64))
        try:
            with pytest.raises(gnitz.GnitzError):
                client.create_table(sn, "t" + _uid(), cols)
        finally:
            client.drop_schema(sn)

    def test_create_table_string_pk_rejected(self, client):
        """STRING is not a valid primary key type."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [
            gnitz.ColumnDef("name", gnitz.TypeCode.STRING, primary_key=True),
            gnitz.ColumnDef("val",  gnitz.TypeCode.I64),
        ]
        try:
            with pytest.raises(gnitz.GnitzError):
                client.create_table(sn, "t" + _uid(), cols)
        finally:
            client.drop_schema(sn)

    def test_create_table_nonexistent_schema(self, client):
        """Creating a table in a nonexistent schema raises GnitzError."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        with pytest.raises(gnitz.GnitzError):
            client.create_table("nosuch_" + _uid(), "t", cols)

    def test_table_ids_monotonic(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        names = ["t" + _uid() for _ in range(3)]
        try:
            tids = [client.create_table(sn, n, cols) for n in names]
            assert tids[0] < tids[1] < tids[2]
        finally:
            for n in names:
                try:
                    client.drop_table(sn, n)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_drop_and_recreate_table(self, client):
        """Recreated table gets a strictly higher ID."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tn = "t" + _uid()
        try:
            tid1 = client.create_table(sn, tn, cols)
            client.drop_table(sn, tn)
            tid2 = client.create_table(sn, tn, cols)
            assert tid2 > tid1
            client.drop_table(sn, tn)
        finally:
            client.drop_schema(sn)

    def test_drop_table_with_data(self, client):
        """Dropping a table that contains data does not raise an error."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tn = "t" + _uid()
        try:
            tid = client.create_table(sn, tn, cols)
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10)
            batch.append(pk=2, val=20)
            client.push(tid, schema, batch)
            client.drop_table(sn, tn)  # must not raise
        finally:
            client.drop_schema(sn)

    def test_scan_after_drop(self, client):
        """Scanning a dropped table raises GnitzError."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tn = "t" + _uid()
        try:
            tid = client.create_table(sn, tn, cols)
            client.drop_table(sn, tn)
            with pytest.raises(gnitz.GnitzError):
                client.scan(tid)
        finally:
            client.drop_schema(sn)

    def test_drop_schema_after_table_cleared(self, client):
        """Schema can be dropped once all its tables are removed."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tn = "t" + _uid()
        client.create_table(sn, tn, cols)
        client.drop_table(sn, tn)
        client.drop_schema(sn)  # must not raise


# ===========================================================================
# DML Edge Cases
# ===========================================================================

class TestDMLEdgeCases:

    def _setup(self, client, sn):
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tn = "t" + _uid()
        tid = client.create_table(sn, tn, cols)
        return tid, tn, schema

    def test_insert_zero_pk(self, client):
        """PK value of 0 is valid and round-trips correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=0, val=42)
            client.push(tid, schema, batch)
            rows = _scan_positive(client, tid)
            assert len(rows) == 1
            assert rows[0].pk == 0
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_insert_max_u64_pk(self, client):
        """PK value of 2^64 - 1 round-trips correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        max_u64 = (1 << 64) - 1
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=max_u64, val=99)
            client.push(tid, schema, batch)
            rows = _scan_positive(client, tid)
            assert len(rows) == 1
            assert rows[0].pk == max_u64
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_insert_negative_values(self, client):
        """Negative I64 payload values round-trip correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=-100)
            batch.append(pk=2, val=-(2**62))
            client.push(tid, schema, batch)
            vals = {r.pk: r.val for r in _scan_positive(client, tid)}
            assert vals[1] == -100
            assert vals[2] == -(2**62)
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_isolated_tables_in_different_schemas(self, client):
        """Same table name in two schemas keeps data isolated."""
        sn1, sn2 = "s" + _uid(), "s" + _uid()
        client.create_schema(sn1)
        client.create_schema(sn2)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        try:
            tid1 = client.create_table(sn1, "items", cols)
            tid2 = client.create_table(sn2, "items", cols)
            b1 = gnitz.ZSetBatch(schema)
            b1.append(pk=1, val=100)
            client.push(tid1, schema, b1)
            b2 = gnitz.ZSetBatch(schema)
            b2.append(pk=1, val=200)
            client.push(tid2, schema, b2)
            rows1 = _scan_positive(client, tid1)
            rows2 = _scan_positive(client, tid2)
            assert rows1[0].val == 100
            assert rows2[0].val == 200
        finally:
            for sn in (sn1, sn2):
                try:
                    client.drop_table(sn, "items")
                except Exception:
                    pass
                client.drop_schema(sn)

    def test_large_batch(self, client):
        """1000 rows pushed in a single batch all appear in scan."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, schema = self._setup(client, sn)
        try:
            batch = gnitz.ZSetBatch(schema)
            for i in range(1, 1001):
                batch.append(pk=i, val=i * 10)
            client.push(tid, schema, batch)
            rows = _scan_positive(client, tid)
            assert len(rows) == 1000
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)


# ===========================================================================
# View Lifecycle
# ===========================================================================

class TestViewLifecycle:

    def _setup(self, client, sn):
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tn = "t" + _uid()
        tid = client.create_table(sn, tn, cols)
        return tid, tn, cols, gnitz.Schema(cols)

    def test_view_starts_empty(self, client):
        """Newly created passthrough view is scannable with no rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn = "v" + _uid()
        try:
            vid = client.create_view(sn, vn, tid, schema)
            assert len(_scan_positive(client, vid)) == 0
        finally:
            try:
                client.drop_view(sn, vn)
            except Exception:
                pass
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_view_receives_deletes(self, client):
        """Retraction in source table propagates through passthrough view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn = "v" + _uid()
        try:
            vid = client.create_view(sn, vn, tid, schema)
            ins = gnitz.ZSetBatch(schema)
            for i in range(1, 4):
                ins.append(pk=i, val=i * 10)
            client.push(tid, schema, ins)
            ret = gnitz.ZSetBatch(schema)
            ret.append(pk=2, val=20, weight=-1)
            client.push(tid, schema, ret)
            rows = _scan_positive(client, vid)
            pks = {r.pk for r in rows}
            assert len(rows) == 2
            assert 2 not in pks
        finally:
            try:
                client.drop_view(sn, vn)
            except Exception:
                pass
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_multiple_views_on_same_table(self, client):
        """Two passthrough views on the same source both receive all inserts."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn1, vn2 = "va" + _uid(), "vb" + _uid()
        try:
            vid1 = client.create_view(sn, vn1, tid, schema)
            vid2 = client.create_view(sn, vn2, tid, schema)
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10)
            batch.append(pk=2, val=20)
            client.push(tid, schema, batch)
            assert len(_scan_positive(client, vid1)) == 2
            assert len(_scan_positive(client, vid2)) == 2
        finally:
            for vn in (vn1, vn2):
                try:
                    client.drop_view(sn, vn)
                except Exception:
                    pass
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_view_data_matches_source(self, client):
        """View contains exactly the same rows as its source table."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn = "v" + _uid()
        try:
            vid = client.create_view(sn, vn, tid, schema)
            batch = gnitz.ZSetBatch(schema)
            for pk, val in [(10, 100), (20, 200), (30, 300)]:
                batch.append(pk=pk, val=val)
            client.push(tid, schema, batch)
            src  = sorted((r.pk, r.val) for r in _scan_positive(client, tid))
            view = sorted((r.pk, r.val) for r in _scan_positive(client, vid))
            assert src == view
        finally:
            try:
                client.drop_view(sn, vn)
            except Exception:
                pass
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_view_independent_from_other_tables(self, client):
        """A view on table A is not affected by pushes to table B."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tn_a, tn_b = "ta" + _uid(), "tb" + _uid()
        tid_a = client.create_table(sn, tn_a, cols)
        tid_b = client.create_table(sn, tn_b, cols)
        vn = "v" + _uid()
        try:
            vid = client.create_view(sn, vn, tid_a, schema)
            ba = gnitz.ZSetBatch(schema)
            ba.append(pk=1, val=10)
            client.push(tid_a, schema, ba)
            bb = gnitz.ZSetBatch(schema)
            bb.append(pk=2, val=20)
            client.push(tid_b, schema, bb)
            rows = _scan_positive(client, vid)
            assert len(rows) == 1
            assert rows[0].pk == 1
        finally:
            try:
                client.drop_view(sn, vn)
            except Exception:
                pass
            for tn in (tn_a, tn_b):
                try:
                    client.drop_table(sn, tn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_drop_view_makes_scan_fail(self, client):
        """Scanning a dropped view raises GnitzError."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn = "v" + _uid()
        vid = client.create_view(sn, vn, tid, schema)
        client.drop_view(sn, vn)
        try:
            with pytest.raises(gnitz.GnitzError):
                client.scan(vid)
        finally:
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_drop_and_recreate_view(self, client):
        """Recreated view gets a higher ID and receives data from source."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn = "v" + _uid()
        try:
            vid1 = client.create_view(sn, vn, tid, schema)
            client.drop_view(sn, vn)
            vid2 = client.create_view(sn, vn, tid, schema)
            assert vid2 > vid1
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10)
            client.push(tid, schema, batch)
            assert len(_scan_positive(client, vid2)) == 1
        finally:
            try:
                client.drop_view(sn, vn)
            except Exception:
                pass
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_drop_one_of_two_views(self, client):
        """Dropping one view leaves the other intact and working."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn1, vn2 = "va" + _uid(), "vb" + _uid()
        try:
            vid1 = client.create_view(sn, vn1, tid, schema)
            vid2 = client.create_view(sn, vn2, tid, schema)
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, val=10)
            client.push(tid, schema, batch)
            client.drop_view(sn, vn1)
            assert len(_scan_positive(client, vid2)) == 1
            with pytest.raises(gnitz.GnitzError):
                client.scan(vid1)
        finally:
            try:
                client.drop_view(sn, vn2)
            except Exception:
                pass
            client.drop_table(sn, tn)
            client.drop_schema(sn)

    def test_drop_table_after_view_dropped(self, client):
        """Dropping a view and then its source table both succeed."""
        sn = "s" + _uid()
        client.create_schema(sn)
        tid, tn, cols, schema = self._setup(client, sn)
        vn = "v" + _uid()
        try:
            client.create_view(sn, vn, tid, schema)
            client.drop_view(sn, vn)
            client.drop_table(sn, tn)
        finally:
            client.drop_schema(sn)
