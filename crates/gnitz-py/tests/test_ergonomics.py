"""
test_ergonomics.py — Full coverage for the Python ergonomics API.

Section 1: Offline (no server)
    TestTypeCode, TestColumnDef, TestSchema, TestStruct,
    TestRow, TestZSetBatchErrors, TestScanResultType

Section 2: Online (server required, uses `client` fixture from conftest.py)
    TestZSetBatchExtend, TestScanResultMethods, TestRowAccess, TestStructOnline
"""
import random
import typing
import pytest

import gnitz
from gnitz import (TypeCode, ColumnDef, Schema, Row, ScanResult, ZSetBatch,
                   Struct, field, U64, I64, F64, STRING, U128)


def _uid():
    return str(random.randint(100000, 999999))


# ---------------------------------------------------------------------------
# Section 1 — Offline (no server, no fixtures)
# ---------------------------------------------------------------------------

class TestTypeCode:

    def test_repr(self):
        assert repr(TypeCode.U64) == "<TypeCode.U64: 8>"

    def test_int_value(self):
        assert TypeCode.U64 == 8
        assert int(TypeCode.U64) == 8

    def test_is_int(self):
        assert isinstance(TypeCode.U64, int)

    def test_construct_from_int(self):
        assert TypeCode(8) is TypeCode.U64

    def test_membership(self):
        assert TypeCode.STRING in TypeCode

    def test_all_values_present(self):
        assert {tc.name for tc in TypeCode} == {
            "U8", "I8", "U16", "I16", "U32", "I32", "F32",
            "U64", "I64", "F64", "STRING", "U128", "UUID", "BLOB",
        }

    def test_blob_constant(self):
        assert TypeCode.BLOB == 14


class TestColumnDef:

    def test_int_type_code_coerced(self):
        col = ColumnDef("x", 8)
        assert col.type_code == TypeCode.U64

    def test_typecode_accepted(self):
        col = ColumnDef("x", TypeCode.I64)
        assert col.type_code == TypeCode.I64

    def test_defaults(self):
        col = ColumnDef("x", TypeCode.U64)
        assert col.is_nullable is False
        assert col.primary_key is False

    def test_repr_contains_name(self):
        assert "x" in repr(ColumnDef("x", TypeCode.U64))


class TestSchema:

    def test_pk_inferred_from_flag(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.I64, primary_key=True)]
        assert Schema(cols).pk_index == 1

    def test_pk_defaults_to_zero(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.I64)]
        assert Schema(cols).pk_index == 0

    def test_multiple_pk_infers_compound(self):
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.I64, primary_key=True)]
        s = Schema(cols)
        assert s.pk_indices == [0, 1]
        # Single-PK convenience getter raises on compound schemas.
        with pytest.raises(ValueError):
            _ = s.pk_index

    def test_explicit_pk_index_overrides(self):
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.I64),
                ColumnDef("c", TypeCode.I64)]
        assert Schema(cols, pk_index=2).pk_index == 2

    def test_columns_stored(self):
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.I64)]
        s = Schema(cols)
        assert len(s.columns) == 2
        assert s.columns[0].name == "a"
        assert s.columns[1].name == "b"

    # --- compound PK construction ------------------------------------------

    def test_pk_indices_explicit(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.U32),
                ColumnDef("v", TypeCode.I64)]
        assert Schema(cols, pk_indices=[0, 1]).pk_indices == [0, 1]

    def test_pk_indices_order_is_preserved(self):
        """pk_indices defines sort order — not just set membership."""
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.U32),
                ColumnDef("v", TypeCode.I64)]
        s = Schema(cols, pk_indices=[1, 0])
        assert s.pk_indices == [1, 0]

    def test_single_pk_returns_one_element_list(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.I64)]
        s = Schema(cols, pk_index=0)
        assert s.pk_indices == [0]
        assert s.pk_index == 0

    def test_pk_index_and_pk_indices_conflict(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.I64)]
        with pytest.raises(ValueError):
            Schema(cols, pk_index=0, pk_indices=[0])

    # --- validation errors -------------------------------------------------

    def test_pk_indices_out_of_bounds(self):
        cols = [ColumnDef("a", TypeCode.U64)]
        with pytest.raises(ValueError, match="out of range"):
            Schema(cols, pk_indices=[99])

    def test_pk_indices_duplicate(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.U32)]
        with pytest.raises(ValueError, match="duplicate"):
            Schema(cols, pk_indices=[0, 0])

    def test_pk_indices_empty(self):
        cols = [ColumnDef("a", TypeCode.U64)]
        with pytest.raises(ValueError, match="empty"):
            Schema(cols, pk_indices=[])

    def test_string_pk_rejected(self):
        cols = [ColumnDef("s", TypeCode.STRING, primary_key=True),
                ColumnDef("v", TypeCode.I64)]
        with pytest.raises(ValueError, match="cannot be a PK"):
            Schema(cols)

    def test_nullable_pk_rejected(self):
        cols = [ColumnDef("pk", TypeCode.U64, is_nullable=True, primary_key=True),
                ColumnDef("v",  TypeCode.I64)]
        with pytest.raises(ValueError, match="nullable"):
            Schema(cols)

    def test_max_pk_columns_exceeded(self):
        # The FFI PK cap is PK_LIST_MAX_COLS (4) — the persisted PK-list codec
        # width — so asking for 5 PK columns must be rejected.
        cols = [ColumnDef(f"c{i}", TypeCode.U64) for i in range(5)]
        with pytest.raises(ValueError, match="PK_LIST_MAX_COLS"):
            Schema(cols, pk_indices=list(range(5)))

    def test_max_arity_wide_pk_accepted(self):
        # The widest FFI PK is PK_LIST_MAX_COLS (4) columns; 4 × U128 = 64 bytes,
        # within MAX_PK_BYTES (80), so the max-arity wide PK round-trips through
        # validation. The PK-stride ceiling is unreachable from this surface: the
        # 4-column cap bounds a U128 PK at 64 bytes, below the 80-byte limit.
        cols_ok = [ColumnDef(f"c{i}", TypeCode.U128) for i in range(4)]
        Schema(cols_ok, pk_indices=list(range(4)))


class TestRow:
    """Row objects are constructed directly — no server required."""

    _FIELDS = ("pk", "val", "label")
    _VALUES = (42, 100, "hello")

    def _row(self, weight=2):
        return Row(self._FIELDS, self._VALUES, weight=weight)

    def test_attr_access_each_field(self):
        row = self._row()
        assert row.pk    == 42
        assert row.val   == 100
        assert row.label == "hello"

    def test_weight_property(self):
        assert self._row(weight=2).weight == 2

    def test_getitem_by_str_key(self):
        row = self._row()
        assert row["pk"]    == 42
        assert row["label"] == "hello"

    def test_getitem_by_int_index(self):
        row = self._row()
        assert row[0] == 42
        assert row[2] == "hello"

    def test_getitem_negative_index(self):
        assert self._row()[-1] == "hello"

    def test_iter_yields_values(self):
        assert list(self._row()) == [42, 100, "hello"]

    def test_len(self):
        assert len(self._row()) == 3

    def test_asdict(self):
        assert self._row()._asdict() == {"pk": 42, "val": 100, "label": "hello"}

    def test_tuple(self):
        assert self._row()._tuple() == (42, 100, "hello")

    def test_repr_has_field_and_value(self):
        r = repr(self._row())
        assert "pk" in r
        assert "42" in r

    def test_repr_has_weight(self):
        assert "weight=2" in repr(self._row(weight=2))

    def test_eq_same_values_ignores_weight(self):
        # __eq__ compares _values only; weight is irrelevant
        assert Row(self._FIELDS, self._VALUES, weight=1) == \
               Row(self._FIELDS, self._VALUES, weight=99)

    def test_eq_different_values(self):
        assert Row(self._FIELDS, (42, 99, "hello"), 1) != \
               Row(self._FIELDS, (42, 100, "hello"), 1)

    def test_missing_attr_raises(self):
        with pytest.raises(AttributeError):
            _ = self._row().nonexistent

    def test_missing_key_raises(self):
        with pytest.raises(KeyError):
            _ = self._row()["nonexistent"]

    def test_hashable_hash_equals_tuple_hash(self):
        row = self._row()
        assert hash(row) == hash(self._VALUES)

    def test_hashable_usable_in_set(self):
        row = self._row()
        s = {row}
        assert row in s

    def test_hashable_usable_as_dict_key(self):
        row = self._row()
        d = {row: "value"}
        assert d[row] == "value"


class TestZSetBatchErrors:

    def _schema(self, nullable_val=False):
        return Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64, is_nullable=nullable_val),
        ])

    def test_missing_pk_raises(self):
        batch = ZSetBatch(self._schema())
        with pytest.raises(ValueError, match="pk"):
            batch.append(val=10)

    def test_nonnullable_none_raises(self):
        batch = ZSetBatch(self._schema(nullable_val=False))
        with pytest.raises(ValueError, match="val"):
            batch.append(pk=1, val=None)

    def test_nullable_none_ok(self):
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("s",  TypeCode.STRING, is_nullable=True),
        ])
        batch = ZSetBatch(schema)
        batch.append(pk=1, s=None)   # must not raise
        assert len(batch) == 1

    def test_append_returns_self(self):
        batch = ZSetBatch(self._schema())
        assert batch.append(pk=1, val=5) is batch

    def test_extend_returns_self(self):
        batch = ZSetBatch(self._schema())
        assert batch.extend([{"pk": 1, "val": 5}]) is batch

    def test_partial_row_rolled_back_on_error(self):
        """A type error on a later column must not leave a half-written row.

        With three columns the failure happens on the second payload column;
        the batch must come back to length 0 (PK already appended, then
        rolled back) and remain reusable.
        """
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("a",  TypeCode.I64),
            ColumnDef("b",  TypeCode.I64),
        ])
        batch = ZSetBatch(schema)
        with pytest.raises(Exception):
            batch.append(pk=1, a=10, b="not-an-int")
        assert len(batch) == 0
        # Reusing the batch with valid values still works.
        batch.append(pk=1, a=10, b=20)
        assert len(batch) == 1

    def test_pk_none_raises_with_descriptive_error(self):
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("v",  TypeCode.I64),
        ])
        batch = ZSetBatch(schema)
        with pytest.raises(ValueError, match="pk"):
            batch.append(pk=None, v=1)
        assert len(batch) == 0


class TestCompoundPkSchema:
    """Offline tests for compound-PK Schema + ZSetBatch construction.

    These exercise the binding layer without needing the server: the batch
    can be built, PK bytes can be inspected via `batch.pks`, and payload
    values round-trip through the in-memory columns.
    """

    def _schema(self):
        return Schema([
            ColumnDef("a", TypeCode.U64),
            ColumnDef("b", TypeCode.U32),
            ColumnDef("v", TypeCode.I64),
        ], pk_indices=[0, 1])

    def test_append_compound_pk_emits_packed_bytes(self):
        batch = ZSetBatch(self._schema())
        batch.append(a=7, b=9, v=100)
        # Single-PK schemas keep returning ints; compound schemas return
        # raw packed PK bytes (LE-encoded a || LE-encoded b).
        pks = batch.pks
        assert len(pks) == 1
        assert isinstance(pks[0], bytes)
        expected = (7).to_bytes(8, "little") + (9).to_bytes(4, "little")
        assert pks[0] == expected

    def test_compound_pk_pk_none_rejected(self):
        batch = ZSetBatch(self._schema())
        with pytest.raises(ValueError):
            batch.append(a=None, b=9, v=1)
        assert len(batch) == 0

    def test_compound_pk_missing_pk_col_rejected(self):
        batch = ZSetBatch(self._schema())
        with pytest.raises(ValueError, match="missing"):
            batch.append(b=9, v=1)
        assert len(batch) == 0


class TestUuidAndBlobRoundtrip:
    """Offline tests for UUID-from-string / uuid.UUID acceptance and Blob
    payload columns. The values are stored in batch.columns, so we can
    inspect them directly without a server."""

    def test_uuid_pk_from_hex_string(self):
        schema = Schema([
            ColumnDef("id", TypeCode.UUID, primary_key=True),
            ColumnDef("v",  TypeCode.I64),
        ])
        batch = ZSetBatch(schema)
        batch.append(id="12345678-1234-5678-1234-567812345678", v=1)
        assert len(batch) == 1

    def test_uuid_pk_from_uuid_object(self):
        import uuid as _uuid
        schema = Schema([
            ColumnDef("id", TypeCode.UUID, primary_key=True),
            ColumnDef("v",  TypeCode.I64),
        ])
        batch = ZSetBatch(schema)
        batch.append(id=_uuid.UUID("12345678-1234-5678-1234-567812345678"), v=1)
        assert len(batch) == 1

    def test_uuid_payload_column_from_uuid_object(self):
        import uuid as _uuid
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("id", TypeCode.UUID),
        ])
        batch = ZSetBatch(schema)
        batch.append(pk=1, id=_uuid.UUID("12345678-1234-5678-1234-567812345678"))
        assert len(batch) == 1
        # columns[1] should be the U128 list — non-empty after one append.
        cols = batch.columns
        assert len(cols[1]) == 1

    def test_uuid_invalid_string_raises(self):
        schema = Schema([
            ColumnDef("id", TypeCode.UUID, primary_key=True),
            ColumnDef("v",  TypeCode.I64),
        ])
        batch = ZSetBatch(schema)
        with pytest.raises(ValueError):
            batch.append(id="not-a-uuid-string", v=1)

    def test_blob_payload_roundtrip(self):
        """Blob payload columns must not crash the interpreter (previously
        hit `unreachable!` in write_fixed_le)."""
        # BLOB type code is 14 on the wire; ColumnDef accepts a raw int.
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("payload", 14),
        ])
        batch = ZSetBatch(schema)
        batch.append(pk=1, payload=b"hello\x00world")
        assert len(batch) == 1
        cols = batch.columns
        assert cols[1] == [b"hello\x00world"]


class TestScanResultType:
    """gnitz.ScanResult must be the native Rust type (not the old Python shim)."""

    def test_is_native_type(self):
        import gnitz._native as _native
        assert gnitz.ScanResult is _native.ScanResult


# ---------------------------------------------------------------------------
# Section 2 — Online (need `client` fixture)
# ---------------------------------------------------------------------------

def _make_table(client):
    """Create a (pk U64 PK, val I64) table. Returns (sn, tid, schema)."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64)]
    schema = Schema(cols)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def _push_rows(client, tid, schema, pk_val_pairs, weight=1):
    batch = ZSetBatch(schema)
    for pk, val in pk_val_pairs:
        batch.append(pk=pk, val=val, weight=weight)
    client.push(tid, batch)


class TestZSetBatchExtend:

    def test_extend_list_of_dicts(self, client):
        sn, tid, schema = _make_table(client)
        batch = ZSetBatch(schema)
        batch.extend([{"pk": 1, "val": 10}, {"pk": 2, "val": 20}])
        client.push(tid, batch)
        result = client.scan(tid)
        assert len(result) == 2
        assert {row.pk: row.val for row in result} == {1: 10, 2: 20}
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_extend_per_row_weight(self, client):
        sn, tid, schema = _make_table(client)
        batch = ZSetBatch(schema)
        batch.extend([{"pk": 1, "val": 10, "_weight": 3}])
        client.push(tid, batch)
        row = client.scan(tid).first()
        assert row is not None
        assert row.pk == 1
        assert row.weight == 3
        client.drop_table(sn, "t")
        client.drop_schema(sn)


class TestScanResultMethods:

    def test_all_returns_list(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        rows = client.scan(tid).all()
        assert isinstance(rows, list)
        assert len(rows) == 3
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_all_values_correct(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        data = {r.pk: r.val for r in client.scan(tid).all()}
        assert data == {1: 10, 2: 20, 3: 30}
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_bool_true(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10)])
        assert bool(client.scan(tid)) is True
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_bool_false_empty(self, client):
        sn, tid, schema = _make_table(client)
        assert bool(client.scan(tid)) is False
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_first_is_a_row(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        row = client.scan(tid).first()
        assert row is not None
        assert isinstance(row, Row)
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_one_raises_on_many_rows(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        with pytest.raises(ValueError):
            client.scan(tid).one()
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_one_on_single_row(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(7, 42)])
        row = client.scan(tid).one()
        assert row.pk == 7
        assert row.val == 42
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_one_or_none_raises_on_many(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        with pytest.raises(ValueError):
            client.scan(tid).one_or_none()
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_one_or_none_on_one(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(5, 50)])
        row = client.scan(tid).one_or_none()
        assert row is not None
        assert row.pk == 5
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_one_or_none_on_empty(self, client):
        sn, tid, schema = _make_table(client)
        assert client.scan(tid).one_or_none() is None
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_mappings_keys(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10)])
        ms = client.scan(tid).mappings()
        assert len(ms) == 1
        assert set(ms[0].keys()) == {"pk", "val"}
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_mappings_values(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        data = {m["pk"]: m["val"] for m in client.scan(tid).mappings()}
        assert data == {1: 10, 2: 20, 3: 30}
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_scalars_default_col0(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        pks = sorted(client.scan(tid).scalars())   # col 0 = pk
        assert pks == [1, 2, 3]
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_scalars_by_name(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        vals = sorted(client.scan(tid).scalars(col="val"))
        assert vals == [10, 20, 30]
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_scalars_by_int_index(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        vals = sorted(client.scan(tid).scalars(col=1))   # col 1 = val
        assert vals == [10, 20, 30]
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_reiterable(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
        result = client.scan(tid)
        pass1 = list(result)
        pass2 = list(result)
        assert len(pass1) == 3
        assert len(pass2) == 3
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_isinstance(self, client):
        """scan() returns a gnitz.ScanResult (the native Rust type)."""
        sn, tid, schema = _make_table(client)
        result = client.scan(tid)
        assert isinstance(result, gnitz.ScanResult)
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_len_zero(self, client):
        sn, tid, schema = _make_table(client)
        assert len(client.scan(tid)) == 0
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_iter(self, client):
        sn, tid, schema = _make_table(client)
        assert list(client.scan(tid)) == []
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_all(self, client):
        sn, tid, schema = _make_table(client)
        assert client.scan(tid).all() == []
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_first_none(self, client):
        sn, tid, schema = _make_table(client)
        assert client.scan(tid).first() is None
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_one_raises(self, client):
        sn, tid, schema = _make_table(client)
        with pytest.raises(ValueError):
            client.scan(tid).one()
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_mappings(self, client):
        sn, tid, schema = _make_table(client)
        assert client.scan(tid).mappings() == []
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_empty_table_scalars(self, client):
        sn, tid, schema = _make_table(client)
        assert client.scan(tid).scalars() == []
        client.drop_table(sn, "t")
        client.drop_schema(sn)


class TestRowAccess:
    """Verify Row access methods using a real round-trip through the server."""

    def _setup(self, client):
        sn, tid, schema = _make_table(client)
        _push_rows(client, tid, schema, [(7, 42)])
        row = client.scan(tid).first()
        return sn, tid, row

    def test_attr_pk(self, client):
        sn, tid, row = self._setup(client)
        assert row.pk == 7
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_attr_val(self, client):
        sn, tid, row = self._setup(client)
        assert row.val == 42
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_getitem_str_pk(self, client):
        sn, tid, row = self._setup(client)
        assert row["pk"] == 7
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_getitem_str_val(self, client):
        sn, tid, row = self._setup(client)
        assert row["val"] == 42
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_getitem_int_0(self, client):
        sn, tid, row = self._setup(client)
        assert row[0] == 7
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_getitem_int_1(self, client):
        sn, tid, row = self._setup(client)
        assert row[1] == 42
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_asdict(self, client):
        sn, tid, row = self._setup(client)
        assert row._asdict() == {"pk": 7, "val": 42}
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_tuple(self, client):
        sn, tid, row = self._setup(client)
        assert row._tuple() == (7, 42)
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_repr(self, client):
        sn, tid, row = self._setup(client)
        assert "pk=7" in repr(row)
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_eq(self, client):
        sn, tid, row = self._setup(client)
        # Same values, different weight — must be equal per __eq__ semantics
        same = Row(("pk", "val"), (7, 42), weight=99)
        assert row == same
        client.drop_table(sn, "t"); client.drop_schema(sn)

    def test_weight_default(self, client):
        sn, tid, row = self._setup(client)
        assert row.weight == 1
        client.drop_table(sn, "t"); client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Section 3 — Struct declarative schema API
# ---------------------------------------------------------------------------

class TestStruct:
    """Offline tests for gnitz.Struct declarative schemas."""

    def test_basic_schema(self):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64
        assert T._schema is not None
        assert len(T._columns) == 2
        assert T._schema.pk_index == 0

    def test_pk_not_first_column(self):
        class T(Struct):
            val: I64
            pk:  U64 = field(primary_key=True)
        assert T._schema.pk_index == 1

    def test_pk_defaults_to_zero(self):
        class T(Struct):
            a: U64
            b: I64
        assert T._schema.pk_index == 0

    def test_column_names(self):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64
        assert T._columns[0].name == "pk"
        assert T._columns[1].name == "val"

    def test_column_type_codes(self):
        class T(Struct):
            a: U64
            b: I64
            c: F64
        assert T._columns[0].type_code == TypeCode.U64
        assert T._columns[1].type_code == TypeCode.I64
        assert T._columns[2].type_code == TypeCode.F64

    def test_nullable_union(self):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64 | None
        assert T._columns[1].is_nullable is True
        assert T._columns[0].is_nullable is False

    def test_nullable_optional(self):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: typing.Optional[I64]
        assert T._columns[1].is_nullable is True

    def test_non_nullable_default(self):
        class T(Struct):
            pk: U64
            val: I64
        assert T._columns[0].is_nullable is False
        assert T._columns[1].is_nullable is False

    def test_multiple_pk_raises(self):
        with pytest.raises(TypeError, match="multiple primary keys"):
            class T(Struct):
                a: U64 = field(primary_key=True)
                b: I64 = field(primary_key=True)

    def test_no_fields_raises(self):
        with pytest.raises(TypeError, match="no fields"):
            class T(Struct):
                pass

    def test_bad_annotation_raises(self):
        with pytest.raises(TypeError, match="expected gnitz type"):
            class T(Struct):
                x: int

    def test_instantiation_raises(self):
        class T(Struct):
            pk: U64
        with pytest.raises(TypeError, match="schema descriptor"):
            T()

    def test_field_spec_cleaned_up(self):
        class T(Struct):
            pk: U64 = field(primary_key=True)
            val: I64
        assert not hasattr(T, 'pk') or not hasattr(getattr(T, 'pk', None), 'primary_key')

    def test_all_type_markers(self):
        class T(Struct):
            a: gnitz.U8
            b: gnitz.I8
            c: gnitz.U16
            d: gnitz.I16
            e: gnitz.U32
            f: gnitz.I32
            g: gnitz.F32
            h: gnitz.U64
            i: gnitz.I64
            j: gnitz.F64
            k: gnitz.STRING
            l: gnitz.U128
        assert len(T._columns) == 12

    def test_string_nullable(self):
        class T(Struct):
            pk:    U64 = field(primary_key=True)
            label: STRING | None
        assert T._columns[1].type_code == TypeCode.STRING
        assert T._columns[1].is_nullable is True

    def test_zsetbatch_accepts_struct(self):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64
        batch = ZSetBatch(T)
        batch.append(pk=1, val=10)
        assert len(batch) == 1

    def test_schema_matches_manual(self):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64 | None
        manual = Schema([ColumnDef("pk", TypeCode.U64, primary_key=True),
                         ColumnDef("val", TypeCode.I64, is_nullable=True)])
        assert T._schema.pk_index == manual.pk_index
        assert len(T._columns) == len(manual.columns)
        for a, b in zip(T._columns, manual.columns):
            assert a.name == b.name
            assert a.type_code == b.type_code
            assert a.is_nullable == b.is_nullable


class TestStructOnline:
    """Online tests: Struct-based create_table → push → scan round-trip."""

    def test_create_table_with_struct(self, client):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64
        sn = "s" + _uid()
        client.create_schema(sn)
        tid = client.create_table(sn, "t", T)
        assert isinstance(tid, int) and tid > 0
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_round_trip(self, client):
        class T(Struct):
            pk:  U64 = field(primary_key=True)
            val: I64
        sn = "s" + _uid()
        client.create_schema(sn)
        tid = client.create_table(sn, "t", T)
        batch = ZSetBatch(T)
        batch.append(pk=1, val=10)
        batch.append(pk=2, val=20)
        client.push(tid, batch)
        data = {r.pk: r.val for r in client.scan(tid)}
        assert data == {1: 10, 2: 20}
        client.drop_table(sn, "t")
        client.drop_schema(sn)

    def test_nullable_round_trip(self, client):
        class T(Struct):
            pk:    U64 = field(primary_key=True)
            label: STRING | None
        sn = "s" + _uid()
        client.create_schema(sn)
        tid = client.create_table(sn, "t", T)
        batch = ZSetBatch(T)
        batch.append(pk=1, label="hello")
        batch.append(pk=2, label=None)
        client.push(tid, batch)
        rows = {r.pk: r.label for r in client.scan(tid)}
        assert rows[1] == "hello"
        assert rows[2] is None
        client.drop_table(sn, "t")
        client.drop_schema(sn)


class TestEmptyTableLazySchema:
    """Fix 3: scan on an empty table must return a ScanResult with a
    non-None schema."""

    def test_scan_empty_table_has_schema(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True),
                ColumnDef("val", TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)
        result = client.scan(tid)
        assert result.schema is not None, "scan on empty table must return non-None schema"
        assert len(result) == 0
        client.drop_table(sn, "t")
        client.drop_schema(sn)


class TestUuidScalars:
    """Fix 8: ScanResult.scalars() on a UUID payload column must return
    hyphenated strings, matching the output of iteration and mappings()."""

    def test_scalars_uuid_col_returns_hyphenated_strings(self, client):
        import uuid as _uuid
        sn = "s" + _uid()
        client.create_schema(sn)
        uid_val = _uuid.uuid4()
        uid_str = str(uid_val)
        cols = [
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("id", TypeCode.UUID),
        ]
        schema = Schema(cols)
        tid = client.create_table(sn, "t", cols)
        batch = ZSetBatch(schema)
        batch.append(pk=1, id=uid_val)
        client.push(tid, batch)
        result = client.scan(tid)
        scalars = result.scalars("id")
        assert len(scalars) == 1
        assert scalars[0] == uid_str, f"expected {uid_str!r}, got {scalars[0]!r}"
        # Verify it matches the iteration path too.
        row = result.first()
        assert row.id == uid_str
        client.drop_table(sn, "t")
        client.drop_schema(sn)


class TestUuidSeekDelete:
    """Fix 11: seek() must accept uuid.UUID objects as PK."""

    def test_seek_with_uuid_object(self, client):
        import uuid as _uuid
        sn = "s" + _uid()
        client.create_schema(sn)
        uid_val = _uuid.uuid4()
        uid_str = str(uid_val)
        cols = [
            ColumnDef("pk", TypeCode.UUID, primary_key=True),
            ColumnDef("val", TypeCode.I64),
        ]
        schema = Schema(cols)
        tid = client.create_table(sn, "t", cols)
        batch = ZSetBatch(schema)
        batch.append(pk=uid_val, val=42)
        client.push(tid, batch)
        # seek() with a uuid.UUID object must not raise TypeError.
        result = client.seek(tid, uid_val)
        assert result is not None
        assert len(result) == 1
        row = result.first()
        assert row is not None
        assert row.pk == uid_str
        client.drop_table(sn, "t")
        client.drop_schema(sn)
