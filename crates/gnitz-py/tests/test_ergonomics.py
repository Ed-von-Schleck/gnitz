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
        assert {tc.value for tc in TypeCode} == set(range(1, 13))


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

    def test_multiple_pk_raises(self):
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.I64, primary_key=True)]
        with pytest.raises(ValueError):
            Schema(cols)

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
