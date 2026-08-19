"""The client-side read path: `ScanResult`'s accessors and the `Row` objects
they hand out — how a scanned batch is presented to Python, what each accessor
promises on an empty result, and how a row addresses its fields.
"""
import random
import uuid

import pytest

from gnitz import TypeCode, ColumnDef, Schema, Row, ZSetBatch


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, *names):
    for name in names:
        try:
            client.drop_table(sn, name)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _table(client, cols=None):
    """Create a table (default: pk U64 PK, val I64). Returns (sn, tid, schema)."""
    sn = "sr" + _uid()
    client.create_schema(sn)
    cols = cols or [ColumnDef("pk", TypeCode.U64, primary_key=True),
                    ColumnDef("val", TypeCode.I64)]
    tid = client.create_table(sn, "t", cols)
    return sn, tid, Schema(cols)


def _push(client, tid, schema, pk_val_pairs):
    batch = ZSetBatch(schema)
    for pk, val in pk_val_pairs:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)


_THREE_ROWS = [(1, 10), (2, 20), (3, 30)]


# ---------------------------------------------------------------------------
# ScanResult accessors
# ---------------------------------------------------------------------------


class TestScanResultAccessors:

    def test_accessors_agree_on_the_same_rows(self, client):
        """`all`, iteration, `mappings` and `scalars` are four presentations of
        one batch — they must not disagree about its contents."""
        sn, tid, schema = _table(client)
        try:
            _push(client, tid, schema, _THREE_ROWS)
            result = client.scan(tid)
            assert len(result) == 3
            assert bool(result) is True
            expected = dict(_THREE_ROWS)
            assert {r.pk: r.val for r in result.all()} == expected
            assert {r.pk: r.val for r in list(result)} == expected
            mappings = result.mappings()
            assert set(mappings[0].keys()) == {"pk", "val"}
            assert {m["pk"]: m["val"] for m in mappings} == expected
            assert result.first() in result.all()
        finally:
            _cleanup(client, sn, "t")

    def test_scalars_resolution_modes(self, client):
        """`col` resolves three ways — omitted is presented column 0, a name is
        a field lookup, an int is a presented position — and each way that can
        fail names its own error."""
        sn, tid, schema = _table(client)
        try:
            _push(client, tid, schema, _THREE_ROWS)
            result = client.scan(tid)
            assert sorted(result.scalars()) == [1, 2, 3]            # col 0 = pk
            assert sorted(result.scalars(col="val")) == [10, 20, 30]
            assert sorted(result.scalars(col=1)) == [10, 20, 30]
            with pytest.raises(KeyError):
                result.scalars(col="nosuchcol")
            with pytest.raises(IndexError):
                result.scalars(col=99)
            with pytest.raises(TypeError):
                result.scalars(col=1.5)
        finally:
            _cleanup(client, sn, "t")

    def test_scalars_of_a_uuid_column_matches_the_row_path(self, client):
        """A UUID reaches Python as canonical hyphenated text; `scalars` must
        decode it the same way iteration does, not hand back the raw u128."""
        sn, tid, schema = _table(client, cols=[
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("id", TypeCode.UUID),
        ])
        try:
            uid_val = uuid.uuid4()
            batch = ZSetBatch(schema)
            batch.append(pk=1, id=uid_val)
            client.push(tid, batch)
            result = client.scan(tid)
            assert result.scalars("id") == [str(uid_val)]
            assert result.first().id == str(uid_val)
        finally:
            _cleanup(client, sn, "t")

    def test_result_is_re_iterable(self, client):
        """The batch is retained, so a second pass sees the same rows — a
        consumed-once iterator would come back empty."""
        sn, tid, schema = _table(client)
        try:
            _push(client, tid, schema, _THREE_ROWS)
            result = client.scan(tid)
            assert len(list(result)) == 3
            assert len(list(result)) == 3
        finally:
            _cleanup(client, sn, "t")


class TestEmptyResult:

    def test_every_accessor_on_an_empty_table(self, client):
        """Each accessor's empty case, including the schema: an empty result
        still carries the table's schema, so a caller can read field names
        without branching on emptiness."""
        sn, tid, schema = _table(client)
        try:
            result = client.scan(tid)
            assert len(result) == 0
            assert bool(result) is False
            assert list(result) == []
            assert result.all() == []
            assert result.first() is None
            assert result.mappings() == []
            assert result.scalars() == []
            assert result.schema is not None
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Row
# ---------------------------------------------------------------------------


class TestRowSemantics:
    """Hand-built rows — the Row object itself, with no server in the way."""

    _FIELDS = ("pk", "val", "label")
    _VALUES = (42, 100, "hello")

    def _row(self, weight=2):
        return Row(self._FIELDS, self._VALUES, weight=weight)

    def test_every_field_access_path_agrees(self):
        row = self._row()
        assert (row.pk, row.val, row.label) == self._VALUES
        assert (row["pk"], row["val"], row["label"]) == self._VALUES
        assert (row[0], row[1], row[2]) == self._VALUES
        assert row[-1] == "hello"
        assert list(row) == list(self._VALUES)
        assert len(row) == 3
        assert row._asdict() == dict(zip(self._FIELDS, self._VALUES))
        assert tuple(row) == self._VALUES
        assert row.weight == 2

    def test_eq_ignores_weight(self):
        assert Row(self._FIELDS, self._VALUES, weight=1) == \
               Row(self._FIELDS, self._VALUES, weight=99)
        assert Row(self._FIELDS, (42, 99, "hello"), 1) != \
               Row(self._FIELDS, self._VALUES, 1)

    def test_hash_follows_the_values_tuple(self):
        row = self._row()
        assert hash(row) == hash(self._VALUES)
        assert row in {row}
        assert {row: "value"}[row] == "value"

    def test_unknown_field_names_its_own_error(self):
        row = self._row()
        with pytest.raises(AttributeError):
            _ = row.nonexistent
        with pytest.raises(KeyError):
            _ = row["nonexistent"]

    def test_non_int_non_str_key_rejected(self):
        with pytest.raises(TypeError, match="key must be int or str"):
            _ = self._row()[1.5]

    def test_repr_shows_fields_and_weight(self):
        r = repr(self._row(weight=2))
        assert "pk=42" in r
        assert "weight=2" in r


class TestScannedRow:

    def test_row_from_a_scan_is_wired_to_the_result_schema(self, client):
        """A scanned row's field names come from the result schema, so every
        access path a hand-built Row supports resolves the same way here."""
        sn, tid, schema = _table(client)
        try:
            _push(client, tid, schema, [(7, 42)])
            row = client.scan(tid).first()
            assert (row.pk, row.val) == (7, 42)
            assert (row["pk"], row["val"]) == (7, 42)
            assert (row[0], row[1]) == (7, 42)
            assert row._asdict() == {"pk": 7, "val": 42}
            assert row.weight == 1
            assert row._weight == 1
            assert row == Row(("pk", "val"), (7, 42), weight=99)
            # The result presents its rows as a synthesised `Row` subclass, so a
            # column resolves as a type attribute rather than through a fallback.
            # It is still a `Row` — `isinstance` is what callers branch on.
            assert isinstance(row, Row)
            assert type(row) is not Row
            # A computed name resolves exactly like a literal one.
            assert getattr(row, "".join("val")) == 42
        finally:
            _cleanup(client, sn, "t")

    def test_a_column_named_weight_wins_over_the_zset_weight(self, client):
        """The schema is the authority on what a name means — the rule the write
        surface already states by spelling the row weight `_weight`. So a column
        named `weight` *is* `row.weight`, and `row._weight` is the Z-set weight,
        which no column can ever be named."""
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True),
                ColumnDef("weight", TypeCode.I64)]
        sn = "sr" + _uid()
        client.create_schema(sn)
        try:
            tid = client.create_table(sn, "t", cols)
            batch = ZSetBatch(Schema(cols))
            batch.append(pk=1, weight=77)
            client.push(tid, batch)
            row = client.scan(tid).first()
            assert row.weight == 77          # the column
            assert row["weight"] == 77
            assert row._weight == 1          # the Z-set weight, which is not 77
        finally:
            _cleanup(client, sn, "t")

    def test_an_underscore_column_is_reachable_only_by_subscript(self, client):
        """The underscore namespace belongs to the row object itself (`_fields`,
        `_asdict`, `_weight`), so a column whose name starts with `_` gets no
        attribute of its own — but it is still a presented column, addressable
        by name and by position."""
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True),
                ColumnDef("_x", TypeCode.I64)]
        sn = "sr" + _uid()
        client.create_schema(sn)
        try:
            tid = client.create_table(sn, "t", cols)
            batch = ZSetBatch(Schema(cols))
            batch.append(pk=1, _x=5)
            client.push(tid, batch)
            row = client.scan(tid).first()
            assert row["_x"] == 5
            assert row[1] == 5
            assert row._asdict() == {"pk": 1, "_x": 5}
            assert row._fields == ("pk", "_x")
            # `_x` names no column-shaped attribute, so the fallback answers it.
            assert row._x == 5
            assert row._weight == 1
        finally:
            _cleanup(client, sn, "t")
