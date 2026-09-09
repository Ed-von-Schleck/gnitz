"""The client-side read path: `ScanResult`'s accessors and the `Row` objects
they hand out — how a scanned batch is presented to Python, what each accessor
promises on an empty result, and how a row addresses its fields.
"""
import uuid

import pytest

from gnitz import TypeCode, ColumnDef, Schema, Row, ZSetBatch


KV_COLS = [ColumnDef("pk", TypeCode.U64, primary_key=True),
           ColumnDef("val", TypeCode.I64)]
_THREE_ROWS = [{"pk": 1, "val": 10}, {"pk": 2, "val": 20}, {"pk": 3, "val": 30}]


def _loaded(client, schema_name, cols=KV_COLS, rows=_THREE_ROWS):
    """A table holding `rows`, scanned. The table is what the accessors are read
    off, so building it is not the subject of any test below."""
    tid = client.create_table(schema_name, "t", cols)
    client.push(tid, ZSetBatch(Schema(cols)).extend(rows))
    return client.scan(tid)


# ---------------------------------------------------------------------------
# ScanResult accessors
# ---------------------------------------------------------------------------


class TestScanResultAccessors:

    def test_accessors_agree_on_the_same_rows(self, client, schema_name):
        """`all`, iteration, `mappings`, `scalars`, `pks` and `weights` are six
        presentations of one batch — they must not disagree about its contents.

        The batch is retained, so iteration is repeatable: a consumed-once
        iterator would leave the second pass empty. `pks` and `weights` are
        positionally aligned with each other, which is what lets a caller `zip`
        them; in a Z-set engine the weight vector is the accessor that matters.
        """
        result = _loaded(client, schema_name)
        expected = {1: 10, 2: 20, 3: 30}
        assert len(result) == 3
        assert {r.pk: r.val for r in result.all()} == expected
        assert {r.pk: r.val for r in list(result)} == expected
        assert {r.pk: r.val for r in list(result)} == expected      # re-iterable
        assert {m["pk"]: m["val"] for m in result.mappings()} == expected
        assert set(result.mappings()[0].keys()) == {"pk", "val"}
        assert result.first() in result.all()
        assert sorted(zip(result.pks, result.weights)) == [(1, 1), (2, 1), (3, 1)]

    def test_scalars_resolution_modes(self, client, schema_name):
        """`col` resolves three ways — omitted is presented column 0, a name is
        a field lookup, an int is a presented position — and each way that can
        fail names its own error."""
        result = _loaded(client, schema_name)
        assert sorted(result.scalars()) == [1, 2, 3]            # col 0 = pk
        assert sorted(result.scalars(col="val")) == [10, 20, 30]
        assert sorted(result.scalars(col=1)) == [10, 20, 30]
        with pytest.raises(KeyError):
            result.scalars(col="nosuchcol")
        with pytest.raises(IndexError):
            result.scalars(col=99)
        with pytest.raises(TypeError):
            result.scalars(col=1.5)

    def test_scalars_of_a_uuid_column_matches_the_row_path(self, client, schema_name):
        """A UUID reaches Python as canonical hyphenated text; `scalars` must
        decode it the same way iteration does, not hand back the raw u128."""
        uid_val = uuid.uuid4()
        result = _loaded(
            client, schema_name,
            cols=[ColumnDef("pk", TypeCode.U64, primary_key=True),
                  ColumnDef("id", TypeCode.UUID)],
            rows=[{"pk": 1, "id": uid_val}])
        assert result.scalars("id") == [str(uid_val)]
        assert result.first().id == str(uid_val)


class TestEmptyResult:

    def test_every_accessor_on_an_empty_table(self, client, schema_name):
        """Each accessor's empty case, including the schema: an empty result
        still carries the table's schema, so a caller can read field names
        without branching on emptiness."""
        result = _loaded(client, schema_name, rows=[])
        assert len(result) == 0
        assert list(result) == []
        assert result.all() == []
        assert result.first() is None
        assert result.mappings() == []
        assert result.scalars() == []
        assert result.pks == []
        assert result.weights == []
        assert result.schema is not None


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
        assert tuple(row) == self._VALUES
        assert len(row) == 3
        assert row._asdict() == dict(zip(self._FIELDS, self._VALUES))
        assert row.weight == 2

    def test_equality_and_hashing_both_ignore_the_weight(self):
        """A row is its values, so two rows differing only in weight are equal —
        and must therefore hash equal, or a dict keyed by rows would hold both."""
        a, b = Row(self._FIELDS, self._VALUES, 1), Row(self._FIELDS, self._VALUES, 99)
        assert a == b and hash(a) == hash(b)
        assert {a: "value"}[b] == "value"
        assert Row(self._FIELDS, (42, 99, "hello"), 1) != a

    def test_a_bad_key_names_its_own_error(self):
        row = self._row()
        with pytest.raises(AttributeError):
            _ = row.nonexistent
        with pytest.raises(KeyError):
            _ = row["nonexistent"]
        with pytest.raises(TypeError, match="key must be int or str"):
            _ = row[1.5]


class TestScannedRow:

    def test_row_from_a_scan_is_wired_to_the_result_schema(self, client, schema_name):
        """A scanned row's field names come from the result schema, so every
        access path a hand-built Row supports resolves the same way here."""
        row = _loaded(client, schema_name, rows=[{"pk": 7, "val": 42}]).first()
        assert (row.pk, row.val) == (7, 42)
        assert (row["pk"], row["val"]) == (7, 42)
        assert (row[0], row[1]) == (7, 42)
        assert row._asdict() == {"pk": 7, "val": 42}
        assert row.weight == row._weight == 1
        assert row == Row(("pk", "val"), (7, 42), weight=99)
        # The result presents its rows as a synthesised `Row` subclass, so a
        # column resolves as a type attribute rather than through a fallback.
        # It is still a `Row` — `isinstance` is what callers branch on.
        assert isinstance(row, Row) and type(row) is not Row
        # A computed name resolves exactly like a literal one.
        assert getattr(row, "".join("val")) == 42

    def test_the_row_object_owns_only_its_underscore_names(self, client, schema_name):
        """The schema is the authority on what a name means — the rule the write
        surface already states by spelling the row weight `_weight`.

        So a column named `weight` *is* `row.weight`, and `row._weight` is the
        Z-set weight, which no column can ever be named. The underscore namespace
        belongs to the row object (`_fields`, `_asdict`, `_weight`), so a column
        whose name starts with `_` gets no attribute of its own — but it is still
        a presented column, and the fallback answers for it.
        """
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True),
                ColumnDef("weight", TypeCode.I64),
                ColumnDef("_x", TypeCode.I64)]
        row = _loaded(client, schema_name, cols=cols,
                      rows=[{"pk": 1, "weight": 77, "_x": 5}]).first()
        assert row.weight == row["weight"] == 77     # the column, via its descriptor
        assert row["_x"] == row[2] == row._x == 5    # no descriptor: the fallback
        assert row._weight == 1                      # the Z-set weight, not 77
        assert row._fields == ("pk", "weight", "_x")
        assert row._asdict() == {"pk": 1, "weight": 77, "_x": 5}
