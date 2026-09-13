"""The client-side read path: `ScanResult`'s accessors and the `Row` objects
they hand out — how a scanned batch is presented to Python, what each accessor
promises on an empty result, and how a row addresses its fields.
"""
import uuid

import pytest

from gnitz import TypeCode, ColumnDef, Schema, Row, ZSetBatch


_UUID = uuid.uuid4()
_TABLES = {
    "kv": ([ColumnDef("pk", TypeCode.U64, primary_key=True), ColumnDef("val", TypeCode.I64)],
           [{"pk": 1, "val": 10}, {"pk": 2, "val": 20}, {"pk": 3, "val": 30}]),
    "empty": ([ColumnDef("pk", TypeCode.U64, primary_key=True), ColumnDef("val", TypeCode.I64)],
              []),
    "uuid": ([ColumnDef("pk", TypeCode.U64, primary_key=True), ColumnDef("id", TypeCode.UUID)],
             [{"pk": 1, "id": _UUID}]),
    "under": ([ColumnDef("pk", TypeCode.U64, primary_key=True),
               ColumnDef("weight", TypeCode.I64),
               ColumnDef("_x", TypeCode.I64)],
              [{"pk": 1, "weight": 77, "_x": 5}]),
}


@pytest.fixture(scope="module")
def scans(module_schema):
    """Each table in `_TABLES`, loaded and scanned once. A `ScanResult` retains
    its batch, so every test reads the same result objects."""
    conn, sn = module_schema
    out = {}
    for name, (cols, rows) in _TABLES.items():
        tid = conn.create_table(sn, name, cols)
        conn.push(tid, ZSetBatch(Schema(cols)).extend(rows))
        out[name] = conn.scan(tid)
    return out


# ---------------------------------------------------------------------------
# ScanResult accessors
# ---------------------------------------------------------------------------


def test_accessors_agree_on_the_same_rows(scans):
    """`all`, iteration, `mappings`, `scalars`, `pks` and `weights` are six
    presentations of one batch — they must not disagree about its contents.

    The batch is retained, so iteration is repeatable: a consumed-once iterator
    would leave the second pass empty. `pks` and `weights` are positionally
    aligned with each other, which is what lets a caller `zip` them.
    """
    result = scans["kv"]
    expected = {1: 10, 2: 20, 3: 30}
    assert len(result) == 3
    assert {r.pk: r.val for r in result.all()} == expected
    assert {r.pk: r.val for r in result} == expected
    assert {r.pk: r.val for r in result} == expected      # re-iterable
    assert {m["pk"]: m["val"] for m in result.mappings()} == expected
    assert set(result.mappings()[0].keys()) == {"pk", "val"}
    assert result.first() in result.all()
    assert sorted(zip(result.pks, result.weights)) == [(1, 1), (2, 1), (3, 1)]


def test_scalars_resolution_modes(scans):
    """`col` resolves three ways — omitted is presented column 0, a name is a
    field lookup, an int is a presented position — and each way that can fail
    names its own error."""
    result = scans["kv"]
    assert sorted(result.scalars()) == [1, 2, 3]            # col 0 = pk
    assert sorted(result.scalars(col="val")) == [10, 20, 30]
    assert sorted(result.scalars(col=1)) == [10, 20, 30]
    with pytest.raises(KeyError):
        result.scalars(col="nosuchcol")
    with pytest.raises(IndexError):
        result.scalars(col=99)
    with pytest.raises(TypeError):
        result.scalars(col=1.5)


def test_scalars_of_a_uuid_column_matches_the_row_path(scans):
    """A UUID reaches Python as canonical hyphenated text; `scalars` must decode
    it the same way iteration does, not hand back the raw u128."""
    result = scans["uuid"]
    assert result.scalars("id") == [str(_UUID)]
    assert result.first().id == str(_UUID)


def test_every_accessor_on_an_empty_result(scans):
    """Each accessor's empty case, including the schema: an empty result still
    carries the table's schema, so a caller can read field names without
    branching on emptiness."""
    result = scans["empty"]
    assert len(result) == 0
    assert list(result) == []
    assert result.all() == []
    assert result.first() is None
    assert result.mappings() == []
    assert result.scalars() == []
    assert result.pks == []
    assert result.weights == []
    assert [c.name for c in result.schema.columns] == ["pk", "val"]


# ---------------------------------------------------------------------------
# Row
# ---------------------------------------------------------------------------


def test_every_field_access_path_agrees(scans):
    """A scanned row's field names come from the result schema, and attribute,
    name, position and iteration all resolve them to the same values. A row is
    its values: rows differing only in weight are equal and hash equal, or a
    dict keyed by rows would hold both."""
    row = next(r for r in scans["kv"] if r.pk == 1)
    assert isinstance(row, Row)
    assert (row.pk, row.val) == (row["pk"], row["val"]) == (row[0], row[1]) == (1, 10)
    assert row[-1] == 10
    assert row["".join(["v", "al"])] == 10      # a non-interned name resolves too
    assert tuple(row) == (1, 10) and len(row) == 2
    assert row._asdict() == {"pk": 1, "val": 10}
    assert row._weight == 1

    twin = Row(row._fields, tuple(row), _weight=99)
    assert row == twin and hash(row) == hash(twin)
    assert {row: "value"}[twin] == "value"
    assert row != Row(row._fields, (1, 11))

    with pytest.raises(AttributeError):
        _ = row.nonexistent
    with pytest.raises(KeyError):
        _ = row["nonexistent"]
    with pytest.raises(TypeError, match="key must be int or str"):
        _ = row[1.5]


def test_the_row_object_owns_only_its_underscore_names(scans):
    """The schema is the authority on what a name means — the rule the write
    surface already states by spelling the row weight `_weight`.

    So a column named `weight` *is* `row.weight`, and `row._weight` is the Z-set
    weight, which no column can ever be named. The underscore namespace belongs
    to the row object (`_fields`, `_asdict`, `_weight`), so a column whose name
    starts with `_` gets no attribute of its own — but it is still a presented
    column, and the fallback answers for it.
    """
    row = scans["under"].first()
    assert row.weight == row["weight"] == 77     # the column, via its descriptor
    assert row["_x"] == row[2] == row._x == 5    # no descriptor: the fallback
    assert row._weight == 1                      # the Z-set weight, not 77
    assert row._fields == ("pk", "weight", "_x")
    assert row._asdict() == {"pk": 1, "weight": 77, "_x": 5}
