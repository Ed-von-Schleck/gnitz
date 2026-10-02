"""The client-side read path: `ScanResult` and the `Row` objects it hands out —
how a scanned batch is presented to Python, what an empty result promises, and
how a row addresses its fields.
"""
import uuid

import pytest

from gnitz import TypeCode, ColumnDef, Schema, Row, ZSetBatch
from _schemas import KV


_UUID = uuid.uuid4()
_TABLES = {
    "kv": (KV, [{"pk": 1, "val": 10}, {"pk": 2, "val": 20}, {"pk": 3, "val": 30}]),
    "empty": (KV, []),
    "uuid": (Schema([ColumnDef("pk", TypeCode.U64), ColumnDef("id", TypeCode.UUID)], [0]),
             [{"pk": 1, "id": _UUID}]),
    "under": (Schema([ColumnDef("pk", TypeCode.U64),
                      ColumnDef("weight", TypeCode.I64),
                      ColumnDef("_x", TypeCode.I64)], [0]),
              [{"pk": 1, "weight": 77, "_x": 5}]),
}


@pytest.fixture(scope="module")
def scans(module_client):
    """Each table in `_TABLES`, loaded and scanned once. A `ScanResult` retains
    its batch, so every test reads the same result objects."""
    conn = module_client
    out = {}
    for name, (schema, rows) in _TABLES.items():
        tid = conn.create_table(name, schema)
        conn.push(tid, ZSetBatch(schema).extend(rows))
        out[name] = conn.scan(tid, schema)
    return out


# ---------------------------------------------------------------------------
# ScanResult
# ---------------------------------------------------------------------------


def test_iteration_presents_every_row_with_its_weight(scans):
    """The batch is retained, so iteration is repeatable: a consumed-once
    iterator would leave the second pass empty."""
    result = scans["kv"]
    assert len(result) == 3
    assert sorted((r.pk, r.val, r._weight) for r in result) == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]
    assert sorted((r.pk, r.val, r._weight) for r in result) == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]
    assert result.lsn is not None


def test_a_uuid_column_reads_as_canonical_text(scans):
    assert [r.id for r in scans["uuid"]] == [str(_UUID)]


def test_an_empty_result(scans):
    """An empty result still carries the table's schema, so a caller can read
    field names without branching on emptiness."""
    result = scans["empty"]
    assert len(result) == 0
    assert not result
    assert list(result) == []
    assert [c.name for c in result.schema.columns] == ["pk", "val"]


def test_including_hidden_presents_every_column_of_the_same_rows(module_client):
    """A dropped column is a hidden slot: absent from the default presentation,
    first-class in `including_hidden()`, which reads the same batch at the same
    LSN."""
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE hid (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "INSERT INTO hid VALUES (1, 10, 100), (2, 20, 200); "
        "ALTER TABLE hid DROP COLUMN a")
    result = conn.scan(*conn.resolve_table("hid"))
    full = result.including_hidden()

    assert [r._fields for r in result] == [("pk", "b")] * 2
    assert sorted(tuple(r) for r in result) == [(1, 100), (2, 200)]
    assert [r._fields for r in full] == [("pk", "a", "b")] * 2
    assert sorted((r.pk, r.b, r._weight) for r in full) == [(1, 100, 1), (2, 200, 1)]
    assert len(full) == len(result) and full.lsn == result.lsn
    assert [c.name for c in full.schema.columns] == [c.name for c in result.schema.columns]


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

    a, b, c = (ZSetBatch(KV).append(pk=1, val=10).append(pk=1, val=10, _weight=99)
               .append(pk=1, val=11).rows())
    assert a._fields == b._fields
    assert a == b and hash(a) == hash(b)
    assert {a: "value"}[b] == "value"
    assert a != c

    # A row comes from a result; the type builds none.
    with pytest.raises(TypeError):
        type(row)((1, 10))

    with pytest.raises(AttributeError):
        _ = row.nonexistent
    with pytest.raises(KeyError):
        _ = row["nonexistent"]
    with pytest.raises(TypeError, match="key must be int or str"):
        _ = row[1.5]


def test_the_row_object_keeps_only_the_names_it_answers(scans):
    """The schema is the authority on what a name means — the rule the write
    surface already states by spelling the row weight `_weight`.

    So a column named `weight` *is* `row.weight`, and `row._weight` is the Z-set
    weight. A name the row object itself answers (`_fields`, `_asdict`,
    `_weight`) keeps that meaning; every other column, underscore-prefixed or
    not, is an attribute.
    """
    row = next(iter(scans["under"]))
    assert row.weight == row["weight"] == 77
    assert row["_x"] == row[2] == row._x == 5
    assert row._weight == 1                      # the Z-set weight, not 77
    assert row._fields == ("pk", "weight", "_x")
    assert row._asdict() == {"pk": 1, "weight": 77, "_x": 5}


_ID = ColumnDef("id", TypeCode.I64)
_A = ColumnDef("a", TypeCode.I64)
_H = ColumnDef("a", TypeCode.I64, is_hidden=True)
_B = ColumnDef("b", TypeCode.I64)


@pytest.mark.parametrize("columns,extra", [
    ([_ID, _H, _B, _A], {"b": 0}),
    ([_ID, _A, _H], {}),
], ids=["tombstone-ahead", "tombstone-behind"])
def test_a_visible_column_claims_a_name_a_hidden_one_repeats(columns, extra):
    """`including_hidden()` presents a tombstone beside the column re-added or
    renamed onto its name. The hidden column holds its zero filler, so every
    by-name path reading 10 is every path reading the visible one."""
    batch = ZSetBatch(Schema(columns, [0])).append(id=1, a=10, **extra)
    [r] = batch.rows().including_hidden()
    assert r._fields == tuple(c.name for c in columns)
    assert r.a == r["a"] == r._asdict()["a"] == 10


def test_the_last_of_two_visible_columns_claims_a_repeated_name(module_client):
    """A join view's wildcard presents both sides' columns under their own
    names, so `pk` and `a` each appear twice."""
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE TABLE d (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL); "
        "CREATE VIEW dup AS SELECT * FROM c JOIN d ON c.a = d.pk; "
        "INSERT INTO c VALUES (4, 1, 4); INSERT INTO d VALUES (1, 1)")
    [r] = conn.execute_sql("SELECT * FROM dup")[0]["rows"]
    assert tuple(r) == (4, 1, 4, 1, 1)
    assert r._fields == ("pk", "a", "b", "pk", "a")
    assert r.pk == r["pk"] == r._asdict()["pk"] == 1
