"""`INSERT INTO … VALUES` through the SQL front end: the multi-row list, the
explicit `ROW` keyword, and the singular `VALUE` spelling.

The three spellings carry no semantic difference, so they are one test over the
`VALUES` clause each writes.
"""

import pytest
import gnitz

_CREATE = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"

_SPELLINGS = [
    ("values", "VALUES (42, 100)", [(42, 100)]),
    ("values-multi-row", "VALUES (1, 10), (2, 20), (3, 30)", [(1, 10), (2, 20), (3, 30)]),
    ("row-keyword", "VALUES ROW(1, 10), ROW(2, 20)", [(1, 10), (2, 20)]),
    ("singular-value", "VALUE (7, 70)", [(7, 70)]),
    # One decoder serves every constant position, so a PK slot and a payload
    # slot accept the same spellings: a unary plus and a parenthesised literal.
    ("signed-and-parenthesised", "VALUES (+1, (2)), ((3), +4)", [(1, 2), (3, 4)]),
]


@pytest.mark.parametrize("clause,expect", [c[1:] for c in _SPELLINGS],
                         ids=[c[0] for c in _SPELLINGS])
def test_insert_values_spellings(client, schema_name, clause, expect):
    client.execute_sql(_CREATE, schema_name=schema_name)
    res = client.execute_sql(f"INSERT INTO t {clause}", schema_name=schema_name)
    assert res[0]["type"] == "RowsAffected"
    assert res[0]["count"] == len(expect)

    tid = client.resolve_table(schema_name, "t")[0]
    rows = list(client.scan(tid))
    assert sorted((r.pk, r.val) for r in rows) == expect
    assert all(r.weight == 1 for r in rows)


def test_a_signed_null_is_the_null_it_spells(client, schema_name):
    """A sign must not hide the literal from the null check while the writer
    sees through it — that writes the cell zero with the null bit left clear,
    and it reads back as a real 0. The same check is what refuses the cell under
    a NOT NULL column."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, +NULL), (2, -NULL), (3, NULL)",
                       schema_name=sn)
    tid = client.resolve_table(sn, "t")[0]
    assert sorted((r.pk, r.v, r.weight) for r in client.scan(tid)) == [
        (1, None, 1), (2, None, 1), (3, None, 1)]

    client.execute_sql(
        "CREATE TABLE nn (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO nn VALUES (1, +NULL)", schema_name=sn)


@pytest.mark.parametrize("cell", ["-'abc'", "+'abc'"])
def test_a_signed_string_cell_is_refused(client, schema_name, cell):
    """Either sign over a string is meaningless, and accepting it silently
    discarded the sign and wrote the bare string."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(f"INSERT INTO t VALUES (1, {cell})", schema_name=sn)
