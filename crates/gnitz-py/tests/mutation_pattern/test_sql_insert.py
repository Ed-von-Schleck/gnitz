"""`INSERT INTO … VALUES` through the SQL front end: the multi-row list, the
explicit `ROW` keyword, and the singular `VALUE` spelling.

The three spellings carry no semantic difference, so they are one test over the
`VALUES` clause each writes.
"""

import pytest

_CREATE = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"

_SPELLINGS = [
    ("values", "VALUES (42, 100)", [(42, 100)]),
    ("values-multi-row", "VALUES (1, 10), (2, 20), (3, 30)", [(1, 10), (2, 20), (3, 30)]),
    ("row-keyword", "VALUES ROW(1, 10), ROW(2, 20)", [(1, 10), (2, 20)]),
    ("singular-value", "VALUE (7, 70)", [(7, 70)]),
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
