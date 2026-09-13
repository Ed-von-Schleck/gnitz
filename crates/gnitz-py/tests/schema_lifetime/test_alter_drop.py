"""ALTER TABLE DROP COLUMN and ALTER COLUMN DROP NOT NULL: a column leaves the
visible schema, or gains NULL as a value, while the rows underneath stay put.

DROP COLUMN is logical — the column is flagged hidden and kept physically
present — so what is asserted is that nothing names it and that the *positional*
forms (a shortened INSERT, `RETURNING *`) address the remaining columns.
"""

import gnitz
import pytest
from _read import access, bag, rows
from _serverproc import NEEDS_MULTI


# ── DROP COLUMN ─────────────────────────────────────────────────────────────


def test_drop_middle_column_remaps_every_positional_form(client, schema_name):
    """Every wildcard stops projecting the dropped middle column, a shortened
    INSERT lands its value in `b` rather than in the vacated slot, UPDATE's
    retraction matches the row as *stored* (hidden slot and all), DELETE
    retracts the row it named and no other, and the name is unusable from either
    side of a statement."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200); "
        "ALTER TABLE t DROP COLUMN a; "
        "INSERT INTO t VALUES (3, 300)", schema_name=sn)

    [ret] = client.execute_sql("INSERT INTO t VALUES (4, 400) RETURNING *", schema_name=sn)
    assert ret["type"] == "Rows"
    ret_rows = list(ret["rows"])
    assert ret_rows[0]._fields == ("id", "b")
    assert bag(ret_rows, "id", "b") == {(4, 400): 1}

    client.execute_sql("UPDATE t SET b = 999 WHERE id = 1; DELETE FROM t WHERE id = 2", schema_name=sn)
    got = rows(client, sn, "SELECT * FROM t")
    assert got[0]._fields == ("id", "b")
    assert bag(got, "id", "b") == {(1, 999): 1, (3, 300): 1, (4, 400): 1}

    with pytest.raises(gnitz.GnitzError, match="column 'a' not found"):
        client.execute_sql("INSERT INTO t (id, a, b) VALUES (5, 1, 2)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match="column 'a' not found"):
        client.execute_sql("SELECT a FROM t", schema_name=sn)


def test_drop_column_refuses_a_bound_column_until_the_binding_goes(client, schema_name):
    """A column a foreign key or a secondary index binds is refused by naming
    the binding, so the author knows which thing to undo; the column survives
    the refusal, and dropping the index makes its column droppable."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
        "ref BIGINT NOT NULL REFERENCES p(id), ix BIGINT NOT NULL); "
        "CREATE INDEX iix ON t (ix); "
        "INSERT INTO p VALUES (1); INSERT INTO t VALUES (1, 1, 5)", schema_name=sn)

    for column, message in (("ref", "carries a foreign key"), ("ix", "covered by a secondary index")):
        with pytest.raises(gnitz.GnitzError, match=message):
            client.execute_sql(f"ALTER TABLE t DROP COLUMN {column}", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM t"), "id", "ref", "ix") == {(1, 1, 5): 1}

    client.execute_sql("DROP INDEX iix; ALTER TABLE t DROP COLUMN ix", schema_name=sn)
    got = rows(client, sn, "SELECT * FROM t")
    assert got[0]._fields == ("id", "ref")
    assert bag(got, "id", "ref") == {(1, 1): 1}


# ── DROP NOT NULL ───────────────────────────────────────────────────────────


@NEEDS_MULTI
def test_drop_not_null_makes_null_a_value_distinct_from_zero(client, schema_name):
    """After DROP NOT NULL a NULL and an explicit 0 coexist on distinct PKs as
    *distinct* values, on every partition. Asserted as a weighted bag: a
    partition still ordering the column as non-nullable sorts the NULL as a real
    0 and consolidates the two, which moves a weight rather than a row count.

    The index survives and still *serves* its seeks, and an index-bounded range
    yields the 0 but never the NULL.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(1, 41)) + "; "
        "CREATE INDEX iv ON t (v); "
        "ALTER TABLE t ALTER COLUMN v DROP NOT NULL; "
        "INSERT INTO t VALUES (100, NULL), (101, 0)", schema_name=sn)

    want = {(i, i * 10): 1 for i in range(1, 41)}
    want |= {(100, None): 1, (101, 0): 1}
    assert bag(rows(client, sn, "SELECT * FROM t"), "id", "v") == want

    for q, want in (("SELECT id FROM t WHERE v = 200", {(20,): 1}),
                    ("SELECT id FROM t WHERE v >= 0",
                     {(i,): 1 for i in range(1, 41)} | {(101,): 1})):
        plan = access(client, sn, q)
        assert "index" in plan, plan
        assert bag(rows(client, sn, q), "id") == want, q
