"""ALTER TABLE DROP COLUMN and ALTER COLUMN DROP NOT NULL: a column leaves the
visible schema, or gains NULL as a value, while the rows underneath stay put.

DROP COLUMN is logical — the column is flagged hidden and kept physically
present — so what is asserted is that nothing names it and that the *positional*
forms (a shortened INSERT, `RETURNING *`) address the remaining columns.

Run at GNITZ_WORKERS=4: the DROP NOT NULL comparator swap must reach every
partition, or one left on the fixed-int comparator sorts a NULL as a real 0.
"""

import pytest
from _read import access, bag, rows
from _serverproc import NEEDS_MULTI

_T3 = ("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
       "a BIGINT NOT NULL, b BIGINT NOT NULL)")


# ── DROP COLUMN ─────────────────────────────────────────────────────────────


def test_drop_middle_column_wildcards_and_positional_remap(client, schema_name):
    """Every wildcard stops projecting the dropped middle column, a shortened
    INSERT lands its value in `b` rather than in the vacated slot, and the name
    is unusable from either side of a statement."""
    client.execute_sql(_T3, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=schema_name)

    client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=schema_name)

    got = rows(client, schema_name, "SELECT * FROM t")
    assert "a" not in got[0]._fields
    assert bag(got, "id", "b") == {(1, 100): 1}

    # Positional remap: two visible columns, so the second value is `b`.
    client.execute_sql("INSERT INTO t VALUES (2, 200)", schema_name=schema_name)

    # `RETURNING *` is the other wildcard, and must agree.
    ret = client.execute_sql("INSERT INTO t VALUES (3, 300) RETURNING *", schema_name=schema_name)
    assert ret[0]["type"] == "Rows"
    ret_rows = list(ret[0]["rows"])
    assert "a" not in ret_rows[0]._fields
    assert bag(ret_rows, "id", "b") == {(3, 300): 1}

    assert bag(rows(client, schema_name, "SELECT * FROM t"), "id", "b") == {
        (1, 100): 1, (2, 200): 1, (3, 300): 1}

    # Unnameable from an explicit column list and from a projection.
    with pytest.raises(Exception):
        client.execute_sql("INSERT INTO t (id, a, b) VALUES (4, 1, 2)", schema_name=schema_name)
    with pytest.raises(Exception):
        client.execute_sql("SELECT a FROM t", schema_name=schema_name)


def test_update_and_delete_address_rows_across_a_logical_drop(client, schema_name):
    """UPDATE's retraction has to match the row as *stored* — hidden slot and
    all — and DELETE has to retract the row it named and no other."""
    client.execute_sql(_T3, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=schema_name)

    client.execute_sql("UPDATE t SET b = 999 WHERE id = 1", schema_name=schema_name)
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=schema_name)
    assert bag(rows(client, schema_name, "SELECT * FROM t"), "id", "b") == {(1, 999): 1}


@pytest.mark.parametrize(
    "column, message",
    [("id", "part of the primary key"),
     ("ref", "it carries a foreign key"),
     ("ix", "covered by a secondary index")],
)
def test_drop_column_refuses_a_column_something_else_binds(
        client, schema_name, column, message):
    """Three rungs of one ladder. Each names what binds the column, so the author
    knows which thing to undo — a bare refusal would leave them guessing which of
    the three fired. The column survives every refusal.

    The ladder's fourth rung, a SERIAL column, is unreachable from SQL: a SERIAL
    column must be the table's single-column PRIMARY KEY, so `is_pk_col` above it
    always answers first.
    """
    client.execute_sql(
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
        "ref BIGINT NOT NULL REFERENCES p(id), ix BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("CREATE INDEX iix ON t (ix)", schema_name=schema_name)

    with pytest.raises(Exception, match=message):
        client.execute_sql(f"ALTER TABLE t DROP COLUMN {column}", schema_name=schema_name)

    names = [c.name for c in client.resolve_table(schema_name, "t")[1].columns]
    assert column in names, f"the refused DROP COLUMN retired '{column}' anyway: {names}"


def test_drop_column_is_allowed_once_the_index_covering_it_goes(client, schema_name):
    """The index rung is the one an author can clear inside the same statement
    sequence: dropping the index makes the column droppable."""
    client.execute_sql(_T3, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=schema_name)
    client.execute_sql("CREATE INDEX ia ON t (a)", schema_name=schema_name)

    with pytest.raises(Exception, match="covered by a secondary index"):
        client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=schema_name)

    client.execute_sql("DROP INDEX ia", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=schema_name)
    got = rows(client, schema_name, "SELECT * FROM t")
    assert "a" not in got[0]._fields
    assert bag(got, "id", "b") == {(1, 100): 1}


# ── DROP NOT NULL ───────────────────────────────────────────────────────────


@NEEDS_MULTI
def test_drop_not_null_makes_null_a_value_distinct_from_zero(client, schema_name):
    """All-fixed-int NOT NULL puts the table on the FixedIntNonnull comparator;
    DROP NOT NULL forces the Generic swap, which must reach every partition.

    A NULL and an explicit 0 then coexist on distinct PKs as *distinct* values.
    Asserted as a weighted bag: a partition on the stale comparator sorts the
    NULL as a real 0 and consolidates the two, which moves a weight rather than
    a row count.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 41))
    client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=schema_name)

    with pytest.raises(Exception):
        client.execute_sql("INSERT INTO t VALUES (100, NULL)", schema_name=schema_name)

    client.execute_sql("ALTER TABLE t ALTER COLUMN v DROP NOT NULL", schema_name=schema_name)

    client.execute_sql("INSERT INTO t VALUES (100, NULL), (101, 0)", schema_name=schema_name)
    want = {(i, i * 10): 1 for i in range(1, 41)}
    want[(100, None)] = 1
    want[(101, 0)] = 1
    assert bag(rows(client, schema_name, "SELECT * FROM t"), "id", "v") == want


def test_drop_not_null_keeps_the_index_serving_and_omits_the_null(client, schema_name):
    """The index survives the swap and still *serves* the seek — a plan that
    lost it answers the same rows by full scan, so the access line is what tells
    them apart. A later NULL is absent from the index and present in the table.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 41))
    client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=schema_name)
    client.execute_sql("CREATE INDEX iv ON t (v)", schema_name=schema_name)

    client.execute_sql("ALTER TABLE t ALTER COLUMN v DROP NOT NULL", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (41, NULL)", schema_name=schema_name)

    seek = "SELECT id FROM t WHERE v = 200"
    assert "index" in access(client, schema_name, seek), access(client, schema_name, seek)
    assert bag(rows(client, schema_name, seek), "id") == {(20,): 1}

    # An index-bounded range never yields the NULL row; a full scan holds it.
    span = "SELECT id FROM t WHERE v >= 0"
    assert "index" in access(client, schema_name, span), access(client, schema_name, span)
    assert (41,) not in bag(rows(client, schema_name, span), "id")
    assert (41, None) in bag(rows(client, schema_name, "SELECT * FROM t"), "id", "v")
