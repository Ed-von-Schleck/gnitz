"""The ORDER BY / OFFSET / LIMIT read bound, over a base table and over a view.

Two properties carry the file. The sink sorts BEFORE projection, so an ORDER BY
key absent from the SELECT list still resolves; and it counts LIMIT/OFFSET by
logical **multiplicity** — summed weight — not by Z-set entries, so a cut lands
in the same place however a bag is split across workers and entries.

Under a LIMIT the worker top-k discards rows before the client ever sees them,
so the two comparators must agree exactly: a divergence drops rows rather than
merely reordering them. That is why the TEXT-with-NULLs and float cases pin
values and not just an order.

What the bound refuses is pinned in the planner's `gnitz-sql/tests/plan_read.rs`.
"""

import pytest
from _read import bag, rows
from _sql import insert


@pytest.fixture(scope="module")
def src(module_schema):
    """Read-only sources for every case: `t (id, v, w, a)`, a TEXT column with
    NULLs, a DOUBLE column, and `dup` — a `UNION ALL` of two identical tables,
    so every row carries logical multiplicity 2."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT, w BIGINT, a BIGINT NOT NULL); "
        "CREATE TABLE names (id BIGINT PRIMARY KEY, name TEXT); "
        "CREATE TABLE floats (id BIGINT PRIMARY KEY, x DOUBLE); "
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE VIEW dup AS SELECT * FROM a UNION ALL SELECT * FROM b", schema_name=sn)
    insert(conn, sn, "t", [(1, 30, 10, 1), (2, 10, None, 1), (3, 20, 20, 2), (4, 40, 5, 2),
                           (5, 50, 15, 3)])
    insert(conn, sn, "names", [(1, "pear"), (2, "apple"), (3, None), (4, "banana"),
                               (5, "apricot"), (6, None), (7, "cherry")])
    insert(conn, sn, "floats", [(1, 2.5), (2, -1.75), (3, 0.0), (4, -3.25), (5, 1.25)])
    for name in ("a", "b"):
        insert(conn, sn, name, [(1, 10), (2, 20)])
    return sn


# ---------------------------------------------------------------------------
# Base-table ordering, and sort before projection
# ---------------------------------------------------------------------------


_ALL = ["id", "v", "w", "a"]


@pytest.mark.parametrize("q,want,fields", [
    ("SELECT * FROM t ORDER BY v", [2, 3, 1, 4, 5], _ALL),
    ("SELECT * FROM t ORDER BY v DESC", [5, 4, 1, 3, 2], _ALL),
    # NULLS placement is absolute, not value-flipped: ASC defaults to LAST and
    # DESC to FIRST, and an explicit clause overrides either.
    ("SELECT * FROM t ORDER BY w", [4, 1, 5, 3, 2], _ALL),
    ("SELECT * FROM t ORDER BY w DESC", [2, 3, 5, 1, 4], _ALL),
    ("SELECT * FROM t ORDER BY w ASC NULLS FIRST", [2, 4, 1, 5, 3], _ALL),
    ("SELECT * FROM t ORDER BY w DESC NULLS LAST", [3, 5, 1, 4, 2], _ALL),
    ("SELECT * FROM t ORDER BY a, v", [2, 1, 3, 4, 5], _ALL),
    ("SELECT * FROM t ORDER BY a ASC, v DESC", [1, 2, 4, 3, 5], _ALL),
    ("SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1", [2, 3], _ALL),
    ("SELECT * FROM t ORDER BY id LIMIT 1, 2", [2, 3], _ALL),     # MySQL `LIMIT off, lim`
    ("SELECT * FROM t ORDER BY id OFFSET 3", [4, 5], _ALL),
    ("SELECT * FROM t ORDER BY id OFFSET 99", [], _ALL),          # past the end
    ("SELECT * FROM t ORDER BY id LIMIT 0", [], _ALL),
    ("SELECT * FROM t ORDER BY id DESC LIMIT 2", [5, 4], _ALL),
    # A PK-set gather cut by the ORDER BY comparator, not by which keys the
    # gather answered first.
    ("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5) ORDER BY v LIMIT 2", [2, 3], _ALL),
    ("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5) ORDER BY v LIMIT 2 OFFSET 1", [3, 1], _ALL),
    # A key outside the projection still orders the result, by the output alias
    # or by the source name even when it was aliased away.
    ("SELECT id FROM t ORDER BY v", [2, 3, 1, 4, 5], ["id"]),
    ("SELECT id, v AS foo FROM t ORDER BY foo", [2, 3, 1, 4, 5], ["id", "foo"]),
    ("SELECT id, v AS foo FROM t ORDER BY v DESC", [5, 4, 1, 3, 2], ["id", "foo"]),
    # An ORDER BY expression binds over the source like a SELECT item and rides
    # as a hidden column, so it never widens the presented shape.
    ("SELECT id FROM t ORDER BY 0 - v", [5, 4, 1, 3, 2], ["id"]),
    ("SELECT id FROM t ORDER BY v % 20, id DESC LIMIT 2", [4, 3], ["id"]),
])
def test_a_base_table_sort_and_window(client, src, q, want, fields):
    got = rows(client, src, q)
    assert [r.id for r in got] == want
    if got:
        assert list(got[0]._fields) == fields


# ---------------------------------------------------------------------------
# The worker top-k's comparator must be the client window's
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("q,want", [
    # German-string order and absolute NULL placement: DESC defaults to NULLS
    # FIRST, so the NULLs occupy the window; explicit NULLS LAST keeps them out.
    ("SELECT name FROM names ORDER BY name LIMIT 3", ["apple", "apricot", "banana"]),
    ("SELECT name FROM names ORDER BY name DESC LIMIT 3", [None, None, "pear"]),
    ("SELECT name FROM names ORDER BY name ASC NULLS LAST LIMIT 5",
     ["apple", "apricot", "banana", "cherry", "pear"]),
    # Negative, fractional and zero under the same total order on both sides.
    ("SELECT id FROM floats ORDER BY x LIMIT 3", [4, 2, 3]),
    ("SELECT id FROM floats ORDER BY x DESC LIMIT 2", [1, 5]),
])
def test_a_top_k_cuts_where_the_client_would(client, src, q, want):
    assert [r[0] for r in rows(client, src, q)] == want


# ---------------------------------------------------------------------------
# Views: multiplicity-correct cuts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tail,want", [
    # LIMIT 2 = 2 logical rows, so only the smallest value survives; an
    # entry-counting LIMIT would spill into val=20.
    ("ORDER BY val LIMIT 2", {(10,): 2}),
    # LIMIT 3 straddles the boundary, clipping the boundary entry to weight 1.
    ("ORDER BY val LIMIT 3", {(10,): 2, (20,): 1}),
    # A window landing inside the weight-2 group.
    ("ORDER BY val LIMIT 1 OFFSET 1", {(10,): 1}),
    ("ORDER BY val DESC LIMIT 3", {(20,): 2, (10,): 1}),
    ("ORDER BY val", {(10,): 2, (20,): 2}),
])
def test_a_cut_over_a_bag_counts_logical_rows(client, src, tail, want):
    assert bag(rows(client, src, f"SELECT val FROM dup {tail}")) == want


def test_a_grouped_views_hidden_key_is_not_a_positional_column(client, schema_name):
    """A GROUP BY view carries a hidden `_group_pk`, so positional ORDER BY must
    target the first VISIBLE column — and ORDER BY over the aggregate works."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT cat, COUNT(*) AS cnt FROM orders GROUP BY cat; "
        "INSERT INTO orders VALUES (1,10),(2,10),(3,10),(4,20),(5,30),(6,30)",
        schema_name=sn)
    assert [(r.cat, r.cnt) for r in rows(client, sn, "SELECT * FROM v ORDER BY cnt DESC LIMIT 2")] \
        == [(10, 3), (30, 2)]
    assert [r.cat for r in rows(client, sn, "SELECT * FROM v ORDER BY 1")] == [10, 20, 30]
