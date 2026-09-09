"""The ORDER BY / OFFSET / LIMIT read bound, over a base table and over a view.

Two properties carry the file. The sink sorts BEFORE projection, so an ORDER BY
key absent from the SELECT list still resolves; and it counts LIMIT/OFFSET by
logical **multiplicity** — summed weight — not by Z-set entries, so a cut lands
in the same place however a bag is split across workers and entries.

Under a LIMIT the worker top-k discards rows before the client ever sees them,
so the two comparators must agree exactly: a divergence drops rows rather than
merely reordering them. That is why the TEXT-with-NULLs and float cases pin
values and not just an order.
"""

import gnitz
import pytest
from _read import bag, rows


def _t(client, sn, values, cols="id BIGINT NOT NULL PRIMARY KEY, v BIGINT"):
    client.execute_sql(f"CREATE TABLE t ({cols})", schema_name=sn)
    vals = ",".join("(" + ",".join("NULL" if x is None else str(x) for x in r) + ")"
                    for r in values)
    client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)


# ---------------------------------------------------------------------------
# Base-table ordering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tail,want", [
    ("ORDER BY v", [2, 3, 1]),
    ("ORDER BY v DESC", [1, 3, 2]),
    # NULLS placement is absolute, not value-flipped: ASC defaults to LAST and
    # DESC to FIRST, and an explicit clause overrides either.
    ("ORDER BY w", [1, 3, 2]),
    ("ORDER BY w DESC", [2, 3, 1]),
    ("ORDER BY w ASC NULLS FIRST", [2, 1, 3]),
    ("ORDER BY w DESC NULLS LAST", [3, 1, 2]),
])
def test_order_by_a_single_key(client, schema_name, tail, want):
    sn = schema_name
    _t(client, sn, [(1, 30, 10), (2, 10, None), (3, 20, 20)],
       cols="id BIGINT NOT NULL PRIMARY KEY, v BIGINT, w BIGINT")
    assert [r.id for r in rows(client, sn, f"SELECT * FROM t {tail}")] == want


@pytest.mark.parametrize("tail,want", [
    ("ORDER BY a, b", [2, 1, 3]),
    ("ORDER BY a ASC, b DESC", [1, 2, 3]),
])
def test_order_by_multiple_keys(client, schema_name, tail, want):
    sn = schema_name
    _t(client, sn, [(1, 1, 30), (2, 1, 10), (3, 2, 5)],
       cols="id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL")
    assert [r.id for r in rows(client, sn, f"SELECT * FROM t {tail}")] == want


@pytest.mark.parametrize("tail,want", [
    ("ORDER BY id LIMIT 2 OFFSET 1", [2, 3]),
    ("ORDER BY id LIMIT 1, 2", [2, 3]),               # MySQL `LIMIT off, lim`
    ("ORDER BY id OFFSET 3", [4, 5]),
    ("ORDER BY id OFFSET 99", []),                    # past the end
    ("ORDER BY id LIMIT 0", []),
    ("ORDER BY id DESC LIMIT 2", [5, 4]),
])
def test_the_offset_limit_window(client, schema_name, tail, want):
    sn = schema_name
    _t(client, sn, [(i, i * 10) for i in range(1, 6)])
    assert [r.id for r in rows(client, sn, f"SELECT * FROM t {tail}")] == want


@pytest.mark.parametrize("tail,match", [
    ("LIMIT 1 BY v", None),
    # A non-literal LIMIT/OFFSET is a clean error, never a silent degrade:
    # `LIMIT 1+1` must not return every row, `OFFSET 1+1` must not skip 0.
    ("LIMIT 1+1", "LIMIT"),
    ("LIMIT 1 OFFSET 1+1", "OFFSET"),
])
def test_an_unsupported_cut_is_refused(client, schema_name, tail, match):
    sn = schema_name
    _t(client, sn, [(1, 10), (2, 20), (3, 30)])
    with pytest.raises(gnitz.GnitzError, match=match):
        client.execute_sql(f"SELECT * FROM t {tail}", schema_name=sn)


# ---------------------------------------------------------------------------
# The worker top-k's comparator must be the client window's
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tail,want", [
    ("ORDER BY name LIMIT 3", ["apple", "apricot", "banana"]),
    # DESC defaults to NULLS FIRST, so the NULLs occupy the window.
    ("ORDER BY name DESC LIMIT 3", [None, None, "pear"]),
    # Explicit NULLS LAST on ASC keeps them out of it entirely.
    ("ORDER BY name ASC NULLS LAST LIMIT 5",
     ["apple", "apricot", "banana", "cherry", "pear"]),
])
def test_a_text_key_with_nulls_cuts_where_the_client_would(client, schema_name, tail, want):
    """German-string order and absolute NULL placement, across workers."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'pear'), (2, 'apple'), (3, NULL), (4, 'banana'), "
        "(5, 'apricot'), (6, NULL), (7, 'cherry')", schema_name=sn)
    assert [r.name for r in rows(client, sn, f"SELECT name FROM t {tail}")] == want


@pytest.mark.parametrize("tail,want", [
    ("ORDER BY x LIMIT 3", [4, 2, 3]),
    ("ORDER BY x DESC LIMIT 2", [1, 5]),
])
def test_a_float_key_cuts_where_the_client_would(client, schema_name, tail, want):
    """Negative, fractional and zero under the same total order on both sides."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, x DOUBLE)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 2.5), (2, -1.75), (3, 0.0), (4, -3.25), (5, 1.25)",
        schema_name=sn)
    assert [r.id for r in rows(client, sn, f"SELECT id FROM t {tail}")] == want


def test_a_pk_set_top_n_selects_by_sort_order_not_key_order(client, schema_name):
    """`WHERE pk IN (…) ORDER BY v LIMIT n` returns the true n smallest by v, not
    the first n keys the gather answered — proving the worker top-k selects under
    the ORDER BY comparator rather than keeping a gather-order prefix. The
    smallest v deliberately sit at the largest pks."""
    sn = schema_name
    _t(client, sn, [(1, 50), (2, 40), (3, 30), (4, 20), (5, 10)])
    q = "SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5) ORDER BY v"
    assert [(r.id, r.v) for r in rows(client, sn, f"{q} LIMIT 2")] == [(5, 10), (4, 20)]
    assert [(r.id, r.v) for r in rows(client, sn, f"{q} LIMIT 2 OFFSET 1")] == [(4, 20), (3, 30)]


# ---------------------------------------------------------------------------
# Sort before projection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("q,want,fields", [
    # `v` is not in the projection but still orders the result.
    ("SELECT id FROM t ORDER BY v", [2, 3, 1], ["id"]),
    # By the output alias, and by the source name even when it was aliased away.
    ("SELECT id, v AS foo FROM t ORDER BY foo", [2, 3, 1], ["id", "foo"]),
    ("SELECT id, v AS foo FROM t ORDER BY v DESC", [1, 3, 2], ["id", "foo"]),
    # An ORDER BY expression binds over the source like a SELECT item and rides
    # as a hidden column, so it never widens the presented shape.
    ("SELECT id FROM t ORDER BY 0 - v", [1, 3, 2], ["id"]),
    ("SELECT id FROM t ORDER BY v % 20, id DESC LIMIT 2", [3, 2], ["id"]),
])
def test_an_order_by_key_outside_the_projection_still_resolves(client, schema_name, q, want, fields):
    sn = schema_name
    _t(client, sn, [(1, 30), (2, 10), (3, 20)])
    got = rows(client, sn, q)
    assert [r.id for r in got] == want
    assert list(got[0]._fields) == fields


# ---------------------------------------------------------------------------
# Views: multiplicity-correct cuts
# ---------------------------------------------------------------------------


@pytest.fixture
def dup(client, schema_name):
    """A `UNION ALL` view of two identical tables, so every row carries logical
    multiplicity 2 — the shape that separates a weight-counting LIMIT from an
    entry-counting one."""
    sn = schema_name
    for name in ("a", "b"):
        client.execute_sql(
            f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            f"INSERT INTO {name} VALUES (1, 10), (2, 20)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b", schema_name=sn)
    return sn


@pytest.mark.parametrize("tail,want", [
    # LIMIT 2 = 2 logical rows, so only the smallest value survives; an
    # entry-counting LIMIT would spill into val=20.
    ("ORDER BY val LIMIT 2", {10: 2}),
    # LIMIT 3 straddles the boundary, clipping the boundary entry to weight 1.
    ("ORDER BY val LIMIT 3", {10: 2, 20: 1}),
    # A window landing inside the weight-2 group.
    ("ORDER BY val LIMIT 1 OFFSET 1", {10: 1}),
    ("ORDER BY val DESC LIMIT 3", {20: 2, 10: 1}),
    ("ORDER BY val", {10: 2, 20: 2}),
])
def test_a_cut_over_a_bag_counts_logical_rows(client, dup, tail, want):
    got = bag(rows(client, dup, f"SELECT val FROM v {tail}"))
    assert {k[0]: w for k, w in got.items()} == want


def test_a_grouped_views_hidden_key_is_not_a_positional_column(client, schema_name):
    """A GROUP BY view carries a hidden `_group_pk`, so positional ORDER BY must
    target the first VISIBLE column — and ORDER BY over the aggregate works."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT cat, COUNT(*) AS cnt FROM orders GROUP BY cat",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1,10),(2,10),(3,10),(4,20),(5,30),(6,30)",
        schema_name=sn)
    assert [(r.cat, r.cnt) for r in rows(client, sn, "SELECT * FROM v ORDER BY cnt DESC LIMIT 2")] \
        == [(10, 3), (30, 2)]
    assert [r.cat for r in rows(client, sn, "SELECT * FROM v ORDER BY 1")] == [10, 20, 30]


def test_a_join_views_cut_is_a_function_of_the_data_alone(client, schema_name):
    """ORDER BY + LIMIT over a fan-out join view (non-unique join key) returns a
    summed-weight bag that is the same at any worker count — run the suite at
    W=1 and W=4."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "label BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT fact.amt AS amt, dim.label AS label "
        "FROM fact JOIN dim ON fact.k = dim.k", schema_name=sn)
    # Two facts share join key k=1 with one dim row; one fact with k=2 joins the
    # other, so amt=10 arrives at multiplicity 2.
    client.execute_sql("INSERT INTO fact VALUES (1,1,10),(2,1,10),(3,2,30)", schema_name=sn)
    client.execute_sql("INSERT INTO dim VALUES (1,1,100),(2,2,200)", schema_name=sn)

    assert bag(rows(client, sn, "SELECT * FROM v ORDER BY amt")) == \
        {(10, 100): 2, (30, 200): 1}
    assert bag(rows(client, sn, "SELECT * FROM v ORDER BY amt LIMIT 2")) == \
        {(10, 100): 2}
