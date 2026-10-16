"""An aggregate or DISTINCT chained to an inner join: the analytics shape where
one compiled bundle holds a hidden view plus a user-named one.

Two directions:

  * the aggregate is the *inner* body — a CTE — and the join runs over it, so the
    bundle is `reduce → join`;
  * the aggregate is the *outer* body over a join, so the join compiles to a
    hidden view H and the reduce or distinct runs over it, `join → reduce`. The
    group, aggregate and WHERE columns then resolve against H, and H is pruned to
    the names that outer operator evaluates.

Every assertion is a weighted bag: a chain that emitted a group twice, or left a
retracted row behind at weight 0, would pass a row-set comparison. The outer-join
form is test_outer_join.py's.
"""
from operator import itemgetter

import gnitz
from _exprs import declared
from _read import bag, rows, scanned
from _shapes import grouped, groups
from _sql import churn, inserting, values

_JOIN = "FROM orders JOIN customers ON orders.cid = customers.id"

_VIEWS = [
    "cte_then_join AS WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) "
    "SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
    # Both join inputs are reduce outputs, keyed by their group keys.
    "two_ctes AS WITH sa AS (SELECT cid AS k, SUM(amt) AS s FROM orders GROUP BY cid), "
    "sb AS (SELECT id AS k, SUM(amt) AS s FROM customers GROUP BY id) "
    "SELECT sa.s AS so, sb.s AS sc FROM sa JOIN sb ON sa.k = sb.k",
    # Both inputs carry `id` and `amt`, so the bare names are ambiguous and a
    # qualified one resolves through the join's alias map rather than H's names.
    f"by_region AS SELECT region AS reg, SUM(orders.amt) AS total {_JOIN} GROUP BY region",
    f"filtered AS SELECT region AS reg, COUNT(*) AS n, SUM(orders.amt) AS s {_JOIN} "
    "WHERE orders.amt > 40 GROUP BY region",
    f"by_order_id AS SELECT region AS reg, COUNT(*) AS n {_JOIN} WHERE orders.id > 1 GROUP BY region",
    f"global AS SELECT SUM(orders.amt) AS s {_JOIN}",
    # MIN/MAX are non-linear: a new extreme, and a receding one.
    f"extremes AS SELECT region AS reg, MIN(orders.amt) AS lo, MAX(orders.amt) AS hi {_JOIN} GROUP BY region",
    # COUNT(*) names no column, so H keeps its wildcard projection.
    f"counted AS SELECT COUNT(*) AS n {_JOIN}",
    f"regions AS SELECT DISTINCT region AS reg {_JOIN}",
    # H is the whole join tree, so an outer operator reads the far dimension.
    f"star AS SELECT grp, SUM(orders.amt) AS total {_JOIN} JOIN zones ON customers.region = zones.id GROUP BY grp",
    f"star_distinct AS SELECT DISTINCT grp {_JOIN} JOIN zones ON customers.region = zones.id",
]

_CUSTOMERS = {1: ("Alice", 100, 1000), 2: ("Bob", 200, 2000), 3: ("Cy", 300, 3000)}
_ZONES = {100: 7, 200: 7, 300: 8}

_CHURN = [
    (None, {}),
    inserting("orders", {1: (1, 50), 2: (1, 80), 3: (2, 30)}),
    inserting("orders", {4: (1, 20), 5: (3, 60)}),
    ("DELETE FROM orders WHERE id = 2", "orders", {2: None}),
    ("UPDATE customers SET region = 300 WHERE id = 2", "customers", {2: ("Bob", 300, 2000)}),
    # Every carrier of one group at once.
    ("DELETE FROM orders WHERE cid = 1", "orders", {1: None, 4: None}),
]


def test_a_reduce_and_a_join_compose_in_either_order_through_churn(client):
    """A new fact row re-aggregates its group and re-joins it, a group's last row
    retracts the joined row entirely, and a dimension update moves every fact
    joined to it into another group."""
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL); "
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, "
        "region BIGINT NOT NULL, amt BIGINT NOT NULL); "
        "CREATE TABLE zones (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW {v}" for v in _VIEWS) + "; "
        f"INSERT INTO customers VALUES {values((i, *r) for i, r in _CUSTOMERS.items())}; "
        f"INSERT INTO zones VALUES {values(_ZONES.items())}")

    state = {"orders": {}, "customers": dict(_CUSTOMERS)}
    for sql in churn(client, state, _CHURN):
        orders, customers = state["orders"], state["customers"]
        joined = [(oi, amt, customers[cid][1]) for oi, (cid, amt) in orders.items() if cid in customers]
        amt, region = itemgetter(1), itemgetter(2)

        totals = grouped(orders.values(), lambda o: o[0], lambda o: o[1])
        assert bag(scanned(client, "cte_then_join"), "nm", "tot") == {
            (customers[cid][0], t): 1 for cid, t in totals if cid in customers}, sql
        assert bag(scanned(client, "two_ctes"), "so", "sc") == {
            (t, customers[cid][2]): 1 for cid, t in totals if cid in customers}, sql
        assert bag(scanned(client, "by_region"), "reg", "total") == grouped(joined, region, amt), sql
        assert bag(scanned(client, "filtered"), "reg", "n", "s") == grouped(
            [j for j in joined if j[1] > 40], region, lambda j: 1, amt), sql
        assert bag(scanned(client, "by_order_id"), "reg", "n") == grouped(
            [j for j in joined if j[0] > 1], region, lambda j: 1), sql
        assert bag(scanned(client, "global"), "s") == {(sum(j[1] for j in joined) if joined else None,): 1}, sql
        assert bag(scanned(client, "extremes"), "reg", "lo", "hi") == {
            (r, min(map(amt, js)), max(map(amt, js))): 1 for r, js in groups(joined, region).items()}, sql
        assert bag(scanned(client, "counted"), "n") == {(len(joined),): 1}, sql
        assert bag(scanned(client, "regions"), "reg") == {(region(j),): 1 for j in joined}, sql
        assert bag(scanned(client, "star"), "grp", "total") == grouped(
            joined, lambda j: _ZONES[region(j)], amt), sql
        assert bag(scanned(client, "star_distinct"), "grp") == {(_ZONES[region(j)],): 1 for j in joined}, sql


def test_a_reduce_over_a_fan_out_join_keeps_each_group_apart(client):
    """One join-output key spans several groups, so a group's extremes must
    never pull a neighbour's rows when its holder leaves."""
    client.execute_sql(
        "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, fkey BIGINT NOT NULL, g INT NOT NULL, "
        "x BIGINT NOT NULL, y BIGINT NOT NULL); "
        "CREATE VIEW j AS SELECT fact.fid AS fid, dim.g AS g, dim.x AS x, dim.y AS y "
        "FROM fact JOIN dim ON fact.fid = dim.fkey; "
        "CREATE VIEW agg AS SELECT g, MIN(x) AS lo, MAX(y) AS hi, COUNT(*) AS c FROM j GROUP BY g; "
        "INSERT INTO fact VALUES (1), (2); "
        "INSERT INTO dim VALUES (1, 1, 10, 5, 100), (2, 1, 20, 7, 200), (3, 2, 10, 3, 50), "
        "(4, 2, 20, 9, 300)")
    assert bag(scanned(client, "agg"), "g", "lo", "hi", "c") == {
        (10, 3, 100, 2): 1, (20, 7, 300, 2): 1}
    # g=10's MIN holder goes: it recomputes to 5, never to g=20's smaller entry.
    client.execute_sql("DELETE FROM dim WHERE did = 3")
    assert bag(scanned(client, "agg"), "g", "lo", "hi", "c") == {
        (10, 5, 100, 1): 1, (20, 7, 300, 2): 1}


def test_a_computed_group_input_is_declared_as_the_value_it_computes(client):
    """A grouped view declares a column for each computed group key and aggregate
    argument, and the declared type must be the value the expression computes: a
    negation of a narrow integer lives in a 64-bit register (`-(-2^31)` leaves
    INT, `-q` over an unsigned column goes negative), a cast to REAL computes a
    double, and a narrowing cast is its target. Each view equals its ad-hoc
    SELECT, which reads the register directly."""
    client.execute_sql(
        "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, q INT UNSIGNED NOT NULL, "
        "i INT NOT NULL, x BIGINT NOT NULL)")
    bodies = {
        "neg_sum": ("SELECT g, SUM(-q) AS s FROM n GROUP BY g", ("g", "s")),
        "neg_key": ("SELECT -i AS k, COUNT(*) AS c FROM n GROUP BY -i", ("k", "c")),
        "real_sum": ("SELECT g, SUM(CAST(x AS REAL)) AS s FROM n GROUP BY g", ("g", "s")),
        "narrow_key": ("SELECT CAST(x AS SMALLINT) AS k, COUNT(*) AS c FROM n GROUP BY CAST(x AS SMALLINT)",
                       ("k", "c")),
    }
    for view, (body, _) in bodies.items():
        client.execute_sql(f"CREATE VIEW {view} AS {body}")

    ty = gnitz.TypeCode
    for view, col, want in (("neg_sum", "s", ty.I64), ("neg_key", "k", ty.I64), ("real_sum", "s", ty.F64),
                            ("narrow_key", "k", ty.I16)):
        assert declared(client, view)[col] == (want, 0), view

    def check(expected):
        for view, (body, cols) in bodies.items():
            got = bag(scanned(client, view), *cols)
            assert got == bag(rows(client, body), *cols), view
            assert got == expected[view], view

    client.execute_sql(
        "INSERT INTO n VALUES (1, 1, 4000000000, -2147483648, 5), (2, 1, 3, 7, 70000), "
        "(3, 2, 10, -2147483648, -3)")
    check({
        "neg_sum": {(1, -4000000003): 1, (2, -10): 1},
        "neg_key": {(2147483648, 2): 1, (-7, 1): 1},
        "real_sum": {(1, 70005.0): 1, (2, -3.0): 1},
        "narrow_key": {(5, 1): 1, (None, 1): 1, (-3, 1): 1},
    })
    client.execute_sql("DELETE FROM n WHERE id = 1")
    check({
        "neg_sum": {(1, -3): 1, (2, -10): 1},
        "neg_key": {(2147483648, 1): 1, (-7, 1): 1},
        "real_sum": {(1, 70000.0): 1, (2, -3.0): 1},
        "narrow_key": {(None, 1): 1, (-3, 1): 1},
    })
