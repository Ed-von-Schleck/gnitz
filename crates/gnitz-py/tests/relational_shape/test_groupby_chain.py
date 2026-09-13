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
from collections import Counter

from _read import bag, scanned

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

_CHURN = [
    ("INSERT INTO customers VALUES (1, 'Alice', 100, 1000), (2, 'Bob', 200, 2000), "
     "(3, 'Cy', 300, 3000); INSERT INTO zones VALUES (100, 7), (200, 7), (300, 8)", "orders", {}),
    ("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)",
     "orders", {1: (1, 50), 2: (1, 80), 3: (2, 30)}),
    ("INSERT INTO orders VALUES (4, 1, 20), (5, 3, 60)", "orders", {4: (1, 20), 5: (3, 60)}),
    ("DELETE FROM orders WHERE id = 2", "orders", {2: None}),
    ("UPDATE customers SET region = 300 WHERE id = 2", "customers", {2: ("Bob", 300, 2000)}),
    # Every carrier of one group at once.
    ("DELETE FROM orders WHERE cid = 1", "orders", {1: None, 4: None}),
]


def test_a_reduce_and_a_join_compose_in_either_order_through_churn(client, schema_name):
    """A new fact row re-aggregates its group and re-joins it, a group's last row
    retracts the joined row entirely, and a dimension update moves every fact
    joined to it into another group."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL); "
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, "
        "region BIGINT NOT NULL, amt BIGINT NOT NULL); "
        "CREATE TABLE zones (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW {v}" for v in _VIEWS), schema_name=sn)
    zones = {100: 7, 200: 7, 300: 8}

    state = {"orders": {}, "customers": {1: ("Alice", 100, 1000), 2: ("Bob", 200, 2000),
                                         3: ("Cy", 300, 3000)}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for i, row in changes.items():
            if row is None:
                del state[table][i]
            else:
                state[table][i] = row
        orders, customers = state["orders"], state["customers"]
        joined = [(oi, amt, customers[cid]) for oi, (cid, amt) in orders.items() if cid in customers]

        def grouped(rows, key, fold):
            out = {}
            for r in rows:
                out.setdefault(key(r), []).append(r)
            return {(k, *fold(rs)): 1 for k, rs in out.items()}

        totals = grouped(orders.items(), lambda o: o[1][0], lambda rs: (sum(r[1][1] for r in rs),))
        assert bag(scanned(client, sn, "cte_then_join"), "nm", "tot") == {
            (customers[cid][0], t): 1 for cid, t in totals if cid in customers}, sql
        assert bag(scanned(client, sn, "two_ctes"), "so", "sc") == {
            (t, customers[cid][2]): 1 for cid, t in totals if cid in customers}, sql
        assert bag(scanned(client, sn, "by_region"), "reg", "total") == grouped(
            joined, lambda j: j[2][1], lambda rs: (sum(r[1] for r in rs),)), sql
        assert bag(scanned(client, sn, "filtered"), "reg", "n", "s") == grouped(
            [j for j in joined if j[1] > 40], lambda j: j[2][1], lambda rs: (len(rs), sum(r[1] for r in rs))), sql
        assert bag(scanned(client, sn, "by_order_id"), "reg", "n") == grouped(
            [j for j in joined if j[0] > 1], lambda j: j[2][1], lambda rs: (len(rs),)), sql
        assert bag(scanned(client, sn, "global"), "s") == {(sum(j[1] for j in joined) if joined else None,): 1}, sql
        assert bag(scanned(client, sn, "extremes"), "reg", "lo", "hi") == grouped(
            joined, lambda j: j[2][1], lambda rs: (min(r[1] for r in rs), max(r[1] for r in rs))), sql
        assert bag(scanned(client, sn, "counted"), "n") == {(len(joined),): 1}, sql
        assert bag(scanned(client, sn, "regions"), "reg") == {(j[2][1],): 1 for j in joined}, sql
        assert bag(scanned(client, sn, "star"), "grp", "total") == grouped(
            joined, lambda j: zones[j[2][1]], lambda rs: (sum(r[1] for r in rs),)), sql
        assert bag(scanned(client, sn, "star_distinct"), "grp") == Counter(
            {(zones[j[2][1]],): 1 for j in joined}), sql
