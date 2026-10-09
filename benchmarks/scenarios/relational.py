"""Scenarios that vary the compiled body: join shapes, subqueries, set
operations, aggregates and the per-row programs under them, ordered state."""

from __future__ import annotations

from .base import BYTES, WORDS, Fact, Shape, Zipf, scatter

FLOAT = "a float sum depends on the order its rows are folded in"
NATIONS = ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA",
           "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "PERU", "CHINA", "ROMANIA",
           "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES", "MEXICO"]
SHIPMODES = ["AIR", "SHIP", "MAIL", "RAIL", "TRUCK"]

CUSTOMERS = ("CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL, "
             "tier INT NOT NULL)")
ORDERS = ("CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, "
          "amount BIGINT NOT NULL, status TEXT NOT NULL)")
FANOUT_VIEWS = [
    "CREATE VIEW v_named AS SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
    "CREATE VIEW v_region AS SELECT o.id, o.status, c.region FROM orders o JOIN customers c ON o.customer_id = c.id",
    "CREATE VIEW v_tier AS SELECT o.id, o.amount, c.tier FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
    "CREATE VIEW v_region_total AS SELECT c.region, COUNT(*) AS n, SUM(o.amount) AS total FROM orders o "
    "JOIN customers c ON o.customer_id = c.id GROUP BY c.region",
    "CREATE VIEW v_cust_ext AS SELECT customer_id, MIN(amount) AS lo, MAX(amount) AS hi FROM orders "
    "GROUP BY customer_id",
    "CREATE VIEW v_cust_sum AS SELECT customer_id, COUNT(*) AS n, SUM(amount) AS total FROM orders "
    "GROUP BY customer_id",
]


def orders(key_range):
    """An order's customer is one of `key_range` times the customers loaded: of
    1.2, a sixth of the orders match nothing."""
    def data(n, rng):
        customers = max(n // 20, 1)
        regions = ["emea-north", "emea-south", "apac", "americas-east", "americas-west"]
        statuses = ["open", "paid", "shipped", "returned_by_customer"]
        return [], {
            "customers": Fact(lambda c: dict(
                id=c + 1, name=f"Customer {rng.choice(WORDS)} {rng.choice(WORDS)} {c:06d} GmbH",
                region=rng.choice(regions), tier=rng.randrange(5)), share=1 / 20),
            "orders": Fact(lambda k: dict(
                id=k + 1, customer_id=scatter(k, 40) % int(customers * key_range) + 1,
                amount=rng.randrange(100_000), status=rng.choice(statuses))),
        }
    return data


def visits(n, rng):
    users = max(n // 40, 1)
    dims = [("banned", (dict(user_id=u) for u in rng.sample(range(users), users // 10)))]
    return dims, {"visits": Fact(lambda k: dict(
        id=k + 1, user_id=rng.randrange(users), page=rng.randrange(200), day=19_000 + k * 365 // n,
        ms=rng.randrange(60_000)))}


def star(n, rng):
    """`a` rows draw their key Zipfian from a `b` a tenth their number, keyed
    `k == pk` so each `a` row matches one `b` row."""
    hot = Zipf(max(n // 10, 1))
    return [], {
        "a": Fact(lambda k: dict(pk=k + 1, k=hot(rng), av=rng.randrange(1000))),
        "b": Fact(lambda k: dict(pk=k + 1, k=k + 1, bv=rng.randrange(1000)), share=0.1),
    }


def multiway(n, rng):
    dim = max(n // 10, 1)
    hot, half = Zipf(dim), max(dim // 2, 1)
    hot_half = Zipf(half)
    _, facts = star(n, rng)
    facts.update({
        "d": Fact(lambda k: dict(pk=k + 1, k=k + 1, dv=rng.randrange(1000)), share=0.1),
        # Managers are the first `dim` employees.
        "emp": Fact(lambda k: dict(id=k + 1, mgr=hot(rng), sal=rng.randrange(100_000)), share=0.5),
        "ca": Fact(lambda k: dict(pk=k + 1, x=hot_half(rng), y=rng.randrange(2), av=rng.randrange(1000)), share=0.5),
        "cb": Fact(lambda k: dict(pk=k + 1, x=k % half + 1, y=k // half % 2, bv=rng.randrange(1000)), share=0.05),
    })
    return [], facts


RANGE_DIM = 64      # a row of `ra` matches half of `rb`, so the view is 32 times `ra`


def ranges(n, rng):
    dim = max(n // 10, 1)
    hot = Zipf(dim)
    dims = [("rb", (dict(pk=i + 1, y=i + 1) for i in range(RANGE_DIM))),
            ("bb", (dict(pk=i + 1, k=i + 1, t=500) for i in range(dim)))]
    return dims, {
        "ra": Fact(lambda k: dict(pk=k + 1, x=rng.randrange(RANGE_DIM + 1))),
        "ba": Fact(lambda k: dict(pk=k + 1, k=hot(rng), lo=rng.randrange(1001))),
    }


def parents(n, rng):
    hot = Zipf(max(n // 10, 1))
    return [], {
        "p": Fact(lambda k: dict(id=k + 1, region=rng.randrange(5)), share=0.1),
        "ch": Fact(lambda k: dict(id=k + 1, pid=hot(rng), v=rng.randrange(1000))),
    }


def overlap(n, rng):
    """`t2` holds a tenth of `t1`'s keys, with the same value for each: the two
    agree on every row they share."""
    def row(k):
        return dict(pk=k + 1, val=scatter(k, 32) % 500)
    return [], {"t1": Fact(row), "t2": Fact(row, share=0.1)}


def measures(n, rng):
    def row(k):
        return dict(
            pk=k + 1, a=rng.randrange(21), b=None if rng.random() < 0.1 else rng.randrange(1001),
            g1=rng.randrange(51), g2=rng.randrange(51), g=rng.randrange(101), v=rng.randrange(1_000_001),
            name=rng.choice(NATIONS), mode=rng.choice(SHIPMODES),
            note=None if rng.random() < 0.3 else f"see ticket {rng.randrange(10**5)} for {rng.choice(WORDS)}",
            d=rng.randrange(301), price=rng.randrange(100, 100_001), disc=rng.randrange(101) / 1000)
    return [], {"e": Fact(row)}


def ledger(n, rng):
    accounts = max(n // 100, 1)
    return [], {"tx": Fact(lambda k: dict(
        id=k + 1, acct=rng.randrange(accounts), ts=k * 7 + rng.randrange(7), amount=rng.randrange(1, 10**6)))}


SCENARIOS = [
    Shape(
        "join", "join",
        "an inner and a left join of an order table to a customer table a twentieth its size",
        BYTES,
        [CUSTOMERS, ORDERS,
         "CREATE VIEW v_inner AS SELECT o.id, o.amount, o.status, c.name, c.region "
         "FROM orders o JOIN customers c ON o.customer_id = c.id",
         "CREATE VIEW v_left AS SELECT o.id, o.amount, c.name "
         "FROM orders o LEFT JOIN customers c ON o.customer_id = c.id"],
        orders(1.2)),
    Shape(
        "fanout", "join",
        "six views and an index that each read an order table by its customer: three joins, a join under an "
        "aggregate, a MIN/MAX and a SUM per customer",
        BYTES,
        [CUSTOMERS, ORDERS, "CREATE INDEX ON orders(customer_id)", *FANOUT_VIEWS], orders(1)),
    Shape(
        "fanout_clustered", "join",
        "the fanout scenario over an order table keyed and clustered by its customer, whose own store is "
        "then the order the views read it in",
        BYTES,
        [CUSTOMERS,
         "CREATE TABLE orders (customer_id BIGINT NOT NULL, id BIGINT NOT NULL, amount BIGINT NOT NULL, "
         "status TEXT NOT NULL, PRIMARY KEY (customer_id, id)) CLUSTER BY customer_id",
         *FANOUT_VIEWS],
        orders(1)),
    Shape(
        "outer_joins", "join",
        "a right and a full join and an inner join under a residual predicate, of rows whose keys are "
        "Zipfian over a table a tenth their number",
        ("l0", "compacted"),
        ["CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
         "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
         "CREATE VIEW v_right AS SELECT a.k AS k, a.av AS av, b.bv AS bv FROM a RIGHT JOIN b ON a.k = b.k",
         "CREATE VIEW v_full AS SELECT a.k AS k, a.av AS av, b.bv AS bv FROM a FULL JOIN b ON a.k = b.k",
         "CREATE VIEW v_resid AS SELECT a.k AS k, a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k AND a.av <> b.bv"],
        star),
    Shape(
        "multiway", "join",
        "a three-way join through a CTE, a self join, and a join on a two-column key",
        ("l0", "compacted"),
        ["CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
         "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
         "CREATE TABLE d (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, dv BIGINT NOT NULL)",
         "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, sal BIGINT NOT NULL)",
         "CREATE TABLE ca (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL)",
         "CREATE TABLE cb (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, bv BIGINT NOT NULL)",
         "CREATE VIEW v_mw3 AS WITH h0 AS (SELECT a.k AS k, a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k) "
         "SELECT h0.k AS k, h0.av AS av, h0.bv AS bv, d.dv AS dv FROM h0 JOIN d ON h0.k = d.k",
         "CREATE VIEW v_self AS SELECT e.id AS id, e.sal AS esal, m.sal AS msal FROM emp e JOIN emp m ON e.mgr = m.id",
         "CREATE VIEW v_ckey AS SELECT ca.av AS av, cb.bv AS bv FROM ca JOIN cb ON ca.x = cb.x AND ca.y = cb.y"],
        multiway),
    Shape(
        "range_joins", "join",
        f"a band join, and an inner and a left join on an inequality alone against {RANGE_DIM} rows, whose "
        "views are 32 times their input",
        ("l0", "compacted"),
        ["CREATE TABLE ra (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
         "CREATE TABLE rb (pk BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
         "CREATE TABLE ba (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
         "CREATE TABLE bb (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
         "CREATE VIEW v_band AS SELECT ba.k AS k, ba.lo AS lo, bb.t AS t FROM ba JOIN bb ON ba.k = bb.k AND ba.lo <= bb.t",
         "CREATE VIEW v_ri AS SELECT ra.x AS x, rb.y AS y FROM ra JOIN rb ON ra.x < rb.y",
         "CREATE VIEW v_rl AS SELECT ra.x AS x, rb.y AS y FROM ra LEFT JOIN rb ON ra.x < rb.y"],
        ranges, fixed_rows=30_000),
    Shape(
        "subqueries", "relational",
        "nine subquery shapes over one parent and one child table: scalar in the projection and in the "
        "filter, correlated and not, EXISTS alone and under a GROUP BY, IN, ANY, a derived table and a CTE",
        ("l0", "compacted"),
        ["CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)",
         "CREATE TABLE ch (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL, v BIGINT NOT NULL)",
         "CREATE VIEW v_scalar_proj AS SELECT p.id AS id, (SELECT COUNT(*) FROM ch WHERE ch.pid = p.id) AS cnt FROM p",
         "CREATE VIEW v_scalar_corr AS SELECT p.id AS id FROM p "
         "WHERE (SELECT COUNT(*) FROM ch WHERE ch.pid = p.id) >= 2",
         "CREATE VIEW v_scalar_uncorr AS SELECT o.id AS id FROM ch o WHERE o.v < (SELECT MAX(ci.v) FROM ch ci)",
         "CREATE VIEW v_exists AS SELECT p.id AS id, p.region AS region FROM p "
         "WHERE EXISTS (SELECT 1 FROM ch WHERE ch.pid = p.id)",
         "CREATE VIEW v_exists_group AS SELECT region, COUNT(*) AS cnt FROM v_exists GROUP BY region",
         "CREATE VIEW v_in AS SELECT p.id AS id FROM p WHERE p.id IN (SELECT ch.pid FROM ch)",
         "CREATE VIEW v_any AS SELECT o.id AS id FROM ch o WHERE o.v < ANY (SELECT ci.v FROM ch ci)",
         "CREATE VIEW v_derived AS SELECT d.id AS id, ch.v AS v FROM "
         "(SELECT id, region FROM p WHERE region > 0) d JOIN ch ON d.id = ch.pid",
         "CREATE VIEW v_cte AS WITH big AS (SELECT id, region FROM p WHERE region > 0) "
         "SELECT big.id AS id, ch.v AS v FROM big JOIN ch ON big.id = ch.pid"],
        parents),
    Shape(
        "setops", "relational",
        "the six set operators over two tables that agree on the tenth of the keys they share",
        ("l0", "compacted"),
        ["CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
         "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
         *(f"CREATE VIEW v_{name} AS SELECT * FROM t1 {op} SELECT * FROM t2" for name, op in (
             ("union_all", "UNION ALL"), ("union", "UNION"), ("intersect", "INTERSECT"),
             ("intersect_all", "INTERSECT ALL"), ("except", "EXCEPT"), ("except_all", "EXCEPT ALL")))],
        overlap),
    Shape(
        "aggregates", "relational",
        "one table under every aggregate and per-row shape: global, two-key and TEXT-keyed groups, a sum of an "
        "expression, CASE, COALESCE and NULLIF, mixed arithmetic, and filters on a string, a list, a float "
        "range and a NULL",
        ("l0", "compacted"),
        ["CREATE TABLE e (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT, g1 BIGINT NOT NULL, "
         "g2 BIGINT NOT NULL, g BIGINT NOT NULL, v BIGINT NOT NULL, name TEXT NOT NULL, mode TEXT NOT NULL, "
         "note TEXT, d BIGINT NOT NULL, price BIGINT NOT NULL, disc DOUBLE NOT NULL)",
         "CREATE VIEW v_global AS SELECT SUM(v) AS s, COUNT(*) AS n, MIN(v) AS mn, MAX(v) AS mx FROM e",
         "CREATE VIEW v_two_keys AS SELECT g1, g2, SUM(v) AS s FROM e GROUP BY g1, g2",
         "CREATE VIEW v_by_name AS SELECT name, SUM(v) AS s, COUNT(*) AS n FROM e GROUP BY name",
         "CREATE VIEW v_by_name_mode AS SELECT name, mode, SUM(v) AS s FROM e GROUP BY name, mode",
         "CREATE VIEW v_net AS SELECT pk AS id, g, price * (1 - disc) AS net FROM e",
         "CREATE VIEW v_net_sum AS SELECT g, SUM(net) AS rev FROM v_net GROUP BY g",
         "CREATE VIEW v_case AS SELECT pk AS id, CASE WHEN a > 10 THEN 1 WHEN a > 5 THEN 2 ELSE 0 END AS hi, "
         "CASE a WHEN 1 THEN 111 WHEN 2 THEN 222 ELSE 999 END AS m FROM e",
         "CREATE VIEW v_nulls AS SELECT pk AS id, COALESCE(b, 0) AS bz, NULLIF(a, 0) AS an FROM e",
         "CREATE VIEW v_germany AS SELECT pk AS id, v FROM e WHERE name = 'GERMANY'",
         "CREATE VIEW v_modes AS SELECT pk AS id, v FROM e WHERE mode IN ('AIR', 'SHIP', 'MAIL')",
         "CREATE VIEW v_band AS SELECT pk AS id FROM e WHERE d BETWEEN 100 AND 200 AND disc BETWEEN 0.05 AND 0.07",
         "CREATE VIEW v_noted AS SELECT pk AS id, v, note FROM e WHERE note IS NOT NULL"],
        measures, unverified={"v_net_sum": FLOAT}),
    Shape(
        "operators", "relational",
        "a DISTINCT and an EXCEPT, whose leaves are keyed on a hash of the row, and a per-group top-N",
        BYTES,
        ["CREATE TABLE visits (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT NOT NULL, page INT NOT NULL, "
         "day INT NOT NULL, ms INT NOT NULL)",
         "CREATE TABLE banned (user_id BIGINT NOT NULL PRIMARY KEY)",
         "CREATE VIEW v_pairs AS SELECT DISTINCT user_id, page FROM visits",
         "CREATE VIEW v_clean AS SELECT user_id FROM visits EXCEPT SELECT user_id FROM banned",
         "CREATE VIEW v_latest AS SELECT id, user_id, page, day FROM visits "
         "QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY day DESC) <= 3"],
        visits),
    Shape(
        "windows", "relational",
        "state kept in order: a running sum and a rank per partition, the latest three rows of each, a global "
        "top hundred, each group's share of the whole, and the ten largest groups",
        ("l0", "compacted"),
        ["CREATE TABLE tx (id BIGINT NOT NULL PRIMARY KEY, acct BIGINT NOT NULL, ts BIGINT NOT NULL, "
         "amount BIGINT NOT NULL)",
         "CREATE VIEW v_running AS SELECT id, acct, ts, SUM(amount) OVER (PARTITION BY acct ORDER BY ts, id) "
         "AS balance FROM tx",
         "CREATE VIEW v_rank AS SELECT id, acct, RANK() OVER (PARTITION BY acct ORDER BY amount DESC) AS r FROM tx",
         "CREATE VIEW v_latest AS SELECT id, acct, ts, amount FROM tx "
         "QUALIFY ROW_NUMBER() OVER (PARTITION BY acct ORDER BY ts DESC) <= 3",
         "CREATE VIEW v_top AS SELECT id, amount FROM tx ORDER BY amount DESC LIMIT 100",
         "CREATE VIEW v_share AS SELECT acct, SUM(amount) AS total, "
         "SUM(amount) * 100 / SUM(SUM(amount)) OVER () AS pct FROM tx GROUP BY acct",
         "CREATE VIEW v_leaders AS SELECT acct, SUM(amount) AS total FROM tx GROUP BY acct "
         "ORDER BY total DESC LIMIT 10"],
        ledger),
]
