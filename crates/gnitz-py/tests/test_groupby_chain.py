"""E2E tests: a GROUP BY / aggregate subquery compiles into a hidden reduce view
chained to the final view — the aggregate-then-join analytics pattern.

`WITH agg AS (SELECT cid, SUM(amt) FROM orders GROUP BY cid) SELECT c.name, agg.total
FROM agg JOIN customers c ON agg.cid = c.id` becomes an atomic bundle: a hidden reduce
view over `orders` (group key = natural PK, so no synthetic-PK machinery) feeding the
user-named join. The aggregate may be the CTE/derived body or the final view.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_groupby_chain.py -v --tb=short
"""
import random

import pytest


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, view, keys):
    """Positive-weight rows as key tuples, NULL-safe sorted (NULLs last — outer joins)."""
    vid = client.resolve_table(sn, view)[0]
    rows = [tuple(r._asdict()[k] for k in keys) for r in client.scan(vid) if r.weight > 0]
    return sorted(rows, key=lambda t: tuple((x is None, x) for x in t))


def _orders_customers(client, sn):
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)", schema_name=sn
    )


class TestGroupByChain:
    def test_aggregate_cte_joined_incremental(self, client):
        """A SUM-per-group CTE joined with a dimension table, maintained incrementally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _orders_customers(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) "
                "SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200)", schema_name=sn)
            # agg: cid1 -> 150, cid2 -> 200.
            assert _rows(client, sn, "v", ["nm", "tot"]) == [("Alice", 150), ("Bob", 200)]

            # New order for Alice re-aggregates and re-joins.
            client.execute_sql("INSERT INTO orders VALUES (4, 1, 25)", schema_name=sn)
            assert _rows(client, sn, "v", ["nm", "tot"]) == [("Alice", 175), ("Bob", 200)]

            # Retract an order.
            client.execute_sql("DELETE FROM orders WHERE id = 3", schema_name=sn)
            assert _rows(client, sn, "v", ["nm", "tot"]) == [("Alice", 175)]
        finally:
            _cleanup(client, sn)

    def test_groupby_final_over_filter_cte(self, client):
        """A filter CTE feeding a GROUP BY final (reduce over a hidden filter view)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS WITH big AS (SELECT id, cid, amt FROM orders WHERE amt > 50) "
                "SELECT cid, COUNT(*) AS n FROM big GROUP BY cid",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 30), (3, 2, 200), (4, 2, 60)", schema_name=sn
            )
            # big keeps o1 (cid1), o3 (cid2), o4 (cid2); count by cid -> cid1:1, cid2:2.
            assert _rows(client, sn, "v", ["cid", "n"]) == [(1, 1), (2, 2)]
        finally:
            _cleanup(client, sn)

    def test_aggregate_derived_joined(self, client):
        """The aggregate as a derived table (FROM-clause form) joined with a dimension."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _orders_customers(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT c.name AS nm, a.total AS tot "
                "FROM (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) a "
                "JOIN customers c ON a.cid = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 'Alice')", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 50)", schema_name=sn)
            assert _rows(client, sn, "v", ["nm", "tot"]) == [("Alice", 150)]
        finally:
            _cleanup(client, sn)

    def test_two_aggregate_ctes_joined(self, client):
        """Two aggregate CTEs over different tables, joined on their group keys."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS WITH sa AS (SELECT k, SUM(x) AS sx FROM a GROUP BY k), "
                "sb AS (SELECT k, SUM(y) AS sy FROM b GROUP BY k) "
                "SELECT sa.sx AS sx, sb.sy AS sy FROM sa JOIN sb ON sa.k = sb.k",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO a VALUES (1, 7, 10), (2, 7, 20), (3, 9, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 7, 100), (2, 8, 200)", schema_name=sn)
            # sa: k7 -> 30, k9 -> 5. sb: k7 -> 100, k8 -> 200. join on k -> k7: (30, 100).
            assert _rows(client, sn, "v", ["sx", "sy"]) == [(30, 100)]
        finally:
            _cleanup(client, sn)


class TestGroupByChainLifecycle:
    def test_aggregate_chain_backfill(self, client):
        """Orders/customers exist before the aggregate-joined view — the reduce + join
        must backfill from the accumulated base state."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _orders_customers(client, sn)
            client.execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) "
                "SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
                schema_name=sn,
            )
            assert _rows(client, sn, "v", ["nm", "tot"]) == [("Alice", 150), ("Bob", 200)]
            # Still incremental after backfill.
            client.execute_sql("INSERT INTO orders VALUES (4, 2, 5)", schema_name=sn)
            assert _rows(client, sn, "v", ["nm", "tot"]) == [("Alice", 150), ("Bob", 205)]
        finally:
            _cleanup(client, sn)

    def test_aggregate_chain_drop_cascade(self, client):
        """DROP VIEW retires the hidden reduce segment, freeing orders and customers."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _orders_customers(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) "
                "SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
                schema_name=sn,
            )
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            _cleanup(client, sn)


class TestGroupByOverJoin:
    """`SELECT k, agg(x) FROM <join> [WHERE …] GROUP BY k` — a GROUP BY directly over a
    join (no explicit CTE). The join compiles to a hidden view H and the reduce runs over
    it; group/aggregate/WHERE columns resolve against H by name (unqualified)."""

    def test_fact_dimension_sum(self, client):
        """orders ⋈ customers, SUM by the dimension's region — the classic analytics shape."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT region AS reg, SUM(amt) AS total "
                "FROM orders JOIN customers ON orders.cid = customers.id GROUP BY region",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "total"]) == [(100, 80), (200, 70)]

            # Incremental + retraction re-aggregate correctly.
            client.execute_sql("INSERT INTO orders VALUES (4, 2, 5)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "total"]) == [(100, 80), (200, 75)]
            client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "total"]) == [(100, 30), (200, 75)]
        finally:
            _cleanup(client, sn)

    def test_group_by_over_join_with_where(self, client):
        """A WHERE (now consumed inside the hidden join circuit) filters rows before
        grouping — incrementally: inserts below/above the threshold flow through, and
        retracting a contributing row re-aggregates its group."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n, SUM(amt) AS s "
                "FROM orders JOIN customers ON orders.cid = customers.id WHERE amt > 40 GROUP BY region",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
            # amt>40 keeps o1(reg100,50), o3(reg200,70); o2(amt30) filtered.
            assert _rows(client, sn, "v", ["reg", "n", "s"]) == [(100, 1, 50), (200, 1, 70)]

            # Insert below threshold (no effect) and above (adds to region 100).
            client.execute_sql("INSERT INTO orders VALUES (4, 1, 10), (5, 1, 60)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "n", "s"]) == [(100, 2, 110), (200, 1, 70)]

            # Retract a contributing row (o1, amt 50, reg 100).
            client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "n", "s"]) == [(100, 1, 60), (200, 1, 70)]
        finally:
            _cleanup(client, sn)

    def test_distinct_over_join_with_where(self, client):
        """SELECT DISTINCT over an INNER join + WHERE: the WHERE runs inside the hidden
        join circuit; DISTINCT dedups the filtered projection, incrementally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT region AS reg "
                "FROM orders JOIN customers ON orders.cid = customers.id WHERE amt > 40",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
            # amt>40: o1(reg100), o3(reg200). DISTINCT regions {100, 200}.
            assert _rows(client, sn, "v", ["reg"]) == [(100,), (200,)]

            # A second above-threshold order into region 100 — DISTINCT keeps one 100.
            client.execute_sql("INSERT INTO orders VALUES (4, 1, 90)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg"]) == [(100,), (200,)]

            # Retract the sole region-200 contributor -> 200 drops.
            client.execute_sql("DELETE FROM orders WHERE id = 3", schema_name=sn)
            assert _rows(client, sn, "v", ["reg"]) == [(100,)]
        finally:
            _cleanup(client, sn)

    def test_group_by_left_join_where_nullfill(self, client):
        """GROUP BY over an OUTER (equi) LEFT JOIN with the WHERE inside the join
        circuit: a preserved-side predicate keeps null-filled groups; a right-side
        predicate drops them; `right IS NULL` keeps exactly the unmatched rows.
        Maintained under churn on the preserved side."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            # Preserved-side WHERE keeps null-filled rows (region = NULL group).
            client.execute_sql(
                "CREATE VIEW vp AS SELECT region AS reg, COUNT(*) AS n "
                "FROM a LEFT JOIN b ON a.k = b.id WHERE a.amt > 40 GROUP BY region",
                schema_name=sn,
            )
            # Right-side WHERE drops null-filled rows (NULL cmp = UNKNOWN).
            client.execute_sql(
                "CREATE VIEW vr AS SELECT region AS reg, COUNT(*) AS n "
                "FROM a LEFT JOIN b ON a.k = b.id WHERE region > 0 GROUP BY region",
                schema_name=sn,
            )
            # `b.id IS NULL` keeps exactly the unmatched rows (anti-join; `id` is carried
            # by both sides, so the qualifier must resolve through the join alias map).
            client.execute_sql(
                "CREATE VIEW vn AS SELECT COUNT(*) AS n "
                "FROM a LEFT JOIN b ON a.k = b.id WHERE b.id IS NULL",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 50), (2, 99, 70), (3, 10, 30), (4, 88, 20)",
                schema_name=sn,
            )
            # vp: amt>40 keeps a1(reg500), a2(reg NULL); groups {500:1, None:1}.
            assert _rows(client, sn, "vp", ["reg", "n"]) == [(500, 1), (None, 1)]
            # vr: region>0 keeps only the matched rows a1, a3 (both k=10); null-fills drop.
            assert _rows(client, sn, "vr", ["reg", "n"]) == [(500, 2)]
            # vn: unmatched a = {a2, a4} -> count 2.
            assert _rows(client, sn, "vn", ["n"]) == [(2,)]

            # Churn: delete matched a1, insert unmatched a5 amt 80 (passes preserved WHERE).
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (5, 77, 80)", schema_name=sn)
            assert _rows(client, sn, "vp", ["reg", "n"]) == [(None, 2)]  # a2, a5 (NULL)
            assert _rows(client, sn, "vr", ["reg", "n"]) == [(500, 1)]  # a1 gone, a3 remains
            assert _rows(client, sn, "vn", ["n"]) == [(3,)]  # a2, a4, a5 unmatched
        finally:
            _cleanup(client, sn)

    def test_group_by_band_left_join_where(self, client):
        """GROUP BY over a band LEFT JOIN (eq prefix + range) + preserved-side WHERE —
        the `[(500,1),(None,1)]` shape under updates and retractions."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL, "
                "amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, hi BIGINT NOT NULL, "
                "region BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n "
                "FROM a LEFT JOIN b ON a.k = b.k AND a.lo < b.hi WHERE amt > 40 GROUP BY region",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 1, 100, 500)", schema_name=sn)
            # a1 matched (k=1, 50<100) reg500; a2 unmatched (k=2) regNULL; a3 range-fail (200<100) drop by amt? no,
            # a3 amt=30 filtered by WHERE.
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 50, 70), (2, 2, 10, 80), (3, 1, 50, 30)",
                schema_name=sn,
            )
            assert _rows(client, sn, "v", ["reg", "n"]) == [(500, 1), (None, 1)]

            # Insert another unmatched above-threshold row -> None group grows.
            client.execute_sql("INSERT INTO a VALUES (4, 3, 5, 100)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "n"]) == [(500, 1), (None, 2)]

            # Retract the matched a1 -> region 500 empties.
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "n"]) == [(None, 2)]
        finally:
            _cleanup(client, sn)

    def test_distinct_pure_range_left_join_where(self, client):
        """SELECT DISTINCT over a pure-range LEFT JOIN + preserved-side WHERE: matched
        rows carry the right value, unmatched carry NULL; DISTINCT dedups. Under churn."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, lo BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, hi BIGINT NOT NULL, region BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT region AS reg "
                "FROM a LEFT JOIN b ON a.lo < b.hi WHERE amt > 40",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 100, 500)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 50, 70), (2, 200, 80), (3, 60, 90)",
                schema_name=sn,
            )
            # a1,a3 matched (lo<100) reg=500; a2 unmatched reg=NULL. DISTINCT {500, None}.
            assert _rows(client, sn, "v", ["reg"]) == [(500,), (None,)]

            # Retract a2 (the only unmatched) -> None drops.
            client.execute_sql("DELETE FROM a WHERE id = 2", schema_name=sn)
            assert _rows(client, sn, "v", ["reg"]) == [(500,)]
        finally:
            _cleanup(client, sn)

    def test_group_by_join_where_backfill_parity(self, client):
        """The filtered grouped-join view over pre-populated tables (backfill) equals
        the fresh-insert run — WHERE-in-circuit is order-independent."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            # Populate BEFORE the view exists.
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70), (4, 1, 60)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n, SUM(amt) AS s "
                "FROM orders JOIN customers ON orders.cid = customers.id WHERE amt > 40 GROUP BY region",
                schema_name=sn,
            )
            # amt>40 keeps o1(50), o4(60) -> reg100 {n2, s110}; o3(70) -> reg200 {n1, s70}.
            assert _rows(client, sn, "v", ["reg", "n", "s"]) == [(100, 2, 110), (200, 1, 70)]
        finally:
            _cleanup(client, sn)

    def test_group_by_join_qualified_dup_name_where(self, client):
        """A WHERE qualifying a column both sides carry (`orders.id`) resolves through
        the join's alias map — formerly 'ambiguous' when bound by bare name over H."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            # customers ALSO carries `id` (the PK) — so a bare `id` in WHERE is ambiguous.
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n "
                "FROM orders JOIN customers ON orders.cid = customers.id WHERE orders.id > 1 GROUP BY region",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
            # orders.id>1 keeps o2(cid1->reg100), o3(cid2->reg200) -> {100:1, 200:1}.
            assert _rows(client, sn, "v", ["reg", "n"]) == [(100, 1), (200, 1)]
        finally:
            _cleanup(client, sn)

    def test_group_by_over_three_way_join(self, client):
        """A star-schema shape: fact ⋈ dim ⋈ dim, SUM by the far dimension's group."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, SUM(amt) AS total "
                "FROM a JOIN b ON a.cid = b.id JOIN c ON b.c = c.id GROUP BY grp",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 7)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 10, 50)", schema_name=sn)
            assert _rows(client, sn, "v", ["grp", "total"]) == [(7, 150)]
            client.execute_sql("INSERT INTO a VALUES (3, 10, 25)", schema_name=sn)
            assert _rows(client, sn, "v", ["grp", "total"]) == [(7, 175)]
        finally:
            _cleanup(client, sn)


class TestDistinctOverJoin:
    """`SELECT DISTINCT … FROM <join>` — a DISTINCT directly over a join. The join
    compiles to a hidden view H and the distinct runs over H."""

    def test_distinct_dimension_of_joined_facts(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT region FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200), (3, 300)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)", schema_name=sn)  # 300 unordered
            assert _rows(client, sn, "v", ["region"]) == [(100,), (200,)]

            client.execute_sql("INSERT INTO orders VALUES (4, 3)", schema_name=sn)  # region 300 now appears
            assert _rows(client, sn, "v", ["region"]) == [(100,), (200,), (300,)]
            client.execute_sql("DELETE FROM orders WHERE cid = 1", schema_name=sn)  # both 100-orders gone
            assert _rows(client, sn, "v", ["region"]) == [(200,), (300,)]
        finally:
            _cleanup(client, sn)

    def test_distinct_over_three_way_join(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT grp FROM a JOIN b ON a.cid = b.id JOIN c ON b.c = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 7)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 10)", schema_name=sn)
            assert _rows(client, sn, "v", ["grp"]) == [(7,)]
        finally:
            _cleanup(client, sn)


class TestPrunedHiddenJoin:
    """The hidden join view H under GROUP BY / DISTINCT projects only
    the names the outer operator evaluates over it (group cols, aggregate arguments,
    HAVING refs, or the DISTINCT items). Bare-name collection keeps every same-named
    candidate, so ambiguity behavior is unchanged; an empty set keeps the wildcard H."""

    def test_group_by_join_min_max_pruned_h(self, client):
        """MIN/MAX over a joined value column: the aggregate argument is retained in H.
        Non-linear MIN/MAX track correctly under insert and retraction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT region AS reg, MIN(amt) AS lo, MAX(amt) AS hi "
                "FROM orders JOIN customers ON orders.cid = customers.id GROUP BY region",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "lo", "hi"]) == [(100, 50, 80), (200, 30, 30)]

            # Insert a new low into region 100 -> MIN moves.
            client.execute_sql("INSERT INTO orders VALUES (4, 1, 20)", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "lo", "hi"]) == [(100, 20, 80), (200, 30, 30)]

            # Retract the region-100 max (amt 80) -> MAX moves down (non-linear).
            client.execute_sql("DELETE FROM orders WHERE id = 2", schema_name=sn)
            assert _rows(client, sn, "v", ["reg", "lo", "hi"]) == [(100, 20, 50), (200, 30, 30)]
        finally:
            _cleanup(client, sn)

    def test_count_star_join_wildcard_h(self, client):
        """`SELECT COUNT(*) FROM a JOIN b ON …` collects no operator names, so H keeps
        the wildcard (degenerate no-pruning). It registers and counts correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS n FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            client.resolve_table(sn, "v")  # registered
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)", schema_name=sn)
            assert _rows(client, sn, "v", ["n"]) == [(3,)]
            client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["n"]) == [(2,)]
        finally:
            _cleanup(client, sn)

    def test_qualified_agg_arg_dup_name_still_ambiguous(self, client):
        """`SUM(orders.amt)` where BOTH sides carry `amt`: bare-name collection keeps
        both candidates in H, so it still errors as ambiguous — the deterministic
        pre-pruning behavior, not a liveness-dependent acceptance."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            # customers ALSO carries `amt`.
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, amt BIGINT NOT NULL)", schema_name=sn
            )
            with pytest.raises(Exception) as ei:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT SUM(orders.amt) AS s "
                    "FROM orders JOIN customers ON orders.cid = customers.id",
                    schema_name=sn,
                )
            assert "ambiguous" in str(ei.value).lower(), str(ei.value)
        finally:
            _cleanup(client, sn)

    def test_distinct_join_pruned_h_backfill(self, client):
        """SELECT DISTINCT of a subset of join columns: H is pruned to those names.
        Backfill (data before view) equals the incremental run."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT region AS reg "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            # regions of orders' customers: {100 (cid1), 200 (cid2)}.
            assert _rows(client, sn, "v", ["reg"]) == [(100,), (200,)]
        finally:
            _cleanup(client, sn)
