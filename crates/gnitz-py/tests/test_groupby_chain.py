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


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, view, keys):
    vid = client.resolve_table(sn, view)[0]
    return sorted(tuple(r._asdict()[k] for k in keys) for r in client.scan(vid) if r.weight > 0)


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
        """A WHERE (over the join output) filters rows before grouping."""
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
                "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n "
                "FROM orders JOIN customers ON orders.cid = customers.id WHERE amt > 40 GROUP BY region",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
            # amt>40 keeps o1(reg100), o3(reg200); COUNT by region -> {100:1, 200:1}.
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
