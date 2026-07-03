"""E2E tests: a non-pass-through CTE compiles into a hidden view chained to the
final view.

`WITH d AS (SELECT id, total FROM t WHERE total > 100) SELECT d.id, u.name FROM d
JOIN u ON d.id = u.id` becomes an atomic two-view bundle: a hidden filter view over
`t` (natural PK — no synthetic-PK machinery needed) feeding the user-named join
view, with dependency-ordered backfill. Plain pass-through CTEs still alias.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_cte_chain.py -v --tb=short
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


def _two_tables(client, sn):
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, total BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)", schema_name=sn
    )


class TestCteChain:
    def test_filter_cte_joined_backfill(self, client):
        """View created before data: the filter CTE + join backfills correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH d AS (SELECT id, total FROM t WHERE total > 100) "
                "SELECT d.id AS did, u.name AS nm FROM d JOIN u ON d.id = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice'), (2, 'Bob')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 200), (2, 50)", schema_name=sn)
            # d = {id:1 (200>100)}; t2 filtered. d JOIN u on id -> (1, Alice).
            assert _rows(client, sn, "v", ["did", "nm"]) == [(1, "Alice")]
        finally:
            _cleanup(client, sn)

    def test_incremental_and_retraction(self, client):
        """Inserts/deletes into the base flow through the hidden CTE into the final view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH d AS (SELECT id, total FROM t WHERE total > 100) "
                "SELECT d.id AS did, u.name AS nm FROM d JOIN u ON d.id = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 200), (2, 50)", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == [(1, "Alice")]

            # t3 crosses the threshold -> appears.
            client.execute_sql("INSERT INTO t VALUES (3, 300)", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == [(1, "Alice"), (3, "Carol")]

            # Retract t1 -> leaves the view.
            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == [(3, "Carol")]

            # An update that drops total below the threshold retracts the row.
            client.execute_sql("UPDATE t SET total = 5 WHERE id = 3", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == []
        finally:
            _cleanup(client, sn)

    def test_drop_view_cascades_hidden_segment(self, client):
        """DROP VIEW retires the hidden CTE segment, freeing the base table to drop."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH d AS (SELECT id, total FROM t WHERE total > 100) "
                "SELECT d.id AS did FROM d JOIN u ON d.id = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 200)", schema_name=sn)
            assert _rows(client, sn, "v", ["did"]) == [(1,)]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            # If the hidden segment were orphaned it would still depend on t and RESTRICT.
            client.execute_sql("DROP TABLE t", schema_name=sn)
            client.execute_sql("DROP TABLE u", schema_name=sn)
        finally:
            _cleanup(client, sn)

    def test_two_hidden_ctes_joined(self, client):
        """Two non-pass-through CTEs each become a hidden segment feeding the final join."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW j AS WITH a AS (SELECT id, v FROM t WHERE v > 10), "
                "b AS (SELECT id, w FROM u WHERE w < 100) "
                "SELECT a.id AS aid, b.w AS bw FROM a JOIN b ON a.id = b.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (1, 50), (2, 200)", schema_name=sn)
            # a = {1 (v 20>10)}, b = {1 (w 50<100)}; join on id -> (1, 50).
            assert _rows(client, sn, "j", ["aid", "bw"]) == [(1, 50)]
        finally:
            _cleanup(client, sn)

    def test_simple_final_over_cte(self, client):
        """A filter/map final over a compiled CTE (no join) is also chained."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW s AS WITH c AS (SELECT id, v FROM t WHERE v > 10) SELECT id FROM c",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
            assert _rows(client, sn, "s", ["id"]) == [(1,)]
        finally:
            _cleanup(client, sn)

    def test_cte_column_aliases(self, client):
        """`WITH d(a, b) AS (SELECT ... WHERE ...)` renames the hidden view's columns."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH d(k, amt) AS (SELECT id, total FROM t WHERE total > 100) "
                "SELECT d.k AS kk, d.amt AS aa FROM d JOIN u ON d.k = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 200)", schema_name=sn)
            assert _rows(client, sn, "v", ["kk", "aa"]) == [(1, 200)]
        finally:
            _cleanup(client, sn)

    def test_passthrough_cte_still_aliases(self, client):
        """A plain pass-through CTE (no WHERE) still aliases — no hidden view created."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH d AS (SELECT * FROM t) "
                "SELECT d.id AS did FROM d JOIN u ON d.id = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice'), (2, 'Bob')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
            assert _rows(client, sn, "v", ["did"]) == [(1,), (2,)]
        finally:
            _cleanup(client, sn)

    def test_distinct_final_over_cte(self, client):
        """A DISTINCT final over a compiled CTE (the hidden filter view feeds distinct)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW g AS WITH c AS (SELECT id, v FROM t WHERE v > 10) SELECT DISTINCT v FROM c",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 20), (3, 5), (4, 30)", schema_name=sn)
            # c keeps v>10: {20 (x2), 30}; DISTINCT collapses to {20, 30}.
            assert _rows(client, sn, "g", ["v"]) == [(20,), (30,)]
            # Retract one of the two 20s — 20 stays (distinct); retract both — it leaves.
            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "g", ["v"]) == [(20,), (30,)]
            client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
            assert _rows(client, sn, "g", ["v"]) == [(30,)]
        finally:
            _cleanup(client, sn)

    def test_distinct_final_over_join_cte(self, client):
        """A DISTINCT final over a JOIN-body CTE — distinct rows of a join projection."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH j AS (SELECT b.v AS bv FROM a JOIN b ON a.k = b.id) "
                "SELECT DISTINCT bv FROM j",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 77), (20, 77)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20), (3, 10)", schema_name=sn)  # three bv=77
            assert _rows(client, sn, "v", ["bv"]) == [(77,)]
        finally:
            _cleanup(client, sn)

    def test_union_final_over_cte(self, client):
        """A UNION final where one side is a compiled CTE, the other a base table."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH c AS (SELECT v FROM t WHERE v > 10) "
                "SELECT v FROM c UNION SELECT w FROM u",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)  # c = {20}
            client.execute_sql("INSERT INTO u VALUES (1, 20), (2, 99)", schema_name=sn)  # u.w = {20, 99}
            assert _rows(client, sn, "v", ["v"]) == [(20,), (99,)]  # UNION distinct
        finally:
            _cleanup(client, sn)
