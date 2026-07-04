"""E2E tests: a derived table (subquery in FROM) compiles into a hidden view
chained to the final view — the FROM-clause form of the CTE chaining in
`test_cte_chain.py`.

`SELECT d.id, u.name FROM (SELECT id, total FROM t WHERE total > 100) d JOIN u ON
d.id = u.id` becomes an atomic bundle: a hidden filter view over `t` feeding the
user-named join view. Only linear (single-table filter/projection) subqueries are
chained for now.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_derived_table.py -v --tb=short
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
    vid = client.resolve_table(sn, view)[0]
    return sorted(tuple(r._asdict()[k] for k in keys) for r in client.scan(vid) if r.weight > 0)


def _two_tables(client, sn):
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, total BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)", schema_name=sn
    )


class TestDerivedTable:
    def test_derived_join_incremental(self, client):
        """A derived table joined with a base, maintained through UPDATE and DELETE."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT d.id AS did, u.name AS nm "
                "FROM (SELECT id, total FROM t WHERE total > 100) d JOIN u ON d.id = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice'), (2, 'Bob')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 200), (2, 50)", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == [(1, "Alice")]

            # t2 crosses the threshold via UPDATE -> appears.
            client.execute_sql("UPDATE t SET total = 300 WHERE id = 2", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == [(1, "Alice"), (2, "Bob")]

            # Retract t1.
            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["did", "nm"]) == [(2, "Bob")]
        finally:
            _cleanup(client, sn)

    def test_derived_as_base_with_column_aliases(self, client):
        """A derived table as the base relation, with column aliases and a simple final."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW s AS SELECT k FROM (SELECT id, v FROM t WHERE v > 10) AS d(k, val)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
            assert _rows(client, sn, "s", ["k"]) == [(1,)]
        finally:
            _cleanup(client, sn)

    def test_two_derived_tables_joined(self, client):
        """Both sides of the join are derived tables -> two hidden segments."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW j AS SELECT a.id AS aid, b.w AS bw "
                "FROM (SELECT id, v FROM t WHERE v > 10) a JOIN (SELECT id, w FROM u WHERE w < 100) b "
                "ON a.id = b.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (1, 50), (2, 200)", schema_name=sn)
            assert _rows(client, sn, "j", ["aid", "bw"]) == [(1, 50)]
        finally:
            _cleanup(client, sn)

    def test_derived_drop_cascade(self, client):
        """DROP VIEW cascades the derived table's hidden segment, freeing the base."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT d.id AS did FROM (SELECT id, total FROM t WHERE total > 100) d "
                "JOIN u ON d.id = u.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 'Alice')", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 200)", schema_name=sn)
            assert _rows(client, sn, "v", ["did"]) == [(1,)]
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
            client.execute_sql("DROP TABLE u", schema_name=sn)
        finally:
            _cleanup(client, sn)

    def test_unaliased_derived_rejected(self, client):
        """A derived table without an alias cannot be referenced, so it is rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            with pytest.raises(Exception) as ei:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT id FROM (SELECT id, v FROM t WHERE v > 10)",
                    schema_name=sn,
                )
            assert "alias" in str(ei.value).lower(), str(ei.value)
        finally:
            _cleanup(client, sn)

    def test_distinct_derived_rejected(self, client):
        """A DISTINCT-body derived table is rejected. (A JOIN-body derived
        table IS supported — see test_multiway_join.py.)"""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn)
            with pytest.raises(Exception) as ei:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT d.v AS dv FROM (SELECT DISTINCT v FROM t) d JOIN u ON d.v = u.id",
                    schema_name=sn,
                )
            assert "distinct" in str(ei.value).lower() or "not yet" in str(ei.value).lower(), str(ei.value)
        finally:
            _cleanup(client, sn)

    def test_sibling_derived_not_correlated(self, client):
        """A sibling derived table is out of scope: a same-named reference in a
        later sibling binds the catalog relation, not the sibling (a non-LATERAL
        derived table is not correlated)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, dummy BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            # `b`'s `FROM a` binds the catalog table `a` (id, v), not the sibling
            # derived `a` (only id). If it bound the sibling, `b.v` would not resolve.
            client.execute_sql(
                "CREATE VIEW v AS SELECT b.v AS bv "
                "FROM (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO a VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 0), (2, 0)", schema_name=sn)
            # Sibling derived a = {1, 2} (from t); catalog a = {(1, 100), (2, 200)}.
            # Join a.id = b.id projects b.v for ids 1 and 2.
            assert _rows(client, sn, "v", ["bv"]) == [(100,), (200,)]
        finally:
            _cleanup(client, sn)
