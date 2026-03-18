"""E2E tests: equijoins in CREATE VIEW.

Run:
    cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_joins.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _scan_dicts(client, tid):
    return client.scan(tid).mappings()


class TestJoins:
    def test_inner_join_int_key(self, client):
        """Basic equijoin on integer key."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            # Insert customers first, then orders
            client.execute_sql(
                "INSERT INTO customers VALUES (10, 'Alice'), (20, 'Bob')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # 3 orders, each matched to a customer
            assert len(rows) == 3, f"expected 3 rows, got {len(rows)}: {rows}"
            # Check that customer name is present
            names = sorted([r["name"] for r in rows])
            assert names == ["Alice", "Alice", "Bob"]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_inner_join_no_match(self, client):
        """Rows without matches are excluded."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO t1 VALUES (1, 99)", schema_name=sn)  # no match
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0, f"expected 0 rows, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_join_incremental_update(self, client):
        """Insert into one table, then the other — join updates incrementally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            # Insert into t1 first — no matches yet
            client.execute_sql("INSERT INTO t1 VALUES (1, 10)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0

            # Now insert matching row into t2 — join should produce result
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1, f"expected 1 row after t2 insert, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_join_cross_rejects(self, client):
        """CROSS JOIN should be rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM t1 CROSS JOIN t2",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_inner_join_left_delete_retraction(self, client):
        """Deleting a left-side row retracts its matched rows from the view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t1 VALUES (1, 10), (2, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 2, f"expected 2 rows before delete, got {rows}"

            client.execute_sql("DELETE FROM t1 WHERE id = 1", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1, f"expected 1 row after left-side delete, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_inner_join_right_delete_retraction(self, client):
        """Deleting a right-side row retracts all matched rows from the view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t1 VALUES (1, 10), (2, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 2, f"expected 2 rows before delete, got {rows}"

            client.execute_sql("DELETE FROM t2 WHERE id = 10", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0, f"expected 0 rows after right-side delete, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_join_non_equi_rejects(self, client):
        """Non-equi join condition should be rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.a < t2.b",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])
