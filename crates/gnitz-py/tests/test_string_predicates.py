"""E2E tests: string predicates in CREATE VIEW.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_string_predicates.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or ["v"]):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or ["t"]):
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


class TestStringPredicates:
    def test_view_string_eq_literal(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alice')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            names = sorted([r["name"] for r in rows])
            assert names == ["Alice", "Alice"], f"expected two Alices, got {names}"
            ids = sorted([r["id"] for r in rows])
            assert ids == [1, 3]
        finally:
            _cleanup(client, sn)

    def test_view_string_ne_literal(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE name != 'Bob'",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            names = sorted([r["name"] for r in rows])
            assert names == ["Alice", "Charlie"]
        finally:
            _cleanup(client, sn)

    def test_view_string_gt_literal(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE name > 'Bob'",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Dave')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            names = sorted([r["name"] for r in rows])
            assert names == ["Charlie", "Dave"]
        finally:
            _cleanup(client, sn)

    def test_view_string_le_literal(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE name <= 'Bob'",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            names = sorted([r["name"] for r in rows])
            assert names == ["Alice", "Bob"]
        finally:
            _cleanup(client, sn)

    def test_view_string_and_int(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL, age BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice' AND age > 21",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'Alice', 25), (2, 'Alice', 18), (3, 'Bob', 30)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1
            assert rows[0]["id"] == 1
            assert rows[0]["name"] == "Alice"
            assert rows[0]["age"] == 25
        finally:
            _cleanup(client, sn)

    def test_view_string_null_filtered(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'Alice'), (2, NULL), (3, 'Bob')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1
            assert rows[0]["name"] == "Alice"
        finally:
            _cleanup(client, sn)
