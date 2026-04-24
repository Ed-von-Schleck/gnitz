"""E2E tests: equijoins in CREATE VIEW.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_joins.py -v --tb=short
"""
import os
import random
import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


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
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


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
            # The remaining row should have val=100 from t2 (both t1 rows matched t2 pk=10)
            assert rows[0]["val"] == 100
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

    def test_inner_join_string_payload(self, client):
        """Join with VARCHAR payload columns: verify blob data correct in output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, "
                "cat_id BIGINT NOT NULL, label VARCHAR(200) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE categories (id BIGINT NOT NULL PRIMARY KEY, "
                "name VARCHAR(200) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM items "
                "JOIN categories ON items.cat_id = categories.id",
                schema_name=sn,
            )
            # Insert categories first
            client.execute_sql(
                "INSERT INTO categories VALUES (100, 'Electronics'), (200, 'Books')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO items VALUES (1, 100, 'Laptop'), (2, 200, 'Novel'), "
                "(3, 100, 'Phone')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 3, f"expected 3 rows, got {len(rows)}"
            labels = sorted([r["label"] for r in rows])
            assert labels == ["Laptop", "Novel", "Phone"]
            cats = sorted([r["name"] for r in rows])
            assert cats == ["Books", "Electronics", "Electronics"]
        finally:
            _cleanup(client, sn, tables=["items", "categories"], views=["v"])

    def test_many_to_many_join(self, client):
        """Both sides have multiple rows matching same key — verify cross-product."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.fk",
                schema_name=sn,
            )
            # 2 left rows with fk=10, 3 right rows with fk=10
            client.execute_sql(
                "INSERT INTO t2 VALUES (101, 10, 1), (102, 10, 2), (103, 10, 3)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t1 VALUES (1, 10, 100), (2, 10, 200)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # 2 left x 3 right = 6 output rows
            assert len(rows) == 6, f"expected 6 cross-product rows, got {len(rows)}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    @_NEEDS_MULTI
    def test_inner_join_wide_u64_pks_multiworker(self, client):
        """Inner join on BIGINT PK columns distributed across multiple workers.

        Both tables use U64 PKs (narrow physical representation). Rows are
        spread across workers by hash-partitioning. Verifies that exchange
        routing, narrow PK encode/decode, and join produce the correct
        cross-matched result with no duplicates or missing rows.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE left_t "
                "(id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, lval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE right_t "
                "(id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, rval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT left_t.id AS lid, right_t.id AS rid, left_t.lval, right_t.rval "
                "FROM left_t JOIN right_t ON left_t.fk = right_t.fk",
                schema_name=sn,
            )
            # Use a range of PKs that span multiple hash buckets / workers.
            n = 20
            left_vals = ", ".join(f"({i}, {i % 5}, {i * 10})" for i in range(1, n + 1))
            right_vals = ", ".join(f"({i + 100}, {i % 5}, {i * 100})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO left_t VALUES {left_vals}", schema_name=sn)
            client.execute_sql(f"INSERT INTO right_t VALUES {right_vals}", schema_name=sn)

            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # 5 fk groups × (4 left × 4 right matches) = 80 rows.
            assert len(rows) == 80, f"expected 80 join rows, got {len(rows)}"
            # Every left row (lid 1..n) must appear in the output.
            lids = {r["lid"] for r in rows}
            assert lids == set(range(1, n + 1)), f"unexpected lids: {lids}"
        finally:
            _cleanup(client, sn, tables=["left_t", "right_t"], views=["v"])
