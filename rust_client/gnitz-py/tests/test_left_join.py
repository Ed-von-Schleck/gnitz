"""E2E tests for LEFT JOIN.

Run:
    cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_left_join.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


class TestLeftJoin:
    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE orders ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  customer_id BIGINT NOT NULL,"
            "  amount BIGINT NOT NULL"
            ")",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE customers ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  name BIGINT NOT NULL"
            ")",
            schema_name=sn,
        )

    def test_left_join_with_match(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM orders "
                "LEFT JOIN customers ON orders.customer_id = customers.pk",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO customers VALUES (100, 1), (200, 2)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 100, 50)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Order 1 matches customer 100
            assert len(rows) >= 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_left_join_no_match(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM orders "
                "LEFT JOIN customers ON orders.customer_id = customers.pk",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert order with customer_id=999 (no matching customer)
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 999, 50)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Should still get the order row, with NULL customer columns
            assert len(rows) >= 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_inner_join_still_works(self, client):
        """Verify that INNER JOIN still works after LEFT JOIN changes."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM orders "
                "JOIN customers ON orders.customer_id = customers.pk",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO customers VALUES (100, 1)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 100, 50), (2, 999, 60)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Only order 1 matches (customer_id=100)
            assert len(rows) == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)
