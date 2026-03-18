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

    def test_left_join_exact_row_count(self, client):
        """Matched left join produces exactly one output row per matching pair."""
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
                "INSERT INTO orders VALUES (1, 100, 50), (2, 200, 60), (3, 999, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # 2 matched rows + 1 null-fill row = 3 total
            assert len(rows) == 3, f"expected 3 rows, got {len(rows)}: {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_left_join_delete_retraction(self, client):
        """Deleting a left-side row retracts its contribution from the view."""
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
                "INSERT INTO customers VALUES (100, 1)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 100, 50), (2, 100, 60)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 2, f"expected 2 rows before delete, got {rows}"

            client.execute_sql(
                "DELETE FROM orders WHERE pk = 1",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1, f"expected 1 row after delete, got {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_left_join_null_fill_delete_retraction(self, client):
        """Deleting an unmatched left-side row retracts its null-fill row."""
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

            # Insert order with no matching customer → null-fill row
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 999, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1, f"expected 1 null-fill row, got {rows}"

            # Delete the order → null-fill row should retract
            client.execute_sql(
                "DELETE FROM orders WHERE pk = 1",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 0, f"expected 0 rows after delete, got {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)

    @pytest.mark.xfail(
        reason=(
            "Known incremental limitation: when right-side row arrives after a null-fill "
            "was already emitted for the left-side row, the stale null-fill is not retracted. "
            "Fixing this requires an integrated anti-join circuit tracking null-filled rows."
        ),
        strict=True,
    )
    def test_right_side_insert_after_null_fill(self, client):
        """Right-side insert after null-fill — stale null-fill stays in view."""
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

            # Insert order first with no matching customer → null-fill
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 100, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1  # null-fill row

            # Now insert matching customer → view should show 1 inner-join row
            client.execute_sql(
                "INSERT INTO customers VALUES (100, 1)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            # Incremental limitation: view shows 2 rows (stale null-fill + new inner row)
            # Expected: exactly 1 inner-join row
            assert len(rows) == 1, f"expected 1 inner row, got {len(rows)}: {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)

    @pytest.mark.xfail(
        reason=(
            "Known incremental limitation: when a matched right-side row is deleted, "
            "the inner-join row retracts but no null-fill row is emitted for the left-side row. "
            "Fixing this requires an integrated anti-join circuit."
        ),
        strict=True,
    )
    def test_right_side_delete_after_match(self, client):
        """Right-side delete after match — null-fill not re-emitted."""
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
                "INSERT INTO customers VALUES (100, 1)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 100, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1  # inner-join row

            # Delete the customer → inner row retracts, but null-fill should appear
            client.execute_sql(
                "DELETE FROM customers WHERE pk = 100",
                schema_name=sn,
            )
            rows = client.scan(vid)
            # Incremental limitation: view is empty (inner row retracted, no null-fill added)
            # Expected: 1 null-fill row (order pk=1 still exists without a customer)
            assert len(rows) == 1, f"expected 1 null-fill row, got {len(rows)}: {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
            client.execute_sql("DROP TABLE customers", schema_name=sn)
        finally:
            client.drop_schema(sn)
