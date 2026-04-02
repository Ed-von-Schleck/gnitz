"""E2E tests for GROUP BY with COUNT/SUM/MIN/MAX/AVG, HAVING.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_aggregates.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


class TestGroupBy:
    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE orders ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  category BIGINT NOT NULL,"
            "  amount BIGINT NOT NULL,"
            "  price BIGINT NOT NULL"
            ")",
            schema_name=sn,
        )

    def test_count_star(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 2
            # Find rows by category
            by_cat = {}
            for r in rows:
                cat = r["category"]
                by_cat[cat] = r
            assert by_cat[10]["cnt"] == 2
            assert by_cat[20]["cnt"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_sum(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            by_cat = {}
            for r in rows:
                by_cat[r["category"]] = r
            assert by_cat[10]["total"] == 300  # 100+200
            assert by_cat[20]["total"] == 300

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_multi_agg(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            by_cat = {}
            for r in rows:
                by_cat[r["category"]] = r
            assert by_cat[10]["cnt"] == 2
            assert by_cat[10]["total"] == 300
            assert by_cat[20]["cnt"] == 1
            assert by_cat[20]["total"] == 300

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_min_max(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, MIN(amount) AS lo, MAX(amount) AS hi "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 10, 50, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["lo"] == 50
            assert row["hi"] == 200

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt "
                "FROM orders GROUP BY category HAVING COUNT(*) > 1",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Only category 10 has count > 1
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["category"] == 10
            assert row["cnt"] == 2

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_avg(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, AVG(amount) AS avg_amt "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            # AVG(100, 200) = 150.0
            assert abs(row["avg_amt"] - 150.0) < 0.001

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_by_with_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt "
                "FROM orders WHERE amount > 100 GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 50, 50), (2, 10, 200, 60), (3, 10, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Only 2 rows have amount > 100
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["cnt"] == 2

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_count_retraction(self, client):
        """Deleting a row decrements the group count."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 10, 300, 70)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1
            assert next(iter(rows))["cnt"] == 3

            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 1
            assert next(iter(rows))["cnt"] == 2, f"expected cnt=2 after delete, got {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_sum_retraction(self, client):
        """Deleting a row decrements the group sum."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert next(iter(rows))["total"] == 300

            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 1
            assert next(iter(rows))["total"] == 200, f"expected total=200, got {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_incremental_group_by(self, client):
        """Insert in two batches; verify the view updates incrementally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Batch 1
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["cnt"] == 1
            assert row["total"] == 100

            # Batch 2
            client.execute_sql(
                "INSERT INTO orders VALUES (2, 10, 200, 60)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["cnt"] == 2
            assert row["total"] == 300

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_elimination(self, client):
        """Insert rows in 2 groups, delete all rows from group A,
        verify group A vanishes while group B remains."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 2

            # Delete all rows from category 10
            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            client.execute_sql("DELETE FROM orders WHERE pk = 2", schema_name=sn)

            by_cat = {r["category"]: r["cnt"] for r in client.scan(vid) if r.weight > 0}
            # Category 10 should have cnt=0 or be absent; category 20 should have cnt=1
            assert by_cat.get(10, 0) == 0
            assert by_cat[20] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_reinsertion_after_deletion(self, client):
        """Insert row, delete it, re-insert it, verify aggregate restores."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert next(iter(rows))["total"] == 100

            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            totals = {r["category"]: r["total"] for r in client.scan(vid) if r.weight > 0}
            assert totals.get(10, 0) == 0

            # Re-insert and verify aggregate restores
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50)",
                schema_name=sn,
            )
            totals = {r["category"]: r["total"] for r in client.scan(vid) if r.weight > 0}
            assert totals[10] == 100

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)
