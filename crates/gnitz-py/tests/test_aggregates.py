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

    def test_multi_col_group_by_min_distinct_groups(self, client):
        """Single MIN over a two-column GROUP BY routes through the byte-form
        AVI (group key = a ++ b ++ encoded value). Each distinct (a, b) must
        resolve its own minimum — the index carries the full group key, so two
        groups can never share a bucket."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  a INT NOT NULL,"
                "  b INT NOT NULL,"
                "  val BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, b, MIN(val) AS lo "
                "FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # 9 distinct (a, b) groups, each with several rows. The expected
            # MIN per group is a*100 + b (the smallest val inserted for it).
            pk = 0
            expected = {}
            rows_sql = []
            for a in range(3):
                for b in range(3):
                    base = a * 100 + b
                    expected[(a, b)] = base
                    for k in range(3):
                        pk += 1
                        rows_sql.append(f"({pk}, {a}, {b}, {base + k * 10})")
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql),
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == len(expected)
            for r in rows:
                key = (r["a"], r["b"])
                assert r["lo"] == expected[key], (
                    f"group {key}: expected MIN {expected[key]}, got {r['lo']}"
                )

            # Incremental update: push a value below group (1,1)'s current MIN.
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk + 1}, 1, 1, -5)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            by_key = {(r["a"], r["b"]): r["lo"] for r in rows}
            assert by_key[(1, 1)] == -5, "pushing a smaller value must lower the MIN"
            # Other groups unchanged.
            assert by_key[(2, 2)] == expected[(2, 2)]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
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


class TestGroupByPkAndNullable:
    """`GROUP BY` containing the PK column or a nullable column."""

    def test_group_by_pk_and_other_col(self, client):
        """`GROUP BY pk, other_col` must produce one group per PK (since PKs
        are unique)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  category BIGINT NOT NULL,"
                "  amount BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT pk, category, COUNT(*) AS cnt "
                "FROM orders GROUP BY pk, category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 3
            by_pk = {r["pk"]: r for r in rows}
            assert by_pk[1]["category"] == 10 and by_pk[1]["cnt"] == 1
            assert by_pk[2]["category"] == 10 and by_pk[2]["cnt"] == 1
            assert by_pk[3]["category"] == 20 and by_pk[3]["cnt"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_by_pk_and_other_col_uuid_pk(self, client):
        """`GROUP BY pk, other_col` with a 16-byte UUID PK projection."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE items ("
                "  pk UUID NOT NULL PRIMARY KEY,"
                "  category BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT pk, category, COUNT(*) AS cnt "
                "FROM items GROUP BY pk, category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            uuid_a = '550e8400-e29b-41d4-a716-446655440000'
            uuid_b = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            client.execute_sql(
                f"INSERT INTO items VALUES ('{uuid_a}', 10), ('{uuid_b}', 20)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 2
            by_pk = {r["pk"]: r for r in rows}
            assert by_pk[uuid_a]["category"] == 10 and by_pk[uuid_a]["cnt"] == 1
            assert by_pk[uuid_b]["category"] == 20 and by_pk[uuid_b]["cnt"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE items", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_by_nullable_int_distinguishes_null_from_zero(self, client):
        """`GROUP BY nullable_int` must form a distinct group for NULL,
        not merge it with the integer-zero group (NULL stores as zero bytes)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  grp BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Two rows with grp=NULL, one with grp=0, one with grp=7.
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL), (2, 0), (3, NULL), (4, 7)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            # Three distinct groups: NULL, 0, 7
            assert len(rows) == 3, f"expected 3 groups, got {len(rows)}: {rows}"
            counts = {r["grp"]: r["cnt"] for r in rows}
            assert counts.get(None) == 2, f"NULL group must have 2 rows; got {counts}"
            assert counts.get(0) == 1, f"0-group must have 1 row; got {counts}"
            assert counts.get(7) == 1, f"7-group must have 1 row; got {counts}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
