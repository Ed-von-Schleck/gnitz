"""E2E tests for UNION ALL, UNION, SELECT DISTINCT.

Run:
    cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_set_ops.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


class TestSetOps:
    def _setup_two_tables(self, client, sn):
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )

    def test_union_all(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (3, 30), (4, 40)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 4
            vals = sorted(r["val"] for r in rows)
            assert vals == [10, 20, 30, 40]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_union_distinct(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert rows with overlapping values (different PKs)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (3, 10), (4, 30)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # UNION deduplicates — but dedup is by PK+content. Since PKs differ,
            # all 4 rows are distinct and should appear.
            assert len(rows) == 4

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_union_all_retraction(self, client):
        """Deleting a row from one source retracts it from the UNION ALL view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (3, 30)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 3

            client.execute_sql("DELETE FROM a WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 2, f"expected 2 rows after delete, got {rows}"
            vals = sorted(r["val"] for r in rows)
            assert vals == [20, 30]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_except_basic(self, client):
        """EXCEPT view excludes rows whose PK is present in the right table.

        b must be populated before a so that I(B) is non-empty when ΔA is processed.
        EXCEPT is implemented as anti_join(ΔA, I(B)), so only ΔA insertions see the
        exclusion; later ΔB changes do not retroactively update the view.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert b FIRST so I(B) is populated when ΔA arrives
            client.execute_sql(
                "INSERT INTO b VALUES (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [10, 30], f"expected [10, 30], got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    @pytest.mark.xfail(
        reason=(
            "Known incremental limitation: EXCEPT is anti_join(ΔA, I(B)), so ΔB changes "
            "after A rows were already emitted do not retroactively update the EXCEPT output. "
            "Fixing this requires a bidirectional anti-join circuit."
        ),
        strict=True,
    )
    def test_except_retraction(self, client):
        """Deleting a row from b should add it back to the EXCEPT view — but doesn't."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert b first so I(B) is populated when ΔA arrives
            client.execute_sql(
                "INSERT INTO b VALUES (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [10], f"expected [10], got {vals}"

            # Delete the exclusion row — (2, 20) should now appear in v
            client.execute_sql("DELETE FROM b WHERE pk = 2", schema_name=sn)
            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            # Incremental limitation: row (2, 20) is NOT added back
            assert vals == [10, 20], f"expected [10, 20] after delete, got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_intersect_basic(self, client):
        """INTERSECT view contains only rows present in both tables."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a INTERSECT SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )
            # Only (2, 20) is shared (same pk AND same val)
            client.execute_sql(
                "INSERT INTO b VALUES (2, 20), (4, 40)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [20], f"expected [20], got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_distinct_view(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT * FROM t",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 3

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
