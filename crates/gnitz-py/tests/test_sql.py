"""Integration tests for the gnitz-sql SQL frontend.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_sql.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


# ---------------------------------------------------------------------------
# TestSqlDdl
# ---------------------------------------------------------------------------

class TestSqlDdl:
    def test_create_and_drop_table(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            results = client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            assert len(results) == 1
            assert results[0]["type"] == "TableCreated"
            tid = results[0]["table_id"]
            assert tid > 0

            # Verify table exists and resolves
            resolved_tid, _ = client.resolve_table(sn, "t")
            assert resolved_tid == tid

            # Drop it
            results2 = client.execute_sql("DROP TABLE t", schema_name=sn)
            assert results2[0]["type"] == "Dropped"
        finally:
            client.drop_schema(sn)

    def test_create_view_with_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Create view BEFORE inserting (DBSP views only process future deltas)
            results = client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            assert results[0]["type"] == "ViewCreated"
            vid = results[0]["view_id"]
            assert vid > 0

            # Insert rows after view creation so they flow through the circuit
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)",
                schema_name=sn,
            )

            # Scan view (rows with val > 10 should be there: val=15, val=25)
            scan_res = client.scan(vid)
            assert len(scan_res) == 2  # val=15 and val=25 pass filter

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestSqlInsert
# ---------------------------------------------------------------------------

class TestSqlInsert:
    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )

    def test_insert_single_row(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            results = client.execute_sql(
                "INSERT INTO t VALUES (42, 100)",
                schema_name=sn,
            )
            assert results[0]["type"] == "RowsAffected"
            assert results[0]["count"] == 1

            scan_res = client.scan(
                client.resolve_table(sn, "t")[0]
            )
            assert len(scan_res) == 1
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_insert_multiple_rows(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            scan_res = client.scan(tid)
            assert len(scan_res) == 3
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestSqlSelect
# ---------------------------------------------------------------------------

class TestSqlSelect:
    def _setup_with_rows(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
            schema_name=sn,
        )

    def test_select_star(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            results = client.execute_sql("SELECT * FROM t", schema_name=sn)
            assert results[0]["type"] == "Rows"
            batch = results[0]["rows"]
            assert len(batch) == 5
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_star_from_view(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 20",
                schema_name=sn,
            )
            results = client.execute_sql("SELECT * FROM v", schema_name=sn)
            assert results[0]["type"] == "Rows"
            batch = results[0]["rows"]
            # SELECT * FROM v scans the view — rows with val 30, 40, 50 pass filter
            assert len(batch) == 3

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_pk_seek(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            results = client.execute_sql("SELECT * FROM t WHERE pk = 3", schema_name=sn)
            assert results[0]["type"] == "Rows"
            batch = results[0]["rows"]
            # Should find row with pk=3
            assert len(batch) == 1  # point lookup returns exactly one row
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_nonindexed_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("SELECT * FROM t WHERE val = 5", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_with_limit(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            results = client.execute_sql("SELECT * FROM t LIMIT 2", schema_name=sn)
            assert results[0]["type"] == "Rows"
            batch = results[0]["rows"]
            assert len(batch) == 2
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestSqlCreateView
# ---------------------------------------------------------------------------

class TestSqlCreateView:
    def _setup_table(self, client, sn):
        """Create table only (no data) — insert after view creation for DBSP."""
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )

    def test_create_view_projection(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_table(client, sn)
            results = client.execute_sql(
                "CREATE VIEW v AS SELECT val FROM t",
                schema_name=sn,
            )
            assert results[0]["type"] == "ViewCreated"
            vid = results[0]["view_id"]
            assert vid > 0
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_create_view_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_table(client, sn)
            results = client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            assert results[0]["type"] == "ViewCreated"
            vid = results[0]["view_id"]
            assert vid > 0

            # Insert after view creation so rows flow through the circuit
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)",
                schema_name=sn,
            )

            # Scan the view — rows with val > 10 should be there (val=15, val=25)
            scan_res = client.scan(vid)
            assert len(scan_res) == 2  # val=15 and val=25 pass filter

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
