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

    def test_select_unsupported_clauses_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            # DISTINCT is *accepted today* on `pk` — it returns rows with the dedup
            # silently dropped (correct only because pk is unique). The guard must
            # intercept it, turning a previously-accepted query into an explicit error.
            with pytest.raises(gnitz.GnitzError, match="DISTINCT is not supported"):
                client.execute_sql("SELECT DISTINCT pk FROM t", schema_name=sn)
            # DISTINCT ON gets its own message (no CREATE VIEW redirect).
            with pytest.raises(gnitz.GnitzError, match="DISTINCT ON is not supported"):
                client.execute_sql("SELECT DISTINCT ON (pk) pk FROM t", schema_name=sn)
            # GROUP BY: returns ungrouped rows today.
            with pytest.raises(gnitz.GnitzError, match="GROUP BY is not supported"):
                client.execute_sql("SELECT pk FROM t GROUP BY pk", schema_name=sn)
            # HAVING: predicate silently dropped today.
            with pytest.raises(gnitz.GnitzError, match="HAVING is not supported"):
                client.execute_sql("SELECT pk FROM t HAVING pk > 0", schema_name=sn)
            # OFFSET: skip silently ignored today (returns the first LIMIT rows).
            with pytest.raises(gnitz.GnitzError, match="OFFSET is not supported"):
                client.execute_sql("SELECT * FROM t LIMIT 2 OFFSET 1", schema_name=sn)
            # FETCH: silently dropped today.
            with pytest.raises(gnitz.GnitzError, match="FETCH is not supported"):
                client.execute_sql("SELECT * FROM t FETCH FIRST 2 ROWS ONLY", schema_name=sn)
            # PREWHERE: filter silently dropped today → whole table scanned.
            with pytest.raises(gnitz.GnitzError, match="PREWHERE is not supported"):
                client.execute_sql("SELECT pk FROM t PREWHERE val > 5", schema_name=sn)
            # TOP: row-limit silently dropped today → every row returned.
            with pytest.raises(gnitz.GnitzError, match="TOP is not supported"):
                client.execute_sql("SELECT TOP 2 pk FROM t", schema_name=sn)
            # An inert exotic clause is rejected uniformly, not silently accepted.
            with pytest.raises(gnitz.GnitzError, match="QUALIFY is not supported"):
                client.execute_sql(
                    "SELECT pk FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY pk) = 1", schema_name=sn
                )
            # WITH on a direct SELECT was silently ignored; a shadowing CTE returned the wrong table.
            with pytest.raises(gnitz.GnitzError, match="WITH .CTE. is not supported"):
                client.execute_sql("WITH c AS (SELECT * FROM t) SELECT pk FROM c", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="WITH .CTE. is not supported"):
                client.execute_sql("WITH t AS (SELECT * FROM t) SELECT pk FROM t", schema_name=sn)
            # Envelope tail clauses GenericDialect parses and used to drop.
            with pytest.raises(gnitz.GnitzError, match="FOR UPDATE/SHARE is not supported"):
                client.execute_sql("SELECT pk FROM t FOR UPDATE", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="SETTINGS is not supported"):
                client.execute_sql("SELECT pk FROM t SETTINGS max_threads = 1", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="FORMAT is not supported"):
                client.execute_sql("SELECT pk FROM t FORMAT JSON", schema_name=sn)
            # INSERT source Query: the whole envelope was silently dropped, inserting every VALUES row.
            with pytest.raises(gnitz.GnitzError, match="LIMIT/OFFSET is not supported"):
                client.execute_sql("INSERT INTO t VALUES (100, 200) LIMIT 1", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="FOR UPDATE/SHARE is not supported"):
                client.execute_sql("INSERT INTO t VALUES (101, 201) FOR UPDATE", schema_name=sn)
            # Ordinary direct SELECT and plain LIMIT are unaffected.
            res = client.execute_sql("SELECT pk FROM t", schema_name=sn)
            assert res[0]["type"] == "Rows"
            assert len(res[0]["rows"]) == 5
            res2 = client.execute_sql("SELECT * FROM t LIMIT 2", schema_name=sn)
            assert len(res2[0]["rows"]) == 2
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_dml_unsupported_clauses_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            # INSERT ... RETURNING is supported (see test_serial.py); UPDATE and
            # DELETE RETURNING remain unsupported.
            with pytest.raises(gnitz.GnitzError, match="UPDATE: RETURNING is not supported"):
                client.execute_sql("UPDATE t SET val = 9 WHERE pk = 1 RETURNING pk", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="DELETE: RETURNING is not supported"):
                client.execute_sql("DELETE FROM t WHERE pk = 1 RETURNING pk", schema_name=sn)
            # ORDER BY is rejected before LIMIT, so each is its own statement.
            with pytest.raises(gnitz.GnitzError, match="DELETE: LIMIT is not supported"):
                client.execute_sql("DELETE FROM t LIMIT 1", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="DELETE: ORDER BY is not supported"):
                client.execute_sql("DELETE FROM t ORDER BY pk", schema_name=sn)
            # FROM/USING rejection fires before any table resolution, so `other` need not exist.
            with pytest.raises(gnitz.GnitzError, match="UPDATE: FROM .join-update. is not supported"):
                client.execute_sql("UPDATE t SET val = u.val FROM other u WHERE t.pk = u.pk", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="DELETE: USING .join-delete. is not supported"):
                client.execute_sql("DELETE FROM t USING other u WHERE t.pk = u.pk", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="INSERT: IGNORE is not supported"):
                client.execute_sql("INSERT IGNORE INTO t VALUES (6, 60)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="INSERT: REPLACE INTO is not supported"):
                client.execute_sql("REPLACE INTO t VALUES (6, 60)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="AS SELECT .CTAS. is not supported"):
                client.execute_sql("CREATE TABLE t2 (pk BIGINT PRIMARY KEY) AS SELECT pk FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="CREATE TABLE: TEMPORARY is not supported"):
                client.execute_sql("CREATE TEMPORARY TABLE tmp (pk BIGINT PRIMARY KEY)", schema_name=sn)
            # Happy paths unaffected.
            client.execute_sql("INSERT INTO t VALUES (6, 60)", schema_name=sn)
            client.execute_sql("UPDATE t SET val = 99 WHERE pk = 6", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE pk = 6", schema_name=sn)
            client.execute_sql("CREATE TABLE t3 (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_ddl_unsupported_clauses_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE p (k BIGINT PRIMARY KEY)", schema_name=sn)
            client.execute_sql("CREATE TABLE t (pk BIGINT PRIMARY KEY, c BIGINT)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="CREATE VIEW: output column aliases is not supported"):
                client.execute_sql("CREATE VIEW v (a) AS SELECT pk FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="CREATE VIEW: TEMPORARY is not supported"):
                client.execute_sql("CREATE TEMPORARY VIEW vt AS SELECT pk FROM t", schema_name=sn)
            # MATERIALIZED accepted (gnitz views are incrementally materialized).
            client.execute_sql("CREATE MATERIALIZED VIEW vm AS SELECT pk FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="CREATE INDEX: WHERE .partial index. is not supported"):
                client.execute_sql("CREATE INDEX ix ON t (c) WHERE c > 0", schema_name=sn)
            # Column/table CHECK, DEFAULT silently dropped → constraint never enforced.
            with pytest.raises(gnitz.GnitzError, match="column definition: CHECK is not supported"):
                client.execute_sql("CREATE TABLE c1 (pk BIGINT PRIMARY KEY, x BIGINT CHECK (x > 0))", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="table constraint: CHECK constraint is not supported"):
                client.execute_sql("CREATE TABLE c2 (pk BIGINT PRIMARY KEY, x BIGINT, CHECK (x > 0))", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="column definition: DEFAULT is not supported"):
                client.execute_sql("CREATE TABLE d1 (pk BIGINT PRIMARY KEY, x BIGINT DEFAULT 5)", schema_name=sn)
            # FK referential action silently dropped.
            with pytest.raises(gnitz.GnitzError, match="FOREIGN KEY ON DELETE/ON UPDATE action"):
                client.execute_sql(
                    "CREATE TABLE f (pk BIGINT PRIMARY KEY, c BIGINT REFERENCES p(k) ON DELETE CASCADE)",
                    schema_name=sn,
                )
            # Happy paths unaffected; USING BTREE is the accepted default.
            client.execute_sql("CREATE VIEW v2 AS SELECT pk FROM t", schema_name=sn)
            client.execute_sql("CREATE INDEX ix2 ON t (c)", schema_name=sn)
            client.execute_sql("CREATE INDEX ixb ON t USING BTREE (c)", schema_name=sn)
            client.execute_sql("CREATE TABLE f2 (pk BIGINT PRIMARY KEY, c BIGINT REFERENCES p(k))", schema_name=sn)
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


# ---------------------------------------------------------------------------
# TestInListViewFilter — the IN-list desugar through the engine expression VM
# ---------------------------------------------------------------------------

class TestInListViewFilter:
    def test_view_where_in_list(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val IN (10, 30)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # A NULL val makes every Eq NULL → the OR-chain NULL → row excluded.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert sorted(r.pk for r in rows) == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_view_where_not_in_excludes_null(self, client):
        """NOT IN over a NULL operand is NOT(NULL) = NULL → excluded (SQL 3VL)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val NOT IN (10, 30)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert sorted(r.pk for r in rows) == [2]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_view_where_string_in_list(self, client):
        """String elements route through the EXPR_STR_COL_EQ_CONST lowering."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, tag TEXT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE tag IN ('red', 'blue')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'red'), (2, 'green'), (3, 'blue'), (4, NULL)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert sorted(r.pk for r in rows) == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)
