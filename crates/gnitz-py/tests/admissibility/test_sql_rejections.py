"""What SQL refuses, and with which message.

A query that derives a new relation (JOIN, set-op, EXISTS/IN, scalar subquery,
derived table, grouped CTE) is rejected from the AST alone with one template
pointing at CREATE VIEW. A single-relation read using a feature the direct
path cannot express is a feature-named error instead — never the derivation
template. The rest are clauses no statement family supports.
"""

import os

import pytest
import gnitz
from _uid import uid as _uid




_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="the read/DDL concurrency path only exercises exchange/fanout at W >= 2",
)


class TestSqlRejections:

    def _setup_with_rows(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
            schema_name=sn,
        )

    def test_derivation_shapes_rejected_with_create_view_advice(self, client):
        """An ad-hoc SELECT reads one relation; a query that derives a new one
        (JOIN, set-op, EXISTS/IN or scalar subquery, derived table, grouped CTE)
        is rejected from the AST alone with one template naming the construct
        and pointing at CREATE VIEW."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)  # t(pk, val)
            client.execute_sql(
                "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql("INSERT INTO u VALUES (1, 10), (2, 20)", schema_name=sn)
            cases = [
                ("SELECT t.pk FROM t JOIN u ON t.val = u.k", "JOIN"),
                ("SELECT val FROM t UNION SELECT k FROM u", "set operation"),
                ("SELECT val FROM t INTERSECT SELECT k FROM u", "set operation"),
                ("SELECT val FROM t EXCEPT SELECT k FROM u", "set operation"),
                ("SELECT pk FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.val)", "EXISTS/IN subquery"),
                ("SELECT pk FROM t WHERE val IN (SELECT k FROM u)", "EXISTS/IN subquery"),
                ("SELECT pk FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.k = t.val)", "EXISTS/IN subquery"),
                ("SELECT pk, (SELECT MAX(k) FROM u) FROM t", "scalar subquery"),
                ("SELECT x FROM (SELECT val AS x FROM t) d", "derived table in FROM"),
                ("WITH c AS (SELECT val, COUNT(*) AS n FROM t GROUP BY val) SELECT val FROM c", "grouped CTE"),
                ("SELECT t.pk FROM t, u WHERE t.val = u.k", "comma-join FROM"),
            ]
            for sql, construct in cases:
                with pytest.raises(gnitz.GnitzError) as ei:
                    client.execute_sql(sql, schema_name=sn)
                msg = str(ei.value)
                assert "this query derives a new one" in msg, f"{sql!r}: not the derivation template: {msg}"
                assert f"({construct})" in msg, f"{sql!r}: must name '{construct}', got: {msg}"
                assert "CREATE VIEW" in msg, f"{sql!r}: must point at CREATE VIEW, got: {msg}"

            # The comma-join's advice is the template's, verbatim: a view body may
            # be written that way, so "CREATE VIEW AS <your query>" is literally
            # what the user should do next.
            client.execute_sql(
                "CREATE VIEW comma_ok AS SELECT t.pk FROM t, u WHERE t.val = u.k", schema_name=sn
            )
        finally:
            client.drop_schema(sn)

    def test_direct_path_feature_limits_are_not_derivation_errors(self, client):
        """A single-relation read using a feature the direct path cannot express
        (a LIKE whose pattern is not a literal) is a feature-named error — never
        the derivation template. A string HAVING is not one of them: it compiles
        through the same expression compiler a grouped view's post-reduce FILTER
        uses, so it is served — and so are a LIKE against a literal pattern and
        an ORDER BY expression."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, s TEXT)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b')", schema_name=sn)
            sql = "SELECT pk FROM t WHERE s LIKE s"
            with pytest.raises(gnitz.GnitzError) as ei:
                client.execute_sql(sql, schema_name=sn)
            assert "this query derives a new one" not in str(ei.value), (
                f"{sql!r} is a feature limit, not a derivation: {ei.value}"
            )
            res = client.execute_sql("SELECT pk FROM t ORDER BY 0 - pk", schema_name=sn)
            assert [r.pk for r in res[0]["rows"]] == [2, 1], res[0]
            # A string HAVING is served on the direct path.
            res = client.execute_sql(
                "SELECT s, COUNT(*) AS c FROM t GROUP BY s HAVING s = 'a'", schema_name=sn
            )
            assert res[0]["type"] == "Rows", res[0]
            got = sorted((r.s, r.c) for r in res[0]["rows"])
            assert got == [("a", 1)], f"string HAVING must be served on the direct path, got {got}"
            # A literal-pattern LIKE contributes no access path, but it is served
            # as a residual filter.
            res = client.execute_sql("SELECT pk FROM t WHERE s LIKE 'a%'", schema_name=sn)
            assert [r.pk for r in res[0]["rows"]] == [1], res[0]
        finally:
            client.drop_schema(sn)

    def test_select_unsupported_clauses_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            # Plain DISTINCT / GROUP BY / HAVING are served by the fold sink and a
            # pass-through WITH by the read path; this guard test keeps only the
            # clauses the direct path has no operator for.
            # DISTINCT ON gets its own message (no CREATE VIEW redirect).
            with pytest.raises(gnitz.GnitzError, match="DISTINCT ON is not supported"):
                client.execute_sql("SELECT DISTINCT ON (pk) pk FROM t", schema_name=sn)
            # OFFSET and ORDER BY are now honored by the client-side sink (see
            # read_verb/test_order_by.py); only the ClickHouse `LIMIT … BY` per-group form
            # stays rejected on the LIMIT envelope.
            with pytest.raises(gnitz.GnitzError, match=r"LIMIT \.\.\. BY is not supported"):
                client.execute_sql("SELECT * FROM t LIMIT 2 BY val", schema_name=sn)
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
            # ORDER BY + OFFSET are honored end-to-end now.
            res3 = client.execute_sql("SELECT * FROM t ORDER BY val DESC LIMIT 2", schema_name=sn)
            assert [r.pk for r in res3[0]["rows"]] == [5, 4]
            res4 = client.execute_sql("SELECT * FROM t ORDER BY val LIMIT 2 OFFSET 1", schema_name=sn)
            assert [r.pk for r in res4[0]["rows"]] == [2, 3]
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_dml_unsupported_clauses_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)
            # INSERT ... RETURNING is supported (see mutation_pattern/test_serial.py); UPDATE and
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
            with pytest.raises(gnitz.GnitzError, match="defines 2 column aliases but body returns 1"):
                client.execute_sql("CREATE VIEW vbad (a, b) AS SELECT pk FROM t", schema_name=sn)
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
