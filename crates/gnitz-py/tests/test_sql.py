"""Integration tests for the gnitz-sql SQL frontend.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_sql.py -v --tb=short
"""
import os
import random
import threading
import time

import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="the read/DDL concurrency path only exercises exchange/fanout at W >= 2",
)


@_NEEDS_MULTI
def test_create_view_under_concurrent_adhoc_reads(client, server):
    """CREATE VIEW must not wedge under concurrent ad-hoc reads and pushes.

    A CREATE VIEW parks the single-threaded reactor (drain_tick_blocking +
    fan_out_backfill are synchronous futex loops) under the catalog write lock.
    Ad-hoc reads take only the catalog READ lock for the whole of one atomic
    fan-out (no mid-flight release), so the writer-preferring catalog_rwlock
    serialises each read entirely before or after the DDL window — never
    interleaved, never wedged. Reads before/during/after the CREATE VIEW must all
    succeed, ingestion must continue, and the post-DDL read must agree with the
    view built mid-flight.
    """
    sn = "s" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn
    )
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i % 5})" for i in range(1, 201)), schema_name=sn
    )
    client.execute_sql(
        "CREATE TABLE other (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
    )

    errors = []
    ddl_done = threading.Event()
    q_count = [0]

    def hammer_reads():
        try:
            with gnitz.connect(server) as c:
                while not ddl_done.is_set() and q_count[0] < 400:
                    res = c.execute_sql("SELECT g, COUNT(*) AS n FROM t GROUP BY g", schema_name=sn)
                    assert res[0]["type"] == "Rows"
                    n = sum(r.n for r in res[0]["rows"])
                    assert n == 200, f"ad-hoc read must see all 200 rows, saw {n}"
                    q_count[0] += 1
        except Exception as e:  # noqa: BLE001
            errors.append(("read", repr(e)))

    def hammer_pushes():
        try:
            with gnitz.connect(server) as c:
                i = 0
                while not ddl_done.is_set() and i < 400:
                    c.execute_sql(f"INSERT INTO other VALUES ({i}, {i})", schema_name=sn)
                    i += 1
        except Exception as e:  # noqa: BLE001
            errors.append(("push", repr(e)))

    tq = threading.Thread(target=hammer_reads)
    tp = threading.Thread(target=hammer_pushes)
    tq.start()
    tp.start()

    time.sleep(0.05)
    q_before = q_count[0]
    t0 = time.time()
    client.execute_sql("CREATE VIEW mid AS SELECT g, COUNT(*) AS n FROM t GROUP BY g", schema_name=sn)
    ddl_secs = time.time() - t0
    q_across = q_count[0] - q_before
    ddl_done.set()

    tq.join(timeout=120)
    tp.join(timeout=120)
    assert not tq.is_alive() and not tp.is_alive(), "deadlock: a concurrent worker never completed"
    assert not errors, f"concurrent work failed: {errors}"
    assert ddl_secs < 60, f"CREATE VIEW took {ddl_secs:.1f}s -- wedged behind the reads"
    assert q_across > 0, (
        "no ad-hoc read overlapped the CREATE VIEW -- the test proved nothing about read/DDL concurrency"
    )

    # The mid-flight DDL produced a correct view, and ad-hoc reads still agree.
    def _rows(sql):
        res = client.execute_sql(sql, schema_name=sn)
        out = []
        for r in res[0]["rows"]:
            if r.weight <= 0:
                continue
            d = r._asdict()
            out.append((tuple(d[k] for k in sorted(d)), r.weight))
        return sorted(out)

    assert _rows("SELECT g, COUNT(*) AS n FROM t GROUP BY g") == _rows("SELECT * FROM mid"), (
        "post-DDL ad-hoc read must agree with the view built mid-flight"
    )


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

    def test_select_nonindexed_where_is_served_by_the_read_path(self, client):
        """A WHERE on a non-indexed column is SERVED, not rejected.

        The residual compiles into the server-side read-spec predicate (a bounded
        read with no usable seek key degrades to a full cursor + predicate), so the
        query is served directly — no circuit, no per-query state.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)  # (1,10) … (5,50)
            res = client.execute_sql("SELECT * FROM t WHERE val = 30", schema_name=sn)
            assert res[0]["type"] == "Rows"
            rows = list(res[0]["rows"])
            assert [(r.pk, r.val) for r in rows] == [(3, 30)], f"val = 30 selects exactly row pk=3, got {rows}"

            # A non-equality residual routes the same way.
            res = client.execute_sql("SELECT * FROM t WHERE val > 30", schema_name=sn)
            rows = list(res[0]["rows"])
            assert sorted(r.pk for r in rows) == [4, 5], f"val > 30 selects pks 4,5, got {rows}"
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_derivation_shapes_rejected_with_create_view_advice(self, client):
        """An ad-hoc SELECT reads one relation; a query that derives a new one
        (JOIN, set-op, EXISTS/IN or scalar subquery, derived table, non-pass-through
        CTE) is rejected from the AST alone with one template naming the construct
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
                ("WITH c AS (SELECT pk FROM t WHERE val > 20) SELECT pk FROM c", "non-pass-through CTE"),
            ]
            for sql, construct in cases:
                with pytest.raises(gnitz.GnitzError) as ei:
                    client.execute_sql(sql, schema_name=sn)
                msg = str(ei.value)
                assert "this query derives a new one" in msg, f"{sql!r}: not the derivation template: {msg}"
                assert f"({construct})" in msg, f"{sql!r}: must name '{construct}', got: {msg}"
                assert "CREATE VIEW" in msg, f"{sql!r}: must point at CREATE VIEW, got: {msg}"

            # A comma-join is a shape CREATE VIEW rejects too, so the template's
            # "CREATE VIEW AS <your query>" advice would be false for it: it gets
            # its own message advising the explicit-JOIN rewrite.
            with pytest.raises(gnitz.GnitzError) as ei:
                client.execute_sql("SELECT pk FROM t, u WHERE t.val = u.k", schema_name=sn)
            msg = str(ei.value)
            assert "this query derives a new one" not in msg, f"comma-join must not use the template: {msg}"
            assert "comma-join" in msg and "explicit JOIN" in msg and "CREATE VIEW" in msg, (
                f"comma-join must advise the explicit-JOIN + CREATE VIEW rewrite, got: {msg}"
            )
        finally:
            client.drop_schema(sn)

    def test_direct_path_feature_limits_are_not_derivation_errors(self, client):
        """A single-relation read using a feature the direct path cannot express
        (LIKE / string-function WHERE, an ORDER BY expression) is a feature-named
        error — never the derivation template. A string HAVING is not one of them:
        it compiles through the same expression compiler a grouped view's
        post-reduce FILTER uses, so it is served."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, s TEXT)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b')", schema_name=sn)
            for sql in ["SELECT pk FROM t WHERE s LIKE 'a%'", "SELECT pk FROM t ORDER BY pk + 1"]:
                with pytest.raises(gnitz.GnitzError) as ei:
                    client.execute_sql(sql, schema_name=sn)
                assert "this query derives a new one" not in str(ei.value), (
                    f"{sql!r} is a feature limit, not a derivation: {ei.value}"
                )
            # A string HAVING is served on the direct path.
            res = client.execute_sql(
                "SELECT s, COUNT(*) AS c FROM t GROUP BY s HAVING s = 'a'", schema_name=sn
            )
            assert res[0]["type"] == "Rows", res[0]
            got = sorted((r.s, r.c) for r in res[0]["rows"])
            assert got == [("a", 1)], f"string HAVING must be served on the direct path, got {got}"
        finally:
            client.drop_schema(sn)

    def test_passthrough_cte_reads_through_the_direct_path(self, client):
        """A pass-through CTE over one relation inlines to it and reads via the
        direct path — WHERE over the aliased CTE still filters."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_with_rows(client, sn)  # t(pk, val): (1,10)…(5,50)
            res = client.execute_sql(
                "WITH x AS (SELECT * FROM t) SELECT pk FROM x WHERE val > 30", schema_name=sn
            )
            assert res[0]["type"] == "Rows"
            pks = sorted(r.pk for r in res[0]["rows"])
            assert pks == [4, 5], f"the CTE inlines to t and the WHERE filters val > 30, got {pks}"

            # Over a view, too.
            client.execute_sql("CREATE VIEW v_hi AS SELECT pk, val FROM t WHERE val >= 30", schema_name=sn)
            res = client.execute_sql(
                "WITH y AS (SELECT * FROM v_hi) SELECT pk FROM y WHERE val = 50", schema_name=sn
            )
            pks = sorted(r.pk for r in res[0]["rows"])
            assert pks == [5], f"the CTE inlines to the view and the WHERE filters val = 50, got {pks}"
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
            # Plain DISTINCT / GROUP BY / HAVING are served by the fold sink and a
            # pass-through WITH by the read path; this guard test keeps only the
            # clauses the direct path has no operator for.
            # DISTINCT ON gets its own message (no CREATE VIEW redirect).
            with pytest.raises(gnitz.GnitzError, match="DISTINCT ON is not supported"):
                client.execute_sql("SELECT DISTINCT ON (pk) pk FROM t", schema_name=sn)
            # OFFSET and ORDER BY are now honored by the client-side sink (see
            # test_order_by.py); only the ClickHouse `LIMIT … BY` per-group form
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
            rows = list(client.scan(vid))
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
            rows = list(client.scan(vid))
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
            rows = list(client.scan(vid))
            assert sorted(r.pk for r in rows) == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)
