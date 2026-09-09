"""The ad-hoc `SELECT` read path: what a direct single-relation read serves
without a view behind it.

`SELECT *` over a table and over a view, a PK seek, a non-indexed WHERE the
read path evaluates server-side, a pass-through CTE, `SELECT` with no FROM,
and LIMIT. What this path *refuses* is in
`admissibility/test_sql_rejections.py`.
"""

import os

import pytest
import gnitz
from _uid import uid as _uid




_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="the read/DDL concurrency path only exercises exchange/fanout at W >= 2",
)


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

    def test_cte_expands_into_the_direct_path(self, client):
        """A CTE over one relation is a macro expanded into the body, so every
        single-relation shape reads via the direct path — an identity, a
        narrowing or computed projection, a WHERE in the CTE conjoined with the
        outer one, a chain, and a fold over it — and reads exactly what the
        flat query reads."""
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

            for cte, flat in [
                ("WITH x AS (SELECT pk, val AS q FROM t) SELECT pk, q FROM x ORDER BY q DESC", "SELECT pk, val AS q FROM t ORDER BY val DESC"),
                ("WITH x AS (SELECT pk, val * 2 AS d FROM t WHERE val > 10) SELECT d FROM x WHERE d < 100 ORDER BY d", "SELECT val * 2 AS d FROM t WHERE val > 10 AND val * 2 < 100 ORDER BY d"),
                ("WITH x(i, j) AS (SELECT pk, val FROM t), y AS (SELECT j FROM x WHERE i > 3) SELECT SUM(j) AS s FROM y", "SELECT SUM(val) AS s FROM t WHERE pk > 3"),
                ("WITH x AS (SELECT val + 1 AS q FROM t) SELECT q, COUNT(*) AS n FROM x GROUP BY q ORDER BY q", "SELECT val + 1 AS q, COUNT(*) AS n FROM t GROUP BY val + 1 ORDER BY q"),
                ("WITH x AS (SELECT * EXCEPT (val) FROM t) SELECT * FROM x ORDER BY pk", "SELECT pk FROM t ORDER BY pk"),
            ]:
                got = [(tuple(r), r.weight) for r in client.execute_sql(cte, schema_name=sn)[0]["rows"]]
                want = [(tuple(r), r.weight) for r in client.execute_sql(flat, schema_name=sn)[0]["rows"]]
                assert got == want and got, f"{cte!r}: {got} != {want}"
            # A CTE exposes only what it projects, whatever the source holds.
            with pytest.raises(gnitz.GnitzError, match="column 'val' not found"):
                client.execute_sql("WITH x AS (SELECT pk FROM t) SELECT val FROM x", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_without_from(self, client):
        """A FROM-less SELECT reads nothing and answers one constant row — the
        probe a driver or health check sends — under the computed-column names,
        with ORDER BY / LIMIT honored."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            res = client.execute_sql("SELECT 1", schema_name=sn)
            assert res[0]["type"] == "Rows", res[0]
            rows = list(res[0]["rows"])
            assert len(rows) == 1 and rows[0].weight == 1, rows
            assert tuple(rows[0]) == (1,), rows
            res = client.execute_sql("SELECT 1 + 2 AS three, 'x' AS s, 2.5 AS f", schema_name=sn)
            r = list(res[0]["rows"])[0]
            assert (r.three, r.s, r.f) == (3, "x", 2.5), r
            assert list(client.execute_sql("SELECT 1 AS a LIMIT 0", schema_name=sn)[0]["rows"]) == []
            with pytest.raises(gnitz.GnitzError, match="column 'x' not found"):
                client.execute_sql("SELECT x", schema_name=sn)
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
