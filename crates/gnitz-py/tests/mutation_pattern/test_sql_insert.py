"""`INSERT INTO … VALUES` through the SQL front end: multi-row, the explicit
`ROW` keyword, and the singular `VALUE` spelling.
"""

import os

import pytest
from _uid import uid as _uid




_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="the read/DDL concurrency path only exercises exchange/fanout at W >= 2",
)


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

    def test_insert_explicit_row_keyword(self, client):
        """MySQL's `VALUES ROW(...)` spelling parses to the same rows as
        `VALUES (...)`, so it is accepted and inserts the identical row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("INSERT INTO t VALUES ROW(1, 10), ROW(2, 20)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r["pk"], r["val"]) for r in client.scan(tid).mappings())
            assert rows == [(1, 10), (2, 20)]
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_insert_singular_value_keyword(self, client):
        """MySQL's singular `VALUE` spelling of `VALUES` carries no semantic
        content, so it is accepted and inserts the identical row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("INSERT INTO t VALUE (7, 70)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r["pk"], r["val"]) for r in client.scan(tid).mappings())
            assert rows == [(7, 70)]
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
