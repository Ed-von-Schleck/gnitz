"""`CREATE VIEW` over one relation: a projection and a WHERE, maintained from the
inserts that follow the CREATE.
"""

import os

import pytest
from _uid import uid as _uid




_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="the read/DDL concurrency path only exercises exchange/fanout at W >= 2",
)


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
