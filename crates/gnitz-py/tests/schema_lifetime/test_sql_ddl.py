"""CREATE / DROP of a table and a view through `execute_sql`, and the result
record each statement hands back.
"""


from _uid import uid as _uid




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
