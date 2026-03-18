import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def test_create_drop_view(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "src", cols)
    vid = client.create_view(sn, "v", tid, gnitz.Schema(cols))
    assert vid > 0
    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)


def test_view_on_view(client):
    """A SQL view reading from another SQL view propagates inserts end-to-end."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, cat BIGINT NOT NULL)",
            schema_name=sn,
        )
        # v1: filter to cat=1
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT * FROM t WHERE cat = 1",
            schema_name=sn,
        )
        # v2: filter v1 to val > 10
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 10",
            schema_name=sn,
        )
        v2_id = client.resolve_table(sn, "v2")[0]

        client.execute_sql(
            "INSERT INTO t VALUES (1, 5, 1), (2, 20, 1), (3, 30, 2), (4, 15, 1)",
            schema_name=sn,
        )

        rows = client.scan(v2_id)
        # cat=1 AND val>10: rows 2 (val=20) and 4 (val=15)
        assert len(rows) == 2, f"expected 2 rows from view-on-view, got {len(rows)}: {rows}"
        vals = sorted(r["val"] for r in rows)
        assert vals == [15, 20]

        client.execute_sql("DROP VIEW v2", schema_name=sn)
        client.execute_sql("DROP VIEW v1", schema_name=sn)
        client.execute_sql("DROP TABLE t", schema_name=sn)
    finally:
        client.drop_schema(sn)


def test_view_scan_propagates_inserts(client):
    """Push rows to source; scan view returns same rows."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "src", cols)
    vid = client.create_view(sn, "v", tid, schema)

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10).append(pk=2, val=20).append(pk=3, val=30)
    client.push(tid, batch)

    result = client.scan(vid)
    assert len(result) == 3

    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)
