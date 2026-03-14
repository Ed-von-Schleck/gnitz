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
    client.push(tid, schema, batch)

    result = client.scan(vid)
    assert len(result) == 3

    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)
