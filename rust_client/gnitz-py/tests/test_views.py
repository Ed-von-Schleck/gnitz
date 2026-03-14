import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def test_create_drop_view(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "src", cols)
    vid = client.create_view(sn, "v", tid, gnitz.Schema(cols, pk_index=0))
    assert vid > 0
    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)


def test_view_scan_propagates_inserts(client):
    """Push rows to source; scan view returns same rows."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "src", cols)
    vid = client.create_view(sn, "v", tid, schema)

    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend([1, 2, 3])
    batch.pk_hi.extend([0, 0, 0])
    batch.weights.extend([1, 1, 1])
    batch.nulls.extend([0, 0, 0])
    batch.columns[1].extend([10, 20, 30])
    client.push(tid, schema, batch)

    _, result = client.scan(vid)
    assert result is not None
    assert len(result.pk_lo) == 3

    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)
