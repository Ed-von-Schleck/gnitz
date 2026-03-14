import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def test_push_scan_multiworker(client):
    """Push 200 rows and scan back — all must be present."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "big", cols)

    n = 200
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, n + 1):
        batch.append(pk=i, val=i * 10)
    client.push(tid, schema, batch)

    result = client.scan(tid)
    pks = sorted(row.pk for row in result if row.weight > 0)
    assert pks == list(range(1, n + 1))

    client.drop_table(sn, "big")
    client.drop_schema(sn)
