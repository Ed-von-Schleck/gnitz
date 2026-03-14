import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def test_push_scan_multiworker(client):
    """Push 200 rows and scan back — all must be present."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "big", cols)

    n = 200
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend(range(1, n + 1))
    batch.pk_hi.extend([0] * n)
    batch.weights.extend([1] * n)
    batch.nulls.extend([0] * n)
    batch.columns[1].extend(i * 10 for i in range(1, n + 1))
    client.push(tid, schema, batch)

    _, result = client.scan(tid)
    assert result is not None
    pks = sorted(result.pk_lo[i] for i in range(len(result.pk_lo))
                 if result.weights[i] > 0)
    assert pks == list(range(1, n + 1))

    client.drop_table(sn, "big")
    client.drop_schema(sn)
