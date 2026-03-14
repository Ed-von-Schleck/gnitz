import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _setup(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def _push_rows(client, tid, schema, rows):
    """rows: list of (pk, val) tuples, weight=1"""
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend(pk for pk, _ in rows)
    batch.pk_hi.extend([0] * len(rows))
    batch.weights.extend([1] * len(rows))
    batch.nulls.extend([0] * len(rows))
    batch.columns[1].extend(val for _, val in rows)
    client.push(tid, schema, batch)


def test_push_and_scan(client):
    sn, tid, schema = _setup(client)
    _push_rows(client, tid, schema, [(i, i * 10) for i in range(1, 6)])
    _, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 5
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_scan_empty_table(client):
    sn, tid, schema = _setup(client)
    _, result = client.scan(tid)
    assert result is None or len(result.pk_lo) == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_rows(client):
    sn, tid, schema = _setup(client)
    _push_rows(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    client.delete(tid, schema, [2])  # delete pk=2
    _, result = client.scan(tid)
    assert result is not None
    pks = sorted(result.pk_lo[i] for i in range(len(result.pk_lo))
                 if result.weights[i] > 0)
    assert pks == [1, 3]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_nullable_string_columns(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]
    schema = gnitz.Schema(cols, pk_index=0)
    tid = client.create_table(sn, "strs", cols)
    batch = gnitz.ZSetBatch(schema)
    batch.pk_lo.extend([1, 2, 3])
    batch.pk_hi.extend([0, 0, 0])
    batch.weights.extend([1, 1, 1])
    batch.nulls.extend([0, 2, 0])
    batch.columns[1].extend(["hello", None, "world"])
    client.push(tid, schema, batch)
    _, result = client.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == 3
    client.drop_table(sn, "strs")
    client.drop_schema(sn)


def test_scan_values_correct(client):
    """Verify pushed values round-trip through scan correctly."""
    sn, tid, schema = _setup(client)
    rows = [(i, i * 100) for i in range(1, 11)]
    _push_rows(client, tid, schema, rows)
    _, result = client.scan(tid)
    assert result is not None
    n = len(result.pk_lo)
    assert n == 10
    pairs = sorted((result.pk_lo[i], result.columns[1][i]) for i in range(n))
    assert pairs == rows
    client.drop_table(sn, "t")
    client.drop_schema(sn)
