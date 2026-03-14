import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _setup(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def test_push_and_scan(client):
    sn, tid, schema = _setup(client)
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, 6):
        batch.append(pk=i, val=i * 10)
    client.push(tid, schema, batch)
    result = client.scan(tid)
    assert len(result) == 5
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_scan_empty_table(client):
    sn, tid, schema = _setup(client)
    result = client.scan(tid)
    assert len(result) == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_rows(client):
    sn, tid, schema = _setup(client)
    batch = gnitz.ZSetBatch(schema)
    for pk, val in [(1, 10), (2, 20), (3, 30)]:
        batch.append(pk=pk, val=val)
    client.push(tid, schema, batch)
    client.delete(tid, schema, [2])
    result = client.scan(tid)
    pks = sorted(row.pk for row in result if row.weight > 0)
    assert pks == [1, 3]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_nullable_string_columns(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "strs", cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, s="hello")
    batch.append(pk=2, s=None)
    batch.append(pk=3, s="world")
    client.push(tid, schema, batch)
    result = client.scan(tid)
    assert len(result) == 3
    client.drop_table(sn, "strs")
    client.drop_schema(sn)


def test_scan_values_correct(client):
    """Verify pushed values round-trip through scan correctly."""
    sn, tid, schema = _setup(client)
    rows_in = [(i, i * 100) for i in range(1, 11)]
    batch = gnitz.ZSetBatch(schema)
    for pk, val in rows_in:
        batch.append(pk=pk, val=val)
    client.push(tid, schema, batch)
    result = client.scan(tid)
    assert len(result) == 10
    pairs = sorted((row.pk, row.val) for row in result)
    assert pairs == rows_in
    client.drop_table(sn, "t")
    client.drop_schema(sn)
