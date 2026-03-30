"""E2E tests for unique_pk UPSERT/DELETE-by-PK enforcement."""

import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _make_table(client, unique_pk=True):
    """Create schema + (pk U64 PK, val I64) table. Returns (tid, schema, sn)."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols, unique_pk=unique_pk)
    return tid, schema, sn


def _push(client, tid, schema, rows, weight=1):
    """Push [(pk, val), ...] with given weight."""
    batch = gnitz.ZSetBatch(schema)
    for pk, val in rows:
        batch.append(pk=pk, val=val, weight=weight)
    client.push(tid, batch)


def _scan_rows(client, tid):
    """Return sorted list of (pk, val) for all positive-weight rows."""
    return sorted(
        (row.pk, row.val)
        for row in client.scan(tid)
        if row.weight > 0
    )


def test_upsert_insert_new(client):
    """Push a new PK — row appears in scan."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    assert _scan_rows(client, tid) == [(1, 10)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_upsert_update_existing(client):
    """Push same PK twice — second value wins."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    _push(client, tid, schema, [(1, 20)])
    assert _scan_rows(client, tid) == [(1, 20)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_by_pk_existing(client):
    """Push row then delete by PK — row disappears."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    client.delete(tid, schema, [1])
    assert _scan_rows(client, tid) == []
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_by_pk_absent(client):
    """Delete a PK that was never inserted — no error, 0 rows."""
    tid, schema, sn = _make_table(client)
    client.delete(tid, schema, [999])
    assert _scan_rows(client, tid) == []
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_intra_batch_overwrite(client):
    """Single batch with two inserts for the same PK — last value wins."""
    tid, schema, sn = _make_table(client)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=7, val=10)
    batch.append(pk=7, val=20)
    client.push(tid, batch)
    assert _scan_rows(client, tid) == [(7, 20)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_raw_table_allows_duplicates(client):
    """unique_pk=False table accumulates weights; scan shows weight=2 after two pushes."""
    tid, schema, sn = _make_table(client, unique_pk=False)
    _push(client, tid, schema, [(1, 10)])
    _push(client, tid, schema, [(1, 10)])  # accumulates to w=+2
    rows = [(row.pk, row.val, row.weight) for row in client.scan(tid)]
    assert rows == [(1, 10, 2)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_multiple_keys_independent(client):
    """Multiple distinct PKs are each handled independently."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    _push(client, tid, schema, [(2, 99)])  # update pk=2 only
    assert _scan_rows(client, tid) == [(1, 10), (2, 99), (3, 30)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_then_reinsert(client):
    """Insert pk=1 val=10, delete pk=1, insert pk=1 val=20; scan shows [(1, 20)]."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    client.delete(tid, schema, [1])
    _push(client, tid, schema, [(1, 20)])
    assert _scan_rows(client, tid) == [(1, 20)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_multiple_pks(client):
    """Insert pk=1,2,3; delete pk=1,3 in one call; scan shows [(2, 20)]."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    client.delete(tid, schema, [1, 3])
    assert _scan_rows(client, tid) == [(2, 20)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_upsert_intra_batch_insert_then_delete(client):
    """Single batch: insert pk=1 then retract pk=1; scan shows empty."""
    tid, schema, sn = _make_table(client)
    # val=0 in the retraction differs from the inserted val=10, but unique_pk
    # tables retract by PK match regardless of payload values.
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10, weight=1)
    batch.append(pk=1, val=0, weight=-1)
    client.push(tid, batch)
    assert _scan_rows(client, tid) == []
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_upsert_intra_batch_delete_then_insert(client):
    """Pre-insert pk=1 val=10; single batch: retract pk=1 then insert pk=1 val=99."""
    tid, schema, sn = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    # val=0 in the retraction differs from the inserted val=10, but unique_pk
    # tables retract by PK match regardless of payload values.
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=0, weight=-1)
    batch.append(pk=1, val=99, weight=1)
    client.push(tid, batch)
    assert _scan_rows(client, tid) == [(1, 99)]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_upsert_updates_view(client):
    """Create table + passthrough view; upsert pk=1; view reflects new value only."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    vid = client.create_view(sn, "v", tid, schema)

    _push(client, tid, schema, [(1, 10)])
    assert _scan_rows(client, vid) == [(1, 10)]

    _push(client, tid, schema, [(1, 99)])
    assert _scan_rows(client, vid) == [(1, 99)]

    client.drop_view(sn, "v")
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_updates_view(client):
    """Create table + passthrough view; delete pk=1; view shows empty."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    vid = client.create_view(sn, "v", tid, schema)

    _push(client, tid, schema, [(1, 10)])
    assert _scan_rows(client, vid) == [(1, 10)]

    client.delete(tid, schema, [1])
    assert _scan_rows(client, vid) == []

    client.drop_view(sn, "v")
    client.drop_table(sn, "t")
    client.drop_schema(sn)
