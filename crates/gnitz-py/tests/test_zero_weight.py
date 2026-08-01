"""E2E tests for weight-0 push rows: not Z-set elements, dropped before the SAL
emission (so the workers never see them)."""

import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _make_table(client, cols, name="t"):
    """Create a schema + table from `cols`. Returns (tid, schema, sn)."""
    sn = "s" + _uid()
    client.create_schema(sn)
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, name, cols)
    return tid, schema, sn


def _int_cols():
    return [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]


def _str_cols():
    """A STRING column makes the schema non-wire-safe, so its SAL group is scattered."""
    return [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]


def _live(client, tid, *cols):
    return sorted(tuple(getattr(r, c) for c in cols) for r in client.scan(tid) if r.weight > 0)


def _drop(client, sn):
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_zero_weight_push_is_dropped(client):
    """A weight-0 row is not stored, and the server stays up."""
    tid, schema, sn = _make_table(client, _int_cols())
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10, _weight=0)
    client.push(tid, batch)
    assert _live(client, tid, "pk", "val") == []

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=2, val=20)
    client.push(tid, batch)
    assert _live(client, tid, "pk", "val") == [(2, 20)]
    _drop(client, sn)


def test_zero_weight_push_string_column(client):
    """Same on a STRING-column table, whose group takes the scatter path."""
    tid, schema, sn = _make_table(client, _str_cols())
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, s="ghost", _weight=0)
    client.push(tid, batch)
    assert _live(client, tid, "pk", "s") == []

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=2, s="live")
    client.push(tid, batch)
    assert _live(client, tid, "pk", "s") == [(2, "live")]
    _drop(client, sn)


def test_zero_weight_row_in_transaction(client):
    """A weight-0 row inside a transaction on a STRING table commits cleanly."""
    tid, schema, sn = _make_table(client, _str_cols())
    with client.transaction() as txn:
        b = gnitz.ZSetBatch(schema)
        b.append(pk=1, s="live")
        b.append(pk=2, s="ghost", _weight=0)
        txn.push(tid, b)
    assert _live(client, tid, "pk", "s") == [(1, "live")]
    _drop(client, sn)


def test_all_zero_weight_push_is_a_noop(client):
    """An all-zero batch ACKs at a real zone LSN; the next push still lands."""
    tid, schema, sn = _make_table(client, _int_cols())
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10, _weight=0)
    batch.append(pk=2, val=20, _weight=0)
    assert client.push(tid, batch) > 0
    assert _live(client, tid, "pk", "val") == []

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=3, val=30)
    client.push(tid, batch)
    assert _live(client, tid, "pk", "val") == [(3, 30)]
    _drop(client, sn)


def test_zero_weight_mixed_with_live_rows(client):
    """Only the nonzero rows are stored, each at the weight enforcement gives it."""
    tid, schema, sn = _make_table(client, _int_cols())
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10, _weight=0)
    batch.append(pk=2, val=20, _weight=3)
    batch.append(pk=3, val=30, _weight=0)
    batch.append(pk=4, val=40, _weight=1)
    client.push(tid, batch)
    rows = sorted((r.pk, r.val, r.weight) for r in client.scan(tid))
    # Every accumulated PK weight on a base table is clamped to 1.
    assert rows == [(2, 20, 1), (4, 40, 1)]
    _drop(client, sn)
