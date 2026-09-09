"""Weight-0 push rows: not Z-set elements, dropped before the SAL emission, so
the workers never see them.

Varied across an integer-only table and a STRING one, whose SAL group takes the
sub-batch path rather than the scatter.
"""

import pytest
import gnitz

_INT_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
             gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
_STR_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
             gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]


@pytest.fixture
def table(client, schema_name):
    """Factory: `table(cols)` → `(tid, schema)` for a table over `cols`."""
    def make(cols):
        return client.create_table(schema_name, "t", cols), gnitz.Schema(cols)
    return make


def _live(client, tid, payload):
    """Live rows as `(pk, <payload>, weight)`, sorted."""
    return sorted((r.pk, getattr(r, payload), r.weight) for r in client.scan(tid))


@pytest.mark.parametrize("cols,payload,ghost,live", [
    (_INT_COLS, "val", 10, 20),
    (_STR_COLS, "s", "ghost", "live"),
], ids=["int", "string"])
def test_zero_weight_push_is_dropped(client, table, cols, payload, ghost, live):
    """A weight-0 row is not stored, and the next real push still lands."""
    tid, schema = table(cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, _weight=0, **{payload: ghost})
    client.push(tid, batch)
    assert _live(client, tid, payload) == []

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=2, **{payload: live})
    client.push(tid, batch)
    assert _live(client, tid, payload) == [(2, live, 1)]


def test_all_zero_weight_push_is_a_noop(client, table):
    """An all-zero batch ACKs at a real zone LSN; the next push still lands."""
    tid, schema = table(_INT_COLS)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10, _weight=0)
    batch.append(pk=2, val=20, _weight=0)
    assert client.push(tid, batch) > 0
    assert _live(client, tid, "val") == []

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=3, val=30)
    client.push(tid, batch)
    assert _live(client, tid, "val") == [(3, 30, 1)]


def test_zero_weight_mixed_with_live_rows(client, table):
    """Only the nonzero rows are stored, each at the weight enforcement gives
    it: every accumulated PK weight on a base table is clamped to 1."""
    tid, schema = table(_INT_COLS)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10, _weight=0)
    batch.append(pk=2, val=20, _weight=3)
    batch.append(pk=3, val=30, _weight=0)
    batch.append(pk=4, val=40, _weight=1)
    client.push(tid, batch)
    assert _live(client, tid, "val") == [(2, 20, 1), (4, 40, 1)]


def test_zero_weight_row_in_transaction(client, table):
    """A weight-0 row inside a transaction on a STRING table commits cleanly."""
    tid, schema = table(_STR_COLS)
    with client.transaction() as txn:
        b = gnitz.ZSetBatch(schema)
        b.append(pk=1, s="live")
        b.append(pk=2, s="ghost", _weight=0)
        txn.push(tid, b)
    assert _live(client, tid, "s") == [(1, "live", 1)]
