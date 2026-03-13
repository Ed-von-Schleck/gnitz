"""E2E tests for unique_pk UPSERT/DELETE-by-PK enforcement."""

import random
import pytest

from gnitz_client import GnitzClient, GnitzError, TypeCode, ColumnDef, Schema
from gnitz_client.batch import ZSetBatch


def _uid():
    return str(random.randint(100000, 999999))


def _make_table_ex(client, unique_pk=True, prefix="upk"):
    """Create a fresh schema + (U64 pk, I64 val) table; return (tid, schema, schema_name)."""
    schema_name = prefix + "_" + _uid()
    client.create_schema(schema_name)
    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("val", TypeCode.I64),
    ]
    tid = client.create_table(schema_name, "t", columns, pk_col_idx=0,
                              unique_pk=unique_pk)
    return tid, Schema(columns=columns, pk_index=0), schema_name


def _make_table(client, unique_pk=True):
    """Create a fresh schema + (U64 pk, I64 val) table."""
    tid, schema, _ = _make_table_ex(client, unique_pk=unique_pk)
    return tid, schema


def _push(client, tid, tbl_schema, rows, weight=1):
    """Push rows as (pk, val) tuples with the given weight."""
    batch = ZSetBatch(schema=tbl_schema)
    for pk, val in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(weight)
        batch.nulls.append(0)
    batch.columns = [[], [val for _, val in rows]]
    client.push(tid, tbl_schema, batch)


def _push_delete(client, tid, tbl_schema, pks):
    """Push delete-by-PK rows (w=-1) with zero payload."""
    batch = ZSetBatch(schema=tbl_schema)
    for pk in pks:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(-1)
        batch.nulls.append(0)
    batch.columns = [[], [0] * len(pks)]
    client.push(tid, tbl_schema, batch)


def _scan_rows(client, tid):
    """Return sorted list of (pk, val) for all positive-weight rows."""
    _, result = client.scan(tid)
    if result is None or len(result.pk_lo) == 0:
        return []
    rows = [
        (result.pk_lo[i], result.columns[1][i])
        for i in range(len(result.pk_lo))
        if result.weights[i] > 0
    ]
    return sorted(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_upsert_insert_new(client):
    """Push a new PK — row appears in scan."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    rows = _scan_rows(client, tid)
    assert rows == [(1, 10)]


def test_upsert_update_existing(client):
    """Push same PK twice — second value wins."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    _push(client, tid, schema, [(1, 20)])
    rows = _scan_rows(client, tid)
    assert rows == [(1, 20)]


def test_delete_by_pk_existing(client):
    """Push row then delete by PK — row disappears."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    _push_delete(client, tid, schema, [1])
    rows = _scan_rows(client, tid)
    assert rows == []


def test_delete_by_pk_absent(client):
    """Delete a PK that was never inserted — no error, 0 rows."""
    tid, schema = _make_table(client)
    _push_delete(client, tid, schema, [999])
    rows = _scan_rows(client, tid)
    assert rows == []


def test_intra_batch_overwrite(client):
    """Single batch with two inserts for the same PK — last value wins."""
    tid, schema = _make_table(client)

    batch = ZSetBatch(schema=schema)
    # Row 0: pk=7, val=10
    batch.pk_lo.append(7)
    batch.pk_hi.append(0)
    batch.weights.append(1)
    batch.nulls.append(0)
    # Row 1: pk=7, val=20 (second write to same PK)
    batch.pk_lo.append(7)
    batch.pk_hi.append(0)
    batch.weights.append(1)
    batch.nulls.append(0)
    batch.columns = [[], [10, 20]]
    client.push(tid, schema, batch)

    rows = _scan_rows(client, tid)
    assert rows == [(7, 20)]


def test_raw_table_allows_duplicates(client):
    """unique_pk=False table does not apply UPSERT semantics.

    Push the same row twice (+1 each) then retract once (-1).
    With unique_pk=False the net weight is +1 so the row is still visible.
    With unique_pk=True the second push would retract-and-reinsert (net +1),
    then the delete-by-PK brings net to 0 (absent).
    """
    tid, schema = _make_table(client, unique_pk=False)
    _push(client, tid, schema, [(1, 10)])
    _push(client, tid, schema, [(1, 10)])  # accumulates to w=+2
    _push_delete(client, tid, schema, [1])  # raw retract: w=+1 remains
    rows = _scan_rows(client, tid)
    assert rows == [(1, 10)]


def test_multiple_keys_independent(client):
    """Multiple distinct PKs are each handled independently."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    _push(client, tid, schema, [(2, 99)])  # update pk=2 only
    rows = _scan_rows(client, tid)
    assert rows == [(1, 10), (2, 99), (3, 30)]


def test_delete_then_reinsert(client):
    """Insert pk=1 val=10, delete pk=1, insert pk=1 val=20; scan shows [(1, 20)]."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    _push_delete(client, tid, schema, [1])
    _push(client, tid, schema, [(1, 20)])
    rows = _scan_rows(client, tid)
    assert rows == [(1, 20)]


def test_delete_multiple_pks(client):
    """Insert pk=1,2,3; delete pk=1,3 in one call; scan shows [(2, 20)]."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    _push_delete(client, tid, schema, [1, 3])
    rows = _scan_rows(client, tid)
    assert rows == [(2, 20)]


def test_upsert_intra_batch_insert_then_delete(client):
    """Single batch: insert pk=1 then delete pk=1 (zero payload); scan shows empty."""
    tid, schema = _make_table(client)
    b = ZSetBatch(schema=schema)
    b.pk_lo.append(1)
    b.pk_hi.append(0)
    b.weights.append(1)
    b.nulls.append(0)
    b.pk_lo.append(1)
    b.pk_hi.append(0)
    b.weights.append(-1)
    b.nulls.append(0)
    b.columns = [[], [10, 0]]
    client.push(tid, schema, b)
    rows = _scan_rows(client, tid)
    assert rows == []


def test_upsert_intra_batch_delete_then_insert(client):
    """Pre-insert pk=1 val=10; single batch: delete pk=1 then insert pk=1 val=99; scan shows [(1, 99)]."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    b = ZSetBatch(schema=schema)
    b.pk_lo.append(1)
    b.pk_hi.append(0)
    b.weights.append(-1)
    b.nulls.append(0)
    b.pk_lo.append(1)
    b.pk_hi.append(0)
    b.weights.append(1)
    b.nulls.append(0)
    b.columns = [[], [0, 99]]
    client.push(tid, schema, b)
    rows = _scan_rows(client, tid)
    assert rows == [(1, 99)]


def test_upsert_updates_view(client):
    """Create table + passthrough view; upsert pk=1; view reflects new value only."""
    tid, schema, schema_name = _make_table_ex(client)
    columns = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
    vid = client.create_view(schema_name, "v", tid, columns)
    _push(client, tid, schema, [(1, 10)])
    view_rows = _scan_rows(client, vid)
    assert view_rows == [(1, 10)]
    _push(client, tid, schema, [(1, 99)])
    view_rows = _scan_rows(client, vid)
    assert view_rows == [(1, 99)]


def test_delete_updates_view(client):
    """Create table + passthrough view; delete pk=1; view shows empty."""
    tid, schema, schema_name = _make_table_ex(client)
    columns = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
    vid = client.create_view(schema_name, "v", tid, columns)
    _push(client, tid, schema, [(1, 10)])
    view_rows = _scan_rows(client, vid)
    assert view_rows == [(1, 10)]
    _push_delete(client, tid, schema, [1])
    view_rows = _scan_rows(client, vid)
    assert view_rows == []


def test_delete_convenience(client):
    """client.delete() removes rows by PK."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10), (2, 20), (3, 30)])
    client.delete(tid, schema, [1, 3])
    rows = _scan_rows(client, tid)
    assert rows == [(2, 20)]


def test_update_convenience(client):
    """client.update() upserts rows; old value replaced by new value."""
    tid, schema = _make_table(client)
    _push(client, tid, schema, [(1, 10)])
    client.update(tid, schema, [[1, 99]])
    rows = _scan_rows(client, tid)
    assert rows == [(1, 99)]
