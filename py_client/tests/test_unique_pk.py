"""E2E tests for unique_pk UPSERT/DELETE-by-PK enforcement."""

import random
import pytest

from gnitz_client import GnitzClient, GnitzError, TypeCode, ColumnDef, Schema
from gnitz_client.batch import ZSetBatch


def _uid():
    return str(random.randint(100000, 999999))


def _make_table(client, unique_pk=True):
    """Create a fresh schema + (U64 pk, I64 val) table."""
    schema_name = "upk_" + _uid()
    client.create_schema(schema_name)
    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("val", TypeCode.I64),
    ]
    tid = client.create_table(schema_name, "t", columns, pk_col_idx=0,
                              unique_pk=unique_pk)
    tbl_schema = Schema(columns=columns, pk_index=0)
    return tid, tbl_schema


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
