"""Test error paths."""

import pytest
from gnitz_client import GnitzClient, GnitzError
from gnitz_client.types import Schema
from gnitz_client.batch import ZSetBatch


def test_push_to_nonexistent_target(client):
    """Push to a target_id that doesn't exist, expect GnitzError."""
    from gnitz_client import TypeCode, ColumnDef

    fake_schema = Schema(
        columns=[ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)],
        pk_index=0,
    )
    batch = ZSetBatch(
        schema=fake_schema,
        pk_lo=[1],
        pk_hi=[0],
        weights=[1],
        nulls=[0],
        columns=[[], [42]],
    )
    with pytest.raises(GnitzError):
        client.push(99999, fake_schema, batch)


def test_scan_nonexistent_target(client):
    """Scan a target_id that doesn't exist, expect GnitzError."""
    with pytest.raises(GnitzError):
        client.scan(99999)
