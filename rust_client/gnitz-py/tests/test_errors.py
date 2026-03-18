import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def test_push_to_nonexistent_target(client):
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10)
    with pytest.raises(gnitz.GnitzError):
        client.push(99999, schema, batch)


def test_scan_nonexistent_target(client):
    with pytest.raises(gnitz.GnitzError):
        client.scan(99999)
