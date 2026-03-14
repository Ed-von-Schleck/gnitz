import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def test_create_schema(client):
    name = "s" + _uid()
    sid = client.create_schema(name)
    assert sid > 0
    client.drop_schema(name)


def test_create_table(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "nums", cols)
    assert tid > 0
    client.drop_table(sn, "nums")
    client.drop_schema(sn)


def test_drop_schema_not_found(client):
    with pytest.raises(gnitz.GnitzError):
        client.drop_schema("nonexistent_schema_xyz")


def test_drop_table_not_found(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    with pytest.raises(gnitz.GnitzError):
        client.drop_table(sn, "nonexistent_table_xyz")
    client.drop_schema(sn)


def test_create_table_with_string_col(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("label", gnitz.TypeCode.STRING, is_nullable=True)]
    tid = client.create_table(sn, "labels", cols)
    assert tid > 0
    client.drop_table(sn, "labels")
    client.drop_schema(sn)


def test_resolve_table_id(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "t", cols)
    resolved_tid, schema = client.resolve_table_id(sn, "t")
    assert resolved_tid == tid
    assert len(schema.columns) == 2
    assert schema.pk_index == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)
