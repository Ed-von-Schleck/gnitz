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


def test_create_table_long_name(client):
    """Table name > 12 chars exercises German String heap allocation path."""
    sn = "s" + _uid()
    client.create_schema(sn)
    long_name = "orders_archive_2024"
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, long_name, cols)
    assert tid > 0
    resolved_tid, _ = client.resolve_table(sn, long_name)
    assert resolved_tid == tid
    client.drop_table(sn, long_name)
    client.drop_schema(sn)


def test_resolve_table_id(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "t", cols)
    resolved_tid, schema = client.resolve_table(sn, "t")
    assert resolved_tid == tid
    assert len(schema.columns) == 2
    assert schema.pk_index == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_create_then_drop_no_worker_race(race_server):
    """
    Regression: master DROP `remove_dir_all` must not race a lagging worker
    still applying the CREATE of the same table.

    With the debug-only `GNITZ_INJECT_TABLE_CREATE_DELAY_MS` seam active
    (see the `race_server` fixture), each worker sleeps between creating the
    table dir and its partition subdirs.  Pre-fix, the master removed the dir
    on DROP while a worker was mid-create → worker ENOENT abort → the next
    request fails with "connection closed".  Post-fix the master defers
    removal to the next checkpoint, so the worker finishes safely.

    Asserts the server survives a handful of immediate CREATE→DROP cycles
    (the follow-up create_schema would raise "connection closed" if a worker
    had aborted).
    """
    client = race_server
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    for i in range(6):
        client.create_table(sn, f"t{i}", cols)
        client.drop_table(sn, f"t{i}")
    # If any worker aborted above, the server is dead and this raises.
    sn2 = "s" + _uid()
    client.create_schema(sn2)
    client.drop_schema(sn2)
    client.drop_schema(sn)


def test_ddl_txn_frame_all_single_family_ops(client):
    """Every catalog write now rides one DDL_TXN frame. Exercise the full set of
    single-family paths — CREATE/DROP of schema, table, index, view — in sequence
    to guard the frame reroute of each working path."""
    sn = "s" + _uid()
    client.create_schema(sn)  # CREATE SCHEMA
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)",
            schema_name=sn,
        )  # CREATE TABLE
        client.execute_sql("CREATE INDEX ON t(b)", schema_name=sn)  # CREATE INDEX
        client.execute_sql("CREATE VIEW v AS SELECT a FROM t", schema_name=sn)  # CREATE VIEW
        client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
        # The view is populated (incremental maintenance ran through the bundle).
        vid, _ = client.resolve_table(sn, "v")
        assert len(list(client.scan(vid))) == 1

        client.execute_sql("DROP VIEW v", schema_name=sn)  # DROP VIEW
        client.execute_sql(f"DROP INDEX {sn}__t__idx_b", schema_name=sn)  # DROP INDEX
        client.execute_sql("DROP TABLE t", schema_name=sn)  # DROP TABLE
        client.drop_schema(sn)  # DROP SCHEMA
    finally:
        for stmt in ("DROP VIEW v", "DROP TABLE t"):
            try:
                client.execute_sql(stmt, schema_name=sn)
            except Exception:
                pass
        try:
            client.drop_schema(sn)
        except Exception:
            pass
