"""What a system family refuses: every write but a DDL's, and every relation
that would live beside it in `_system`."""

import pytest
import gnitz
from _catalog import family, schema_id
from _read import bag, rows, scanned

PLANNED = [
    "INSERT INTO _system.tables VALUES (999999, 1, 'x', 0, 0)",
    "UPDATE _system.tables SET name = 'x' WHERE table_id = 2",
    "UPDATE _system.tables SET name = 'x' WHERE table_id = 999999999",
    "DELETE FROM _system.tables WHERE table_id = 2",
    "DELETE FROM _system.tables WHERE table_id = 999999999",
    "ALTER TABLE _system.tables ADD COLUMN extra BIGINT",
    "ALTER TABLE _system.tables DROP COLUMN flags",
    "ALTER TABLE _system.tables RENAME COLUMN name TO label",
    "CREATE INDEX ON _system.tables (schema_id)",
    "CREATE INDEX sysix ON _system.tables (schema_id)",
    "CREATE UNIQUE INDEX sysux ON _system.tables (name)",
    "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, "
    "t BIGINT UNSIGNED NOT NULL REFERENCES _system.tables(table_id))",
]


@pytest.mark.parametrize("stmt", PLANNED)
def test_sql_refuses_it_where_the_statement_is_planned(client, stmt):
    before = family(client, "tables", "table_id", "name", "flags")
    with pytest.raises(gnitz.GnitzError, match="system table"):
        client.execute_sql(stmt)
    assert family(client, "tables", "table_id", "name", "flags") == before
    assert family(client, "tables", "name", where=f"schema_id = {schema_id(client)}") == {}


@pytest.mark.parametrize("stmt", PLANNED[:5])
def test_a_refused_write_leaves_the_open_transaction_as_it_was(client, stmt):
    """Refused at `COMMIT` instead, the write would take the transaction's
    other writes with it."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    client.execute_sql("BEGIN")
    client.execute_sql("INSERT INTO t VALUES (1, 10)")
    with pytest.raises(gnitz.GnitzError, match="system table"):
        client.execute_sql(stmt)
    client.execute_sql("INSERT INTO t VALUES (2, 20)")
    client.execute_sql("COMMIT")
    assert bag(rows(client, "SELECT id, v FROM t")) == {(1, 10): 1, (2, 20): 1}


def test_the_engine_refuses_a_push_to_a_family(client):
    before = family(client, "tables", "table_id", "name")
    schema = gnitz.sys_schema(gnitz.TABLE_TAB)
    batch = gnitz.ZSetBatch(schema)
    batch.append(table_id=999999, schema_id=1, name="forged", pk_col_idx=0, flags=0)
    with pytest.raises(gnitz.GnitzRefusedError):
        client.push(gnitz.TABLE_TAB, batch)
    assert family(client, "tables", "table_id", "name") == before


@pytest.mark.parametrize("stmt", [
    "CREATE TABLE _system.mine (id BIGINT NOT NULL PRIMARY KEY)",
    "CREATE VIEW _system.mine AS SELECT table_id, name FROM _system.tables",
])
def test_the_engine_refuses_a_relation_in_the_system_schema(client, stmt):
    tables = family(client, "tables", "table_id", "name")
    views = family(client, "views", "view_id", "name")
    with pytest.raises(gnitz.GnitzRefusedError, match="system schema takes no relation"):
        client.execute_sql(stmt)
    assert family(client, "tables", "table_id", "name") == tables
    assert family(client, "views", "view_id", "name") == views


@pytest.mark.parametrize("stmt", [
    "DROP TABLE _system.tables",
    "ALTER TABLE _system.tables RENAME TO relations",
])
def test_the_engine_refuses_to_drop_or_rename_a_family(client, stmt):
    before = family(client, "tables", "table_id", "name")
    with pytest.raises(gnitz.GnitzRefusedError):
        client.execute_sql(stmt)
    assert family(client, "tables", "table_id", "name") == before


def test_no_view_scans_sequences(client):
    """It holds positions rather than a set, and no worker holds it."""
    views = family(client, "views", "view_id", "name")
    with pytest.raises(gnitz.GnitzRefusedError, match="holds no set a view can scan"):
        client.execute_sql("CREATE VIEW sv AS SELECT * FROM _system.sequences")
    assert family(client, "views", "view_id", "name") == views
    # The refused bundle left no dependency behind: the next view registers.
    client.execute_sql("CREATE VIEW ok AS SELECT table_id, name FROM _system.tables")
    assert bag(scanned(client, "ok"), "table_id", "name") == family(client, "tables", "table_id", "name")
