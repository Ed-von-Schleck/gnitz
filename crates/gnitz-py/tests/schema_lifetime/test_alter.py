"""ALTER TABLE renames and table-level constraints: the catalog names change
while the ordinals underneath do not.

An index and a foreign key bind by ordinal, so a rename must leave both
working; a view's output store must not be re-derived by one. The column-level
ALTERs are `test_alter_add.py` and `test_alter_drop.py`; the ALTER VIEW family
is `test_alter_view.py`.
"""

import gnitz
import pytest
from _read import access, bag, rows, scanned
from _serverproc import NEEDS_MULTI


def test_rename_table_keeps_its_rows_and_is_seen_by_a_second_connection(
        client, schema_name, server):
    """The old name stops resolving, the new one answers with the same Z-set,
    and a connection that ran none of the DDL sees it on its next statement."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t RENAME TO t2", schema_name=schema_name)

    with pytest.raises(Exception):
        client.resolve_table(schema_name, "t")
    tid = client.resolve_table(schema_name, "t2")[0]
    assert bag(client.scan(tid), "pk", "v") == {(1, 10): 1, (2, 20): 1}

    with gnitz.connect(server) as c2:
        assert c2.resolve_table(schema_name, "t2")[0] == tid
        with pytest.raises(Exception):
            c2.resolve_table(schema_name, "t")


def test_rename_column_keeps_the_index_and_the_fk_bound_to_it(client, schema_name):
    """An index and a foreign key both bind by ordinal, so renaming the column
    each covers leaves them working under the new name.

    The index is asserted through EXPLAIN's access line, not through the rows:
    a plan that lost the index answers the same seek with a full scan and the
    same result, so only the access line can tell the two apart.
    """
    client.execute_sql(
        "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, "
        "ref BIGINT NOT NULL REFERENCES parent(id))", schema_name=schema_name)
    client.execute_sql("CREATE INDEX iv ON parent(v)", schema_name=schema_name)
    client.execute_sql("INSERT INTO parent VALUES (1, 100), (2, 200)", schema_name=schema_name)
    client.execute_sql("INSERT INTO child VALUES (1, 1)", schema_name=schema_name)

    client.execute_sql("ALTER TABLE parent RENAME COLUMN v TO w", schema_name=schema_name)
    client.execute_sql("ALTER TABLE child RENAME TO child2", schema_name=schema_name)
    client.execute_sql("ALTER TABLE parent RENAME TO parent2", schema_name=schema_name)

    # The index still serves a seek on the renamed column, through the renamed
    # table, and still answers with the right rows.
    q = "SELECT id FROM parent2 WHERE w = 200"
    assert "index" in access(client, schema_name, q), access(client, schema_name, q)
    assert bag(rows(client, schema_name, q), "id") == {(2,): 1}

    # The FK still validates against the renamed parent: a live reference is
    # accepted and a dangling one refused.
    client.execute_sql("INSERT INTO child2 VALUES (2, 1)", schema_name=schema_name)
    with pytest.raises(Exception):
        client.execute_sql("INSERT INTO child2 VALUES (3, 999)", schema_name=schema_name)


@NEEDS_MULTI
def test_rename_populated_join_view_keeps_its_output_weights(client, schema_name):
    """Renaming a populated exchange-join view must not re-backfill it: the
    output Z-set is asserted whole before and after, so a second fill shows as
    doubled weights rather than as extra rows."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, av BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW j AS SELECT a.id AS id, a.av AS av, b.bv AS bv "
        "FROM a JOIN b ON a.id = b.id", schema_name=schema_name)
    client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1, 100), (2, 200)", schema_name=schema_name)

    want = {(10, 100): 1, (20, 200): 1}
    assert bag(scanned(client, schema_name, "j"), "av", "bv") == want

    client.execute_sql("ALTER TABLE j RENAME TO j2", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "j2"), "av", "bv") == want


def test_add_and_drop_unique_constraint(client, schema_name):
    """`ADD CONSTRAINT ... UNIQUE` builds an enforcing index that a duplicate
    trips, `DROP CONSTRAINT` retires it by name, and the unnamed form builds one
    that enforces just the same — asserted over a column that already holds a
    duplicate, so a constraint that created nothing could not stand."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("ALTER TABLE t ADD CONSTRAINT uq UNIQUE (v)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="[Uu]nique index violation"):
        client.execute_sql("INSERT INTO t VALUES (2, 5)", schema_name=schema_name)

    client.execute_sql("ALTER TABLE t DROP CONSTRAINT uq", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (2, 5)", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t DROP CONSTRAINT IF EXISTS nope", schema_name=schema_name)

    # The unnamed form builds an enforcing index too: it refuses to build over
    # the duplicate that is now there, and once that is gone it refuses the next
    # duplicate INSERT.
    with pytest.raises(gnitz.GnitzError, match="contains duplicate values"):
        client.execute_sql("ALTER TABLE t ADD CONSTRAINT UNIQUE (v)", schema_name=schema_name)
    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t ADD CONSTRAINT UNIQUE (v)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="[Uu]nique index violation"):
        client.execute_sql("INSERT INTO t VALUES (3, 5)", schema_name=schema_name)
