"""ALTER renames, and the RESTRICT a dependent view puts on every schema change
under it.

An index and a foreign key bind by ordinal, so a rename must leave both
working; a view's output store must not be re-derived by one. The column-level
ALTERs are `test_alter_add.py` and `test_alter_drop.py`; the ALTER VIEW family
is `test_alter_view.py`.
"""

import gnitz
import pytest
from _read import access, bag, rows, scanned
from _serverproc import NEEDS_MULTI


def test_rename_column_keeps_the_index_and_the_fk_bound_to_it(client, schema_name):
    """An index and a foreign key both bind by ordinal, so renaming the column
    each covers leaves them working under the new name.

    The index is asserted through EXPLAIN's access line, not through the rows:
    a plan that lost the index answers the same seek with a full scan and the
    same result, so only the access line can tell the two apart.
    """
    client.execute_sql(
        "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, "
        "ref BIGINT NOT NULL REFERENCES parent(id)); "
        "CREATE INDEX iv ON parent(v); "
        "INSERT INTO parent VALUES (1, 100), (2, 200); "
        "INSERT INTO child VALUES (1, 1); "
        "ALTER TABLE parent RENAME COLUMN v TO w; "
        "ALTER TABLE child RENAME TO child2; "
        "ALTER TABLE parent RENAME TO parent2", schema_name=schema_name)

    q = "SELECT id FROM parent2 WHERE w = 200"
    plan = access(client, schema_name, q)
    assert "index" in plan, plan
    assert bag(rows(client, schema_name, q), "id") == {(2,): 1}

    # A live reference is accepted and a dangling one refused.
    client.execute_sql("INSERT INTO child2 VALUES (2, 1)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="Foreign Key violation"):
        client.execute_sql("INSERT INTO child2 VALUES (3, 999)", schema_name=schema_name)


@NEEDS_MULTI
def test_rename_populated_join_view_keeps_its_output_weights(client, schema_name):
    """Renaming a populated exchange-join view must not re-backfill it: the
    output Z-set is asserted whole before and after, so a second fill shows as
    doubled weights rather than as extra rows."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, av BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL); "
        "CREATE VIEW j AS SELECT a.id AS id, a.av AS av, b.bv AS bv "
        "FROM a JOIN b ON a.id = b.id; "
        "INSERT INTO a VALUES (1, 10), (2, 20); "
        "INSERT INTO b VALUES (1, 100), (2, 200)", schema_name=schema_name)

    want = {(10, 100): 1, (20, 200): 1}
    assert bag(scanned(client, schema_name, "j"), "av", "bv") == want

    client.execute_sql("ALTER TABLE j RENAME TO j2", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "j2"), "av", "bv") == want


_RESTRICTED = [
    "ALTER TABLE t ADD COLUMN c BIGINT",
    "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
    "ALTER TABLE t DROP COLUMN a",
    "DROP VIEW IF EXISTS v",
    "ALTER VIEW v AS SELECT id, b FROM t",
    "CREATE OR REPLACE VIEW v AS SELECT id, b FROM t",
]


def test_a_dependent_view_restricts_until_it_is_dropped(client, schema_name):
    """A column transition under a view that scans the table, and a drop or a
    retarget of a view another view scans, are each refused — `IF EXISTS`
    included, since it answers "no such object" only. Nothing is torn down on
    the way: both views keep maintaining, and dropping them lifts the RESTRICT.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT id, a FROM t WHERE a > 5; "
        "CREATE VIEW dep AS SELECT id FROM v; "
        "INSERT INTO t VALUES (1, 10, 100)", schema_name=sn)

    for sql in _RESTRICTED:
        with pytest.raises(gnitz.GnitzError, match="dependen"):
            client.execute_sql(sql, schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (2, 20, 200)", schema_name=sn)
    assert rows(client, sn, "SELECT * FROM t")[0]._fields == ("id", "a", "b")
    assert bag(scanned(client, sn, "v"), "id", "a") == {(1, 10): 1, (2, 20): 1}
    assert bag(scanned(client, sn, "dep"), "id") == {(1,): 1, (2,): 1}

    client.execute_sql("DROP VIEW dep; DROP VIEW v; " + "; ".join(_RESTRICTED[:3]), schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM t"), "id", "b", "c") == {
        (1, 100, None): 1, (2, 200, None): 1}
