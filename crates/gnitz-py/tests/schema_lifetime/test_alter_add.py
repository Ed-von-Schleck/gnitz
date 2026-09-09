"""ALTER TABLE ADD COLUMN: the appended column reads NULL for every row that
predates it, and is writable from then on.

Run at GNITZ_WORKERS=4 — the descriptor swap widens each partition's resident
runs and re-opens its shards, so a pre-ALTER shard is read back through the
per-shard NULL pad on every worker, not just one.

The clause refusals are `admissibility/test_sql_rejections.py`'s; the crash and
checkpoint replays are `state_lifetime/test_persistence.py`'s; the ALTER racing
live traffic is `interleaving/test_alter_under_traffic.py`'s.
"""

import pytest
from _read import access, bag, rows
from _serverproc import NEEDS_MULTI

_T = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)"


def _insert(client, sn, table, pairs):
    """One multi-row INSERT — every row here is a durable base-table write, so a
    per-row statement would buy one fdatasync apiece for nothing."""
    vals = ", ".join("(" + ", ".join(str(c) for c in r) + ")" for r in pairs)
    client.execute_sql(f"INSERT INTO {table} VALUES {vals}", schema_name=sn)


@NEEDS_MULTI
def test_add_column_reads_null_for_pre_alter_rows(client, schema_name):
    """The core contract, over enough rows to land on every partition: existing
    rows read the appended column as NULL, and it is writable from then on —
    through inserts, updates, deletes, scans, point seeks and filters.

    Asserted as a weighted bag rather than a row count: a partition left on the
    narrow run would consolidate a padded row against a real one, which moves a
    weight and not the row set.
    """
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, "t", [(k, k * 10) for k in range(1, 41)])

    client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=schema_name)

    got = rows(client, schema_name, "SELECT * FROM t")
    assert "c" in got[0]._fields
    assert bag(got, "id", "a", "c") == {(k, k * 10, None): 1 for k in range(1, 41)}

    # A new INSERT supplies it; an explicit NULL leaves it unset. UPDATE sets it
    # on a pre-ALTER row (a retract + insert whose retraction has to match the
    # stored row, whose `c` is NULL), and DELETE removes one.
    _insert(client, schema_name, "t", [(41, 410, 4100), (42, 420, "NULL")])
    client.execute_sql("UPDATE t SET c = 55 WHERE id = 1", schema_name=schema_name)
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=schema_name)

    want = {(k, k * 10, None): 1 for k in range(3, 41)}
    want[(1, 10, 55)] = 1
    want[(41, 410, 4100)] = 1
    want[(42, 420, None)] = 1
    assert bag(rows(client, schema_name, "SELECT * FROM t"), "id", "a", "c") == want

    # Point seek on a padded row and on a post-ALTER row, and a filter by name.
    assert bag(rows(client, schema_name, "SELECT c FROM t WHERE id = 3"), "c") == {(None,): 1}
    assert bag(rows(client, schema_name, "SELECT c FROM t WHERE id = 41"), "c") == {(4100,): 1}
    assert bag(rows(client, schema_name, "SELECT id FROM t WHERE c = 55"), "id") == {(1,): 1}


def test_add_string_column(client, schema_name):
    """A STRING column appended over existing rows: old rows read NULL (no heap
    read for a cell the shard has no bytes for), new writes store both German
    string forms — inline (<= 12 bytes) and heap-resident."""
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, "t", [(k, k) for k in range(1, 6)])

    client.execute_sql("ALTER TABLE t ADD COLUMN s TEXT", schema_name=schema_name)
    assert all(r["s"] is None for r in rows(client, schema_name, "SELECT * FROM t"))

    long = "a string comfortably past the inline prefix"
    client.execute_sql("INSERT INTO t VALUES (6, 6, 'short')", schema_name=schema_name)
    client.execute_sql(f"INSERT INTO t VALUES (7, 7, '{long}')", schema_name=schema_name)
    client.execute_sql("UPDATE t SET s = 'set later' WHERE id = 1", schema_name=schema_name)

    assert bag(rows(client, schema_name, "SELECT id, s FROM t"), "id", "s") == {
        (1, "set later"): 1, (2, None): 1, (3, None): 1, (4, None): 1, (5, None): 1,
        (6, "short"): 1, (7, long): 1}


def test_two_serialized_add_columns(client, schema_name):
    """Two ADD COLUMNs in sequence add two columns in order: the second reads
    the already-widened count and picks the next index."""
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, "t", [(1, 10)])
    client.execute_sql("ALTER TABLE t ADD COLUMN c1 BIGINT", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t ADD COLUMN c2 BIGINT", schema_name=schema_name)

    got = rows(client, schema_name, "SELECT * FROM t")
    assert got[0]._fields == ("id", "a", "c1", "c2")

    _insert(client, schema_name, "t", [(2, 20, 21, 22)])
    assert bag(rows(client, schema_name, "SELECT * FROM t"), "id", "c1", "c2") == {
        (1, None, None): 1, (2, 21, 22): 1}


def test_add_column_keeps_the_secondary_index_serving(client, schema_name):
    """An index schema folds every indexed column into its PK, so it has zero
    payload columns and no locator a trailing append moves: no index swap, no
    rebuild, and seeks keep being *served by the index* at the wider width.

    Asserted through EXPLAIN's access line — an unregistered index would leave
    the planner answering both seeks by full scan, with identical rows.
    """
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, "t", [(k, k * 10) for k in range(1, 41)])
    client.execute_sql("CREATE INDEX ia ON t (a)", schema_name=schema_name)

    client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=schema_name)

    # Point seek through the index, on a pre-ALTER row.
    point = "SELECT id, a, c FROM t WHERE a = 30"
    assert "index" in access(client, schema_name, point), access(client, schema_name, point)
    assert bag(rows(client, schema_name, point), "id", "a", "c") == {(3, 30, None): 1}

    # Range seek through the index, spanning pre- and post-ALTER rows.
    _insert(client, schema_name, "t", [(41, 410, 1)])
    span = "SELECT id, c FROM t WHERE a >= 390"
    assert "index" in access(client, schema_name, span), access(client, schema_name, span)
    assert bag(rows(client, schema_name, span), "id", "c") == {
        (39, None): 1, (40, None): 1, (41, 1): 1}


def test_a_dependent_view_restricts_the_alter_until_it_is_dropped(client, schema_name):
    """A compiled circuit's ScanDelta register schema is baked from the base
    descriptor, so a column transition is refused while a view scans the table.

    One guard, three transitions: `precheck_column_append` and both arms of
    `precheck_column_family` call it, so all three are asserted together.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t WHERE a > 5", schema_name=schema_name)
    _insert(client, schema_name, "t", [(1, 10)])

    for clause in ("ADD COLUMN c BIGINT", "DROP COLUMN a", "ALTER COLUMN a DROP NOT NULL"):
        with pytest.raises(Exception, match="dependent view"):
            client.execute_sql(f"ALTER TABLE t {clause}", schema_name=schema_name)

    # Each rejection left nothing behind: no new column, no lost one, and the
    # view still maintains incrementally.
    assert rows(client, schema_name, "SELECT * FROM t")[0]._fields == ("id", "a")
    _insert(client, schema_name, "t", [(2, 20)])
    assert bag(rows(client, schema_name, "SELECT * FROM v"), "id", "a") == {(1, 10): 1, (2, 20): 1}

    # Dropping the view lifts the RESTRICT for all three.
    client.execute_sql("DROP VIEW v", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t ALTER COLUMN a DROP NOT NULL", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=schema_name)
    assert rows(client, schema_name, "SELECT * FROM t")[0]._fields == ("id", "c")
