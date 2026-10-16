"""ALTER TABLE on a live table: a column appended, dropped or un-nulled, a
relation or a column renamed, and the RESTRICT a dependent view puts on every
schema change under it.

DROP COLUMN is logical — the column is flagged hidden and kept physically
present — so what is asserted is that nothing names it and that the positional
forms (a shortened INSERT, `RETURNING *`) address the remaining columns. An
index and a foreign key bind by ordinal, so a rename must leave both working.
The ALTER VIEW family is `test_alter_view.py`.
"""

import gnitz
import pytest
from _read import access, bag, rows, scanned
from _sql import values


def test_add_column_and_drop_not_null_reach_every_row_and_keep_the_index_serving(client):
    """Two ADD COLUMNs (BIGINT, TEXT) and a DROP NOT NULL over enough rows to
    land on every partition: pre-ALTER rows read NULL in the appended columns,
    an UPDATE of one retracts the stored padded row, and a NULL and a 0 in the
    un-nulled column stay distinct values. Strings cover the inline and the heap
    German-string forms.

    The index on the un-nulled column keeps *serving* — asserted through
    EXPLAIN's access line, since a plan that lost it answers the same rows by
    full scan — and a range below 10 yields the 0 but never the NULL, which
    sorts first in the index.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL); "
        f"INSERT INTO t VALUES {values((k, k * 10) for k in range(1, 41))}; "
        "CREATE INDEX ia ON t (a); "
        "ALTER TABLE t ADD COLUMN c BIGINT; ALTER TABLE t ADD COLUMN s TEXT; "
        "ALTER TABLE t ALTER COLUMN a DROP NOT NULL")
    got = rows(client, "SELECT * FROM t")
    assert got[0]._fields == ("id", "a", "c", "s")
    want = {(k, k * 10, None, None): 1 for k in range(1, 41)}
    assert bag(got) == want

    long = "a string comfortably past the inline prefix"
    client.execute_sql(
        f"INSERT INTO t VALUES {values([(41, 0, 4100, 'short'), (42, None, None, long)])}; "
        "UPDATE t SET c = 55, s = 'set later' WHERE id = 1; DELETE FROM t WHERE id = 2")
    del want[(1, 10, None, None)], want[(2, 20, None, None)]
    want |= {(1, 10, 55, "set later"): 1, (41, 0, 4100, "short"): 1, (42, None, None, long): 1}
    assert bag(rows(client, "SELECT * FROM t")) == want
    assert bag(rows(client, "SELECT c FROM t WHERE id = 3")) == {(None,): 1}
    assert bag(rows(client, "SELECT id FROM t WHERE c = 55")) == {(1,): 1}

    for q, hit in (("SELECT id, c FROM t WHERE a = 30", {(3, None): 1}),
                   ("SELECT id, c FROM t WHERE a <= 10", {(1, 55): 1, (41, 4100): 1})):
        assert access(client, q) == "index range on (a)", q
        assert bag(rows(client, q)) == hit, q


def test_drop_middle_column_remaps_every_positional_form(client, base):
    """Every wildcard stops projecting the dropped middle column, a shortened
    INSERT lands its value in `b` rather than in the vacated slot, UPDATE's
    retraction matches the row as *stored* (hidden slot and all), DELETE
    retracts the row it named and no other, and the name is unusable from either
    side of a statement."""
    client.execute_sql("ALTER TABLE t DROP COLUMN a; INSERT INTO t VALUES (3, 300)")

    ret = rows(client, "INSERT INTO t VALUES (4, 400) RETURNING *")
    assert ret[0]._fields == ("id", "b")
    assert bag(ret) == {(4, 400): 1}

    client.execute_sql("UPDATE t SET b = 999 WHERE id = 1; DELETE FROM t WHERE id = 2")
    got = rows(client, "SELECT * FROM t")
    assert got[0]._fields == ("id", "b")
    assert bag(got) == {(1, 999): 1, (3, 300): 1, (4, 400): 1}

    for sql in ("INSERT INTO t (id, a, b) VALUES (5, 1, 2)", "SELECT a FROM t"):
        with pytest.raises(gnitz.GnitzRefusedError, match="column 'a' not found"):
            client.execute_sql(sql)


def test_drop_column_refuses_a_bound_column_until_the_binding_goes(client):
    """A column a foreign key or a secondary index binds is refused by naming
    the kind of binding, so the author knows what to undo; the column survives
    the refusal, and dropping the index makes its column droppable."""
    client.execute_sql(
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
        "ref BIGINT NOT NULL REFERENCES p(id), ix BIGINT NOT NULL); "
        "CREATE INDEX iix ON t (ix); "
        "INSERT INTO p VALUES (1); INSERT INTO t VALUES (1, 1, 5)")

    for column, message in (("ref", "carries a foreign key"), ("ix", "covered by a secondary index")):
        with pytest.raises(gnitz.GnitzRefusedError, match=message):
            client.execute_sql(f"ALTER TABLE t DROP COLUMN {column}")
    assert bag(rows(client, "SELECT * FROM t")) == {(1, 1, 5): 1}

    client.execute_sql("DROP INDEX iix; ALTER TABLE t DROP COLUMN ix")
    got = rows(client, "SELECT * FROM t")
    assert got[0]._fields == ("id", "ref")
    assert bag(got) == {(1, 1): 1}


def test_renames_keep_every_binding_and_every_store(client):
    """A rename changes names only: an index, a foreign key and a view's circuit
    bind by ordinal or id, and a batch laid out under the old names still lays
    out the table, so each keeps working. Renaming a populated join view does
    not re-backfill it, which would show as doubled weights."""
    client.execute_sql(
        "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, ref BIGINT NOT NULL REFERENCES parent(id)); "
        "CREATE INDEX iv ON parent(v); "
        "CREATE VIEW j AS SELECT child.id AS cid, parent.v AS v "
        "FROM child JOIN parent ON child.ref = parent.id; "
        "INSERT INTO parent VALUES (1, 100), (2, 200); INSERT INTO child VALUES (1, 1)")
    pid, schema = client.resolve_table("parent")
    stale = gnitz.ZSetBatch(schema).append(id=3, v=300)
    client.execute_sql(
        "ALTER TABLE parent RENAME COLUMN v TO w; ALTER TABLE parent RENAME TO parent2; "
        "ALTER TABLE child RENAME TO child2; ALTER TABLE j RENAME TO j2")

    client.push(pid, stale)
    client.execute_sql("INSERT INTO child2 VALUES (2, 3)")
    with pytest.raises(gnitz.GnitzIntegrityError, match="Foreign Key violation"):
        client.execute_sql("INSERT INTO child2 VALUES (3, 999)")
    q = "SELECT id FROM parent2 WHERE w >= 200"
    assert access(client, q) == "index range on (w)"
    assert bag(rows(client, q)) == {(2,): 1, (3,): 1}
    assert bag(scanned(client, "j2")) == {(1, 100): 1, (2, 300): 1}


_COLUMN_ALTERS = [
    "ALTER TABLE t ADD COLUMN c BIGINT",
    "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
    "ALTER TABLE t DROP COLUMN a",
]
_VIEW_RETARGETS = [
    "DROP VIEW IF EXISTS v",
    "ALTER VIEW v AS SELECT id, b FROM t",
    "CREATE OR REPLACE VIEW v AS SELECT id, b FROM t",
]


def test_a_dependent_view_restricts_until_it_is_dropped(client, base):
    """A column transition under a view that scans the table, and a drop or a
    retarget of a view another view scans, are each refused naming the direct
    dependent — `IF EXISTS` included, since it answers "no such object" only.
    Nothing is torn down on the way: both views keep maintaining, and dropping
    them lifts the RESTRICT."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t WHERE a > 5; CREATE VIEW dep AS SELECT id FROM v")

    for stmts, blocker in ((_COLUMN_ALTERS, "v"), (_VIEW_RETARGETS, "dep")):
        for sql in stmts:
            with pytest.raises(gnitz.GnitzRefusedError, match=rf"has dependent views \({client.schema}\.{blocker}\)"):
                client.execute_sql(sql)

    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)")
    assert bag(rows(client, "SELECT * FROM t")) == {(1, 10, 100): 1, (2, 20, 200): 1, (3, 30, 300): 1}
    assert bag(scanned(client, "v")) == {(1, 10): 1, (2, 20): 1, (3, 30): 1}
    assert bag(scanned(client, "dep")) == {(1,): 1, (2,): 1, (3,): 1}

    client.execute_sql("DROP VIEW dep; DROP VIEW v; " + "; ".join(_COLUMN_ALTERS))
    assert bag(rows(client, "SELECT * FROM t")) == {(1, 100, None): 1, (2, 200, None): 1, (3, 300, None): 1}
