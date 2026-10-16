"""The DDL clauses that decide what happens when a name is, or is not, already
taken: `IF NOT EXISTS` and `IF EXISTS` across tables, views and indexes, and
`CREATE OR REPLACE VIEW` over a free name.

`IF NOT EXISTS` and `IF EXISTS` test the NAME: a *different* body under a taken
name is skipped rather than compiled, and a name held by the wrong kind of
relation still ends the statement.
"""

import gnitz
import pytest
from _catalog import family
from _read import access, bag, scanned


def test_if_not_exists_yields_to_any_holder_of_the_name(client, base):
    """A taken name is skipped whatever holds it and whatever the new definition
    says: the standing table, view and index keep their ids and rows. A free
    name under each clause still creates."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t; CREATE INDEX ix_a ON t (a)")
    ids = [client.resolve_table(n)[0] for n in ("t", "v")]
    indexes = lambda: family(client, "indices", "index_id", "name", where=f"owner_id = {ids[0]}")
    index = indexes()

    client.execute_sql(
        "CREATE VIEW IF NOT EXISTS v AS SELECT id, b FROM t; "
        "CREATE VIEW IF NOT EXISTS t AS SELECT id FROM t; "
        "CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL); "
        "CREATE INDEX IF NOT EXISTS ix_a ON t (b)")
    assert [client.resolve_table(n)[0] for n in ("t", "v")] == ids
    assert indexes() == index
    assert bag(scanned(client, "t")) == {(1, 10, 100): 1, (2, 20, 200): 1}
    assert bag(scanned(client, "v")) == {(1, 10): 1, (2, 20): 1}

    client.execute_sql(
        "CREATE VIEW IF NOT EXISTS fv AS SELECT id, a FROM t; "
        "CREATE OR REPLACE VIEW fr AS SELECT id, a FROM t; "
        "CREATE TABLE IF NOT EXISTS ft (id BIGINT NOT NULL PRIMARY KEY); "
        "CREATE INDEX IF NOT EXISTS ix_b ON t (b); "
        "INSERT INTO ft VALUES (7)")
    for name in ("fv", "fr"):
        assert bag(scanned(client, name)) == {(1, 10): 1, (2, 20): 1}, name
    assert bag(scanned(client, "ft")) == {(7,): 1}
    assert access(client, "SELECT id FROM t WHERE b = 200") == "index range on (b)"


def test_if_exists_answers_only_no_such_object(client, base):
    """A missing name is a no-op for every kind, per name in a list; a name held
    by another kind is not "no such object" and is refused as the wrong kind —
    never as a not-found, or a teardown script would read the relation as gone
    and leave it standing."""
    client.execute_sql(
        "CREATE VIEW v1 AS SELECT id FROM t; CREATE VIEW v2 AS SELECT a FROM t; "
        "CREATE INDEX ix_a ON t (a); "
        "DROP TABLE IF EXISTS nosuch; DROP VIEW IF EXISTS nosuch; DROP INDEX IF EXISTS nosuch")

    for sql, msg in (("DROP TABLE IF EXISTS v1", "is a view; DROP TABLE"),
                     ("DROP VIEW IF EXISTS t", "is a table; DROP VIEW")):
        with pytest.raises(gnitz.GnitzRefusedError, match=msg) as refused:
            client.execute_sql(sql)
        assert not isinstance(refused.value, gnitz.GnitzNotFoundError), sql
    assert bag(scanned(client, "t")) == {(1, 10, 100): 1, (2, 20, 200): 1}
    client.resolve_table("v1")

    client.execute_sql(
        "DROP VIEW IF EXISTS v1, nosuch, v2; DROP INDEX IF EXISTS ix_a; DROP TABLE IF EXISTS t")
    for name in ("v1", "v2", "t"):
        with pytest.raises(gnitz.GnitzNotFoundError):
            client.resolve_table(name)
