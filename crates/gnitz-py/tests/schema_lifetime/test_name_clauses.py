"""The DDL clauses that decide what happens when a name is, or is not, already
taken: `IF NOT EXISTS` and `IF EXISTS` across tables, views and indexes, and
`CREATE OR REPLACE VIEW` over a free name.

`IF NOT EXISTS` and `IF EXISTS` test the NAME: a *different* body under a taken
name is skipped rather than compiled, and a name held by the wrong kind of
relation still ends the statement.
"""

import gnitz
import pytest
from _read import bag, scanned

_ROWS = {(1, 10, 100): 1, (2, 20, 200): 1}


def test_if_not_exists_yields_to_any_holder_of_the_name(client, base):
    """A taken name is skipped whatever holds it and whatever the new definition
    says: the standing table, view and index keep their ids and rows, and the
    index skip answers with the standing index. A free name under each clause
    still creates."""
    [created] = client.execute_sql(
        "CREATE VIEW v AS SELECT id, a FROM t; CREATE INDEX ix_a ON t (a)")[1:]
    ids = [client.resolve_table(n)[0] for n in ("t", "v")]

    skipped = client.execute_sql(
        "CREATE VIEW IF NOT EXISTS v AS SELECT id, b FROM t; "
        "CREATE VIEW IF NOT EXISTS t AS SELECT id FROM t; "
        "CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL); "
        "CREATE INDEX IF NOT EXISTS ix_a ON t (a)")
    assert skipped[3] == created, "the skip answers with the standing index"
    assert [client.resolve_table(n)[0] for n in ("t", "v")] == ids
    assert bag(scanned(client, "t"), "id", "a", "b") == _ROWS
    assert bag(scanned(client, "v"), "id", "a") == {(1, 10): 1, (2, 20): 1}

    client.execute_sql(
        "CREATE VIEW IF NOT EXISTS fv AS SELECT id, a FROM t; "
        "CREATE OR REPLACE VIEW fr AS SELECT id, a FROM t; "
        "CREATE TABLE IF NOT EXISTS ft (id BIGINT NOT NULL PRIMARY KEY); "
        "INSERT INTO ft VALUES (7)")
    for name in ("fv", "fr"):
        assert bag(scanned(client, name), "id", "a") == {(1, 10): 1, (2, 20): 1}, name
    assert bag(scanned(client, "ft"), "id") == {(7,): 1}


def test_if_exists_answers_only_no_such_object(client, base):
    """A missing name is a no-op for every kind, per name in a list; a name held
    by another kind is not "no such object" and still refuses, or a teardown
    script would silently leave the relation standing."""
    client.execute_sql(
        "CREATE VIEW v1 AS SELECT id FROM t; CREATE VIEW v2 AS SELECT a FROM t; "
        "CREATE INDEX ix_a ON t (a); "
        "DROP TABLE IF EXISTS nosuch; DROP VIEW IF EXISTS nosuch; DROP INDEX IF EXISTS nosuch")

    for sql in ("DROP TABLE IF EXISTS v1", "DROP VIEW IF EXISTS t"):
        with pytest.raises(gnitz.GnitzRefusedError):
            client.execute_sql(sql)
    assert bag(scanned(client, "t"), "id", "a", "b") == _ROWS
    client.resolve_table("v1")

    client.execute_sql(
        "DROP VIEW IF EXISTS v1, nosuch, v2; DROP INDEX IF EXISTS ix_a; DROP TABLE IF EXISTS t")
    for name in ("v1", "v2", "t"):
        with pytest.raises(gnitz.GnitzNotFoundError):
            client.resolve_table(name)

