"""The DDL clauses that decide what happens when a name is, or is not, already
taken: `IF NOT EXISTS` and `IF EXISTS` across tables, views and indexes.

Every one of these is a NAME test — no dialect compares the existing definition,
and gnitz could not: a view's catalog rows are its compiled circuit, never its
text. So a *different* body under a taken name is skipped rather than compiled,
and a name held by the wrong kind of relation still ends the statement.

`CREATE OR REPLACE VIEW`, the clause that resolves a collision by rewriting
rather than by yielding, lives in `test_alter_view.py` beside the ALTER VIEW it
is the counterpart of.
"""

import gnitz
import pytest
from _oracle import assert_view_matches

_ROWS = {(1, 10, 100): 1, (2, 20, 200): 1}


@pytest.fixture
def base(client, schema_name):
    """`t` holding `(1, 10, 100)` and `(2, 20, 200)`."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=schema_name)
    return schema_name


# ── IF NOT EXISTS ───────────────────────────────────────────────────────────


def test_view_skip_leaves_the_standing_definition(client, base):
    """The clause is a name test, so a *different* body under a taken name is
    skipped, not compiled — and the standing view keeps serving its own rows."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=base)
    vid = client.resolve_table(base, "v")[0]
    client.execute_sql("CREATE VIEW IF NOT EXISTS v AS SELECT id, b FROM t", schema_name=base)
    assert client.resolve_table(base, "v")[0] == vid, "the skip creates nothing"
    assert_view_matches(client, vid, ["id", "a"], {(1, 10): 1, (2, 20): 1})


def test_view_creates_when_the_name_is_free(client, base):
    client.execute_sql("CREATE VIEW IF NOT EXISTS v AS SELECT id, a FROM t", schema_name=base)
    assert_view_matches(client, client.resolve_table(base, "v")[0], ["id", "a"],
                        {(1, 10): 1, (2, 20): 1})


def test_table_skip_keeps_the_rows(client, base):
    tid = client.resolve_table(base, "t")[0]
    client.execute_sql(
        "CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL)",
        schema_name=base)
    assert client.resolve_table(base, "t")[0] == tid
    assert_view_matches(client, tid, ["id", "a", "b"], _ROWS)


def test_table_creates_when_the_name_is_free(client, base):
    client.execute_sql(
        "CREATE TABLE IF NOT EXISTS fresh (id BIGINT NOT NULL PRIMARY KEY)", schema_name=base)
    client.execute_sql("INSERT INTO fresh VALUES (7)", schema_name=base)
    assert_view_matches(client, client.resolve_table(base, "fresh")[0], ["id"], {(7,): 1})


def test_index_skip_answers_with_the_standing_index(client, base):
    first = client.execute_sql("CREATE INDEX ix_a ON t (a)", schema_name=base)
    again = client.execute_sql("CREATE INDEX IF NOT EXISTS ix_a ON t (a)", schema_name=base)
    assert first == again, "the skip answers with the standing index"
    with pytest.raises(gnitz.GnitzError, match="Index already exists"):
        client.execute_sql("CREATE INDEX ix_a ON t (a)", schema_name=base)
    # A free name under the clause still creates.
    client.execute_sql("CREATE INDEX IF NOT EXISTS ix_b ON t (b)", schema_name=base)


def test_index_clause_requires_a_name(client, base):
    """`IF NOT EXISTS` tests a name, and the grammar will not let it be omitted
    — so the auto-named form never carries the clause."""
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE INDEX IF NOT EXISTS ON t (b)", schema_name=base)


def test_create_view_if_not_exists_yields_to_a_table(client, base):
    """No dialect compares the standing definition, and gnitz could not — a
    view's catalog rows are its compiled circuit, never its text. So any
    relation under the name ends the statement, whatever its kind."""
    tid = client.resolve_table(base, "t")[0]
    client.execute_sql("CREATE VIEW IF NOT EXISTS t AS SELECT id FROM t", schema_name=base)
    assert client.resolve_table(base, "t")[0] == tid, "the table is untouched"
    with pytest.raises(gnitz.GnitzError, match="already exists"):
        client.execute_sql("CREATE VIEW t AS SELECT id FROM t", schema_name=base)


# ── IF EXISTS ───────────────────────────────────────────────────────────────


def test_drop_missing_is_a_no_op_for_every_kind(client, base):
    client.execute_sql("DROP TABLE IF EXISTS nosuch", schema_name=base)
    client.execute_sql("DROP VIEW IF EXISTS nosuch", schema_name=base)
    client.execute_sql("DROP INDEX IF EXISTS nosuch", schema_name=base)
    assert_view_matches(client, client.resolve_table(base, "t")[0], ["id", "a", "b"], _ROWS)


def test_drop_present_still_drops(client, base):
    client.execute_sql("CREATE VIEW v AS SELECT id FROM t", schema_name=base)
    client.execute_sql("CREATE INDEX ix_a ON t (a)", schema_name=base)
    client.execute_sql("DROP VIEW IF EXISTS v", schema_name=base)
    client.execute_sql("DROP INDEX IF EXISTS ix_a", schema_name=base)
    client.execute_sql("DROP TABLE IF EXISTS t", schema_name=base)
    with pytest.raises(gnitz.GnitzError):
        client.resolve_table(base, "t")


def test_if_exists_does_not_soften_any_other_refusal(client, base):
    """The clause answers "no such object" only. A dependent view still blocks
    the drop, with the same error the bare form raises."""
    client.execute_sql("CREATE VIEW base AS SELECT id, a FROM t", schema_name=base)
    client.execute_sql("CREATE VIEW dep AS SELECT id FROM base", schema_name=base)
    with pytest.raises(gnitz.GnitzError, match="[Vv]iew dependency"):
        client.execute_sql("DROP VIEW IF EXISTS base", schema_name=base)


def test_drop_if_exists_does_not_cross_kinds(client, base):
    """`IF EXISTS` answers "no such object", not "some other kind of object" —
    `DROP TABLE IF EXISTS v` where `v` is a view must still refuse, or a
    teardown script would silently leave the view standing."""
    client.execute_sql("CREATE VIEW v AS SELECT id FROM t", schema_name=base)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("DROP TABLE IF EXISTS v", schema_name=base)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("DROP VIEW IF EXISTS t", schema_name=base)
    client.resolve_table(base, "v")
    client.resolve_table(base, "t")


def test_drop_if_exists_is_per_name_in_a_list(client, base):
    """One statement, several names: the clause no-ops the missing ones and
    still drops the present ones."""
    client.execute_sql("CREATE VIEW v1 AS SELECT id FROM t", schema_name=base)
    client.execute_sql("CREATE VIEW v2 AS SELECT a FROM t", schema_name=base)
    client.execute_sql("DROP VIEW IF EXISTS v1, nosuch, v2", schema_name=base)
    for name in ("v1", "v2"):
        with pytest.raises(gnitz.GnitzError):
            client.resolve_table(base, name)
