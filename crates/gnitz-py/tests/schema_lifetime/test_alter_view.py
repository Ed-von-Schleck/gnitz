"""A view's definition changing under its own name: `ALTER VIEW ... AS` and
`CREATE OR REPLACE VIEW`.

Both retarget the name onto a fresh view id in one DDL zone, unless the
definition they state is the one standing, which keeps the view. What separates
them is the `WITH (...)` clause: `CREATE OR REPLACE VIEW` restates the whole
definition, budgets included, while `ALTER VIEW ... AS` has nowhere to say one
and so refuses to retarget a view that carries one. The statement-level
refusals are planned without a server in `crates/gnitz-sql/src/suites`; the
RESTRICT a dependent view imposes is `test_alter.py`'s; the output column
aliases are `relational_shape/test_view_column_order.py`'s.
"""

import pytest
from _catalog import family, schema_id
from _feedviews import Subscriber
from _read import bag, scanned


def _replaces(client, sql, name):
    """Whether `sql` put a new view under `name`: a replaced view takes a fresh id."""
    before = client.resolve_table(name)[0]
    client.execute_sql(sql)
    return client.resolve_table(name)[0] != before


_DEFINITIONS = [  # (first, second, replaced)
    ("SELECT id, a FROM t", "select  id ,\n a\nfrom   t", False),
    ("SELECT id, a FROM t", "SELECT x.id, x.a FROM t AS x", False),
    ("SELECT id, a, b FROM t", "SELECT * FROM t", False),
    ("SELECT id FROM t WHERE a > 5 AND b > 50", "SELECT id FROM t WHERE ((a > 5) AND (b > 50))", False),
    ("SELECT a, SUM(b) AS s FROM t GROUP BY a", "SELECT a, SUM(b) AS s FROM t GROUP BY a", False),
    ("SELECT x.id, y.b FROM t x JOIN t y ON x.a = y.a", "SELECT x.id, y.b FROM t x JOIN t y ON x.a = y.a", False),
    ("SELECT id, a FROM t WHERE a > 5", "SELECT id, a FROM t WHERE a > 15", True),
    ("SELECT id, a AS x FROM t", "SELECT id, b AS x FROM t", True),
    ("SELECT a, SUM(b) AS s FROM t GROUP BY a", "SELECT a, MAX(b) AS s FROM t GROUP BY a", True),
    ("SELECT x.id, y.b FROM t x JOIN t y ON x.a = y.a", "SELECT x.id, y.b FROM t x JOIN t y ON x.a = y.b", True),
    ("SELECT id, a FROM t", "SELECT id, a AS a2 FROM t", True),
]


@pytest.mark.parametrize("verb", ["ALTER VIEW", "CREATE OR REPLACE VIEW"])
def test_a_view_is_replaced_only_by_a_different_definition(client, base, verb):
    """Another spelling of the standing definition keeps the view; another
    definition takes a fresh id. Either way the view then holds, and keeps
    maintaining, what a view freshly created over the second body holds —
    weights included, so the old body's rows left beside the new ones show."""
    for i, (first, _, _) in enumerate(_DEFINITIONS):
        client.execute_sql(f"CREATE VIEW v{i} AS {first}")
    assert [_replaces(client, f"{verb} v{i} AS {second}", f"v{i}")
            for i, (_, second, _) in enumerate(_DEFINITIONS)] == [r for *_, r in _DEFINITIONS]
    client.execute_sql("; ".join(f"CREATE VIEW f{i} AS {second}" for i, (_, second, _) in enumerate(_DEFINITIONS)))
    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)")
    for i, d in enumerate(_DEFINITIONS):
        assert bag(scanned(client, f"v{i}")) == bag(scanned(client, f"f{i}")), d


def test_the_with_clause_is_part_of_the_definition(client, base):
    """Adding or changing a `WITH (...)` replaces the view and restating it does
    not. `CREATE OR REPLACE VIEW` writes the budget out as stated: a restated one
    is kept on the new body, and an omitted one asks for an unbounded, unfed view
    rather than silently carrying the old budget forward."""
    body = "SELECT id, a FROM t"
    client.execute_sql(f"CREATE VIEW bounded WITH (capacity = '4 MB') AS {body}; CREATE VIEW fed AS {body}")
    for budget, replaced in (("1 MB", True), ("1 MB", False), ("4 MB", True)):
        sql = f"CREATE OR REPLACE VIEW fed WITH (delta = '{budget}') AS {body}"
        assert _replaces(client, sql, "fed") == replaced, budget

    def budgets():
        return family(client, "views", "name", "capacity_bytes", "delta_bytes",
                      where=f"schema_id = {schema_id(client)}")

    client.execute_sql(
        "CREATE OR REPLACE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, b FROM t; "
        "CREATE OR REPLACE VIEW fed WITH (delta = '4 MB') AS SELECT id, b FROM t")
    assert budgets() == {("bounded", 4 << 20, 0): 1, ("fed", 0, 4 << 20): 1}
    for name in ("bounded", "fed"):
        assert bag(scanned(client, name)) == {(1, 100): 1, (2, 200): 1}, name

    for name in ("bounded", "fed"):
        assert _replaces(client, f"CREATE OR REPLACE VIEW {name} AS {body}", name)
    assert budgets() == {("bounded", 0, 0): 1, ("fed", 0, 0): 1}


def test_an_unchanged_replace_keeps_a_feed_cursor_and_a_dependent_view(client, base):
    """Restating the standing definition replaces nothing: a feed cursor taken
    before it polls on, and a view over it keeps maintaining."""
    body = "SELECT id, a FROM t"
    client.execute_sql(
        f"CREATE VIEW v WITH (delta = '1 MB') AS {body}; CREATE VIEW w AS SELECT id FROM v WHERE a > 5")
    sub = Subscriber(client, "v")
    sub.bootstrap()

    assert not _replaces(client, f"CREATE OR REPLACE VIEW v WITH (delta = '1 MB') AS {body}", "v")
    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)")
    assert bag(sub.poll()) == {(3, 30): 1}
    assert bag(scanned(client, "w")) == {(1,): 1, (2,): 1, (3,): 1}


def test_a_renamed_source_leaves_the_definition_standing(client, base):
    """A definition records what it resolved to, not its text: restated over the
    renamed source's new names it is the same definition."""
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, a FROM t; ALTER TABLE t RENAME COLUMN a TO a_new; ALTER TABLE t RENAME TO u")
    assert not _replaces(client, "CREATE OR REPLACE VIEW v AS SELECT id, a_new AS a FROM u", "v")


def test_one_text_read_in_another_schema_is_another_definition(other_client, client, base):
    """The same text, resolved from another schema, reads another `t`: it
    replaces the view, which then maintains from that `t`."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t")
    before = client.resolve_table("v")[0]
    other_client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        f"CREATE OR REPLACE VIEW {client.schema}.v AS SELECT id, a FROM t; "
        "INSERT INTO t VALUES (9, 90, 900)")
    assert client.resolve_table("v")[0] != before
    assert bag(scanned(client, "v")) == {(9, 90): 1}
