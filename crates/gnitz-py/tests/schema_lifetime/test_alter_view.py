"""A view's definition changing under its own name: `ALTER VIEW ... AS`,
`CREATE OR REPLACE VIEW`, and the output column aliases both spellings accept.

Both retarget the name onto a fresh view id in one DDL zone, unless the
definition they state is the one standing, which keeps the view. What separates
them is the `WITH (...)` clause: `CREATE OR REPLACE VIEW` restates the whole
definition, budgets included, while `ALTER VIEW ... AS` has nowhere to say one
and so refuses to retarget a view that carries one. The statement-level
refusals are planned without a server in `crates/gnitz-sql/tests`; the RESTRICT
a dependent view imposes is `test_alter.py`'s.
"""

import gnitz
import pytest
from _read import bag, scanned


def _replaces(client, sql, name="v"):
    """Whether `sql` put a new view under `name`: a replaced view takes a fresh id."""
    before = client.resolve_table(name)[0]
    client.execute_sql(sql)
    return client.resolve_table(name)[0] != before

@pytest.mark.parametrize("retarget", ["ALTER VIEW v AS", "CREATE OR REPLACE VIEW v AS"])
def test_retarget_swaps_the_definition_and_backfills(client, base, retarget):
    """The retargeted view serves the NEW body's rows over the data already
    there, at weight 1 each — not the old body's, and not both — takes a fresh
    id, and stays maintained afterwards."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t")
    assert bag(scanned(client, "v"), "id", "a") == {(1, 10): 1, (2, 20): 1}

    assert _replaces(client, f"{retarget} SELECT id, b FROM t")
    assert bag(scanned(client, "v"), "id", "b") == {(1, 100): 1, (2, 200): 1}

    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)")
    assert bag(scanned(client, "v"), "id", "b") == {(1, 100): 1, (2, 200): 1, (3, 300): 1}


def test_or_replace_writes_the_with_clause_out_as_stated(client, base):
    """`CREATE OR REPLACE VIEW` restates the whole definition: a restated
    `WITH (...)` keeps the budget, and an omitted one asks for an unbounded,
    unfed view rather than silently carrying the old budget forward.

    Rows alone cannot tell a bounded view under its cap from an unbounded one,
    nor a fed view from an unfed one, so the two properties are checked
    directly: a bounded view is a leaf, and a fed view is the only kind a delta
    bootstrap answers.
    """
    client.execute_sql(
        "CREATE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, a FROM t; "
        "CREATE VIEW fed WITH (delta = '4 MB') AS SELECT id, a FROM t; "
        "CREATE OR REPLACE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, b FROM t; "
        "CREATE OR REPLACE VIEW fed WITH (delta = '4 MB') AS SELECT id, b FROM t")
    for name in ("bounded", "fed"):
        assert bag(scanned(client, name), "id", "b") == {(1, 100): 1, (2, 200): 1}, name
    with pytest.raises(gnitz.GnitzRefusedError, match="requires a relation a view can be created over"):
        client.execute_sql("CREATE VIEW over_it AS SELECT id FROM bounded")
    assert bag(client.delta_bootstrap(*client.resolve_table("fed"))[0], "id", "b") == {
        (1, 100): 1, (2, 200): 1}

    client.execute_sql(
        "CREATE OR REPLACE VIEW bounded AS SELECT id, a FROM t; "
        "CREATE OR REPLACE VIEW fed AS SELECT id, a FROM t; "
        "CREATE VIEW over_it AS SELECT id FROM bounded")
    with pytest.raises(gnitz.GnitzRefusedError, match="carries no delta feed"):
        client.delta_bootstrap(*client.resolve_table("fed"))


def test_aliases_rename_the_registered_output(client, base):
    """The alias list renames the output positionally on every body and budget
    and on both spellings: a plain body, a bounded view, a join body whose
    hidden `_join_pk` the list skips, and `ALTER VIEW v (x, y)`. The rename lands
    on the registered schema, so a second view selects the aliased names and
    the body's own names no longer resolve."""
    ab = {(1, 10): 1, (2, 20): 1}
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL); "
        "INSERT INTO u VALUES (7, 10); "
        "CREATE VIEW v (x, y) AS SELECT id, a FROM t; "
        "CREATE VIEW bv (x, y) WITH (capacity = '4 MB') AS SELECT id, a FROM t; "
        "CREATE VIEW j (x, y) AS SELECT t.id, u.a FROM t JOIN u ON t.a = u.a; "
        "CREATE VIEW downstream AS SELECT x, y FROM v; "
        "CREATE VIEW w AS SELECT id, a FROM t; "
        "ALTER VIEW w (x, y) AS SELECT id, b FROM t")

    for name, want in (("v", ab), ("bv", ab), ("downstream", ab), ("j", {(1, 10): 1}),
                       ("w", {(1, 100): 1, (2, 200): 1})):
        got = scanned(client, name)
        assert got[0]._fields == ("x", "y"), name
        assert bag(got, "x", "y") == want, name

    with pytest.raises(gnitz.GnitzRefusedError, match="column 'id' not found"):
        client.execute_sql("CREATE VIEW nope AS SELECT id FROM v")



@pytest.mark.parametrize("first, second, replaced", [
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
])
def test_or_replace_replaces_only_a_different_definition(client, base, first, second, replaced):
    """A view is replaced by another definition, not by another spelling of its own."""
    client.execute_sql(f"CREATE VIEW v AS {first}")
    assert _replaces(client, f"CREATE OR REPLACE VIEW v AS {second}") == replaced
    client.execute_sql(f"CREATE VIEW fresh AS {second}")
    assert bag(scanned(client, "v")) == bag(scanned(client, "fresh"))


def test_a_views_options_and_alter_view_are_compared_too(client, base):
    body = "SELECT id, a FROM t"
    client.execute_sql(f"CREATE VIEW v AS {body}")
    assert _replaces(client, f"CREATE OR REPLACE VIEW v WITH (delta = '1 MB') AS {body}")
    assert not _replaces(client, f"CREATE OR REPLACE VIEW v WITH (delta = '1 MB') AS {body}")
    assert _replaces(client, f"CREATE OR REPLACE VIEW v WITH (delta = '2 MB') AS {body}")
    assert _replaces(client, f"CREATE OR REPLACE VIEW v AS {body}")
    assert not _replaces(client, f"ALTER VIEW v AS {body}")
    assert _replaces(client, f"ALTER VIEW v AS {body} WHERE a > 15")
    assert bag(scanned(client, "v"), "id", "a") == {(2, 20): 1}


def test_an_unchanged_replace_keeps_a_feed_cursor_and_a_dependent_view(client, base):
    body = "SELECT id, a FROM t"
    client.execute_sql(
        f"CREATE VIEW v WITH (delta = '1 MB') AS {body}; CREATE VIEW w AS SELECT id FROM v WHERE a > 5")
    vid, schema = client.resolve_table("v")
    _, cursor = client.delta_bootstrap(vid, schema)

    assert not _replaces(client, f"CREATE OR REPLACE VIEW v WITH (delta = '1 MB') AS {body}")
    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)")
    rows, _ = client.delta_poll(vid, schema, cursor)
    assert bag(rows, "id", "a") == {(3, 30): 1}

    with pytest.raises(gnitz.GnitzRefusedError):
        client.execute_sql(f"CREATE OR REPLACE VIEW v WITH (delta = '1 MB') AS {body} WHERE a > 15")
    assert bag(scanned(client, "w"), "id") == {(1,): 1, (2,): 1, (3,): 1}


def test_a_renamed_source_leaves_the_definition_standing(client, base):
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t")
    client.execute_sql("ALTER TABLE t RENAME COLUMN a TO a_new; ALTER TABLE t RENAME TO u")
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE OR REPLACE VIEW v AS SELECT id, a FROM t")
    assert not _replaces(client, "CREATE OR REPLACE VIEW v AS SELECT id, a_new AS a FROM u")


def test_one_text_read_in_another_schema_is_another_definition(client, base, server):
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t")
    before = client.resolve_table("v")[0]
    other = f"o{client.schema}"
    client.create_schema(other)
    try:
        with gnitz.connect(server, schema=other) as second:
            second.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)")
            second.execute_sql(f"CREATE OR REPLACE VIEW {client.schema}.v AS SELECT id, a FROM t")
        assert client.resolve_table("v")[0] != before
        assert bag(scanned(client, "v"), "id", "a") == {}
        client.execute_sql("DROP VIEW v")
    finally:
        client.drop_schema(other)
