"""A view's definition changing under its own name: `ALTER VIEW ... AS`,
`CREATE OR REPLACE VIEW`, and the output column aliases both spellings accept.

The two statements share `plan_create_view`'s `replace` arm, so they compile,
register and backfill identically. What separates them is the `WITH (...)`
clause: `CREATE OR REPLACE VIEW` restates the whole definition and can write the
budgets out, while `ALTER VIEW ... AS` has nowhere to say either thing and so
refuses to retarget a view that carries one.
"""

import gnitz
import pytest
from _oracle import assert_view_matches
from _read import bag, scanned


@pytest.fixture
def base(client, schema_name):
    """`t` holding `(1, 10, 100)` and `(2, 20, 200)`, in a schema of its own."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=schema_name)
    return schema_name


# ── Retargeting a definition ────────────────────────────────────────────────


@pytest.mark.parametrize("retarget", ["ALTER VIEW v AS", "CREATE OR REPLACE VIEW v AS"])
def test_retarget_swaps_the_definition_and_backfills(client, base, retarget):
    """The retargeted view serves the NEW body's rows over the data already
    there, at weight 1 each — not the old body's, and not both — takes a fresh
    id, and stays maintained afterwards."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=base)
    first = client.resolve_table(base, "v")[0]
    assert_view_matches(client, first, ["id", "a"], {(1, 10): 1, (2, 20): 1}, "before")

    client.execute_sql(f"{retarget} SELECT id, b FROM t", schema_name=base)
    second = client.resolve_table(base, "v")[0]
    assert second != first, "a retarget takes a fresh view id"
    assert_view_matches(client, second, ["id", "b"], {(1, 100): 1, (2, 200): 1}, "after")

    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)", schema_name=base)
    assert_view_matches(client, second, ["id", "b"],
                        {(1, 100): 1, (2, 200): 1, (3, 300): 1}, "after + insert")


def test_or_replace_over_a_free_name_is_a_plain_create(client, base):
    client.execute_sql("CREATE OR REPLACE VIEW fresh AS SELECT id, a FROM t", schema_name=base)
    assert_view_matches(client, client.resolve_table(base, "fresh")[0], ["id", "a"],
                        {(1, 10): 1, (2, 20): 1})


@pytest.mark.parametrize("retarget", ["ALTER VIEW base AS", "CREATE OR REPLACE VIEW base AS"])
def test_retarget_is_restricted_by_a_dependent_view(client, base, retarget):
    """A retarget takes a fresh id, so retiring the old one would strand every
    circuit that scans it. Nothing is torn down: the old definition still serves
    its own rows after the refusal."""
    client.execute_sql("CREATE VIEW base AS SELECT id, a FROM t", schema_name=base)
    client.execute_sql("CREATE VIEW dep AS SELECT id FROM base", schema_name=base)

    with pytest.raises(gnitz.GnitzError, match="[Vv]iew dependency"):
        client.execute_sql(f"{retarget} SELECT id, b FROM t", schema_name=base)

    assert_view_matches(client, client.resolve_table(base, "base")[0], ["id", "a"],
                        {(1, 10): 1, (2, 20): 1}, "after the refused retarget")


@pytest.mark.parametrize("retarget", ["ALTER VIEW v AS", "CREATE OR REPLACE VIEW v AS"])
def test_retarget_refuses_a_body_that_reads_the_view_itself(client, base, retarget):
    """Rejected before any zone is issued, and the standing view survives."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=base)
    with pytest.raises(gnitz.GnitzError, match="itself"):
        client.execute_sql(f"{retarget} SELECT id FROM v", schema_name=base)
    assert_view_matches(client, client.resolve_table(base, "v")[0], ["id", "a"],
                        {(1, 10): 1, (2, 20): 1}, "after the refused self-reference")


def test_a_table_is_the_wrong_kind_for_or_replace(client, base):
    with pytest.raises(gnitz.GnitzError, match="is a table; CREATE OR REPLACE VIEW requires a view"):
        client.execute_sql("CREATE OR REPLACE VIEW t AS SELECT id FROM t", schema_name=base)


def test_a_stream_is_the_wrong_kind_for_or_replace(client, schema_name):
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL) "
        "WITH (stream = true)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="is a stream; CREATE OR REPLACE VIEW requires a view"):
        client.execute_sql("CREATE OR REPLACE VIEW ev AS SELECT k FROM ev", schema_name=schema_name)


def test_or_replace_with_if_not_exists_is_refused(client, base):
    with pytest.raises(gnitz.GnitzError, match="ask for opposite outcomes"):
        client.execute_sql(
            "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT id FROM t", schema_name=base)


def test_or_replace_on_a_table_is_refused_by_name(client, base):
    """`CREATE OR REPLACE TABLE` parses under GenericDialect and means "discard
    this table's rows"; DROP TABLE already spells that out loud."""
    with pytest.raises(gnitz.GnitzError, match=r"OR REPLACE \(it would discard"):
        client.execute_sql("CREATE OR REPLACE TABLE t (id BIGINT PRIMARY KEY)", schema_name=base)


# ── The WITH clause is what separates the two spellings ─────────────────────


def test_or_replace_carries_the_with_clause_where_alter_view_cannot(client, base):
    """`ALTER VIEW ... AS` refuses a bounded or fed view because its grammar has
    nowhere to restate the budget. `CREATE OR REPLACE VIEW` writes it out, so
    the same retarget is exactly what the statement asked for.

    Rows alone cannot tell a bounded view under its cap from an unbounded one,
    nor a fed view from an unfed one, so the two properties are checked
    directly: a bounded view is a leaf, and a fed view is the only kind a delta
    bootstrap answers.
    """
    client.execute_sql(
        "CREATE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, a FROM t", schema_name=base)
    client.execute_sql(
        "CREATE VIEW fed WITH (delta = '4 MB') AS SELECT id, a FROM t", schema_name=base)

    with pytest.raises(gnitz.GnitzError, match="cannot retarget a capacity-bounded view"):
        client.execute_sql("ALTER VIEW bounded AS SELECT id, b FROM t", schema_name=base)
    with pytest.raises(gnitz.GnitzError, match="cannot retarget a view with a delta feed"):
        client.execute_sql("ALTER VIEW fed AS SELECT id, b FROM t", schema_name=base)

    client.execute_sql(
        "CREATE OR REPLACE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, b FROM t",
        schema_name=base)
    client.execute_sql(
        "CREATE OR REPLACE VIEW fed WITH (delta = '4 MB') AS SELECT id, b FROM t", schema_name=base)
    for name in ("bounded", "fed"):
        assert_view_matches(client, client.resolve_table(base, name)[0], ["id", "b"],
                            {(1, 100): 1, (2, 200): 1}, name)

    with pytest.raises(gnitz.GnitzError, match="views cannot be created over it"):
        client.execute_sql("CREATE VIEW over_it AS SELECT id FROM bounded", schema_name=base)
    fed_id, fed_schema = client.resolve_table(base, "fed")
    client.delta_bootstrap(fed_id, fed_schema)


def test_or_replace_honours_an_omitted_with_clause_as_written(client, base):
    """`CREATE OR REPLACE VIEW` restates the whole definition, so omitting
    `WITH (...)` asks for an unbounded, unfed view — it does not silently carry
    the old view's budgets forward. That is the decision that separates it from
    `ALTER VIEW ... AS`, which refuses the retarget precisely because it has no
    way to say either thing."""
    client.execute_sql(
        "CREATE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, a FROM t", schema_name=base)
    client.execute_sql(
        "CREATE VIEW fed WITH (delta = '4 MB') AS SELECT id, a FROM t", schema_name=base)

    client.execute_sql("CREATE OR REPLACE VIEW bounded AS SELECT id, b FROM t", schema_name=base)
    client.execute_sql("CREATE OR REPLACE VIEW fed AS SELECT id, b FROM t", schema_name=base)

    # No longer a leaf, and no longer fed.
    client.execute_sql("CREATE VIEW over_it AS SELECT id FROM bounded", schema_name=base)
    fed_id, fed_schema = client.resolve_table(base, "fed")
    with pytest.raises(gnitz.GnitzError):
        client.delta_bootstrap(fed_id, fed_schema)


def test_alter_view_refuses_a_with_clause_outright(client, base):
    """sqlparser reads a `WITH (...)` between ALTER VIEW's column list and its
    `AS`. gnitz rejects it: letting ALTER VIEW set a budget would make the
    clause-less form silently drop the one the view already had."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=base)
    with pytest.raises(gnitz.GnitzError, match="ALTER VIEW: WITH options is not supported"):
        client.execute_sql(
            "ALTER VIEW v WITH (capacity = '4 MB') AS SELECT id, b FROM t", schema_name=base)


# ── Output column aliases ───────────────────────────────────────────────────


@pytest.mark.parametrize("stmt", ["CREATE VIEW v (x, y) AS", "CREATE VIEW v (x, y) WITH (capacity = '4 MB') AS"])
def test_aliases_rename_the_output_whatever_budget_the_view_carries(client, base, stmt):
    """The alias step runs over the final segment whatever budgets it carries,
    so a bounded view is registered under the aliased names too."""
    client.execute_sql(f"{stmt} SELECT id, a FROM t", schema_name=base)
    got = scanned(client, base, "v")
    assert sorted(got[0]._fields) == ["x", "y"]
    assert bag(got, "x", "y") == {(1, 10): 1, (2, 20): 1}


def test_aliases_skip_a_join_bodys_hidden_key_region(client, base):
    """A join body's synthetic `_join_pk` is hidden, so the alias list names
    exactly the columns a reader sees — no off-by-one against the key slot."""
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=base)
    client.execute_sql("INSERT INTO u VALUES (7, 10)", schema_name=base)
    client.execute_sql(
        "CREATE VIEW j (left_id, right_val) AS SELECT t.id, u.a FROM t JOIN u ON t.a = u.a",
        schema_name=base)
    got = scanned(client, base, "j")
    assert sorted(got[0]._fields) == ["left_id", "right_val"]
    assert bag(got, "left_id", "right_val") == {(1, 10): 1}


def test_aliases_rename_the_projection_but_do_not_disambiguate_it(client, base):
    """The alias list renames what the body produced; it does not stand in for
    naming the projection. A body that names one output twice is rejected where
    it is written, before any alias is applied — the fix is `AS` on the
    projection item."""
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=base)
    client.execute_sql("INSERT INTO u VALUES (7, 10)", schema_name=base)
    with pytest.raises(gnitz.GnitzError,
                       match="duplicate column name 'id' in CREATE VIEW projection"):
        client.execute_sql(
            "CREATE VIEW j (x, y) AS SELECT t.id, u.id FROM t JOIN u ON t.a = u.a",
            schema_name=base)
    client.execute_sql(
        "CREATE VIEW j (x, y) AS SELECT t.id AS l, u.id AS r FROM t JOIN u ON t.a = u.a",
        schema_name=base)
    assert bag(scanned(client, base, "j"), "x", "y") == {(1, 7): 1}


def test_aliases_reach_the_catalog_and_resolve_downstream(client, base):
    """The rename lands on the registered view schema, not just the local plan:
    a second view selects the aliased names, and the body's own names no longer
    resolve."""
    client.execute_sql("CREATE VIEW v (x, y) AS SELECT id, a FROM t", schema_name=base)
    client.execute_sql("CREATE VIEW downstream AS SELECT x, y FROM v", schema_name=base)
    assert bag(scanned(client, base, "downstream"), "x", "y") == {(1, 10): 1, (2, 20): 1}
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE VIEW nope AS SELECT id FROM v", schema_name=base)


def test_alter_view_honours_the_same_aliases(client, base):
    """One clause, one meaning: `ALTER VIEW v (x, y) AS ...` renames the output
    exactly as the CREATE form does, through the same helper."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=base)
    client.execute_sql("ALTER VIEW v (x, y) AS SELECT id, b FROM t", schema_name=base)
    got = scanned(client, base, "v")
    assert sorted(got[0]._fields) == ["x", "y"]
    assert bag(got, "x", "y") == {(1, 100): 1, (2, 200): 1}
