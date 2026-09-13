"""A view's definition changing under its own name: `ALTER VIEW ... AS`,
`CREATE OR REPLACE VIEW`, and the output column aliases both spellings accept.

Both retarget the name onto a fresh view id in one DDL zone. What separates
them is the `WITH (...)` clause: `CREATE OR REPLACE VIEW` restates the whole
definition, budgets included, while `ALTER VIEW ... AS` has nowhere to say one
and so refuses to retarget a view that carries one. The statement-level
refusals are planned without a server in `crates/gnitz-sql/tests`; the RESTRICT
a dependent view imposes is `test_alter.py`'s.
"""

import gnitz
import pytest
from _read import bag, scanned


@pytest.mark.parametrize("retarget", ["ALTER VIEW v AS", "CREATE OR REPLACE VIEW v AS"])
def test_retarget_swaps_the_definition_and_backfills(client, base, retarget):
    """The retargeted view serves the NEW body's rows over the data already
    there, at weight 1 each — not the old body's, and not both — takes a fresh
    id, and stays maintained afterwards."""
    client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=base)
    first = client.resolve_table(base, "v")[0]
    assert bag(scanned(client, base, "v"), "id", "a") == {(1, 10): 1, (2, 20): 1}

    client.execute_sql(f"{retarget} SELECT id, b FROM t", schema_name=base)
    assert client.resolve_table(base, "v")[0] != first, "a retarget takes a fresh view id"
    assert bag(scanned(client, base, "v"), "id", "b") == {(1, 100): 1, (2, 200): 1}

    client.execute_sql("INSERT INTO t VALUES (3, 30, 300)", schema_name=base)
    assert bag(scanned(client, base, "v"), "id", "b") == {(1, 100): 1, (2, 200): 1, (3, 300): 1}


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
        "CREATE OR REPLACE VIEW fed WITH (delta = '4 MB') AS SELECT id, b FROM t", schema_name=base)
    for name in ("bounded", "fed"):
        assert bag(scanned(client, base, name), "id", "b") == {(1, 100): 1, (2, 200): 1}, name
    with pytest.raises(gnitz.GnitzError, match="views cannot be created over it"):
        client.execute_sql("CREATE VIEW over_it AS SELECT id FROM bounded", schema_name=base)
    client.delta_bootstrap(*client.resolve_table(base, "fed"))

    client.execute_sql(
        "CREATE OR REPLACE VIEW bounded AS SELECT id, a FROM t; "
        "CREATE OR REPLACE VIEW fed AS SELECT id, a FROM t; "
        "CREATE VIEW over_it AS SELECT id FROM bounded", schema_name=base)
    with pytest.raises(gnitz.GnitzError):
        client.delta_bootstrap(*client.resolve_table(base, "fed"))


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
        "ALTER VIEW w (x, y) AS SELECT id, b FROM t", schema_name=base)

    for name, want in (("v", ab), ("bv", ab), ("downstream", ab), ("j", {(1, 10): 1}),
                       ("w", {(1, 100): 1, (2, 200): 1})):
        got = scanned(client, base, name)
        assert got[0]._fields == ("x", "y"), name
        assert bag(got, "x", "y") == want, name

    with pytest.raises(gnitz.GnitzError, match="column 'id' not found"):
        client.execute_sql("CREATE VIEW nope AS SELECT id FROM v", schema_name=base)
