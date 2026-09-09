"""The class-agnostic control core: NULL, the connectives, and branch selection.

`Select`, `BoolBinary`/`BoolNot`, `IsNull` and `IntInSet` carry no register class
of their own — CASE, COALESCE, NULLIF, IF, AND/OR/NOT, IS [NOT] NULL, IS [NOT]
DISTINCT FROM and IN/NOT IN obey one three-valued rule whatever the operands are,
so they are one file rather than a section of each per-class one.

**A NULL payload cell is zero-filled and the null bitmap is the sole truth**, so
a test only proves an operator reads that bitmap if the *zero* would have
answered differently. `WHERE a > 5` over a NULL excludes the row either way and
proves nothing; `WHERE a = 0` admits it under the bug and excludes it under the
rule. Every predicate here is chosen for that disconfirming power, and the ones
that lack it are why this file is shorter than the four it replaces.

Asserted on weights throughout: the failures this core can have are a row
admitted twice and a retraction that leaves a ghost, and a row set reads both as
correct.
"""

import pytest
from _read import bag, rows, scanned


@pytest.fixture
def nulls(client, schema_name):
    """`t (pk, a, b)` with both payload columns nullable, holding every
    TRUE/FALSE/NULL pairing of `a > 0` and `b > 0` at once.

    pk 1 T/T, 2 T/F, 3 F/T, 4 F/F, 5 N/T, 6 N/F, 7 T/N, 8 F/N, 9 N/N — so one
    insert drives the whole 3×3 connective table, and the `0` rows separate a
    genuine zero from the zero filler a NULL writes.
    """
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 1), (2, 1, 0), (3, 0, 1), (4, 0, 0), "
        "(5, NULL, 1), (6, NULL, 0), (7, 1, NULL), (8, 0, NULL), (9, NULL, NULL)",
        schema_name=schema_name)
    return schema_name


@pytest.mark.parametrize("pred,want", [
    # AND is TRUE only when both are, and FALSE beats NULL: pk 6 and 8 are
    # excluded because one side is definitely FALSE, not because the other is
    # unknown — the distinction a NULL-propagating AND would lose.
    ("a > 0 AND b > 0", {1}),
    # OR is TRUE when either is, so a NULL beside a TRUE still admits the row.
    ("a > 0 OR b > 0", {1, 2, 3, 5, 7}),
    # NOT maps NULL to NULL, so the negation is not the complement.
    ("NOT (a > 0)", {3, 4, 8}),
    # A NULL is not equal to zero, though its payload cell holds zero bytes.
    ("a = 0", {3, 4, 8}),
    ("a IS NULL", {5, 6, 9}),
    ("a IS NOT NULL", {1, 2, 3, 4, 7, 8}),
    # Arithmetic propagates NULL, and the test reads the propagated bit.
    ("(a + b) IS NULL", {5, 6, 7, 8, 9}),
    ("a + b = 0", {4}),
    # IS DISTINCT FROM is total: it is never itself unknown.
    ("a IS DISTINCT FROM b", {2, 3, 5, 6, 7, 8}),
    ("a IS NOT DISTINCT FROM b", {1, 4, 9}),
    # IN over a NULL operand is unknown; NOT IN over one is NOT(unknown), which
    # is also unknown — so a NULL is admitted by neither, and a zero-filler read
    # would admit pk 5, 6 and 9 to the second.
    ("a IN (0, 1)", {1, 2, 3, 4, 7, 8}),
    ("a NOT IN (1, 2)", {3, 4, 8}),
])
def test_a_connective_or_null_test_follows_the_three_valued_table(
        client, nulls, pred, want):
    """One predicate per row of the 3VL table, each over the nine pairings the
    fixture holds. Every row enters at weight 1 or not at all."""
    client.execute_sql(f"CREATE VIEW v AS SELECT pk FROM t WHERE {pred}",
                       schema_name=nulls)
    assert bag(scanned(client, nulls, "v"), "pk") == {(p,): 1 for p in want}


def test_a_predicate_over_a_null_retracts_and_readmits_its_row(client, nulls):
    """The verdict is recomputed on the retraction, so a row moving across the
    unknown boundary in either direction must cancel exactly — a stale verdict
    leaves the old row at weight 1 beside the new one."""
    client.execute_sql("CREATE VIEW v AS SELECT pk FROM t WHERE a = 0", schema_name=nulls)
    assert bag(scanned(client, nulls, "v"), "pk") == {(3,): 1, (4,): 1, (8,): 1}

    # A definite value becomes unknown …
    client.execute_sql("UPDATE t SET a = NULL WHERE pk = 3", schema_name=nulls)
    assert bag(scanned(client, nulls, "v"), "pk") == {(4,): 1, (8,): 1}
    # … and an unknown becomes definite.
    client.execute_sql("UPDATE t SET a = 0 WHERE pk = 5", schema_name=nulls)
    assert bag(scanned(client, nulls, "v"), "pk") == {(4,): 1, (5,): 1, (8,): 1}

    client.execute_sql("DELETE FROM t", schema_name=nulls)
    assert bag(scanned(client, nulls, "v"), "pk") == {}


def test_case_takes_its_first_true_branch_and_defaults_to_null(client, schema_name):
    """A searched CASE and a simple CASE are one node — the operand form desugars
    to `operand = label` — and an absent ELSE is `ELSE NULL`, not a dropped row.
    A branch whose test is unknown is not taken, exactly as a FALSE one is not.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, "
        "CASE WHEN a > 10 THEN 100 WHEN a > 0 THEN 10 ELSE 0 END AS searched, "
        "CASE a WHEN 1 THEN 111 WHEN 2 THEN 222 ELSE 999 END AS simple, "
        "CASE WHEN a > 10 THEN a END AS no_else FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 20), (2, 1), (3, 2), (4, -3), (5, NULL)",
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "pk", "searched", "simple", "no_else") == {
        (1, 100, 999, 20): 1,
        (2, 10, 111, None): 1,
        (3, 10, 222, None): 1,
        (4, 0, 999, None): 1,
        # Every test is unknown, so every branch is skipped and the ELSE runs;
        # with no ELSE the result is NULL, and the row still appears at weight 1.
        (5, 0, 999, None): 1,
    }


def test_the_null_substitutions_differ_only_where_their_rules_do(client, schema_name):
    """COALESCE, IFNULL, NVL and IF all desugar onto CASE, so the fact worth
    stating is the one place each differs: COALESCE walks to its first non-NULL
    over any arity, NULLIF *introduces* a NULL, and IF takes its else branch on
    an unknown test exactly as CASE does."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, s TEXT)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, COALESCE(a + b, a, -1) AS walk, "
        "IFNULL(a * 2, 0) AS ifn, NVL(a, -2) AS nvl, NULLIF(a, 0) AS nif, "
        "IF(a < b, 1, 0) AS iff FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 2, 'x'), (2, 5, NULL, NULL), "
        "(3, NULL, NULL, 'y'), (4, 0, 0, NULL)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "pk", "walk", "ifn", "nvl", "nif", "iff") == {
        (1, 3, 2, 1, 1, 1): 1,
        # a+b is NULL, so COALESCE steps to `a`; IFNULL sees a non-NULL product.
        (2, 5, 10, 5, 5, 0): 1,
        # Every argument is NULL, so COALESCE falls to its literal tail.
        (3, -1, 0, -2, None, 0): 1,
        # NULLIF(0, 0) is the NULL it introduces, and `0 < 0` is FALSE.
        (4, 0, 0, 0, None, 0): 1,
    }
    # A string COALESCE resolves in the string channel, where the substituted
    # literal has to be a string for the branches to share a register class.
    assert bag(rows(client, sn, "SELECT pk, COALESCE(s, 'none') AS c FROM t")) == {
        (1, "x"): 1, (2, "none"): 1, (3, "y"): 1, (4, "none"): 1}


def test_a_null_test_over_a_computed_operand_tests_its_value(client, schema_name):
    """The subject of a null test is any expression, not only a column, so the
    test has to read the *result's* null bit rather than the source column's."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, s TEXT)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, UPPER(s) IS NULL AS un, "
        "(a + 1) IS NOT NULL AS pn, (a + 1) IS DISTINCT FROM 1 AS dn FROM t",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 'x'), (2, 0, NULL), (3, NULL, 'y')",
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "pk", "un", "pn", "dn") == {
        (1, 0, 1, 1): 1,
        (2, 1, 1, 0): 1,   # 0 + 1 is 1, so it is not distinct from 1
        (3, 0, 0, 1): 1,   # NULL + 1 is NULL, which is distinct from 1
    }


@pytest.mark.parametrize("items", [2, 500])
def test_an_integer_membership_test_is_flat_in_the_size_of_its_list(
        client, schema_name, items):
    """An integer operand over integer literals is one set-membership opcode
    whatever the list's length, so a 500-element list has to compile and maintain
    exactly as a 2-element one does — including through a retraction that moves a
    row out of the set. A per-item comparison chain would spend a register each
    and fail to compile long before this size.
    """
    sn = schema_name
    members = list(range(-100, 400))[:items] if items > 2 else [0, 399]
    lo, hi = min(members), max(members)
    sql = ", ".join(str(v) for v in members)
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)", schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS SELECT pk FROM t WHERE val IN ({sql})",
                       schema_name=sn)
    client.execute_sql(f"CREATE VIEW vn AS SELECT pk FROM t WHERE val NOT IN ({sql})",
                       schema_name=sn)
    client.execute_sql(
        f"INSERT INTO t VALUES (1, {lo}), (2, {hi}), (3, {hi + 1}), (4, NULL)",
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "pk") == {(1,): 1, (2,): 1}
    # The NULL row is in neither view: NOT IN over an unknown is unknown too.
    assert bag(scanned(client, sn, "vn"), "pk") == {(3,): 1}

    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk") == {(1,): 1}
    assert bag(scanned(client, sn, "vn"), "pk") == {(3,): 1}


def test_a_string_membership_test_holds_the_same_rule(client, schema_name):
    """A string list cannot take the integer set opcode and falls back to a fused
    comparison per element, which must answer the same way — membership, and a
    NULL admitted by neither direction."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, tag TEXT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk FROM t WHERE tag IN ('red', 'blue')", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vn AS SELECT pk FROM t WHERE tag NOT IN ('red', 'blue')",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'red'), (2, 'green'), (3, 'blue'), (4, NULL)",
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "pk") == {(1,): 1, (3,): 1}
    assert bag(scanned(client, sn, "vn"), "pk") == {(2,): 1}
