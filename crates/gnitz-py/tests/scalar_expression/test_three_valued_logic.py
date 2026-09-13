"""The class-agnostic control core: NULL, the connectives, and branch selection.

`Select`, `BoolBinary`/`BoolNot`, `IsNull` and `IntInSet` carry no register class
of their own — CASE, COALESCE, NULLIF, IF, AND/OR/NOT, IS [NOT] NULL, IS [NOT]
DISTINCT FROM and IN/NOT IN obey one three-valued rule whatever the operands are,
so they are one file rather than a section of each per-class one.

**A NULL payload cell is zero-filled and the null bitmap is the sole truth**, so
a test only proves an operator reads that bitmap if the *zero* would have
answered differently. `WHERE a > 5` over a NULL excludes the row either way and
proves nothing; `WHERE a = 0` admits it under the bug and excludes it under the
rule. Every predicate here is chosen for that disconfirming power.

Asserted on weights throughout: the failures this core can have are a row
admitted twice and a retraction that leaves a ghost, and a row set reads both as
correct.
"""

from _read import bag, rows, scanned

# 500 integers ending at 0: one set-membership opcode whatever the list's
# length, where a per-item comparison chain would spend a register each and fail
# to compile long before this size.
_WIDE = ", ".join(str(v) for v in range(-499, 1))

# Over the nine `(a, b)` pairings of TRUE/FALSE/NULL for `a > 0` and `b > 0`,
# each predicate against the pks it admits. `s` is 'x' where a = 1, 'y' where
# a = 0, and NULL where a is NULL.
_PREDICATES = {
    # AND is TRUE only when both are, and FALSE beats NULL: pk 6 and 8 are
    # excluded because one side is definitely FALSE, not because the other is
    # unknown — the distinction a NULL-propagating AND would lose.
    "a > 0 AND b > 0": {1},
    # OR is TRUE when either is, so a NULL beside a TRUE still admits the row.
    "a > 0 OR b > 0": {1, 2, 3, 5, 7},
    # NOT maps NULL to NULL, so the negation is not the complement.
    "NOT (a > 0)": {3, 4, 8},
    # A NULL is not equal to zero, though its payload cell holds zero bytes.
    "a = 0": {3, 4, 8},
    "a IS NULL": {5, 6, 9},
    "a IS NOT NULL": {1, 2, 3, 4, 7, 8},
    # Arithmetic propagates NULL, and the test reads the propagated bit — the
    # result's, not the source column's.
    "(a + b) IS NULL": {5, 6, 7, 8, 9},
    "a + b = 0": {4},
    "UPPER(s) IS NULL": {5, 6, 9},
    # IS DISTINCT FROM is total: it is never itself unknown.
    "a IS DISTINCT FROM b": {2, 3, 5, 6, 7, 8},
    "a IS NOT DISTINCT FROM b": {1, 4, 9},
    "(a + 1) IS DISTINCT FROM 1": {1, 2, 5, 6, 7, 9},
    # IN over a NULL operand is unknown; NOT IN over one is NOT(unknown), which
    # is also unknown — so a NULL is admitted by neither, and a zero-filler read
    # would admit pk 5, 6 and 9 to the NOT IN. A string list cannot take the
    # integer set opcode and falls back to a fused compare per element.
    "a IN (0, 1)": {1, 2, 3, 4, 7, 8},
    "a NOT IN (1, 2)": {3, 4, 8},
    f"a IN ({_WIDE})": {3, 4, 8},
    f"a NOT IN ({_WIDE})": {1, 2, 7},
    "s IN ('x', 'z')": {1, 2, 7},
    "s NOT IN ('x', 'z')": {3, 4, 8},
}


def test_a_predicate_follows_the_three_valued_table_across_a_retraction(client, schema_name):
    """One filter view per predicate over one insert holding every pairing. The
    verdict is recomputed on the retraction, so a row moving across the unknown
    boundary in either direction must cancel exactly — a stale verdict leaves
    the old row at weight 1 beside the new one."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, s TEXT)",
        schema_name=sn)
    views = {p: f"v{n}" for n, p in enumerate(_PREDICATES)}
    for p, v in views.items():
        client.execute_sql(f"CREATE VIEW {v} AS SELECT pk FROM t WHERE {p}", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 1, 'x'), (2, 1, 0, 'x'), (3, 0, 1, 'y'), (4, 0, 0, 'y'), "
        "(5, NULL, 1, NULL), (6, NULL, 0, NULL), (7, 1, NULL, 'x'), (8, 0, NULL, 'y'), "
        "(9, NULL, NULL, NULL)", schema_name=sn)

    for p, want in _PREDICATES.items():
        assert bag(scanned(client, sn, views[p])) == {(k,): 1 for k in want}, views[p]

    # A definite value becomes unknown, and an unknown becomes definite.
    client.execute_sql("UPDATE t SET a = NULL WHERE pk = 3", schema_name=sn)
    client.execute_sql("UPDATE t SET a = 0 WHERE pk = 5", schema_name=sn)
    assert bag(scanned(client, sn, views["a = 0"])) == {(4,): 1, (5,): 1, (8,): 1}
    for p, v in views.items():
        assert bag(scanned(client, sn, v)) == \
            bag(rows(client, sn, f"SELECT pk FROM t WHERE {p}")), v

    client.execute_sql("DELETE FROM t", schema_name=sn)
    for v in views.values():
        assert bag(scanned(client, sn, v)) == {}, v


def test_a_branch_is_taken_on_true_alone_and_a_missing_else_is_null(client, schema_name):
    """A searched CASE and a simple CASE are one node — the operand form desugars
    to `operand = label` — and an absent ELSE is `ELSE NULL`, not a dropped row. A
    branch whose test is unknown is not taken, exactly as a FALSE one is not.

    COALESCE, IFNULL, NVL and IF all desugar onto CASE, so the fact worth
    stating is where each differs: COALESCE walks to its first non-NULL over any
    arity, NULLIF *introduces* a NULL, and IF takes its else branch on an unknown
    test. A string CASE and COALESCE resolve in the string channel."""
    sn = schema_name
    exprs = ["CASE WHEN a > 10 THEN 100 WHEN a > 0 THEN 10 ELSE 0 END",
             "CASE a WHEN 1 THEN 111 WHEN 2 THEN 222 ELSE 999 END",
             "CASE WHEN a > 10 THEN a END", "COALESCE(a + b, a, -1)", "IFNULL(a * 2, 0)",
             "NVL(a, -2)", "NULLIF(a, 0)", "IF(a < b, 1, 0)", "CASE WHEN a > 10 THEN s END",
             "COALESCE(s, 'none')"]
    select = "SELECT pk, " + ", ".join(f"{e} AS c{n}" for n, e in enumerate(exprs)) + " FROM t"
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, s TEXT)",
        schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS {select}", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 20, NULL, 'x'), (2, 1, 2, NULL), (3, 2, NULL, 'y'), "
        "(4, -3, -3, NULL), (5, NULL, NULL, NULL), (6, 0, 0, 'z')", schema_name=sn)

    expected = {
        # `a + b` is NULL, so COALESCE steps to `a`; `20 < NULL` is unknown, so
        # IF takes its else branch.
        (1, 100, 999, 20, 20, 40, 20, 20, 0, "x", "x"): 1,
        (2, 10, 111, None, 3, 2, 1, 1, 1, None, "none"): 1,
        (3, 10, 222, None, 2, 4, 2, 2, 0, None, "y"): 1,
        (4, 0, 999, None, -6, -6, -3, -3, 0, None, "none"): 1,
        # Every test is unknown, so every branch is skipped and the ELSE runs;
        # with no ELSE the result is NULL, and the row still appears at weight 1.
        # Every COALESCE argument is NULL, so it falls to its literal tail.
        (5, 0, 999, None, -1, 0, -2, None, 0, None, "none"): 1,
        # NULLIF(0, 0) is the NULL it introduces, and `0 < 0` is FALSE.
        (6, 0, 999, None, 0, 0, 0, None, 0, None, "z"): 1,
    }
    assert bag(scanned(client, sn, "v")) == expected
    assert bag(rows(client, sn, select)) == expected
