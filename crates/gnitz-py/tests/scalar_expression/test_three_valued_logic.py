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

import gnitz
from _exprs import filtered, projected

I64 = (gnitz.TypeCode.I64, 0)

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


def test_a_predicate_follows_the_three_valued_table_across_a_retraction(client):
    """One filter view per predicate over one insert holding every pairing. The
    verdict is recomputed on the retraction, so a row moving across the unknown
    boundary in either direction must cancel exactly — a stale verdict leaves
    the old row at weight 1 beside the new one."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, s TEXT)")
    held = filtered(client, _PREDICATES, key="pk")
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 1, 'x'), (2, 1, 0, 'x'), (3, 0, 1, 'y'), (4, 0, 0, 'y'), "
        "(5, NULL, 1, NULL), (6, NULL, 0, NULL), (7, 1, NULL, 'x'), (8, 0, NULL, 'y'), "
        "(9, NULL, NULL, NULL)")
    assert held() == _PREDICATES

    # A definite value becomes unknown, and an unknown becomes definite.
    client.execute_sql("UPDATE t SET a = NULL WHERE pk = 3")
    client.execute_sql("UPDATE t SET a = 0 WHERE pk = 5")
    assert held()["a = 0"] == {4, 5, 8}

    client.execute_sql("DELETE FROM t")
    assert held() == dict.fromkeys(_PREDICATES, set())


# Each branch form against its value on ids 1..6, over the `(a, b)` rows
# (20, NULL), (1, 2), (2, NULL), (-3, -3), (NULL, NULL), (0, 0).
_BRANCHES = {
    "CASE WHEN a > 10 THEN 100 WHEN a > 0 THEN 10 ELSE 0 END": (I64, [100, 10, 10, 0, 0, 0]),
    "CASE a WHEN 1 THEN 111 WHEN 2 THEN 222 ELSE 999 END": (I64, [999, 111, 222, 999, 999, 999]),
    # With no ELSE the result is NULL, and the row still appears at weight 1.
    "CASE WHEN a > 10 THEN a END": (I64, [20, None, None, None, None, None]),
    # `a + b` is NULL on id 1 and 3, so COALESCE steps to `a`; on id 5 every
    # argument is NULL, so it falls to its literal tail.
    "COALESCE(a + b, a, -1)": (I64, [20, 3, 2, -6, -1, 0]),
    "NULLIF(a, 0)": (I64, [20, 1, 2, -3, None, None]),
}


def test_a_branch_is_taken_on_true_alone_and_a_missing_else_is_null(client):
    """A searched CASE and a simple CASE are one node — the operand form desugars
    to `operand = label` — and an absent ELSE is `ELSE NULL`, not a dropped row. A
    branch whose test is unknown is not taken, exactly as a FALSE one is not:
    on id 5 every test is unknown, so every branch is skipped and the ELSE runs.

    COALESCE, IFNULL, NVL and IF all desugar onto CASE, so the fact worth
    stating is where each differs: COALESCE walks to its first non-NULL over any
    arity, and NULLIF *introduces* a NULL."""
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)")
    projected(client, "INSERT INTO t VALUES (1, 20, NULL), (2, 1, 2), (3, 2, NULL), "
                      "(4, -3, -3), (5, NULL, NULL), (6, 0, 0)", _BRANCHES, key="pk")
