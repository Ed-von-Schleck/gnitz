"""One program, every surface that compiles one.

A scalar expression is lowered once and run by one evaluator, so a view
projection, a view WHERE, an ad-hoc SELECT, an ad-hoc WHERE, a GROUP BY key, a
DML residual and a SET right-hand side cannot disagree about what an expression
means. That is the fact this file exists for; what an individual operator
*computes* belongs to the per-class files beside it and, below them, to the
evaluator's own kernel tests.

The surfaces are not one code path reached seven ways: a view filter is compiled
into a maintained circuit and evaluated per worker partition, an ad-hoc read
compiles a residual against a scan, and DML compiles a program the client ships
as a blob. Agreement across them is a property only a running cluster answers.
"""

import pytest
import gnitz
from _read import bag, rows, scanned


@pytest.fixture
def surf(client, schema_name):
    """`t (pk, i, s, f, u)` holding one row per interesting sign and width, with
    every column NOT NULL so a NULL anywhere below is the expression's own."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "s TEXT NOT NULL, f DOUBLE NOT NULL, u BIGINT UNSIGNED NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, -9, 'alpha', 0.5, 5), (2, 3, 'beta', 1.5, 7), "
        "(3, -1, 'alpha', 2.5, 9)", schema_name=schema_name)
    return schema_name


def test_every_read_surface_answers_one_expression_the_same_way(client, surf):
    """`ABS(i)` as a value and `ABS(i) > 5` as a predicate, through the maintained
    projection, the maintained filter, the ad-hoc projection, the ad-hoc filter
    and a group key. The maintained answers are weights over a circuit's output
    store; the ad-hoc ones are weights over a residual scan."""
    sn = surf
    client.execute_sql("CREATE VIEW vp AS SELECT pk, ABS(i) AS a FROM t", schema_name=sn)
    client.execute_sql("CREATE VIEW vw AS SELECT pk FROM t WHERE ABS(i) > 5",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vg AS SELECT ABS(i) AS k, COUNT(*) AS c FROM t GROUP BY ABS(i)",
        schema_name=sn)

    values = {(1, 9): 1, (2, 3): 1, (3, 1): 1}
    assert bag(scanned(client, sn, "vp"), "pk", "a") == values
    assert bag(rows(client, sn, "SELECT pk, ABS(i) AS a FROM t")) == values

    assert bag(scanned(client, sn, "vw"), "pk") == {(1,): 1}
    assert bag(rows(client, sn, "SELECT pk FROM t WHERE ABS(i) > 5")) == {(1,): 1}

    # A written group key groups by the expression's value, not the column's.
    assert bag(scanned(client, sn, "vg"), "k", "c") == {(9, 1): 1, (3, 1): 1, (1, 1): 1}


def test_a_group_key_moves_its_row_when_the_key_expression_re_evaluates(client, surf):
    """A retraction under a computed group key has to leave the old group as well
    as join the new one; a stale group survives at weight 1 and a row count over
    the groups cannot see it."""
    sn = surf
    client.execute_sql(
        "CREATE VIEW v AS SELECT LEFT(s, 1) AS k, COUNT(*) AS c FROM t GROUP BY LEFT(s, 1)",
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "k", "c") == {("a", 2): 1, ("b", 1): 1}

    client.execute_sql("UPDATE t SET s = 'gamma' WHERE pk = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "k", "c") == {
        ("a", 1): 1, ("b", 1): 1, ("g", 1): 1}
    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "k", "c") == {("a", 1): 1, ("g", 1): 1}


@pytest.mark.parametrize("residual,survivors", [
    # A string conjunct, a float comparison and an unsigned ordered compare: an
    # integer-only client-side interpreter could serve none of the three.
    ("s = 'alpha' AND pk = 1", {2, 3}),
    ("f > 1.0", {1}),
    ("u > 100", {1, 2, 3}),
    ("UPPER(s) = 'BETA'", {1, 3}),
    ("ABS(i) > 5", {2, 3}),
    # The binder const-folds a null test on a NOT NULL column to a true literal,
    # which the compiler reports as "no filter" — every row must still go.
    ("i IS NOT NULL", set()),
])
def test_a_delete_residual_serves_every_operand_class(client, surf, residual, survivors):
    """The residual is the same program a view WHERE compiles to, so it serves
    the same operand classes and the same NULL rule."""
    sn = surf
    client.execute_sql(f"DELETE FROM t WHERE {residual}", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "pk") == {(p,): 1 for p in survivors}


def test_a_residual_and_a_view_filter_cannot_disagree(client, surf):
    """The same predicate on both surfaces, over data whose answer turns on the
    NULL a zero divisor produces: whatever the view keeps, the residual must
    delete, exactly."""
    sn = surf
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk FROM t WHERE 100 / (i + 1) > 1", schema_name=sn)
    kept = bag(scanned(client, sn, "v"), "pk")
    assert kept == {(2,): 1}, "only the non-zero divisor with a quotient past 1"

    client.execute_sql("DELETE FROM t WHERE 100 / (i + 1) > 1", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "pk") == {(1,): 1, (3,): 1}
    assert bag(scanned(client, sn, "v"), "pk") == {}, "the view retracts what was deleted"


def test_a_set_right_hand_side_computes_over_every_column_it_can_read(client, schema_name):
    """SET compiles the same program with the target column's class as its sink.
    A nullable source resolves it with nullability on, so a NULL source writes
    NULL rather than the filler zeros read back as a real 0; the PK region is
    readable through the client adapter like any other column; and an unsigned
    operand divides in the unsigned domain."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, "
        "s TEXT NOT NULL, u BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql(
        f"INSERT INTO t VALUES (41, 0, 5, '  pad  ', {2**63 + 5}), "
        "(7, 0, NULL, 'keep', 4)", schema_name=sn)

    client.execute_sql("UPDATE t SET a = b + 1", schema_name=sn)
    client.execute_sql("UPDATE t SET s = TRIM(s) WHERE pk = 41", schema_name=sn)
    client.execute_sql("UPDATE t SET u = u / 2 WHERE pk = 41", schema_name=sn)

    assert bag(scanned(client, sn, "t"), "pk", "a", "s", "u") == {
        (41, 6, "pad", (2**63 + 5) // 2): 1,
        (7, None, "keep", 4): 1,   # NULL + 1 stays NULL, not 0
    }

    # The PK region reads like a payload column.
    client.execute_sql("UPDATE t SET a = pk + 1", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "pk", "a") == {(41, 42): 1, (7, 8): 1}


@pytest.mark.parametrize("stmt,why", [
    # An f64 register has no integer destination and nothing downstream can tell
    # its bit pattern from an integer's, so the rule is the RHS's own class —
    # not the target's.
    ("UPDATE t SET a = f", "floating-point"),
    ("UPDATE t SET f = f + 1.0", "floating-point"),
    ("UPDATE t SET a = UPPER(s)", "cannot assign"),
])
def test_a_set_right_hand_side_of_the_wrong_class_is_refused(client, schema_name, stmt, why):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "f DOUBLE NOT NULL, s TEXT NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 0, 1.5, 'abc')", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=why):
        client.execute_sql(stmt, schema_name=sn)


# ---------------------------------------------------------------------------
# A literal with no register slot, in the lazily-interpreted mutate positions
# ---------------------------------------------------------------------------

_U64_MAX = 18446744073709551615


@pytest.fixture
def wide(client, schema_name):
    """`t (pk, v)` both `BIGINT UNSIGNED`, holding one row — so a statement that
    should reject has a row it would otherwise have touched."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
        "v BIGINT UNSIGNED NOT NULL)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=schema_name)
    return schema_name


def test_a_wide_literal_the_register_file_cannot_hold_is_refused_not_ignored(
        client, wide):
    """An integer past `i64` has no register slot, so a residual naming one
    cannot be compiled at all. The failure to avoid is silence: a predicate that
    quietly matched nothing would report a successful zero-row DELETE, which is
    indistinguishable from a row that legitimately was not there.

    The row must survive, so the refusal is not a partial apply either.
    """
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(f"DELETE FROM t WHERE v = {_U64_MAX}", schema_name=wide)
    assert bag(scanned(client, wide, "t"), "pk", "v") == {(1, 100): 1}


def test_a_set_right_hand_side_reaches_the_target_columns_whole_domain(client, wide):
    """SET parses its literal against the *target's* type rather than against the
    register file, so the full unsigned range is writable — the same seam the key
    codec uses, which is what makes the write and the seek accept one set of
    values."""
    client.execute_sql(f"UPDATE t SET v = {_U64_MAX}", schema_name=wide)
    assert bag(scanned(client, wide, "t"), "pk", "v") == {(1, _U64_MAX): 1}


@pytest.mark.parametrize("stmt", [
    # The WHERE matches nothing and the ON CONFLICT target does not conflict, so
    # in both the guard is the only thing that can fire.
    f"UPDATE t SET v = {_U64_MAX + 1} WHERE pk = 999",
    "UPDATE t SET v = -1 WHERE pk = 999",
    f"INSERT INTO t VALUES (2, 1) ON CONFLICT (pk) DO UPDATE SET v = {_U64_MAX + 1}",
], ids=["update-over", "update-under", "on-conflict-over"])
def test_an_out_of_range_assignment_is_refused_even_when_it_would_touch_no_row(
        client, wide, stmt):
    """The value is checked at plan time, not when a row reaches it. Deferring it
    would make the statement succeed silently whenever the match set is empty,
    and wrap two's-complement whenever it is not — so an empty match is exactly
    the case that has to reject."""
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(stmt, schema_name=wide)
    # Nothing was written: no wrapped value, and no row inserted by the upsert.
    assert bag(scanned(client, wide, "t"), "pk", "v") == {(1, 100): 1}


def test_the_guard_admits_the_in_range_wide_literal_on_the_same_surface(client, wide):
    """The rejection above is about the range, not about the width: the same
    upsert with a literal at the top of the target's domain is served, and its
    DO UPDATE applies when the key does conflict."""
    client.execute_sql(
        f"INSERT INTO t VALUES (1, 1) ON CONFLICT (pk) DO UPDATE SET v = {_U64_MAX}",
        schema_name=wide)
    assert bag(scanned(client, wide, "t"), "pk", "v") == {(1, _U64_MAX): 1}
