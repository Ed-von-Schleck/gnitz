"""The Int and Float register classes: arithmetic and the numeric functions.

One opcode family under one NULL rule. Two halves of that rule are worth keeping
apart, because they look alike and are not:

- **A zero divisor is SQL NULL.** `MOD(x, 0)` and `x / 0` null the row's value
  rather than faulting, and the kernel is `wrapping_rem`, so `i64::MIN % -1` is
  a value and not a panic.
- **An IEEE domain error is the IEEE value, never NULL.** `SQRT(-1)` is NaN and
  `LN(0)` is `-inf`; these are results, and they have to survive a round trip
  through a column and cancel against their own retraction.

Every function here is a pure per-row transform — a linear map, in DBSP terms —
so the property that matters beyond the value is that a retraction re-running the
same expression over the same payload cancels *byte*-exactly. A computed column
that differed by one bit between an insert and its retraction would leave both
rows in the trace forever. That is a weight fact: the row set is unchanged and
only the weights show it, which is why nothing here counts rows.

The per-value truth tables belong to `gnitz-expr`'s kernel tests, which drive
each opcode over every cell class and both nullability arms directly.
"""

import math

import pytest
import gnitz
from _read import bag, scanned

_INT_COLS = ("id", "ai", "mi", "mz", "gi", "li", "si", "su")


@pytest.fixture
def nums(client, schema_name):
    """`t (id, i, f, u)` under one view carrying every numeric function at once,
    seeded with a negative, a positive, a zero and an all-NULL row — plus
    `u64::MAX`, whose top bit is set and which a signed read calls negative."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT, f DOUBLE, "
        "u BIGINT UNSIGNED)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, "
        "ABS(i) AS ai, MOD(i, 3) AS mi, MOD(i, 0) AS mz, "
        "GREATEST(i, 0) AS gi, LEAST(i, 0) AS li, "
        "SIGN(i) AS si, SIGN(u) AS su, "
        "ABS(f) AS af, FLOOR(f) AS ff, CEIL(f) AS cf, CEILING(f) AS cgf, "
        "ROUND(f) AS rf, TRUNC(f) AS tf, SIGN(f) AS sf, "
        "SQRT(f) AS sq, LN(f) AS ln, LOG(f) AS lg, EXP(i) AS ex, "
        "POWER(i, 2) AS p, POW(f, 0.5) AS ph FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, -7, -2.5, 5), (2, 7, 2.5, 18446744073709551615), "
        "(3, 9, 100.0, 0), (4, 0, 0.0, 7), (5, NULL, NULL, NULL)", schema_name=sn)
    return sn


def test_every_function_answers_over_each_sign_and_over_null(client, nums):
    """The integer-valued half as one weighted bag — every row present exactly
    once, so an accumulated retraction shows up here rather than hiding behind a
    row count."""
    assert bag(scanned(client, nums, "v"), *_INT_COLS) == {
        # ROUND/TRUNC differ from FLOOR on a negative; MOD keeps the dividend's
        # sign; a zero divisor nulls; GREATEST/LEAST fold against the literal.
        (1, 7, -1, None, 0, -7, -1, 1): 1,
        (2, 7, 1, None, 7, 0, 1, 1): 1,      # u64::MAX is positive, not -1
        (3, 9, 0, None, 9, 0, 1, 0): 1,   # SIGN of an unsigned zero is zero
        (4, 0, 0, None, 0, 0, 0, 1): 1,
        # NULL in, NULL out — except GREATEST/LEAST, which skip a NULL argument
        # rather than propagating it, and are NULL only when every argument is.
        (5, None, None, None, 0, 0, None, None): 1,
    }


def test_the_float_half_is_the_ieee_result_including_its_domain_errors(client, nums):
    """A domain error is a value: `SQRT(-1)` is NaN and `LN(0)` is `-inf`, both
    stored and read back as themselves. Only a NULL operand yields NULL."""
    by_id = {r.id: r for r in scanned(client, nums, "v")}

    assert by_id[1].af == pytest.approx(2.5)
    assert by_id[1].ff == pytest.approx(-3.0)          # toward -inf
    assert by_id[1].cf == by_id[1].cgf == pytest.approx(-2.0)
    assert by_id[1].tf == pytest.approx(-2.0)          # toward zero, unlike FLOOR
    # round_ties_even: -2.5 -> -2 and 2.5 -> 2, not away from zero.
    assert by_id[1].rf == pytest.approx(-2.0) and by_id[2].rf == pytest.approx(2.0)
    assert (by_id[1].sf, by_id[3].sf, by_id[4].sf) == (-1.0, 1.0, 0.0)

    assert by_id[3].sq == pytest.approx(10.0)
    assert by_id[3].ln == pytest.approx(math.log(100.0))
    assert by_id[3].lg == pytest.approx(2.0)
    assert by_id[3].p == pytest.approx(81.0)
    assert by_id[3].ph == pytest.approx(10.0)
    assert by_id[4].ex == pytest.approx(1.0)

    assert math.isnan(by_id[1].sq), "SQRT of a negative is NaN, not NULL"
    assert math.isnan(by_id[1].ln)
    assert by_id[4].ln == -math.inf and by_id[4].lg == -math.inf
    for c in ("af", "ff", "cf", "rf", "tf", "sf", "sq", "ln", "lg", "ex", "p", "ph"):
        assert getattr(by_id[5], c) is None, c


def test_a_retraction_re_derives_every_value_and_cancels_it(client, nums):
    """An UPDATE is a retract plus an insert, so every computed column is
    recomputed — including the ones derived from columns the UPDATE did not
    touch, and including a NaN, which must cancel against its own re-derivation
    bit for bit. The view accumulates unless the derivation is bit-stable, and
    that shows only in the weights."""
    sn = nums
    before = bag(scanned(client, sn, "v"), *_INT_COLS)

    # Touch `i` on the negative row: `ai`, `mi`, `gi`, `li`, `si`, `ex` and `p`
    # move, while every float-derived column must re-derive unchanged and cancel.
    client.execute_sql("UPDATE t SET i = 42 WHERE id = 1", schema_name=sn)
    after = bag(scanned(client, sn, "v"), *_INT_COLS)
    assert len(after) == len(before), "the pre-update row must have cancelled"
    assert after[(1, 42, 0, None, 42, 0, 1, 1)] == 1
    by_id = {r.id: r for r in scanned(client, sn, "v")}
    assert math.isnan(by_id[1].sq), "the untouched NaN survives the round trip"
    assert by_id[1].rf == pytest.approx(-2.0)

    client.execute_sql("DELETE FROM t", schema_name=sn)
    assert bag(scanned(client, sn, "v"), *_INT_COLS) == {}


def test_an_integer_argument_folds_the_transform_away(client, schema_name):
    """ROUND/FLOOR/CEIL/TRUNC of an integer are the identity and fold entirely.
    The fold is for correctness, not economy: lifting to f64 would mangle any
    magnitude past 2^53, and rounding an integer to more decimal places changes
    nothing. The declared type is what shows the fold happened."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, ROUND(i, 2) AS r2, ROUND(i) AS r, "
        "FLOOR(i) AS fl, TRUNC(i) AS tr, ABS(i) AS a FROM t", schema_name=sn)
    big = 9007199254740993  # 2^53 + 1: not representable in f64
    client.execute_sql(f"INSERT INTO t VALUES (1, {big})", schema_name=sn)

    types = {c.name: c.type_code for c in client.resolve_table(sn, "v")[1].columns}
    assert types["r2"] == gnitz.TypeCode.I64, "a folded ROUND stays an integer"
    assert bag(scanned(client, sn, "v"), "id", "r2", "r", "fl", "tr", "a") == {
        (1, big, big, big, big, big): 1}


def test_rounding_a_float_goes_half_to_even_at_every_scale(client, schema_name):
    """One tie rule, applied to the value *after* scaling, so the two-argument
    form is not a second rule: `2.665 * 100` is an exact tie and rounds down to
    even, `2.675 * 100` rounds up to exactly 267.5 first and then ties to even,
    and `n = 0` is the one-argument form."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, ROUND(f, 2) AS r2, ROUND(f, -5) AS rm5, "
        "ROUND(f, 0) AS r0, ROUND(f) AS r FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 2.665), (2, 2.675), (3, 881469.0444980001), "
        "(4, 3.5), (5, 0.5)", schema_name=sn)
    by_id = {r.id: r for r in scanned(client, sn, "v")}

    assert by_id[1].r2 == pytest.approx(2.66) and by_id[2].r2 == pytest.approx(2.68)
    assert by_id[3].rm5 == pytest.approx(900000.0)
    assert by_id[3].r0 == by_id[3].r == pytest.approx(881469.0)
    # The tie rule is the same one at scale 0: 3.5 -> 4 but 0.5 -> 0.
    assert by_id[4].r == pytest.approx(4.0) and by_id[5].r == pytest.approx(0.0)


def test_a_mixed_sign_extremum_fixes_its_compare_domain_before_folding(client, schema_name):
    """The unsigned verdict only exists from the first U64 operand onward, so a
    fold in written order would compare the leading signed pairs signed and
    answer differently for the two spellings. One U64 argument is rotated to the
    head, which fixes the domain for the whole fold: `-1` is `u64::MAX` there,
    and it is the greatest."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u BIGINT UNSIGNED NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, GREATEST(-1, 1, u) AS a, GREATEST(u, -1, 1) AS b "
        "FROM t", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "a", "b") == {(1, 2**64 - 1, 2**64 - 1): 1}


@pytest.mark.parametrize("expr,why", [
    # A scale that no shift can represent, one that is not an integer, and one
    # that is not a literal at all — all decided once, while planning.
    ("ROUND(f, 16)", "scale must be an integer literal"),
    ("ROUND(f, -16)", "scale must be an integer literal"),
    ("ROUND(f, 2.5)", "scale must be an integer literal"),
    ("ROUND(f, id)", "scale must be an integer literal"),
    # MOD is the integer selector; there is no float arm to fall back to.
    ("MOD(f, 2)", "modulo"),
    # Every argument is read through the numeric operand helper, which names the
    # column and the surfaces a string value does have.
    ("GREATEST(s, s)", 'column "s" is a string'),
    ("SQRT(s)", "string"),
    ("POWER(i)", "exactly two arguments"),
    ("LOG(i, 2)", "exactly one argument"),
])
def test_an_argument_outside_a_functions_signature_is_refused_at_create_time(
        client, schema_name, expr, why):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "f DOUBLE NOT NULL, s TEXT NOT NULL)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=why):
        client.execute_sql(f"CREATE VIEW bad AS SELECT id, {expr} AS x FROM t",
                           schema_name=sn)


def test_the_boundary_scales_are_accepted(client, schema_name):
    """The control for the rejection table above: the scales just inside the
    representable range compile, so those rejections are the bound and not a
    blanket refusal of a second argument."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, ROUND(f, 15) AS a, ROUND(f, -15) AS b FROM t",
        schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 1.5)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1}
