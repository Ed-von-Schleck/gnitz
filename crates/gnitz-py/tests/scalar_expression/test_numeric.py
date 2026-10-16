"""The Int and Float register classes: arithmetic and the numeric functions.

One opcode family under one NULL rule. Two halves of that rule are worth keeping
apart, because they look alike and are not:

- **A zero divisor is SQL NULL.** `MOD(x, 0)` and `x / 0` null the row's value
  rather than faulting.
- **An IEEE domain error is the IEEE value, never NULL.** `SQRT(-1)` is NaN and
  `LN(0)` is `-inf`; these are results, and they have to survive a round trip
  through a column and cancel against their own retraction.

Every function here is a pure per-row transform — a linear map, in DBSP terms —
so the property that matters beyond the value is that a retraction re-running the
same expression over the same payload cancels *byte*-exactly. A computed column
that differed by one bit between an insert and its retraction would leave both
rows in the trace forever. That is a weight fact: the row set is unchanged and
only a bag over the whole row shows it.
"""

import math

import gnitz
from _exprs import projected

I64, U64, F64 = (gnitz.TypeCode.I64, 0), (gnitz.TypeCode.U64, 0), (gnitz.TypeCode.F64, 0)
B, M, F8 = 2**53 + 1, 2**64 - 1, 881469.0444980001
nan, inf = math.nan, math.inf

# `(i, f, u)` by id: each sign, a zero, an all-NULL row, `u64::MAX` (whose top
# bit a signed read calls negative), an integer past f64's exact range, and the
# rounding ties.
_SEED = ("INSERT INTO t VALUES "
         f"(1, -7, -2.5, 5), (2, 7, 2.5, {M}), (3, 9, 100.0, 0), (4, 0, 0.0, 7), "
         f"(5, NULL, NULL, NULL), (6, {B}, 2.665, 1), (7, -1, 2.675, 2), (8, 3, {F8}, 3)")

# Each expression against its type and its value on ids 1..8.
_FUNCTIONS = {
    "ABS(i)": (I64, [7, 7, 9, 0, None, B, 1, 3]),
    # MOD keeps the dividend's sign, and a zero divisor nulls.
    "MOD(i, 3)": (I64, [-1, 1, 0, 0, None, 0, -1, 0]),
    "MOD(i, 0)": (I64, [None] * 8),
    # GREATEST/LEAST skip a NULL argument rather than propagating it.
    "GREATEST(i, 0)": (I64, [0, 7, 9, 0, 0, B, 0, 3]),
    "LEAST(i, 0)": (I64, [-7, 0, 0, 0, 0, 0, -1, 0]),
    "SIGN(i)": (I64, [-1, 1, 1, 0, None, 1, -1, 1]),
    # u64::MAX is positive, not -1; an unsigned zero is zero.
    "SIGN(u)": (I64, [1, 1, 0, 1, None, 1, 1, 1]),
    # An integer argument folds the transform away: lifting to f64 would mangle
    # 2^53 + 1, and rounding an integer to more places changes nothing.
    "ROUND(i, 2)": (I64, [-7, 7, 9, 0, None, B, -1, 3]),
    "FLOOR(i)": (I64, [-7, 7, 9, 0, None, B, -1, 3]),
    # A U64 argument makes the result unsigned, where `-1` has no value: it is
    # NULL in either spelling, and the fold skips it.
    "GREATEST(-1, 1, u)": (U64, [5, M, 1, 7, 1, 1, 2, 3]),
    "GREATEST(u, -1, 1)": (U64, [5, M, 1, 7, 1, 1, 2, 3]),
    "ABS(f)": (F64, [2.5, 2.5, 100.0, 0.0, None, 2.665, 2.675, F8]),
    "FLOOR(f)": (F64, [-3.0, 2.0, 100.0, 0.0, None, 2.0, 2.0, 881469.0]),
    "CEIL(f)": (F64, [-2.0, 3.0, 100.0, 0.0, None, 3.0, 3.0, 881470.0]),
    "TRUNC(f)": (F64, [-2.0, 2.0, 100.0, 0.0, None, 2.0, 2.0, 881469.0]),
    # One tie rule, half to even, applied to the value *after* scaling:
    # `2.665 * 100` is an exact tie and rounds down to even, `2.675 * 100` is
    # exactly 267.5 and rounds up to even.
    "ROUND(f)": (F64, [-2.0, 2.0, 100.0, 0.0, None, 3.0, 3.0, 881469.0]),
    "ROUND(f, 2)": (F64, [-2.5, 2.5, 100.0, 0.0, None, 2.66, 2.68, 881469.04]),
    "ROUND(f, -5)": (F64, [-0.0, 0.0, 0.0, 0.0, None, 0.0, 0.0, 900000.0]),
    "SIGN(f)": (F64, [-1.0, 1.0, 1.0, 0.0, None, 1.0, 1.0, 1.0]),
    "SQRT(f)": (F64, [nan, math.sqrt(2.5), 10.0, 0.0, None, math.sqrt(2.665), math.sqrt(2.675),
                      math.sqrt(F8)]),
    "LN(f)": (F64, [nan, math.log(2.5), math.log(100.0), -inf, None, math.log(2.665),
                    math.log(2.675), math.log(F8)]),
    "LOG(f)": (F64, [nan, math.log10(2.5), 2.0, -inf, None, math.log10(2.665), math.log10(2.675),
                     math.log10(F8)]),
    "EXP(i)": (F64, [math.exp(-7), math.exp(7), math.exp(9), 1.0, None, inf, math.exp(-1),
                     math.exp(3)]),
    "POWER(i, 2)": (F64, [49.0, 49.0, 81.0, 0.0, None, 2.0**106, 1.0, 9.0]),
    "POW(f, 0.5)": (F64, [nan, 2.5**0.5, 10.0, 0.0, None, 2.665**0.5, 2.675**0.5, F8**0.5]),
}


def test_every_function_answers_over_each_sign_and_null_and_cancels_on_retraction(
        client):
    """Every function over each sign and over NULL, as one bag over the whole
    row. A NaN among the computed cells has to cancel against its own
    re-derivation bit for bit."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT, f DOUBLE, u BIGINT UNSIGNED)")
    projected(client, _SEED, _FUNCTIONS)
