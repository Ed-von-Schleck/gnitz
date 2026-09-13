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
only a bag over the whole row shows it.
"""

import math

import gnitz
from _read import bag, rows, scanned

B, M, F8 = 2**53 + 1, 2**64 - 1, 881469.0444980001
nan, inf = math.nan, math.inf

# `(i, f, u)` by id: each sign, a zero, an all-NULL row, `u64::MAX` (whose top
# bit a signed read calls negative), an integer past f64's exact range, and the
# rounding ties.
_IDS = [1, 2, 3, 4, 5, 6, 7, 8]
_ROWS = (f"(1, -7, -2.5, 5), (2, 7, 2.5, {M}), (3, 9, 100.0, 0), (4, 0, 0.0, 7), "
         f"(5, NULL, NULL, NULL), (6, {B}, 2.665, 1), (7, -1, 2.675, 2), (8, 3, {F8}, 3)")

# Each expression against its value on every row, in `_IDS` order.
_FUNCTIONS = {
    "ABS(i)": [7, 7, 9, 0, None, B, 1, 3],
    # MOD keeps the dividend's sign, and a zero divisor nulls.
    "MOD(i, 3)": [-1, 1, 0, 0, None, 0, -1, 0],
    "MOD(i, 0)": [None] * 8,
    # GREATEST/LEAST skip a NULL argument rather than propagating it.
    "GREATEST(i, 0)": [0, 7, 9, 0, 0, B, 0, 3],
    "LEAST(i, 0)": [-7, 0, 0, 0, 0, 0, -1, 0],
    "SIGN(i)": [-1, 1, 1, 0, None, 1, -1, 1],
    # u64::MAX is positive, not -1; an unsigned zero is zero.
    "SIGN(u)": [1, 1, 0, 1, None, 1, 1, 1],
    # An integer argument folds the transform away: lifting to f64 would mangle
    # 2^53 + 1, and rounding an integer to more places changes nothing.
    "ROUND(i, 2)": [-7, 7, 9, 0, None, B, -1, 3],
    "FLOOR(i)": [-7, 7, 9, 0, None, B, -1, 3],
    # One U64 argument is rotated to the head of the fold, which fixes the
    # unsigned domain for every pair: `-1` is `u64::MAX` there, in either
    # spelling, and it is the greatest.
    "GREATEST(-1, 1, u)": [M] * 8,
    "GREATEST(u, -1, 1)": [M] * 8,
    "ABS(f)": [2.5, 2.5, 100.0, 0.0, None, 2.665, 2.675, F8],
    "FLOOR(f)": [-3.0, 2.0, 100.0, 0.0, None, 2.0, 2.0, 881469.0],
    "CEIL(f)": [-2.0, 3.0, 100.0, 0.0, None, 3.0, 3.0, 881470.0],
    "TRUNC(f)": [-2.0, 2.0, 100.0, 0.0, None, 2.0, 2.0, 881469.0],
    # One tie rule, half to even, applied to the value *after* scaling:
    # `2.665 * 100` is an exact tie and rounds down to even, `2.675 * 100` is
    # exactly 267.5 and rounds up to even.
    "ROUND(f)": [-2.0, 2.0, 100.0, 0.0, None, 3.0, 3.0, 881469.0],
    "ROUND(f, 2)": [-2.5, 2.5, 100.0, 0.0, None, 2.66, 2.68, 881469.04],
    "ROUND(f, -5)": [-0.0, 0.0, 0.0, 0.0, None, 0.0, 0.0, 900000.0],
    "SIGN(f)": [-1.0, 1.0, 1.0, 0.0, None, 1.0, 1.0, 1.0],
    "SQRT(f)": [nan, math.sqrt(2.5), 10.0, 0.0, None, math.sqrt(2.665), math.sqrt(2.675),
                math.sqrt(F8)],
    "LN(f)": [nan, math.log(2.5), math.log(100.0), -inf, None, math.log(2.665),
              math.log(2.675), math.log(F8)],
    "LOG(f)": [nan, math.log10(2.5), 2.0, -inf, None, math.log10(2.665), math.log10(2.675),
               math.log10(F8)],
    "EXP(i)": [math.exp(-7), math.exp(7), math.exp(9), 1.0, None, inf, math.exp(-1),
               math.exp(3)],
    "POWER(i, 2)": [49.0, 49.0, 81.0, 0.0, None, 2.0**106, 1.0, 9.0],
    "POW(f, 0.5)": [nan, 2.5**0.5, 10.0, 0.0, None, 2.665**0.5, 2.675**0.5, F8**0.5],
}


def test_every_function_answers_over_each_sign_and_null_and_cancels_on_retraction(
        client, schema_name):
    """Every function over each sign and over NULL, as one bag over the whole
    row — a maintained view and an ad-hoc read of the same projection alike.

    An UPDATE is then a retract plus an insert, so every computed column is
    recomputed — including those derived from the untouched `f`, and including
    a NaN, which must cancel against its own re-derivation bit for bit. The
    ad-hoc read has no trace to accumulate in, so a view still equal to it has
    cancelled."""
    sn = schema_name
    select = "SELECT id, " + ", ".join(
        f"{e} AS c{n}" for n, e in enumerate(_FUNCTIONS)) + " FROM t"
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT, f DOUBLE, u BIGINT UNSIGNED)",
        schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS {select}", schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {_ROWS}", schema_name=sn)

    rounded = client.resolve_table(sn, "v")[1].columns[1 + list(_FUNCTIONS).index("ROUND(i, 2)")]
    assert rounded.type_code == gnitz.TypeCode.I64, "a folded ROUND stays an integer"

    expected = {r: 1 for r in zip(_IDS, *_FUNCTIONS.values())}
    assert bag(scanned(client, sn, "v")) == expected
    assert bag(rows(client, sn, select)) == expected

    client.execute_sql("UPDATE t SET i = 42 WHERE id = 1", schema_name=sn)
    after = bag(scanned(client, sn, "v"))
    assert after == bag(rows(client, sn, select))
    assert len(after) == len(expected), "the pre-update row must have cancelled"

    client.execute_sql("DELETE FROM t", schema_name=sn)
    assert bag(scanned(client, sn, "v")) == {}
