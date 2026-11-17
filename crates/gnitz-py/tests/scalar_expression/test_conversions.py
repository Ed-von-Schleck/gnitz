"""Conversions between register classes, and the unsignedness they carry.

A scalar value lives in one of four register classes — Int, Dec, Float, Str —
and the class picks the opcode family. This file is the boundary between them:
the nine crossing opcodes (`IntCast`, `IntToFloat`, `FloatToInt`, `FloatToF32`,
`IntToStr`, `TemporalToStr`, `FloatToStr`, `StrToInt`, `StrToFloat`), plus the
per-register unsignedness verdict that decides `signed:` on every compare,
divide, extremum, cast and print downstream of it.

One rule binds all nine, and no per-class file can state it: **a conversion
that cannot represent its value is a per-row NULL — never an error, never a
dropped row.** The distinction is observable only in weights: a dropped row
changes the view's Z-set, a NULL does not.
"""

import struct

import gnitz
from _exprs import projected
from _read import bag, rows, scanned

TC = gnitz.TypeCode
I64, U64, F64, STR = (TC.I64, 0), (TC.U64, 0), (TC.F64, 0), (TC.STRING, 0)
I8, I16, I32 = (TC.I8, 0), (TC.I16, 0), (TC.I32, 0)
U64_HIGH, U64_MAX = 2**63 + 5, 2**64 - 1  # past the signed boundary; the top
I64_MIN, I64_MAX = -(2**63), 2**63 - 1
F32_2_7 = struct.unpack("f", struct.pack("f", 2.7))[0]

# `(i, u, f, g, w, s)` by id.
_SEED = ("INSERT INTO t VALUES "
         f"(1, 100, 5, 2.7, 2.7, 3000000000, '42'), "
         f"(2, 300, {U64_HIGH}, 1e300, -0.25, 0, ' -7 '), "
         f"(3, -1, 0, 1e-300, 0.0, 1, '1.5'), "
         f"(4, {I64_MIN}, {U64_MAX}, 1.5, 1.5, 4, '42abc'), "
         f"(5, {I64_MAX}, 3, 0.0, 2.0, 7, '3000000000')")

# Each conversion against its declared type and its value on ids 1..5.
_CONVERSIONS = {
    # A narrowing cast is declared at its target, and a value outside it is NULL.
    "CAST(i AS SMALLINT)": (I16, [100, 300, -1, None, None]),
    "CAST(i AS TINYINT)": (I8, [100, None, -1, None, None]),
    # A literal that cannot convert is not a CREATE-time error either.
    "CAST(300 AS TINYINT)": (I8, [None] * 5),
    # The unsigned image is the one that is not merely wider: U64 is a distinct
    # domain.
    "CAST(i AS BIGINT UNSIGNED)": (U64, [100, 300, None, None, I64_MAX]),
    "CAST(u AS BIGINT)": (I64, [5, None, 0, None, 3]),
    # float -> int truncates toward zero.
    "CAST(f AS INT)": (I32, [2, None, 0, 1, 0]),
    # A value really goes through f32 precision; a finite overflow of f32's
    # range is NULL, and underflow flushes to zero rather than being a domain
    # error.
    "CAST(f AS FLOAT)": (F64, [F32_2_7, None, 0.0, 1.5, 0.0]),
    "CAST(i AS DOUBLE)": (F64, [100.0, 300.0, -1.0, float(I64_MIN), float(I64_MAX)]),
    "-g": (F64, [-F32_2_7, 0.25, -0.0, -1.5, -2.0]),
    # A U32 lands on I64 because every U32 value fits there, and a U32 sum
    # cannot reach 2^63.
    "-w": (I64, [-3000000000, 0, -1, -4, -7]),
    "w + w": (I64, [6000000000, 0, 2, 8, 14]),
    # A parse that does not consume the whole trimmed input is a NULL: a
    # fraction, trailing garbage, and a value that fits BIGINT but not INT.
    "CAST(s AS BIGINT)": (I64, [42, -7, None, None, 3000000000]),
    "CAST(s AS INT)": (I32, [42, -7, None, None, None]),
    "CAST(s AS DOUBLE)": (F64, [42.0, -7.0, 1.5, None, 3e9]),
    "CAST(i AS TEXT)": (STR, ["100", "300", "-1", str(I64_MIN), str(I64_MAX)]),
    # Outside [1e-4, 1e15) the float form goes scientific, which is what keeps
    # the printed length bounded.
    "CAST(f AS TEXT)": (STR, ["2.7", "1e300", "1e-300", "1.5", "0"]),
    # A FLOAT column prints as its own f32, not as the f64 it widens to.
    "CAST(g AS TEXT)": (STR, ["2.7", "-0.25", "0", "1.5", "2"]),
    # The print side reads its operand's signedness from the register rather
    # than the column: a U64 above 2^63 must not print negative.
    "CAST(u AS TEXT)": (STR, ["5", str(U64_HIGH), "0", str(U64_MAX), "3"]),
    "CAST(CAST(i AS TEXT) AS BIGINT)": (I64, [100, 300, -1, I64_MIN, I64_MAX]),
    # A DECIMAL prints exactly its scale's digits, as a client prints the
    # column, never through a float that drops a trailing zero.
    "CAST(CAST(w AS DECIMAL(12, 2)) AS TEXT)": (
        STR, ["3000000000.00", "0.00", "1.00", "4.00", "7.00"]),
    "CAST(CAST(1.1 AS DECIMAL(10, 2)) AS TEXT)": (STR, ["1.10"] * 5),
    # A DATE or TIMESTAMP prints its calendar value, not its storage integer;
    # a TIMESTAMP's fraction is as long as it needs to be, none on a whole
    # second.
    "CAST(DATE '2024-01-01' AS TEXT)": (STR, ["2024-01-01"] * 5),
    "CAST(TIMESTAMP '2024-01-01 12:00:00.5' AS TEXT)": (
        STR, ["2024-01-01 12:00:00.5"] * 5),
    "CAST(TIMESTAMP '2024-01-01 12:00:00' AS TEXT)": (
        STR, ["2024-01-01 12:00:00"] * 5),
}


def test_a_conversion_lands_on_its_declared_type_or_a_null_never_a_dropped_row(
        client):
    """No fault-delivery mechanism exists at row granularity, so a failed
    conversion nulls the value and keeps the row at its weight — asserted as one
    bag over the whole row."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "u BIGINT UNSIGNED NOT NULL, f DOUBLE NOT NULL, g FLOAT NOT NULL, "
        "w INT UNSIGNED NOT NULL, s TEXT NOT NULL)")
    projected(client, _SEED, _CONVERSIONS)


def test_an_unsigned_value_keeps_its_domain_through_a_view_chain(client):
    """A value at or above 2^63 has a negative i64 bit pattern, so a downstream
    reader that lost the unsignedness answers `> 100` FALSE. The verdict rides
    the register through arithmetic, a CASE blend, an extremum fold and a SUM
    alike, and has to survive being written to one view's column and read back
    by the next.

    An elided `u32 -> BIGINT UNSIGNED` cast and a native one must agree: every
    unsigned value is above -1, so `> -1` admits every row of both."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, flag BIGINT NOT NULL, "
        "w INT UNSIGNED NOT NULL)")
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, a + b AS arith, "
        "CASE WHEN flag > 0 THEN a + b ELSE 0 END AS blend, GREATEST(a + b, 1) AS ext FROM t")
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT id, arith > 100 AS p1, blend > 100 AS p2, ext > 100 AS p3 FROM v")
    client.execute_sql("CREATE VIEW s AS SELECT g, SUM(a) AS total FROM t GROUP BY g")
    client.execute_sql("CREATE VIEW s2 AS SELECT g FROM s WHERE total > 100")
    client.execute_sql(
        "CREATE VIEW reseeded AS SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > -1")
    client.execute_sql(
        "CREATE VIEW native AS SELECT id FROM t WHERE CAST(a AS BIGINT UNSIGNED) > -1")
    half = 2**62
    client.execute_sql(
        f"INSERT INTO t VALUES (1, 1, {half}, {half}, 1, 7), (2, 1, {half}, {half}, 0, 7), "
        "(3, 2, 1, 1, 1, 7)")

    assert bag(scanned(client, "v2")) == {
        (1, True, True, True): 1,
        (2, True, False, True): 1,
        (3, False, False, False): 1,
    }
    assert bag(scanned(client, "s2")) == {(1,): 1}
    every = {(1,): 1, (2,): 1, (3,): 1}
    assert bag(scanned(client, "reseeded")) == every
    assert bag(scanned(client, "native")) == every
    assert bag(rows(client, "SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > 0")) == every
