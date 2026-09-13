"""Conversions between register classes, and the unsignedness they carry.

A scalar value lives in one of four register classes — Int, Dec, Float, Str —
and the class picks the opcode family. This file is the boundary between them:
the eight crossing opcodes (`IntCast`, `IntToFloat`, `FloatToInt`, `FloatToF32`,
`IntToStr`, `FloatToStr`, `StrToInt`, `StrToFloat`), plus the per-register
unsignedness verdict that decides `signed:` on every compare, divide, extremum,
cast and print downstream of it.

One rule binds all eight, and no per-class file can state it: **a conversion
that cannot represent its value is a per-row NULL — never an error, never a
dropped row.** The distinction is observable only in weights: a dropped row
changes the view's Z-set, a NULL does not.
"""

import struct

import gnitz
from _read import bag, rows, scanned

TC = gnitz.TypeCode
U64_HIGH, U64_MAX = 2**63 + 5, 2**64 - 1  # past the signed boundary; the top
I64_MIN, I64_MAX = -(2**63), 2**63 - 1
F32_2_7 = struct.unpack("f", struct.pack("f", 2.7))[0]

# `(i, u, f, g, w, s)` by id.
_ROWS = (f"(1, 100, 5, 2.7, 1.5, 3000000000, '42'), "
         f"(2, 300, {U64_HIGH}, 1e300, -0.25, 0, ' -7 '), "
         f"(3, -1, 0, 1e-300, 0.0, 1, '1.5'), "
         f"(4, {I64_MIN}, {U64_MAX}, 1.5, 1.5, 4, '42abc'), "
         f"(5, {I64_MAX}, 3, 0.0, 2.0, 7, '3000000000')")

# Each conversion against its declared type and its value on ids 1..5.
_CONVERSIONS = {
    # EMIT stores a whole 8-byte register into an 8-byte slot, so the column a
    # conversion lands in is the register's width, not the target's: a narrow
    # target still carries the range-checked value, and declaring it at the
    # target width would truncate the store.
    "CAST(i AS SMALLINT)": (TC.I64, [100, 300, -1, None, None]),
    "CAST(i AS TINYINT)": (TC.I64, [100, None, -1, None, None]),
    # A literal that cannot convert is not a CREATE-time error either.
    "CAST(300 AS TINYINT)": (TC.I64, [None] * 5),
    # The unsigned image is the one that is not merely wider: U64 is a distinct
    # domain.
    "CAST(i AS BIGINT UNSIGNED)": (TC.U64, [100, 300, None, None, I64_MAX]),
    "CAST(u AS BIGINT)": (TC.I64, [5, None, 0, None, 3]),
    # float -> int truncates toward zero.
    "CAST(f AS INT)": (TC.I64, [2, None, 0, 1, 0]),
    # A value really goes through f32 precision; a finite overflow of f32's
    # range is NULL, and underflow flushes to zero rather than being a domain
    # error.
    "CAST(f AS FLOAT)": (TC.F64, [F32_2_7, None, 0.0, 1.5, 0.0]),
    "CAST(i AS DOUBLE)": (TC.F64, [100.0, 300.0, -1.0, float(I64_MIN), float(I64_MAX)]),
    "-g": (TC.F64, [-1.5, 0.25, -0.0, -1.5, -2.0]),
    # A U32 lands on I64 because every U32 value fits there, and a U32 sum
    # cannot reach 2^63.
    "-w": (TC.I64, [-3000000000, 0, -1, -4, -7]),
    "w + w": (TC.I64, [6000000000, 0, 2, 8, 14]),
    "LENGTH(s)": (TC.I64, [2, 4, 3, 5, 10]),
    # A parse that does not consume the whole trimmed input is a NULL: a
    # fraction, trailing garbage, and a value that fits BIGINT but not INT.
    "CAST(s AS BIGINT)": (TC.I64, [42, -7, None, None, 3000000000]),
    "CAST(s AS INT)": (TC.I64, [42, -7, None, None, None]),
    "CAST(i AS TEXT)": (TC.STRING, ["100", "300", "-1", str(I64_MIN), str(I64_MAX)]),
    # The print side reads its operand's signedness from the register rather
    # than the column: a U64 above 2^63 must not print negative.
    "CAST(u AS TEXT)": (TC.STRING, ["5", str(U64_HIGH), "0", str(U64_MAX), "3"]),
    "CAST(CAST(i AS TEXT) AS BIGINT)": (TC.I64, [100, 300, -1, I64_MIN, I64_MAX]),
}


def test_a_conversion_lands_on_its_register_image_or_a_null_never_a_dropped_row(
        client, schema_name):
    """No fault-delivery mechanism exists at row granularity, so a failed
    conversion nulls the value and keeps the row at its weight — asserted as one
    bag over the whole row, through a maintained view and the ad-hoc binder's
    own path alike. A predicate over such a NULL is unknown, so the row does
    not pass."""
    sn = schema_name
    select = "SELECT id, " + ", ".join(
        f"{e} AS c{n}" for n, e in enumerate(_CONVERSIONS)) + " FROM t"
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "u BIGINT UNSIGNED NOT NULL, f DOUBLE NOT NULL, g FLOAT NOT NULL, "
        "w INT UNSIGNED NOT NULL, s TEXT NOT NULL)", schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS {select}", schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {_ROWS}", schema_name=sn)

    declared = [c.type_code for c in client.resolve_table(sn, "v")[1].columns[1:]]
    assert declared == [tc for tc, _ in _CONVERSIONS.values()]
    expected = {r: 1 for r in zip([1, 2, 3, 4, 5], *(vs for _, vs in _CONVERSIONS.values()))}
    assert bag(scanned(client, sn, "v")) == expected
    assert bag(rows(client, sn, select)) == expected

    # Outside [1e-4, 1e15) the float form goes scientific, which is what keeps
    # the printed length bounded.
    ft = dict(rows(client, sn, "SELECT id, CAST(f AS TEXT) AS ft FROM t"))
    assert ft[4] == "1.5"
    assert len(ft[2]) < 12 and float(ft[2]) == 1e300

    assert bag(rows(client, sn, "SELECT id FROM t WHERE CAST(f AS INT) = 2")) == {(1,): 1}


def test_an_unsigned_value_keeps_its_domain_through_a_view_chain(client, schema_name):
    """A value at or above 2^63 has a negative i64 bit pattern, so a downstream
    reader that lost the unsignedness answers `> 100` FALSE. The verdict rides
    the register through arithmetic, a CASE blend, an extremum fold in either
    argument order and a SUM alike, and has to survive being written to one
    view's column and read back by the next.

    Eliding a `u32 -> BIGINT UNSIGNED` cast because the value already fits would
    leave the register signed while the column reads U64, and `> -1` would then
    answer TRUE for the elided source and FALSE for the native one. Both must
    read -1 as 2^64-1 and admit nothing."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, flag BIGINT NOT NULL, "
        "w INT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, a + b AS arith, "
        "CASE WHEN flag > 0 THEN a + b ELSE 0 END AS blend, GREATEST(a + b, 1) AS ext, "
        "GREATEST(-1, 1, a) AS late, GREATEST(a, -1, 1) AS early FROM t", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT id, arith > 100 AS p1, blend > 100 AS p2, ext > 100 AS p3, "
        "late > 100 AS p4, early > 100 AS p5 FROM v", schema_name=sn)
    client.execute_sql("CREATE VIEW s AS SELECT g, SUM(a) AS total FROM t GROUP BY g",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW s2 AS SELECT g FROM s WHERE total > 100", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW reseeded AS SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > -1",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW native AS SELECT id FROM t WHERE CAST(a AS BIGINT UNSIGNED) > -1",
        schema_name=sn)
    half = 2**62
    client.execute_sql(
        f"INSERT INTO t VALUES (1, 1, {half}, {half}, 1, 7), (2, 1, {half}, {half}, 0, 7), "
        "(3, 2, 1, 1, 1, 7)", schema_name=sn)

    assert bag(scanned(client, sn, "v2")) == {
        (1, 1, 1, 1, 1, 1): 1,
        (2, 1, 0, 1, 1, 1): 1,
        # `-1` is u64::MAX once one U64 argument fixes the fold's domain.
        (3, 0, 0, 0, 1, 1): 1,
    }
    assert bag(scanned(client, sn, "s2")) == {(1,): 1}
    assert bag(scanned(client, sn, "reseeded")) == {}
    assert bag(scanned(client, sn, "native")) == {}
    # Control: the same predicate against 0 does match, so the empty results
    # above are the compare domain and not a broken view.
    assert bag(rows(client, sn, "SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > 0")) \
        == {(1,): 1, (2,): 1, (3,): 1}
