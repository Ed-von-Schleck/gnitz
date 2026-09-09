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

The per-value truth tables belong to `gnitz-expr`'s own kernel tests, which
drive every cell class and both nullability arms of each opcode directly. What
is asserted here is what only a running engine answers: the declared type a
computed column lands on, and that a conversion survives a maintained view, a
view chain and a retraction.
"""

import pytest
import gnitz
from _read import bag, rows, scanned

U64_HIGH = 2**63 + 5  # past the signed boundary: a negative i64 bit pattern


def _types(client, sn, name):
    """The declared type code of each column of view `name`."""
    return {c.name: c.type_code for c in client.resolve_table(sn, name)[1].columns}


def test_a_computed_column_is_declared_at_its_targets_register_image(client, schema_name):
    """EMIT stores a whole 8-byte register into an 8-byte slot, so the column a
    conversion lands in is the register's width, not the target's. A narrow
    target still carries the range-checked value, and declaring the column at the
    source width instead would truncate the store — an F32 collapsing to a
    denormal, a `-u32col` wrapping.

    The unsigned image is the one that is not merely wider: U64 is a distinct
    domain, and a U32 source lands on I64 because every U32 value fits there.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "f DOUBLE NOT NULL, g FLOAT NOT NULL, w INT UNSIGNED NOT NULL, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, CAST(i AS SMALLINT) AS narrow, "
        "CAST(i AS BIGINT UNSIGNED) AS u, CAST(f AS FLOAT) AS f32, "
        "CAST(i AS DOUBLE) AS d, CAST(i AS TEXT) AS txt, "
        "-g AS neg_f32, -w AS neg_u32, w + w AS narrow_sum, LENGTH(s) AS n FROM t",
        schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 300, 0.1, 1.5, 3000000000, 'abc')",
                       schema_name=sn)

    types = _types(client, sn, "v")
    assert types["narrow"] == gnitz.TypeCode.I64
    assert types["u"] == gnitz.TypeCode.U64
    assert types["f32"] == gnitz.TypeCode.F64
    assert types["d"] == gnitz.TypeCode.F64
    assert types["txt"] == gnitz.TypeCode.STRING
    assert types["neg_f32"] == gnitz.TypeCode.F64
    assert types["neg_u32"] == gnitz.TypeCode.I64
    # A U32 sum cannot reach 2^63, so it stays in the signed domain.
    assert types["narrow_sum"] == gnitz.TypeCode.I64
    assert types["n"] == gnitz.TypeCode.I64

    assert bag(scanned(client, sn, "v"), "id", "narrow", "u", "neg_u32", "narrow_sum") == {
        (1, 300, 300, -3000000000, 6000000000): 1}
    row = next(iter(scanned(client, sn, "v")))
    assert row.neg_f32 == pytest.approx(-1.5)

    # The ad-hoc binder types the same expressions through its own path.
    adhoc = rows(client, sn, "SELECT -g AS neg_f32, -w AS neg_u32 FROM t")[0]
    assert adhoc.neg_f32 == pytest.approx(-1.5) and adhoc.neg_u32 == -3000000000


def test_a_value_outside_the_targets_domain_is_a_null_not_a_dropped_row(client, schema_name):
    """No fault-delivery mechanism exists at row granularity, so a failed
    conversion nulls the value and keeps the row at its weight. A literal that
    cannot convert is not a CREATE-time error either — it is the same per-row
    NULL, on every row.

    Asserted on weights: dropping the row is the failure this rules out, and a
    row count over three ids reads a dropped row and a nulled one alike only
    until one of them is missing.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "u BIGINT UNSIGNED NOT NULL, f DOUBLE NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, CAST(i AS TINYINT) AS b, CAST(300 AS TINYINT) AS lit, "
        "CAST(u AS BIGINT) AS signed, CAST(f AS INT) AS fi, CAST(f AS FLOAT) AS f32 FROM t",
        schema_name=sn)
    client.execute_sql(
        f"INSERT INTO t VALUES (1, 100, 5, 2.7), (2, 300, {U64_HIGH}, 1e300), "
        "(3, -1, 0, 1e-300)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "b", "lit", "signed", "fi") == {
        (1, 100, None, 5, 2): 1,
        # 300 has no TINYINT image; U64_HIGH has no BIGINT one; 1e300 no INT one.
        (2, None, None, None, None): 1,
        # A negative fits TINYINT, and float→int truncates toward zero.
        (3, -1, None, 0, 0): 1,
    }
    by_id = {r.id: r for r in scanned(client, sn, "v")}
    assert by_id[1].f32 == pytest.approx(2.7) and by_id[1].f32 != 2.7, \
        "the value really went through f32 precision"
    assert by_id[2].f32 is None, "a finite value overflowing f32's range"
    assert by_id[3].f32 == 0.0, "underflow flushes to zero; it is not a domain error"


def test_a_failed_conversion_in_a_predicate_excludes_its_row(client, schema_name):
    """The NULL a failed conversion produces is an ordinary SQL NULL, so a
    predicate over it is unknown and the row does not pass — on a view filter and
    on the DML residual that compiles the same program."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL, "
        "u BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT id FROM t WHERE CAST(f AS INT) = 2",
                       schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES (1, 2.7, 5), (2, 1e300, {U64_HIGH}), "
                       "(3, 5.0, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1}

    # The high row's cast is NULL, so its residual is unknown and it survives.
    client.execute_sql("DELETE FROM t WHERE CAST(u AS SMALLINT) = 5", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "id") == {(2,): 1, (3,): 1}


def test_text_and_number_convert_in_both_directions(client, schema_name):
    """A parse that does not consume the whole trimmed input is a NULL, and the
    print side reads its operand's signedness from the register rather than the
    column: a U64 above 2^63 must not print negative."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, "
        "i BIGINT NOT NULL, u BIGINT UNSIGNED NOT NULL, f DOUBLE NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, CAST(s AS BIGINT) AS si, CAST(s AS INT) AS s32, "
        "CAST(i AS TEXT) AS it, CAST(u AS TEXT) AS ut, CAST(f AS TEXT) AS ft, "
        "CAST(CAST(i AS TEXT) AS BIGINT) AS back FROM t", schema_name=sn)
    big_u = 2**64 - 1
    client.execute_sql(
        f"INSERT INTO t VALUES (1, '42', -9, {big_u}, 1.5), (2, ' -7 ', {-(2**63)}, 0, 1e300), "
        f"(3, '1.5', {2**63 - 1}, 1, 0.0), (4, '42abc', 0, 2, 0.0), "
        "(5, '3000000000', 1, 3, 0.0)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "si", "s32", "back") == {
        (1, 42, 42, -9): 1,
        (2, -7, -7, -(2**63)): 1,          # surrounding space is trimmed
        (3, None, None, 2**63 - 1): 1,     # a fraction is not an integer
        (4, None, None, 0): 1,             # trailing garbage is not consumed
        (5, 3000000000, None, 1): 1,       # fits BIGINT, not INT
    }
    by_id = {r.id: r for r in scanned(client, sn, "v")}
    assert by_id[1].it == "-9"
    assert by_id[1].ut == str(big_u), "a signed read of the register would print negative"
    assert by_id[1].ft == "1.5"
    # Outside [1e-4, 1e15) the float form goes scientific, which is what keeps
    # the printed length bounded.
    assert len(by_id[2].ft) < 12 and float(by_id[2].ft) == 1e300


@pytest.mark.parametrize("body,want", [
    ("SELECT id, a + b AS c FROM t", {(1,): 1, (2,): 1}),
    ("SELECT id, CASE WHEN flag > 0 THEN a + b ELSE 0 END AS c FROM t", {(1,): 1}),
    ("SELECT id, GREATEST(a + b, 1) AS c FROM t", {(1,): 1, (2,): 1}),
    ("SELECT g AS id, SUM(a) AS c FROM t GROUP BY g", {(1,): 1}),
])
def test_an_unsigned_value_keeps_its_domain_through_a_view_chain(
        client, schema_name, body, want):
    """A value at or above 2^63 has a negative i64 bit pattern, so a downstream
    reader that lost the unsignedness answers `c > 100` FALSE and drops the row.
    The verdict rides the register through arithmetic, a CASE blend, an extremum
    fold and a SUM alike, and has to survive being written to one view's column
    and read back by the next."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, flag BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)
    client.execute_sql("CREATE VIEW v2 AS SELECT id FROM v WHERE c > 100", schema_name=sn)
    half = 2**62
    client.execute_sql(
        f"INSERT INTO t VALUES (1, 1, {half}, {half}, 1), (2, 1, {half}, {half}, 0), "
        "(3, 2, 1, 1, 1)", schema_name=sn)

    assert bag(scanned(client, sn, "v2"), "id") == want


def test_casting_to_unsigned_reseeds_the_compare_domain(client, schema_name):
    """Eliding a `u32 -> BIGINT UNSIGNED` cast because the value already fits
    would leave the register signed while the column reads U64, and `> -1` would
    then answer TRUE for the elided source and FALSE for the native one. Both
    must read -1 as 2^64-1 and admit nothing."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
        "w INT UNSIGNED NOT NULL, u BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > -1",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT id FROM t WHERE CAST(u AS BIGINT UNSIGNED) > -1",
        schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 7, 7)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id") == {}
    assert bag(scanned(client, sn, "v2"), "id") == {}
    # Control: the same predicate against 0 does match, so the empty results
    # above are the compare domain and not a broken view.
    assert bag(rows(client, sn, "SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > 0")) \
        == {(1,): 1}


@pytest.mark.parametrize("expr,why", [
    ("CAST(i AS UUID)", "not supported"),
    ("CAST(i AS BOOLEAN)", "BOOLEAN"),
    # A wide integer literal has no register slot at all — the general limit of
    # the 8-byte register file, not something CAST adds.
    ("CAST(18446744073709551615 AS BIGINT UNSIGNED)", "18446744073709551615"),
])
def test_an_unrepresentable_target_or_source_is_refused_at_create_time(
        client, schema_name, expr, why):
    """Unlike a per-row domain failure, a target the register file cannot hold at
    all is decided once, while planning."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
        schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=why):
        client.execute_sql(f"CREATE VIEW bad AS SELECT id, {expr} AS c FROM t",
                           schema_name=sn)
