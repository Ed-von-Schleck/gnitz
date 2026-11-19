use super::*;
use crate::bind::bind_constant;
use crate::test_support::parse_expr_sql;
use Placed::*;
use std::convert::Infallible;

fn lit(src: &str) -> BExpr<Infallible> {
    bind_constant(&parse_expr_sql(src)).unwrap_or_else(|e| panic!("{src}: {e}"))
}

const BELOW: Option<Placed> = Some(Below { nearest: None });
const ABOVE: Option<Placed> = Some(Above { nearest: None });
const U128_MAX: &str = "340282366920938463463374607431768211455";
const I128_MIN: &str = "-170141183460469231731687303715884105728";
/// 2020-01-02 as DATE storage.
const D: u128 = 18263;
const DAY: u128 = MICROS_PER_DAY as u128;

/// Every integer type holds both ends of its range and places the value one past
/// either end outside it, on that end's side, rather than wrapping it.
#[test]
fn an_integer_type_holds_its_range_and_places_one_past_it_outside() {
    for fi in FixedInt::ALL {
        let tc = fi.type_code();
        let (lo, hi) = fi.range();
        for (v, want) in [
            (lo - 1, BELOW),
            (lo, Some(At(fi.pack(lo)))),
            (hi, Some(At(fi.pack(hi)))),
            (hi + 1, ABOVE),
        ] {
            assert_eq!(place(&lit(&v.to_string()), tc.into()), want, "{tc:?} {v}");
        }
    }
}

#[test]
fn a_literal_is_placed_among_the_values_of_the_type() {
    use TypeCode::*;
    let d2 = ColType::decimal(2);
    let day_before = FixedInt::I32.pack(-1);
    let neg_huge = format!("-{U128_MAX}");
    for (ty, src, want) in [
        // Past `i128` a literal lies outside every type but a U128 or UUID.
        (I64.into(), U128_MAX, ABOVE),
        (I64.into(), &neg_huge, BELOW),
        (I64.into(), "1e3", Some(At(1000))),
        (I64.into(), "2.5", Some(Between { lo: 2, nearest: 3 })),
        (I64.into(), "DATE '2020-01-02'", Some(At(D))),
        // A 16-byte type takes whole numbers up to its edges, and a UUID its
        // string spelling too.
        (I128.into(), I128_MIN, Some(At(1 << 127))),
        (I128.into(), "-170141183460469231731687303715884105729", BELOW),
        (
            I128.into(),
            "170141183460469231731687303715884105727",
            Some(At(i128::MAX as u128)),
        ),
        (I128.into(), "170141183460469231731687303715884105728", ABOVE),
        (U128.into(), "-1", BELOW),
        (U128.into(), U128_MAX, Some(At(u128::MAX))),
        (U128.into(), "1e3", Some(At(1000))),
        (U128.into(), "1.5", None),
        (
            UUID.into(),
            "'550e8400-e29b-41d4-a716-446655440000'",
            Some(At(0x550e8400_e29b_41d4_a716_446655440000)),
        ),
        (UUID.into(), "42", Some(At(42))),
        (UUID.into(), "'not-a-uuid'", None),
        // A DECIMAL reads a literal, or its string spelling, at its scale.
        (d2, "1.5", Some(At(150))),
        (d2, "'1.50'", Some(At(150))),
        (d2, "1.005", Some(Between { lo: 100, nearest: 101 })),
        (d2, "'abc'", None),
        (d2, "NULL", None),
        // A DATE and a TIMESTAMP convert between units, flooring before 1970.
        (Timestamp.into(), "DATE '2020-01-02'", Some(At(D * DAY))),
        (Date.into(), "TIMESTAMP '2020-01-02 00:00:00'", Some(At(D))),
        (
            Date.into(),
            "TIMESTAMP '2020-01-02 12:00:00'",
            Some(Between { lo: D, nearest: D }),
        ),
        (
            Date.into(),
            "TIMESTAMP '1969-12-31 12:00:00'",
            Some(Between { lo: day_before, nearest: day_before }),
        ),
        // A string is the temporal type's own spelling, a number its storage value.
        (Date.into(), "'2020-01-02'", Some(At(D))),
        (Timestamp.into(), "'2020-01-02 00:00:01'", Some(At(D * DAY + 1_000_000))),
        (Date.into(), "7", Some(At(7))),
        (Date.into(), "1e1", Some(At(10))),
        (Date.into(), "1.5", None),
        // A BOOLEAN is its keyword or one of its text spellings, never a
        // number, and is a value of no other type.
        (Bool.into(), "TRUE", Some(At(1))),
        (Bool.into(), "' OFF '", Some(At(0))),
        (Bool.into(), "'1'", Some(At(1))),
        (Bool.into(), "'maybe'", None),
        (Bool.into(), "1", None),
        (I64.into(), "TRUE", None),
        // A type not stored as an integer places nothing.
        (F64.into(), "1", None),
        (String.into(), "'a'", None),
    ] {
        assert_eq!(place(&lit(src), ty), want, "{ty} {src}");
    }
}

/// A float column stores a numeric literal's binary64 value, an F32 one that
/// value narrowed; any other column the nearest value `place` finds.
#[test]
fn an_assignment_stores_the_nearest_value_or_names_the_refusal() {
    use TypeCode::*;
    let f64_bits = |v: f64| u128::from(v.to_bits());
    let f32_bits = |v: f32| u128::from(v.to_bits());
    for (tc, src, want) in [
        (I64, "2.5", Ok(3)),
        (I8, "127.5", Err("TINYINT value out of range: 127.5")),
        (F64, "5", Ok(f64_bits(5.0))),
        (F64, "-0.0", Ok(f64_bits(-0.0))),
        (F64, "DATE '2020-01-02'", Ok(f64_bits(18263.0))),
        (F64, "-18446744073709551616", Ok(f64_bits(-18446744073709551616.0))),
        (F64, U128_MAX, Ok(f64_bits(340282366920938463463374607431768211455.0))),
        (F32, "-0.0", Ok(f32_bits(-0.0))),
        (F32, "3.4028234e38", Ok(f32_bits(f32::MAX))),
        // Through binary64: parsed straight to f32 this would be the float above 1.0.
        (F32, "1.00000005960464477539063", Ok(f32_bits(1.0))),
        // A float literal past the column's range is refused, as an integer
        // column refuses one, where it was stored as an infinity.
        // A literal past `i128` has no exact decimal, so it is quoted as its
        // float.
        (F32, "1e39", Err("FLOAT value out of range: 1e39")),
        (I64, "1.10", Ok(1)),
        (I8, "-128.5", Err("TINYINT value out of range: -128.5")),
        (
            Date,
            "123456789012345678901.5",
            Err("123456789012345678901.5 is not a DATE value"),
        ),
        (I64, "DATE '2020-01-02'", Ok(D)),
        (Date, "TIMESTAMP '2020-01-02 03:04:05.5'", Ok(D)),
        (
            String,
            "DATE '2020-01-02'",
            Err("DATE '2020-01-02' is not a TEXT value"),
        ),
        (
            Bool,
            "TIMESTAMP '2020-01-02 03:04:05.5'",
            Err("TIMESTAMP '2020-01-02 03:04:05.5' is not a BOOLEAN value"),
        ),
        (
            F32,
            U128_MAX,
            Err("FLOAT value out of range: 340282366920938463463374607431768211455"),
        ),
        (F64, "1e400", Err("DOUBLE value out of range: Infinity")),
        (Date, "1.5", Err("1.5 is not a DATE value")),
        (U32, "'5'", Err("invalid INT UNSIGNED literal: '5'")),
        (F64, "'5'", Err("invalid DOUBLE literal: '5'")),
        (String, "5", Err("5 is not a TEXT value")),
        (Blob, "'5'", Err("invalid BLOB literal: '5'")),
    ] {
        let got = assign(&lit(src), ColType::of(tc));
        assert_eq!(got, want.map_err(str::to_owned), "{tc:?} {src}");
    }
}

/// `x CMP lit` against a value of `x`'s type, or a verdict for every value.
#[test]
fn a_comparison_with_a_placed_literal_is_one_against_a_value_or_a_constant() {
    use CmpOp::*;
    let between = Between { lo: 2, nearest: 3 };
    let below = Below { nearest: None };
    let above = Above { nearest: Some(9) };
    for (p, cmp, want) in [
        (At(5), Lt, Compared::Cmp(Lt, 5)),
        (between, Eq, Compared::Always(false)),
        (between, Ne, Compared::Always(true)),
        (between, Lt, Compared::Cmp(Le, 2)),
        (between, Le, Compared::Cmp(Le, 2)),
        (between, Gt, Compared::Cmp(Gt, 2)),
        (between, Ge, Compared::Cmp(Gt, 2)),
        (below, Gt, Compared::Always(true)),
        (below, Le, Compared::Always(false)),
        (below, Eq, Compared::Always(false)),
        (above, Lt, Compared::Always(true)),
        (above, Ge, Compared::Always(false)),
        (above, Ne, Compared::Always(true)),
    ] {
        assert_eq!(p.compare(cmp), want, "{p:?} {cmp:?}");
    }
}

/// What an assignment stores: the value itself, or the rounded one while that
/// lies inside the type.
#[test]
fn an_assignment_stores_the_nearest_value_inside_the_type() {
    let image = |v: i128| FixedInt::I8.pack(v);
    for (n, placed, stored) in [
        (1270, At(image(127)), Some(image(127))),
        (1264, Between { lo: image(126), nearest: image(126) }, Some(image(126))),
        // Half rounds away from zero, on either side of it.
        (25, Between { lo: image(2), nearest: image(3) }, Some(image(3))),
        (-25, Between { lo: image(-3), nearest: image(-3) }, Some(image(-3))),
        (-24, Between { lo: image(-3), nearest: image(-2) }, Some(image(-2))),
        (1274, Above { nearest: Some(image(127)) }, Some(image(127))),
        (1276, Above { nearest: None }, None),
        (-1284, Below { nearest: Some(image(-128)) }, Some(image(-128))),
    ] {
        let p = place_ratio(FixedInt::I8, n, 10, Round::HalfAwayFromZero);
        assert_eq!(p, placed, "{n}/10");
        assert_eq!(p.stored(), stored, "{n}/10");
    }
}

/// A scale gap whose power of ten reaches `i128`'s edge still places: scaled
/// down, every `i128` over a power past it is under a half, on its own side of
/// zero; scaled up past it, a value lies outside the type on its own side.
#[test]
fn a_scale_gap_at_i128s_edge_still_places() {
    let i64_of = |v: i128| FixedInt::I64.pack(v);
    for (v, lo) in [(i128::MAX, 0), (1, 0), (i128::MIN + 1, -1), (-1, -1)] {
        let p = place_scaled(FixedInt::I64, v, 60, 0);
        assert_eq!(p, Between { lo: i64_of(lo), nearest: i64_of(0) }, "{v}");
    }
    assert_eq!(place_scaled(FixedInt::I64, 0, 60, 2), At(0));
    // `den = 10^38`, the largest that fits, either side of one half.
    let half = 5 * 10i128.pow(37);
    assert_eq!(
        place_scaled(FixedInt::I64, half - 1, 38, 0),
        Between { lo: 0, nearest: 0 }
    );
    let minus_one = i64_of(-1);
    assert_eq!(
        place_scaled(FixedInt::I64, -half, 38, 0),
        Between { lo: minus_one, nearest: minus_one }
    );
    assert_eq!(place_scaled(FixedInt::I64, i128::MAX, 0, 2), Above { nearest: None });
    assert_eq!(place_scaled(FixedInt::I64, -i128::MAX, 0, 2), Below { nearest: None });
    // An unsigned type holds nothing below zero, and rounding still reaches it.
    assert_eq!(place_scaled(FixedInt::U8, -1, 60, 0), Below { nearest: Some(0) });
}

/// A numeric literal's text is the decimal it spells, exponent applied, at its
/// smallest exact scale — digits no float print could carry included.
#[test]
fn a_number_text_is_the_decimal_it_spells() {
    for (text, want) in [
        ("1.10", Some((11, 1))),
        ("1.0", Some((1, 0))),
        ("0.0000000100", Some((1, 8))),
        ("1.5e3", Some((1500, 0))),
        ("25e-1", Some((25, 1))),
        ("1234567890123456.78", Some((123456789012345678, 2))),
        ("0.1234567890123456789", Some((1234567890123456789, 19))),
        // Bounded by value and fractional digits, not by digit count.
        (
            "0.49999999999999999999999999999999999999",
            Some((49999999999999999999999999999999999999, 38)),
        ),
        ("100000000000000000000000000000000000000", Some((10i128.pow(38), 0))),
        ("999999999999999999999999999999999999999", None),
        ("1e39", None),
        ("1e-2147483648", None),
    ] {
        assert_eq!(decimal_of_number_text(text), want, "{text}");
    }
}
