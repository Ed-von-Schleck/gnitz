use super::*;
use crate::bind::structural::bind_constant;
use crate::test_support::parse_expr_sql;
use std::convert::Infallible;
use Placed::*;

fn lit(src: &str) -> BExpr<Infallible> {
    bind_constant(&parse_expr_sql(src)).unwrap_or_else(|e| panic!("{src}: {e}"))
}

const BELOW: Option<Placed> = Some(Below { nearest: None });
const ABOVE: Option<Placed> = Some(Above { nearest: None });
const U128_MAX: &str = "340282366920938463463374607431768211455";
/// 2020-01-02 as DATE storage.
const D: i128 = 18263;
const DAY: i128 = MICROS_PER_DAY as i128;

fn i8(v: i128) -> u128 {
    FixedInt::I8.pack(v)
}
fn i32(v: i128) -> u128 {
    FixedInt::I32.pack(v)
}
fn i64(v: i128) -> u128 {
    FixedInt::I64.pack(v)
}

#[test]
fn a_literal_is_placed_among_the_values_of_the_type() {
    let d0 = ColType::decimal(0);
    let d2 = ColType::decimal(2);
    let neg_huge = format!("-{U128_MAX}");
    for (ty, src, want) in [
        // An integer is at its value or outside the type.
        (TypeCode::I8.into(), "-128", Some(At(i8(-128)))),
        (TypeCode::I8.into(), "127", Some(At(i8(127)))),
        (TypeCode::I8.into(), "-129", BELOW),
        (TypeCode::I8.into(), "128", ABOVE),
        (TypeCode::U8.into(), "-1", BELOW),
        (TypeCode::U8.into(), "255", Some(At(255))),
        (TypeCode::U8.into(), "300", ABOVE),
        (TypeCode::I32.into(), "3000000000", ABOVE),
        (TypeCode::U64.into(), "18446744073709551615", Some(At(u64::MAX.into()))),
        (TypeCode::U64.into(), "18446744073709551616", ABOVE),
        (TypeCode::U64.into(), "-0", Some(At(0))),
        (TypeCode::I64.into(), U128_MAX, ABOVE),
        (TypeCode::I64.into(), &neg_huge, BELOW),
        (TypeCode::I64.into(), "1e3", Some(At(i64(1000)))),
        // A fraction lies between two values; `nearest` rounds half away from zero.
        (
            TypeCode::I64.into(),
            "2.5",
            Some(Between { lo: i64(2), nearest: i64(3) }),
        ),
        (
            TypeCode::I64.into(),
            "-2.5",
            Some(Between { lo: i64(-3), nearest: i64(-3) }),
        ),
        (
            TypeCode::I64.into(),
            "2.4",
            Some(Between { lo: i64(2), nearest: i64(2) }),
        ),
        (
            TypeCode::I64.into(),
            "-2.4",
            Some(Between { lo: i64(-3), nearest: i64(-2) }),
        ),
        // A U64 neighbour above `i64::MAX`.
        (
            TypeCode::U64.into(),
            "9223372036854775807.5",
            Some(Between { lo: i64::MAX as u128, nearest: 1 << 63 }),
        ),
        // Rounding lands on a type's edge from just past it, and not from further.
        (TypeCode::I8.into(), "127.4", Some(Above { nearest: Some(i8(127)) })),
        (TypeCode::I8.into(), "-128.4", Some(Below { nearest: Some(i8(-128)) })),
        (TypeCode::I8.into(), "127.5", ABOVE),
        (TypeCode::I8.into(), "-128.5", BELOW),
        // A 16-byte type takes whole numbers, and a UUID its string spelling.
        (TypeCode::I128.into(), "-5", Some(At(-5i128 as u128))),
        (TypeCode::I128.into(), U128_MAX, ABOVE),
        (TypeCode::U128.into(), "-0", Some(At(0))),
        (TypeCode::U128.into(), "-1", BELOW),
        (TypeCode::U128.into(), U128_MAX, Some(At(u128::MAX))),
        (TypeCode::U128.into(), "1e3", Some(At(1000))),
        (TypeCode::U128.into(), "1.5", None),
        (
            TypeCode::UUID.into(),
            "'550e8400-e29b-41d4-a716-446655440000'",
            Some(At(0x550e8400_e29b_41d4_a716_446655440000)),
        ),
        (TypeCode::UUID.into(), "'not-a-uuid'", None),
        // A DECIMAL reads a literal, or its string spelling, at its scale.
        (d2, "1.5", Some(At(150))),
        (d2, "'1.50'", Some(At(150))),
        (d2, "1.005", Some(Between { lo: 100, nearest: 101 })),
        (d2, "1234567890123456.78", Some(At(123456789012345678))),
        (d2, "0.1234567890123456789", Some(Between { lo: 12, nearest: 12 })),
        (
            d2,
            "92233720368547758.071",
            Some(Above { nearest: Some(i64::MAX as u128) }),
        ),
        // Scaling up past `i128`.
        (d2, "'170141183460469231731687303715884105727'", ABOVE),
        (d2, "'-170141183460469231731687303715884105727'", BELOW),
        (d2, "'abc'", None),
        (d2, "NULL", None),
        // `den = 10^38`: 38 fractional digits, either side of one half.
        (
            d0,
            "'.49999999999999999999999999999999999999'",
            Some(Between { lo: 0, nearest: 0 }),
        ),
        (
            d0,
            "0.49999999999999999999999999999999999999",
            Some(Between { lo: 0, nearest: 0 }),
        ),
        (
            d0,
            "'-.50000000000000000000000000000000000000'",
            Some(Between { lo: i64(-1), nearest: i64(-1) }),
        ),
        // A DATE and a TIMESTAMP convert between units, flooring before 1970.
        (TypeCode::Timestamp.into(), "DATE '2020-01-02'", Some(At(i64(D * DAY)))),
        (
            TypeCode::Date.into(),
            "TIMESTAMP '2020-01-02 00:00:00'",
            Some(At(i32(D))),
        ),
        (
            TypeCode::Date.into(),
            "TIMESTAMP '2020-01-02 12:00:00'",
            Some(Between { lo: i32(D), nearest: i32(D) }),
        ),
        (
            TypeCode::Date.into(),
            "TIMESTAMP '1969-12-31 12:00:00'",
            Some(Between { lo: i32(-1), nearest: i32(-1) }),
        ),
        // A string is the temporal type's own spelling, a number its storage value.
        (TypeCode::Date.into(), "'2020-01-02'", Some(At(i32(D)))),
        (
            TypeCode::Timestamp.into(),
            "'2020-01-02 00:00:01'",
            Some(At(i64(D * DAY + 1_000_000))),
        ),
        (TypeCode::Date.into(), "7", Some(At(7))),
        (TypeCode::Date.into(), "1e1", Some(At(10))),
        (TypeCode::Date.into(), "1.5", None),
        (TypeCode::Date.into(), "'2020-02-30'", None),
        // A temporal literal into a numeric column is its storage integer.
        (TypeCode::I64.into(), "DATE '2020-01-02'", Some(At(i64(D)))),
        // A type not stored as an integer places nothing.
        (TypeCode::F64.into(), "1", None),
        (TypeCode::String.into(), "'a'", None),
    ] {
        assert_eq!(place(&lit(src), ty), want, "{ty} {src}");
    }
}

#[test]
fn an_assignment_stores_the_nearest_value_or_names_the_refusal() {
    for (tc, src, want) in [
        (TypeCode::I64, "-2.5", Ok(i64(-3))),
        (TypeCode::I8, "127.4", Ok(i8(127))),
        (TypeCode::I8, "-128.4", Ok(i8(-128))),
        (TypeCode::F64, "-0.0", Ok((-0.0f64).to_bits().into())),
        (TypeCode::F32, "0.1", Ok((0.1f64 as f32).to_bits().into())),
        (TypeCode::F32, "3.4028234e38", Ok(f32::MAX.to_bits().into())),
        // A float literal past the column's range is refused, as an integer
        // column refuses one, where it was stored as an infinity.
        (
            TypeCode::F32,
            "1e39",
            Err("F32 value out of range: 1000000000000000000000000000000000000000"),
        ),
        (
            TypeCode::F32,
            U128_MAX,
            Err("F32 value out of range: 340282366920938463463374607431768211455"),
        ),
        (TypeCode::F64, "1e400", Err("F64 value out of range: inf")),
        (TypeCode::I8, "127.5", Err("I8 value out of range: 127.5")),
        (TypeCode::Date, "1.5", Err("1.5 is not a DATE value")),
        (TypeCode::U32, "'5'", Err("invalid U32 literal: '5'")),
        (TypeCode::F64, "'5'", Err("invalid F64 literal: '5'")),
        (TypeCode::String, "5", Err("5 is not a STRING value")),
        (TypeCode::Blob, "'5'", Err("invalid BLOB literal: '5'")),
    ] {
        let got = assign(&lit(src), ColType::of(tc));
        assert_eq!(got, want.map_err(String::from), "{tc:?} {src}");
    }
}
