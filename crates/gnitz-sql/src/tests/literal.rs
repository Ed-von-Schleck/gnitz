use super::*;
use crate::bind::bind_constant;
use crate::test_support::parse_expr_sql;
use std::convert::Infallible;
use Placed::*;

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
    for &tc in TypeCode::ALL {
        let Some(fi) = FixedInt::exact(tc) else { continue };
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
        (I8, "127.5", Err("I8 value out of range: 127.5")),
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
        (
            F32,
            "1e39",
            Err("F32 value out of range: 1000000000000000000000000000000000000000"),
        ),
        (
            F32,
            U128_MAX,
            Err("F32 value out of range: 340282366920938463463374607431768211455"),
        ),
        (F64, "1e400", Err("F64 value out of range: inf")),
        (Date, "1.5", Err("1.5 is not a DATE value")),
        (U32, "'5'", Err("invalid U32 literal: '5'")),
        (F64, "'5'", Err("invalid F64 literal: '5'")),
        (String, "5", Err("5 is not a STRING value")),
        (Blob, "'5'", Err("invalid BLOB literal: '5'")),
    ] {
        let got = assign(&lit(src), ColType::of(tc));
        assert_eq!(got, want.map_err(str::to_owned), "{tc:?} {src}");
    }
}
