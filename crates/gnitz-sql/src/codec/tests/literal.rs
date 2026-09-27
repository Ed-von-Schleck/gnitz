use super::*;
use crate::bind::structural::bind_constant;
use crate::test_support::parse_expr_sql;
use std::convert::Infallible;

fn lit(src: &str) -> BExpr<Infallible> {
    bind_constant(&parse_expr_sql(src)).unwrap_or_else(|e| panic!("{src}: {e}"))
}

fn at(tc: TypeCode, src: &str) -> Option<Placed> {
    place(&lit(src), ColType::of(tc))
}

fn packed(fi: FixedInt, v: i128) -> u128 {
    fi.pack(v)
}

#[test]
fn an_integer_is_at_its_value_or_outside_the_type() {
    use Placed::*;
    let i8 = |v| packed(FixedInt::I8, v);
    assert_eq!(at(TypeCode::I8, "-128"), Some(At(i8(-128))));
    assert_eq!(at(TypeCode::I8, "127"), Some(At(i8(127))));
    assert_eq!(at(TypeCode::I8, "-129"), Some(Below { nearest: None }));
    assert_eq!(at(TypeCode::I8, "128"), Some(Above { nearest: None }));
    assert_eq!(at(TypeCode::U8, "-1"), Some(Below { nearest: None }));
    assert_eq!(at(TypeCode::U8, "255"), Some(At(255)));
    assert_eq!(at(TypeCode::U8, "300"), Some(Above { nearest: None }));
    assert_eq!(at(TypeCode::I32, "3000000000"), Some(Above { nearest: None }));
    assert_eq!(at(TypeCode::I32, "-5"), Some(At(packed(FixedInt::I32, -5))));
    assert_eq!(
        at(TypeCode::U64, "18446744073709551615"),
        Some(At(u128::from(u64::MAX)))
    );
    assert_eq!(at(TypeCode::U64, "18446744073709551616"), Some(Above { nearest: None }));
    assert_eq!(at(TypeCode::U64, "-0"), Some(At(0)));
    // Past `i128`, both signs.
    let huge = "340282366920938463463374607431768211455";
    assert_eq!(at(TypeCode::I64, huge), Some(Above { nearest: None }));
    assert_eq!(at(TypeCode::I64, &format!("-{huge}")), Some(Below { nearest: None }));
}

#[test]
fn a_sixteen_byte_type_takes_integers_only() {
    use Placed::*;
    assert_eq!(at(TypeCode::I128, "-5"), Some(At(-5i128 as u128)));
    assert_eq!(at(TypeCode::U128, "-0"), Some(At(0)));
    assert_eq!(at(TypeCode::U128, "-1"), Some(Below { nearest: None }));
    assert_eq!(
        at(TypeCode::U128, "340282366920938463463374607431768211455"),
        Some(At(u128::MAX))
    );
    assert_eq!(
        at(TypeCode::I128, "340282366920938463463374607431768211455"),
        Some(Above { nearest: None })
    );
    assert_eq!(at(TypeCode::U128, "1.5"), None);
    assert_eq!(
        at(TypeCode::UUID, "'550e8400-e29b-41d4-a716-446655440000'"),
        Some(At(0x550e8400_e29b_41d4_a716_446655440000_u128))
    );
    assert_eq!(at(TypeCode::UUID, "'not-a-uuid'"), None);
}

#[test]
fn a_fraction_is_between_two_values_and_rounds_half_away_from_zero() {
    use Placed::*;
    let i64v = |v| packed(FixedInt::I64, v);
    assert_eq!(
        at(TypeCode::I64, "2.5"),
        Some(Between {
            lo: i64v(2),
            hi: i64v(3),
            nearest: i64v(3)
        })
    );
    assert_eq!(
        at(TypeCode::I64, "-2.5"),
        Some(Between {
            lo: i64v(-3),
            hi: i64v(-2),
            nearest: i64v(-3)
        })
    );
    assert_eq!(
        at(TypeCode::I64, "2.4"),
        Some(Between {
            lo: i64v(2),
            hi: i64v(3),
            nearest: i64v(2)
        })
    );
    assert_eq!(
        at(TypeCode::I64, "-2.4"),
        Some(Between {
            lo: i64v(-3),
            hi: i64v(-2),
            nearest: i64v(-2)
        })
    );
    assert_eq!(at(TypeCode::I64, "1e3"), Some(At(i64v(1000))));
    // A U64 neighbour above `i64::MAX`.
    assert_eq!(
        at(TypeCode::U64, "9223372036854775807.5"),
        Some(Between {
            lo: 9223372036854775807,
            hi: 9223372036854775808,
            nearest: 9223372036854775808
        })
    );
    // `den = 10^38`: 38 fractional digits, either side of one half.
    let d0 = ColType::decimal(0);
    assert_eq!(
        place(&lit("'.49999999999999999999999999999999999999'"), d0),
        Some(Between { lo: 0, hi: 1, nearest: 0 })
    );
    assert_eq!(
        place(&lit("'-.50000000000000000000000000000000000000'"), d0),
        Some(Between { lo: i64v(-1), hi: 0, nearest: i64v(-1) })
    );
}

#[test]
fn rounding_at_a_type_edge_keeps_the_edge_as_nearest() {
    use Placed::*;
    let i8 = |v| packed(FixedInt::I8, v);
    assert_eq!(at(TypeCode::I8, "127.4"), Some(Above { nearest: Some(i8(127)) }));
    assert_eq!(at(TypeCode::I8, "-128.4"), Some(Below { nearest: Some(i8(-128)) }));
    assert_eq!(at(TypeCode::I8, "127.5"), Some(Above { nearest: None }));
    assert_eq!(at(TypeCode::I8, "-128.5"), Some(Below { nearest: None }));
}

#[test]
fn a_decimal_reads_a_literal_at_its_scale() {
    use Placed::*;
    let d2 = ColType::decimal(2);
    let dec = |src: &str| place(&lit(src), d2);
    assert_eq!(dec("1.5"), Some(At(150)));
    assert_eq!(dec("'1.50'"), Some(At(150)));
    assert_eq!(dec("1.005"), Some(Between { lo: 100, hi: 101, nearest: 101 }));
    assert_eq!(dec("1234567890123456.78"), Some(At(123456789012345678)));
    assert_eq!(
        dec("0.1234567890123456789"),
        Some(Between { lo: 12, hi: 13, nearest: 12 })
    );
    // Rounds onto `i64::MAX` from just past it.
    assert_eq!(
        dec("92233720368547758.071"),
        Some(Above { nearest: Some(i64::MAX as u128) })
    );
    assert_eq!(dec("'abc'"), None);
    assert_eq!(dec("NULL"), None);
}

#[test]
fn a_date_and_a_timestamp_convert_between_units() {
    use Placed::*;
    let day = i128::from(MICROS_PER_DAY);
    let d = parse_temporal(TypeCode::Date, "2020-01-02").unwrap();
    assert_eq!(
        at(TypeCode::Timestamp, "DATE '2020-01-02'"),
        Some(At(packed(FixedInt::I64, i128::from(d) * day)))
    );
    assert_eq!(
        at(TypeCode::Date, "TIMESTAMP '2020-01-02 00:00:00'"),
        Some(At(packed(FixedInt::I32, d.into())))
    );
    assert_eq!(
        at(TypeCode::Date, "TIMESTAMP '2020-01-02 12:00:00'"),
        Some(Between {
            lo: packed(FixedInt::I32, d.into()),
            hi: packed(FixedInt::I32, i128::from(d) + 1),
            nearest: packed(FixedInt::I32, d.into())
        })
    );
    // Before 1970 the floor goes down, not toward zero.
    let pre = parse_temporal(TypeCode::Date, "1969-12-31").unwrap();
    assert_eq!(
        at(TypeCode::Date, "TIMESTAMP '1969-12-31 12:00:00'"),
        Some(Between {
            lo: packed(FixedInt::I32, pre.into()),
            hi: packed(FixedInt::I32, i128::from(pre) + 1),
            nearest: packed(FixedInt::I32, pre.into())
        })
    );
    // A string is the column type's own spelling; an integer its storage value.
    assert_eq!(
        at(TypeCode::Date, "'2020-01-02'"),
        Some(At(packed(FixedInt::I32, d.into())))
    );
    assert_eq!(at(TypeCode::Date, "7"), Some(At(7)));
    assert_eq!(at(TypeCode::Date, "1.5"), None);
    assert_eq!(at(TypeCode::Date, "'garbage'"), None);
}

/// The parsers themselves are `gnitz-expr`'s; what is this crate's is that each
/// temporal type reaches its own one, and that every other type spells nothing.
#[test]
fn a_temporal_spelling_routes_to_its_parser() {
    assert_eq!(
        parse_temporal(TypeCode::Date, "2024-02-29"),
        gnitz_expr::calendar::parse_date("2024-02-29").map(i64::from)
    );
    assert!(parse_temporal(TypeCode::Date, "2024-02-29").is_some());
    assert_eq!(
        parse_temporal(TypeCode::Timestamp, "2024-02-29 13:45:07"),
        gnitz_expr::calendar::parse_timestamp("2024-02-29 13:45:07")
    );
    assert!(parse_temporal(TypeCode::Timestamp, "2024-02-29 13:45:07").is_some());
    assert_eq!(
        parse_temporal(TypeCode::Date, "2024-02-30"),
        None,
        "2024 has no 30th of February"
    );
    assert_eq!(parse_temporal(TypeCode::I64, "2024-02-29"), None);
}
