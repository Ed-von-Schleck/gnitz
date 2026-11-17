use super::*;
use crate::test_support::{opk_pk, pk_only_schema};
use gnitz_wire::TypeCode;

/// Each PK column decodes at its own offset and width, signed and
/// wider-than-16-byte keys included.
#[test]
fn format_pk_bytes_renders_every_pk_column_from_its_opk_image() {
    for (tc, v, want) in [
        (TypeCode::I8, -5i128, "-5"),
        (TypeCode::I16, -300, "-300"),
        (TypeCode::I32, -70_000, "-70000"),
        (TypeCode::I64, i64::MIN as i128, "-9223372036854775808"),
        (TypeCode::U8, 200, "200"),
        (TypeCode::U64, u64::MAX as i128, "18446744073709551615"),
        (
            TypeCode::U128,
            u128::MAX as i128,
            "340282366920938463463374607431768211455",
        ),
        (TypeCode::I128, -1, "-1"),
        (TypeCode::I128, i128::MIN, "-170141183460469231731687303715884105728"),
    ] {
        let schema = pk_only_schema(&[tc]);
        assert_eq!(
            format_pk_bytes(&schema, &opk_pk(&schema, &[v as u128]), &[]),
            want,
            "type {tc}"
        );
    }

    let uuid = pk_only_schema(&[TypeCode::UUID]);
    let v = 0x550e8400_e29b_41d4_a716_446655440000u128;
    assert_eq!(
        format_pk_bytes(&uuid, &opk_pk(&uuid, &[v]), &[]),
        "550e8400-e29b-41d4-a716-446655440000",
        "a UUID renders from all 128 bits, not a truncated low word",
    );

    // 24-byte compound PK: every column read at its own offset.
    let wide = pk_only_schema(&[TypeCode::U64; 3]);
    assert_eq!(format_pk_bytes(&wide, &opk_pk(&wide, &[7, 8, 9]), &[]), "7, 8, 9");
}

/// A calendar key renders as its date or timestamp, and a DECIMAL key at the
/// scale its catalog column carries; a DECIMAL with no scale given as its
/// stored integer.
#[test]
fn format_pk_bytes_spells_a_typed_key_as_a_client_prints_it() {
    let day = gnitz_wire::MICROS_PER_DAY;
    let (date, ts, dec) = (TypeCode::Date, TypeCode::Timestamp, TypeCode::Decimal);
    let schema = pk_only_schema(&[date, ts, dec]);
    let key = opk_pk(
        &schema,
        &[
            (-1i32) as u32 as u128,
            (19_723 * day + day / 2 + 500_000) as u64 as u128,
            (-110i64) as u64 as u128,
        ],
    );
    assert_eq!(
        format_pk_bytes(&schema, &key, &[0, 0, 2]),
        "1969-12-31, 2024-01-01 12:00:00.5, -1.10"
    );
    assert_eq!(
        format_pk_bytes(&schema, &key, &[]),
        "1969-12-31, 2024-01-01 12:00:00.5, -110"
    );
}
