use super::*;
use sqlparser::ast::{DataType, ExactNumberInfo, TimezoneInfo};

fn ok(dt: DataType) -> TypeCode {
    sql_col_type(&dt).expect("expected Ok").tc
}

fn err(dt: DataType) -> String {
    sql_col_type(&dt).expect_err("expected Err").to_string()
}

// --- signed integers ---

#[test]
fn tinyint_maps_to_i8() {
    assert_eq!(ok(DataType::TinyInt(None)), TypeCode::I8);
    assert_eq!(ok(DataType::TinyInt(Some(3))), TypeCode::I8);
}

#[test]
fn smallint_maps_to_i16() {
    assert_eq!(ok(DataType::SmallInt(None)), TypeCode::I16);
}

#[test]
fn int_and_integer_map_to_i32() {
    assert_eq!(ok(DataType::Int(None)), TypeCode::I32);
    assert_eq!(ok(DataType::Integer(None)), TypeCode::I32);
}

#[test]
fn bigint_maps_to_i64() {
    assert_eq!(ok(DataType::BigInt(None)), TypeCode::I64);
}

// --- unsigned integers ---

#[test]
fn tinyint_unsigned_maps_to_u8() {
    assert_eq!(ok(DataType::TinyIntUnsigned(None)), TypeCode::U8);
}

#[test]
fn smallint_unsigned_maps_to_u16() {
    assert_eq!(ok(DataType::SmallIntUnsigned(None)), TypeCode::U16);
}

#[test]
fn int_unsigned_maps_to_u32() {
    assert_eq!(ok(DataType::IntUnsigned(None)), TypeCode::U32);
    assert_eq!(ok(DataType::IntegerUnsigned(None)), TypeCode::U32);
    assert_eq!(ok(DataType::UnsignedInteger), TypeCode::U32);
}

#[test]
fn bigint_unsigned_maps_to_u64() {
    assert_eq!(ok(DataType::BigIntUnsigned(None)), TypeCode::U64);
}

// --- floats ---

#[test]
fn float_maps_to_f32() {
    assert_eq!(ok(DataType::Float(ExactNumberInfo::None)), TypeCode::F32);
    assert_eq!(ok(DataType::Float(ExactNumberInfo::Precision(24))), TypeCode::F32);
}

#[test]
fn double_and_aliases_map_to_f64() {
    assert_eq!(ok(DataType::Double(ExactNumberInfo::None)), TypeCode::F64);
    assert_eq!(ok(DataType::DoublePrecision), TypeCode::F64);
    assert_eq!(ok(DataType::Real), TypeCode::F64);
}

// --- strings ---

#[test]
fn varchar_text_char_map_to_string() {
    assert_eq!(ok(DataType::Varchar(None)), TypeCode::String);
    assert_eq!(ok(DataType::Text), TypeCode::String);
    assert_eq!(ok(DataType::Char(None)), TypeCode::String);
}

// --- U128 ---

#[test]
fn decimal_38_0_maps_to_u128() {
    assert_eq!(
        ok(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 0))),
        TypeCode::U128,
    );
}

#[test]
fn decimal_39_0_maps_to_u128() {
    assert_eq!(
        ok(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(39, 0))),
        TypeCode::U128,
    );
}

#[test]
fn numeric_38_0_maps_to_u128() {
    assert_eq!(
        ok(DataType::Numeric(ExactNumberInfo::PrecisionAndScale(38, 0))),
        TypeCode::U128,
    );
}

#[test]
fn numeric_39_0_maps_to_u128() {
    assert_eq!(
        ok(DataType::Numeric(ExactNumberInfo::PrecisionAndScale(39, 0))),
        TypeCode::U128,
    );
}

// DECIMAL with non-zero scale must NOT map to U128
#[test]
fn decimal_38_2_does_not_map_to_u128() {
    assert!(sql_col_type(&DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 2))).is_err());
}

// DECIMAL with no precision must NOT map to U128
#[test]
fn decimal_bare_is_unsupported() {
    assert!(sql_col_type(&DataType::Decimal(ExactNumberInfo::None)).is_err());
}

/// Up to 18 digits a DECIMAL is the fixed-point type at the written scale,
/// under every spelling; past that only the 128-bit integer idiom survives.
#[test]
fn decimal_up_to_18_digits_is_fixed_point() {
    let ty = |dt: DataType| sql_col_type(&dt).expect("expected Ok");
    assert_eq!(
        ty(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(10, 2))),
        ColType::decimal(2)
    );
    assert_eq!(
        ty(DataType::Numeric(ExactNumberInfo::PrecisionAndScale(18, 18))),
        ColType::decimal(18)
    );
    assert_eq!(
        ty(DataType::Decimal(ExactNumberInfo::Precision(5))),
        ColType::decimal(0)
    );
    assert_eq!(
        ty(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(18, 0))),
        ColType::decimal(0)
    );
    assert!(err(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(19, 2))).contains("precision"));
    assert!(err(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(5, 6))).contains("scale"));
    assert!(err(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(0, 0))).contains("precision"));
}

// --- UUID ---

#[test]
fn uuid_keyword_maps_to_typecode_uuid() {
    assert_eq!(ok(DataType::Uuid), TypeCode::UUID);
}

// --- error cases ---

#[test]
fn boolean_gives_helpful_error() {
    let msg = err(DataType::Boolean);
    assert!(msg.contains("TINYINT(1)"), "error should mention TINYINT(1): {msg}");
}

#[test]
fn unknown_type_gives_unsupported_error() {
    // The type is echoed as written, not as a parser dump.
    let msg = err(DataType::Interval { fields: None, precision: None });
    assert!(
        msg.contains("unsupported SQL type: INTERVAL"),
        "unexpected error: {msg}"
    );
}

#[test]
fn temporal_types_map_to_their_codes() {
    assert_eq!(ok(DataType::Date), TypeCode::Date);
    assert_eq!(ok(DataType::Timestamp(None, TimezoneInfo::None)), TypeCode::Timestamp);
    // The spellings that mean "no zone" all land on the one TIMESTAMP; only a
    // zoned one is refused.
    assert_eq!(ok(DataType::Datetime(None)), TypeCode::Timestamp);
    assert_eq!(ok(DataType::TimestampNtz(None)), TypeCode::Timestamp);
    assert!(err(DataType::Timestamp(None, TimezoneInfo::WithTimeZone)).contains("time zone"));
}

/// The parsers themselves are `gnitz-expr`'s; what is this crate's is that each
/// type reaches its own one and that a bad literal surfaces as a bind error.
#[test]
fn a_temporal_literal_routes_to_its_parser() {
    assert_eq!(
        temporal_literal(TypeCode::Date, "2024-02-29").unwrap(),
        i64::from(gnitz_expr::calendar::parse_date("2024-02-29").unwrap())
    );
    assert_eq!(
        temporal_literal(TypeCode::Timestamp, "2024-02-29 13:45:07").unwrap(),
        gnitz_expr::calendar::parse_timestamp("2024-02-29 13:45:07").unwrap()
    );
    let e = temporal_literal(TypeCode::Date, "2024-02-30").expect_err("2024 has no 30th of February");
    assert!(matches!(e, GnitzSqlError::Bind(_)), "unexpected error: {e:?}");
}

// --- SERIAL recognition (serial_underlying) ---

fn custom(name: &str) -> DataType {
    use sqlparser::ast::{Ident, ObjectName};
    DataType::Custom(ObjectName::from(Ident::new(name)), vec![])
}

#[test]
fn serial_family_maps_to_signed_ints() {
    assert_eq!(serial_underlying(&custom("SMALLSERIAL")), Some(TypeCode::I16));
    assert_eq!(serial_underlying(&custom("SERIAL2")), Some(TypeCode::I16));
    assert_eq!(serial_underlying(&custom("SERIAL")), Some(TypeCode::I32));
    assert_eq!(serial_underlying(&custom("SERIAL4")), Some(TypeCode::I32));
    assert_eq!(serial_underlying(&custom("BIGSERIAL")), Some(TypeCode::I64));
    assert_eq!(serial_underlying(&custom("SERIAL8")), Some(TypeCode::I64));
}

#[test]
fn serial_recognition_is_case_insensitive() {
    assert_eq!(serial_underlying(&custom("serial")), Some(TypeCode::I32));
    assert_eq!(serial_underlying(&custom("BigSerial")), Some(TypeCode::I64));
}

#[test]
fn non_serial_types_are_none() {
    assert_eq!(serial_underlying(&custom("HYPERLOGLOG")), None); // unrelated custom type
    assert_eq!(serial_underlying(&DataType::Int(None)), None);
    assert_eq!(serial_underlying(&DataType::BigInt(None)), None);
    assert_eq!(serial_underlying(&DataType::Text), None);
}
