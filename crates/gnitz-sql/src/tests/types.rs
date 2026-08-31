use super::*;
use sqlparser::ast::{DataType, ExactNumberInfo};

fn ok(dt: DataType) -> TypeCode {
    sql_type_to_typecode(&dt).expect("expected Ok")
}

fn err(dt: DataType) -> String {
    sql_type_to_typecode(&dt).expect_err("expected Err").to_string()
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
    assert!(sql_type_to_typecode(&DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 2))).is_err());
}

// DECIMAL with no precision must NOT map to U128
#[test]
fn decimal_bare_is_unsupported() {
    assert!(sql_type_to_typecode(&DataType::Decimal(ExactNumberInfo::None)).is_err());
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
    // Date is not supported
    let msg = err(DataType::Date);
    assert!(msg.contains("unsupported SQL type"), "unexpected error: {msg}");
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

// --- FK integer-domain compatibility (int_domain_fits) ---

#[test]
fn int_domain_fits_accepts_lossless_rewrites() {
    for (c, p) in [
        (TypeCode::I32, TypeCode::I64),  // signed widen
        (TypeCode::U32, TypeCode::I64),  // unsigned → strictly wider signed
        (TypeCode::U8, TypeCode::U16),   // unsigned widen
        (TypeCode::I32, TypeCode::I32),  // identity
        (TypeCode::U64, TypeCode::U64),  // identity
        (TypeCode::U64, TypeCode::I128), // u64 fits i128
    ] {
        assert!(int_domain_fits(c, p), "{c:?} → {p:?} should fit");
    }
}

#[test]
fn int_domain_fits_rejects_lossy_rewrites() {
    for (c, p) in [
        (TypeCode::U64, TypeCode::I64),   // same width, unsigned → signed
        (TypeCode::U32, TypeCode::I32),   // same width, unsigned → signed
        (TypeCode::I64, TypeCode::U64),   // signed → unsigned
        (TypeCode::I32, TypeCode::U32),   // signed → unsigned
        (TypeCode::U128, TypeCode::I128), // 16→16, unsigned → signed
        (TypeCode::U8, TypeCode::I8),     // 1→1, unsigned → signed
    ] {
        assert!(!int_domain_fits(c, p), "{c:?} → {p:?} should not fit");
    }
}
