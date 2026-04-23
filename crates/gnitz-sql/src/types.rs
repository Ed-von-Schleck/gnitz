use sqlparser::ast::{DataType, ExactNumberInfo};
use gnitz_core::TypeCode;
use crate::error::GnitzSqlError;

pub fn sql_type_to_typecode(dt: &DataType) -> Result<TypeCode, GnitzSqlError> {
    match dt {
        DataType::BigInt(_)                                 => Ok(TypeCode::I64),
        DataType::Int(_) | DataType::Integer(_)             => Ok(TypeCode::I32),
        DataType::SmallInt(_)                               => Ok(TypeCode::I16),
        DataType::TinyInt(_)                                => Ok(TypeCode::I8),
        DataType::BigIntUnsigned(_)                                                   => Ok(TypeCode::U64),
        DataType::IntUnsigned(_) | DataType::IntegerUnsigned(_) | DataType::UnsignedInteger => Ok(TypeCode::U32),
        DataType::SmallIntUnsigned(_)                                                 => Ok(TypeCode::U16),
        DataType::TinyIntUnsigned(_)                                                  => Ok(TypeCode::U8),
        DataType::Float(_)                                                            => Ok(TypeCode::F32),
        DataType::Double(_) | DataType::DoublePrecision | DataType::Real              => Ok(TypeCode::F64),
        DataType::Varchar(_) | DataType::Text | DataType::Char(_)     => Ok(TypeCode::String),
        // DECIMAL(p,0) with p in {38,39} maps to U128.
        // DECIMAL(38,0) is the common idiom for 128-bit integers (used by Spark, etc.).
        // DECIMAL(39,0) covers the full u128 range (u128::MAX has 39 decimal digits).
        DataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, 0))
        | DataType::Numeric(ExactNumberInfo::PrecisionAndScale(p, 0))
            if *p == 38 || *p == 39 => Ok(TypeCode::U128),
        DataType::Boolean => Err(GnitzSqlError::Unsupported(
            "BOOLEAN has no gnitz type; use TINYINT(1)".to_string()
        )),
        _ => Err(GnitzSqlError::Unsupported(format!("unsupported SQL type: {:?}", dt))),
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(ok(DataType::Float(None)), TypeCode::F32);
        assert_eq!(ok(DataType::Float(Some(24))), TypeCode::F32);
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
        assert!(sql_type_to_typecode(
            &DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 2))
        ).is_err());
    }

    // DECIMAL with no precision must NOT map to U128
    #[test]
    fn decimal_bare_is_unsupported() {
        assert!(sql_type_to_typecode(&DataType::Decimal(ExactNumberInfo::None)).is_err());
    }

    // --- error cases ---

    #[test]
    fn boolean_gives_helpful_error() {
        let msg = err(DataType::Boolean);
        assert!(msg.contains("TINYINT(1)"), "error should mention TINYINT(1): {}", msg);
    }

    #[test]
    fn unknown_type_gives_unsupported_error() {
        // Date is not supported
        let msg = err(DataType::Date);
        assert!(msg.contains("unsupported SQL type"), "unexpected error: {}", msg);
    }
}
