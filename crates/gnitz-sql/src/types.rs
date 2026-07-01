use crate::error::GnitzSqlError;
use gnitz_core::TypeCode;
use sqlparser::ast::{DataType, ExactNumberInfo};

pub(crate) fn sql_type_to_typecode(dt: &DataType) -> Result<TypeCode, GnitzSqlError> {
    match dt {
        DataType::BigInt(_) => Ok(TypeCode::I64),
        DataType::Int(_) | DataType::Integer(_) => Ok(TypeCode::I32),
        DataType::SmallInt(_) => Ok(TypeCode::I16),
        DataType::TinyInt(_) => Ok(TypeCode::I8),
        DataType::BigIntUnsigned(_) => Ok(TypeCode::U64),
        DataType::IntUnsigned(_) | DataType::IntegerUnsigned(_) | DataType::UnsignedInteger => Ok(TypeCode::U32),
        DataType::SmallIntUnsigned(_) => Ok(TypeCode::U16),
        DataType::TinyIntUnsigned(_) => Ok(TypeCode::U8),
        DataType::Float(_) => Ok(TypeCode::F32),
        DataType::Double(_) | DataType::DoublePrecision | DataType::Real => Ok(TypeCode::F64),
        DataType::Varchar(_) | DataType::Text | DataType::Char(_) => Ok(TypeCode::String),
        DataType::Uuid => Ok(TypeCode::UUID),
        // DECIMAL(p,0) with p in {38,39} maps to U128.
        // DECIMAL(38,0) is the common idiom for 128-bit integers (used by Spark, etc.).
        // DECIMAL(39,0) covers the full u128 range (u128::MAX has 39 decimal digits).
        DataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, 0))
        | DataType::Numeric(ExactNumberInfo::PrecisionAndScale(p, 0))
            if *p == 38 || *p == 39 =>
        {
            Ok(TypeCode::U128)
        }
        DataType::Boolean => Err(GnitzSqlError::Unsupported(
            "BOOLEAN has no gnitz type; use TINYINT(1)".to_string(),
        )),
        _ => Err(GnitzSqlError::Unsupported(format!("unsupported SQL type: {dt:?}"))),
    }
}

/// Postgres SERIAL family → the underlying signed integer type, case-insensitive.
/// `None` for any non-SERIAL type. SERIAL/BIGSERIAL/SMALLSERIAL are not sqlparser
/// keywords, so 0.56 tokenizes them as words and parses them as
/// `DataType::Custom(ObjectName, Vec<String>)`; `SERIAL4`/`SERIAL8`/`SERIAL2` are
/// accepted aliases via the same fallthrough. Kept separate from
/// `sql_type_to_typecode` (which stays pure real-types) — a SERIAL column carries
/// the underlying type plus the `is_serial` marker.
pub(crate) fn serial_underlying(dt: &DataType) -> Option<TypeCode> {
    let DataType::Custom(name, _mods) = dt else {
        return None;
    };
    let ident = name.0.last().and_then(|p| p.as_ident())?;
    match ident.value.to_ascii_uppercase().as_str() {
        "SMALLSERIAL" | "SERIAL2" => Some(TypeCode::I16),
        "SERIAL" | "SERIAL4" => Some(TypeCode::I32),
        "BIGSERIAL" | "SERIAL8" => Some(TypeCode::I64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// TypeCode capability predicates
//
// One home for the type-membership tests that the binder (MIN/MAX rejection),
// the lowerer (ColRef rejection), and DDL (FK numeric compatibility) all query.
// gnitz_core already owns `is_float` / `is_german_string`; these add the sets
// it does not.
// ---------------------------------------------------------------------------

/// Integer column types — signed/unsigned at every width, including the 128-bit
/// pair. Used for FK compatibility (an integer child column widens to an integer
/// parent) and the numeric-aggregate check.
pub(crate) fn is_integer_type(tc: TypeCode) -> bool {
    matches!(
        tc,
        TypeCode::I8
            | TypeCode::I16
            | TypeCode::I32
            | TypeCode::I64
            | TypeCode::U8
            | TypeCode::U16
            | TypeCode::U32
            | TypeCode::U64
            | TypeCode::U128
            | TypeCode::I128
    )
}

/// The 16-byte integer-ish types (U128, UUID, I128) that have no i64 slot: the
/// engine's payload integer load handles only 1/2/4/8-byte columns, so the
/// lowerer must reject these in arithmetic/comparison contexts.
pub(crate) fn is_wide_int(tc: TypeCode) -> bool {
    matches!(tc, TypeCode::U128 | TypeCode::UUID | TypeCode::I128)
}

/// Whether MIN/MAX has a correct accumulator path for this column type. Wide
/// integer-ish types (U128/UUID/I128) have no i64 slot, and STRING/BLOB have no
/// usable ordering in the i64 comparator (`decode_signed` reads the descriptor
/// prefix as a garbage signed int). Everything else — narrow and 64-bit ints,
/// and floats — orders correctly.
pub(crate) fn is_min_max_orderable(tc: TypeCode) -> bool {
    !is_wide_int(tc) && !tc.is_german_string()
}

/// True iff every value of integer type `child` is representable in integer type
/// `parent`, so rewriting an FK child column to the parent's type loses no value
/// the child could legally hold. Only valid for integer inputs (both
/// `is_integer_type`); UUID and the non-integers never reach it.
pub(crate) fn int_domain_fits(child: TypeCode, parent: TypeCode) -> bool {
    let (cw, pw) = (child.wire_stride(), parent.wire_stride());
    match (child.is_signed_int(), parent.is_signed_int()) {
        (false, false) | (true, true) => cw <= pw, // same signedness → parent ≥ child width
        (false, true) => cw < pw,                  // unsigned child needs a strictly wider signed parent
        (true, false) => false,                    // signed child has negatives no unsigned parent holds
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

    #[test]
    fn decimal_38_0_still_maps_to_u128() {
        assert_eq!(
            ok(DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 0))),
            TypeCode::U128,
        );
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
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
        DataType::Custom(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]), vec![])
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
}
