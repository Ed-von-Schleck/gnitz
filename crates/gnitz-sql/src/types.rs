use crate::error::GnitzSqlError;
use gnitz_core::{FixedInt, TypeCode};
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
        _ => Err(GnitzSqlError::Unsupported(format!("unsupported SQL type: {dt}"))),
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
    let ident = crate::ast_util::object_name_ident(name)?;
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
/// parent) and the numeric-aggregate check. `gnitz_wire` owns the partition, and
/// its own drift matrix holds this set against every other type predicate.
pub(crate) fn is_integer_type(tc: TypeCode) -> bool {
    gnitz_wire::is_int(tc as u8)
}

/// Whether a value of this type fits the expression VM's 8-byte *scalar*
/// register — [`gnitz_core::ScalarKind`], the same classification the engine's
/// reduce kernel and aggregate-value index resolve their columns through, so the
/// binder cannot admit an aggregate the engine then refuses.
///
/// Not the same question as `register_image`, which is total over *both*
/// register classes and maps STRING to itself.
pub(crate) fn has_scalar_register(tc: TypeCode) -> bool {
    gnitz_core::ScalarKind::from_type_code(tc).is_some()
}

/// Whether `CAST(… AS tc)` has a form the VM can compute. STRING joins the
/// scalar-register types because the VM has a string register class of its own;
/// BLOB does not, and is not expressible anyway — `sql_type_to_typecode`
/// produces `TypeCode::Blob` for no SQL type. This is the codebase's only
/// cast-target gate.
pub(crate) fn is_cast_target(tc: TypeCode) -> bool {
    has_scalar_register(tc) || tc == TypeCode::String
}

/// A cast target that lands in an *integer* register, as the width it names —
/// what [`is_cast_target`] admits, minus STRING and the two floats. The
/// rejection is for a `BoundExpr::Cast` some rewrite built rather than the
/// binder, which cannot produce one.
pub(crate) fn int_cast_target(tc: TypeCode) -> Result<FixedInt, GnitzSqlError> {
    FixedInt::from_type_code(tc).ok_or_else(|| GnitzSqlError::Unsupported(format!("CAST to {tc:?} is not supported")))
}

/// True iff every value of integer type `child` is representable in integer type
/// `parent`, so rewriting an FK child column to the parent's type loses no value
/// the child could legally hold.
///
/// Reads back `join_key_common_type`'s contract rather than re-deriving the
/// sign/width ladder gnitz-wire owns: `parent` holds `child` exactly when the
/// pair's common type *is* `parent`. Valid over integer inputs alone (both
/// `is_integer_type`) — that ladder sends a UUID pair to U128. Not
/// `is_widening_promotion`, whose `is_fixed_int(target)` gate would reject the
/// `(U64, I128)` an FK needs.
pub(crate) fn int_domain_fits(child: TypeCode, parent: TypeCode) -> bool {
    child.join_key_common_type(parent) == Some(parent)
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
