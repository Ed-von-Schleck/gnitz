use crate::error::GnitzSqlError;
use gnitz_core::{ColType, TypeCode};
use gnitz_wire::decimal::MAX_DECIMAL_SCALE;
use sqlparser::ast::{DataType, ExactNumberInfo, TimezoneInfo};

pub(crate) fn sql_col_type(dt: &DataType) -> Result<ColType, GnitzSqlError> {
    let tc = match dt {
        DataType::BigInt(_) => TypeCode::I64,
        DataType::Int(_) | DataType::Integer(_) => TypeCode::I32,
        DataType::SmallInt(_) => TypeCode::I16,
        DataType::TinyInt(_) => TypeCode::I8,
        DataType::BigIntUnsigned(_) => TypeCode::U64,
        DataType::IntUnsigned(_) | DataType::IntegerUnsigned(_) | DataType::UnsignedInteger => TypeCode::U32,
        DataType::SmallIntUnsigned(_) => TypeCode::U16,
        DataType::TinyIntUnsigned(_) => TypeCode::U8,
        DataType::Float(_) => TypeCode::F32,
        DataType::Double(_) | DataType::DoublePrecision | DataType::Real => TypeCode::F64,
        DataType::Varchar(_) | DataType::Text | DataType::Char(_) => TypeCode::String,
        DataType::Uuid => TypeCode::UUID,
        DataType::Date => TypeCode::Date,
        // `TIMESTAMP_NTZ` says "no time zone", which is the one form supported.
        DataType::Timestamp(_, TimezoneInfo::None) | DataType::Datetime(_) | DataType::TimestampNtz(_) => {
            TypeCode::Timestamp
        }
        DataType::Timestamp(..) => {
            return Err(GnitzSqlError::Unsupported(
                "a TIMESTAMP carries no time zone here; write TIMESTAMP without one".to_string(),
            ))
        }
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => return decimal_type(info),
        DataType::Boolean => {
            return Err(GnitzSqlError::Unsupported(
                "BOOLEAN has no gnitz type; use TINYINT(1)".to_string(),
            ))
        }
        _ => return Err(GnitzSqlError::Unsupported(format!("unsupported SQL type: {dt}"))),
    };
    Ok(ColType::of(tc))
}

/// `DECIMAL(p, s)` / `NUMERIC(p, s)`. Up to 18 digits of precision the column
/// is a fixed-point `DECIMAL` of scale `s` (`DECIMAL(p)` is scale 0); the
/// precision picks nothing finer than that — a value is bounded by the `i64`
/// behind the scale, not by `p` digits. `DECIMAL(38,0)` and `(39,0)` keep
/// their meaning as the SQL spelling of a 128-bit unsigned integer.
fn decimal_type(info: &ExactNumberInfo) -> Result<ColType, GnitzSqlError> {
    let (p, s) = match *info {
        ExactNumberInfo::PrecisionAndScale(p, s) => (p, s),
        ExactNumberInfo::Precision(p) => (p, 0),
        ExactNumberInfo::None => {
            return Err(GnitzSqlError::Unsupported(
                "DECIMAL needs a precision and scale, e.g. DECIMAL(18, 2)".to_string(),
            ))
        }
    };
    if (p == 38 || p == 39) && s == 0 {
        return Ok(ColType::of(TypeCode::U128));
    }
    if p == 0 || p > MAX_DECIMAL_SCALE as u64 {
        return Err(GnitzSqlError::Unsupported(format!(
            "DECIMAL({p}, {s}): the precision must be 1..={MAX_DECIMAL_SCALE} (DECIMAL(38, 0) is the 128-bit integer)"
        )));
    }
    if s < 0 || s as u64 > p {
        return Err(GnitzSqlError::Unsupported(format!(
            "DECIMAL({p}, {s}): the scale must be 0..={p}"
        )));
    }
    Ok(ColType::decimal(s as u8))
}

/// Postgres SERIAL family → the underlying signed integer type, case-insensitive.
/// `None` for any non-SERIAL type. SERIAL/BIGSERIAL/SMALLSERIAL are not sqlparser
/// keywords, so 0.56 tokenizes them as words and parses them as
/// `DataType::Custom(ObjectName, Vec<String>)`; `SERIAL4`/`SERIAL8`/`SERIAL2` are
/// accepted aliases via the same fallthrough. Kept separate from
/// `sql_col_type` (which stays pure real-types) — a SERIAL column carries
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
// One home for the type-membership tests that the binder (SUM/AVG rejection)
// and the lowerer (ColRef rejection) query. gnitz_core already owns `is_float`
// / `is_german_string`; these add the sets it does not.
// ---------------------------------------------------------------------------

/// A `DATE '…'` / `TIMESTAMP '…'` literal, or a string written into a
/// temporal column, as the integer the column stores.
pub(crate) fn temporal_literal(tc: TypeCode, s: &str) -> Result<i64, GnitzSqlError> {
    let v = match tc {
        TypeCode::Date => gnitz_expr::calendar::parse_date(s).map(i64::from),
        TypeCode::Timestamp => gnitz_expr::calendar::parse_timestamp(s),
        _ => unreachable!("temporal_literal over a non-temporal type"),
    };
    v.ok_or_else(|| GnitzSqlError::Bind(format!("invalid {} literal: {s:?}", tc.wire_name())))
}

/// Whether a value of this type fits the expression VM's 8-byte *scalar*
/// register — [`gnitz_core::ScalarKind`], the same classification the engine's
/// reduce kernel resolves a summed column through, so the binder cannot admit a
/// SUM the engine then refuses.
///
/// Not the same question as `register_image`, which is total over *both*
/// register classes and maps STRING to itself.
pub(crate) fn has_scalar_register(tc: TypeCode) -> bool {
    gnitz_core::ScalarKind::from_type_code(tc).is_some()
}

/// Whether `CAST(… AS tc)` has a form the VM can compute. STRING joins the
/// scalar-register types because the VM has a string register class of its own;
/// BLOB does not, and is not expressible anyway — `sql_col_type`
/// produces `TypeCode::Blob` for no SQL type. This is the codebase's only
/// cast-target gate.
pub(crate) fn is_cast_target(tc: TypeCode) -> bool {
    has_scalar_register(tc) || tc == TypeCode::String
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
