use crate::error::GnitzSqlError;
use gnitz_core::{ColType, ColumnDef, TypeCode};
use gnitz_wire::decimal::MAX_DECIMAL_SCALE;
use sqlparser::ast::{ColumnOption, DataType, ExactNumberInfo, TimezoneInfo};

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
        DataType::UInt128 | DataType::UHugeInt => TypeCode::U128,
        DataType::Int128 | DataType::HugeInt => TypeCode::I128,
        DataType::Uuid => TypeCode::UUID,
        DataType::Date => TypeCode::Date,
        // Every spelling that says no time zone; a zoned one is refused.
        DataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        | DataType::Datetime(_)
        | DataType::TimestampNtz(_) => TypeCode::Timestamp,
        DataType::Timestamp(..) => {
            return Err(GnitzSqlError::Unsupported(
                "a TIMESTAMP carries no time zone here; write TIMESTAMP without one".to_string(),
            ))
        }
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => return decimal_type(info),
        DataType::Boolean | DataType::Bool => {
            return Err(GnitzSqlError::Unsupported(
                "BOOLEAN has no gnitz type; use TINYINT(1)".to_string(),
            ))
        }
        _ => return Err(GnitzSqlError::Unsupported(format!("unsupported SQL type: {dt}"))),
    };
    Ok(ColType::of(tc))
}

/// `DECIMAL(p, s)` / `NUMERIC(p, s)`: fixed-point at scale `s` (`DECIMAL(p)` is
/// scale 0), `p` in `1..=MAX_DECIMAL_SCALE`. The precision bounds nothing finer — a value is
/// bounded by the `i64` behind its scale.
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
    if p == 0 || p > MAX_DECIMAL_SCALE as u64 {
        return Err(GnitzSqlError::Unsupported(format!(
            "DECIMAL({p}, {s}): the precision must be 1..={MAX_DECIMAL_SCALE}"
        )));
    }
    if s < 0 || s as u64 > p {
        return Err(GnitzSqlError::Unsupported(format!(
            "DECIMAL({p}, {s}): the scale must be 0..={p}"
        )));
    }
    Ok(ColType::decimal(s as u8))
}

/// A declared column, and whether it was spelled SERIAL: a SERIAL column is its
/// signed integer, NOT NULL.
pub(crate) fn column_def(col: &sqlparser::ast::ColumnDef) -> Result<(ColumnDef, bool), GnitzSqlError> {
    let name = col.name.value.clone();
    if let Some(tc) = serial_type(&col.data_type) {
        return Ok((ColumnDef::new(name, tc, false), true));
    }
    let nullable = !col.options.iter().any(|o| matches!(o.option, ColumnOption::NotNull));
    Ok((ColumnDef::typed(name, sql_col_type(&col.data_type)?, nullable), false))
}

/// The SERIAL family's signed integer; a modifier (`SERIAL(5)`) makes it no SERIAL.
fn serial_type(dt: &DataType) -> Option<TypeCode> {
    let DataType::Custom(name, mods) = dt else {
        return None;
    };
    if !mods.is_empty() {
        return None;
    }
    let ident = crate::ast_util::object_name_ident(name)?;
    match ident.value.to_ascii_uppercase().as_str() {
        "SMALLSERIAL" | "SERIAL2" => Some(TypeCode::I16),
        "SERIAL" | "SERIAL4" => Some(TypeCode::I32),
        "BIGSERIAL" | "SERIAL8" => Some(TypeCode::I64),
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
