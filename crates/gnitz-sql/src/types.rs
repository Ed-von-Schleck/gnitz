use crate::error::GnitzSqlError;
use gnitz_wire::{ColType, DECIMAL_PRECISION, TypeCode};
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
        DataType::Float(info) => float_type(info)?,
        DataType::Double(_) | DataType::DoublePrecision | DataType::Real => TypeCode::F64,
        DataType::Varchar(_) | DataType::Text | DataType::Char(_) => TypeCode::String,
        DataType::Blob(None) | DataType::Bytea => TypeCode::Blob,
        DataType::UInt128 | DataType::UHugeInt => TypeCode::U128,
        DataType::Int128 | DataType::HugeInt => TypeCode::I128,
        DataType::Uuid => TypeCode::UUID,
        DataType::Date => TypeCode::Date,
        // Every spelling that says no time zone; a zoned one is refused.
        DataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        | DataType::Datetime(_)
        | DataType::TimestampNtz(_) => TypeCode::Timestamp,
        DataType::Timestamp(..) => {
            return Err(GnitzSqlError::rejected(
                "a TIMESTAMP carries no time zone here; write TIMESTAMP without one",
            ));
        }
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => return decimal_type(info),
        DataType::Boolean | DataType::Bool => TypeCode::Bool,
        _ => return Err(GnitzSqlError::rejected(format!("unsupported SQL type: {dt}"))),
    };
    Ok(ColType::of(tc))
}

/// `FLOAT` / `FLOAT(p)`: `p` counts binary digits of mantissa, so `1..=24` is single
/// precision and `25..=53` double; any other modifier is refused.
fn float_type(info: &ExactNumberInfo) -> Result<TypeCode, GnitzSqlError> {
    match *info {
        ExactNumberInfo::None | ExactNumberInfo::Precision(1..=24) => Ok(TypeCode::F32),
        ExactNumberInfo::Precision(25..=53) => Ok(TypeCode::F64),
        ExactNumberInfo::Precision(p) => Err(GnitzSqlError::rejected(format!(
            "FLOAT({p}): the precision must be 1..=53"
        ))),
        ExactNumberInfo::PrecisionAndScale(p, s) => Err(GnitzSqlError::rejected(format!(
            "FLOAT({p}, {s}): a FLOAT takes no scale; write DECIMAL({p}, {s}) for fixed point"
        ))),
    }
}

/// `DECIMAL(p, s)` / `NUMERIC(p, s)`: fixed-point at scale `s` (`DECIMAL(p)` is
/// scale 0), `p` in `1..=DECIMAL_PRECISION`. `p` bounds no value; the `i64` behind
/// the scale does.
fn decimal_type(info: &ExactNumberInfo) -> Result<ColType, GnitzSqlError> {
    let (p, s) = match *info {
        ExactNumberInfo::PrecisionAndScale(p, s) => (p, s),
        ExactNumberInfo::Precision(p) => (p, 0),
        ExactNumberInfo::None => {
            return Err(GnitzSqlError::rejected(
                "DECIMAL needs a precision and scale, e.g. DECIMAL(18, 2)",
            ));
        }
    };
    if p == 0 || p > DECIMAL_PRECISION as u64 {
        return Err(GnitzSqlError::rejected(format!(
            "DECIMAL({p}, {s}): the precision must be 1..={DECIMAL_PRECISION}"
        )));
    }
    if s < 0 || s as u64 > p {
        return Err(GnitzSqlError::rejected(format!(
            "DECIMAL({p}, {s}): the scale must be 0..={p}"
        )));
    }
    Ok(ColType::decimal(s as u8))
}

/// A column's declared type, and whether it was spelled SERIAL: a SERIAL
/// spelling declares its signed integer.
pub(crate) fn declared_type(dt: &DataType) -> Result<(ColType, bool), GnitzSqlError> {
    match serial_type(dt) {
        Some(tc) => Ok((ColType::of(tc), true)),
        None => Ok((sql_col_type(dt)?, false)),
    }
}

/// The SERIAL family's signed integer; a modifier (`SERIAL(5)`) or a qualifier
/// (`foo.SERIAL`) makes it no SERIAL.
fn serial_type(dt: &DataType) -> Option<TypeCode> {
    let DataType::Custom(name, mods) = dt else {
        return None;
    };
    if !mods.is_empty() {
        return None;
    }
    let ident = crate::ast_util::single_part_ident(name)?;
    SERIAL_TYPES
        .iter()
        .find(|(n, _)| ident.eq_ignore_ascii_case(n))
        .map(|&(_, tc)| tc)
}

/// The SERIAL spelling a column of integer type `tc` is declared by.
pub(crate) fn serial_type_name(tc: TypeCode) -> Option<&'static str> {
    SERIAL_TYPES.iter().find(|&&(_, t)| t == tc).map(|&(n, _)| n)
}

/// The SERIAL family, by spelling: each its signed integer, the spelling a
/// column is written back by first.
const SERIAL_TYPES: &[(&str, TypeCode)] = &[
    ("SMALLSERIAL", TypeCode::I16),
    ("SERIAL2", TypeCode::I16),
    ("SERIAL", TypeCode::I32),
    ("SERIAL4", TypeCode::I32),
    ("BIGSERIAL", TypeCode::I64),
    ("SERIAL8", TypeCode::I64),
];

/// Every spelling [`sql_col_type`] accepts, without its modifiers.
const TYPE_NAMES: &[&str] = &[
    "TINYINT",
    "SMALLINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "TINYINT UNSIGNED",
    "SMALLINT UNSIGNED",
    "INT UNSIGNED",
    "INTEGER UNSIGNED",
    "UNSIGNED INTEGER",
    "BIGINT UNSIGNED",
    "UINT128",
    "UHUGEINT",
    "INT128",
    "HUGEINT",
    "FLOAT",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
    "VARCHAR",
    "TEXT",
    "CHAR",
    "BLOB",
    "BYTEA",
    "UUID",
    "BOOLEAN",
    "BOOL",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE",
    "DATETIME",
    "TIMESTAMP_NTZ",
    "DECIMAL",
    "NUMERIC",
    "DEC",
];

/// Every column type spelling a `CREATE TABLE` accepts, the SERIAL family's
/// included, without modifiers.
pub fn type_names() -> impl Iterator<Item = &'static str> {
    TYPE_NAMES.iter().copied().chain(SERIAL_TYPES.iter().map(|&(n, _)| n))
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
