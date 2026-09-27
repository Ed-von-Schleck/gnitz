use super::*;
use crate::test_support::parse_stmt;
use sqlparser::ast::{DataType, ExactNumberInfo as N, Statement, TimezoneInfo};

/// Every accepted arm of `sql_col_type`, once.
#[test]
fn each_accepted_spelling_maps_to_its_type() {
    let t = ColType::of;
    let ok: &[(DataType, ColType)] = &[
        (DataType::TinyInt(None), t(TypeCode::I8)),
        (DataType::TinyInt(Some(3)), t(TypeCode::I8)),
        (DataType::SmallInt(None), t(TypeCode::I16)),
        (DataType::Int(None), t(TypeCode::I32)),
        (DataType::Integer(None), t(TypeCode::I32)),
        (DataType::BigInt(None), t(TypeCode::I64)),
        (DataType::TinyIntUnsigned(None), t(TypeCode::U8)),
        (DataType::SmallIntUnsigned(None), t(TypeCode::U16)),
        (DataType::IntUnsigned(None), t(TypeCode::U32)),
        (DataType::IntegerUnsigned(None), t(TypeCode::U32)),
        (DataType::UnsignedInteger, t(TypeCode::U32)),
        (DataType::BigIntUnsigned(None), t(TypeCode::U64)),
        (DataType::UInt128, t(TypeCode::U128)),
        (DataType::UHugeInt, t(TypeCode::U128)),
        (DataType::Int128, t(TypeCode::I128)),
        (DataType::HugeInt, t(TypeCode::I128)),
        (DataType::Float(N::None), t(TypeCode::F32)),
        (DataType::Float(N::Precision(24)), t(TypeCode::F32)),
        (DataType::Double(N::None), t(TypeCode::F64)),
        (DataType::DoublePrecision, t(TypeCode::F64)),
        (DataType::Real, t(TypeCode::F64)),
        (DataType::Varchar(None), t(TypeCode::String)),
        (DataType::Text, t(TypeCode::String)),
        (DataType::Char(None), t(TypeCode::String)),
        (DataType::Uuid, t(TypeCode::UUID)),
        (DataType::Date, t(TypeCode::Date)),
        (DataType::Timestamp(None, TimezoneInfo::None), t(TypeCode::Timestamp)),
        (
            DataType::Timestamp(None, TimezoneInfo::WithoutTimeZone),
            t(TypeCode::Timestamp),
        ),
        (DataType::Datetime(None), t(TypeCode::Timestamp)),
        (DataType::TimestampNtz(None), t(TypeCode::Timestamp)),
        (DataType::Decimal(N::PrecisionAndScale(10, 2)), ColType::decimal(2)),
        (DataType::Decimal(N::PrecisionAndScale(18, 0)), ColType::decimal(0)),
        (DataType::Decimal(N::Precision(5)), ColType::decimal(0)),
        (DataType::Numeric(N::PrecisionAndScale(18, 18)), ColType::decimal(18)),
        (DataType::Dec(N::PrecisionAndScale(4, 1)), ColType::decimal(1)),
    ];
    for (dt, want) in ok {
        assert_eq!(sql_col_type(dt).unwrap_or_else(|e| panic!("{dt}: {e}")), *want, "{dt}");
    }
}

/// Every refused spelling names what is wrong with it.
#[test]
fn each_refused_spelling_names_its_reason() {
    let err: &[(DataType, &str)] = &[
        (DataType::Boolean, "TINYINT(1)"),
        (DataType::Bool, "TINYINT(1)"),
        (DataType::Timestamp(None, TimezoneInfo::WithTimeZone), "time zone"),
        (DataType::Decimal(N::None), "precision and scale"),
        (DataType::Decimal(N::PrecisionAndScale(19, 2)), "precision"),
        // The 128-bit integer is spelled UINT128, not as a DECIMAL.
        (DataType::Decimal(N::PrecisionAndScale(38, 0)), "precision"),
        (DataType::Decimal(N::PrecisionAndScale(5, 6)), "scale"),
        (DataType::Decimal(N::PrecisionAndScale(0, 0)), "precision"),
        // Echoed as written, not as a parser dump.
        (
            DataType::Interval { fields: None, precision: None },
            "unsupported SQL type: INTERVAL",
        ),
    ];
    for (dt, needle) in err {
        let msg = sql_col_type(dt).expect_err(&dt.to_string()).to_string();
        assert!(msg.contains(needle), "{dt}: {msg:?} does not name {needle:?}");
    }
}

/// `CREATE TABLE t (<decl>)`'s one column, through `column_def`.
fn declared(decl: &str) -> Result<(ColumnDef, bool), GnitzSqlError> {
    let Statement::CreateTable(create) = parse_stmt(&format!("CREATE TABLE t ({decl})")) else {
        panic!("{decl}: not a CREATE TABLE");
    };
    column_def(&create.columns[0])
}

/// A SERIAL spelling is its signed integer, NOT NULL, and reported as SERIAL;
/// any other column carries its declared nullability.
#[test]
fn column_def_reads_serial_and_nullability() {
    let ok: &[(&str, TypeCode, bool, bool)] = &[
        // (declaration, type, nullable, declared SERIAL)
        ("c SMALLSERIAL", TypeCode::I16, false, true),
        ("c SERIAL2", TypeCode::I16, false, true),
        ("c SERIAL", TypeCode::I32, false, true),
        ("c SERIAL4", TypeCode::I32, false, true),
        ("c BIGSERIAL", TypeCode::I64, false, true),
        ("c SERIAL8", TypeCode::I64, false, true),
        ("c serial", TypeCode::I32, false, true),
        ("c BigSerial", TypeCode::I64, false, true),
        ("c BIGINT NOT NULL", TypeCode::I64, false, false),
        ("c TEXT", TypeCode::String, true, false),
    ];
    for &(decl, tc, nullable, serial) in ok {
        let (def, is_serial) = declared(decl).unwrap_or_else(|e| panic!("{decl}: {e}"));
        assert_eq!(
            (def.name.as_str(), def.ty, def.is_nullable, is_serial),
            ("c", ColType::of(tc), nullable, serial),
            "{decl}"
        );
    }
    for (decl, needle) in [
        // A modifier makes it no SERIAL spelling, so it falls to the type map.
        ("c SERIAL(5)", "unsupported SQL type: SERIAL(5)"),
        ("c HYPERLOGLOG", "unsupported SQL type: HYPERLOGLOG"),
    ] {
        let msg = declared(decl).expect_err(decl).to_string();
        assert!(msg.contains(needle), "{decl}: {msg:?} does not name {needle:?}");
    }
}
