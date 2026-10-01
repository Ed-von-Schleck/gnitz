use super::*;
use crate::test_support::{parse_stmt, rejected};
use sqlparser::ast::Statement;

/// `CREATE TABLE t (c <decl>)`'s one column, through `column_def`.
fn declared(decl: &str) -> Result<(ColumnDef, bool), GnitzSqlError> {
    let Statement::CreateTable(create) = parse_stmt(&format!("CREATE TABLE t (c {decl})")) else {
        panic!("{decl}: not a CREATE TABLE");
    };
    column_def(&create.columns[0])
}

/// Every accepted spelling, as the type it declares. A column is nullable unless
/// it says otherwise, and a SERIAL spelling is its signed integer, NOT NULL.
#[test]
fn each_accepted_spelling_declares_its_type() {
    use TypeCode::*;
    let t = ColType::of;
    let dec = ColType::decimal;
    let plain: &[(&str, ColType)] = &[
        ("TINYINT", t(I8)),
        ("TINYINT(3)", t(I8)),
        ("SMALLINT", t(I16)),
        ("INT", t(I32)),
        ("INTEGER", t(I32)),
        ("BIGINT", t(I64)),
        ("TINYINT UNSIGNED", t(U8)),
        ("SMALLINT UNSIGNED", t(U16)),
        ("INT UNSIGNED", t(U32)),
        ("INTEGER UNSIGNED", t(U32)),
        ("UNSIGNED INTEGER", t(U32)),
        ("BIGINT UNSIGNED", t(U64)),
        ("UINT128", t(U128)),
        ("UHUGEINT", t(U128)),
        ("INT128", t(I128)),
        ("HUGEINT", t(I128)),
        ("FLOAT", t(F32)),
        ("FLOAT(24)", t(F32)),
        ("DOUBLE", t(F64)),
        ("DOUBLE PRECISION", t(F64)),
        ("REAL", t(F64)),
        ("VARCHAR", t(String)),
        ("TEXT", t(String)),
        ("CHAR", t(String)),
        ("UUID", t(UUID)),
        ("DATE", t(Date)),
        ("TIMESTAMP", t(Timestamp)),
        ("TIMESTAMP WITHOUT TIME ZONE", t(Timestamp)),
        ("DATETIME", t(Timestamp)),
        ("TIMESTAMP_NTZ", t(Timestamp)),
        ("DECIMAL(10, 2)", dec(2)),
        ("DECIMAL(18, 0)", dec(0)),
        ("DECIMAL(5)", dec(0)),
        ("NUMERIC(18, 18)", dec(18)),
        ("DEC(4, 1)", dec(1)),
    ];
    for &(decl, ty) in plain {
        let got = declared(decl).unwrap_or_else(|e| panic!("{decl}: {e}"));
        assert_eq!(got, (ColumnDef::typed("c", ty, true), false), "{decl}");
    }
    assert_eq!(
        declared("BIGINT NOT NULL").unwrap(),
        (ColumnDef::new("c", I64, false), false)
    );
    for (decl, tc) in [
        ("SMALLSERIAL", I16),
        ("SERIAL2", I16),
        ("SERIAL", I32),
        ("SERIAL4", I32),
        ("serial", I32),
        ("BIGSERIAL", I64),
        ("SERIAL8", I64),
        ("BigSerial", I64),
    ] {
        assert_eq!(
            declared(decl).unwrap(),
            (ColumnDef::new("c", tc, false), true),
            "{decl}"
        );
    }
}

/// Every refused spelling names what is wrong with it.
#[test]
fn each_refused_spelling_names_its_reason() {
    for (decl, needle) in [
        ("BOOLEAN", "TINYINT(1)"),
        ("BOOL", "TINYINT(1)"),
        ("TIMESTAMP WITH TIME ZONE", "time zone"),
        ("TIMESTAMPTZ", "time zone"),
        ("DECIMAL", "precision and scale"),
        ("DECIMAL(19, 2)", "the precision must be 1..=18"),
        // The 128-bit integer is spelled UINT128, not as a DECIMAL.
        ("DECIMAL(38, 0)", "the precision must be 1..=18"),
        ("DECIMAL(0, 0)", "the precision must be 1..=18"),
        ("DECIMAL(5, 6)", "the scale must be 0..=5"),
        // Echoed as written, not as a parser dump.
        ("INTERVAL", "unsupported SQL type: INTERVAL"),
        ("HYPERLOGLOG", "unsupported SQL type: HYPERLOGLOG"),
        // A modifier makes it no SERIAL spelling, so it falls to the type map.
        ("SERIAL(5)", "unsupported SQL type: SERIAL(5)"),
    ] {
        let m = rejected(declared(decl));
        assert!(m.contains(needle), "{decl}: {m:?} does not name {needle:?}");
    }
}
