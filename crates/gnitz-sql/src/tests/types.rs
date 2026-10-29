use super::*;
use crate::test_support::{assert_rejects, parse_stmt};
use sqlparser::ast::Statement;
use sqlparser::parser::Parser;

/// `CREATE TABLE t (c <decl>)`'s one column, through `column_def`.
fn declared(decl: &str) -> Result<(ColumnDef, bool), GnitzSqlError> {
    let Statement::CreateTable(create) = parse_stmt(&format!("CREATE TABLE t (c {decl})")) else {
        panic!("{decl}: not a CREATE TABLE");
    };
    column_def(&create.columns[0])
}

/// Every alias and modifier, as the type it declares — each type's own spelling
/// is [`sql_type_name_round_trips`]'s. A column is nullable unless it says
/// otherwise, and a SERIAL spelling is its signed integer, NOT NULL.
#[test]
fn each_accepted_spelling_declares_its_type() {
    use TypeCode::*;
    let t = ColType::of;
    let dec = ColType::decimal;
    let plain: &[(&str, ColType)] = &[
        ("TINYINT(3)", t(I8)),
        ("INTEGER", t(I32)),
        ("INTEGER UNSIGNED", t(U32)),
        ("UNSIGNED INTEGER", t(U32)),
        ("UINT128", t(U128)),
        ("INT128", t(I128)),
        ("FLOAT(1)", t(F32)),
        ("FLOAT(24)", t(F32)),
        ("FLOAT(25)", t(F64)),
        ("FLOAT(53)", t(F64)),
        ("DOUBLE PRECISION", t(F64)),
        ("REAL", t(F64)),
        ("VARCHAR", t(String)),
        ("CHAR", t(String)),
        ("BYTEA", t(Blob)),
        ("BOOL", t(Bool)),
        ("TIMESTAMP WITHOUT TIME ZONE", t(Timestamp)),
        ("DATETIME", t(Timestamp)),
        ("TIMESTAMP_NTZ", t(Timestamp)),
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
    assert_eq!(serial_type_name(U64), None);
}

/// Every refused spelling names what is wrong with it.
#[test]
fn each_refused_spelling_names_its_reason() {
    for (decl, needle) in [
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
        // So does a qualifier.
        ("foo.SERIAL", "unsupported SQL type: foo.SERIAL"),
        ("pg_catalog.BIGSERIAL", "unsupported SQL type: pg_catalog.BIGSERIAL"),
        // FLOAT(p) counts mantissa bits: 1..=53, and no scale.
        ("FLOAT(0)", "the precision must be 1..=53"),
        ("FLOAT(54)", "the precision must be 1..=53"),
        ("FLOAT(7, 2)", "a FLOAT takes no scale"),
        // A BLOB is unbounded; a length would have to be enforced or ignored.
        ("BLOB(16)", "unsupported SQL type: BLOB(16)"),
    ] {
        assert_rejects(decl, declared(decl), needle);
    }
}

/// Every type's SQL spelling declares that type again.
#[test]
fn sql_type_name_round_trips() {
    let types = TypeCode::ALL
        .iter()
        .filter(|&&tc| tc != TypeCode::Decimal)
        .map(|&tc| ColType::of(tc))
        .chain([
            ColType::decimal(0),
            ColType::decimal(2),
            ColType::decimal(MAX_DECIMAL_SCALE),
        ]);
    for ty in types {
        let spelled = sql_type_name(ty);
        let (def, _) = declared(&spelled).unwrap_or_else(|e| panic!("{spelled}: {e}"));
        assert_eq!(def.ty, ty, "{spelled}");
    }
}

/// `type_names` and the planner agree both ways: every listed spelling
/// declares, and every single word the planner declares is a word of one.
#[test]
fn type_names_are_the_spellings_the_planner_accepts() {
    for name in type_names() {
        let decl = match name {
            "DECIMAL" | "NUMERIC" | "DEC" => format!("{name}(10, 2)"),
            _ => name.to_string(),
        };
        declared(&decl).unwrap_or_else(|e| panic!("{decl}: {e}"));
    }
    let words: Vec<&str> = type_names().flat_map(str::split_whitespace).collect();
    for &kw in sqlparser::keywords::ALL_KEYWORDS {
        let accepted =
            Parser::parse_sql(&crate::DIALECT, &format!("CREATE TABLE t (c {kw})")).is_ok_and(|_| declared(kw).is_ok());
        assert!(
            !accepted || words.contains(&kw),
            "{kw} declares but is not in type_names()"
        );
    }
}
