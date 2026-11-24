use super::*;
use crate::test_support::{assert_rejects, parse_stmt, rejected};
use sqlparser::ast::Statement;

/// The `WITH (…)` clause of a `CREATE TABLE`; a `CREATE VIEW`'s parses the same.
fn options_of(with: &str) -> CreateTableOptions {
    match parse_stmt(&format!("CREATE TABLE t (id BIGINT PRIMARY KEY) {with}")) {
        Statement::CreateTable(c) => c.table_options,
        other => panic!("not a CREATE TABLE: {other}"),
    }
}

/// Each table property as `with_clause` spells it, read back by the decoder.
#[test]
fn table_properties_spell_as_with_entries_that_read_back() {
    let (p, repl) = (TableProps::default(), TableDistribution::Replicated);
    for (props, with) in [
        (p, ""),
        (TableProps { distribution: repl, ..p }, " WITH (replicated = true)"),
        (TableProps { stream: true, ..p }, " WITH (stream = true)"),
        (TableProps { compress: true, ..p }, " WITH (compress = true)"),
        (
            TableProps {
                distribution: repl,
                stream: true,
                compress: true,
                ..p
            },
            " WITH (replicated = true, stream = true, compress = true)",
        ),
    ] {
        assert_eq!(with_clause(&RelProps::Table(props)), with);
        assert_eq!(parse_table_options(&options_of(with)).unwrap(), props, "{with}");
    }
    // An explicit `false` is the default; keys read in any order and case.
    assert_eq!(
        parse_table_options(&options_of("WITH (STREAM = true, replicated = false)")).unwrap(),
        TableProps { stream: true, ..p }
    );
}

/// Each view property as `with_clause` spells it, in every size unit, read
/// back by the decoder.
#[test]
fn view_properties_spell_as_with_entries_that_read_back() {
    let n = |bytes: u64| NonZeroU64::new(bytes).unwrap();
    let bounded = |bytes| ViewBudget::Bounded { capacity_bytes: n(bytes) };
    let fed = |bytes| ViewBudget::Fed { delta_bytes: n(bytes) };
    let plain = ViewProps::default();
    for (props, with) in [
        (plain, ""),
        (ViewProps { compress: true, ..plain }, " WITH (compress = true)"),
        (
            ViewProps { budget: bounded(4 << 20), ..plain },
            " WITH (capacity = '4 MB')",
        ),
        (
            ViewProps { budget: bounded(1536), compress: true },
            " WITH (capacity = '1536 B', compress = true)",
        ),
        (ViewProps { budget: fed(1 << 30), ..plain }, " WITH (delta = '1 GB')"),
        (
            ViewProps { budget: fed(3 << 10), compress: true },
            " WITH (delta = '3 KB', compress = true)",
        ),
    ] {
        assert_eq!(with_clause(&RelProps::View(props)), with);
        assert_eq!(parse_view_options(&options_of(with)).unwrap(), props, "{with}");
    }
    assert_eq!(
        with_option_names().collect::<Vec<_>>(),
        ["replicated", "stream", "compress", "capacity", "delta"]
    );
}

/// A size reads leniently in spacing and case; everything else is refused,
/// malformed apart from out of range.
#[test]
fn a_size_is_a_positive_integer_and_a_binary_unit() {
    for (lit, bytes) in [("16KB", 16 << 10), ("4mb", 4 << 20), (" 1 gb ", 1 << 30)] {
        assert_eq!(parse_size("capacity", lit).unwrap().get(), bytes, "{lit:?}");
    }
    for (lit, needle) in [
        ("lots", "is not a size"),
        ("5", "is not a size"),
        ("5 TB", "is not a size"),
        ("-5 MB", "is not a size"),
        ("", "is not a size"),
        ("0 MB", "out of range"),
        ("18446744073709551615 GB", "out of range"),
        ("99999999999999999999 B", "out of range"),
    ] {
        assert_rejects(lit, parse_size("capacity", lit), needle);
    }
}

/// Any key the list does not name, a repeat under any spelling, any clause form
/// but `WITH (key = value, …)`, or a value of the wrong kind is refused by name.
#[test]
fn a_malformed_option_is_refused_by_name() {
    let table: fn(&str) -> Result<(), GnitzSqlError> = |w| parse_table_options(&options_of(w)).map(drop);
    let view: fn(&str) -> Result<(), GnitzSqlError> = |w| parse_view_options(&options_of(w)).map(drop);
    for (decode, with, needle) in [
        (table, "WITH (c = 1)", "unknown CREATE TABLE option 'c'"),
        (
            table,
            "WITH (stream = true, STREAM = false)",
            "CREATE TABLE option `stream` is given more than once",
        ),
        (table, "OPTIONS(stream = true)", "OPTIONS (…) is not supported"),
        (
            table,
            "WITH (HEAP)",
            "unsupported CREATE TABLE option `HEAP` in WITH (…)",
        ),
        (
            table,
            "WITH (CLUSTERED INDEX (id))",
            "unsupported CREATE TABLE option `CLUSTERED INDEX (id)` in WITH (…)",
        ),
        (table, "WITH (stream = 1)", "WITH (stream = …) expects a boolean"),
        (
            view,
            "WITH (capacity = 5)",
            "`capacity` takes a single-quoted size string",
        ),
        (
            view,
            "WITH (capacity = '1 MB', delta = '1 MB')",
            "CREATE VIEW WITH (capacity …, delta …): a capacity-bounded view cannot carry a delta feed",
        ),
    ] {
        let m = rejected(decode(with));
        assert!(m.contains(needle) && !m.contains("Span"), "{with}: {m}");
    }
}
