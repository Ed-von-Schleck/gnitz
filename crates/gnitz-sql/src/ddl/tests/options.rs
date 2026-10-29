use super::*;
use crate::test_support::{parse_stmt, rejected};
use sqlparser::ast::Statement;

/// One slot per named key, filled case-insensitively; any other key, a repeat
/// under any spelling, or any clause form but `WITH (key = value, …)` is refused.
#[test]
fn kv_options_fills_named_slots_and_refuses_anything_else() {
    let decode = |clause: &str| {
        let sql = format!("CREATE TABLE t (id BIGINT PRIMARY KEY) {clause}");
        let Statement::CreateTable(c) = parse_stmt(&sql) else {
            panic!("not a CREATE TABLE: {sql}");
        };
        kv_options(&c.table_options, "X", ["a", "b"]).map(|slots| slots.map(|v| v.map(ToString::to_string)))
    };
    assert_eq!(decode("").unwrap(), [None, None]);
    assert_eq!(decode("WITH (B = 2)").unwrap(), [None, Some("2".into())]);
    assert_eq!(
        decode("WITH (b = 2, a = 1)").unwrap(),
        [Some("1".into()), Some("2".into())]
    );
    for (clause, needle) in [
        ("WITH (c = 1)", "unknown X option 'c'"),
        ("WITH (a = 1, A = 2)", "X option `a` is given more than once"),
        ("OPTIONS(a = 1)", "OPTIONS (…) is not supported"),
        ("WITH (HEAP)", "unsupported X option `HEAP` in WITH (…)"),
        (
            "WITH (CLUSTERED INDEX (id))",
            "unsupported X option `CLUSTERED INDEX (id)` in WITH (…)",
        ),
    ] {
        let m = rejected(decode(clause));
        assert!(m.contains(needle), "{clause}: {m}");
        assert!(!m.contains("Span"), "{clause}: {m}");
    }
}

fn options_of(with: &str) -> Result<ViewProps, GnitzSqlError> {
    let Statement::CreateView(cv) = parse_stmt(&format!("CREATE VIEW v {with} AS SELECT 1")) else {
        panic!("not a CREATE VIEW");
    };
    decode_view_options(&cv.options)
}

#[test]
fn a_size_is_a_positive_integer_and_a_binary_unit() {
    for (lit, bytes) in [
        ("16 KB", 16 << 10),
        ("16KB", 16 << 10),
        ("4mb", 4 << 20),
        ("1 GB", 1 << 30),
        (" 1 gb ", 1 << 30),
    ] {
        assert_eq!(parse_size("capacity", lit).unwrap().get(), bytes, "{lit:?}");
    }
    for lit in ["lots", "5", "5 TB", "-5 MB", "0 MB", "18446744073709551615 GB", ""] {
        assert!(
            matches!(parse_size("capacity", lit), Err(GnitzSqlError::Rejected(_))),
            "{lit:?}"
        );
    }
}

#[test]
fn each_budget_decodes_to_its_view_class() {
    assert_eq!(
        options_of("WITH (capacity = '1 MB')").unwrap().budget,
        ViewBudget::Bounded {
            capacity_bytes: NonZeroU64::new(1 << 20).unwrap()
        }
    );
    assert_eq!(
        options_of("WITH (delta = '2 KB')").unwrap().budget,
        ViewBudget::Fed {
            delta_bytes: NonZeroU64::new(2 << 10).unwrap()
        }
    );
}

#[test]
fn compress_is_a_boolean_beside_any_budget() {
    let props = options_of("WITH (capacity = '1 MB', compress = true)").unwrap();
    assert!(props.compress && matches!(props.budget, ViewBudget::Bounded { .. }));
    assert!(!options_of("WITH (delta = '2 KB')").unwrap().compress);
    assert!(options_of("WITH (compress = 1)").is_err(), "not a boolean");
}

#[test]
fn with_options_spell_every_budget_a_with_clause_reads_back() {
    let kb = |n: u64| NonZeroU64::new(n).unwrap();
    for budget in [
        ViewBudget::default(),
        ViewBudget::Bounded { capacity_bytes: kb(4 << 20) },
        ViewBudget::Bounded { capacity_bytes: kb(1536) },
        ViewBudget::Bounded { capacity_bytes: kb(7) },
        ViewBudget::Fed { delta_bytes: kb(1 << 30) },
        ViewBudget::Fed { delta_bytes: kb(3 << 10) },
    ] {
        for compress in [false, true] {
            let props = ViewProps { budget, compress };
            let entries = view_with_options(&props);
            let with = match entries.is_empty() {
                true => String::new(),
                false => format!("WITH ({})", entries.join(", ")),
            };
            assert_eq!(options_of(&with).unwrap(), props, "{with}");
        }
    }
    let props = ViewProps {
        budget: ViewBudget::Bounded { capacity_bytes: kb(1536) },
        compress: false,
    };
    assert_eq!(view_with_options(&props), ["capacity = '1536 B'"]);
}

fn table_options_of(with: &str) -> CreateTableOptions {
    match parse_stmt(&format!("CREATE TABLE t (id BIGINT PRIMARY KEY) {with}")) {
        Statement::CreateTable(c) => c.table_options,
        other => panic!("not a CREATE TABLE: {other}"),
    }
}

#[test]
fn with_options_carry_replicated_and_stream_independently() {
    let (keyed, repl) = (TableDistribution::default(), TableDistribution::Replicated);
    for (with, stream, distribution) in [
        ("", false, keyed),
        ("WITH (replicated = true)", false, repl),
        ("WITH (stream = true)", true, keyed),
        ("WITH (stream = true, replicated = false)", true, keyed),
        ("WITH (stream = true, replicated = true)", true, repl),
    ] {
        let want = TableProps {
            stream,
            distribution,
            serial: false,
            compress: false,
        };
        assert_eq!(parse_table_options(&table_options_of(with)).unwrap(), want, "{with}");
    }
    let e = parse_table_options(&table_options_of("WITH (stream = 1)")).unwrap_err();
    assert!(
        matches!(&e, GnitzSqlError::Rejected(m) if m.contains("WITH (stream = …) expects a boolean")),
        "{e:?}"
    );
}

#[test]
fn with_options_spell_every_table_property_a_with_clause_reads_back() {
    for replicated in [false, true] {
        for stream in [false, true] {
            for compress in [false, true] {
                let props = TableProps {
                    stream,
                    serial: false,
                    distribution: match replicated {
                        true => TableDistribution::Replicated,
                        false => TableDistribution::default(),
                    },
                    compress,
                };
                let entries = table_with_options(&props);
                let with = match entries.is_empty() {
                    true => String::new(),
                    false => format!("WITH ({})", entries.join(", ")),
                };
                assert_eq!(parse_table_options(&table_options_of(&with)).unwrap(), props, "{with}");
            }
        }
    }
}
