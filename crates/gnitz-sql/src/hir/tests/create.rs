use super::*;
use crate::test_support::parse_stmt;
use sqlparser::ast::Statement;

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
        assert_eq!(parse_size("capacity", lit).unwrap(), bytes, "{lit:?}");
    }
    for lit in ["lots", "5", "5 TB", "-5 MB", "0 MB", "18446744073709551615 GB", ""] {
        assert!(
            matches!(parse_size("capacity", lit), Err(GnitzSqlError::Plan(_))),
            "{lit:?}"
        );
    }
}

#[test]
fn the_option_list_fills_the_two_budgets_and_refuses_anything_else() {
    let both = options_of("WITH (capacity = '1 MB', delta = '2 MB')");
    assert!(matches!(both, Err(GnitzSqlError::Unsupported(ref m)) if m.contains("delta feed")));
    let cap = options_of("WITH (capacity = '1 MB')").unwrap();
    assert_eq!(cap, ViewProps::Bounded { capacity_bytes: 1 << 20 });
    let delta = options_of("WITH (DELTA = '2 KB')").unwrap();
    assert_eq!(delta, ViewProps::Fed { delta_bytes: 2 << 10 });
    match options_of("WITH (foo = '1 MB')") {
        Err(GnitzSqlError::Unsupported(m)) => assert!(m.contains("unknown CREATE VIEW option 'foo'"), "{m}"),
        other => panic!("{other:?}"),
    }
    for (with, needle) in [
        ("WITH (capacity = 5)", "single-quoted"),
        ("WITH (delta = 'lots')", "`delta`"),
        // A repeated key is refused rather than won by its last spelling.
        ("WITH (capacity = '1 MB', capacity = '4 GB')", "more than once"),
        ("WITH (delta = '1 MB', Delta = '1 MB')", "more than once"),
    ] {
        match options_of(with) {
            Err(GnitzSqlError::Plan(m)) => assert!(m.contains(needle), "{with}: {m}"),
            other => panic!("{with}: {other:?}"),
        }
    }
}
