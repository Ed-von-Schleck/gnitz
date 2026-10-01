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
            matches!(parse_size("capacity", lit), Err(GnitzSqlError::Rejected(_))),
            "{lit:?}"
        );
    }
}

#[test]
fn each_budget_decodes_to_its_view_class() {
    assert_eq!(
        options_of("WITH (capacity = '1 MB')").unwrap(),
        ViewProps::Bounded { capacity_bytes: 1 << 20 }
    );
    assert_eq!(
        options_of("WITH (delta = '2 KB')").unwrap(),
        ViewProps::Fed { delta_bytes: 2 << 10 }
    );
}
