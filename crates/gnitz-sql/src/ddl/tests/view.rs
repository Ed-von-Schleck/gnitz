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
