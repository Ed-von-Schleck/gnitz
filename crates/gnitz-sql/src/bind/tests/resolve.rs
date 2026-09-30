use super::*;
use crate::test_support::{col, parse_expr_sql, rejected};
use gnitz_wire::TypeCode;

/// `[_k hidden, id, a, ID hidden, A]` — a visible name shared with a hidden
/// column, and a visible name written twice (as a `SELECT *` join view carries).
fn cols() -> Vec<ColumnDef> {
    let u = |n| col(n, TypeCode::U64);
    vec![u("_k").hidden(), u("id"), u("a"), u("ID").hidden(), u("A")]
}

#[test]
fn a_name_resolves_to_its_one_visible_column_by_physical_index() {
    let cols = cols();
    for (name, want) in [("id", Some(1)), ("Id", Some(1)), ("_k", None), ("missing", None)] {
        assert_eq!(find_unique_column(&cols, name).unwrap(), want, "{name}");
    }
    assert_eq!(
        rejected(find_unique_column(&cols, "a")),
        "column reference 'a' is ambiguous"
    );
    assert_eq!(rejected(require_column(&cols, "_k")), "column '_k' not found");
}

/// Only an unqualified name can name an output column; a qualified one is left
/// to the caller's scope, which checks the qualifier.
#[test]
fn output_column_matches_unqualified_names_only() {
    let cols = cols();
    for (src, want) in [
        ("ID", Some(1)),
        ("(id)", Some(1)),
        ("t.id", None),
        ("id + 1", None),
        ("_k", None),
    ] {
        assert_eq!(output_column(&parse_expr_sql(src), &cols).unwrap(), want, "{src}");
    }
}

#[test]
fn positional_aliases_rename_the_visible_columns_in_order() {
    let rename = |aliases: &[&str]| {
        let aliases: Vec<Ident> = aliases.iter().map(|a| Ident::new(*a)).collect();
        let mut defs = vec![
            col("_k", TypeCode::U64).hidden(),
            col("a", TypeCode::U64),
            col("b", TypeCode::U64),
        ];
        apply_positional_aliases(aliases.iter(), &mut defs, "CTE 'd'")
            .map(|()| defs.into_iter().map(|c| c.name).collect::<Vec<_>>())
    };
    assert_eq!(rename(&["x", "y"]).unwrap(), ["_k", "x", "y"]);
    assert_eq!(rename(&[]).unwrap(), ["_k", "a", "b"]);
    assert_eq!(
        rejected(rename(&["x"])),
        "CTE 'd' defines 1 column aliases but body returns 2 columns"
    );
    assert!(rejected(rename(&["x", "X"])).contains("duplicate column name 'X'"));
}

/// A name is asked once per statement, whatever its case and whether or not
/// it exists.
#[test]
fn the_catalog_asks_the_resolver_once_per_name() {
    let asked = RefCell::new(Vec::new());
    let resolve = |name: &str| {
        asked.borrow_mut().push(name.to_string());
        Ok(None)
    };
    let cat = Catalog::new("s", &resolve);
    assert!(cat.probe("T").unwrap().is_none());
    assert!(cat.probe("t").unwrap().is_none());
    let err = cat.probe_relation("t").unwrap_err();
    assert_eq!(
        format!("{err:?}"),
        format!("{:?}", crate::error::missing_relation("s", "t"))
    );
    assert_eq!(*asked.borrow(), ["T"]);
}
