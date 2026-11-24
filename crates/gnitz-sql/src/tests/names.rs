use super::*;
use crate::test_support::parse_query;
use sqlparser::ast::SetExpr;

/// Every keyword, and names no keyword is, written as a column or as the parts of
/// a relation name it may be, reads back as itself; a name that reads back bare is written
/// bare, and a quote inside one is doubled.
#[test]
fn a_name_is_written_as_sql_reads_it_back() {
    let specials = ["id", "orders", "a b", "say \"hi\"", "2024_sales", "ü"];
    let keywords = sqlparser::keywords::ALL_KEYWORDS.iter().map(|k| k.to_lowercase());
    for n in keywords.chain(specials.map(String::from)) {
        let sql = format!("SELECT {} FROM t", column_text(&n));
        let read = match &*parse_query(&sql).body {
            SetExpr::Select(s) => match &s.projection[..] {
                [SelectItem::UnnamedExpr(Expr::Identifier(i))] => Some(i.value.clone()),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(read.as_deref(), Some(&n[..]), "{sql}");
        if RelName::new(&n, &n).is_err() {
            continue;
        }
        let text = format!("{}.{}", relation_text(&n), relation_text(&n));
        let rel = parse_relation_name(&text, "public").unwrap_or_else(|| panic!("{text}"));
        assert_eq!((rel.schema(), rel.name()), (&n[..], &n[..]), "{text}");
        assert_eq!(parse_schema_name(&relation_text(&n)).as_deref(), Some(&n[..]), "{n}");
    }
    assert_eq!(column_text("id"), "id");
    assert_eq!(relation_text("orders"), "orders");
    assert_eq!(column_text("say \"hi\""), "\"say \"\"hi\"\"\"");
}

#[test]
fn a_relation_name_reads_as_sql_reads_it() {
    let n = parse_relation_name("\"orders\"", "public").unwrap();
    assert_eq!((n.schema(), n.name()), ("public", "orders"));
    assert!(parse_relation_name("hot oops", "public").is_none());
    assert!(parse_relation_name("a.b.c", "public").is_none());
    assert!(parse_relation_name("", "public").is_none());
}

#[test]
fn an_identifier_reads_unquoted_and_otherwise_as_written() {
    assert_eq!(parse_identifier("\"My T\"").as_deref(), Some("My T"));
    assert_eq!(parse_identifier("Ab").as_deref(), Some("Ab"));
    assert!(parse_identifier("a.b").is_none());
}

#[test]
fn a_schema_name_reads_as_the_schema_of_a_qualified_name() {
    assert_eq!(parse_schema_name("App").as_deref(), Some("app"));
    assert_eq!(parse_schema_name("\"app\"").as_deref(), Some("app"));
    assert_eq!(parse_schema_name("\"table\"").as_deref(), Some("table"));
    assert_eq!(parse_schema_name("_SYSTEM").as_deref(), Some("_system"));
    assert!(parse_schema_name("a.b").is_none());
    assert!(parse_schema_name("").is_none());
}
