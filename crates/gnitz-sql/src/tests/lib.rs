use super::*;

#[test]
fn a_column_is_written_bare_only_when_it_reads_back_as_itself() {
    assert_eq!(column_text("id"), "id");
    assert_eq!(column_text("name"), "name");
    assert_eq!(column_text("a b"), "\"a b\"");
    assert_eq!(column_text("null"), "\"null\"");
    assert_eq!(column_text("true"), "\"true\"");
    assert_eq!(column_text("from"), "\"from\"");
    assert_eq!(column_text("exists"), "\"exists\"");
    assert_eq!(column_text("say \"hi\""), "\"say \"\"hi\"\"\"");
}

#[test]
fn a_relation_part_is_written_bare_only_when_it_reads_back_as_itself() {
    assert_eq!(relation_text("orders"), "orders");
    assert_eq!(relation_text("2024_sales"), "\"2024_sales\"");
    assert_eq!(relation_text("table"), "\"table\"");
    assert_eq!(relation_text("lateral"), "\"lateral\"");
}

#[test]
fn a_relation_name_reads_as_sql_reads_it() {
    let n = parse_relation_name("\"orders\"", "public").unwrap();
    assert_eq!((n.schema(), n.name()), ("public", "orders"));
    let n = parse_relation_name("sales.\"leads\"", "public").unwrap();
    assert_eq!((n.schema(), n.name()), ("sales", "leads"));
    assert!(parse_relation_name("hot oops", "public").is_none());
    assert!(parse_relation_name("a.b.c", "public").is_none());
    assert!(parse_relation_name("", "public").is_none());
}
