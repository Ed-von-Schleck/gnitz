use super::*;
use std::num::NonZeroU64;

use gnitz_core::{IndexRow, RelProps};
use gnitz_wire::{CatalogColumn, ColType, ColumnDef, PkColList, TableProps, TypeCode, ViewBudget, ViewProps};

fn rel(id: u64, schema: &str, name: &str, cols: &[&str]) -> CatalogRelation {
    CatalogRelation {
        id,
        schema: schema.into(),
        name: name.into(),
        props: RelProps::Table(TableProps::default()),
        columns: cols
            .iter()
            .map(|c| CatalogColumn {
                def: ColumnDef::typed(c.to_string(), ColType::of(TypeCode::I64), true),
                fk: None,
            })
            .collect(),
        pk: PkColList::from_slice(&[0]),
        indexes: vec![],
        reads: vec![],
        definition: vec![],
    }
}

fn ctx() -> Context {
    Context {
        catalog: Catalog {
            schemas: vec!["public".into(), "sales".into()],
            relations: vec![
                CatalogRelation {
                    indexes: vec![IndexRow {
                        owner: 1,
                        name: "orders_customer".into(),
                        cols: PkColList::from_slice(&[1]),
                        is_unique: false,
                    }],
                    ..rel(1, "public", "orders", &["id", "customer", "amount"])
                },
                rel(2, "public", "customers", &["id", "name"]),
                rel(3, "sales", "leads", &["lead_id"]),
                CatalogRelation {
                    props: RelProps::View(ViewProps {
                        budget: ViewBudget::Fed {
                            delta_bytes: NonZeroU64::new(1 << 20).unwrap(),
                        },
                        compress: false,
                    }),
                    ..rel(4, "public", "feed", &["id", "total"])
                },
            ],
        },
        schema: "public".into(),
    }
}

fn values(line: &str) -> Vec<String> {
    suggest(&ctx(), line, line.len()).into_iter().map(|s| s.value).collect()
}

#[test]
fn after_from_come_relations_and_schemas() {
    let v = values("SELECT * FROM ord");
    assert_eq!(v[0], "orders");
    let v = values("SELECT * FROM ");
    assert!(
        v.contains(&"customers".to_string())
            && v.contains(&"sales.leads".to_string())
            && v.contains(&"sales.".to_string())
    );
}

#[test]
fn a_schema_qualifier_lists_its_relations() {
    assert_eq!(values("SELECT * FROM sales."), vec!["sales.leads"]);
}

#[test]
fn an_alias_qualifier_lists_its_columns() {
    let line = "SELECT o. FROM orders AS o";
    let v: Vec<String> = suggest(&ctx(), line, 9).into_iter().map(|s| s.value).collect();
    assert_eq!(v, vec!["o.amount", "o.customer", "o.id"]);
}

#[test]
fn columns_of_named_relations_rank_before_keywords() {
    let line = "SELECT n FROM customers";
    let v: Vec<String> = suggest(&ctx(), line, 8).into_iter().map(|s| s.value).collect();
    assert_eq!(v[0], "name");
}

#[test]
fn keywords_follow_the_case_being_typed() {
    assert!(values("sel").contains(&"select".to_string()));
    assert!(values("SEL").contains(&"SELECT".to_string()));
}

#[test]
fn functions_are_offered() {
    assert!(values("SELECT coal").contains(&"coalesce".to_string()));
    assert!(values("SELECT ROW_").contains(&"ROW_NUMBER".to_string()));
}

#[test]
fn meta_commands_and_their_arguments_complete() {
    let v = values("\\d");
    assert!(v.contains(&"\\dt".to_string()) && v.contains(&"\\d".to_string()));
    assert_eq!(values("\\pset form"), vec!["format"]);
    assert_eq!(values("\\x a"), vec!["auto"]);
    assert_eq!(values("\\use sa"), vec!["sales"]);
    assert_eq!(values("\\d cust"), vec!["customers"]);
}

#[test]
fn a_pset_value_completes_from_the_option_it_follows() {
    assert_eq!(values("\\pset footer "), vec!["off", "on"]);
    assert_eq!(values("\\pset format "), vec!["aligned", "csv", "json", "unaligned"]);
    assert!(values("\\pset null ").is_empty());
    assert_eq!(values("\\timing "), vec!["off", "on"]);
}

#[test]
fn di_completes_index_names() {
    assert_eq!(values("\\di ord"), vec!["orders_customer"]);
}

#[test]
fn watch_completes_delta_fed_views_and_sql_over_them() {
    assert!(values("\\watch ord").is_empty());
    assert_eq!(values("\\watch SELECT * FROM fe"), vec!["feed"]);
    assert_eq!(values("\\watch "), vec!["feed", "SELECT"]);
}

#[test]
fn replacement_spans_cover_the_typed_word() {
    let line = "SELECT * FROM sales.le";
    let s = suggest(&ctx(), line, line.len());
    assert_eq!(s[0].span, Span::new(14, line.len()));
}

#[test]
fn a_cursor_in_the_command_word_completes_command_names() {
    let v = values("\\d+");
    assert!(!v.iter().any(|s| s.contains("orders")), "{v:?}");
    assert_eq!(values("\\d+ cust"), vec!["customers"]);
}

#[test]
fn a_meta_argument_starts_past_any_whitespace() {
    assert_eq!(values("\\d\u{a0}cu"), vec!["customers"]);
    suggest(&ctx(), " \\d", 0);
}

#[test]
fn a_relation_is_qualified_only_by_the_name_the_binder_reads() {
    assert!(values("SELECT * FROM orders AS o WHERE orders.").is_empty());
    assert_eq!(
        values("SELECT * FROM orders source WHERE source."),
        vec!["source.amount", "source.customer", "source.id"]
    );
    assert!(values("SELECT sales.leads.").is_empty());
    assert_eq!(
        values("SELECT * FROM orders o, customers c WHERE c."),
        vec!["c.id", "c.name"]
    );
    assert!(values("DROP TABLE IF EXISTS ord").contains(&"orders".to_string()));
}

#[test]
fn only_the_statement_under_the_cursor_names_relations() {
    assert!(values("SELECT * FROM orders o; SELECT o.").is_empty());
    assert!(!values("SELECT 1; ").iter().any(|v| v.starts_with('\\')));
}

#[test]
fn a_derived_table_leaves_the_items_after_it() {
    let line = "SELECT o. FROM (SELECT * FROM customers) d, orders o";
    let v: Vec<String> = suggest(&ctx(), line, 9).into_iter().map(|s| s.value).collect();
    assert_eq!(v, vec!["o.amount", "o.customer", "o.id"]);
}

#[test]
fn relation_arguments_list_their_command_s_classes() {
    let dt = values("\\dt ");
    assert!(dt.contains(&"orders".to_string()) && !dt.contains(&"feed".to_string()));
    assert_eq!(values("\\dv "), vec!["feed"]);
    assert_eq!(values("\\d sales."), vec!["sales.leads"]);
}

#[test]
fn types_and_keyword_functions_come_from_the_planner() {
    assert!(values("CREATE TABLE t (a BIGINT UNS").contains(&"UNSIGNED".to_string()));
    assert!(values("CREATE TABLE t (a SMALLS").contains(&"SMALLSERIAL".to_string()));
    assert!(values("SELECT flo").contains(&"floor".to_string()));
}

#[test]
fn a_path_lists_its_directory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.sql"), "").unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    let typed = format!("{}/", dir.path().display());
    let s = suggest(&ctx(), &format!("\\i {typed}"), typed.len() + 3);
    let v: Vec<(String, bool)> = s.into_iter().map(|s| (s.value, s.append_whitespace)).collect();
    assert_eq!(
        v,
        vec![(format!("{typed}a.sql"), true), (format!("{typed}sub/"), false)]
    );
}

#[test]
fn nothing_is_offered_inside_a_literal_a_comment_or_a_number() {
    for line in [
        "INSERT INTO t VALUES ('ord",
        "-- sel",
        "/* sel",
        "SELECT * FROM orders LIMIT 10",
        "SELECT * FROM sales.\"ord",
    ] {
        assert!(values(line).is_empty(), "{line}: {:?}", values(line));
    }
    assert!(values("-- c\nsel").contains(&"select".to_string()));
    assert_eq!(values("SELECT * FROM \"ord")[0], "orders");
}

#[test]
fn a_qualifier_the_binder_refuses_names_no_relation() {
    let ctx = Context {
        catalog: Catalog {
            relations: vec![rel(9, "public", "sales.leads", &["x"])],
            ..ctx().catalog
        },
        ..ctx()
    };
    let line = "SELECT * FROM \"sales.leads\" x WHERE x.";
    assert!(suggest(&ctx, line, line.len()).is_empty());
}

#[test]
fn a_name_is_written_as_sql_reads_it_back() {
    let ctx = Context {
        catalog: Catalog {
            relations: vec![
                rel(1, "public", "t", &["id", "name", "a b", "null", "from"]),
                rel(2, "public", "2024_sales", &["id"]),
            ],
            ..ctx().catalog
        },
        ..ctx()
    };
    let line = "SELECT  FROM t";
    let v: Vec<String> = suggest(&ctx, line, 7).into_iter().map(|s| s.value).collect();
    for c in ["id", "name", "\"a b\"", "\"null\"", "\"from\""] {
        assert!(v.contains(&c.to_string()), "{c}: {v:?}");
    }
    let line = "SELECT * FROM 20";
    let v: Vec<String> = suggest(&ctx, line, line.len()).into_iter().map(|s| s.value).collect();
    assert!(v.is_empty(), "a number is no word: {v:?}");
    let line = "SELECT * FROM ";
    let v: Vec<String> = suggest(&ctx, line, line.len()).into_iter().map(|s| s.value).collect();
    assert!(v.contains(&"\"2024_sales\"".to_string()), "{v:?}");
    let line = "SELECT a FROM t";
    let v: Vec<String> = suggest(&ctx, line, 8).into_iter().map(|s| s.value).collect();
    assert!(v.contains(&"\"a b\"".to_string()), "{v:?}");
}

#[test]
fn quantifiers_are_offered() {
    assert!(values("SELECT * FROM orders WHERE id = AN").contains(&"ANY".to_string()));
    assert!(values("SELECT * FROM orders WHERE id = SOM").contains(&"SOME".to_string()));
}
