use super::*;
use gnitz_core::RelProps;
use gnitz_wire::sys_rows::CatalogColumn;
use gnitz_wire::{ColType, ColumnDef, PkColList, TableProps, TypeCode};

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
    }
}

fn ctx() -> Context {
    Context {
        catalog: Catalog {
            schemas: vec!["public".into(), "sales".into()],
            relations: vec![
                rel(1, "public", "orders", &["id", "customer", "amount"]),
                rel(2, "public", "customers", &["id", "name"]),
                rel(3, "sales", "leads", &["lead_id"]),
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
fn replacement_spans_cover_the_typed_word() {
    let line = "SELECT * FROM sales.le";
    let s = suggest(&ctx(), line, line.len());
    assert_eq!(s[0].span, Span::new(14, line.len()));
}
