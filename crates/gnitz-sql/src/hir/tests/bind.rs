use super::*;
use crate::test_support::{col_def, parse_stmt};
use gnitz_core::{RelClass, RelDescriptor};
use sqlparser::ast::Statement;
use std::sync::Arc;

/// `t(id BIGINT PK, a BIGINT, b BIGINT NULL, f DOUBLE NULL)` and
/// `u(uid BIGINT PK, a BIGINT)` — one snapshot both a linear and a join body
/// bind against.
fn catalog() -> CatalogSnapshot {
    let mut cat = CatalogSnapshot::default();
    let mut add = |tid: u64, name: &str, columns: Vec<ColumnDef>| {
        let schema = Arc::new(Schema {
            columns,
            pk_cols: vec![0],
        });
        cat.insert(
            "public",
            name,
            Some(Arc::new(RelDescriptor {
                tid,
                class: RelClass::Table,
                replicated: false,
                delta: false,
                schema,
                indexes: Arc::new(Vec::new()),
            })),
        );
    };
    add(
        1,
        "t",
        vec![
            col_def("id", TypeCode::I64, false),
            col_def("a", TypeCode::I64, false),
            col_def("b", TypeCode::I64, true),
            col_def("f", TypeCode::F64, true),
        ],
    );
    add(
        2,
        "u",
        vec![col_def("uid", TypeCode::I64, false), col_def("a", TypeCode::I64, false)],
    );
    cat
}

/// Bind one `CREATE VIEW` body and return its output columns.
fn bound_cols(sql: &str) -> Result<Vec<HirCol>, GnitzSqlError> {
    let Statement::CreateView(cv) = parse_stmt(&format!("CREATE VIEW v AS {sql}")) else {
        panic!("not a CREATE VIEW");
    };
    let cat = catalog();
    let mut binder = Binder::new("public");
    let ids = ColIdGen::new();
    let body = crate::validate::reject_query_envelope_body(&cv.query, "view body")?;
    let mut cx = BindCx::new(&cat, &mut binder, &ids);
    bind_body(&mut cx, body).map(|r| r.cols())
}

/// `(name, type, nullable)` of every output column — what a projection item's
/// def declares, and what the view schema publishes.
fn shape(sql: &str) -> Vec<(String, TypeCode, bool)> {
    bound_cols(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|c| (c.def.name.clone(), c.def.type_code, c.def.is_nullable))
        .collect()
}

fn s(name: &str, tc: TypeCode, nullable: bool) -> (String, TypeCode, bool) {
    (name.to_string(), tc, nullable)
}

#[test]
fn a_linear_body_names_and_types_its_projection() {
    assert_eq!(
        shape("SELECT id, b AS bee, a + 1 FROM t"),
        vec![
            s("id", TypeCode::I64, false),
            s("bee", TypeCode::I64, true),
            // A computed column is always declared nullable, never inferred.
            s("_expr2", TypeCode::I64, true),
        ]
    );
    assert_eq!(
        shape("SELECT * FROM t"),
        vec![
            s("id", TypeCode::I64, false),
            s("a", TypeCode::I64, false),
            s("b", TypeCode::I64, true),
            s("f", TypeCode::F64, true),
        ]
    );
}

/// A LEFT join widens the preserved-away side's columns, and the projection
/// resolves against that widened scope.
#[test]
fn a_join_body_projects_the_widened_scope() {
    assert_eq!(
        shape("SELECT t.id, u.a FROM t LEFT JOIN u ON t.a = u.a"),
        vec![s("id", TypeCode::I64, false), s("a", TypeCode::I64, true)]
    );
    assert_eq!(
        shape("SELECT t.id, u.a FROM t RIGHT JOIN u ON t.a = u.a"),
        vec![s("id", TypeCode::I64, true), s("a", TypeCode::I64, false)]
    );
}

/// A derived table's `AS d(col…)` aliases rename its output columns — with and
/// without a GROUP BY over them, which must agree.
#[test]
fn a_derived_tables_positional_aliases_name_the_projection() {
    assert_eq!(
        shape("SELECT x FROM (SELECT a FROM t) AS d(x)"),
        vec![s("x", TypeCode::I64, false)]
    );
    assert_eq!(
        shape("SELECT x, COUNT(*) AS n FROM (SELECT a FROM t) AS d(x) GROUP BY x"),
        vec![s("x", TypeCode::I64, false), s("n", TypeCode::I64, false)]
    );
}

/// A computed group key and a computed aggregate argument both land in the
/// pre-map; the key reads as computed (it has no name the user wrote), the
/// aggregate takes its own default name and typing.
#[test]
fn a_grouped_body_types_computed_keys_and_arguments() {
    assert_eq!(
        shape("SELECT a + 1, SUM(a * 2) FROM t GROUP BY a + 1"),
        // The pre-map column holding `a * 2` is minted nullable (a computed value
        // can be NULL), so the SUM over it carries a COUNT companion and its
        // finalize is nullable.
        vec![s("_expr0", TypeCode::I64, true), s("_sum1", TypeCode::I64, true)]
    );
    // A nullable SUM argument carries a COUNT companion, so the finalize is
    // nullable; COUNT never is.
    assert_eq!(
        shape("SELECT a, SUM(b), COUNT(b) FROM t GROUP BY a"),
        vec![
            s("a", TypeCode::I64, false),
            s("_sum1", TypeCode::I64, true),
            s("_count2", TypeCode::I64, false),
        ]
    );
}

/// AVG renders F64 whatever its argument's type, and is nullable through its
/// COUNT companion.
#[test]
fn avg_renders_f64_and_is_nullable() {
    assert_eq!(
        shape("SELECT AVG(a) AS m, AVG(f) AS n FROM t"),
        vec![s("m", TypeCode::F64, true), s("n", TypeCode::F64, true)]
    );
}

#[test]
fn distinct_and_set_operations_publish_their_projections() {
    assert_eq!(
        shape("SELECT DISTINCT a, b FROM t"),
        vec![s("a", TypeCode::I64, false), s("b", TypeCode::I64, true)]
    );
    // A set operation pairs positionally and takes the left side's names; UNION
    // widens nullability across the pair.
    assert_eq!(
        shape("SELECT a FROM t UNION SELECT b FROM t"),
        vec![s("a", TypeCode::I64, true)]
    );
}

/// A qualifier that names no relation in scope is a rejection, not a silently
/// ignored decoration.
#[test]
fn a_mismatched_qualifier_is_rejected() {
    for sql in [
        "SELECT b.a FROM t",
        "SELECT a FROM t WHERE b.a = 1",
        "SELECT a FROM t AS x WHERE t.a = 1",
    ] {
        assert!(bound_cols(sql).is_err(), "{sql} should not bind");
    }
}
