use super::*;
use crate::test_support::{col_def, parse_stmt};
use gnitz_core::{RelClass, RelDescriptor, TypeCode};
use sqlparser::ast::Statement;
use std::rc::Rc;
use std::sync::Arc;

/// `t(id BIGINT PK, a BIGINT, b BIGINT NULL, f DOUBLE NULL)` and
/// `u(uid BIGINT PK, a BIGINT)` — one snapshot both a linear and a join body
/// bind against.
fn catalog() -> CatalogSnapshot {
    let mut cat = CatalogSnapshot::default();
    let mut add = |tid: u64, name: &str, columns: Vec<ColumnDef>| {
        let schema = Arc::new(Schema { columns, pk_cols: vec![0] });
        cat.insert(
            "public",
            name,
            Some(Arc::new(RelDescriptor {
                tid,
                class: RelClass::Table,
                replicated: false,
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

/// Bind one `CREATE VIEW` body.
pub(in crate::hir) fn bound(sql: &str) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let Statement::CreateView(cv) = parse_stmt(&format!("CREATE VIEW v AS {sql}")) else {
        panic!("not a CREATE VIEW");
    };
    let cat = catalog();
    let ids = ColIdGen::new();
    let body = crate::validate::reject_query_envelope_body(&cv.query, "view body")?;
    let view = crate::hir::bind::ViewBody { stmt: "CREATE VIEW", replacing: None };
    let mut cx = BindCx::new(&cat, "public", &ids, view);
    bind_body(&mut cx, body)
}

/// Bind one `CREATE VIEW` body and return its output columns.
fn bound_cols(sql: &str) -> Result<Vec<HirCol>, GnitzSqlError> {
    bound(sql).map(|r| r.cols())
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

/// The projection under the reduce's `Distinct`, naming a materialized column
/// `<computed>`, or `None` when the reduce reads its input directly.
fn reduce_over_distinct(sql: &str) -> Option<Vec<String>> {
    let rel = bound(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let RelExpr::Project { input, .. } = rel.as_ref() else {
        panic!("{sql}: no projection")
    };
    let RelExpr::Reduce { input, .. } = input.as_ref() else {
        panic!("{sql}: no reduce")
    };
    let RelExpr::Distinct { input } = input.as_ref() else {
        return None;
    };
    let RelExpr::Project { items, .. } = input.as_ref() else {
        panic!("{sql}: no distinct projection")
    };
    Some(
        items
            .iter()
            .map(|e| match e.out.def.is_hidden {
                true => "<computed>".to_string(),
                false => e.out.def.name.clone(),
            })
            .collect(),
    )
}

/// A DISTINCT aggregate is the plain aggregate over `Distinct(group cols, arg)`,
/// whose projection carries exactly the group columns and the argument.
#[test]
fn a_distinct_aggregate_reduces_over_a_distinct_input() {
    let names = |sql: &str| reduce_over_distinct(sql).unwrap_or_else(|| panic!("{sql}: no distinct"));
    assert_eq!(names("SELECT a, COUNT(DISTINCT b) FROM t GROUP BY a"), ["a", "b"]);
    assert_eq!(names("SELECT COUNT(DISTINCT b) FROM t"), ["b"]);
    assert_eq!(
        names("SELECT COUNT(DISTINCT a + 1) FROM t GROUP BY b"),
        ["b", "<computed>"]
    );
    // A group column that is also the argument is carried once.
    assert_eq!(names("SELECT a, COUNT(DISTINCT a) FROM t GROUP BY a"), ["a"]);
}

/// Every DISTINCT aggregate of one body rides that one distinct set, and a
/// MIN/MAX of its argument may ride it too.
#[test]
fn distinct_aggregates_of_one_argument_share_the_set() {
    assert_eq!(
        reduce_over_distinct("SELECT a, COUNT(DISTINCT b), SUM(DISTINCT b), MAX(DISTINCT b) FROM t GROUP BY a"),
        Some(vec!["a".to_string(), "b".to_string()])
    );
    assert_eq!(
        shape(
            "SELECT a, COUNT(DISTINCT b) AS n, SUM(DISTINCT b) AS s, MAX(DISTINCT b) AS m FROM t GROUP BY a \
             HAVING COUNT(DISTINCT b) > 1"
        ),
        vec![
            s("a", TypeCode::I64, false),
            s("n", TypeCode::I64, false),
            s("s", TypeCode::I64, true),
            s("m", TypeCode::I64, true),
        ]
    );
}

/// `MIN`/`MAX(DISTINCT x)` is `MIN`/`MAX(x)`, so the binder drops the qualifier:
/// such a body plans as its unqualified twin, and keeps company the
/// all-or-nothing DISTINCT rule would otherwise refuse.
#[test]
fn an_inert_distinct_on_min_max_is_dropped() {
    for sql in [
        "SELECT a, MAX(DISTINCT b) FROM t GROUP BY a",
        "SELECT MIN(DISTINCT b), MAX(DISTINCT a) FROM t",
        "SELECT a, MAX(DISTINCT b), COUNT(*) FROM t GROUP BY a",
        // A float argument is no key here, because nothing hashes it.
        "SELECT MIN(DISTINCT f) FROM t",
    ] {
        assert_eq!(reduce_over_distinct(sql), None, "{sql} should need no distinct set");
    }
    // The qualified and unqualified spellings are one aggregate, not two.
    assert_eq!(
        shape("SELECT a, MAX(DISTINCT b) AS m1, MAX(b) AS m2 FROM t GROUP BY a"),
        vec![
            s("a", TypeCode::I64, false),
            s("m1", TypeCode::I64, true),
            s("m2", TypeCode::I64, true),
        ]
    );
}

#[test]
fn a_distinct_aggregate_that_cannot_share_one_distinct_set_is_rejected() {
    for (sql, needle) in [
        ("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t", "same argument"),
        ("SELECT COUNT(DISTINCT a), COUNT(*) FROM t", "plain aggregate"),
        ("SELECT COUNT(DISTINCT a), SUM(a) FROM t", "plain aggregate"),
        ("SELECT COUNT(DISTINCT a), COUNT(a) FROM t", "plain aggregate"),
        // MIN/MAX rides the set only for the argument the set is built from.
        ("SELECT COUNT(DISTINCT a), MAX(b) FROM t", "plain aggregate"),
        ("SELECT COUNT(DISTINCT f) FROM t", "cannot be a key"),
        ("SELECT COUNT(DISTINCT *) FROM t", "needs a column argument"),
    ] {
        match bound_cols(sql).map(|_| ()) {
            Err(GnitzSqlError::Unsupported(m)) => assert!(m.contains(needle), "{sql}: {m}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

/// Aggregates of one reduce computing the same op over the same argument share
/// one physical column: over a nullable `b`, SUM's companion, AVG's two columns
/// and COUNT(b) are the one `Sum(b)` and the one `CountNonNull(b)`.
#[test]
fn aggregates_of_one_reduce_share_their_physical_columns() {
    let rel = bound("SELECT SUM(b), AVG(b), COUNT(b) FROM t GROUP BY a").unwrap();
    let RelExpr::Project { input, .. } = rel.as_ref() else {
        panic!("no projection")
    };
    let RelExpr::Reduce { aggs, .. } = input.as_ref() else {
        panic!("no reduce")
    };
    assert_eq!(aggs.len(), 3);
    let mut distinct: Vec<&crate::hir::AggCol> = Vec::new();
    for c in aggs.iter().flat_map(HirAgg::cols) {
        if !distinct.iter().any(|d| d.col.id == c.col.id) {
            distinct.push(c);
        }
    }
    let ops: Vec<_> = distinct.iter().map(|c| c.op).collect();
    assert_eq!(ops, [gnitz_wire::AggFunc::Sum, gnitz_wire::AggFunc::CountNonNull]);
}
