use super::*;
use crate::test_support::{col, ncol, parse_stmt, table};
use gnitz_wire::TypeCode;
use sqlparser::ast::Statement;
use std::rc::Rc;

/// `t(id BIGINT PK, a BIGINT, b BIGINT NULL, f DOUBLE NULL)` and
/// `u(uid BIGINT PK, a BIGINT)` — one catalog both a linear and a join body
/// bind against.
fn catalog() -> Catalog<'static> {
    let i = TypeCode::I64;
    crate::test_support::catalog(vec![
        (
            "t",
            table(
                1,
                vec![col("id", i), col("a", i), ncol("b", i), ncol("f", TypeCode::F64)],
                vec![0],
            ),
        ),
        ("u", table(2, vec![col("uid", i), col("a", i)], vec![0])),
    ])
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
    let mut cx = BindCx::new(&cat, &ids, view);
    bind_body(&mut cx, body)
}

/// `(name, type, nullable)` of every output column — what a projection item's
/// def declares, and what the view schema publishes.
fn shape(sql: &str) -> Vec<(String, TypeCode, bool)> {
    bound(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .cols()
        .iter()
        .map(|c| (c.def.name.clone(), c.def.ty.tc, c.def.is_nullable))
        .collect()
}

fn s(name: &str, tc: TypeCode, nullable: bool) -> (String, TypeCode, bool) {
    (name.to_string(), tc, nullable)
}

/// A RIGHT join widens the left side's columns, and the projection resolves
/// against that widened scope.
#[test]
fn a_right_join_body_projects_the_widened_scope() {
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
}

/// The bound body's reduce, under its HAVING filter if any: its input, and the
/// physical op of each aggregate column.
fn reduce_of(sql: &str) -> (Rc<RelExpr>, Vec<gnitz_wire::AggFunc>) {
    let rel = bound(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let RelExpr::Project { input, .. } = rel.as_ref() else {
        panic!("{sql}: no projection")
    };
    let input = match input.as_ref() {
        RelExpr::Filter { input, .. } => input,
        _ => input,
    };
    let RelExpr::Reduce { input, aggs, .. } = input.as_ref() else {
        panic!("{sql}: no reduce")
    };
    (Rc::clone(input), aggs.iter().map(|c| c.op).collect())
}

/// A DISTINCT aggregate is the plain aggregate over `Distinct(group cols, arg)`;
/// every DISTINCT aggregate of one body rides that one set, HAVING's included.
/// `MIN`/`MAX(DISTINCT x)` is `MIN`/`MAX(x)`, so it needs no set, and neither
/// does an argument that keeps the source's PK. Each row names the projection
/// under the `Distinct` (`<computed>` for a materialized column), or `None`
/// when the reduce reads its input directly.
#[test]
fn a_distinct_aggregate_reduces_over_one_distinct_input() {
    for (sql, want) in [
        ("SELECT a, COUNT(DISTINCT b) FROM t GROUP BY a", Some(&["a", "b"][..])),
        ("SELECT COUNT(DISTINCT b) FROM t", Some(&["b"])),
        (
            "SELECT COUNT(DISTINCT a + 1) FROM t GROUP BY b",
            Some(&["b", "<computed>"]),
        ),
        (
            "SELECT a, COUNT(DISTINCT b), SUM(DISTINCT b), MAX(DISTINCT b) FROM t GROUP BY a \
             HAVING COUNT(DISTINCT b) > 1",
            Some(&["a", "b"]),
        ),
        ("SELECT a, MAX(DISTINCT b) FROM t GROUP BY a", None),
        ("SELECT MIN(DISTINCT b), MAX(DISTINCT a) FROM t", None),
        // Keeps company the all-or-nothing DISTINCT rule would otherwise refuse.
        ("SELECT a, MAX(DISTINCT b), COUNT(*) FROM t GROUP BY a", None),
        // A float argument is no key here, because nothing hashes it.
        ("SELECT MIN(DISTINCT f) FROM t", None),
        ("SELECT a, COUNT(DISTINCT id) FROM t GROUP BY a", None),
    ] {
        let (input, _) = reduce_of(sql);
        let got = match input.as_ref() {
            RelExpr::Distinct { input } => {
                let RelExpr::Project { items, .. } = input.as_ref() else {
                    panic!("{sql}: no distinct projection")
                };
                let names = items.iter().map(|e| match e.out.def.is_hidden {
                    true => "<computed>",
                    false => e.out.def.name.as_str(),
                });
                Some(names.collect::<Vec<_>>())
            }
            _ => None,
        };
        assert_eq!(got.as_deref(), want, "{sql}");
    }
}

/// Aggregates of one reduce computing the same op over the same argument share
/// one physical column: over a nullable `b`, SUM's companion, AVG's two columns
/// and COUNT(b) are the one `Sum(b)` and the one `CountNonNull(b)`; a MAX with
/// and without an inert DISTINCT is one `Max(b)`.
#[test]
fn aggregates_of_one_reduce_share_their_physical_columns() {
    use gnitz_wire::AggFunc::{CountNonNull, Max, Sum};
    for (sql, want) in [
        (
            "SELECT SUM(b), AVG(b), COUNT(b) FROM t GROUP BY a",
            &[Sum, CountNonNull][..],
        ),
        ("SELECT a, MAX(DISTINCT b), MAX(b) FROM t GROUP BY a", &[Max]),
    ] {
        assert_eq!(reduce_of(sql).1, want, "{sql}");
    }
}
