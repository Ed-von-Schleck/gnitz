//! The window desugar through the whole planner: every accepted shape must
//! bind *and* lower to a segment chain, and every rejection must name the
//! clause the user wrote.

use crate::error::GnitzSqlError;
use crate::hir::{plan_create_view, ViewPlan};
use crate::test_support::{col_def, parse_stmt};
use gnitz_core::{CatalogSnapshot, ColumnDef, RelClass, RelDescriptor, Schema, TypeCode};
use std::sync::Arc;

/// `t(id BIGINT PK, k BIGINT, a BIGINT, b BIGINT NULL, s TEXT, f DOUBLE)`,
/// `u(uid BIGINT PK, k BIGINT)`, and `st`, a stream with `t`'s columns.
fn catalog() -> CatalogSnapshot {
    let mut cat = CatalogSnapshot::default();
    let mut add = |tid: u64, name: &str, class: RelClass, columns: Vec<ColumnDef>| {
        let schema = Arc::new(Schema { columns, pk_cols: vec![0] });
        cat.insert(
            "public",
            name,
            Some(Arc::new(RelDescriptor {
                tid,
                class,
                replicated: false,
                schema,
                indexes: Arc::new(Vec::new()),
            })),
        );
    };
    let t_cols = || {
        vec![
            col_def("id", TypeCode::I64, false),
            col_def("k", TypeCode::I64, false),
            col_def("a", TypeCode::I64, false),
            col_def("b", TypeCode::I64, true),
            col_def("s", TypeCode::String, false),
            col_def("f", TypeCode::F64, false),
        ]
    };
    add(1, "t", RelClass::Table, t_cols());
    add(
        2,
        "u",
        RelClass::Table,
        vec![col_def("uid", TypeCode::I64, false), col_def("k", TypeCode::I64, false)],
    );
    add(3, "st", RelClass::Stream, t_cols());
    cat.insert("public", "v", None);
    cat
}

/// Plan `CREATE VIEW v AS <sql>`: the chain's segment count (the final view
/// included) and the final view's output columns.
fn plan(sql: &str) -> Result<(usize, Vec<ColumnDef>), GnitzSqlError> {
    let sqlparser::ast::Statement::CreateView(cv) = parse_stmt(&format!("CREATE VIEW v AS {sql}")) else {
        panic!("not a CREATE VIEW");
    };
    match plan_create_view(&cv, &catalog(), "public")? {
        ViewPlan::Create { chain, .. } => {
            let cols = chain
                .views
                .last()
                .expect("a chain ends in the view")
                .output_columns
                .clone();
            Ok((chain.views.len(), cols))
        }
        ViewPlan::Skip { .. } => panic!("a free name is never skipped"),
    }
}

fn visible(cols: &[ColumnDef]) -> Vec<(String, TypeCode, bool)> {
    cols.iter()
        .filter(|c| !c.is_hidden)
        .map(|c| (c.name.clone(), c.type_code, c.is_nullable))
        .collect()
}

fn rejects(sql: &str, needle: &str) {
    match plan(sql) {
        Err(GnitzSqlError::Unsupported(m)) | Err(GnitzSqlError::Bind(m)) | Err(GnitzSqlError::Plan(m)) => {
            assert!(
                m.contains(needle),
                "{sql}\n  rejected with {m:?}\n  expected {needle:?}"
            )
        }
        Ok(_) => panic!("{sql}: accepted, expected a rejection naming {needle:?}"),
        Err(e) => panic!("{sql}: {e:?}, expected a rejection naming {needle:?}"),
    }
}

#[test]
fn a_partition_aggregate_over_a_table_reads_it_in_place_and_joins_one_reduce() {
    let (n, cols) = plan("SELECT id, a, SUM(a) OVER (PARTITION BY k) AS total FROM t").unwrap();
    // The reduce is the join's right input, cut to one hidden segment; the
    // table itself is read in place on both sides.
    assert_eq!(n, 2);
    assert_eq!(
        visible(&cols),
        vec![
            ("id".into(), TypeCode::I64, false),
            ("a".into(), TypeCode::I64, false),
            ("total".into(), TypeCode::I64, false),
        ]
    );
}

#[test]
fn a_window_call_types_and_names_like_the_aggregate_it_is() {
    let (_, cols) = plan(
        "SELECT id, COUNT(*) OVER (), SUM(b) OVER (), AVG(a) OVER (), MIN(b) OVER (PARTITION BY k), \
         RANK() OVER (ORDER BY a), ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC) FROM t",
    )
    .unwrap();
    assert_eq!(
        visible(&cols),
        vec![
            ("id".into(), TypeCode::I64, false),
            ("_count1".into(), TypeCode::I64, false),
            ("_sum2".into(), TypeCode::I64, true),
            ("_avg3".into(), TypeCode::F64, false),
            ("_min4".into(), TypeCode::I64, true),
            ("_rank5".into(), TypeCode::I64, false),
            ("_row_number6".into(), TypeCode::I64, false),
        ]
    );
}

#[test]
fn every_cumulative_shape_lowers() {
    for sql in [
        "SELECT id, SUM(a) OVER (PARTITION BY k ORDER BY id) AS running FROM t",
        "SELECT id, SUM(a) OVER (ORDER BY id) AS running FROM t",
        "SELECT id, RANK() OVER (PARTITION BY k ORDER BY a) AS r, DENSE_RANK() OVER (PARTITION BY k ORDER BY a) AS d FROM t",
        "SELECT id, RANK() OVER (ORDER BY a DESC, id) AS r FROM t",
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC) AS rn FROM t",
        "SELECT id, ROW_NUMBER() OVER () AS rn FROM t",
        "SELECT SUM(a) AS s, ROW_NUMBER() OVER () AS rn FROM t",
        "SELECT id, AVG(b) OVER (PARTITION BY k ORDER BY a) AS av, MAX(a) OVER (PARTITION BY k ORDER BY a) AS mx FROM t",
        "SELECT id, COUNT(b) OVER (PARTITION BY k ORDER BY a, s) AS c FROM t",
        "SELECT id, SUM(a) OVER (PARTITION BY k ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t",
        "SELECT id, SUM(a) OVER (PARTITION BY k ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS c FROM t",
    ] {
        plan(sql).unwrap_or_else(|e| panic!("{sql}\n  {e:?}"));
    }
}

#[test]
fn a_where_a_computed_key_and_a_grouped_body_go_through_a_segment() {
    // A WHERE, a computed partition key, a subquery: W is compiled to a segment
    // both reads share.
    for sql in [
        "SELECT id, SUM(a) OVER (PARTITION BY k) AS total FROM t WHERE a > 1",
        "SELECT id, SUM(a) OVER (PARTITION BY k % 10) AS total FROM t",
        "SELECT id, a * 2 AS a2, RANK() OVER (ORDER BY a * 2) AS r FROM t",
        "SELECT id, (SELECT COUNT(*) FROM u WHERE u.k = t.k) AS c, RANK() OVER (ORDER BY a) AS r FROM t",
        "SELECT t.id, u.uid, SUM(t.a) OVER (PARTITION BY u.uid) AS total FROM t JOIN u ON t.k = u.k",
        "SELECT k, SUM(a) AS total, RANK() OVER (ORDER BY SUM(a) DESC) AS r FROM t GROUP BY k",
        "SELECT k, SUM(a) * 100 / SUM(SUM(a)) OVER () AS pct FROM t GROUP BY k",
        "SELECT k, COUNT(*) AS n, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn FROM t GROUP BY k HAVING COUNT(*) > 1",
        "SELECT DISTINCT k, SUM(a) OVER (PARTITION BY k) AS total FROM t",
        "WITH c AS (SELECT id, k, a FROM t WHERE a > 0) SELECT id, RANK() OVER (PARTITION BY k ORDER BY a) AS r FROM c",
        "SELECT * FROM (SELECT id, k, a, RANK() OVER (PARTITION BY k ORDER BY a) AS r FROM t) d WHERE r <= 3",
    ] {
        let (n, _) = plan(sql).unwrap_or_else(|e| panic!("{sql}\n  {e:?}"));
        assert!(n >= 3, "{sql}: {n} segments");
    }
}

#[test]
fn qualify_filters_on_window_values_and_select_aliases() {
    for sql in [
        "SELECT id, k, a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC) = 1",
        "SELECT id, k, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM t QUALIFY r <= 3",
        "SELECT id, k, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM t QUALIFY r <= 3 AND a > 0",
        "SELECT k, SUM(a) AS total FROM t GROUP BY k QUALIFY RANK() OVER (ORDER BY SUM(a) DESC) <= 2",
        "SELECT id, k, a FROM t WINDOW w AS (PARTITION BY k ORDER BY a) QUALIFY RANK() OVER w = 1",
    ] {
        plan(sql).unwrap_or_else(|e| panic!("{sql}\n  {e:?}"));
    }
    let (_, cols) = plan("SELECT id, k FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) = 1").unwrap();
    assert_eq!(
        visible(&cols),
        vec![("id".into(), TypeCode::I64, false), ("k".into(), TypeCode::I64, false)]
    );
}

#[test]
fn named_windows_resolve_through_the_window_clause() {
    plan("SELECT id, RANK() OVER w AS r, SUM(a) OVER w AS s FROM t WINDOW w AS (PARTITION BY k ORDER BY a)").unwrap();
    plan("SELECT id, RANK() OVER w2 AS r FROM t WINDOW w AS (PARTITION BY k ORDER BY a), w2 AS w").unwrap();
    rejects(
        "SELECT id, RANK() OVER w AS r FROM t",
        "not defined in the WINDOW clause",
    );
    rejects(
        "SELECT id, RANK() OVER w AS r FROM t WINDOW w AS w2, w2 AS w",
        "defined in terms of itself",
    );
    rejects(
        "SELECT id, RANK() OVER (w ORDER BY a) AS r FROM t WINDOW w AS (PARTITION BY k)",
        "extending a named window",
    );
}

#[test]
fn the_key_rules_are_stated_at_bind() {
    rejects(
        "SELECT id, SUM(a) OVER (PARTITION BY b) FROM t",
        "window PARTITION BY: the key must be provably NOT NULL",
    );
    rejects(
        "SELECT id, SUM(a) OVER (PARTITION BY f) FROM t",
        "window PARTITION BY: a float-valued expression",
    );
    rejects(
        "SELECT id, RANK() OVER (ORDER BY b) FROM t",
        "window ORDER BY: the key must be provably NOT NULL",
    );
    rejects(
        "SELECT id, RANK() OVER (ORDER BY f) FROM t",
        "window ORDER BY: a float-valued expression",
    );
    rejects(
        "SELECT id, RANK() OVER (ORDER BY s) FROM t",
        "a string cannot be the first ORDER BY key",
    );
    rejects(
        "SELECT id, RANK() OVER (ORDER BY a / k) FROM t",
        "window ORDER BY: the key must be provably NOT NULL",
    );
    // A division by a non-zero literal is never NULL; a string is fine as a hash
    // key and as a residual key.
    plan("SELECT id, RANK() OVER (ORDER BY a / 2) FROM t").unwrap();
    plan("SELECT id, COUNT(*) OVER (PARTITION BY s) FROM t").unwrap();
    plan("SELECT id, RANK() OVER (ORDER BY a, s) FROM t").unwrap();
}

#[test]
fn frames_and_functions_outside_the_supported_set_are_rejected() {
    rejects(
        "SELECT id, SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "ROWS / GROUPS … CURRENT ROW",
    );
    rejects(
        "SELECT id, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        "only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    );
    rejects(
        "SELECT id, RANK() OVER (PARTITION BY k) FROM t",
        "RANK / DENSE_RANK need an ORDER BY",
    );
    rejects(
        "SELECT id, RANK(a) OVER (ORDER BY a) FROM t",
        "RANK: takes no arguments",
    );
    rejects(
        "SELECT id, LAG(a) OVER (ORDER BY a) FROM t",
        "LAG: not supported as a window function",
    );
    // A scalar name with an OVER reads the same at an item's top level, which
    // `call_item` routes, as nested, which `bind_structural` routes.
    rejects(
        "SELECT id, ABS(a) OVER (ORDER BY a) FROM t",
        "ABS: not supported as a window function",
    );
    rejects(
        "SELECT id, ABS(a) OVER (ORDER BY a) + 1 AS x FROM t",
        "ABS: not supported as a window function",
    );
    rejects(
        "SELECT id, SUM(DISTINCT a) OVER () FROM t",
        "DISTINCT: not supported on window functions",
    );
    rejects("SELECT id, SUM(a) FILTER (WHERE a > 1) OVER () FROM t", "FILTER");
    rejects("SELECT id, SUM(s) OVER () FROM t", "SUM: not supported on String");
}

#[test]
fn a_window_call_belongs_to_the_select_list_and_qualify_only() {
    rejects(
        "SELECT id FROM t WHERE SUM(a) OVER () > 1",
        "only supported in the SELECT list and QUALIFY",
    );
    rejects(
        "SELECT k FROM t GROUP BY k HAVING RANK() OVER (ORDER BY k) = 1",
        "only supported in the SELECT list and QUALIFY",
    );
    rejects(
        "SELECT id, SUM(RANK() OVER (ORDER BY a)) OVER () FROM t",
        "cannot be nested",
    );
    rejects(
        "SELECT id, SUM(a) OVER (PARTITION BY RANK() OVER (ORDER BY a)) FROM t",
        "cannot be nested",
    );
    // A bare `EXISTS (SELECT …)` ignores its select list by definition, so a
    // window call written there is never bound — the body still compiles.
    plan("SELECT t.id FROM t WHERE EXISTS (SELECT ROW_NUMBER() OVER (ORDER BY u.k) FROM u WHERE u.k = t.k)").unwrap();
    rejects("SELECT id, k FROM t QUALIFY a > 1", "QUALIFY needs a window function");
    rejects(
        "SELECT k FROM t GROUP BY k QUALIFY k > 1",
        "QUALIFY needs a window function",
    );
}

#[test]
fn row_number_needs_a_unique_row_key() {
    rejects(
        "SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.a) FROM t JOIN u ON t.k = u.k",
        "ROW_NUMBER needs an input with a unique row key",
    );
    rejects(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY a) FROM st",
        "ROW_NUMBER needs an input with a unique row key",
    );
    // RANK is fine over both.
    plan("SELECT t.id, RANK() OVER (ORDER BY t.a) FROM t JOIN u ON t.k = u.k").unwrap();
    // A join equating the right side's key matches each left row at most once, and
    // a decorrelated EXISTS emits each outer row at most once: both keep the left key.
    plan("SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.a) FROM t JOIN u ON t.k = u.uid").unwrap();
    plan(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM \
         (SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.k)) d",
    )
    .unwrap();
    plan("SELECT id, RANK() OVER (ORDER BY a) FROM st").unwrap();
    // A derived table that drops the key loses it; one that keeps it keeps it.
    rejects(
        "SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM (SELECT a, k FROM t) d",
        "ROW_NUMBER needs an input with a unique row key",
    );
    plan("SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM (SELECT id, a, k FROM t) d").unwrap();
}

#[test]
fn a_subquery_body_and_a_cte_pass_through_keep_their_own_rules() {
    // A QUALIFY inside a scalar subquery is that surface's rejection, not a
    // silent drop.
    rejects(
        "SELECT id, (SELECT COUNT(*) FROM u WHERE u.k = t.k QUALIFY RANK() OVER (ORDER BY uid) = 1) AS c FROM t",
        "QUALIFY",
    );
    // A CTE whose body is windowed compiles rather than aliasing the source.
    let (n, _) = plan(
        "WITH c AS (SELECT id, k FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) = 1) SELECT id FROM c",
    )
    .unwrap();
    assert!(n >= 3);
}

/// A windowed MIN/MAX selects a row, so a TEXT argument survives the desugar's
/// join and reduce at its source type: the join-key rules bind the partition and
/// order keys, not the aggregate's own argument.
#[test]
fn a_windowed_extreme_carries_a_string_argument() {
    for sql in [
        "SELECT id, MIN(s) OVER () AS m FROM t",
        "SELECT id, MIN(s) OVER (PARTITION BY k) AS m FROM t",
        "SELECT id, MAX(s) OVER (PARTITION BY k) AS m FROM t",
    ] {
        let (_, cols) = plan(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(
            visible(&cols),
            vec![
                ("id".to_string(), TypeCode::I64, false),
                ("m".to_string(), TypeCode::String, false)
            ],
            "{sql}"
        );
    }
}
