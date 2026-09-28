//! The window desugar through the whole planner: every accepted shape must
//! bind *and* lower to a segment chain, and every rejection must name the
//! clause the user wrote.

use crate::bind::Catalog;
use crate::error::GnitzSqlError;
use crate::hir::plan_create_view;
use crate::test_support::{col, ncol, parse_stmt, register, rel, table};
use gnitz_core::{ColumnDef, PlannedView, RelClass, TypeCode};
use std::sync::Arc;

/// `t(id BIGINT PK, k BIGINT, a BIGINT, b BIGINT NULL, s TEXT, f DOUBLE)`,
/// `u(uid BIGINT PK, k BIGINT)`, and `st`, a stream with `t`'s columns.
fn catalog() -> Catalog<'static> {
    let i = TypeCode::I64;
    let t_cols = || {
        vec![
            col("id", i),
            col("k", i),
            col("a", i),
            ncol("b", i),
            col("s", TypeCode::String),
            col("f", TypeCode::F64),
        ]
    };
    crate::test_support::catalog(vec![
        ("t", table(1, t_cols(), vec![0])),
        ("u", table(2, vec![col("uid", i), col("k", i)], vec![0])),
        // A stream's PK is a routing and sort key, never unique.
        ("st", rel(3, RelClass::Stream, t_cols(), vec![0], &[])),
    ])
}

/// Plan `CREATE VIEW v AS <sql>`: the chain's segment count (the final view
/// included) and the final view's output columns.
fn plan(sql: &str) -> Result<(usize, Vec<ColumnDef>), GnitzSqlError> {
    let (n, last) = plan_in(&catalog(), sql)?;
    Ok((n, Arc::unwrap_or_clone(last.schema).columns))
}

/// [`plan`] against `cat`, returning the final view whole.
fn plan_in(cat: &Catalog<'_>, sql: &str) -> Result<(usize, PlannedView), GnitzSqlError> {
    let sqlparser::ast::Statement::CreateView(cv) = parse_stmt(&format!("CREATE VIEW v AS {sql}")) else {
        panic!("not a CREATE VIEW");
    };
    let chain = plan_create_view(&cv, cat)?.expect("a free name is never skipped");
    Ok((chain.bundle.segments.len() + 1, chain.bundle.view))
}

fn visible(cols: &[ColumnDef]) -> Vec<(String, TypeCode, bool)> {
    cols.iter()
        .filter(|c| !c.is_hidden)
        .map(|c| (c.name.clone(), c.ty.tc, c.is_nullable))
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
    rejects("SELECT id, SUM(s) OVER () FROM t", "SUM: not supported on STRING");
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

/// `ROW_NUMBER` reads the row key off the relation, so a view whose own lowering
/// said its PK repeats loses it: over a stream, over a join key, or over a
/// partition holding more than one slot.
#[test]
fn row_number_reads_the_row_key_off_the_view_that_states_it() {
    let cat = catalog();
    // Plan `sql` as a view body and register it under `name`, carrying the
    // `pk_repeats` its own lowering stated. Returns that bit.
    let reg = |tid, name, sql: &str| {
        let (_, v) = plan_in(&cat, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        register(&cat, name, tid, RelClass::View, &v);
        v.pk_repeats
    };
    assert!(reg(20, "sv", "SELECT id, a FROM st"));
    assert!(reg(21, "jv2", "SELECT t.id AS id, t.a AS a FROM t JOIN u ON t.k = u.k"));
    assert!(reg(22, "tv2", "SELECT id, a FROM t ORDER BY a LIMIT 2"));
    assert!(!reg(23, "pv", "SELECT id, a FROM t"));
    assert!(!reg(24, "tv1", "SELECT id, a FROM t ORDER BY a LIMIT 1"));

    let over = |cat: &Catalog<'_>, name: &str| {
        plan_in(
            cat,
            &format!("SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM {name}"),
        )
    };
    let refusal = |cat: &Catalog<'_>, name: &str| {
        let Err(err) = over(cat, name) else {
            panic!("{name}: ROW_NUMBER must be refused");
        };
        format!("{err:?}")
    };
    for name in ["sv", "jv2", "tv2"] {
        let err = refusal(&cat, name);
        assert!(err.contains("unique row key"), "{name}: {err}");
    }
    over(&cat, "pv").unwrap_or_else(|e| panic!("pv: {e:?}"));
    // `tv1` keeps its row key — the refusal names its `_group_pk`'s width, not
    // the key.
    let err = refusal(&cat, "tv1");
    assert!(err.contains("128-bit columns"), "tv1: {err}");
}

/// The relation states whether its PK repeats; no key column's *name* is read.
/// So a user may name a key column `_join_pk`, over a table or through a view's
/// alias, and keep its row key.
#[test]
fn a_user_named_join_pk_column_is_a_row_key() {
    let cat = catalog();
    let jt_cols = vec![col("_join_pk", TypeCode::I64), col("a", TypeCode::I64)];
    cat.insert("jt", Some(table(10, jt_cols, vec![0])));
    plan_in(&cat, "SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM jt").unwrap();

    let (_, aliased) = plan_in(&cat, "SELECT id AS _join_pk, a FROM t").unwrap();
    let pk = aliased
        .schema
        .pk_cols
        .iter()
        .map(|&i| &aliased.schema.columns[i as usize]);
    assert!(pk.clone().all(|c| !c.is_hidden) && pk.clone().any(|c| c.name == "_join_pk"));
    // Carrying the bit that view's own lowering stated — a projection passing a
    // table's PK through.
    assert!(!aliased.pk_repeats);
    register(&cat, "jv", 11, RelClass::View, &aliased);
    plan_in(&cat, "SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM jv").unwrap();
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
