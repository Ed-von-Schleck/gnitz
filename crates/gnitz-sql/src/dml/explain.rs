//! `EXPLAIN <select>`: the access decisions the ad-hoc read path makes, rendered
//! as rows. It runs the *identical* planning `dml::select` runs — the shared
//! `route_select` and the same two shape builders — and diverges only at the tail:
//! `execute_select` dispatches, this formats. So a query EXPLAIN describes is a
//! query the direct path can serve, and a query the planner rejects returns that
//! query's own rejection.
//!
//! Metadata lookups only: planning reaches the wire where it always does
//! (`binder.resolve` and the `plan_where` index probe) and never scans.

use crate::access::pk_point_tuple;
use crate::bind::Binder;
use crate::dml::plan::{bind_where, plan_where, AccessPlan, ReadBudget};
use crate::dml::select::{build_fold_shape, build_rows_shape, route_select, FoldShape, Route, Sink, Target};
use crate::error::GnitzSqlError;
use crate::SqlResult;
use gnitz_core::{BatchAppender, ColumnDef, GnitzClient, Schema, TypeCode, ZSetBatch};
use gnitz_wire::{AggFunc, ReadBound};
use sqlparser::ast::Query;

/// Describe the plan for `query` without running it.
pub(crate) fn execute_explain(
    client: &mut GnitzClient,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let route = route_select(client, query, binder)?;
    let (target, select) = (&route.target, route.select);
    let schema = &*target.schema;

    // The same WHERE → plan the tails build. A route with no WHERE short-circuits
    // inside `bound_and_predicate` without an index probe, so this stays free.
    let bound_where = bind_where(schema, select.selection.as_ref())?;
    let plan = plan_where(client, target.tid, schema, bound_where.as_ref(), ReadBudget::OneRequest)?;

    // Line 4 is the sink's own shape: what the fold accumulates, or how wide the
    // projected reply is.
    let shape_line = match route.sink {
        Sink::Fold => fold_line(&build_fold_shape(select, schema)?, schema, select.distinct.is_some()),
        _ => {
            let reply = build_rows_shape(select, query, schema)?.reply_schema;
            // The hidden columns `resolve_read_spec_order` appended to order by a
            // non-projected source column. The prepended source PK is hidden too,
            // but is a PK column rather than a payload one.
            let extra = (0..reply.columns.len()).filter(|&i| reply.is_hidden_payload(i)).count();
            projection_line(reply.visible_columns().count(), extra)
        }
    };

    let facts = order_limit_facts(&route, query.order_by.is_some());
    Ok(plan_rows(&[
        read_line(target),
        format!("access: {}", access(&plan, schema, &route.sink)),
        format!(
            "predicate: {}",
            if plan.has_predicate() { "server-side" } else { "none" }
        ),
        shape_line,
        if facts.is_empty() {
            "order/limit: none".to_string()
        } else {
            format!("order/limit: {}", facts.join(", "))
        },
    ]))
}

/// Where the ORDER BY / LIMIT / OFFSET work happens. Only the rows sink pushes
/// anything down; all fold finishing is client-side.
fn order_limit_facts(route: &Route<'_>, has_order: bool) -> Vec<String> {
    // Both tails short-circuit `LIMIT 0` to an empty result before dispatching.
    if route.limit == Some(0) {
        return vec!["no request (LIMIT 0)".to_string()];
    }
    let mut facts = Vec::new();
    // The per-worker cut is OFFSET+LIMIT deep, because the client windows. With no
    // ORDER BY keys the same wire field just stops the worker early.
    if let (Sink::Rows, Some(l)) = (&route.sink, route.limit) {
        let limit_k = l.saturating_add(route.offset);
        facts.push(if has_order {
            format!("server top-{limit_k}")
        } else {
            format!("server early-stop {limit_k}")
        });
    }
    // The client sorts the union of the per-worker replies, which no per-worker
    // cut orders.
    if has_order {
        facts.push("client sort".to_string());
    }
    if route.offset > 0 || route.limit.is_some() {
        facts.push("client window".to_string());
    }
    facts
}

// ---------------------------------------------------------------------------
// The line vocabulary
// ---------------------------------------------------------------------------

/// What is read. The view suffix is a real cost of the read: a view read whose
/// source closure has a committed-but-unticked write drains the pending ticks
/// before serving. EXPLAIN cannot know staleness at plan time, so it names the
/// condition, not a verdict.
fn read_line(target: &Target) -> String {
    match target.kind {
        Some(c) if c.is_view() => format!("read view {} (drains pending ticks when stale)", target.name),
        Some(c) => format!("read {} {}", c.noun(), target.name),
        None => format!("read {}", target.name),
    }
}

/// The walk the bound names. A `PkRange` is a point lookup exactly when it pins
/// every PK column — the same [`pk_point_tuple`] test `AccessPlan::buffered_scope`
/// uses to decide a bound names a key rather than a key group.
fn access(plan: &AccessPlan<'_>, schema: &Schema, sink: &Sink) -> String {
    match plan.bound() {
        // The unprojected scan is the one route that ships no `ReadSpec`, so it
        // reads through a different request kind than any other full scan.
        ReadBound::None => match sink {
            Sink::PlainScan => "full scan (unprojected)".to_string(),
            _ => "full scan".to_string(),
        },
        ReadBound::PkRange(desc) => match pk_point_tuple(desc, schema) {
            Some(_) => "pk point lookup".to_string(),
            None => "pk range walk".to_string(),
        },
        // The keys ship deduplicated, so this is the distinct key count.
        ReadBound::PkSet(keys) => format!("pk set gather ({} keys)", keys.len()),
        ReadBound::IndexRange { idx_cols, exact, .. } => {
            let cols = gnitz_wire::unpack_pk_cols(*idx_cols)
                .as_slice()
                .iter()
                .map(|&c| schema.columns[c as usize].name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            // `exact` means the bounded conjuncts were stripped from the predicate,
            // so the walk alone applies them and the worker runs it un-gated.
            // Otherwise they still ride the predicate and the worker's selectivity
            // gate may drop the walk (`ReadBound::IndexRange` in gnitz-wire is the
            // authority on that rule).
            if *exact {
                format!("index range on ({cols}) — exact walk, never traded")
            } else {
                format!("index range on ({cols}) — may be traded for a full scan on low selectivity")
            }
        }
    }
}

/// How wide the reply is: the columns the client sees, plus any appended purely
/// to order the result.
fn projection_line(visible: usize, extra: usize) -> String {
    if extra > 0 {
        format!("projection: {visible} columns (+{extra} for ordering)")
    } else {
        format!("projection: {visible} columns")
    }
}

/// What the worker folds. The aggregate list is the PHYSICAL one — the reduce
/// items the worker accumulates — which is why an AVG shows as its SUM +
/// COUNT_NON_NULL pair. `ast_util::agg_func_name` gives the SQL spelling instead
/// (it renders `CountNonNull` as `count`), so it is deliberately not used here.
fn fold_line(shape: &FoldShape, schema: &Schema, is_distinct: bool) -> String {
    let cols = shape
        .layout
        .group_col_indices
        .iter()
        .map(|&c| schema.columns[c].name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let ops = shape
        .layout
        .agg_specs
        .iter()
        .map(|s| {
            let op = match s.op {
                AggFunc::Count => return "COUNT(*)".to_string(),
                AggFunc::Sum => "SUM",
                AggFunc::Min => "MIN",
                AggFunc::Max => "MAX",
                AggFunc::CountNonNull => "COUNT_NON_NULL",
                AggFunc::SumZero => "SUM_ZERO",
            };
            format!("{op}({})", schema.columns[s.col].name)
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut line = if is_distinct {
        format!("fold: distinct on ({cols})")
    } else if shape.layout.global_ground() {
        format!("fold: global aggregate: {ops}")
    } else if ops.is_empty() {
        format!("fold: group by ({cols})")
    } else {
        format!("fold: group by ({cols}): {ops}")
    };
    if shape.having.is_some() {
        line.push_str("; HAVING applied client-side");
    }
    line
}

// ---------------------------------------------------------------------------
// The result shape
// ---------------------------------------------------------------------------

/// The two-column EXPLAIN reply: a hidden 1-based line number as the PK (a schema
/// needs one and STRING is not PK-eligible; it also fixes the row order) plus the
/// visible `plan` column every presentation surface shows.
fn plan_rows(lines: &[String]) -> SqlResult {
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("_line", TypeCode::U64, false).hidden(),
            ColumnDef::new("plan", TypeCode::String, false),
        ],
        vec![0],
    )
    .expect("the EXPLAIN reply schema is a valid two-column schema");
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        for (i, line) in lines.iter().enumerate() {
            a.add_row(i as u128 + 1, 1).str_val(line);
        }
    }
    SqlResult::Rows { schema, batch }
}
