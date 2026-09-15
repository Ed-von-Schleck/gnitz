//! `EXPLAIN <select>`: the access decisions the ad-hoc read path makes, rendered
//! as rows. It formats the same [`ReadPlan`], from the same `plan_read`, that
//! `dml::select` would dispatch, so a query the planner rejects returns that
//! query's own rejection and every line describes what the SELECT would run.
//!
//! It reaches nothing: describing a query is as server-free as compiling one.

use crate::dml::plan::Access;
use crate::dml::select::{ReadCase, ReadPlan, SpecRead};
use crate::SqlResult;
use gnitz_core::{BatchAppender, ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_wire::sys_rows::SysRowSink;
use gnitz_wire::{AggFunc, AggReadSpec, IndexWalk, ReadBound, SinkKind};

/// Describe `plan` without running it: the EXPLAIN reply.
pub(crate) fn execute_explain(plan: &ReadPlan) -> SqlResult {
    plan_rows(&explain_lines(plan))
}

/// The five lines EXPLAIN renders for `plan`: what is read, the access path,
/// where the predicate runs, the sink's shape, and the ORDER BY / LIMIT tail.
pub fn explain_lines(plan: &ReadPlan) -> Vec<String> {
    let order_limit = order_limit_line(plan);
    let (read, shape) = match &plan.case {
        ReadCase::Constant { schema, .. } => {
            return vec![
                "read nothing (constant row)".to_string(),
                "access: none".to_string(),
                "predicate: none".to_string(),
                projection_line(schema, false),
                order_limit,
            ]
        }
        ReadCase::Rows { read, reply_schema } => (read, projection_line(reply_schema, read.sink.map.is_none())),
        ReadCase::Fold { read, finish, reduce_schema, is_distinct } => {
            let SinkKind::Fold(agg) = &read.sink.kind else {
                unreachable!("a fold read ships a fold sink")
            };
            (
                read,
                fold_line(agg, reduce_schema, finish.having.is_some(), *is_distinct),
            )
        }
    };
    vec![
        read_line(read),
        format!("access: {}", access_line(&read.access, &read.desc.schema)),
        format!(
            "predicate: {}",
            if read.access.has_predicate() {
                "server-side"
            } else {
                "none"
            }
        ),
        shape,
        order_limit,
    ]
}

/// Where the ORDER BY / LIMIT / OFFSET work happens.
fn order_limit_line(plan: &ReadPlan) -> String {
    if plan.window.limit == Some(0) {
        return "order/limit: no request (LIMIT 0)".to_string();
    }
    let mut facts = Vec::new();
    // The per-worker cut is OFFSET+LIMIT deep, because the client windows. With no
    // ORDER BY keys the same wire field just stops the worker early.
    if let ReadCase::Rows { read, .. } = &plan.case {
        if let SinkKind::Rows { limit_k, .. } = &read.sink.kind {
            if *limit_k > 0 {
                facts.push(if plan.order.is_empty() {
                    format!("server early-stop {limit_k}")
                } else {
                    format!("server top-{limit_k}")
                });
            }
        }
    }
    if !plan.order.is_empty() {
        facts.push("client sort".to_string());
    }
    if plan.window.cuts() {
        facts.push("client window".to_string());
    }
    if facts.is_empty() {
        "order/limit: none".to_string()
    } else {
        format!("order/limit: {}", facts.join(", "))
    }
}

// ---------------------------------------------------------------------------
// The line vocabulary
// ---------------------------------------------------------------------------

/// What is read. The view suffix is a real cost of the read: a view read whose
/// source closure has a committed-but-unticked write drains the pending ticks
/// before serving. EXPLAIN cannot know staleness at plan time, so it names the
/// condition, not a verdict.
fn read_line(read: &SpecRead) -> String {
    let class = read.desc.class;
    if class.is_view() {
        format!("read view {} (drains pending ticks when stale)", read.name)
    } else {
        format!("read {} {}", class.noun(), read.name)
    }
}

/// The walk the bound names; a one-key `PkSet` is a point lookup.
fn access_line(access: &Access, schema: &Schema) -> String {
    match access.bound() {
        ReadBound::None => "full scan".to_string(),
        ReadBound::PkRange(_) => "pk range walk".to_string(),
        ReadBound::PkSet(keys) if keys.len() == 1 => "pk point lookup".to_string(),
        // The keys ship deduplicated, so this is the distinct key count.
        ReadBound::PkSet(keys) => format!("pk set gather ({} keys)", keys.len()),
        ReadBound::IndexRange { bound, walk } => {
            let cols = bound
                .idx_cols
                .as_slice()
                .iter()
                .map(|&c| schema.columns[c as usize].name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            match walk {
                IndexWalk::Required => format!("index range on ({cols}) — exact walk, never traded"),
                IndexWalk::Optional => {
                    format!("index range on ({cols}) — may be traded for a full scan on low selectivity")
                }
            }
        }
    }
}

/// How wide the reply is: its visible columns, the hidden ones appended to order it, and
/// whether it ships no map.
fn projection_line(schema: &Schema, unprojected: bool) -> String {
    let visible = schema.visible_columns().count();
    let extra = (0..schema.columns.len())
        .filter(|&i| schema.is_hidden_payload(i))
        .count();
    let mut line = if extra > 0 {
        format!("projection: {visible} columns (+{extra} for ordering)")
    } else {
        format!("projection: {visible} columns")
    };
    if unprojected {
        line.push_str(" (unprojected)");
    }
    line
}

/// What the worker folds: the PHYSICAL aggregate list, which is why an AVG shows
/// as its SUM and a count and why the match below is over the *wire*
/// enum — the one carrying `SumZero` and no `Avg`.
fn fold_line(agg: &AggReadSpec, reduce_schema: &Schema, has_having: bool, is_distinct: bool) -> String {
    // Named against the reduce input, not the source: with a pre-map the reduce
    // groups and aggregates columns the source does not have, and the hidden
    // `_preN` the pre-map minted for the written expression is the only name one
    // of those has.
    let cols = agg
        .group_cols
        .iter()
        .map(|&c| reduce_schema.columns[c as usize].name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let ops = agg
        .aggs
        .iter()
        .map(|s| {
            let op = match s.agg_op {
                AggFunc::Count => return "COUNT(*)".to_string(),
                AggFunc::Sum => "SUM",
                AggFunc::Min => "MIN",
                AggFunc::Max => "MAX",
                AggFunc::CountNonNull => "COUNT_NON_NULL",
                AggFunc::SumZero => "SUM_ZERO",
            };
            format!("{op}({})", reduce_schema.columns[s.col_idx as usize].name)
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut line = if is_distinct {
        format!("fold: distinct on ({cols})")
    } else if agg.group_cols.is_empty() {
        format!("fold: global aggregate: {ops}")
    } else if ops.is_empty() {
        format!("fold: group by ({cols})")
    } else {
        format!("fold: group by ({cols}): {ops}")
    };
    if has_having {
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
        // Through the row sink, so the row-completeness check `end_row` carries
        // applies here as it does to every catalog row.
        let mut a = BatchAppender::new(&mut batch, &schema);
        for (i, line) in lines.iter().enumerate() {
            a.begin_row(&[i as u128 + 1], 1);
            a.put_string(line);
            a.end_row();
        }
    }
    SqlResult::Rows {
        schema: std::sync::Arc::new(schema),
        batch,
    }
}
