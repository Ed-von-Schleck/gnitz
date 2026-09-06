//! `EXPLAIN <select>`: the access decisions the ad-hoc read path makes, rendered
//! as rows. It formats the same [`ReadPlan`], from the same `plan_read`, that
//! `dml::select` would dispatch, so a query the planner rejects returns that
//! query's own rejection and every line describes what the SELECT would run.
//!
//! It reaches nothing: describing a query is as server-free as compiling one.

use crate::access::pk_point_tuple;
use crate::dml::plan::Access;
use crate::dml::select::{ReadCase, ReadPlan, SinkTail, SpecRead, Target};
use crate::exec::agg_finish::FoldShape;
use crate::SqlResult;
use gnitz_core::{BatchAppender, ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_wire::sys_rows::SysRowSink;
use gnitz_wire::{AggFunc, ReadBound};

/// Describe `plan` without running it: the EXPLAIN reply.
pub(crate) fn execute_explain(plan: &ReadPlan) -> SqlResult {
    plan_rows(&explain_lines(plan))
}

/// The five lines EXPLAIN renders for `plan`: what is read, the access path,
/// where the predicate runs, the sink's shape, and the ORDER BY / LIMIT tail.
pub fn explain_lines(plan: &ReadPlan) -> Vec<String> {
    let target = &plan.target;
    let schema = &*target.schema;

    // The bare-`*` scan ships no `ReadSpec`, so it reads through a different
    // request kind than any other full scan. It projects nothing and orders by
    // nothing, so the reply is the source's own visible width.
    let ReadCase::Spec(spec) = &plan.case else {
        return vec![
            read_line(target),
            "access: full scan (unprojected)".to_string(),
            "predicate: none".to_string(),
            projection_line(schema.visible_columns().count(), 0),
            "order/limit: none".to_string(),
        ];
    };

    // Line 4 is the sink's own shape: what the fold accumulates, or how wide the
    // projected reply is.
    let shape_line = match &spec.tail {
        SinkTail::Rows { reply_schema } => {
            // The hidden columns `resolve_read_spec_order` appended to order by a
            // non-projected source column. The prepended source PK is hidden too,
            // but is a PK column rather than a payload one.
            let extra = (0..reply_schema.columns.len())
                .filter(|&i| reply_schema.is_hidden_payload(i))
                .count();
            projection_line(reply_schema.visible_columns().count(), extra)
        }
        SinkTail::Fold { shape, is_distinct } => fold_line(shape, *is_distinct),
    };

    let facts = order_limit_facts(spec);
    vec![
        read_line(target),
        format!("access: {}", access_line(&spec.access, schema)),
        format!(
            "predicate: {}",
            if spec.access.has_predicate() {
                "server-side"
            } else {
                "none"
            }
        ),
        shape_line,
        if facts.is_empty() {
            "order/limit: none".to_string()
        } else {
            format!("order/limit: {}", facts.join(", "))
        },
    ]
}

/// Where the ORDER BY / LIMIT / OFFSET work happens. Only the rows sink pushes
/// anything down; all fold finishing is client-side.
fn order_limit_facts(spec: &SpecRead) -> Vec<String> {
    // Both tails short-circuit `LIMIT 0` to an empty result before dispatching.
    if spec.limit == Some(0) {
        return vec!["no request (LIMIT 0)".to_string()];
    }
    let has_order = !spec.order.is_empty();
    let mut facts = Vec::new();
    // The per-worker cut is OFFSET+LIMIT deep, because the client windows. With no
    // ORDER BY keys the same wire field just stops the worker early.
    if let (SinkTail::Rows { .. }, Some(l)) = (&spec.tail, spec.limit) {
        let limit_k = l.saturating_add(spec.offset);
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
    if spec.offset > 0 || spec.limit.is_some() {
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
    let class = target.desc.class;
    if class.is_view() {
        format!("read view {} (drains pending ticks when stale)", target.name)
    } else {
        format!("read {} {}", class.noun(), target.name)
    }
}

/// The walk the bound names. A `PkRange` is a point lookup exactly when it pins
/// every PK column — the same [`pk_point_tuple`] test `AccessPlan::buffered_scope`
/// uses to decide a bound names a key rather than a key group.
fn access_line(access: &Access, schema: &Schema) -> String {
    match access.bound() {
        ReadBound::None => "full scan".to_string(),
        ReadBound::PkRange(desc) => match pk_point_tuple(desc, schema) {
            Some(_) => "pk point lookup".to_string(),
            None => "pk range walk".to_string(),
        },
        // The keys ship deduplicated, so this is the distinct key count.
        ReadBound::PkSet(keys) => format!("pk set gather ({} keys)", keys.len()),
        // No SQL surface reaches a delta bound: a `SELECT` projects rows without
        // weights, so a retraction would render indistinguishable from an insert.
        // It is reachable through the read verbs, where weights are native, and
        // nowhere else — so no planner ever builds one for EXPLAIN to describe.
        ReadBound::Delta { after_tick } => format!("delta feed after round {after_tick}"),
        ReadBound::IndexRange { idx_cols, exact, .. } => {
            let cols = match gnitz_wire::unpack_pk_cols(*idx_cols) {
                Ok(cols) => cols
                    .as_slice()
                    .iter()
                    .map(|&c| schema.columns[c as usize].name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                // Packed by the planner that built this very plan: render the rule
                // rather than give the line builder an error channel.
                Err(rule) => rule.to_string(),
            };
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
fn fold_line(shape: &FoldShape, is_distinct: bool) -> String {
    // Named against the reduce input, not the source: with a pre-map the reduce
    // groups and aggregates columns the source does not have, and the hidden
    // `_preN` the pre-map minted for the written expression is the only name one
    // of those has.
    let schema = &shape.reduce_schema;
    let cols = shape
        .group_positions
        .iter()
        .map(|&c| schema.columns[c].name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let ops = shape
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
    } else if shape.global_ground() {
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
        // Through the row sink, so the row-completeness check `end_row` carries
        // applies here as it does to every catalog row.
        let mut a = BatchAppender::new(&mut batch, &schema);
        for (i, line) in lines.iter().enumerate() {
            a.begin_row(&[i as u128 + 1], 1);
            a.put_string(line);
            a.end_row();
        }
    }
    SqlResult::Rows { schema, batch }
}
