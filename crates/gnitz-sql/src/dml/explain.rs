//! `EXPLAIN <select>`: the access decisions the ad-hoc read path makes, rendered
//! as rows. It formats the same [`ReadPlan`], from the same `plan_read`, that
//! `dml::select` would dispatch, so a query the planner rejects returns that
//! query's own rejection and every line describes what the SELECT would run.
//!
//! Describing a plan sends no request: its names were resolved when it was
//! planned, and whether a read is served locally is read off the client's copy.

use crate::SqlResult;
use crate::dml::select::{ReadCase, ReadPlan, SpecRead};
use crate::error::{GnitzSqlError, reject_if};
use gnitz_core::{GnitzClient, ScanReply, Schema, ZSetBatch};
use gnitz_wire::{AggFunc, GroupAggs, ReadBound, RowsCut, SinkKind};
use gnitz_wire::{ColumnDef, TypeCode};
use sqlparser::ast::{Query, Statement};

/// The SELECT an `EXPLAIN` describes. EXPLAIN describes the plan a query *would*
/// take, so it consumes only the statement it wraps. `describe_alias` is inert
/// phrasing: `EXPLAIN`, `DESCRIBE` and `DESC` introduce the same statement.
pub(crate) fn explained_query(stmt: &Statement) -> Result<&Query, GnitzSqlError> {
    if let Statement::Explain {
        describe_alias: _,
        analyze,
        verbose,
        query_plan,
        estimate,
        statement,
        format,
        options,
    } = stmt
    {
        const CTX: &str = "EXPLAIN";
        // `analyze` runs the query, the one option that changes what EXPLAIN
        // does; the rest each ask for a rendering this output shape lacks.
        reject_if(*analyze, CTX, "ANALYZE")?;
        reject_if(*verbose, CTX, "VERBOSE")?;
        reject_if(*query_plan, CTX, "QUERY PLAN")?;
        reject_if(*estimate, CTX, "ESTIMATE")?;
        reject_if(format.is_some(), CTX, "FORMAT")?;
        // `GenericDialect` sets `supports_explain_with_utility_options`, so the
        // parenthesized Postgres form parses into `options` rather than failing
        // at parse time.
        reject_if(options.is_some(), CTX, "the parenthesized option list")?;
        if let Statement::Query(q) = statement.as_ref() {
            return Ok(q);
        }
    }
    Err(GnitzSqlError::rejected(
        "EXPLAIN describes a SELECT; this statement is not one",
    ))
}

/// Describe `plan` without running it: the EXPLAIN reply. A read of a relation
/// `client` mirrors is described as the local read it is served as.
pub(crate) fn execute_explain(client: &GnitzClient, plan: &ReadPlan) -> SqlResult {
    let local = plan
        .spec_read()
        .is_some_and(|read| client.cursor_of(read.desc.tid).is_some());
    plan_rows(&explain_lines(plan, local))
}

/// The five lines EXPLAIN renders for `plan`: what is read, the access path,
/// where the predicate runs, the sink's shape, and the ORDER BY / LIMIT tail.
/// `local`: the read is served off the client's copy, which executes the same
/// `ReadSpec` the server would.
pub(crate) fn explain_lines(plan: &ReadPlan, local: bool) -> Vec<String> {
    let order_limit = order_limit_line(plan, local);
    let (read, shape) = match &plan.case {
        ReadCase::Constant { .. } => {
            return vec![
                "read nothing (constant row)".to_string(),
                "access: none".to_string(),
                "predicate: none".to_string(),
                projection_line(plan.cut.schema(), None),
                order_limit,
            ];
        }
        ReadCase::Rows { read, .. } => {
            let source = read.spec.sink.map.is_none().then_some(&*read.desc.schema);
            (read, projection_line(plan.cut.schema(), source))
        }
        ReadCase::Fold { read, finish, reduce_schema, is_distinct } => {
            let SinkKind::Fold(agg) = &read.spec.sink.kind else {
                unreachable!("a fold read ships a fold sink")
            };
            (read, fold_line(agg, reduce_schema, finish.has_having(), *is_distinct))
        }
    };
    let predicate = match (read.spec.predicate.is_empty(), local) {
        (true, _) => "none",
        (false, true) => "local",
        (false, false) => "server-side",
    };
    vec![
        read_line(read, local),
        format!("access: {}", access_line(&read.spec.bound, &read.desc.schema)),
        format!("predicate: {predicate}"),
        shape,
        order_limit,
    ]
}

/// Where the ORDER BY / LIMIT / OFFSET work happens.
fn order_limit_line(plan: &ReadPlan, local: bool) -> String {
    if plan.answers_from_schema() {
        return "order/limit: no request (LIMIT 0)".to_string();
    }
    let mut facts = Vec::new();
    // The per-worker cut is OFFSET+LIMIT deep, because the client cuts the exact slice. A cut
    // shipping no ORDER BY keys stops the worker at its first rows.
    if let ReadCase::Rows { read, .. } = &plan.case
        && let SinkKind::Rows { cut: Some(RowsCut { k, order }) } = &read.spec.sink.kind
    {
        let at = if local { "local" } else { "server" };
        facts.push(if order.is_empty() {
            format!("{at} early-stop {k}")
        } else {
            format!("{at} top-{k}")
        });
    }
    if plan.cut.is_ordered() {
        facts.push("client sort".to_string());
    }
    if plan.cut.cuts() {
        facts.push("client slice".to_string());
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

/// What is read. The view suffix is a real cost of the read: a server read of a
/// view whose source closure has a committed-but-unticked write drains the
/// pending ticks before serving. EXPLAIN cannot know staleness at plan time, so it
/// names the condition, not a verdict. A local read drains nothing.
fn read_line(read: &SpecRead, local: bool) -> String {
    let class = read.desc.class;
    if local {
        format!("read view {} (local copy)", read.name)
    } else if class.is_view() {
        format!("read view {} (drains pending ticks when stale)", read.name)
    } else {
        format!("read {} {}", class.noun(), read.name)
    }
}

/// The walk the bound names; a one-key `PkSet` is a point lookup.
fn access_line(bound: &ReadBound, schema: &Schema) -> String {
    match bound {
        ReadBound::None => "full scan".to_string(),
        ReadBound::Range(r) if r.walks_pk(schema.pk_cols()) => "pk range walk".to_string(),
        ReadBound::PkSet(keys) if keys.len() == 1 => "pk point lookup".to_string(),
        // The keys ship deduplicated, so this is the distinct key count.
        ReadBound::PkSet(keys) => format!("pk set gather ({} keys)", keys.len()),
        ReadBound::Range(r) => {
            let cols = r
                .cols()
                .as_slice()
                .iter()
                .map(|&c| &*schema.columns()[c as usize].name)
                .collect::<Vec<_>>()
                .join(", ");
            format!("index range on ({cols})")
        }
    }
}

/// How wide the reply is: its visible columns, the hidden ones appended to order it, and
/// whether it ships no map — `source`'s regions as they are, where `Some`.
fn projection_line(schema: &Schema, source: Option<&Schema>) -> String {
    let visible = schema.visible_columns().count();
    // A hidden payload column is appended to order the reply, unless it is one the
    // unprojected source hides itself; a hidden PK column carries its key.
    let hidden = schema.payload_columns().map(|(_, _, d)| d.is_hidden);
    let extra = match source {
        Some(source) => hidden
            .zip(source.payload_columns())
            .filter(|&(h, (_, _, d))| h && !d.is_hidden)
            .count(),
        None => hidden.filter(|&h| h).count(),
    };
    let noun = if visible == 1 { "column" } else { "columns" };
    let mut line = if extra > 0 {
        format!("projection: {visible} {noun} (+{extra} for ordering)")
    } else {
        format!("projection: {visible} {noun}")
    };
    if source.is_some() {
        line.push_str(" (unprojected)");
    }
    line
}

/// What the worker folds: the PHYSICAL aggregate list, which is why an AVG shows
/// as its SUM and a count and why the match below is over the *wire*
/// enum — the one carrying no `Avg`.
fn fold_line(agg: &GroupAggs, reduce_schema: &Schema, has_having: bool, is_distinct: bool) -> String {
    // Named against the reduce input, not the source: with a pre-map the reduce
    // groups and aggregates columns the source does not have, and the hidden
    // `_preN` the pre-map minted for the written expression is the only name one
    // of those has.
    let cols = agg
        .group_cols
        .iter()
        .map(|&c| &*reduce_schema.columns()[c as usize].name)
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
                AggFunc::CountPosInf => "COUNT_POS_INF",
                AggFunc::CountNegInf => "COUNT_NEG_INF",
                AggFunc::FiniteSum => "FINITE_SUM",
            };
            format!("{op}({})", reduce_schema.columns()[s.col_idx as usize].name)
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut line = if is_distinct {
        format!("fold: distinct on ({cols})")
    } else if agg.group_cols.is_empty() && ops.is_empty() {
        "fold: global aggregate".to_string()
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
        &[0],
    )
    .expect("the EXPLAIN reply schema is a valid two-column schema");
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = batch.builder();
        for (i, line) in lines.iter().enumerate() {
            a.begin_row(i as u128 + 1, 1).put_string(line);
        }
    }
    SqlResult::Rows(ScanReply {
        schema: std::sync::Arc::new(schema),
        batch,
        lsn: None,
    })
}
