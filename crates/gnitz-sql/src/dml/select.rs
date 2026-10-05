//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (a `ReadSpec` rows sink executed
//! server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT top-k)
//! or, for aggregate / DISTINCT shapes, through the **fold sink** (a per-worker
//! hash-fold + client finishing) — except a DISTINCT over rows already a set,
//! which reads as rows. A FROM-less SELECT reads nothing: its one
//! constant row is the fold sink's client finish over the global ground row,
//! computed at plan time.
//!
//! A query that *derives* a new relation — a JOIN, a set operation, an EXISTS/IN
//! or scalar subquery, a derived table — has no single-relation sink and is
//! rejected ([`crate::error::derivation`]), pointing at CREATE VIEW:
//! [`plan_query`] refuses the body's own shape from the AST alone, and the read
//! lowering refuses a CTE that derives. A read the direct path merely cannot
//! express is a feature-named rejection, never that template. The body and its
//! CTEs bind through the view binder (`hir::bind_adhoc_read`), so a CTE reads
//! through the sink of the flat query it composes to.
//!
//! Planning is separate from dispatch, and the seam is [`ReadPlan`]:
//! [`plan_read`] validates, resolves, and decides the sink's access and reply
//! shape without reaching a server; [`execute_select`] runs the resulting plan and
//! `dml::explain` formats the same plan instead of dispatching it.

use crate::agg::ground_partial_schema;
use crate::ast_util::{
    body_is_grouped, classify_from, extract_table_name_and_alias, has_exists_in_subquery, has_scalar_subquery,
    select_is_distinct, FromShape,
};
use crate::bind::Catalog;
use crate::dml::plan::{bound_and_predicate, rows_reply, walks_in_pk_order, RowsReply};
use crate::error::{derivation, reject_if, GnitzSqlError};
use crate::exec::agg_finish::FoldFinish;
use crate::exec::order::{order_and_window, Window};
use crate::expr_lower::compile_scalar_evaluator;
use crate::hir::{bind_adhoc_read, AdhocRead, AdhocShape, FoldPieces};
use crate::ir::BoundExpr;
use crate::project::compute_map;
use crate::tail::{extract_limit, extract_offset, key_slots, order_exprs, parse_order_by, wire_keys};
use crate::validate::{
    as_plain_select, reject_unhonored_query_clauses, reject_unhonored_select_clauses, HonoredClauses, QueryEnvelope,
};
use crate::SqlResult;
use gnitz_core::{BatchAppender, GnitzClient, RelDescriptor, Schema, ZSetBatch};
use gnitz_wire::{ReadSink, ReadSpec, RowsCut, SinkKind};
use sqlparser::ast::{Query, SetExpr, Statement};
use std::num::NonZeroU64;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// The planning seam
// ---------------------------------------------------------------------------

/// A finished ad-hoc read: what it reads and the client finish over the result.
pub(crate) struct ReadPlan {
    pub(super) case: ReadCase,
    /// ORDER BY over the finished result's columns; empty for a constant row, which one row
    /// sorts to itself.
    pub(super) order: Vec<gnitz_wire::OrderKey>,
    pub(super) window: Window,
}

pub(super) enum ReadCase {
    Rows {
        read: SpecRead,
        reply_schema: Arc<Schema>,
    },
    Fold {
        read: SpecRead,
        finish: Box<FoldFinish>,
        /// The reduce input the fold spec's columns index; what EXPLAIN names them against.
        reduce_schema: Arc<Schema>,
        /// Nothing in the layout tells a DISTINCT fold from a zero-aggregate GROUP BY.
        is_distinct: bool,
    },
    /// A FROM-less SELECT: its one row, finished at plan time.
    Constant {
        schema: Arc<Schema>,
        row: ZSetBatch,
    },
}

/// The relation a `ReadSpec` names, and what ships.
pub(super) struct SpecRead {
    /// As the query names it.
    pub(super) name: String,
    pub(super) desc: Arc<RelDescriptor>,
    pub(super) spec: ReadSpec,
}

impl ReadPlan {
    /// The schema of the batch the read produces before client finishing: the rows reply,
    /// the fold's partial reply, or the constant row.
    #[cfg(test)]
    pub(crate) fn reply_schema(&self) -> &Schema {
        match &self.case {
            ReadCase::Rows { reply_schema, .. } => reply_schema,
            ReadCase::Fold { finish, .. } => &finish.partial_schema,
            ReadCase::Constant { schema, .. } => schema,
        }
    }

    /// The `ReadSpec` this read ships; `None` for a constant row.
    #[cfg(test)]
    pub(crate) fn spec(&self) -> Option<&ReadSpec> {
        self.spec_read().map(|read| &read.spec)
    }

    /// The relation read and what ships; `None` for a constant row.
    pub(super) fn spec_read(&self) -> Option<&SpecRead> {
        match &self.case {
            ReadCase::Rows { read, .. } | ReadCase::Fold { read, .. } => Some(read),
            ReadCase::Constant { .. } => None,
        }
    }

    /// Whether the result is `LIMIT 0`'s: answered from the schema alone, with
    /// no request issued.
    pub(crate) fn answers_from_schema(&self) -> bool {
        self.window.limit == Some(0)
    }

    /// The schema the finished result is built under.
    fn out_schema(&self) -> &Arc<Schema> {
        match &self.case {
            ReadCase::Rows { reply_schema, .. } => reply_schema,
            ReadCase::Fold { finish, .. } => finish.out_schema(),
            ReadCase::Constant { schema, .. } => schema,
        }
    }
}

/// Plan a `SELECT`, or the `EXPLAIN` of one, into the read it runs. Both
/// statement forms yield the same plan.
pub(crate) fn plan_read(stmt: &Statement, cat: &Catalog<'_>) -> Result<ReadPlan, GnitzSqlError> {
    let query = match stmt {
        Statement::Query(q) => q.as_ref(),
        // EXPLAIN describes the plan a query *would* take, so it consumes only
        // the statement it wraps. `describe_alias` is inert phrasing: `EXPLAIN`,
        // `DESCRIBE` and `DESC` all introduce the same statement.
        Statement::Explain {
            describe_alias: _,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            const CTX: &str = "EXPLAIN";
            // `analyze` runs the query — the one option that changes what EXPLAIN
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
            match statement.as_ref() {
                Statement::Query(q) => q.as_ref(),
                _ => {
                    return Err(GnitzSqlError::Rejected(
                        "EXPLAIN describes a SELECT; this statement is not one".to_string(),
                    ))
                }
            }
        }
        _ => {
            return Err(GnitzSqlError::Rejected(
                "plan_read describes a SELECT or the EXPLAIN of one; this statement is neither".to_string(),
            ))
        }
    };
    plan_query(cat, query)
}

/// Validate an ad-hoc SELECT's shape, bind it to the one relation it reads, and
/// decide its access and sink.
fn plan_query(cat: &Catalog<'_>, query: &Query) -> Result<ReadPlan, GnitzSqlError> {
    // ORDER BY / LIMIT / OFFSET are the client finish's; any other query clause is refused.
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, "direct SELECT")?;
    let select = match query.body.as_ref() {
        SetExpr::SetOperation { .. } => return Err(derivation("set operation")),
        // Not the derivation template: CREATE VIEW refuses these bodies too.
        body => as_plain_select(body, "direct SELECT")?,
    };
    // Detected nowhere else: without this the binder rejects these without naming CREATE VIEW.
    if has_exists_in_subquery(select) {
        return Err(derivation("EXISTS/IN subquery"));
    }
    if has_scalar_subquery(select) {
        return Err(derivation("scalar subquery"));
    }
    let window = Window {
        limit: extract_limit(query)?,
        offset: extract_offset(query)?,
    };
    // DISTINCT wins the split: its fold does not group, so DISTINCT + GROUP BY keeps the
    // GROUP BY rejection.
    let distinct = select_is_distinct(select);
    let fold = distinct || body_is_grouped(select);
    let ctx = if distinct {
        "SELECT DISTINCT"
    } else if fold {
        "aggregate SELECT"
    } else {
        "direct SELECT"
    };
    match classify_from(&select.from) {
        FromShape::Empty => {
            const CTX: &str = "SELECT without FROM";
            reject_unhonored_select_clauses(select, HonoredClauses::PLAIN, CTX)?;
            reject_if(select.selection.is_some(), CTX, "WHERE")?;
        }
        FromShape::SinglePlainRelation(factor) => {
            // The FROM name is checked before the clause gate.
            extract_table_name_and_alias(factor, cat.schema_name(), "FROM")?;
            reject_unhonored_select_clauses(select, HonoredClauses::for_body(fold, distinct), ctx)?;
        }
        FromShape::Derived(construct) => return Err(derivation(construct)),
    }
    let keys = parse_order_by(query.order_by.as_ref())?;
    let (desc, name, conjuncts, shape) = match bind_adhoc_read(cat, query, select, ctx, &order_exprs(&keys))? {
        AdhocRead::Relation { desc, name, conjuncts, shape } => (desc, name, conjuncts, shape),
        AdhocRead::Constant(items) => {
            // One row sorts to itself: a positional key is range-checked and no key
            // is placed, so an expression key's slot is never read.
            key_slots(&keys, items.iter().map(|(_, d)| d), std::iter::repeat(0))?;
            let (schema, row) = plan_constant(items)?;
            return Ok(ReadPlan {
                case: ReadCase::Constant { schema, row },
                order: Vec::new(),
                window,
            });
        }
    };
    let (bound, predicate) = bound_and_predicate(&desc.schema, &conjuncts, &desc.indexes)?;
    let in_pk_order = walks_in_pk_order(&bound, &desc.schema.pk_cols);
    let read = |sink| SpecRead {
        name,
        desc: Arc::clone(&desc),
        spec: ReadSpec { bound, predicate, sink },
    };
    let (case, order) = match shape {
        AdhocShape::Fold(pieces, order_cols) => {
            let FoldPlan { sink, finish, reduce_schema, order } = plan_fold(*pieces, &keys, &order_cols)?;
            let case = ReadCase::Fold {
                read: read(sink),
                finish: Box::new(finish),
                reduce_schema,
                is_distinct: distinct,
            };
            (case, order)
        }
        AdhocShape::Rows(rows) => {
            let RowsReply {
                schema: reply_schema,
                program,
                order,
                pk_ordered,
            } = rows_reply(rows, &keys, &desc)?;
            // OFFSET+LIMIT logical rows; an OFFSET with no LIMIT cuts nothing.
            let cut = window
                .end()
                .and_then(|end| NonZeroU64::new(end as u64))
                .map(|k| RowsCut {
                    k,
                    // A worker walking in the order asked for meets its smallest rows
                    // first, so it stops at the window instead of ranking every row.
                    order: match pk_ordered && in_pk_order {
                        true => Vec::new(),
                        false => order.clone(),
                    },
                });
            let sink = ReadSink {
                map: program.map(|p| compute_map(p, &reply_schema)),
                kind: SinkKind::Rows { cut },
            };
            (ReadCase::Rows { read: read(sink), reply_schema }, order)
        }
    };
    Ok(ReadPlan { case, order, window })
}

/// A planned fold read.
struct FoldPlan {
    sink: ReadSink,
    finish: FoldFinish,
    /// The reduce input the sink's columns index.
    reduce_schema: Arc<Schema>,
    /// ORDER BY over the finished output.
    order: Vec<gnitz_wire::OrderKey>,
}

/// A GROUP BY / global aggregate / HAVING / DISTINCT read's sink and client finish.
fn plan_fold(
    pieces: FoldPieces,
    keys: &[crate::tail::OrderKey<'_>],
    order_cols: &[usize],
) -> Result<FoldPlan, GnitzSqlError> {
    // Compiled here rather than at finish, so every rejection is pre-dispatch — and
    // the same one a view gives, the finalize being compiled as a view's map is.
    let finish = FoldFinish::new(
        pieces.partial_schema,
        pieces.agg.aggs.iter().map(|d| d.agg_op),
        &pieces.having,
        pieces.finalize,
    )?;
    // `group_cols` / `col_idx` index the reduce input — the pre-map's output when
    // the fold carries one.
    let sink = ReadSink {
        map: pieces.pre,
        kind: SinkKind::Fold(pieces.agg),
    };
    // The finalize items follow the output's key.
    let order = wire_keys(
        keys,
        &finish.out_schema().columns,
        order_cols.iter().map(|&at| finish.out_schema().pk_cols.len() + at),
    )?;
    Ok(FoldPlan {
        sink,
        finish,
        reduce_schema: pieces.reduce_schema,
        order,
    })
}

/// A FROM-less SELECT's one row, finished at plan time: each item is a constant
/// expression, compiled as a fold's finalize item over the ground row. A hidden
/// item is an ORDER BY key, which is compiled and dropped.
fn plan_constant(items: Vec<(BoundExpr, gnitz_wire::ColumnDef)>) -> Result<(Arc<Schema>, ZSetBatch), GnitzSqlError> {
    let ground = ground_partial_schema();
    let (keys, items): (Vec<_>, Vec<_>) = items.into_iter().partition(|(_, def)| def.is_hidden);
    for (key, _) in &keys {
        compile_scalar_evaluator(key, &ground)?;
    }
    let mut finish = FoldFinish::new(Arc::new(ground), [], &[], items)?;
    let mut ground_row = ZSetBatch::with_capacity(&finish.partial_schema, 1);
    BatchAppender::new(&mut ground_row).add_row(gnitz_wire::global_group_key(), 1);
    Ok((Arc::clone(finish.out_schema()), finish.finish(ground_row)))
}

// ---------------------------------------------------------------------------
// The dispatch tail
// ---------------------------------------------------------------------------

/// Run one planned `SELECT`, reading local-first: a relation the client's copy
/// holds is answered off it, and every other one over the wire.
pub(crate) fn execute_select(client: &mut GnitzClient, plan: ReadPlan) -> Result<SqlResult, GnitzSqlError> {
    if plan.answers_from_schema() {
        let schema = Arc::clone(plan.out_schema());
        return Ok(SqlResult::Rows {
            batch: ZSetBatch::new(&schema),
            schema,
            lsn: None,
        });
    }
    let ReadPlan { case, order, window } = plan;
    let (schema, batch, lsn) = match case {
        ReadCase::Constant { schema, row } => (schema, row, None),
        ReadCase::Rows { read, reply_schema } => {
            let reply = client.scan_spec_local_first(&*read.desc, read.spec, &reply_schema)?;
            (reply_schema, reply.batch, reply.lsn)
        }
        ReadCase::Fold { read, mut finish, .. } => {
            // A wire error (the per-worker group cap included) is hard: the fold is
            // mid-flight on the workers and cannot fall back.
            let partial = client.scan_spec_local_first(&*read.desc, read.spec, &finish.partial_schema)?;
            (
                Arc::clone(finish.out_schema()),
                finish.finish(partial.batch),
                partial.lsn,
            )
        }
    };
    let batch = order_and_window(&schema, batch, &order, window);
    Ok(SqlResult::Rows { schema, batch, lsn })
}
