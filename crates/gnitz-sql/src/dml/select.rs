//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (a `ReadSpec` rows sink executed
//! server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT top-k)
//! or, for aggregate / DISTINCT shapes, through the **fold sink** (a per-worker
//! hash-fold + client finishing). A FROM-less SELECT reads nothing: its one
//! constant row is the fold sink's client finish over the global ground row,
//! computed at plan time.
//!
//! A query that *derives* a new relation — a JOIN, a set operation, an EXISTS/IN
//! or scalar subquery, a derived table, a grouped CTE — has no single-relation
//! sink; [`plan_query`] rejects it from the AST alone
//! ([`crate::error::derivation`]), pointing at CREATE VIEW. A read the direct
//! path merely cannot express is a feature-named `Unsupported`, never that
//! template. A `WITH` is expanded into the body first (`dml::cte`), so a CTE
//! reads through the flat query's sink.
//!
//! Planning is separate from dispatch, and the seam is [`ReadPlan`]:
//! [`plan_read`] validates, resolves, and decides the sink's access and reply
//! shape without reaching a server; [`execute_select`] runs the resulting plan and
//! `dml::explain` formats the same plan instead of dispatching it.

use crate::agg::ground_partial_schema;
use crate::ast_util::{
    body_is_grouped, classify_from, extract_table_name_and_alias, has_exists_in_subquery, has_scalar_subquery,
    reject_position_out_of_range, scalar_projection_item, FromShape,
};
use crate::bind::{bind_single_table, output_column, Binder};
use crate::dml::cte::inline_ctes;
use crate::dml::plan::{bind_where, bound_and_predicate, fetch_bound, rows_sink, Access, ReadBudget};
use crate::error::{derivation, reject_if, GnitzSqlError};
use crate::exec::agg_finish::FoldFinish;
use crate::exec::order::{order_and_window, Window};
use crate::expr_lower::compile_scalar_evaluator;
use crate::hir::bind_and_lower_fold;
use crate::ir::BoundExpr;
use crate::tail::{extract_limit, extract_offset, order_exprs, parse_order_by, wire_keys, OrderTarget};
use crate::validate::{
    as_plain_select, computed_column, reject_duplicate_projection_names, reject_unhonored_query_clauses,
    reject_unhonored_select_clauses, HonoredClauses, QueryEnvelope,
};
use crate::SqlResult;
use gnitz_core::{BatchAppender, CatalogSnapshot, GnitzClient, RelDescriptor, Schema, ZSetBatch};
use gnitz_wire::{ReadSink, ReadSpec, SinkKind};
use sqlparser::ast::{OrderBy, Query, Select, SetExpr, Statement};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// The planning seam
// ---------------------------------------------------------------------------

/// A finished ad-hoc read: what it reads and the client finish over the result.
pub struct ReadPlan {
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
    /// As written in FROM.
    pub(super) name: String,
    pub(super) desc: Arc<RelDescriptor>,
    pub(super) access: Access,
    pub(super) sink: ReadSink,
}

impl ReadPlan {
    /// The schema of the batch the read produces before client finishing: the rows reply,
    /// the fold's partial reply, or the constant row.
    pub fn reply_schema(&self) -> &Schema {
        match &self.case {
            ReadCase::Rows { reply_schema, .. } => reply_schema,
            ReadCase::Fold { finish, .. } => &finish.partial_schema,
            ReadCase::Constant { schema, .. } => schema,
        }
    }

    /// The `ReadSpec` this read ships; `None` for a constant row. One, not a list:
    /// an over-cap `PkSet` gather needs `ReadBudget::MayChunk`, and a read plans
    /// under `OneRequest`.
    pub fn spec(&self) -> Option<ReadSpec> {
        match &self.case {
            ReadCase::Rows { read, .. } | ReadCase::Fold { read, .. } => {
                Some(read.access.spec(read.access.bound().clone(), &read.sink))
            }
            ReadCase::Constant { .. } => None,
        }
    }

    /// The schema the finished result is built under.
    fn out_schema(&self) -> &Arc<Schema> {
        match &self.case {
            ReadCase::Rows { reply_schema, .. } => reply_schema,
            ReadCase::Fold { finish, .. } => &finish.out_schema,
            ReadCase::Constant { schema, .. } => schema,
        }
    }
}

/// Plan a `SELECT`, or the `EXPLAIN` of one, into the read it runs: a pure
/// function of `(stmt, cat)`, reaching no server. Both statement forms yield the
/// same plan, down to the rejection a query unsupported on two axes reports.
pub fn plan_read(stmt: &Statement, cat: &CatalogSnapshot, schema_name: &str) -> Result<ReadPlan, GnitzSqlError> {
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
                    return Err(GnitzSqlError::Unsupported(
                        "EXPLAIN describes a SELECT; this statement is not one".to_string(),
                    ))
                }
            }
        }
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "plan_read describes a SELECT or the EXPLAIN of one; this statement is neither".to_string(),
            ))
        }
    };
    let binder = Binder::new(schema_name);
    let flat = inline_ctes(cat, &binder, query)?;
    plan_query(cat, flat.as_ref().unwrap_or(query), &binder)
}

/// Validate an ad-hoc SELECT's shape, resolve the one relation it reads, and
/// decide its access and sink.
fn plan_query(cat: &CatalogSnapshot, query: &Query, binder: &Binder<'_>) -> Result<ReadPlan, GnitzSqlError> {
    // ORDER BY / LIMIT / OFFSET are the client finish's; any other query clause is refused.
    reject_unhonored_query_clauses(query, QueryEnvelope::DirectSelect, "direct SELECT")?;
    let select = match query.body.as_ref() {
        SetExpr::SetOperation { .. } => return Err(derivation("set operation")),
        // Not the derivation template: CREATE VIEW refuses these bodies too.
        body => as_plain_select(body, "direct SELECT")?,
    };
    // A single-relation subquery is detected nowhere else: without this an EXISTS/IN would
    // surface as a bind error and a scalar subquery as a projection error.
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
    let factor = match classify_from(&select.from) {
        FromShape::Empty => {
            const CTX: &str = "SELECT without FROM";
            reject_unhonored_select_clauses(select, HonoredClauses::PLAIN, CTX)?;
            reject_if(select.selection.is_some(), CTX, "WHERE")?;
            let (schema, row) = plan_constant(query, select)?;
            return Ok(ReadPlan {
                case: ReadCase::Constant { schema, row },
                order: Vec::new(),
                window,
            });
        }
        FromShape::SinglePlainRelation(f) => f,
        FromShape::Derived(construct) => return Err(derivation(construct)),
    };
    let (name, alias) = extract_table_name_and_alias(factor, binder.schema_name(), "FROM")?;
    // DISTINCT wins the split: its fold does not group, so DISTINCT + GROUP BY keeps the
    // GROUP BY rejection.
    let distinct = select.distinct.is_some();
    let fold = distinct || body_is_grouped(select);
    let ctx = match (distinct, fold) {
        (true, _) => "SELECT DISTINCT",
        (false, true) => "aggregate SELECT",
        (false, false) => "direct SELECT",
    };
    reject_unhonored_select_clauses(select, HonoredClauses::for_body(fold, distinct), ctx)?;
    let desc = binder.resolve(cat, &name)?;
    // WHERE → access before either sink's shape, so a query unsupported on both axes names
    // the same one whichever sink it lands on.
    let access = plan_access(&desc, &alias, select)?;
    let (case, order) = if fold {
        plan_fold(select, query.order_by.as_ref(), ctx, name, &alias, desc, access)?
    } else {
        // OFFSET+LIMIT logical rows; `0` = unbounded (an OFFSET with no LIMIT too).
        let limit_k = window.end().map_or(0, |e| e as u64);
        let (reply_schema, sink, order) = rows_sink(
            &select.projection,
            query.order_by.as_ref(),
            &desc.schema,
            &alias,
            limit_k,
        )?;
        let read = SpecRead { name, desc, access, sink };
        (ReadCase::Rows { read, reply_schema }, order)
    };
    Ok(ReadPlan { case, order, window })
}

/// The WHERE → access step. The bound WHERE lives and dies here; only the owned
/// [`Access`] outlives it.
fn plan_access(desc: &RelDescriptor, alias: &str, select: &Select) -> Result<Access, GnitzSqlError> {
    let bound_where = bind_where(&desc.schema, alias, select.selection.as_ref())?;
    Ok(bound_and_predicate(&desc.schema, &bound_where, ReadBudget::OneRequest, &desc.indexes)?.access)
}

/// A GROUP BY / global aggregate / HAVING / DISTINCT read, bound by the front end a
/// grouped `CREATE VIEW` body uses, and its ORDER BY over the finished output.
fn plan_fold(
    select: &Select,
    order_by: Option<&OrderBy>,
    ctx: &str,
    name: String,
    alias: &str,
    desc: Arc<RelDescriptor>,
    access: Access,
) -> Result<(ReadCase, Vec<gnitz_wire::OrderKey>), GnitzSqlError> {
    // The keys bind with the SELECT list, so one over a column the grouping does
    // not cover rejects before the fold is dispatched, as HAVING already does.
    let keys = parse_order_by(order_by)?;
    let (pieces, order_cols) = bind_and_lower_fold(select, &desc.schema, alias, &order_exprs(&keys))?;
    // Over this sink's output, which lacks the group columns a view's reduce carries:
    // `SELECT COUNT(*) AS kind FROM t GROUP BY kind` is refused as a view, accepted here.
    reject_duplicate_projection_names(&select.projection, pieces.finalize.iter().map(|(_, d)| d), ctx)?;
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
    // The finalize items follow the hidden `_group_pk` `FoldFinish::new` prepends.
    let order = wire_keys(
        &keys,
        &finish.out_schema.columns,
        order_cols.iter().map(|&at| finish.out_schema.pk_cols.len() + at),
    )?;
    let case = ReadCase::Fold {
        read: SpecRead { name, desc, access, sink },
        finish: Box::new(finish),
        reduce_schema: pieces.reduce_schema,
        is_distinct: select.distinct.is_some(),
    };
    Ok((case, order))
}

/// A FROM-less SELECT's one row, finished at plan time: each item is a constant
/// expression, compiled as a fold's finalize item over the ground row.
fn plan_constant(query: &Query, select: &Select) -> Result<(Arc<Schema>, ZSetBatch), GnitzSqlError> {
    const CTX: &str = "SELECT without FROM";
    let ground = ground_partial_schema();
    // No relation is in scope: the ground row's one column is hidden, so every
    // written name is unresolvable, a qualified one included.
    let bind = |e: &sqlparser::ast::Expr| bind_single_table(e, &ground, "");
    let mut items: Vec<(BoundExpr, gnitz_core::ColumnDef)> = Vec::new();
    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = scalar_projection_item(item, CTX)?;
        let bound = bind(expr)?;
        let def = computed_column(alias, idx, bound.infer_ty(&ground.columns));
        items.push((bound, def));
    }
    reject_duplicate_projection_names(&select.projection, items.iter().map(|(_, d)| d), CTX)?;
    // One row sorts to itself: a key is refused where invalid, never placed.
    for key in &parse_order_by(query.order_by.as_ref())? {
        match key.target {
            OrderTarget::Position(pos) => reject_position_out_of_range(pos, items.len(), "ORDER BY")?,
            OrderTarget::Expr(e) => {
                if output_column(e, items.iter().map(|(_, d)| d))?.is_none() {
                    compile_scalar_evaluator(&bind(e)?, &ground)?;
                }
            }
        }
    }
    let finish = FoldFinish::new(ground, [], &[], items)?;
    let mut ground_row = ZSetBatch::with_capacity(&finish.partial_schema, 1);
    BatchAppender::new(&mut ground_row, &finish.partial_schema).add_row(gnitz_wire::global_group_key(), 1);
    Ok((Arc::clone(&finish.out_schema), finish.apply(ground_row)))
}

// ---------------------------------------------------------------------------
// The dispatch tail
// ---------------------------------------------------------------------------

/// Run one planned `SELECT`, reading local-first: a relation the client's copy
/// holds is answered off it, and every other one over the wire.
pub(crate) fn execute_select(client: &mut GnitzClient, plan: ReadPlan) -> Result<SqlResult, GnitzSqlError> {
    // `LIMIT 0` answers from the schema alone, with no request issued.
    if plan.window.limit == Some(0) {
        let schema = Arc::clone(plan.out_schema());
        return Ok(SqlResult::Rows { batch: ZSetBatch::new(&schema), schema });
    }
    let ReadPlan { case, order, window } = plan;
    let (schema, batch) = match case {
        ReadCase::Constant { schema, row } => (schema, row),
        ReadCase::Rows { read, reply_schema } => {
            let batch = fetch_bound(client, read.desc.tid, &read.access, &read.sink, &reply_schema)?;
            (reply_schema, batch)
        }
        ReadCase::Fold { read, finish, .. } => {
            // A wire error (the per-worker group cap included) is hard: the fold is
            // mid-flight on the workers and cannot fall back.
            let partial = fetch_bound(client, read.desc.tid, &read.access, &read.sink, &finish.partial_schema)?;
            (Arc::clone(&finish.out_schema), finish.apply(finish.combine(partial)))
        }
    };
    let batch = order_and_window(&schema, batch, &order, window);
    Ok(SqlResult::Rows { schema, batch })
}
