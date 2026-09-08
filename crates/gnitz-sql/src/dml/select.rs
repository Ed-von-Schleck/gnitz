//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (a `ReadSpec` rows sink executed
//! server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT top-k)
//! or, for aggregate / DISTINCT shapes, through the **fold sink** (a per-worker
//! hash-fold + client finishing). A FROM-less SELECT reads nothing: its one
//! constant row is the fold sink's client finish over the global ground row,
//! with no request issued.
//!
//! A query that *derives* a new relation — a JOIN, a set operation, an EXISTS/IN
//! or scalar subquery, a derived table, a grouped CTE — has no single-relation
//! sink; [`route_select`] rejects it from the AST alone (`reject_derivation`),
//! pointing at CREATE VIEW. A read the direct path merely cannot express is a
//! feature-named `Unsupported`, never that template. A `WITH` is expanded into
//! the body first (`dml::cte`), so a CTE reads through the flat query's sink.
//!
//! Planning is separate from dispatch, and the seam is [`ReadPlan`]:
//! [`plan_read`] validates, resolves, and decides the sink's access and reply
//! shape without reaching a server; [`execute_select`] runs the resulting plan and
//! `dml::explain` formats the same plan instead of dispatching it.

use crate::agg::ground_partial_schema;
use crate::ast_util::{
    body_is_grouped, classify_from, extract_table_name_and_alias, has_exists_in_subquery, has_scalar_subquery,
    is_bare_wildcard_projection, scalar_projection_item, DerivedFrom, FromShape,
};
use crate::bind::{bind_single_table, output_column, Binder};
use crate::codec::project_schema::{build_read_projection, read_reply_shape};
use crate::dml::cte::inline_ctes;
use crate::dml::group_by::build_fold_shape;
use crate::dml::plan::{bind_where, bound_and_predicate, fetch_bound, Access, ReadBudget};
use crate::error::{reject_if, GnitzSqlError};
use crate::exec::agg_finish::{agg_finish, build_agg_out_schema, FinalizeItem, FoldShape};
use crate::exec::order::{read_spec_finish, resolve_read_spec_order, wire_order, Window};
use crate::expr_lower::compile_scalar_evaluator;
use crate::ir::BoundExpr;
use crate::tail::{extract_limit, extract_offset, order_exprs, parse_order_by};
use crate::validate::{
    computed_column, order_column, reject_duplicate_projection_names, reject_unhonored_query_clauses,
    reject_unhonored_select_clauses, HonoredClauses, QueryEnvelope,
};
use crate::SqlResult;
use gnitz_core::{CatalogSnapshot, GnitzClient, RelDescriptor, Schema, ZSetBatch};
use gnitz_wire::{AggDescriptor, AggReadSpec, ReadSink};
use sqlparser::ast::{Query, Select, SetExpr, Statement};
use std::sync::Arc;

/// The single derivation-rejection: an ad-hoc SELECT reads one relation, but this
/// query derives a new one (`construct` names what was detected — `JOIN`, `set
/// operation`, `EXISTS/IN subquery`, `scalar subquery`, `derived table in FROM`,
/// `grouped CTE`). One template, one code path, asserted verbatim by tests. The
/// remedy is the product answer for a derived relation: a view, which the engine
/// maintains incrementally.
pub(super) fn reject_derivation<T>(construct: &str) -> Result<T, GnitzSqlError> {
    Err(GnitzSqlError::Unsupported(format!(
        "ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).\n\
         CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it."
    )))
}

/// The construct a deriving FROM shape is rejected as.
pub(super) fn from_construct(from: DerivedFrom) -> &'static str {
    match from {
        DerivedFrom::Join => "JOIN",
        DerivedFrom::CommaJoin => "comma-join FROM",
        DerivedFrom::DerivedTable => "derived table in FROM",
    }
}

/// An empty `Rows` result over `schema` — the `LIMIT 0` short-circuit both sinks
/// share (no request dispatched).
fn empty_rows(schema: Schema) -> SqlResult {
    let batch = ZSetBatch::new(&schema);
    SqlResult::Rows { schema, batch }
}

// ---------------------------------------------------------------------------
// route_select — validation, routing, relation resolution
// ---------------------------------------------------------------------------

/// The one relation an ad-hoc SELECT reads, as resolved.
pub(super) struct Target {
    /// The FROM item as written.
    pub(super) name: String,
    /// The FROM item's effective alias — the written one if there is one, else
    /// the relation name. A qualified reference in the query must name it.
    pub(super) alias: String,
    pub(super) tid: u64,
    pub(super) schema: Arc<Schema>,
    pub(super) desc: Arc<RelDescriptor>,
}

/// Which sink a validated ad-hoc SELECT lands on.
/// Which sink a validated ad-hoc SELECT lands on, carrying the relation that
/// sink reads. The target rides in the variant so the sink that reads nothing
/// holds nothing.
enum Sink {
    /// Bare `SELECT *` with no WHERE / ORDER BY / LIMIT / OFFSET: the
    /// unprojected full scan (`client.scan`), which builds no `ReadSpec`.
    PlainScan(Target),
    Rows(Target),
    Fold(Target),
    /// No FROM: one constant row, nothing read.
    Constant,
}

/// A validated ad-hoc SELECT: its sink and everything that sink needs.
/// `plan_read` consumes it on the way to a [`ReadPlan`], which is what both tails
/// see.
struct Route<'q> {
    select: &'q Select,
    window: Window,
    sink: Sink,
}

/// Resolve `name` to the read's target; the relation kind rides the resolution.
fn resolve_target(
    cat: &CatalogSnapshot,
    binder: &mut Binder<'_>,
    (name, alias): (String, String),
) -> Result<Target, GnitzSqlError> {
    let (tid, schema, desc) = binder.resolve(cat, &name)?;
    Ok(Target { name, alias, tid, schema, desc })
}

/// Validate an ad-hoc SELECT's shape, route it to its sink, and resolve the one
/// relation it reads. Both tails then run the same builders in the same order, so
/// describing a query that has no plan yields the identical rejection.
fn route_select<'q>(
    cat: &CatalogSnapshot,
    query: &'q Query,
    binder: &mut Binder<'_>,
) -> Result<Route<'q>, GnitzSqlError> {
    // Envelope guard: ORDER BY / LIMIT / OFFSET are applied by the client-side
    // ordering sink over the fetched batch, and WITH was expanded before routing.
    // Everything else is rejected up front so nothing is silently dropped.
    reject_unhonored_query_clauses(query, QueryEnvelope::DirectSelect, "direct SELECT")?;
    // Step 1 — body kind. A set-op body derives; a plain SELECT continues; every
    // other body (VALUES, a parenthesized query, TABLE t) is a plain `Unsupported`
    // naming the shape — NOT the derivation template, because CREATE VIEW rejects
    // the identical bodies, so its "CREATE VIEW AS <your query>" advice would be false.
    let select = match query.body.as_ref() {
        SetExpr::SetOperation { .. } => return reject_derivation("set operation"),
        SetExpr::Select(s) => s.as_ref(),
        other => {
            return Err(GnitzSqlError::Unsupported(format!(
                "direct SELECT does not support a {} body",
                match other {
                    SetExpr::Query(_) => "parenthesized subquery",
                    SetExpr::Values(_) => "VALUES",
                    SetExpr::Table(_) => "TABLE",
                    SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) => "DML",
                    _ => "non-SELECT",
                }
            )))
        }
    };

    // Step 2 — subquery walk over the selection and projection. A single-relation
    // subquery is detected by no other step; without this an EXISTS/IN in the
    // WHERE would surface as a low-level bind error and a projected scalar subquery
    // as a generic projection error. EXISTS/IN (`has_exists_in_subquery`) is
    // checked before the scalar / ANY / ALL forms (`has_scalar_subquery`),
    // mirroring the view-shape classifier.
    if has_exists_in_subquery(select) {
        return reject_derivation("EXISTS/IN subquery");
    }
    if has_scalar_subquery(select) {
        return reject_derivation("scalar subquery");
    }

    let window = Window {
        limit: extract_limit(query)?,
        offset: extract_offset(query)?,
    };

    // Step 3 — FROM shape. No FROM reads nothing; every multi-relation or
    // derived shape derives a new relation, which is what a view is for — one
    // template for all of them.
    let from_factor = match classify_from(&select.from) {
        FromShape::Empty => {
            const CTX: &str = "SELECT without FROM";
            reject_unhonored_select_clauses(select, HonoredClauses::PLAIN, CTX)?;
            reject_if(select.selection.is_some(), CTX, "WHERE")?;
            return Ok(Route { select, window, sink: Sink::Constant });
        }
        FromShape::SinglePlainRelation(f) => f,
        FromShape::Derived(d) => return reject_derivation(from_construct(d)),
    };
    let from = extract_table_name_and_alias(from_factor, binder.schema_name(), "FROM")?;

    // Step 4 — aggregate / DISTINCT shapes fold via the fold sink. DISTINCT takes
    // precedence over GROUP BY (its arm rejects GROUP BY), matching the view path.
    if select.distinct.is_some() || body_is_grouped(select) {
        let is_distinct = select.distinct.is_some();
        // The routing split sits above the shared clause gate, so invoke it here —
        // `grouping`/`distinct` per query. DISTINCT's arm sets `grouping:false`, so a
        // query with both DISTINCT and GROUP BY is rejected (DISTINCT-first). The
        // unconditional PREWHERE/TOP/QUALIFY/DISTINCT-ON rejection is preserved.
        reject_unhonored_select_clauses(
            select,
            HonoredClauses::for_body(true, is_distinct),
            if is_distinct {
                "SELECT DISTINCT"
            } else {
                "aggregate SELECT"
            },
        )?;
        return Ok(Route {
            select,
            window,
            sink: Sink::Fold(resolve_target(cat, binder, from)?),
        });
    }

    // Body-clause guard: WHERE and the projection are honored; DISTINCT / grouping
    // routed above; the exotic tail (PREWHERE, TOP, QUALIFY, …) rejects here.
    reject_unhonored_select_clauses(select, HonoredClauses::PLAIN, "direct SELECT")?;

    let target = resolve_target(cat, binder, from)?;

    // A plain `SELECT *` (no EXCEPT/RENAME/… modifiers) with no WHERE / ORDER BY /
    // LIMIT / OFFSET reads through `client.scan` rather than a `ReadSpec`: the rows
    // come back verbatim with no per-row projection program, and the reply carries
    // the server's own schema block, which warms the client schema cache (a
    // `ReadSpec` ships a per-query projected schema and never touches that cache).
    // A modifier-bearing wildcard falls through to the rows sink, which expands
    // it, as does a DROP COLUMN'd base table — its hidden *payload* slot must be
    // projected away rather than leaked. A view's hidden synthetic *key* slots do
    // not: those are filtered at presentation.
    let bare_star = is_bare_wildcard_projection(&select.projection);
    let sink = if bare_star
        && !target.schema.has_hidden_payload()
        && select.selection.is_none()
        && query.order_by.is_none()
        && window.limit.is_none()
        && window.offset == 0
    {
        Sink::PlainScan(target)
    } else {
        Sink::Rows(target)
    };

    Ok(Route { select, window, sink })
}

// ---------------------------------------------------------------------------
// The planning seam
// ---------------------------------------------------------------------------

/// Which sink a finished [`ReadPlan`] lands on. Fieldless: the sink's own
/// description stays in-crate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadKind {
    PlainScan,
    Rows,
    Fold,
    Constant,
}

/// A finished ad-hoc read: the relation it reads plus everything its sink needs,
/// fully owned and borrowing nothing from the AST.
///
/// A `pub` struct wrapping a private enum, so the variants' fields stay in-crate
/// where the two tails match on them; the accessors below are what leaves.
pub struct ReadPlan {
    pub(super) case: ReadCase,
}

/// The sink a finished read lands on, carrying the relation it reads. The
/// constant row reads none, so it holds none.
pub(super) enum ReadCase {
    /// The unprojected full scan: no `ReadSpec` at all, the relation read whole.
    PlainScan(Target),
    /// Boxed so a variant shipping no `ReadSpec` does not carry the sink shapes'
    /// schemas.
    Spec(Box<SpecRead>),
    /// One constant row: the fold sink's client finish over the global ground
    /// row, with no request issued.
    Constant(Box<ConstRead>),
}

pub(super) struct ConstRead {
    /// Zero groups, zero aggregates: every finalize item is a constant
    /// expression, evaluated over the one ground row.
    pub(super) shape: FoldShape,
    pub(super) order: Vec<gnitz_wire::OrderKey>,
    pub(super) window: Window,
}

/// A read that ships a `ReadSpec`: the pushed-down access, the sink it ships
/// under, and the client-side window. These are the same for both sinks; only
/// [`SpecRead::tail`] differs.
pub(super) struct SpecRead {
    pub(super) target: Target,
    pub(super) access: Access,
    pub(super) sink: ReadSink,
    /// The ORDER BY keys, over the reply columns. For the rows sink these are the
    /// keys the worker's top-k selects by *and* the client re-sorts the
    /// concatenation by; the fold resolves them over its own output schema and
    /// finishes client-side.
    pub(super) order: Vec<gnitz_wire::OrderKey>,
    pub(super) window: Window,
    pub(super) tail: SinkTail,
}

/// What the client does with the reply.
pub(super) enum SinkTail {
    /// Decode against `reply_schema` and window. `Arc` because `fetch_bound`
    /// may chunk one read into several requests, and each would otherwise
    /// deep-clone the schema onto its pending slot.
    Rows { reply_schema: Arc<Schema> },
    /// Finish the partial fold, then window.
    Fold {
        shape: Box<FoldShape>,
        /// Whether the shape came from `DISTINCT`. Nothing in the layout
        /// distinguishes a DISTINCT fold from a zero-aggregate GROUP BY, and
        /// `EXPLAIN` names them differently.
        is_distinct: bool,
    },
}

impl ReadPlan {
    pub fn kind(&self) -> ReadKind {
        match &self.case {
            ReadCase::PlainScan(_) => ReadKind::PlainScan,
            ReadCase::Spec(s) => match s.tail {
                SinkTail::Rows { .. } => ReadKind::Rows,
                SinkTail::Fold { .. } => ReadKind::Fold,
            },
            ReadCase::Constant(_) => ReadKind::Constant,
        }
    }

    /// The relation this read names; `None` for a FROM-less SELECT.
    pub fn target_id(&self) -> Option<u64> {
        self.target().map(|t| t.tid)
    }

    /// The relation this read names, `None` for the constant row.
    pub(super) fn target(&self) -> Option<&Target> {
        match &self.case {
            ReadCase::PlainScan(t) => Some(t),
            ReadCase::Spec(s) => Some(&s.target),
            ReadCase::Constant(_) => None,
        }
    }

    /// The schema the result is built under; `None` for `PlainScan`, which
    /// replies under the relation's own schema.
    pub fn reply_schema(&self) -> Option<&Schema> {
        match &self.case {
            ReadCase::PlainScan(_) => None,
            ReadCase::Spec(s) => Some(s.reply_schema()),
            ReadCase::Constant(c) => Some(&c.shape.out_schema),
        }
    }

    /// The encoded `ReadSpec` this read ships; `None` when none is. One, not a
    /// list: an over-cap `PkSet` gather needs `ReadBudget::MayChunk`, and a read
    /// plans under `OneRequest`.
    pub fn encoded_spec(&self) -> Option<Vec<u8>> {
        match &self.case {
            ReadCase::Spec(s) => Some(s.access.encode(s.access.bound(), &s.sink)),
            ReadCase::PlainScan(_) | ReadCase::Constant(_) => None,
        }
    }

    /// Whether running this plan issues a request at all. False for `LIMIT 0`,
    /// which answers empty without one, and for a constant row.
    pub fn dispatches(&self) -> bool {
        match &self.case {
            ReadCase::PlainScan(_) => true,
            ReadCase::Spec(s) => s.window.limit != Some(0),
            ReadCase::Constant(_) => false,
        }
    }
}

impl SpecRead {
    /// The schema this read's reply decodes against — the projected reply for the
    /// rows sink, the partial fold for the fold sink.
    pub(super) fn reply_schema(&self) -> &Schema {
        match &self.tail {
            SinkTail::Rows { reply_schema } => reply_schema,
            SinkTail::Fold { shape, .. } => &shape.partial_schema,
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
    // A fresh binder per pass: the cache a re-run's discarded pass filled must
    // not reach the next one.
    let mut binder = Binder::new(schema_name);
    let flat = inline_ctes(cat, &mut binder, query)?;
    let query = flat.as_ref().unwrap_or(query);
    let Route { select, window, sink } = route_select(cat, query, &mut binder)?;
    let case = match sink {
        Sink::PlainScan(target) => ReadCase::PlainScan(target),
        Sink::Rows(target) => ReadCase::Spec(Box::new(plan_rows_read(query, select, window, target)?)),
        Sink::Fold(target) => ReadCase::Spec(Box::new(plan_fold_read(query, select, window, target)?)),
        Sink::Constant => ReadCase::Constant(Box::new(plan_const_read(query, select, window)?)),
    };
    Ok(ReadPlan { case })
}

/// The WHERE → access step. Both sinks take it before building their own shape,
/// so a query unsupported on both axes names the same one whichever sink it
/// routes to. The bound WHERE lives and dies here; only the owned [`Access`]
/// outlives it.
fn plan_access(target: &Target, select: &Select) -> Result<Access, GnitzSqlError> {
    let bound_where = bind_where(&target.schema, &target.alias, select.selection.as_ref())?;
    let indexes = &target.desc.indexes[..];
    let plan = bound_and_predicate(&target.schema, &bound_where, ReadBudget::OneRequest, indexes)?;
    Ok(plan.access)
}

fn plan_rows_read(query: &Query, select: &Select, window: Window, target: Target) -> Result<SpecRead, GnitzSqlError> {
    let access = plan_access(&target, select)?;
    let shape = build_rows_shape(select, query, &target.schema, &target.alias)?;
    // OFFSET+LIMIT logical rows; `0` = unbounded (an OFFSET with no LIMIT too).
    let limit_k = window
        .limit
        .map(|l| l.saturating_add(window.offset) as u64)
        .unwrap_or(0);
    Ok(SpecRead {
        target,
        access,
        sink: ReadSink::Rows {
            projection: shape.projection,
            order: shape.order.clone(),
            limit_k,
        },
        order: shape.order,
        window,
        tail: SinkTail::Rows {
            reply_schema: Arc::new(shape.reply_schema),
        },
    })
}

fn plan_fold_read(query: &Query, select: &Select, window: Window, target: Target) -> Result<SpecRead, GnitzSqlError> {
    let access = plan_access(&target, select)?;
    // The keys bind with the SELECT list, so one over a column the grouping does
    // not cover rejects before the fold is dispatched, as HAVING already does.
    let keys = parse_order_by(query.order_by.as_ref())?;
    let (shape, order) = build_fold_shape(select, &target.schema, &target.alias, &keys)?;
    // `group_cols` / `src_col` index the reduce input — the pre-map's output when
    // the fold carries one, which is exactly what `shape` resolved them against.
    let sink = ReadSink::Fold(AggReadSpec {
        group_cols: shape.group_positions.iter().map(|&c| c as u32).collect(),
        aggs: shape
            .agg_specs
            .iter()
            .map(|s| AggDescriptor { agg_op: s.op, col_idx: s.col as u32 })
            .collect(),
        pre: shape.pre.clone(),
    });
    Ok(SpecRead {
        target,
        access,
        sink,
        order,
        window,
        tail: SinkTail::Fold {
            is_distinct: select.distinct.is_some(),
            shape: Box::new(shape),
        },
    })
}

/// A FROM-less SELECT has no columns: each item is a constant expression,
/// compiled as a fold's finalize item over the ground row; an ORDER BY
/// expression rides as a hidden one.
fn plan_const_read(query: &Query, select: &Select, window: Window) -> Result<ConstRead, GnitzSqlError> {
    const CTX: &str = "SELECT without FROM";
    let ground = Arc::new(ground_partial_schema());
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
    // A key that is not an output item is bound and appended as a hidden one.
    let keys = parse_order_by(query.order_by.as_ref())?;
    let mut placed = Vec::new();
    for (i, e) in order_exprs(&keys).into_iter().enumerate() {
        placed.push(match output_column(e, items.iter().map(|(_, d)| d))? {
            Some(at) => at,
            None => {
                let bound = bind(e)?;
                let def = order_column(i, bound.infer_ty(&ground.columns));
                items.push((bound, def));
                items.len() - 1
            }
        });
    }
    reject_duplicate_projection_names(&select.projection, items.iter().map(|(_, d)| d), CTX)?;
    let finalize = items
        .iter()
        .map(|(bound, _)| {
            Ok(FinalizeItem::Computed {
                ev: Box::new(compile_scalar_evaluator(bound, &ground)?),
            })
        })
        .collect::<Result<_, GnitzSqlError>>()?;
    let (out_schema, base) = build_agg_out_schema(items.into_iter().map(|(_, d)| d).collect())?;
    let order = wire_order(&keys, &out_schema, &placed, base)?;
    Ok(ConstRead {
        shape: FoldShape {
            reduce_schema: Arc::clone(&ground),
            group_positions: Vec::new(),
            agg_specs: Vec::new(),
            pre: None,
            partial_schema: ground,
            out_schema,
            having: None,
            finalize,
        },
        order,
        window,
    })
}

// ---------------------------------------------------------------------------
// The dispatch tail
// ---------------------------------------------------------------------------

/// Run one planned `SELECT`, reading local-first: a relation the client's copy
/// holds is answered off it, and every other one over the wire.
pub(crate) fn execute_select(client: &mut GnitzClient, plan: ReadPlan) -> Result<SqlResult, GnitzSqlError> {
    let spec = match plan.case {
        ReadCase::PlainScan(target) => {
            // The served LSN is dropped here: a `SELECT` holds no cursor across
            // reads, and a local answer carries none anyway.
            let (schema_out, batch_opt, _lsn) = client.scan_local_first(target.tid)?;
            let out_schema = schema_out
                .map(|s| (*s).clone())
                .unwrap_or_else(|| (*target.schema).clone());
            let batch = batch_opt.unwrap_or_else(|| ZSetBatch::new(&out_schema));
            return Ok(SqlResult::Rows { schema: out_schema, batch });
        }
        ReadCase::Constant(c) => {
            let ConstRead { shape, order, window } = *c;
            if window.limit == Some(0) {
                return Ok(empty_rows(shape.out_schema));
            }
            // No partials: the finish synthesizes the ground row and projects it.
            let batch = agg_finish(&shape, &ZSetBatch::new(&shape.partial_schema));
            let (schema, batch) = read_spec_finish(shape.out_schema, batch, &order, window);
            return Ok(SqlResult::Rows { schema, batch });
        }
        ReadCase::Spec(spec) => spec,
    };
    let SpecRead {
        target,
        access,
        sink,
        order,
        window,
        tail,
    } = *spec;
    let tid = target.tid;
    // The output schema the window runs over, and the batch to window. `LIMIT 0`
    // answers from the schema alone, with no request issued.
    let (out_schema, batch) = match tail {
        SinkTail::Rows { reply_schema } => {
            if window.limit == Some(0) {
                return Ok(empty_rows(Arc::unwrap_or_clone(reply_schema)));
            }
            let batch = fetch_bound(client, tid, &access, &sink, &reply_schema)?;
            (Arc::unwrap_or_clone(reply_schema), batch)
        }
        SinkTail::Fold { shape, .. } => {
            if window.limit == Some(0) {
                return Ok(empty_rows(shape.out_schema));
            }
            // A wire error (including the runtime per-worker group cap) is hard: by
            // now the fold is mid-flight on the workers and cannot fall back.
            let partial = fetch_bound(client, tid, &access, &sink, &shape.partial_schema)?;
            // Client finishing: combine by group value, ground row, AVG/NullfillSum,
            // HAVING, projection.
            let out_batch = agg_finish(&shape, &partial);
            (shape.out_schema, out_batch)
        }
    };
    // Sort the concatenation by the wire keys, window, present.
    let (schema, batch) = read_spec_finish(out_schema, batch, &order, window);
    Ok(SqlResult::Rows { schema, batch })
}

// ---------------------------------------------------------------------------
// The sink shapes
// ---------------------------------------------------------------------------

/// The rows sink's reply shape: what the worker projects and how it orders.
struct RowsShape {
    reply_schema: Schema,
    /// The compiled projection-map program.
    projection: Vec<u8>,
    order: Vec<gnitz_wire::OrderKey>,
}

/// The `(reply schema, projection, ORDER BY keys)` a rows read replies under — a
/// pure function of the AST and the source schema. A shape the read spec cannot
/// express (a qualified-wildcard / other non-map projection) surfaces the
/// offending resolver's own feature-named `Unsupported`; an unknown column is a
/// `Bind` error.
fn build_rows_shape(select: &Select, query: &Query, schema: &Schema, alias: &str) -> Result<RowsShape, GnitzSqlError> {
    // Projection items — the source PK hidden-prepended to slots `0..k`, then
    // every SELECT item as a payload slot in SELECT order.
    let (mut items, mut out_cols) = build_read_projection(&select.projection, schema, alias)?;

    // ORDER BY keys over the reply columns; a key that is not an output column
    // is appended as a hidden payload column (so it can still order the result).
    let keys = parse_order_by(query.order_by.as_ref())?;
    let order = resolve_read_spec_order(&mut items, &mut out_cols, schema, alias, &keys)?;

    // Reply schema + projection blob.
    let (reply_schema, projection) = read_reply_shape(&items, out_cols, schema)?;
    Ok(RowsShape { reply_schema, projection, order })
}
