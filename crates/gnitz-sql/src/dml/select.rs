//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (a `ReadSpec` rows sink executed
//! server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT top-k)
//! or, for aggregate / DISTINCT shapes, through the **fold sink** (a per-worker
//! hash-fold + client finishing).
//!
//! A query that *derives* a new relation — a JOIN, a set operation, an EXISTS/IN
//! or scalar subquery, a derived table, a non-pass-through CTE — has no
//! single-relation sink; [`route_select`] rejects it from the AST alone
//! (`reject_derivation`) with one actionable message pointing at CREATE VIEW,
//! which maintains the derived relation incrementally. A single-relation read the
//! direct path cannot express (a LIKE whose pattern is not a literal, an ORDER BY
//! expression) is a feature-named `Unsupported`, never the derivation template. A
//! pass-through CTE over one relation is inlined (`cte_passthrough`) so trivial
//! `WITH` queries keep reading through the direct path.
//!
//! Planning is separate from dispatch, and the seam is [`ReadPlan`]:
//! [`plan_read`] validates, resolves, and decides the sink's access and reply
//! shape without reaching a server; [`execute_select`] runs the resulting plan and
//! `dml::explain` formats the same plan instead of dispatching it.

use crate::agg::{synthetic_fold_cols, GroupByLayout};
use crate::ast_util::{
    body_is_grouped, classify_from, extract_table_factor_name, has_exists_in_subquery, has_scalar_subquery,
    is_bare_wildcard_projection, FromShape,
};
use crate::bind::{cte_passthrough, Binder};
use crate::codec::project_schema::{build_read_projection, read_reply_shape};
use crate::dml::group_by::{analyze_group_by, bind_having_expr, resolve_set_projection, HavingCtx};
use crate::dml::plan::{
    bind_where, bound_and_predicate, extract_limit, extract_offset, fetch_bound, Access, ReadBudget,
};
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::{agg_finish, build_agg_out_schema, FoldShape};
use crate::exec::order::{read_spec_finish, resolve_out_schema_order, resolve_read_spec_order};
use crate::expr_lower::compile_conjuncts_evaluator;
use crate::validate::{
    cte_select_body, non_recursive_ctes, reject_unhonored_query_clauses, reject_unhonored_select_clauses,
    HonoredClauses, HonoredQueryClauses,
};
use crate::SqlResult;
use gnitz_core::{CatalogSnapshot, GnitzClient, ReduceOutKey, RelDescriptor, Schema, ZSetBatch, MAX_COLUMNS};
use gnitz_wire::{AggReadItem, AggReadSpec, ReadSink};
use sqlparser::ast::{LimitClause, Query, Select, SetExpr, Statement};
use std::sync::Arc;

/// The single derivation-rejection: an ad-hoc SELECT reads one relation, but this
/// query derives a new one (`construct` names what was detected — `JOIN`, `set
/// operation`, `EXISTS/IN subquery`, `scalar subquery`, `derived table in FROM`,
/// `non-pass-through CTE`). One template, one code path, asserted verbatim by
/// tests. The remedy is the product answer for a derived relation: a view, which
/// the engine maintains incrementally.
fn reject_derivation<T>(construct: &str) -> Result<T, GnitzSqlError> {
    Err(GnitzSqlError::Unsupported(format!(
        "ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).\n\
         CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it."
    )))
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
    /// The FROM item as written — a pass-through CTE keeps its alias.
    pub(super) name: String,
    pub(super) tid: u64,
    pub(super) schema: Arc<Schema>,
    /// `None` is a chain-minted segment id, which never reaches here: a derived
    /// table in FROM is rejected as a derivation before any resolution.
    ///
    /// `None` only for a chain-minted id, which an ad-hoc read never names: a
    /// derived table or non-pass-through CTE is rejected as a derivation first.
    pub(super) desc: Option<Arc<RelDescriptor>>,
}

/// Which sink a validated ad-hoc SELECT lands on.
enum Sink {
    /// Bare `SELECT *` with no WHERE / ORDER BY / LIMIT / OFFSET: the
    /// unprojected full scan (`client.scan`), which builds no `ReadSpec`.
    PlainScan,
    Rows,
    Fold,
}

/// A validated ad-hoc SELECT: its sink and everything that sink needs.
/// `plan_read` consumes it on the way to a [`ReadPlan`], which is what both tails
/// see.
struct Route<'q> {
    target: Target,
    select: &'q Select,
    limit: Option<usize>,
    offset: usize,
    sink: Sink,
}

/// Resolve `name` to the read's target; the relation kind rides the resolution.
fn resolve_target(cat: &CatalogSnapshot, binder: &mut Binder<'_>, name: String) -> Result<Target, GnitzSqlError> {
    let (tid, schema, desc) = binder.resolve(cat, &name)?;
    Ok(Target {
        name,
        tid,
        schema,
        desc,
    })
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
    // ordering sink over the fetched batch, and WITH is inlined below
    // (`cte_passthrough`). Everything else (FETCH, FOR UPDATE/SHARE, SETTINGS,
    // FORMAT, pipe operators) is rejected up front so nothing is silently dropped.
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            with: true,
            ordering_sink: true,
        },
        "direct SELECT",
    )?;
    // The `LIMIT … BY` (ClickHouse per-group) sub-form has no operator here, so
    // reject it rather than silently accept-and-ignore it.
    if let Some(LimitClause::LimitOffset { limit_by, .. }) = &query.limit_clause {
        if !limit_by.is_empty() {
            return Err(GnitzSqlError::Unsupported(
                "LIMIT ... BY is not supported in direct SELECT".to_string(),
            ));
        }
    }

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

    // Step 2 — CTE inlining. Every CTE must be a pass-through over one relation,
    // aliased into the binder cache (the source table's real catalog id, so a
    // downstream `WHERE indexed = 5` keeps its bound); a non-pass-through CTE
    // derives, and one such CTE rejects the whole query.
    for cte in non_recursive_ctes(query)? {
        let ctx = format!("CTE '{}'", cte.alias.name.value);
        let cte_select = cte_select_body(cte, &ctx)?;
        // Clauses CREATE VIEW cannot serve on a CTE either (DISTINCT, the exotic
        // tail: PREWHERE, TOP, QUALIFY, …) keep their targeted error — the
        // derivation template's CREATE VIEW advice would be false for them.
        reject_unhonored_select_clauses(cte_select, HonoredClauses::for_body(true, false), &ctx)?;
        // A WHERE'd / grouped / aggregated CTE derives a new relation, and a
        // view genuinely serves it (as a hidden segment) — so does any
        // non-pass-through body `cte_passthrough` refuses (a joined / derived
        // FROM, a non-identity projection): the derivation advice is true here.
        if cte_select.selection.is_some() || cte_select.having.is_some() || body_is_grouped(cte_select) {
            return reject_derivation("non-pass-through CTE");
        }
        match cte_passthrough(cat, cte_select, &cte.alias.columns, binder)? {
            Some(resolved) => binder.cache_alias(&cte.alias.name.value, resolved)?,
            None => return reject_derivation("non-pass-through CTE"),
        }
    }

    // Step 3 — FROM shape. Zero FROM is a plain `Unsupported`; an explicit JOIN
    // or a derived table derives (a view serves both); a comma-join is a shape
    // CREATE VIEW rejects too, so it gets its own advice (rewrite as an explicit
    // JOIN first) instead of the derivation template.
    match classify_from(&select.from) {
        FromShape::Empty => {
            return Err(GnitzSqlError::Unsupported(
                "direct SELECT without FROM is not supported".to_string(),
            ))
        }
        FromShape::Join => return reject_derivation("JOIN"),
        FromShape::CommaJoin => {
            return Err(GnitzSqlError::Unsupported(
                "comma-join FROM (FROM a, b) is not supported; rewrite it as an explicit JOIN … ON …, then \
                 CREATE VIEW <name> AS <that query> and SELECT from it"
                    .to_string(),
            ))
        }
        FromShape::DerivedTable => return reject_derivation("derived table in FROM"),
        FromShape::SinglePlainRelation => {}
    }

    // Step 4 — subquery walk over the selection and projection. A single-relation
    // subquery is detected by no earlier step; without this an EXISTS/IN in the
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

    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;
    // Step 3 established that `from[0]` is a plain table/view, so this only
    // rejects an exotic table qualifier (a table function, AS OF, …).
    let table_name = extract_table_factor_name(&select.from[0].relation, "FROM")?;

    // Step 5 — aggregate / DISTINCT shapes fold via the fold sink. DISTINCT takes
    // precedence over GROUP BY (its arm rejects GROUP BY), matching the view path.
    if select.distinct.is_some() || select.having.is_some() || body_is_grouped(select) {
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
        let target = resolve_target(cat, binder, table_name)?;
        return Ok(Route {
            target,
            select,
            limit,
            offset,
            sink: Sink::Fold,
        });
    }

    // Body-clause guard: WHERE and the projection are honored; DISTINCT / grouping
    // routed above; the exotic tail (PREWHERE, TOP, QUALIFY, …) rejects here.
    reject_unhonored_select_clauses(select, HonoredClauses::PLAIN, "direct SELECT")?;

    let target = resolve_target(cat, binder, table_name)?;

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
        && limit.is_none()
        && offset == 0
    {
        Sink::PlainScan
    } else {
        Sink::Rows
    };

    Ok(Route {
        target,
        select,
        limit,
        offset,
        sink,
    })
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
}

/// A finished ad-hoc read: the relation it reads plus everything its sink needs,
/// fully owned and borrowing nothing from the AST.
///
/// A `pub` struct wrapping a private enum, so the variants' fields stay in-crate
/// where the two tails match on them; the accessors below are what leaves.
pub struct ReadPlan {
    pub(super) target: Target,
    pub(super) case: ReadCase,
}

pub(super) enum ReadCase {
    /// The unprojected full scan: no `ReadSpec` at all, the relation read whole.
    PlainScan,
    /// Boxed so the plain scan, which carries nothing, does not widen the plan to
    /// the sink shapes' schemas.
    Spec(Box<SpecRead>),
}

/// A read that ships a `ReadSpec`: the pushed-down access, the sink it ships
/// under, and the client-side window. These are the same for both sinks; only
/// [`SpecRead::tail`] differs.
pub(super) struct SpecRead {
    pub(super) access: Access,
    pub(super) sink: ReadSink,
    /// The ORDER BY keys, over the reply columns. For the rows sink these are the
    /// keys the worker's top-k selects by *and* the client re-sorts the
    /// concatenation by; the fold resolves them over its own output schema and
    /// finishes client-side.
    pub(super) order: Vec<gnitz_wire::OrderKey>,
    pub(super) offset: usize,
    pub(super) limit: Option<usize>,
    pub(super) tail: SinkTail,
}

/// What the client does with the reply.
pub(super) enum SinkTail {
    /// Decode against `reply_schema` and window.
    Rows { reply_schema: Schema },
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
            ReadCase::PlainScan => ReadKind::PlainScan,
            ReadCase::Spec(s) => match s.tail {
                SinkTail::Rows { .. } => ReadKind::Rows,
                SinkTail::Fold { .. } => ReadKind::Fold,
            },
        }
    }

    /// The relation this read names.
    pub fn target_id(&self) -> u64 {
        self.target.tid
    }

    /// The schema the reply decodes against; `None` for `PlainScan`, which
    /// replies under the relation's own schema.
    pub fn reply_schema(&self) -> Option<&Schema> {
        match &self.case {
            ReadCase::PlainScan => None,
            ReadCase::Spec(s) => Some(s.reply_schema()),
        }
    }

    /// The encoded `ReadSpec` this read ships; `None` for `PlainScan`. One, not a
    /// list: an over-cap `PkSet` gather needs `ReadBudget::MayChunk`, and a read
    /// plans under `OneRequest`.
    pub fn encoded_spec(&self) -> Option<Vec<u8>> {
        match &self.case {
            ReadCase::PlainScan => None,
            ReadCase::Spec(s) => Some(s.access.encode(s.access.bound(), &s.sink)),
        }
    }

    /// Whether running this plan issues a request at all. False for `LIMIT 0`,
    /// which answers empty without one.
    pub fn dispatches(&self) -> bool {
        match &self.case {
            ReadCase::PlainScan => true,
            ReadCase::Spec(s) => s.limit != Some(0),
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
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(q) => q.as_ref(),
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "EXPLAIN describes a SELECT; this statement is not one".to_string(),
                ))
            }
        },
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "plan_read describes a SELECT or the EXPLAIN of one; this statement is neither".to_string(),
            ))
        }
    };
    // A fresh binder per pass: the alias cache a re-run's discarded pass filled
    // must not reach the next one.
    let mut binder = Binder::new(schema_name);
    let route = route_select(cat, query, &mut binder)?;
    let case = match route.sink {
        Sink::PlainScan => ReadCase::PlainScan,
        Sink::Rows => ReadCase::Spec(Box::new(plan_rows_read(query, &route)?)),
        Sink::Fold => ReadCase::Spec(Box::new(plan_fold_read(query, &route)?)),
    };
    Ok(ReadPlan {
        target: route.target,
        case,
    })
}

/// The WHERE → access step. Both sinks take it before building their own shape,
/// so a query unsupported on both axes names the same one whichever sink it
/// routes to. The bound WHERE lives and dies here; only the owned [`Access`]
/// outlives it.
fn plan_access(target: &Target, select: &Select) -> Result<Access, GnitzSqlError> {
    let bound_where = bind_where(&target.schema, select.selection.as_ref())?;
    let indexes = target.desc.as_ref().map(|d| &d.indexes[..]).unwrap_or_default();
    let plan = bound_and_predicate(&target.schema, bound_where.as_ref(), ReadBudget::OneRequest, indexes)?;
    Ok(plan.access)
}

fn plan_rows_read(query: &Query, route: &Route<'_>) -> Result<SpecRead, GnitzSqlError> {
    let (target, select) = (&route.target, route.select);
    let access = plan_access(target, select)?;
    let shape = build_rows_shape(select, query, &target.schema)?;
    // OFFSET+LIMIT logical rows; `0` = unbounded (an OFFSET with no LIMIT too).
    let limit_k = route.limit.map(|l| l.saturating_add(route.offset) as u64).unwrap_or(0);
    Ok(SpecRead {
        access,
        sink: ReadSink::Rows {
            projection: shape.projection,
            order: shape.order.clone(),
            limit_k,
        },
        order: shape.order,
        offset: route.offset,
        limit: route.limit,
        tail: SinkTail::Rows {
            reply_schema: shape.reply_schema,
        },
    })
}

fn plan_fold_read(query: &Query, route: &Route<'_>) -> Result<SpecRead, GnitzSqlError> {
    let (target, select) = (&route.target, route.select);
    let access = plan_access(target, select)?;
    let shape = build_fold_shape(select, &target.schema)?;
    // Resolved against the finished output schema at plan time, so a key naming a
    // missing column rejects before the fold is dispatched, as HAVING already does.
    let order = resolve_out_schema_order(query.order_by.as_ref(), &shape.out_schema)?;
    let sink = ReadSink::Fold(AggReadSpec {
        group_cols: shape.layout.group_col_indices.iter().map(|&c| c as u16).collect(),
        aggs: shape
            .layout
            .agg_specs
            .iter()
            .map(|s| AggReadItem {
                op: s.op,
                src_col: s.col as u16,
            })
            .collect(),
    });
    Ok(SpecRead {
        access,
        sink,
        order,
        offset: route.offset,
        limit: route.limit,
        tail: SinkTail::Fold {
            is_distinct: select.distinct.is_some(),
            shape: Box::new(shape),
        },
    })
}

// ---------------------------------------------------------------------------
// The dispatch tail
// ---------------------------------------------------------------------------

/// Run one planned `SELECT`, reading local-first: a relation the client's copy
/// holds is answered off it, and every other one over the wire.
pub(crate) fn execute_select(client: &mut GnitzClient, plan: ReadPlan) -> Result<SqlResult, GnitzSqlError> {
    let tid = plan.target.tid;
    let ReadCase::Spec(spec) = plan.case else {
        // The served LSN is dropped here: a `SELECT` holds no cursor across
        // reads, and a local answer carries none anyway.
        let (schema_out, batch_opt, _lsn) = client.scan_local_first(tid)?;
        let out_schema = schema_out
            .map(|s| (*s).clone())
            .unwrap_or_else(|| (*plan.target.schema).clone());
        let batch = batch_opt.unwrap_or_else(|| ZSetBatch::new(&out_schema));
        return Ok(SqlResult::Rows {
            schema: out_schema,
            batch,
        });
    };
    let SpecRead {
        access,
        sink,
        order,
        offset,
        limit,
        tail,
    } = *spec;
    // The output schema the window runs over, and the batch to window. `LIMIT 0`
    // answers from the schema alone, with no request issued.
    let (out_schema, batch) = match tail {
        SinkTail::Rows { reply_schema } => {
            if limit == Some(0) {
                return Ok(empty_rows(reply_schema));
            }
            let batch = fetch_bound(client, tid, &access, &sink, &reply_schema)?;
            (reply_schema, batch)
        }
        SinkTail::Fold { shape, .. } => {
            if limit == Some(0) {
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
    let (schema, batch) = read_spec_finish(out_schema, batch, &order, offset, limit);
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
/// express (a qualified-wildcard / other non-map projection, an ORDER BY
/// expression) surfaces the offending resolver's own feature-named `Unsupported`;
/// an unknown column is a `Bind` error.
fn build_rows_shape(select: &Select, query: &Query, schema: &Schema) -> Result<RowsShape, GnitzSqlError> {
    // Projection items — the source PK hidden-prepended to slots `0..k`, then
    // every SELECT item as a payload slot in SELECT order.
    let (mut items, mut out_cols) = build_read_projection(&select.projection, schema)?;

    // ORDER BY keys over the reply columns; a non-projected source column is
    // appended as a hidden payload column (so it can still order the result).
    let order = resolve_read_spec_order(&mut items, &mut out_cols, schema, query.order_by.as_ref())?;

    // Reply schema + projection blob.
    let (reply_schema, projection) = read_reply_shape(&items, out_cols, schema)?;
    Ok(RowsShape {
        reply_schema,
        projection,
        order,
    })
}

/// The physical layout and reply schemas a GROUP BY / global aggregate / HAVING /
/// DISTINCT read folds under — a pure function of the AST and the source schema. A
/// shape the fold cannot express (a partial reply wider than the column limit, a
/// HAVING the shared expression compiler rejects) is a feature-named
/// `Unsupported`; a resolver's own `Unsupported`/`Bind` propagates.
fn build_fold_shape(select: &Select, schema: &Schema) -> Result<FoldShape, GnitzSqlError> {
    // Resolve the physical layout (shared with the view path). DISTINCT is the
    // degenerate grouped fold — zero aggregates over the set-op projection
    // resolver (bare columns only, float keys rejected); GROUP BY / global
    // aggregates use the shared `analyze_group_by`.
    let layout = if select.distinct.is_some() {
        let (indices, out_cols) = resolve_set_projection(&select.projection, schema, "SELECT DISTINCT")?;
        GroupByLayout::distinct(indices, &out_cols)
    } else {
        analyze_group_by(select, schema)?
    };
    let n_group = layout.group_col_indices.len();

    // The one width invariant bounding a fold plan: the partial reply layout
    // `[_group_pk | group cols | agg partials]` must be a legal schema. Wider
    // (e.g. many repeated aggregates) than the column limit has no fold reply
    // layout — a feature limit of the direct path.
    if 1 + n_group + layout.agg_specs.len() > MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "aggregate SELECT with {} group + aggregate columns exceeds the {MAX_COLUMNS}-column fold reply limit",
            n_group + layout.agg_specs.len()
        )));
    }

    // The final output schema — built at plan time so a duplicate output name
    // rejects before any dispatch, exactly as every view compile does.
    let out_schema = build_agg_out_schema(&layout, schema)?;

    // The partial reply schema — the shared SyntheticFold reduce-output layout the
    // worker emits (parity with the view path's reduce schema by construction).
    // It ships with the request and decodes every reply frame. Partial agg columns
    // are nullable: an all-NULL SUM/MIN/MAX group emits a NULL partial the client
    // must carry.
    let partial_schema = Schema::from_parts(
        // Blanket-nullable: this schema decodes the worker partials and is also
        // what the HAVING predicate resolves against, so over-declaring only
        // forces the evaluator's null-carrying arm.
        synthetic_fold_cols(schema, &layout.group_col_indices, &layout.agg_specs, None),
        vec![0],
    )
    .map_err(|e| GnitzSqlError::Unsupported(format!("ad-hoc aggregate reply schema is invalid: {e}")))?;

    // HAVING: bind against the SyntheticFold reduce layout, then compile it with
    // the same `BoundExpr → Evaluator` pipeline a grouped view's post-reduce
    // FILTER uses, resolved against that layout. Compiling here (rather than in
    // the client finish) keeps every rejection pre-dispatch, and it is the same
    // rejection either path gives: a HAVING that fails to compile here fails as a
    // view too — including a wide literal, which `OpcodeBackend::lower` rejects with
    // the message that names it.
    let having = match &select.having {
        Some(having_expr) => {
            let ctx = HavingCtx {
                source_schema: schema,
                group_col_indices: &layout.group_col_indices,
                out_key: ReduceOutKey::SyntheticFold,
                agg_mappings: &layout.agg_mappings,
                agg_col_offset: layout.synthetic_agg_col_offset(),
            };
            compile_conjuncts_evaluator(&[&bind_having_expr(having_expr, &ctx)?], &partial_schema)?
        }
        None => None,
    };

    Ok(FoldShape {
        layout,
        partial_schema,
        out_schema,
        having,
    })
}
