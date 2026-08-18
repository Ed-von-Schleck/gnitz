//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (`plan_read_spec` → a `ReadSpec` rows sink
//! executed server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT
//! top-k) or, for aggregate / DISTINCT shapes, through the **fold sink**
//! (`execute_aggregate_select` → a per-worker hash-fold + client finishing).
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
//! Planning is separable from dispatch: [`route_select`] validates and resolves,
//! [`build_rows_shape`] / [`build_fold_shape`] decide each sink's reply shape, and
//! only the tails below reach the wire. `dml::explain` runs the same builders in
//! the same order and formats their decisions instead of dispatching them.

use crate::agg::{synthetic_fold_cols, GroupByLayout};
use crate::ast_util::{
    body_is_grouped, classify_from, extract_table_factor_name, has_exists_in_subquery, has_scalar_subquery,
    is_bare_wildcard_projection, FromShape,
};
use crate::bind::cte_passthrough;
use crate::bind::Binder;
use crate::codec::project_schema::{build_read_projection, read_reply_shape};
use crate::dml::group_by::{analyze_group_by, bind_having_expr, resolve_set_projection, HavingCtx};
use crate::dml::plan::{bind_where, extract_limit, extract_offset, fetch_bound, plan_where, ReadBudget};
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::{agg_finish, build_agg_out_schema, FoldShape};
use crate::exec::order::{order_limit_passthrough, read_spec_finish, resolve_read_spec_order};
use crate::expr_lower::compile_filter_evaluator;
use crate::validate::{
    cte_select_body, non_recursive_ctes, reject_unhonored_query_clauses, reject_unhonored_select_clauses,
    HonoredClauses, HonoredQueryClauses,
};
use crate::SqlResult;
use gnitz_core::{GnitzClient, ReduceOutKey, RelClass, Schema, ZSetBatch, MAX_COLUMNS};
use gnitz_wire::{AggReadItem, AggReadSpec, ReadSink};
use sqlparser::ast::{LimitClause, Query, Select, SetExpr};
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
    pub(super) kind: Option<RelClass>,
}

/// Which sink a validated ad-hoc SELECT lands on.
pub(super) enum Sink {
    /// Bare `SELECT *` with no WHERE / ORDER BY / LIMIT / OFFSET: the
    /// unprojected full scan (`client.scan`), which builds no `ReadSpec`.
    PlainScan,
    Rows,
    Fold,
}

/// A validated ad-hoc SELECT: its sink and everything that sink needs. The one
/// seam between planning and dispatch — `execute_select` runs the tail,
/// `dml::explain` formats it.
pub(super) struct Route<'q> {
    pub(super) target: Target,
    pub(super) select: &'q Select,
    pub(super) limit: Option<usize>,
    pub(super) offset: usize,
    pub(super) sink: Sink,
}

/// Resolve `name` to the read's target; the relation kind rides the resolution.
fn resolve_target(client: &mut GnitzClient, binder: &mut Binder<'_>, name: String) -> Result<Target, GnitzSqlError> {
    let (tid, schema, kind) = binder.resolve(client, &name)?;
    Ok(Target {
        name,
        tid,
        schema,
        kind,
    })
}

/// Validate an ad-hoc SELECT's shape, route it to its sink, and resolve the one
/// relation it reads. Both tails then run the same builders in the same order, so
/// describing a query that has no plan yields the identical rejection.
pub(super) fn route_select<'q>(
    client: &mut GnitzClient,
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
        match cte_passthrough(client, cte_select, &cte.alias.columns, binder)? {
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
        let target = resolve_target(client, binder, table_name)?;
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

    let target = resolve_target(client, binder, table_name)?;

    // A plain `SELECT *` (no EXCEPT/RENAME/… modifiers) with no WHERE / ORDER BY /
    // LIMIT / OFFSET reads through `client.scan` rather than a `ReadSpec`: the rows
    // come back verbatim with no per-row projection program, and the reply carries
    // the server's own schema block, which warms the client schema cache (a
    // `ReadSpec` ships a per-query projected schema and never touches that cache).
    // A modifier-bearing wildcard falls through to `plan_read_spec`, which expands
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

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let route = route_select(client, query, binder)?;
    match route.sink {
        Sink::PlainScan => {
            let (schema_out, batch_opt, _) = client.scan(route.target.tid)?;
            let out_schema = schema_out
                .map(|s| (*s).clone())
                .unwrap_or_else(|| (*route.target.schema).clone());
            let batch = batch_opt.unwrap_or_else(|| ZSetBatch::new(&out_schema));
            Ok(SqlResult::Rows {
                schema: out_schema,
                batch,
            })
        }
        Sink::Rows => plan_read_spec(client, query, &route),
        Sink::Fold => execute_aggregate_select(client, query, &route),
    }
}

// ---------------------------------------------------------------------------
// plan_read_spec — the parameterized bounded read
// ---------------------------------------------------------------------------

/// The rows sink's reply shape: what the worker projects and how it orders.
pub(super) struct RowsShape {
    pub(super) reply_schema: Schema,
    /// The compiled projection-map program.
    pub(super) projection: Vec<u8>,
    pub(super) order: Vec<gnitz_wire::OrderKey>,
}

/// The `(reply schema, projection, ORDER BY keys)` a rows read replies under — a
/// pure function of the AST and the source schema. A shape the read spec cannot
/// express (a qualified-wildcard / other non-map projection, an ORDER BY
/// expression) surfaces the offending resolver's own feature-named `Unsupported`;
/// an unknown column is a `Bind` error.
pub(super) fn build_rows_shape(select: &Select, query: &Query, schema: &Schema) -> Result<RowsShape, GnitzSqlError> {
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

/// Build and run a `ReadSpec` for a single-relation, non-aggregate SELECT.
fn plan_read_spec(client: &mut GnitzClient, query: &Query, route: &Route<'_>) -> Result<SqlResult, GnitzSqlError> {
    let (target, select, limit, offset) = (&route.target, route.select, route.limit, route.offset);
    let schema = &*target.schema;
    // WHERE → bound + compiled server-side predicate, then the reply shape.
    let bound_where = bind_where(schema, select.selection.as_ref())?;
    let plan = plan_where(client, target.tid, schema, bound_where.as_ref(), ReadBudget::OneRequest)?;
    let shape = build_rows_shape(select, query, schema)?;

    // `LIMIT 0` short-circuits to an empty result — no request dispatched.
    if limit == Some(0) {
        return Ok(empty_rows(shape.reply_schema));
    }
    // OFFSET+LIMIT logical rows; `0` = unbounded (an OFFSET with no LIMIT too).
    let limit_k = limit.map(|l| l.saturating_add(offset) as u64).unwrap_or(0);

    // Ship the spec; the reply schema is the one we built.
    let sink = ReadSink::Rows {
        projection: shape.projection,
        order: shape.order.clone(),
        limit_k,
    };
    let batch = fetch_bound(client, target.tid, &plan, &sink, &shape.reply_schema)?;

    // Client finish: sort the concatenation by the wire keys, window, present.
    let (schema, batch) = read_spec_finish(shape.reply_schema, batch, &shape.order, offset, limit);
    Ok(SqlResult::Rows { schema, batch })
}

// ---------------------------------------------------------------------------
// execute_aggregate_select — the ad-hoc aggregate / DISTINCT fold
// ---------------------------------------------------------------------------

/// The physical layout and reply schemas a GROUP BY / global aggregate / HAVING /
/// DISTINCT read folds under — a pure function of the AST and the source schema. A
/// shape the fold cannot express (a partial reply wider than the column limit, a
/// HAVING the shared expression compiler rejects) is a feature-named
/// `Unsupported`; a resolver's own `Unsupported`/`Bind` propagates.
pub(super) fn build_fold_shape(select: &Select, schema: &Schema) -> Result<FoldShape, GnitzSqlError> {
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
            compile_filter_evaluator(&bind_having_expr(having_expr, &ctx)?, &partial_schema)?
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

/// Serve a single-relation GROUP BY / global aggregate / HAVING / DISTINCT query
/// via the ReadSpec fold sink (a per-worker hash-fold) + client finishing. The
/// only runtime error is the per-worker group cap, surfaced from the wire call.
fn execute_aggregate_select(
    client: &mut GnitzClient,
    query: &Query,
    route: &Route<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let (target, select, limit, offset) = (&route.target, route.select, route.limit, route.offset);
    let schema = &*target.schema;

    // WHERE → bound + residual predicate, exactly as the plain read path (the
    // grouped view applies WHERE via the identical compiler, so a WHERE the direct
    // path cannot compile also fails the view). It precedes the sink shape as it
    // does in `plan_read_spec`: a query unsupported on both axes must name the
    // same one whichever sink it routes to.
    let bound_where = bind_where(schema, select.selection.as_ref())?;
    let plan = plan_where(client, target.tid, schema, bound_where.as_ref(), ReadBudget::OneRequest)?;
    let shape = build_fold_shape(select, schema)?;

    // `LIMIT 0` short-circuits to an empty result — no request dispatched
    // (parity with the rows path).
    if limit == Some(0) {
        return Ok(empty_rows(shape.out_schema));
    }

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
    // Dispatch. A wire error (including the runtime per-worker group cap) is HARD —
    // by now the fold is mid-flight on the workers and cannot fall back.
    let partial = fetch_bound(client, target.tid, &plan, &sink, &shape.partial_schema)?;

    // Client finishing (combine by group value, ground row, AVG/NullfillSum,
    // HAVING, projection), then the shared ORDER BY / OFFSET / LIMIT sink.
    let out_batch = agg_finish(&shape, &partial);

    let (schema_out, batch_out) =
        order_limit_passthrough(shape.out_schema, out_batch, query.order_by.as_ref(), offset, limit)?;
    Ok(SqlResult::Rows {
        schema: schema_out,
        batch: batch_out,
    })
}
