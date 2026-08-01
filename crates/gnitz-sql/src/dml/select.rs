//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (`plan_read_spec` → a `ReadSpec` rows sink
//! executed server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT
//! top-k) or, for aggregate / DISTINCT shapes, through the **fold sink**
//! (`execute_aggregate_select` → a per-worker hash-fold + client finishing).
//!
//! A query that *derives* a new relation — a JOIN, a set operation, an EXISTS/IN
//! or scalar subquery, a derived table, a non-pass-through CTE — has no
//! single-relation sink; `execute_select` rejects it from the AST alone
//! (`reject_derivation`) with one actionable message pointing at CREATE VIEW,
//! which maintains the derived relation incrementally. A single-relation read the
//! direct path cannot express (a LIKE / string-function WHERE, an ORDER BY
//! expression) is a feature-named `Unsupported`, never the derivation template. A
//! pass-through CTE over one relation is inlined (`cte_passthrough`) so trivial
//! `WITH` queries keep reading through the direct path.

use crate::access::{best_index_bound, try_extract_pk_in, try_extract_pk_range};
use crate::agg::{synthetic_fold_cols, GroupByLayout};
use crate::ast_util::{
    body_is_grouped, classify_from, extract_table_factor_name, has_exists_in_subquery, has_scalar_subquery,
    is_bare_wildcard_projection, FromShape,
};
use crate::bind::cte_passthrough;
use crate::bind::{bind_single_table, Binder};
use crate::codec::project_schema::{build_read_projection, compile_projection_map};
use crate::dml::group_by::{analyze_group_by, bind_having_expr, resolve_set_projection, HavingCtx};
use crate::dml::plan::{extract_limit, extract_offset};
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::{agg_finish, build_agg_out_schema, AggFinish};
use crate::exec::order::{order_limit_passthrough, read_spec_finish, resolve_read_spec_order};
use crate::ir::BoundExpr;
use crate::lower::{compile_filter_evaluator, compile_filter_program};
use crate::validate::{
    cte_select_body, non_recursive_ctes, reject_unhonored_query_clauses, reject_unhonored_select_clauses,
    HonoredClauses, HonoredQueryClauses,
};
use crate::SqlResult;
use gnitz_core::{GnitzClient, ReduceOutKey, Schema, ZSetBatch, MAX_COLUMNS};
use gnitz_wire::{AggReadItem, AggReadSpec, ReadBound, ReadSink, ReadSpec};
use sqlparser::ast::{Expr, LimitClause, Query, Select, SetExpr};

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

/// WHERE → the pushed-down `ReadBound` (an access superset) plus the compiled
/// server-side predicate re-imposing the residual conjuncts — the shared front
/// half of both sinks. Binds the WHERE once up front, then recognizes over the
/// bound conjuncts. The predicate is: empty for `PkSet` (the gather is exact);
/// the extractor's residual for `PkRange` and a wide-int `IndexRange` (byte-exact
/// walks — consumed conjuncts are applied exactly and stripped); and the whole
/// bound WHERE for `None` and a ≤8-byte-int `IndexRange` (whose selectivity gate
/// may degrade the bound to a full cursor). A wide-int / LIKE / string-arithmetic
/// WHERE the expression VM cannot compile is an `Unsupported`, propagated.
fn where_bound_and_predicate(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    where_expr: Option<&Expr>,
) -> Result<(ReadBound, Vec<u8>), GnitzSqlError> {
    let Some(we) = where_expr else {
        return Ok((ReadBound::None, Vec::new()));
    };
    let bound_where = bind_single_table(we, schema)?;

    // `pk IN (…)` → an exact gather of those keys, with the remaining conjuncts as
    // the predicate. Keys ship deduplicated (`try_extract_pk_in`); the worker
    // OPK-sorts before its forward sweep. A list past the wire's key cap declines to
    // the ladder below, which serves it as an ordinary predicate scan.
    match try_extract_pk_in(&bound_where, schema) {
        Some((keys, residual)) if keys.len() <= gnitz_wire::MAX_PK_SET_KEYS => {
            let predicate = compile_read_spec_predicate(&residual, schema)?;
            return Ok((ReadBound::PkSet(keys), predicate));
        }
        _ => {}
    }

    // A PK equality / range → a byte-exact bounded PK walk; the residual (the WHERE
    // minus every conjunct the walk applies exactly) is the predicate. Exactness at
    // any PK width is what serves a wide (U128) PK range without the predicate VM.
    if let Some((desc, residual)) = try_extract_pk_range(&bound_where, schema) {
        let predicate = compile_read_spec_predicate(&residual, schema)?;
        return Ok((ReadBound::PkRange(desc), predicate));
    }

    // The best secondary-index bound, keeping its residual. The walk gating is the
    // worker's own decision, derived from the range column's type
    // (`TypeCode::is_wide_int`): a wide-int bound runs the byte-exact walk, so the
    // candidate's residual is the predicate; a narrow bound may be gate-degraded to
    // a full cursor, so the whole WHERE stays the predicate.
    if let Some(c) = best_index_bound(&bound_where, schema, || client.table_indexes(tid))? {
        let wide = schema.columns[c.range_col()].type_code.is_wide_int();
        let pred_exprs: Vec<&BoundExpr> = if wide { c.residual } else { vec![&bound_where] };
        let predicate = compile_read_spec_predicate(&pred_exprs, schema)?;
        let bound = ReadBound::IndexRange {
            idx_cols: gnitz_wire::pack_pk_cols(c.idx_cols.as_slice()),
            desc: c.desc,
        };
        return Ok((bound, predicate));
    }

    let predicate = compile_read_spec_predicate(&[&bound_where], schema)?;
    Ok((ReadBound::None, predicate))
}

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    _schema_name: &str,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
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
            Some(resolved) => binder.cache_alias(&cte.alias.name.value, resolved, true)?,
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

    // Step 5 — aggregate / DISTINCT shapes fold via the fold sink. DISTINCT takes
    // precedence over GROUP BY (its arm rejects GROUP BY), matching the view path.
    if select.distinct.is_some() || select.having.is_some() || body_is_grouped(select) {
        return execute_aggregate_select(client, select, query, binder);
    }

    // Body-clause guard: WHERE and the projection are honored; DISTINCT / grouping
    // routed above; the exotic tail (PREWHERE, TOP, QUALIFY, …) rejects here.
    reject_unhonored_select_clauses(select, HonoredClauses::PLAIN, "direct SELECT")?;

    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;

    let table_name = extract_table_factor_name(&select.from[0].relation, "FROM")?;
    let (tid, schema) = binder.resolve(client, &table_name)?;

    // A plain `SELECT *` (no EXCEPT/RENAME/… modifiers) with no WHERE / ORDER BY
    // / LIMIT / OFFSET stays a plain scan (the server's full-scan snapshot cache);
    // the read spec never emits an identity descriptor. A modifier-bearing
    // wildcard falls through to `plan_read_spec`, which expands it. A DROP
    // COLUMN'd base table (hidden *payload* slot) also falls through, so
    // `build_read_projection` filters the dropped slot out of the raw scan
    // schema/batch (§6) instead of leaking it to the client. A view's hidden
    // synthetic *key* slots do NOT fall through — they are filtered at
    // presentation, and the scan path keeps the server's snapshot cache.
    let bare_star = is_bare_wildcard_projection(&select.projection);
    if bare_star
        && !schema.has_hidden_payload()
        && select.selection.is_none()
        && query.order_by.is_none()
        && limit.is_none()
        && offset == 0
    {
        let (schema_out, batch_opt, _) = client.scan(tid)?;
        let out_schema = schema_out.map(|s| (*s).clone()).unwrap_or_else(|| (*schema).clone());
        let batch = batch_opt.unwrap_or_else(|| ZSetBatch::new(&out_schema));
        return Ok(SqlResult::Rows {
            schema: out_schema,
            batch,
        });
    }

    // Step 6 — the read spec. Every planning outcome is terminal: a shape the
    // direct path cannot express is a feature-named `Unsupported`, propagated from
    // the offending resolver.
    plan_read_spec(client, select, query, &schema, tid, limit, offset)
}

// ---------------------------------------------------------------------------
// plan_read_spec — the parameterized bounded read
// ---------------------------------------------------------------------------

/// Build and run a `ReadSpec` for a single-relation, non-aggregate SELECT. Every
/// planning outcome is terminal: a shape the read spec cannot express (a wide-int
/// / LIKE / string-arithmetic WHERE, a qualified-wildcard / other non-map
/// projection, an ORDER BY expression) surfaces the offending resolver's own
/// feature-named `Unsupported`; an unknown column is a `Bind` error.
fn plan_read_spec(
    client: &mut GnitzClient,
    select: &Select,
    query: &Query,
    schema: &Schema,
    tid: u64,
    limit: Option<usize>,
    offset: usize,
) -> Result<SqlResult, GnitzSqlError> {
    // 1–2. WHERE → bound + compiled server-side predicate.
    let (bound, predicate) = where_bound_and_predicate(client, tid, schema, select.selection.as_ref())?;

    // 3. Projection items — the source PK hidden-prepended to slots `0..k`, then
    //    every SELECT item as a payload slot in SELECT order. A qualified wildcard
    //    / subquery / other non-map projection is an `Unsupported`, propagated.
    let (mut items, mut out_cols) = build_read_projection(&select.projection, schema)?;
    let k = schema.pk_indices().len();

    // 4. ORDER BY keys over the reply columns; a non-projected source column is
    //    appended as a hidden payload column (so it can still order the result).
    //    An ORDER BY expression is an `Unsupported`, propagated.
    let order = resolve_read_spec_order(&mut items, &mut out_cols, schema, query.order_by.as_ref())?;

    // 5. Reply schema + projection blob (the payload slice `items[k..]`).
    let reply_schema = Schema::from_parts(out_cols, (0..k).collect())
        .map_err(|e| GnitzSqlError::Unsupported(format!("read-spec reply schema is invalid: {e}")))?;
    let projection = compile_projection_map(&items[k..], schema)?.encode();

    // `LIMIT 0` short-circuits to an empty result — no request dispatched.
    if limit == Some(0) {
        return Ok(empty_rows(reply_schema));
    }
    // OFFSET+LIMIT logical rows; `0` = unbounded (an OFFSET with no LIMIT too).
    let limit_k = limit.map(|l| l.saturating_add(offset) as u64).unwrap_or(0);

    // 6. Ship the spec; the reply schema is the one we built.
    let spec = ReadSpec {
        bound,
        predicate,
        sink: ReadSink::Rows {
            projection,
            order,
            limit_k,
        },
    };
    let batch = client
        .scan_spec(tid, &spec.encode(), &reply_schema)
        .map_err(GnitzSqlError::Exec)?;

    // 7. Client finish: sort the concatenation by the wire keys, window, present.
    let batch = batch.unwrap_or_else(|| ZSetBatch::new(&reply_schema));
    let ReadSink::Rows { order, .. } = &spec.sink else {
        unreachable!()
    };
    let (schema, batch) = read_spec_finish(reply_schema, batch, order, offset, limit);
    Ok(SqlResult::Rows { schema, batch })
}

/// AND-combine the bound residual conjuncts and compile to the wire predicate
/// blob. Empty input or a statically-true predicate → an empty blob (the bound is
/// exact).
fn compile_read_spec_predicate(exprs: &[&BoundExpr], schema: &Schema) -> Result<Vec<u8>, GnitzSqlError> {
    let Some(pred) = crate::ir::and_fold(exprs.iter().map(|e| (*e).clone())) else {
        return Ok(Vec::new());
    };
    match compile_filter_program(&pred, &schema.columns)? {
        Some(prog) => Ok(prog.encode()),
        None => Ok(Vec::new()),
    }
}

// ---------------------------------------------------------------------------
// execute_aggregate_select — the ad-hoc aggregate / DISTINCT fold
// ---------------------------------------------------------------------------

/// Serve a single-relation GROUP BY / global aggregate / HAVING / DISTINCT query
/// via the ReadSpec fold sink (a per-worker hash-fold) + client finishing. Every
/// planning outcome is terminal: a shape the fold cannot express (a partial reply
/// wider than the column limit, a HAVING the shared expression compiler rejects)
/// is a feature-named `Unsupported`; a resolver's own `Unsupported`/`Bind`
/// propagates. The only runtime error is the per-worker group cap, surfaced from
/// the wire call.
fn execute_aggregate_select(
    client: &mut GnitzClient,
    select: &Select,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // The caller validated `from[0]` is a plain table/view (step 3), so this only
    // rejects an exotic table qualifier (a table function, AS OF, …).
    let table_name = extract_table_factor_name(&select.from[0].relation, "FROM")?;
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

    let (tid, schema) = binder.resolve(client, &table_name)?;

    // Resolve the physical layout (shared with the view path). DISTINCT is the
    // degenerate grouped fold — zero aggregates over the set-op projection
    // resolver (bare columns only, float keys rejected); GROUP BY / global
    // aggregates use the shared `analyze_group_by`.
    let layout = if is_distinct {
        let (indices, out_cols) = resolve_set_projection(&select.projection, &schema, "SELECT DISTINCT")?;
        GroupByLayout::distinct(indices, &out_cols)
    } else {
        analyze_group_by(select, &schema)?
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
    let out_schema = build_agg_out_schema(&layout, &schema)?;

    // The partial reply schema — the shared SyntheticFold reduce-output layout the
    // worker emits (parity with the view path's reduce schema by construction).
    // It ships with the request and decodes every reply frame. Partial agg columns
    // are nullable: an all-NULL SUM/MIN/MAX group emits a NULL partial the client
    // must carry.
    let partial_schema = Schema::from_parts(
        // Blanket-nullable: this schema decodes the worker partials and is also
        // what the HAVING predicate resolves against, so over-declaring only
        // forces the evaluator's null-carrying arm.
        synthetic_fold_cols(&schema, &layout.group_col_indices, &layout.agg_specs, &|_| true),
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
    let having_ev = match &select.having {
        Some(having_expr) => {
            let ctx = HavingCtx {
                source_schema: &schema,
                group_col_indices: &layout.group_col_indices,
                out_key: ReduceOutKey::SyntheticFold,
                agg_mappings: &layout.agg_mappings,
                agg_col_offset: layout.synthetic_agg_col_offset(),
            };
            compile_filter_evaluator(&bind_having_expr(having_expr, &ctx)?, &partial_schema)?
        }
        None => None,
    };

    // WHERE → bound + residual predicate, exactly as the plain read path (the
    // grouped view applies WHERE via the identical compiler, so a WHERE the direct
    // path cannot compile also fails the view — no works→error regression).
    let (bound, predicate) = where_bound_and_predicate(client, tid, &schema, select.selection.as_ref())?;

    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;
    // `LIMIT 0` short-circuits to an empty result — no request dispatched
    // (parity with the rows path).
    if limit == Some(0) {
        return Ok(empty_rows(out_schema));
    }

    let spec = ReadSpec {
        bound,
        predicate,
        sink: ReadSink::Fold(AggReadSpec {
            group_cols: layout.group_col_indices.iter().map(|&c| c as u16).collect(),
            aggs: layout
                .agg_specs
                .iter()
                .map(|s| AggReadItem {
                    op: s.op,
                    src_col: s.col as u16,
                })
                .collect(),
        }),
    };
    // Dispatch. A wire error (including the runtime per-worker group cap) is HARD —
    // by now the fold is mid-flight on the workers and cannot fall back.
    let batch = client
        .scan_spec(tid, &spec.encode(), &partial_schema)
        .map_err(GnitzSqlError::Exec)?;
    let partial = batch.unwrap_or_else(|| ZSetBatch::new(&partial_schema));

    // Client finishing (combine by group value, ground row, AVG/NullfillSum,
    // HAVING, projection), then the shared ORDER BY / OFFSET / LIMIT sink.
    let finish = AggFinish {
        source_schema: &schema,
        layout: &layout,
        partial_schema: &partial_schema,
        out_schema: &out_schema,
        having: having_ev.as_ref(),
    };
    let out_batch = agg_finish(&finish, &partial);

    let (schema_out, batch_out) =
        order_limit_passthrough(out_schema, out_batch, query.order_by.as_ref(), offset, limit)?;
    Ok(SqlResult::Rows {
        schema: schema_out,
        batch: batch_out,
    })
}
