//! Direct `SELECT`: route a single-relation, non-aggregate SELECT through the
//! **parameterized bounded read** (`plan_read_spec` → a `ReadSpec` executed
//! server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT top-k)
//! and everything else — set-ops, JOINs, DISTINCT / GROUP BY / HAVING,
//! aggregates, CTEs — through the transient circuit executor. A shape the read
//! spec cannot serve (a non-projected ORDER BY key, an aggregate, a subquery in
//! the projection, a WHERE the expression VM cannot compile) falls back to the
//! executor, which either handles it or produces the same rejection.

use crate::ast_util::{
    extract_table_factor_name, flatten_conjuncts, group_by_is_present, is_bare_wildcard_projection,
    projection_has_aggregate,
};
use crate::bind::{bind_single_table, Binder};
use crate::codec::project_schema::{build_read_projection, compile_projection_map};
use crate::dml::plan::{extract_limit, extract_offset, try_extract_pk_in, try_extract_pk_range};
use crate::error::GnitzSqlError;
use crate::exec::order::{order_limit_passthrough, read_spec_finish, resolve_read_spec_order};
use crate::ir::{BinOp, BoundExpr};
use crate::lower::compile_filter_program;
use crate::plan::index_bound::best_index_bound;
use crate::plan::validate::{
    reject_unhonored_query_clauses, reject_unhonored_select_clauses, HonoredClauses, HonoredQueryClauses,
};
use crate::SqlResult;
use gnitz_core::protocol::encode_schema_block;
use gnitz_core::{GnitzClient, PlannedView, Schema, ZSetBatch};
use gnitz_wire::{ReadBound, ReadSpec};
use sqlparser::ast::{Expr, LimitClause, Query, Select, SetExpr};

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    _schema_name: &str,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Envelope guard for BOTH paths: ORDER BY / LIMIT / OFFSET are applied by the
    // client-side ordering sink over either path's fetched batch, and WITH is
    // honored by the executor (CTE inlining — a WITH always routes there below,
    // so the read-spec path can never resolve a FROM name a CTE shadows). Everything
    // else (FETCH, FOR UPDATE/SHARE, SETTINGS, FORMAT, pipe operators) is
    // rejected up front so neither path silently drops it.
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            with: true,
            ordering_sink: true,
        },
        "direct SELECT",
    )?;
    // The `LIMIT … BY` (ClickHouse per-group) sub-form has no operator on either
    // path, so reject it rather than silently accept-and-ignore it.
    if let Some(LimitClause::LimitOffset { limit_by, .. }) = &query.limit_clause {
        if !limit_by.is_empty() {
            return Err(GnitzSqlError::Unsupported(
                "LIMIT ... BY is not supported in direct SELECT".to_string(),
            ));
        }
    }

    // Shape routing, from the AST alone: a WITH, a set-op body, a JOIN, DISTINCT
    // / GROUP BY / HAVING, or an aggregate projection has no read-spec operator
    // and compiles through the executor instead.
    let select = match query.body.as_ref() {
        SetExpr::Select(s) if query.with.is_none() => s,
        _ => return execute_select_via_executor(client, query, binder),
    };
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || select.distinct.is_some()
        || group_by_is_present(&select.group_by)
        || select.having.is_some()
        || projection_has_aggregate(select)
    {
        return execute_select_via_executor(client, query, binder);
    }

    // Body-clause guard: WHERE and the projection are honored; DISTINCT / grouping
    // routed above; the exotic tail (PREWHERE, TOP, QUALIFY, …) rejects here.
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "direct SELECT",
    )?;

    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;

    let table_name = extract_table_factor_name(&select.from[0].relation, "FROM")?;
    let (tid, schema) = binder.resolve(client, &table_name)?;

    // A plain `SELECT *` (no EXCEPT/RENAME/… modifiers) with no WHERE / ORDER BY
    // / LIMIT / OFFSET stays a plain scan (the server's full-scan snapshot cache);
    // the read spec never emits an identity descriptor. A modifier-bearing
    // wildcard falls through to `plan_read_spec`, which expands it.
    let bare_star = is_bare_wildcard_projection(&select.projection);
    if bare_star && select.selection.is_none() && query.order_by.is_none() && limit.is_none() && offset == 0 {
        let (schema_out, batch_opt, _) = client.scan(tid)?;
        let out_schema = schema_out.map(|s| (*s).clone()).unwrap_or_else(|| (*schema).clone());
        let batch = batch_opt.unwrap_or_else(|| ZSetBatch::new(&out_schema));
        return Ok(SqlResult::Rows {
            schema: out_schema,
            batch,
        });
    }

    match plan_read_spec(client, select, query, &schema, tid, limit, offset)? {
        Some(result) => Ok(result),
        // A shape the read spec cannot serve (non-projected ORDER BY, subquery /
        // qualified-wildcard projection, a WHERE the expression VM cannot compile)
        // routes to the executor, which handles it or re-raises the same error.
        None => execute_select_via_executor(client, query, binder),
    }
}

// ---------------------------------------------------------------------------
// plan_read_spec — the parameterized bounded read
// ---------------------------------------------------------------------------

/// Collapse the routing verdict on a fallible planning step: a `Bind` error is
/// hard (no path could resolve it); any other error routes to the executor,
/// which serves the shape or re-raises the identical rejection.
fn hard_or_route<T>(r: Result<T, GnitzSqlError>) -> Result<Option<T>, GnitzSqlError> {
    match r {
        Ok(x) => Ok(Some(x)),
        Err(e @ GnitzSqlError::Bind(_)) => Err(e),
        Err(_) => Ok(None),
    }
}

/// Build and run a `ReadSpec` for a single-relation, non-aggregate SELECT.
/// `Ok(Some(result))` = served; `Ok(None)` = route to the executor (a shape the
/// read spec cannot express); `Err(Bind)` = a hard bind error (unknown column).
fn plan_read_spec(
    client: &mut GnitzClient,
    select: &Select,
    query: &Query,
    schema: &Schema,
    tid: u64,
    limit: Option<usize>,
    offset: usize,
) -> Result<Option<SqlResult>, GnitzSqlError> {
    // 1. Bound (an access superset) + the conjuncts to re-impose as the predicate.
    let (bound, pred_exprs) = extract_bound(client, tid, schema, select.selection.as_ref())?;

    // 2. Compile the predicate over the SOURCE schema. `Unsupported` (a wide-int /
    //    LIKE / string-arithmetic WHERE) routes to the executor, which re-raises
    //    the identical rejection.
    let Some(predicate) = hard_or_route(compile_read_spec_predicate(&pred_exprs, schema))? else {
        return Ok(None);
    };

    // 3. Projection items — the source PK hidden-prepended to slots `0..k`, then
    //    every SELECT item as a payload slot in SELECT order. A qualified wildcard
    //    / subquery / other non-map projection → executor.
    let Some((mut items, mut out_cols)) = hard_or_route(build_read_projection(&select.projection, schema))? else {
        return Ok(None);
    };
    let k = schema.pk_indices().len();

    // 4. ORDER BY keys over the reply columns; a non-projected source column is
    //    appended as a hidden payload column (so it can still order the result).
    //    An ORDER BY expression → `Unsupported` (routes; the executor rejects it
    //    identically).
    let Some(order) = hard_or_route(resolve_read_spec_order(
        &mut items,
        &mut out_cols,
        schema,
        query.order_by.as_ref(),
    ))?
    else {
        return Ok(None);
    };

    // 5. Reply schema + projection blob (the payload slice `items[k..]`).
    let reply_schema = Schema::from_parts(out_cols, (0..k).collect())
        .map_err(|e| GnitzSqlError::Unsupported(format!("read-spec reply schema is invalid: {e}")))?;
    let projection = compile_projection_map(&items[k..], schema)?.encode();

    // `LIMIT 0` short-circuits to an empty result — no request dispatched.
    if limit == Some(0) {
        let batch = ZSetBatch::new(&reply_schema);
        return Ok(Some(SqlResult::Rows {
            schema: reply_schema,
            batch,
        }));
    }
    // OFFSET+LIMIT logical rows; `0` = unbounded (an OFFSET with no LIMIT too).
    let limit_k = limit.map(|l| l.saturating_add(offset) as u64).unwrap_or(0);

    // 6. Ship the spec + the reply-schema wire block (with hidden flags).
    let reply_block = encode_schema_block(&reply_schema, tid as u32);
    let spec = ReadSpec {
        bound,
        predicate,
        projection,
        order,
        limit_k,
    };
    let (recv_schema, batch) = client
        .scan_spec(tid, &spec.encode(), &reply_block)
        .map_err(GnitzSqlError::Exec)?;

    // 7. Client finish: sort the concatenation by the wire keys, window, present.
    //    Decode against the echoed schema (the batch's own layout), falling back
    //    to the block we built.
    let out_schema = recv_schema.map(|s| (*s).clone()).unwrap_or(reply_schema);
    let batch = batch.unwrap_or_else(|| ZSetBatch::new(&out_schema));
    let (schema, batch) = read_spec_finish(out_schema, batch, &spec.order, offset, limit);
    Ok(Some(SqlResult::Rows { schema, batch }))
}

/// Extract the `ReadBound` and the WHERE conjuncts to re-impose as the
/// server-side predicate. The predicate is: empty for `PkSet` (the gather is
/// exact); the extractor's residual for `PkRange` and a wide-int `IndexRange`
/// (byte-exact walks — consumed conjuncts are applied exactly and stripped);
/// and the whole WHERE for `None` and a ≤8-byte-int `IndexRange` (whose
/// selectivity gate may degrade the bound to a full cursor).
fn extract_bound<'e>(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    where_expr: Option<&'e Expr>,
) -> Result<(ReadBound, Vec<&'e Expr>), GnitzSqlError> {
    let Some(we) = where_expr else {
        return Ok((ReadBound::None, Vec::new()));
    };
    let mut all_conjuncts = Vec::new();
    flatten_conjuncts(we, &mut all_conjuncts);

    // `pk IN (…)` → an exact gather (empty predicate). Keys ship deduplicated
    // (`try_extract_pk_in`) in whatever order the list gave them — the worker
    // OPK-sorts before its forward sweep, so wire order is irrelevant.
    if let Some(keys) = try_extract_pk_in(we, schema) {
        if keys.len() > gnitz_wire::MAX_PK_SET_KEYS {
            return Err(GnitzSqlError::Unsupported(format!(
                "pk IN (…) with {} keys exceeds the {}-key limit",
                keys.len(),
                gnitz_wire::MAX_PK_SET_KEYS
            )));
        }
        return Ok((ReadBound::PkSet(keys), Vec::new()));
    }

    // A PK equality / range → a byte-exact bounded PK walk; the residual (the
    // WHERE minus every conjunct the walk applies exactly — equalities and
    // consumed range cuts alike) is the predicate. Exactness at any PK width
    // is what serves a wide (U128) PK range without the predicate VM.
    if let Some((desc, residual)) = try_extract_pk_range(we, schema) {
        return Ok((ReadBound::PkRange(desc), residual));
    }

    // The best secondary-index bound, keeping its residual. The walk gating is
    // the worker's own decision, derived from the range column's type
    // (`TypeCode::is_wide_int` — the shared authority): a wide-int bound runs
    // the byte-exact walk, so the candidate's residual (the WHERE minus the
    // consumed conjuncts, which the VM could not compile anyway) is the
    // predicate; a narrow bound may be gate-degraded to a full cursor, so the
    // whole WHERE stays the predicate.
    if let Some(c) = best_index_bound(we, schema, || client.table_indexes(tid))? {
        let range_col = c.idx_cols.as_slice()[c.desc.eq_vals().len()] as usize;
        let wide = schema.columns[range_col].type_code.is_wide_int();
        let bound = ReadBound::IndexRange {
            idx_cols: gnitz_wire::pack_pk_cols(c.idx_cols.as_slice()),
            desc: c.desc,
        };
        let pred = if wide { c.residual } else { all_conjuncts };
        return Ok((bound, pred));
    }

    Ok((ReadBound::None, all_conjuncts))
}

/// Bind + AND-combine `exprs`, compile to the wire predicate blob. Empty input or
/// a statically-true predicate → an empty blob (the bound is exact).
fn compile_read_spec_predicate(exprs: &[&Expr], schema: &Schema) -> Result<Vec<u8>, GnitzSqlError> {
    let Some((first, rest)) = exprs.split_first() else {
        return Ok(Vec::new());
    };
    let mut bound = bind_single_table(first, schema)?;
    for &e in rest {
        let next = bind_single_table(e, schema)?;
        bound = BoundExpr::BinOp(Box::new(bound), BinOp::And, Box::new(next));
    }
    match compile_filter_program(&bound, schema)? {
        Some(prog) => Ok(prog.encode()),
        None => Ok(Vec::new()),
    }
}

/// Resolve the ORDER BY clause to wire `OrderKey`s over the (server-projected)
/// reply columns, **appending any non-projected source column as a hidden payload
/// column** so it can order the result (§ the read path sorts server-side over a
/// The executor branch: compile the SELECT into the same circuit a CREATE VIEW
/// would build, run it once as a transient, and apply the shared client-side
/// ordering sink (ORDER BY / OFFSET / LIMIT) over the streamed result. The
/// transient's output is already projected server-side, so the sink runs with
/// the identity projection (hidden synthetic keys stay physical and are
/// stripped at presentation, exactly like a view scan). A multi-segment compile
/// is out of scope (a separate plan).
fn execute_select_via_executor(
    client: &mut GnitzClient,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;
    let mut segments = crate::plan::compile_query_to_circuit(client, query, binder)?;
    if segments.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "this ad-hoc query compiles to a multi-segment chain (3+-way join, self-join, DISTINCT / GROUP BY over a \
             join, correlated subquery, non-pass-through CTE, or a derived table in FROM); use CREATE VIEW"
                .to_string(),
        ));
    }
    let PlannedView {
        circuit,
        output_columns,
        pk_cols,
        ..
    } = segments.pop().unwrap();
    // The result schema is the transient's own output schema, validated once and
    // shared by the wire call, the sink, and the returned rows.
    let schema = Schema::from_parts(output_columns, pk_cols.iter().map(|&c| c as usize).collect())
        .map_err(|e| GnitzSqlError::Unsupported(format!("transient output schema is invalid: {e}")))?;
    let batch = client.run_query(circuit, &schema).map_err(GnitzSqlError::Exec)?;
    let (schema, batch) = order_limit_passthrough(schema, batch, query.order_by.as_ref(), offset, limit)?;
    Ok(SqlResult::Rows { schema, batch })
}
