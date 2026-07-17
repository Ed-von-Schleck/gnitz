//! Direct `SELECT`: route between the thin keyseek path (single table,
//! bare-column projection, index-served WHERE whose residual the client-side
//! interpreter can evaluate) and the transient circuit executor (every other
//! shape — set-ops, JOINs, DISTINCT / GROUP BY / HAVING, computed projections,
//! CTEs, and typed / non-indexed WHERE). Routing is decided at the exact points
//! the thin ladder would otherwise hard-error, from the SAME classification the
//! thin execution then consumes — so a thin verdict can never die later at seek
//! or row eval, and no classification or index-metadata fetch runs twice.

use crate::ast_util::{extract_table_factor_name, group_by_is_present, projection_item_expr, single_relation_col_name};
use crate::bind::{bind_single_table, Binder};
use crate::dml::plan::{
    classify_access, collect_index_range_candidates, collect_index_seek_candidates, extract_limit, extract_offset,
    first_index_hit, seek_pk_multi, AccessPath, IndexListMemo,
};
use crate::error::GnitzSqlError;
use crate::exec::eval::expr_is_thin;
use crate::exec::order::{order_limit_passthrough, order_limit_project};
use crate::exec::residual::residual_filtered;
use crate::ir::BoundExpr;
use crate::plan::validate::{
    reject_unhonored_query_clauses, reject_unhonored_select_clauses, HonoredClauses, HonoredQueryClauses,
};
use crate::SqlResult;
use gnitz_core::{GnitzClient, PlannedView, Schema};
use sqlparser::ast::{Expr, LimitClause, Query, SelectItem, SetExpr};

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    _schema_name: &str,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Envelope guard for BOTH paths: ORDER BY / LIMIT / OFFSET are applied by the
    // client-side ordering sink over either path's fetched batch, and WITH is
    // honored by the executor (CTE inlining — a WITH always routes there below,
    // so the thin path can never resolve a FROM name a CTE shadows). Everything
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

    // §3 shape routing, from the AST alone: a WITH, a set-op body, a JOIN,
    // DISTINCT / GROUP BY / HAVING, or a computed projection has no thin
    // operator and compiles through the executor instead.
    let select = match query.body.as_ref() {
        SetExpr::Select(s) if query.with.is_none() => s,
        _ => return execute_select_via_executor(client, query, binder),
    };
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || select.distinct.is_some()
        || group_by_is_present(&select.group_by)
        || select.having.is_some()
        || !projection_is_thin(&select.projection)
    {
        return execute_select_via_executor(client, query, binder);
    }

    // Thin body-clause guard: WHERE (the ladder below) and the projection are
    // honored; DISTINCT / grouping routed above; the exotic tail (PREWHERE, TOP,
    // QUALIFY, …) has no operator on either path and rejects here.
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
    let has_order_by = query.order_by.is_some();

    let table_name = extract_table_factor_name(&select.from[0].relation, "FROM")?;
    let (tid, schema) = binder.resolve(client, &table_name)?;

    // The WHERE access path, classified ONCE by the shared UPDATE/DELETE
    // recognizer. Each arm either serves thin or routes to the executor when its
    // residual is not client-evaluable — decided by binding the residual and
    // checking the bound IR against exactly what the interpreter runs
    // (`bind_thin_residuals`), never by a parallel AST walk that could drift.
    let (schema_out, batch_opt, _) = match classify_access(select.selection.as_ref(), &schema) {
        AccessPath::ScanAll => client.scan(tid)?,
        AccessPath::PkMultiSeek { pks } => {
            // `pk IN (…)` multi-seek: the IN list is the whole top-level WHERE
            // (`try_extract_pk_in` matches only a bare InList), so no residual
            // filtering applies and every fetched row is an output row. `row_cap`
            // early-stops the seek in IN-list order, so it is a pure no-ORDER-BY
            // fetch optimization: disabled under ORDER BY (which must sort the
            // full result), else it fetches `offset + limit` rows — enough logical
            // rows for the window, since every fetched entry has weight 1 here.
            let row_cap = if has_order_by {
                None
            } else {
                limit.map(|l| l.saturating_add(offset))
            };
            let (schema_opt, batch_opt) = seek_pk_multi(client, tid, &schema, &pks, row_cap)?;
            (schema_opt, batch_opt, 0)
        }
        AccessPath::PkSeek { pk, residual } => {
            let Some(preds) = bind_thin_residuals(&residual, &schema)? else {
                return execute_select_via_executor(client, query, binder);
            };
            let res = client.seek(tid, &pk)?;
            residual_filtered(&schema, res, &preds)?
        }
        AccessPath::Filtered { where_expr } => {
            // Order: PK equality (above) → range index → equality index →
            // executor. Try RANGE candidates first so a composite `a = 5 AND
            // b > 10` uses the (a,b) range scan instead of a bare `a = 5` prefix
            // seek + in-memory `b > 10` residual. A range candidate is never less
            // selective than the equality prefix it extends (a full
            // all-columns-equality match yields no range column, hence no range
            // candidate), so range-first is strict.
            //
            // Each candidate list is pre-filtered to the candidates whose
            // residual is thin-evaluable — a candidate whose residual the
            // interpreter cannot run can never be served thin, so attempting its
            // seek would be wasted I/O ending in a hard error.

            let mut idx_memo = IndexListMemo::default();
            let range_cands =
                collect_index_range_candidates(where_expr, &schema, || idx_memo.get(|| client.table_indexes(tid)))
                    .map_err(GnitzSqlError::Exec)?;
            let mut thin_range = Vec::new();
            for c in range_cands {
                if let Some(preds) = bind_thin_residuals(&c.residual, &schema)? {
                    thin_range.push((c, preds));
                }
            }
            let mut hit = match first_index_hit(thin_range, |(c, _)| {
                client.seek_by_index_range(tid, c.idx_cols.as_slice(), &c.desc)
            })? {
                Some(((_, preds), res)) => Some(residual_filtered(&schema, res, &preds)?),
                None => None,
            };

            if hit.is_none() {
                let candidates =
                    collect_index_seek_candidates(where_expr, &schema, || idx_memo.get(|| client.table_indexes(tid)))
                        .map_err(GnitzSqlError::Exec)?;
                let mut thin_seek = Vec::new();
                for (cols, vals, residual) in candidates {
                    if let Some(preds) = bind_thin_residuals(&residual, &schema)? {
                        thin_seek.push((cols, vals, preds));
                    }
                }
                if let Some(((_, _, preds), res)) = first_index_hit(thin_seek, |(cols, vals, _)| {
                    client.seek_by_index(tid, cols.as_slice(), vals)
                })? {
                    hit = Some(residual_filtered(&schema, res, &preds)?);
                }
            }

            match hit {
                Some(res) => res,
                // No index (or no thin-evaluable candidate) serves this WHERE:
                // the executor runs it as a server-side typed scan + filter.
                None => return execute_select_via_executor(client, query, binder),
            }
        }
    };

    // Use the resolved schema for column metadata
    let actual_schema = schema_out.as_deref().unwrap_or(&*schema);

    // ORDER BY / OFFSET / LIMIT sink: sort the full fetched batch, apply the
    // multiplicity-aware window, then project. It sorts BEFORE projection so an
    // ORDER BY key absent from the projected columns still resolves.
    let (proj_schema, final_batch) = order_limit_project(
        &select.projection,
        actual_schema,
        batch_opt,
        query.order_by.as_ref(),
        offset,
        limit,
    )?;

    Ok(SqlResult::Rows {
        schema: proj_schema,
        batch: final_batch,
    })
}

// ---------------------------------------------------------------------------
// §3 routing helpers
// ---------------------------------------------------------------------------

/// True iff every projection item is a shape the thin projection resolver
/// (`resolve_projection`) accepts, derived from the SAME helpers it consumes so
/// the verdict cannot drift from the execution: a plain `*` (`tbl.*` is not
/// resolvable thin), or an expression `single_relation_col_name` recognizes — a
/// bare `Identifier` or two-part `CompoundIdentifier`, optionally aliased. A
/// computed expression, function call, aggregate, multi-alias item, or
/// deeper-qualified reference routes to the executor.
fn projection_is_thin(items: &[SelectItem]) -> bool {
    items.iter().all(|item| match projection_item_expr(item) {
        None => matches!(item, SelectItem::Wildcard(_)),
        Some(e) => single_relation_col_name(e).is_some(),
    })
}

/// Bind residual conjuncts for the thin path, accepting them only when the
/// interpreter (`exec::eval`) can run every node — probed via `expr_is_thin`,
/// which shares the interpreter's own `BoundExprBackend` walk. `Ok(Some(preds))`
/// is both the thin verdict AND the execution plan — the same bound objects
/// filter the fetched rows, so approve-then-fail drift is impossible.
/// `Ok(None)` routes to the typed executor: a float / string / U128 / UUID
/// column or literal, an aggregate, or an expression shape the thin grammar
/// lacks. A `Bind` error (unknown / ambiguous column) stays a hard error — no
/// path could resolve it.
fn bind_thin_residuals(residual: &[&Expr], schema: &Schema) -> Result<Option<Vec<BoundExpr>>, GnitzSqlError> {
    let mut preds = Vec::with_capacity(residual.len());
    for &e in residual {
        match bind_single_table(e, schema) {
            Ok(b) if expr_is_thin(&b, schema) => preds.push(b),
            Ok(_) => return Ok(None),
            Err(e @ GnitzSqlError::Bind(_)) => return Err(e),
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(preds))
}

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
