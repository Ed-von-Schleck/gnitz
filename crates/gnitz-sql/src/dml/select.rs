//! Direct `SELECT ... FROM <one table>`: drive the WHERE access-path ladder
//! (PK seek → ordered range index → equality index → "non-indexed" error),
//! then project and limit the seek/scan reply client-side. Unlike UPDATE/DELETE
//! there is no predicate full-scan fallback — a WHERE no index can serve is a
//! clean error, not a table scan.

use crate::ast_util::extract_table_factor_name;
use crate::bind::{bind_single_table, Binder};
use crate::dml::plan::{
    classify_access, collect_index_range_candidates, collect_index_seek_candidates, extract_limit, extract_offset,
    first_index_hit, seek_pk_multi, AccessPath,
};
use crate::error::GnitzSqlError;
use crate::exec::order::order_limit_project;
use crate::exec::residual::residual_filtered;
use crate::plan::validate::{
    reject_unhonored_query_clauses, reject_unhonored_select_clauses, HonoredClauses, HonoredQueryClauses,
};
use crate::SqlResult;
use gnitz_core::GnitzClient;
use sqlparser::ast::{LimitClause, Query, SetExpr};
use std::sync::Arc;

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    _schema_name: &str,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Direct SELECT honors ORDER BY and LIMIT/OFFSET — all applied client-side by the ordering sink
    // over the fetched batch — and nothing else on the `Query` envelope. `with: false` closes the
    // silent `WITH` drop (a shadowing CTE resolved the wrong table); `limit: true` / `order_by: true`
    // leave `limit_clause` / `order_by` to the sink; the `LIMIT … BY` sub-form is rejected below.
    // FETCH, FOR UPDATE/SHARE, SETTINGS, FORMAT are all rejected here.
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            ordering_sink: true,
            ..HonoredQueryClauses::NONE
        },
        "direct SELECT",
    )?;
    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;
    let has_order_by = query.order_by.is_some();

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "only SELECT ... FROM is supported".to_string(),
            ))
        }
    };

    // Direct SELECT is a thin seek/scan plus stateless client-side project/limit. It
    // honors WHERE (the access-path ladder below) and the projection; every other
    // Select-body clause has no operator here and would be silently dropped — a wrong
    // result. Route the whole tail through the shared guard, as ORDER BY and a
    // non-indexed WHERE already error. DISTINCT, GROUP BY, and HAVING belong in a
    // CREATE VIEW (real incremental distinct / reduce).
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "direct SELECT",
    )?;

    // The envelope guard honors `limit_clause` (LIMIT + OFFSET, applied by the sink); its
    // `LIMIT … BY` (ClickHouse per-group) sub-form has no operator here, so reject it rather than
    // silently accept-and-ignore it.
    if let Some(LimitClause::LimitOffset { limit_by, .. }) = &query.limit_clause {
        if !limit_by.is_empty() {
            return Err(GnitzSqlError::Unsupported(
                "LIMIT ... BY is not supported in direct SELECT".to_string(),
            ));
        }
    }

    // Exactly one FROM table, no joins
    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "SELECT must have exactly one FROM table".to_string(),
        ));
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "JOINs not supported in direct SELECT".to_string(),
        ));
    }
    let table_name = extract_table_factor_name(&from.relation, "FROM")?;

    let (tid, schema) = binder.resolve(client, &table_name)?;

    // The WHERE access path, classified by the shared UPDATE/DELETE recognizer;
    // only the fetch policy per path is SELECT's own.
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
            let res = client.seek(tid, &pk)?;
            residual_filtered(&schema, res, &residual)?
        }
        AccessPath::Filtered { where_expr } => {
            // Order: PK equality (above) → range index → equality index → error.
            // Try RANGE candidates first so a composite `a = 5 AND b > 10` uses the
            // (a,b) range scan instead of a bare `a = 5` prefix seek + in-memory
            // `b > 10` residual. A range candidate is never less selective than the
            // equality prefix it extends (a full all-columns-equality match yields
            // no range column, hence no range candidate), so range-first is strict.

            // One epoch-validated GET_INDICES round-trip serves both collectors:
            // `table_indexes` always hits the wire (its epoch cache only skips
            // re-decoding), so the range→equality fall-through must not fetch
            // the same list twice within one statement.
            let mut idx_memo: Option<Arc<Vec<gnitz_core::IndexMeta>>> = None;
            let range_cands = collect_index_range_candidates(where_expr, &schema, || {
                let list = client.table_indexes(tid)?;
                idx_memo = Some(Arc::clone(&list));
                Ok(list)
            })
            .map_err(GnitzSqlError::Exec)?;
            let mut hit = match first_index_hit(range_cands, |c| {
                client.seek_by_index_range(tid, c.idx_cols.as_slice(), &c.desc)
            })? {
                Some((cand, res)) => Some(residual_filtered(&schema, res, &cand.residual)?),
                None => None,
            };

            if hit.is_none() {
                let candidates = collect_index_seek_candidates(where_expr, &schema, || match idx_memo {
                    Some(list) => Ok(list),
                    None => client.table_indexes(tid),
                })
                .map_err(GnitzSqlError::Exec)?;
                if let Some(((_, _, residual), res)) = first_index_hit(candidates, |(cols, vals, _)| {
                    client.seek_by_index(tid, cols.as_slice(), vals)
                })? {
                    hit = Some(residual_filtered(&schema, res, &residual)?);
                }
            }

            match hit {
                Some(res) => res,
                None => {
                    // No index served this WHERE. Bind it first so an ambiguous or
                    // unknown column raises its exact Bind error (a SELECT * join
                    // view can carry two same-named columns) instead of the generic
                    // "non-indexed" message. Direct SELECT — unlike UPDATE/DELETE —
                    // has no predicate full-scan fallback that would bind it.
                    bind_single_table(where_expr, &schema)?;
                    return Err(GnitzSqlError::Unsupported(
                        "WHERE on non-indexed column not supported in direct SELECT; \
                         use CREATE INDEX first, or CREATE VIEW for server-side filtering"
                            .to_string(),
                    ));
                }
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
