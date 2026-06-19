//! Direct `SELECT ... FROM <one table>`: drive the WHERE access-path ladder
//! (PK seek → ordered range index → equality index → "non-indexed" error),
//! then project and limit the seek/scan reply client-side. Unlike UPDATE/DELETE
//! there is no predicate full-scan fallback — a WHERE no index can serve is a
//! clean error, not a table scan.

use crate::ast_util::extract_table_factor_name;
use crate::bind::Binder;
use crate::dml::plan::{
    collect_index_range_candidates, collect_index_seek_candidates, extract_limit, try_extract_pk_seek_residual,
};
use crate::error::GnitzSqlError;
use crate::exec::batch::{apply_limit, apply_projection};
use crate::exec::residual::residual_filtered;
use crate::SqlResult;
use gnitz_core::{ClientError, GnitzClient};
use sqlparser::ast::{Query, SetExpr};
use std::sync::Arc;

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    _schema_name: &str,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    if query.order_by.is_some() {
        return Err(GnitzSqlError::Unsupported("ORDER BY not supported".to_string()));
    }
    let limit = extract_limit(query);

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "only SELECT ... FROM is supported".to_string(),
            ))
        }
    };

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

    // Check WHERE clause
    let (schema_out, batch_opt, _) = if let Some(where_expr) = &select.selection {
        if let Some((pk, residual)) = try_extract_pk_seek_residual(where_expr, &schema) {
            let res = client.seek(tid, &pk)?;
            residual_filtered(binder, &schema, res, &residual)?
        } else {
            // Order: PK equality (above) → range index → equality index → error.
            // Try RANGE candidates first so a composite `a = 5 AND b > 10` uses the
            // (a,b) range scan instead of a bare `a = 5` prefix seek + in-memory
            // `b > 10` residual. A range candidate is never less selective than the
            // equality prefix it extends (a full all-columns-equality match yields
            // no range column, hence no range candidate), so range-first is strict.
            let mut hit = None;

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
            for cand in range_cands {
                match client.seek_by_index_range(tid, cand.idx_cols.as_slice(), &cand.desc) {
                    Ok(res) => {
                        hit = Some(residual_filtered(binder, &schema, res, &cand.residual)?);
                        break;
                    }
                    Err(ClientError::NoIndex) => continue,
                    Err(e) => return Err(GnitzSqlError::Exec(e)),
                }
            }

            if hit.is_none() {
                let candidates = collect_index_seek_candidates(where_expr, &schema, || match idx_memo {
                    Some(list) => Ok(list),
                    None => client.table_indexes(tid),
                })
                .map_err(GnitzSqlError::Exec)?;
                for (col_indices, key_vals, residual) in candidates {
                    match client.seek_by_index(tid, col_indices.as_slice(), &key_vals) {
                        Ok(res) => {
                            hit = Some(residual_filtered(binder, &schema, res, &residual)?);
                            break;
                        }
                        Err(ClientError::NoIndex) => continue,
                        Err(e) => return Err(GnitzSqlError::Exec(e)),
                    }
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
                    binder.bind_expr(where_expr, &schema)?;
                    return Err(GnitzSqlError::Unsupported(
                        "WHERE on non-indexed column not supported in direct SELECT; \
                         use CREATE INDEX first, or CREATE VIEW for server-side filtering"
                            .to_string(),
                    ));
                }
            }
        }
    } else {
        client.scan(tid)?
    };

    // Use the resolved schema for column metadata
    let actual_schema = schema_out.as_deref().unwrap_or(&*schema);

    // Apply projection
    let (proj_schema, proj_batch) = apply_projection(&select.projection, actual_schema, batch_opt)?;

    // Apply LIMIT
    let final_batch = if let Some(lim) = limit {
        apply_limit(proj_batch, &proj_schema, lim)
    } else {
        proj_batch
    };

    Ok(SqlResult::Rows {
        schema: proj_schema,
        batch: final_batch,
    })
}
