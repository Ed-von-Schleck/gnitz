//! UPDATE and DELETE: resolve the matching rows once via `resolve_where_rows`
//! (the shared PK-seek → secondary-index → predicate-scan ladder), then write
//! the SET batch (UPDATE) or collect the PKs to retract (DELETE). The SET-list
//! helpers (`eval_set_expr`, `resolve_set_target`) are also reused by INSERT's
//! `ON CONFLICT DO UPDATE`.

use crate::ast_util::{extract_name, extract_table_factor_name};
use crate::bind::{bind_single_table, find_unique_column, Binder};
use crate::codec::colwrite::{append_column_value, ColumnValue};
use crate::codec::nullmap::null_word_set;
use crate::dml::overlay::{buffered_all, buffered_keys, overlay_batch, Net};
use crate::dml::plan::{classify_access, collect_index_seek_candidates, seek_pk_multi, AccessPath};
use crate::error::GnitzSqlError;
use crate::exec::eval::eval_expr;
use crate::exec::residual::{bind_residuals, matching_indices};
use crate::ir::BoundExpr;
use crate::SqlResult;
use gnitz_core::{ClientError, ColData, GnitzClient, PkColumn, PkTuple, Schema, WireConflictMode, ZSetBatch};
use sqlparser::ast::{Assignment, AssignmentTarget, Expr, FromTable, Statement};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SET-list helpers (shared with INSERT's ON CONFLICT DO UPDATE)
// ---------------------------------------------------------------------------

pub(crate) fn eval_set_expr(
    expr: &BoundExpr,
    batch: &ZSetBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<ColumnValue, GnitzSqlError> {
    match expr {
        BoundExpr::LitStr(s) => return Ok(ColumnValue::Str(s.clone())),
        BoundExpr::ColRef(c) => {
            if let ColData::Strings(v) = &batch.columns[*c] {
                return Ok(match &v[row_idx] {
                    Some(s) => ColumnValue::Str(s.clone()),
                    None => ColumnValue::Null,
                });
            }
            // fall through: numeric column handled by eval_expr below
        }
        _ => {}
    }
    match eval_expr(expr, batch, row_idx, schema)? {
        None => Ok(ColumnValue::Null),
        Some(v) => Ok(ColumnValue::Int(v)),
    }
}

fn extract_assignment_col_name(assignment: &Assignment, clause: &str) -> Result<String, GnitzSqlError> {
    match &assignment.target {
        AssignmentTarget::ColumnName(obj_name) => extract_name(obj_name, clause),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "only simple column assignments supported in {clause}"
        ))),
    }
}

/// Resolve and validate one `col = expr` SET-list target — shared by UPDATE and
/// ON CONFLICT DO UPDATE, which differ only in the RHS binding and the `clause`
/// label used in messages. Extracts the column name, resolves it to a column
/// index, rejects a PK target, and rejects a column already present in `seen`
/// (recording it there on success so the next duplicate is caught). Both clauses
/// enforce the same SQL rules: no PK writes, no duplicate columns.
pub(crate) fn resolve_set_target(
    assignment: &Assignment,
    schema: &Schema,
    seen: &mut Vec<usize>,
    clause: &str,
) -> Result<usize, GnitzSqlError> {
    let col_name = extract_assignment_col_name(assignment, clause)?;
    let col_idx = find_unique_column(&schema.columns, &col_name)?
        .ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found in {clause}")))?;
    if schema.is_pk_col(col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot assign to primary key column in {clause}"
        )));
    }
    if seen.contains(&col_idx) {
        return Err(GnitzSqlError::Bind(format!(
            "multiple assignments to column '{col_name}' in {clause}"
        )));
    }
    seen.push(col_idx);
    Ok(col_idx)
}

/// Build one merged Z-set row into `dst`: PK from `(pk_src, pk_idx)`; null-bitmap
/// seed and carried (unassigned) columns from `(carry_src, carry_idx)`. For each
/// payload column, `resolve(ci)` returns `Some(value)` to write that value (and
/// set/clear its null bit) or `None` to carry the column through unchanged from
/// `carry_src`. Shared by UPDATE SET (pk_src == carry_src) and ON CONFLICT DO
/// UPDATE (PK from the incoming row, carry/null-seed from the existing row).
pub(crate) fn build_merged_row<F>(
    pk_src: &ZSetBatch,
    pk_idx: usize,
    carry_src: &ZSetBatch,
    carry_idx: usize,
    schema: &Schema,
    dst: &mut ZSetBatch,
    mut resolve: F,
) -> Result<(), GnitzSqlError>
where
    F: FnMut(usize) -> Result<Option<ColumnValue>, GnitzSqlError>,
{
    dst.pks.push_from(&pk_src.pks, pk_idx);
    dst.weights.push(1);
    // Seed from the carry source's null word; each assignment flips only its own
    // payload bit (set on a NULL result, clear on non-NULL), unassigned bits ride.
    let mut null_bits = carry_src.nulls[carry_idx];
    for (payload_idx, ci, col_def) in schema.payload_columns() {
        match resolve(ci)? {
            Some(cv) => {
                null_word_set(&mut null_bits, payload_idx, matches!(cv, ColumnValue::Null));
                append_column_value(&mut dst.columns[ci], cv, col_def.type_code)?;
            }
            None => {
                let stride = col_def.type_code.wire_stride();
                carry_src.columns[ci].push_row_from(carry_idx, stride, &mut dst.columns[ci]);
            }
        }
    }
    dst.nulls.push(null_bits);
    Ok(())
}

/// Write the SET-merged update row for every `matched` index of `current` into
/// `dst` (each at weight +1). The assignment index is built once and reused
/// across rows — it depends only on `assignments` and `schema`, not the row —
/// mirroring INSERT's `client_side_merge_do_update` loop.
fn write_set_rows(
    current: &ZSetBatch,
    matched: &[usize],
    assignments: &[(usize, BoundExpr)],
    schema: &Schema,
    dst: &mut ZSetBatch,
) -> Result<(), GnitzSqlError> {
    // Pre-index assignments by column for O(1) lookup per payload column
    // (closes the prior O(cols²) per-row `assignments.iter().find`).
    let mut asn_by_col: Vec<Option<&BoundExpr>> = vec![None; schema.columns.len()];
    for (ci, expr) in assignments {
        asn_by_col[*ci] = Some(expr);
    }
    for &row_idx in matched {
        build_merged_row(current, row_idx, current, row_idx, schema, dst, |ci| {
            asn_by_col[ci]
                .map(|expr| eval_set_expr(expr, current, row_idx, schema))
                .transpose()
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared WHERE-row resolution (the UPDATE/DELETE access-path ladder)
// ---------------------------------------------------------------------------

/// The rows a single-table UPDATE/DELETE `WHERE` (or its absence) resolves to.
/// `batch` is the effective state (the fetched seek/scan reply, overlaid with the
/// open transaction's buffered writes — see `overlay`); `matched` indexes the
/// rows that passed the residual/predicate. UPDATE writes a SET batch from
/// `matched`; DELETE collects their PKs.
struct ResolvedRows {
    /// Schema returned with the fetched batch (the wire reply overrides the
    /// catalog schema for column metadata), if any.
    schema: Option<Arc<Schema>>,
    /// The effective rows.
    batch: ZSetBatch,
    /// Indices into `batch` that passed the residual/predicate.
    matched: Vec<usize>,
}

/// Resolve `selection` to the matching rows of `tid`, walking the shared
/// UPDATE/DELETE access-path ladder: PK point-seek → secondary-index equality
/// seek (first existing index wins) → predicate full scan; or a full scan of
/// every row when there is no `WHERE`.
///
/// Each arm fetches the committed rows, then overlays the open transaction's own
/// buffered writes ([`overlay_batch`]) before the predicate derives `matched` —
/// so a buffered payload that fails a WHERE the committed payload passed matches
/// nothing, and a transaction-born row is discovered. **In autocommit every net
/// map is empty, the overlay is the identity, and this is exactly the plain
/// committed ladder.** The seek arms overlay only the keys they seeked: their
/// residual does not constrain the PK, so an unrelated buffered row must not
/// enter their effective batch.
fn resolve_where_rows(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    selection: Option<&Expr>,
) -> Result<ResolvedRows, GnitzSqlError> {
    match classify_access(selection, schema) {
        AccessPath::ScanAll => {
            let (schema_opt, committed, _) = client.scan(tid)?;
            let net = buffered_all(client, tid);
            resolve(schema, schema_opt, committed, &net, &[])
        }
        AccessPath::PkSeek { pk, residual } => {
            let (schema_opt, committed, _) = client.seek(tid, &pk)?;
            let net = buffered_keys(client, tid, std::slice::from_ref(&pk));
            resolve(schema, schema_opt, committed, &net, &residual)
        }
        AccessPath::PkMultiSeek { pks } => {
            // `pk IN (…)`: the concatenated seek replies ARE the matching rows —
            // no residual; an absent key contributes none, so the count reports
            // rows actually touched.
            let (schema_opt, committed) = seek_pk_multi(client, tid, schema, &pks, None)?;
            let stride = schema.pk_stride() as u8;
            let keys: Vec<PkTuple> = pks.iter().map(|&k| PkTuple::from_u128(stride, k)).collect();
            let net = buffered_keys(client, tid, &keys);
            resolve(schema, schema_opt, committed, &net, &[])
        }
        AccessPath::Filtered { where_expr } => {
            let (schema_opt, committed, residual) = fetch_filtered(client, tid, schema, where_expr)?;
            let net = buffered_all(client, tid);
            // Committed rows already satisfy the winning index's equality prefix,
            // so its reduced `residual` decides them — and it is the only form the
            // row evaluator can always handle (an indexed STRING/U128 equality is
            // seekable but not evaluable). Buffered rows are unindexed, so once the
            // transaction has touched this table every row must face the FULL
            // predicate instead.
            let preds: Vec<&Expr> = if net.is_empty() { residual } else { vec![where_expr] };
            resolve(schema, schema_opt, committed, &net, &preds)
        }
    }
}

/// The ladder's shared tail: overlay the buffered writes onto the committed
/// batch, bind `residual` against the catalog schema, and match it per effective
/// row. An empty residual passes every row.
fn resolve(
    schema: &Schema,
    schema_opt: Option<Arc<Schema>>,
    committed: Option<ZSetBatch>,
    net: &Net,
    residual: &[&Expr],
) -> Result<ResolvedRows, GnitzSqlError> {
    let actual = schema_opt.as_deref().unwrap_or(schema);
    let batch = overlay_batch(committed, net, actual);
    let preds = bind_residuals(residual, schema)?;
    let matched = matching_indices(&preds, &batch, actual)?;
    Ok(ResolvedRows {
        schema: schema_opt,
        batch,
        matched,
    })
}

/// Fetch the committed candidates for a `Filtered` WHERE: the first existing
/// index's equality seek (with the reduced residual left over from consuming its
/// key columns), else the full scan (residual = the whole predicate). The first
/// index that exists serves the query — a hit with no matching rows is terminal,
/// it does NOT fall through to the scan.
type FilteredFetch<'a> = (Option<Arc<Schema>>, Option<ZSetBatch>, Vec<&'a Expr>);

fn fetch_filtered<'a>(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    where_expr: &'a Expr,
) -> Result<FilteredFetch<'a>, GnitzSqlError> {
    let candidates =
        collect_index_seek_candidates(where_expr, schema, || client.table_indexes(tid)).map_err(GnitzSqlError::Exec)?;
    for (col_indices, key_vals, residual) in candidates {
        match client.seek_by_index(tid, col_indices.as_slice(), &key_vals) {
            Ok((schema_opt, batch_opt, _)) => return Ok((schema_opt, batch_opt, residual)),
            Err(ClientError::NoIndex) => continue,
            Err(e) => return Err(GnitzSqlError::Exec(e)),
        }
    }
    let (schema_opt, batch_opt, _) = client.scan(tid)?;
    Ok((schema_opt, batch_opt, vec![where_expr]))
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

pub(crate) fn execute_update(
    client: &mut GnitzClient,
    _schema_name: &str,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let (table, assignments_raw, selection) = match stmt {
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => (table, assignments, selection),
        _ => return Err(GnitzSqlError::Bind("not an UPDATE statement".to_string())),
    };

    let table_name = extract_table_factor_name(&table.relation, "UPDATE")?;

    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    // Bind SET assignments; reject PK writes and duplicate columns.
    let mut assignments: Vec<(usize, BoundExpr)> = Vec::new();
    let mut seen: Vec<usize> = Vec::with_capacity(assignments_raw.len());
    for assignment in assignments_raw {
        let col_idx = resolve_set_target(assignment, &schema, &mut seen, "UPDATE SET")?;
        let bound_val = bind_single_table(&assignment.value, &schema)?;
        assignments.push((col_idx, bound_val));
    }

    let resolved = resolve_where_rows(client, table_id, &schema, selection.as_ref())?;
    let actual_schema = resolved.schema.as_deref().unwrap_or(&*schema);
    let count = resolved.matched.len();
    if count > 0 {
        let mut updates = ZSetBatch::new(actual_schema);
        write_set_rows(
            &resolved.batch,
            &resolved.matched,
            &assignments,
            actual_schema,
            &mut updates,
        )?;
        client.push_with_mode(table_id, actual_schema, &updates, WireConflictMode::Update)?;
    }
    Ok(SqlResult::RowsAffected { count })
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

pub(crate) fn execute_delete(
    client: &mut GnitzClient,
    _schema_name: &str,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let del = match stmt {
        Statement::Delete(d) => d,
        _ => return Err(GnitzSqlError::Bind("not a DELETE statement".to_string())),
    };

    let tables = match &del.from {
        FromTable::WithFromKeyword(ts) | FromTable::WithoutKeyword(ts) => ts,
    };
    if tables.len() != 1 || !tables[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "DELETE: exactly one simple FROM table required".to_string(),
        ));
    }
    let table_name = extract_table_factor_name(&tables[0].relation, "DELETE")?;

    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    let resolved = resolve_where_rows(client, table_id, &schema, del.selection.as_ref())?;
    let actual_schema = resolved.schema.as_deref().unwrap_or(&*schema);
    let count = resolved.matched.len();
    if count > 0 {
        let batch = resolved.batch;
        // Whole batch matched (the common no-WHERE `DELETE FROM t`): move the
        // PK region wholesale rather than copying it row by row.
        let pks = if count == batch.pks.len() {
            batch.pks
        } else {
            let mut out_pks = PkColumn::empty_for_schema(actual_schema);
            for &i in &resolved.matched {
                out_pks.push_from(&batch.pks, i);
            }
            out_pks
        };
        client.delete(table_id, actual_schema, pks)?;
    }
    Ok(SqlResult::RowsAffected { count })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{batch_2col, col_def, two_col};
    use gnitz_core::TypeCode;

    // ------------------------------------------------------------------
    // write_set_rows must update the null bitmap for assignments
    // ------------------------------------------------------------------

    #[test]
    fn test_write_set_clears_null_bit_on_non_null_assignment() {
        // Existing row: val = NULL (null bit set). Assignment: SET val = 99.
        // Expected: val = 99, null bit cleared.
        let schema = two_col(TypeCode::I64);
        let current = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // val is NULL

        let assignments = vec![(1usize, BoundExpr::LitInt(99))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(&current, &[0], &assignments, &schema, &mut dst).unwrap();

        assert_eq!(
            dst.nulls[0] & 0b1,
            0,
            "null bit must be cleared after non-null assignment"
        );
        if let ColData::Fixed(ref buf) = dst.columns[1] {
            assert_eq!(i64::from_le_bytes(buf[..8].try_into().unwrap()), 99);
        }
    }

    #[test]
    fn test_write_set_sets_null_bit_when_source_col_is_null() {
        // Three-column schema: pk, a (non-null), b (null).
        // Assignment: SET a = b (b is NULL → a should become NULL).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::I64, true),
                col_def("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0b10); // payload bit 1 (b) is NULL; bit 0 (a) is non-null
        if let ColData::Fixed(ref mut buf) = current.columns[1] {
            buf.extend_from_slice(&5i64.to_le_bytes());
        }
        if let ColData::Fixed(ref mut buf) = current.columns[2] {
            buf.extend_from_slice(&[0u8; 8]);
        }

        // SET a = b  (ColRef(2) = b, which is NULL in current)
        let assignments = vec![(1usize, BoundExpr::ColRef(2))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(&current, &[0], &assignments, &schema, &mut dst).unwrap();

        // a's null bit (payload_idx 0 → bit 0) must now be set
        assert_ne!(dst.nulls[0] & 0b01, 0, "a must be null after SET a = NULL_col");
        // b's null bit (payload_idx 1 → bit 1) must remain set (not touched)
        assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null");
    }

    #[test]
    fn test_write_set_preserves_null_bits_for_unassigned_cols() {
        // Unassigned columns must carry their original null status unchanged.
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::I64, true),
                col_def("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0b10); // b is NULL, a is not
        if let ColData::Fixed(ref mut buf) = current.columns[1] {
            buf.extend_from_slice(&5i64.to_le_bytes());
        }
        if let ColData::Fixed(ref mut buf) = current.columns[2] {
            buf.extend_from_slice(&[0u8; 8]);
        }

        // Only assign to a; b is untouched
        let assignments = vec![(1usize, BoundExpr::LitInt(10))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(&current, &[0], &assignments, &schema, &mut dst).unwrap();

        assert_eq!(dst.nulls[0] & 0b01, 0, "a must not be null (assigned non-null)");
        assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null (unassigned)");
    }

    #[test]
    fn write_set_rows_carries_blob_column_through() {
        // UPDATE assigns `v` only; the unmodified BLOB column must carry
        // through. Red against the missing `ColData::Bytes` arm (unreachable!).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("b", TypeCode::Blob, true),
                col_def("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0);
        if let ColData::Bytes(v) = &mut current.columns[1] {
            v.push(Some(vec![1, 2, 3]));
        }
        if let ColData::Fixed(buf) = &mut current.columns[2] {
            buf.extend_from_slice(&7i64.to_le_bytes());
        }

        let assignments = vec![(2usize, BoundExpr::LitInt(99))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(&current, &[0], &assignments, &schema, &mut dst).unwrap();

        if let ColData::Bytes(v) = &dst.columns[1] {
            assert_eq!(v[0].as_deref(), Some(&[1u8, 2, 3][..]));
        } else {
            panic!("expected Bytes column carried through");
        }
        if let ColData::Fixed(buf) = &dst.columns[2] {
            assert_eq!(i64::from_le_bytes(buf[..8].try_into().unwrap()), 99);
        }
    }

    #[test]
    fn build_merged_row_takes_pk_from_pk_src_and_carries_from_carry_src() {
        // DO UPDATE shape: PK from the incoming (excluded) row; null-seed and
        // carried columns from the existing stored row. Schema: pk, v (I64), b (Blob).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("v", TypeCode::I64, true),
                col_def("b", TypeCode::Blob, true),
            ],
            pk_cols: vec![0],
        };

        // pk_src (incoming): PK = 100; payload irrelevant (only the PK is read).
        let mut pk_src = ZSetBatch::new(&schema);
        pk_src.pks.push_u128(100u128);
        pk_src.weights.push(1);
        pk_src.nulls.push(0);
        if let ColData::Fixed(buf) = &mut pk_src.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        if let ColData::Bytes(v) = &mut pk_src.columns[2] {
            v.push(Some(vec![9, 9, 9]));
        }

        // carry_src (existing): PK = 200; v = 7 (non-null); b = [1,2,3] to carry.
        let mut carry_src = ZSetBatch::new(&schema);
        carry_src.pks.push_u128(200u128);
        carry_src.weights.push(1);
        carry_src.nulls.push(0);
        if let ColData::Fixed(buf) = &mut carry_src.columns[1] {
            buf.extend_from_slice(&7i64.to_le_bytes());
        }
        if let ColData::Bytes(v) = &mut carry_src.columns[2] {
            v.push(Some(vec![1, 2, 3]));
        }

        // Resolver: assign v = NULL (must SET its null bit); leave b unassigned (carry).
        let mut dst = ZSetBatch::new(&schema);
        build_merged_row(&pk_src, 0, &carry_src, 0, &schema, &mut dst, |ci| {
            Ok(if ci == 1 { Some(ColumnValue::Null) } else { None })
        })
        .unwrap();

        let stride = schema.pk_stride() as u8;
        // PK comes from pk_src (100), NOT carry_src (200).
        assert_eq!(dst.pks.get_tuple(0, stride), pk_src.pks.get_tuple(0, stride));
        assert_ne!(dst.pks.get_tuple(0, stride), carry_src.pks.get_tuple(0, stride));
        // v's null bit (payload_idx 0) must be SET after the NULL assignment.
        assert_ne!(dst.nulls[0] & 0b01, 0, "v must be null after SET v = NULL");
        // b carried from carry_src ([1,2,3]), not pk_src ([9,9,9]).
        if let ColData::Bytes(v) = &dst.columns[2] {
            assert_eq!(v[0].as_deref(), Some(&[1u8, 2, 3][..]));
        } else {
            panic!("expected carried Bytes column");
        }
    }
}
