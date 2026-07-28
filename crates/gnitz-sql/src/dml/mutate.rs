//! UPDATE and DELETE: resolve the matching rows once via `resolve_where_rows`
//! (the shared PK-seek → secondary-index → predicate-scan ladder), then write
//! the SET batch (UPDATE) or collect the PKs to retract (DELETE). The SET-list
//! helpers (`classify_set_rhs`, `eval_set_program`, `resolve_set_target`) are
//! also reused by INSERT's `ON CONFLICT DO UPDATE`.

use crate::access::collect_index_seek_candidates;
use crate::ast_util::{extract_name, extract_table_factor_name};
use crate::bind::{bind_single_table, find_unique_column, Binder};
use crate::codec::colwrite::{append_column_value, check_not_null, set_target_admits, ColumnValue};
use crate::dml::overlay::{buffered_all, buffered_keys, overlay_batch, Net};
use crate::dml::plan::{classify_access, first_index_hit, seek_pk_multi, AccessPath};
use crate::dml::rmw::{commit_rmw_or_buffer, RmwBuild, RmwWrite};
use crate::error::GnitzSqlError;
use crate::exec::residual::matching_indices;
use crate::ir::BoundExpr;
use crate::lower::compile_scalar_evaluator;
use crate::SqlResult;
use gnitz_core::null_word_set;
use gnitz_core::{
    retraction_batch, ColData, GnitzClient, PkColumn, PkTuple, Schema, TypeCode, ViewBuffers, WireConflictMode,
    ZSetBatch, ZSetBatchView,
};
use gnitz_expr::Evaluator;
use sqlparser::ast::{Assignment, AssignmentTarget, Expr, FromTable};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// SET-list helpers (shared with INSERT's ON CONFLICT DO UPDATE)
// ---------------------------------------------------------------------------

/// A compiled SET / `DO UPDATE SET` right-hand side.
///
/// `Const` and `StrCol` are **structural, not an optimisation**: `ColumnValue`
/// carries an owned `String` and the expression VM has no string result
/// register, so `SET s = 'lit'` and `SET s = other_str` have no compiled form at
/// all. A literal integer or NULL joins `Const` because it is the same thing —
/// a row-independent value — and reaching it through the VM would run a whole
/// single-row `eval_batch` per matched row to re-derive a constant.
///
/// `StrCol` is gated on `TypeCode::String` rather than `is_german_string()` on
/// purpose: a BLOB column must fall into `Num`, where `OpcodeBackend`'s column
/// load rejects it. That gate also keeps the arm structurally safe — a PK column
/// can never be STRING (§1), so `StrCol` can never name one, whose slot in
/// `ZSetBatch.columns` is an empty placeholder.
pub(crate) enum SetProgram {
    /// A row-independent value: a literal, or `NULL`.
    Const(ColumnValue),
    /// A bare reference to a `TypeCode::String` column, read verbatim.
    StrCol(usize),
    /// Everything else — integer-valued. Boxed so a `SetProgram` stays small
    /// enough to sit in a `Vec` beside the other two arms (`Evaluator` owns a
    /// resolved program plus its register file).
    Num(Box<Evaluator>),
}

/// Classify one SET RHS for target column `target` against the schema the rows
/// it will run over carry. Shared by `UPDATE SET` and `ON CONFLICT DO UPDATE
/// SET`, including the latter's `EXCLUDED.<col>` short-circuit, so a string
/// EXCLUDED assignment classifies the same way a bare one does.
///
/// The target check is what makes [`eval_set_program`] infallible. Without it a
/// kind mismatch (`SET int_col = 'abc'`) would surface only from
/// `append_column_value`, i.e. only once a row matched — so a zero-match WHERE
/// would report 0 rows updated instead of rejecting.
pub(crate) fn classify_set_rhs(expr: &BoundExpr, target: usize, schema: &Schema) -> Result<SetProgram, GnitzSqlError> {
    let p = match expr {
        BoundExpr::LitStr(s) => SetProgram::Const(ColumnValue::Str(s.clone())),
        BoundExpr::LitInt(v) => SetProgram::Const(ColumnValue::Int(*v)),
        BoundExpr::LitNull => SetProgram::Const(ColumnValue::Null),
        // `ColData::empty_for(TypeCode::String)` is the `Strings` variant and
        // every batch reaching SET is built from a `Schema`, so the declared type
        // code decides the representation.
        BoundExpr::ColRef(c) if schema.columns[*c].type_code == TypeCode::String => SetProgram::StrCol(*c),
        _ => SetProgram::Num(Box::new(compile_scalar_evaluator(expr, schema)?)),
    };
    let tc = schema.columns[target].type_code;
    let str_valued = match &p {
        SetProgram::Const(ColumnValue::Null) => return Ok(p), // NULL suits every column
        SetProgram::Const(cv) => matches!(cv, ColumnValue::Str(_)),
        SetProgram::StrCol(_) => true,
        SetProgram::Num(_) => false,
    };
    if !set_target_admits(tc, str_valued) {
        return Err(GnitzSqlError::Bind(format!(
            "cannot assign {} value to column '{}' ({tc:?})",
            if str_valued { "a string" } else { "an integer" },
            schema.columns[target].name,
        )));
    }
    Ok(p)
}

/// Read one SET RHS for `row` of the batch `view` presents. Infallible — every
/// rejection happened at [`classify_set_rhs`].
pub(crate) fn eval_set_program(p: &SetProgram, view: &ZSetBatchView<'_>, row: usize) -> ColumnValue {
    match p {
        SetProgram::Const(cv) => cv.clone(),
        SetProgram::StrCol(c) => match &view.batch().columns[*c] {
            ColData::Strings(v) => match &v[row] {
                Some(s) => ColumnValue::Str(s.clone()),
                None => ColumnValue::Null,
            },
            other => unreachable!("classify_set_rhs gates StrCol on TypeCode::String, got {other:?}"),
        },
        SetProgram::Num(ev) => match ev.eval_row(view, row) {
            (_, true) => ColumnValue::Null,
            (v, false) => ColumnValue::Int(v),
        },
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
    F: FnMut(usize) -> Option<ColumnValue>,
{
    dst.pks.push_from(&pk_src.pks, pk_idx);
    dst.weights.push(1);
    // Seed from the carry source's null word; each assignment flips only its own
    // payload bit (set on a NULL result, clear on non-NULL), unassigned bits ride.
    let mut null_bits = carry_src.nulls[carry_idx];
    for (payload_idx, ci, col_def) in schema.payload_columns() {
        match resolve(ci) {
            Some(cv) => {
                // The NULL a SET produces at *run* time — an explicit `= NULL`, or
                // NULL propagation through the compiled RHS — which
                // `classify_set_rhs` cannot see.
                let is_null = matches!(cv, ColumnValue::Null);
                check_not_null(col_def, is_null)?;
                null_word_set(&mut null_bits, payload_idx, is_null);
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
    assignments: &[(usize, SetProgram)],
    schema: &Schema,
    dst: &mut ZSetBatch,
) -> Result<(), GnitzSqlError> {
    // Pre-index assignments by column for O(1) lookup per payload column
    // (closes the prior O(cols²) per-row `assignments.iter().find`).
    let mut asn_by_col: Vec<Option<&SetProgram>> = vec![None; schema.columns.len()];
    for (ci, p) in assignments {
        asn_by_col[*ci] = Some(p);
    }
    // One view over `current` for the whole loop — `ViewBuffers::view` rebuilds
    // its region list per call, so a per-row view would be a malloc plus a
    // PK-region rebuild per row.
    let mut bufs = ViewBuffers::default();
    let view = bufs.view(current, schema);
    for &row_idx in matched {
        build_merged_row(current, row_idx, current, row_idx, schema, dst, |ci| {
            asn_by_col[ci].map(|p| eval_set_program(p, &view, row_idx))
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
    // Bind the WHERE once; classification and residuals run on the bound conjuncts.
    let bound = selection.map(|s| bind_single_table(s, schema)).transpose()?;
    match classify_access(bound.as_ref(), schema) {
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
            let (schema_opt, committed) = seek_pk_multi(client, tid, schema, &pks)?;
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
            // residual evaluator can always handle (an indexed 128-bit equality is
            // seekable but not evaluable). Buffered rows are unindexed, so once the
            // transaction has touched this table every row must face the FULL
            // predicate instead.
            let preds: Vec<&BoundExpr> = if net.is_empty() { residual } else { vec![where_expr] };
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
    preds: &[&BoundExpr],
) -> Result<ResolvedRows, GnitzSqlError> {
    let actual = schema_opt.as_deref().unwrap_or(schema);
    let batch = overlay_batch(committed, net, actual);
    let matched = matching_indices(preds, &batch, actual)?;
    Ok(ResolvedRows {
        schema: schema_opt,
        batch,
        matched,
    })
}

/// Fetch the committed candidates for a `Filtered` WHERE: the first existing
/// index's equality seek (with the reduced residual left over from consuming its
/// key columns), else the full scan (residual = the whole bound predicate). The
/// first index that exists serves the query — a hit with no matching rows is
/// terminal, it does NOT fall through to the scan.
type FilteredFetch<'e> = (Option<Arc<Schema>>, Option<ZSetBatch>, Vec<&'e BoundExpr>);

fn fetch_filtered<'e>(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    where_expr: &'e BoundExpr,
) -> Result<FilteredFetch<'e>, GnitzSqlError> {
    let candidates =
        collect_index_seek_candidates(where_expr, schema, || client.table_indexes(tid)).map_err(GnitzSqlError::Exec)?;
    if let Some(((_, _, residual), (schema_opt, batch_opt, _))) = first_index_hit(candidates, |(cols, vals, _)| {
        client.seek_by_index(tid, cols.as_slice(), vals)
    })? {
        return Ok((schema_opt, batch_opt, residual));
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
    update: &sqlparser::ast::Update,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let (table, assignments_raw, selection) = (&update.table, &update.assignments, &update.selection);

    let table_name = extract_table_factor_name(&table.relation, "UPDATE")?;

    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    // Bind SET assignments; reject PK writes and duplicate columns.
    let mut assignments: Vec<(usize, BoundExpr)> = Vec::new();
    let mut seen: Vec<usize> = Vec::with_capacity(assignments_raw.len());
    for assignment in assignments_raw {
        let col_idx = resolve_set_target(assignment, &schema, &mut seen, "UPDATE SET")?;
        assignments.push((col_idx, bind_single_table(&assignment.value, &schema)?));
    }

    // Read the target rows and build the SET batch under the RMW driver: an
    // autocommit UPDATE commits it lose-update-free via a one-precondition TXN
    // frame with bounded retry; inside a transaction it buffers and records the
    // read-set. The build re-runs per retry so a conflict re-reads fresh state.
    let count = commit_rmw_or_buffer(client, &table_name, table_id, |client| {
        let resolved = resolve_where_rows(client, table_id, &schema, selection.as_ref())?;
        let actual_schema = resolved.schema.as_deref().unwrap_or(&*schema);
        // Compile against the schema the rows actually carry — the reply schema,
        // not the catalog one the RHS was bound against: resolution bakes in
        // payload slots, PK byte offsets, type codes and the nullability verdict,
        // so compiling against the wrong schema miscomputes silently.
        //
        // Unconditional, above the row-count check: an un-compilable RHS
        // (`SET int_col = float_col`, a wide literal) then errors deterministically
        // instead of only when at least one row matched.
        let programs = assignments
            .iter()
            .map(|(ci, e)| Ok((*ci, classify_set_rhs(e, *ci, actual_schema)?)))
            .collect::<Result<Vec<_>, GnitzSqlError>>()?;
        let count = resolved.matched.len();
        let write = if count > 0 {
            let mut updates = ZSetBatch::new(actual_schema);
            write_set_rows(
                &resolved.batch,
                &resolved.matched,
                &programs,
                actual_schema,
                &mut updates,
            )?;
            Some(RmwWrite {
                schema: actual_schema.clone(),
                batch: updates,
                mode: WireConflictMode::Update,
            })
        } else {
            None
        };
        Ok(RmwBuild { count, write })
    })?;
    Ok(SqlResult::RowsAffected { count })
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

pub(crate) fn execute_delete(
    client: &mut GnitzClient,
    _schema_name: &str,
    del: &sqlparser::ast::Delete,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
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

    // Resolve the target PKs and build the retraction batch under the RMW driver
    // (autocommit: one-precondition TXN frame with bounded retry; in a
    // transaction: buffer + record the read-set). The build re-runs per retry.
    let count = commit_rmw_or_buffer(client, &table_name, table_id, |client| {
        let resolved = resolve_where_rows(client, table_id, &schema, del.selection.as_ref())?;
        let actual_schema = resolved.schema.as_deref().unwrap_or(&*schema);
        let count = resolved.matched.len();
        let write = if count > 0 {
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
            Some(RmwWrite {
                schema: actual_schema.clone(),
                batch: retraction_batch(actual_schema, pks),
                mode: WireConflictMode::Update,
            })
        } else {
            None
        };
        Ok(RmwBuild { count, write })
    })?;
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

    /// Compile a SET list the way the RMW closure does — against the schema the
    /// rows carry. A test whose RHS does not compile is a bug in the test.
    fn programs(assignments: &[(usize, BoundExpr)], schema: &Schema) -> Vec<(usize, SetProgram)> {
        assignments
            .iter()
            .map(|(ci, e)| {
                (
                    *ci,
                    classify_set_rhs(e, *ci, schema).expect("test SET RHS must compile"),
                )
            })
            .collect()
    }

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
        write_set_rows(&current, &[0], &programs(&assignments, &schema), &schema, &mut dst).unwrap();

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
        write_set_rows(&current, &[0], &programs(&assignments, &schema), &schema, &mut dst).unwrap();

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
        write_set_rows(&current, &[0], &programs(&assignments, &schema), &schema, &mut dst).unwrap();

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
        write_set_rows(&current, &[0], &programs(&assignments, &schema), &schema, &mut dst).unwrap();

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
            if ci == 1 {
                Some(ColumnValue::Null)
            } else {
                None
            }
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

    // ------------------------------------------------------------------
    // SET right-hand sides through the shared evaluator
    // ------------------------------------------------------------------

    /// The written `i64` of `dst`'s single-row payload column `ci`.
    fn written_i64(dst: &ZSetBatch, ci: usize) -> i64 {
        match &dst.columns[ci] {
            ColData::Fixed(buf) => i64::from_le_bytes(buf[..8].try_into().unwrap()),
            other => panic!("expected a Fixed column, got {other:?}"),
        }
    }

    /// A numeric RHS reading a **nullable** source column: the compiled program
    /// resolves `no_nulls = false`, so a NULL source must come back as
    /// `ColumnValue::Null` and set the destination's null bit rather than reading
    /// the filler zeros as a real `0`.
    #[test]
    fn set_numeric_over_a_nullable_column() {
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::I64, true),
                col_def("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        // b = 5 (non-null) in row 0, b = NULL in row 1.
        let mut current = ZSetBatch::new(&schema);
        for (i, (bits, null_word)) in [(5i64, 0u64), (0, 0b10)].into_iter().enumerate() {
            current.pks.push_u128(i as u128 + 1);
            current.weights.push(1);
            current.nulls.push(null_word);
            if let ColData::Fixed(buf) = &mut current.columns[1] {
                buf.extend_from_slice(&0i64.to_le_bytes());
            }
            if let ColData::Fixed(buf) = &mut current.columns[2] {
                buf.extend_from_slice(&bits.to_le_bytes());
            }
        }
        // SET a = b + 1
        let rhs = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(2)),
            crate::ir::BinOp::Add,
            Box::new(BoundExpr::LitInt(1)),
        );
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(&current, &[0, 1], &programs(&[(1, rhs)], &schema), &schema, &mut dst).unwrap();

        assert_eq!(written_i64(&dst, 1), 6, "non-null source: b + 1");
        assert_eq!(dst.nulls[0] & 0b01, 0, "row 0's a is not null");
        assert_ne!(dst.nulls[1] & 0b01, 0, "NULL + 1 is NULL, and its bit must be set");
    }

    /// A SET RHS reading the PK column — the PK region through the adapter, which
    /// no other SET test exercises. It is also the shape a deferred cross-column
    /// cell copy would have aborted on: a PK column's slot in `ZSetBatch.columns`
    /// is an empty placeholder.
    #[test]
    fn set_reads_the_pk_column() {
        let schema = two_col(TypeCode::I64);
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(41u128);
        current.weights.push(1);
        current.nulls.push(0);
        if let ColData::Fixed(buf) = &mut current.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        // SET val = pk + 1
        let plus_one = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(0)),
            crate::ir::BinOp::Add,
            Box::new(BoundExpr::LitInt(1)),
        );
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(&current, &[0], &programs(&[(1, plus_one)], &schema), &schema, &mut dst).unwrap();
        assert_eq!(written_i64(&dst, 1), 42);

        // And the bare `SET val = pk` form.
        let mut dst = ZSetBatch::new(&schema);
        write_set_rows(
            &current,
            &[0],
            &programs(&[(1, BoundExpr::ColRef(0))], &schema),
            &schema,
            &mut dst,
        )
        .unwrap();
        assert_eq!(written_i64(&dst, 1), 41);
    }

    /// A float-typed RHS into an integer column is rejected at compile — without
    /// it the raw f64 bit pattern would pass `append_column_value`'s guard and
    /// commit as a nonsense integer.
    #[test]
    fn set_int_column_from_float_expression_rejects() {
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("n", TypeCode::I64, true),
                col_def("f", TypeCode::F64, true),
            ],
            pk_cols: vec![0],
        };
        let Err(err) = classify_set_rhs(&BoundExpr::ColRef(2), 1, &schema) else {
            panic!("SET int = float must reject");
        };
        assert!(
            err.to_string().contains("floating-point"),
            "error must name the cause: {err}"
        );
        // A float *comparison* is integer-valued (0/1) and stays servable.
        let cmp = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(2)),
            crate::ir::BinOp::Gt,
            Box::new(BoundExpr::LitFloat(1.5)),
        );
        assert!(classify_set_rhs(&cmp, 1, &schema).is_ok());
    }
}
