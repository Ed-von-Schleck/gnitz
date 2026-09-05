//! UPDATE and DELETE: plan the WHERE once through the shared access-path ladder
//! (`dml::plan`), read the matching rows (UPDATE) or just their keys (DELETE)
//! back from the server, then write the SET batch or the retraction. The SET-list
//! helpers (`classify_set_rhs`, `bind_set_program`, `eval_set_value`,
//! `resolve_set_target`) are also reused by INSERT's `ON CONFLICT DO UPDATE`.

use std::sync::Arc;

use crate::ast_util::{extract_name, extract_table_name_and_alias};
use crate::bind::{bind_single_table, find_unique_column, Binder};
use crate::codec::colwrite::{append_column_value, check_not_null, set_target_admits, ColumnValue};
use crate::codec::pk_codec::{bound_num_literal, pack_pk_value, NumLit};
use crate::codec::project_schema::{build_read_projection, read_reply_shape};
use crate::dml::overlay::{buffered_net, present_rows};
use crate::dml::plan::{bind_where, bound_and_predicate, fetch_bound, AccessPlan, ReadBudget};
use crate::dml::rmw::{commit_rmw_or_buffer, RmwBuild, RmwWrite};
use crate::error::GnitzSqlError;
use crate::exec::batch::RowGather;
use crate::exec::residual::matching_indices;
use crate::expr_lower::compile_scalar_evaluator;
use crate::ir::BoundExpr;
use crate::validate::{reject_unhonored_delete_clauses, reject_unhonored_update_clauses};
use crate::SqlResult;
use gnitz_core::null_word_set;
use gnitz_core::{
    retraction_batch, ColData, ColumnDef, GnitzClient, Schema, TypeCode, ViewBuffers, WireConflictMode, ZSetBatch,
    ZSetBatchView,
};
use gnitz_expr::{Evaluator, ExprResults};
use gnitz_wire::ReadSink;
use sqlparser::ast::{Assignment, AssignmentTarget, FromTable};

// ---------------------------------------------------------------------------
// SET-list helpers (shared with INSERT's ON CONFLICT DO UPDATE)
// ---------------------------------------------------------------------------

/// A compiled SET / `DO UPDATE SET` right-hand side.
///
/// `Const` and `StrCol` are shortcuts for row-independent or verbatim values:
/// reaching a literal through the VM would run a whole single-row `eval_batch`
/// per matched row to re-derive a constant, and a bare column read needs no
/// program at all.
///
/// `StrCol` is gated on `TypeCode::String` rather than `is_german_string()` on
/// purpose: a BLOB column must fall into `Expr`, where `OpcodeBackend`'s column
/// load rejects it. That gate also keeps the arm structurally safe — a PK column
/// can never be STRING (§1), so `StrCol` can never name one, whose slot in
/// `ZSetBatch.columns` is an empty placeholder.
pub(crate) enum SetProgram {
    /// A row-independent value: a literal, or `NULL`.
    Const(ColumnValue),
    /// A bare reference to a `TypeCode::String` column, read verbatim.
    StrCol(usize),
    /// A computed value, of whichever class the program resolved to. Boxed so a
    /// `SetProgram` stays small enough to sit in a `Vec` beside the other arms.
    Expr(Box<Evaluator>),
}

/// Classify one SET RHS for target column `target` against the schema the rows
/// it will run over carry. Shared by `UPDATE SET` and `ON CONFLICT DO UPDATE
/// SET`, including the latter's `EXCLUDED.<col>` short-circuit, so a string
/// EXCLUDED assignment classifies the same way a bare one does.
///
/// The target check is what makes [`eval_set_value`] infallible. Without it a
/// kind mismatch (`SET int_col = 'abc'`) would surface only from
/// `append_column_value`, i.e. only once a row matched — so a zero-match WHERE
/// would report 0 rows updated instead of rejecting.
pub(crate) fn classify_set_rhs(expr: &BoundExpr, target: usize, schema: &Schema) -> Result<SetProgram, GnitzSqlError> {
    let tc = schema.columns[target].type_code;
    // Every constant integer shape lands in `Const`, including the negated and
    // the wide ones the VM cannot lower — so a literal past `i64::MAX` writes
    // (SET reaches the upper half of U64) and an out-of-range one is caught by
    // the range check below, before any row is read.
    let p = if let Some(v) = bound_num_literal(expr).and_then(NumLit::to_i128) {
        SetProgram::Const(ColumnValue::Int(v))
    } else {
        match expr {
            BoundExpr::LitStr(s) => SetProgram::Const(ColumnValue::Str(s.clone())),
            BoundExpr::LitNull => SetProgram::Const(ColumnValue::Null),
            // `ColData::empty_for(TypeCode::String)` is the `Strings` variant and
            // every batch reaching SET is built from a `Schema`, so the declared
            // type code decides the representation.
            BoundExpr::ColRef(c) if schema.columns[*c].type_code == TypeCode::String => SetProgram::StrCol(*c),
            // A SET value is written into a fixed-width integer or a string
            // column; an f64 register has no destination, and nothing downstream
            // can tell its bit pattern from an integer's.
            _ if expr.infer_type(&schema.columns).is_float() => {
                return Err(GnitzSqlError::Unsupported(
                    "SET from a floating-point expression is not supported".to_string(),
                ))
            }
            _ => SetProgram::Expr(Box::new(compile_scalar_evaluator(expr, schema)?)),
        }
    };
    let str_valued = match &p {
        SetProgram::Const(ColumnValue::Null) => return Ok(p), // NULL suits every column
        SetProgram::Const(cv) => matches!(cv, ColumnValue::Str(_)),
        SetProgram::StrCol(_) => true,
        SetProgram::Expr(ev) => ev.result_is_str(),
    };
    if !set_target_admits(tc, str_valued) {
        return Err(GnitzSqlError::Bind(format!(
            "cannot assign {} value to column '{}' ({tc:?})",
            if str_valued { "a string" } else { "an integer" },
            schema.columns[target].name,
        )));
    }
    // A row-independent value is range-checked here, where the column has a name
    // and no row has been read yet; a computed one can only be checked per row,
    // in `append_column_value`, against the same `pack_pk_value` rule.
    if let SetProgram::Const(ColumnValue::Int(v)) = &p {
        if pack_pk_value(tc, *v).is_none() {
            return Err(GnitzSqlError::Bind(format!(
                "{tc:?} value out of range for column '{}': {v}",
                schema.columns[target].name,
            )));
        }
    }
    Ok(p)
}

/// A SET right-hand side bound to the batch it will be read against: the
/// computed arm is driven over the whole batch when this is built, where a
/// per-row drive would pay `eval_batch`'s prologue for one row.
pub(crate) enum SetValues<'a> {
    Const(&'a ColumnValue),
    /// A `TypeCode::String` column of the bound batch, read verbatim.
    StrCol(usize),
    Computed(ExprResults),
}

/// Bind `p` to `view`, driving its computed arm over every row up front.
pub(crate) fn bind_set_program<'a>(p: &'a SetProgram, view: &ZSetBatchView<'_>) -> SetValues<'a> {
    match p {
        SetProgram::Const(cv) => SetValues::Const(cv),
        SetProgram::StrCol(c) => SetValues::StrCol(*c),
        SetProgram::Expr(ev) => SetValues::Computed(ev.eval_all(view)),
    }
}

/// Read one SET RHS for `row` of the batch `v` was bound against. Infallible:
/// every *kind* rejection happened at [`classify_set_rhs`]. A computed value's
/// range is the one verdict that cannot be reached until the value exists, so it
/// is taken where the value is written (`append_column_value`).
pub(crate) fn eval_set_value(v: &SetValues<'_>, view: &ZSetBatchView<'_>, row: usize) -> ColumnValue {
    match v {
        SetValues::Const(cv) => (*cv).clone(),
        SetValues::StrCol(c) => match &view.batch().columns[*c] {
            ColData::Strings(vals) => match &vals[row] {
                Some(s) => ColumnValue::Str(s.clone()),
                None => ColumnValue::Null,
            },
            other => unreachable!("classify_set_rhs gates StrCol on TypeCode::String, got {other:?}"),
        },
        // A `match` rather than `map_or`: this module builds at opt-level 0,
        // where `map_or` plus a constructor-as-closure is two out-of-line calls
        // moving a ~24-byte enum, and the match is free.
        SetValues::Computed(ExprResults::Scalar(vals)) => match vals[row] {
            None => ColumnValue::Null,
            Some(x) => ColumnValue::Int(x as i128),
        },
        SetValues::Computed(ExprResults::Str { bytes, spans }) => match spans[row] {
            None => ColumnValue::Null,
            // Valid UTF-8 by construction: this path runs over the client-side
            // `ZSetBatchView`, whose strings were UTF-8-validated at the wire
            // decode boundary, and every string function preserves validity —
            // ASCII case fold, character-unit substring, ASCII trim set, and
            // concatenation of valid inputs.
            Some((o, l)) => String::from_utf8(bytes[o..o + l].to_vec()).map_or(ColumnValue::Null, ColumnValue::Str),
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

/// [`build_merged_row`]'s row-invariant plan: each payload column's bitmap slot,
/// physical index, definition and wire stride, resolved once per statement — the
/// caller hoists it out of its row loop.
pub(crate) fn merge_payload_plan(schema: &Schema) -> Vec<(usize, usize, &ColumnDef, usize)> {
    schema
        .payload_columns()
        .map(|(pi, ci, def)| (pi, ci, def, def.type_code.wire_stride()))
        .collect()
}

/// Build one merged Z-set row into `dst`: PK from `(pk_src, pk_idx)`; null-bitmap
/// seed and carried (unassigned) columns from `(carry_src, carry_idx)`. For each
/// payload column, `resolve(ci)` returns `Some(value)` to write that value (and
/// set/clear its null bit) or `None` to carry the column through unchanged from
/// `carry_src`. Shared by UPDATE SET (pk_src == carry_src) and ON CONFLICT DO
/// UPDATE (PK from the incoming row, carry/null-seed from the existing row).
///
/// `payload` is [`merge_payload_plan`] collected once by the caller: both the
/// `pk_cols` re-scan and the per-column wire stride are row-invariant, and over
/// a large matched set re-deriving them costs more than the merge itself — the
/// same reason [`RowGather`] resolves its plan up front.
pub(crate) fn build_merged_row<F>(
    pk_src: &ZSetBatch,
    pk_idx: usize,
    carry_src: &ZSetBatch,
    carry_idx: usize,
    payload: &[(usize, usize, &ColumnDef, usize)],
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
    for &(payload_idx, ci, col_def, stride) in payload {
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
            None => carry_src.columns[ci].push_row_from(carry_idx, stride, &mut dst.columns[ci]),
        }
    }
    dst.nulls.push(null_bits);
    Ok(())
}

/// Write the SET-merged update row for every row of `current` into `dst` (each at
/// weight +1). `current` is the matched set outright — the server applied the
/// whole WHERE to the committed rows and `resolve_where_rows` filtered the
/// buffered ones. The assignment index is built once and reused across rows — it
/// depends only on `assignments` and `schema`, not the row — mirroring INSERT's
/// `client_side_merge_do_update` loop.
fn write_set_rows(
    current: &ZSetBatch,
    assignments: &[(usize, SetProgram)],
    schema: &Schema,
    dst: &mut ZSetBatch,
) -> Result<(), GnitzSqlError> {
    // One view over `current` for the whole loop — `ViewBuffers::view` rebuilds
    // its region list per call, so a per-row view would be a malloc plus a
    // PK-region rebuild per row.
    let mut bufs = ViewBuffers::default();
    let view = bufs.view(current, schema);
    // Bound before the row loop, which then only reads a buffer — leaving
    // `build_merged_row` and `append_column_value` row-major, so the order their
    // errors are raised in is unchanged.
    let bound: Vec<SetValues<'_>> = assignments.iter().map(|(_, p)| bind_set_program(p, &view)).collect();
    // Pre-index assignments by column for O(1) lookup per payload column
    // (closes the prior O(cols²) per-row `assignments.iter().find`).
    let mut asn_by_col: Vec<Option<&SetValues<'_>>> = vec![None; schema.columns.len()];
    for ((ci, _), v) in assignments.iter().zip(&bound) {
        asn_by_col[*ci] = Some(v);
    }
    let payload = merge_payload_plan(schema);
    for row_idx in 0..current.len() {
        build_merged_row(current, row_idx, current, row_idx, &payload, dst, |ci| {
            asn_by_col[ci].map(|v| eval_set_value(v, &view, row_idx))
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// WHERE resolution — one read for both verbs, under the sink each one needs
// ---------------------------------------------------------------------------

/// DELETE's read shape: the source PK columns and nothing else. `build_read_projection`
/// over an empty SELECT list is exactly that — the PK is always prepended, and
/// there is no payload item to follow it — so the reply carries the key region
/// and no blob heap, and the projection program relocates nothing.
fn pk_only_reply(schema: &Schema, alias: &str) -> Result<(Schema, ReadSink), GnitzSqlError> {
    let (items, out_cols) = build_read_projection(&[], schema, alias)?;
    let (reply_schema, projection) = read_reply_shape(&items, out_cols, schema)?;
    let sink = ReadSink::Rows {
        projection,
        order: Vec::new(),
        limit_k: 0, // unbounded
    };
    Ok((reply_schema, sink))
}

/// The rows a single-table UPDATE/DELETE `WHERE` (or its absence) resolves to,
/// under `reply_schema`. Every row of the result matches, so the caller writes
/// the whole batch — UPDATE reads it under the catalog schema (an identity sink)
/// and merges the SET list into it; DELETE reads a PK-only reply and keeps its
/// key region.
///
/// **Committed reply rows are final** — the server applied `bound ∧ predicate`,
/// which together are the whole WHERE. Only the transaction's own buffered rows,
/// which no server-side walk ever saw, are re-filtered here. So **in autocommit
/// the reply IS the answer**, returned wholesale with no copy and no client-side
/// predicate compiled at all.
fn resolve_where_matches(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    plan: &AccessPlan<'_>,
    sink: &ReadSink,
    reply_schema: &Arc<Schema>,
) -> Result<ZSetBatch, GnitzSqlError> {
    let mut committed = fetch_bound(client, tid, &plan.access, sink, reply_schema)?;
    // In autocommit there is no buffer to overlay, and `buffered_scope`'s gather
    // can be megabytes of `PkTuple` for a large `pk IN (…)` — so ask only when a
    // transaction is open, which is the condition its doc already states.
    if !client.txn_active() {
        return Ok(committed);
    }
    let (keys, preds) = plan.buffered_scope(schema);
    let net = buffered_net(client, tid, keys.as_deref());
    if net.is_empty() {
        return Ok(committed);
    }
    let mut present = present_rows(&net, schema);
    let matched = matching_indices(preds, &present, schema)?;

    let n = committed.len() + matched.len();
    let mut eff = ZSetBatch::with_capacity(reply_schema, n);
    let gather = RowGather::new(reply_schema);
    for i in 0..committed.len() {
        // A PK the transaction has written is decided by its buffered version
        // below, whatever the committed row said.
        if !net.contains_key(&committed.pks.get_tuple(i)) {
            gather.take(&mut committed, i, &mut eff);
        }
    }
    for i in matched {
        gather.take(&mut present, i, &mut eff);
    }
    Ok(eff)
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

pub(crate) fn execute_update(
    client: &mut GnitzClient,
    update: &sqlparser::ast::Update,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_update_clauses(update)?;
    let (table, assignments_raw, selection) = (&update.table, &update.assignments, &update.selection);

    let (table_name, table_alias) = extract_table_name_and_alias(&table.relation, "UPDATE")?;

    let target = binder.resolve_base_table(client, &table_name)?;
    let (table_id, schema) = (target.tid, &target.schema);

    // Bind SET assignments; reject PK writes and duplicate columns.
    let mut assignments: Vec<(usize, BoundExpr)> = Vec::new();
    let mut seen: Vec<usize> = Vec::with_capacity(assignments_raw.len());
    for assignment in assignments_raw {
        let col_idx = resolve_set_target(assignment, schema, &mut seen, "UPDATE SET")?;
        assignments.push((col_idx, bind_single_table(&assignment.value, schema, &table_alias)?));
    }

    // Compile the SET list against the catalog schema — the one the RHS was bound
    // against, and the one the rows come back under, since the identity sink
    // replies in the source shape. Resolution bakes in payload slots, PK byte
    // offsets, type codes and the nullability verdict.
    //
    // Ahead of the read, not inside it: an un-compilable RHS (`SET int_col =
    // float_col`) or an out-of-range constant errors deterministically, never
    // only when at least one row matched.
    let programs = assignments
        .iter()
        .map(|(ci, e)| Ok((*ci, classify_set_rhs(e, *ci, schema)?)))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;

    let where_expr = bind_where(schema, &table_alias, selection.as_ref())?;
    let plan = bound_and_predicate(schema, &where_expr, ReadBudget::MayChunk, &target.indexes)?;
    let sink = ReadSink::all_rows();

    // Read the target rows and build the SET batch under the RMW driver: an
    // autocommit UPDATE commits it lose-update-free via a one-precondition TXN
    // frame with bounded retry; inside a transaction it buffers and records the
    // read-set. The build re-runs per retry so a conflict re-reads fresh state.
    let count = commit_rmw_or_buffer(client, &table_name, table_id, schema, |client| {
        let matched = resolve_where_matches(client, table_id, schema, &plan, &sink, schema)?;
        let count = matched.len();
        let write = if count > 0 {
            let mut updates = ZSetBatch::with_capacity(schema, count);
            write_set_rows(&matched, &programs, schema, &mut updates)?;
            Some(RmwWrite {
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
    del: &sqlparser::ast::Delete,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_delete_clauses(del)?;
    let tables = match &del.from {
        FromTable::WithFromKeyword(ts) | FromTable::WithoutKeyword(ts) => ts,
    };
    if tables.len() != 1 || !tables[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "DELETE: exactly one simple FROM table required".to_string(),
        ));
    }
    let (table_name, table_alias) = extract_table_name_and_alias(&tables[0].relation, "DELETE")?;

    let target = binder.resolve_base_table(client, &table_name)?;
    let (table_id, schema) = (target.tid, &target.schema);

    let where_expr = bind_where(schema, &table_alias, del.selection.as_ref())?;
    let plan = bound_and_predicate(schema, &where_expr, ReadBudget::MayChunk, &target.indexes)?;
    let (reply_schema, sink) = pk_only_reply(schema, &table_alias)?;
    let reply_schema = Arc::new(reply_schema);

    // Resolve the target PKs and build the retraction batch under the RMW driver
    // (autocommit: one-precondition TXN frame with bounded retry; in a
    // transaction: buffer + record the read-set). The build re-runs per retry.
    //
    // The retraction is built under the CATALOG schema, whose payload placeholders
    // `retraction_batch` fills, so an in-transaction DELETE buffers under the same
    // schema INSERT does (`TxnBuffer` extends a tid's later batches into the first
    // family's schema).
    let count = commit_rmw_or_buffer(client, &table_name, table_id, schema, |client| {
        let matched = resolve_where_matches(client, table_id, schema, &plan, &sink, &reply_schema)?;
        let pks = matched.pks;
        let count = pks.len();
        let write = (count > 0).then(|| RmwWrite {
            batch: retraction_batch(schema, pks),
            mode: WireConflictMode::Update,
        });
        Ok(RmwBuild { count, write })
    })?;
    Ok(SqlResult::RowsAffected { count })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/mutate.rs"]
mod tests;
