//! INSERT, including the `ON CONFLICT` upsert family. The default form pushes
//! with `WireConflictMode::Error`; `DO NOTHING` / `DO UPDATE` are resolved
//! client-side (seek existing PKs, then filter or merge) before a single push.
//! The SET-list binding and evaluation reuse `mutate`'s shared helpers so a
//! `DO UPDATE SET` assignment behaves exactly like an `UPDATE ... SET`.

use std::sync::Arc;

use crate::ast_util::{bind_constant, extract_object_name, Constant};
use crate::bind::{bind_single_table, find_unique_column, Binder};
use crate::codec::colwrite::{append_value_to_col, check_not_null};
use crate::codec::pk_codec::PkPlan;
use crate::dml::mutate::{
    bind_set_program, build_merged_row, classify_set_rhs, eval_set_value, merge_payload_plan, resolve_set_target,
    SetProgram, SetValues,
};
use crate::dml::overlay::{effective_rows, Conflict};
use crate::dml::rmw::{commit_rmw_or_buffer, RmwBuild, RmwWrite};
use crate::error::GnitzSqlError;
use crate::exec::batch::{project, resolve_projection, RowGather};
use crate::ir::{BExpr, BoundExpr};
use crate::validate::reject_unhonored_insert_clauses;
use crate::SqlResult;
use gnitz_core::{
    push_zero_cell, ColumnDef, GnitzClient, RelClass, Schema, WireConflictMode, ZSetBatch, ZSetBatchView,
};
use sqlparser::ast::{
    Assignment, ConflictTarget, Expr, Insert, ObjectName, OnConflict, OnConflictAction, OnInsert, Parens, Query,
    SetExpr, TableObject, Values,
};

/// The resolved INSERT disposition after the ON CONFLICT clause (if any) is bound.
/// The conflict target itself is validated and discarded in `validate_conflict_target`;
/// only the action survives into the plan.
enum ConflictPlan {
    /// Default SQL INSERT: push with WireConflictMode::Error.
    Error,
    /// `ON CONFLICT ... DO NOTHING`: filter the conflicting rows out, push the rest.
    DoNothing,
    /// `ON CONFLICT ... DO UPDATE SET ...`: merge each conflicting row, push all.
    DoUpdate {
        assignments: Vec<(usize, SetProgram, Scope)>,
    },
}

/// Which row a `DO UPDATE SET` right-hand side reads. `EXCLUDED.col` is the only
/// construct that escapes the existing-row scope.
#[derive(Clone, Copy)]
enum Scope {
    Existing,
    Excluded,
}

/// A conflict target must name exactly the primary key — in any order, as
/// PostgreSQL allows — or be absent, which means the same key. A `UNIQUE` span is
/// not a target: a row duplicating one reaches the engine and raises there.
fn validate_conflict_target(target: &Option<ConflictTarget>, schema: &Schema) -> Result<(), GnitzSqlError> {
    match target {
        None => Ok(()),
        Some(ConflictTarget::Columns(cols)) => {
            let mut named: Vec<u32> = Vec::with_capacity(cols.len());
            for c in cols {
                let ci = find_unique_column(&schema.columns, c.value.as_str())?
                    .ok_or_else(|| GnitzSqlError::Bind(format!("ON CONFLICT ({}): column not found", c.value)))?;
                named.push(ci as u32);
            }
            let mut pk = schema.pk_cols.clone();
            named.sort_unstable();
            pk.sort_unstable();
            if named != pk {
                return Err(GnitzSqlError::Unsupported(format!(
                    "ON CONFLICT target must name exactly the primary key ({})",
                    pk.iter()
                        .map(|&ci| schema.columns[ci as usize].name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                )));
            }
            Ok(())
        }
        Some(ConflictTarget::OnConstraint(_)) => Err(GnitzSqlError::Unsupported(
            "ON CONFLICT ON CONSTRAINT not supported".to_string(),
        )),
    }
}

/// How an INSERT's VALUES rows map onto the table's columns.
struct RowShape {
    /// Physical column → VALUES slot. `None` where the column takes no user
    /// value: SERIAL, hidden, or unnamed by an explicit column list.
    slot_of: Vec<Option<usize>>,
    /// Values every VALUES row must supply.
    expected: usize,
    /// The SERIAL PK's column, if the table has one.
    serial_ci: Option<usize>,
}

/// A column list names the slots and the arity; without one a row supplies every
/// visible non-SERIAL column in schema order. An unnamed column is written NULL —
/// gnitz has no column DEFAULTs, and `check_not_null` catches the rest.
fn insert_row_shape(columns: &[ObjectName], schema: &Schema) -> Result<RowShape, GnitzSqlError> {
    let mut slot_of: Vec<Option<usize>> = vec![None; schema.columns.len()];
    if columns.is_empty() {
        let mut expected = 0usize;
        let mut serial_ci = None;
        for (ci, c) in schema.visible_columns() {
            if c.is_serial {
                serial_ci = Some(ci);
            } else {
                slot_of[ci] = Some(expected);
                expected += 1;
            }
        }
        return Ok(RowShape { slot_of, expected, serial_ci });
    }
    for (k, name) in columns.iter().enumerate() {
        // One part only: `INSERT INTO t (t.a)` is rejected, not truncated to `a`.
        let ident = match &name.0[..] {
            [part] => part.as_ident(),
            _ => None,
        }
        .ok_or_else(|| GnitzSqlError::Plan("INSERT column list: column must be a simple identifier".into()))?
        .value
        .as_str();
        let ci = find_unique_column(&schema.columns, ident)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{ident}' not found in the INSERT column list")))?;
        if schema.columns[ci].is_serial {
            return Err(GnitzSqlError::Unsupported(
                "cannot supply a value for a SERIAL column; omit it from the INSERT".to_string(),
            ));
        }
        if slot_of[ci].is_some() {
            return Err(GnitzSqlError::Bind(format!(
                "column '{ident}' specified more than once in the INSERT column list"
            )));
        }
        slot_of[ci] = Some(k);
    }
    // The written list never names the SERIAL column, so its index still comes
    // from a scan. A SERIAL column is never hidden: DROP COLUMN refuses one.
    let serial_ci = schema.columns.iter().position(|c| c.is_serial);
    Ok(RowShape {
        slot_of,
        expected: columns.len(),
        serial_ci,
    })
}

pub(crate) fn execute_insert(
    client: &mut GnitzClient,
    insert: &Insert,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_insert_clauses(insert)?;
    let table_name_str = match &insert.table {
        TableObject::TableName(obj_name) => extract_object_name(obj_name, binder.schema_name(), "INSERT")?,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "INSERT with table function not supported".to_string(),
            ))
        }
    };
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| GnitzSqlError::Unsupported("INSERT without VALUES not supported".to_string()))?;
    let rows = extract_values_rows(source)?;

    let target = binder.resolve_push_target(client, &table_name_str)?;
    let (tid, schema) = (target.tid, &target.schema);
    let is_stream = target.class == RelClass::Stream;

    // RETURNING is supported on the plain-INSERT path only; capturing the
    // effective row under ON CONFLICT (which may UPDATE or skip a row) is out of
    // scope.
    if insert.returning.is_some() && insert.on.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "RETURNING with ON CONFLICT is not supported".to_string(),
        ));
    }

    // Resolve the ON CONFLICT clause into a `ConflictPlan`.
    let plan = match insert.on.as_ref() {
        None => ConflictPlan::Error,
        Some(OnInsert::DuplicateKeyUpdate(_)) => {
            return Err(GnitzSqlError::Unsupported(
                "ON DUPLICATE KEY UPDATE not supported — use PostgreSQL-style \
                 ON CONFLICT (col) DO UPDATE"
                    .to_string(),
            ));
        }
        Some(OnInsert::OnConflict(OnConflict { conflict_target, action })) => {
            // A stream has no unique PK to conflict on, and no stored rows for
            // either action to resolve the incoming one against.
            if is_stream {
                return Err(GnitzSqlError::Unsupported(format!(
                    "'{table_name_str}' is a stream; ON CONFLICT needs stored rows to resolve against"
                )));
            }
            validate_conflict_target(conflict_target, schema)?;

            match action {
                OnConflictAction::DoNothing => ConflictPlan::DoNothing,
                OnConflictAction::DoUpdate(do_update) => {
                    if do_update.selection.is_some() {
                        return Err(GnitzSqlError::Unsupported(
                            "ON CONFLICT ... DO UPDATE WHERE not supported".to_string(),
                        ));
                    }
                    let assignments = bind_do_update_assignments(&do_update.assignments, schema, &table_name_str)?;
                    ConflictPlan::DoUpdate { assignments }
                }
            }
        }
        Some(_) => {
            return Err(GnitzSqlError::Unsupported(
                "unsupported ON clause in INSERT".to_string(),
            ));
        }
    };

    // Build the incoming batch from VALUES rows, sized for the known row count.
    let n = rows.len();
    let mut batch = ZSetBatch::with_capacity(schema, n);

    let RowShape { slot_of, expected, serial_ci } = insert_row_shape(&insert.columns, schema)?;
    // Built once: `payload_columns` filters on `is_pk_col`, itself a PK-list scan,
    // so leaving it in the row loop pays that scan per row per column.
    let payload = merge_payload_plan(schema);
    // One bound cell per VALUES slot, reused across rows and read by both
    // consumers below, so a row's PK slot and payload slot cannot disagree on
    // what a written constant is.
    let mut cells: Vec<Constant> = Vec::new();
    // The row count is known here, so a SERIAL statement's ids come from one
    // durable advance and each row stamps `base + i`. A row failing the arity
    // guard below abandons the rest — a wider gap of the same intentional kind.
    let pk_plan = match serial_ci {
        Some(ci) => PkPlan::serial(
            schema,
            client.reserve_serial_ids(tid, n as u64)?,
            schema.columns[ci].type_code,
        ),
        None => PkPlan::written(&slot_of, schema)?,
    };

    for (row_i, row) in rows.iter().enumerate() {
        // Standard SQL rejects a VALUES row whose arity differs from the expected
        // count, in either direction — too few values, or excess trailing ones.
        // This guard makes every per-column index below in-bounds.
        if row.len() != expected {
            let hint = if pk_plan.is_serial() {
                " (its SERIAL primary key is auto-assigned)"
            } else {
                ""
            };
            return Err(GnitzSqlError::Bind(format!(
                "INSERT specifies {} value(s) but table '{}' expects {} value(s){}",
                row.len(),
                table_name_str,
                expected,
                hint
            )));
        }
        cells.clear();
        for e in row.iter() {
            cells.push(bind_constant(e)?);
        }
        pk_plan.push(row_i, &cells, &mut batch.pks)?;
        batch.weights.push(1);

        let mut null_bits: u64 = 0;
        for &(payload_idx, ci, col_def) in &payload {
            if col_def.is_hidden {
                // Logical-dropped column: a zero-filled NOT-NULL filler cell (null
                // bit left unset), keeping the batch rectangular and the table on
                // the FixedIntNonnull comparator. The value is unobservable (§6).
                push_zero_cell(&mut batch.columns[ci], col_def.type_code);
                continue;
            }
            // Read off the *bound* constant, so `+NULL` is the NULL it spells;
            // a column the list left out is NULL too.
            let cell = slot_of[ci].map(|s| &cells[s]);
            let is_null = cell.is_none_or(|c| matches!(c.lit, BExpr::LitNull));
            // The check is here for the *conflicting* row of an ON CONFLICT DO
            // UPDATE: its incoming NULL is consumed into the merged row and is
            // never pushed, so the wire boundary's own check never sees it.
            check_not_null(col_def, is_null)?;
            if is_null {
                gnitz_wire::null_word_set(&mut null_bits, payload_idx, true);
            }
            let ZSetBatch { columns, blob, .. } = &mut batch;
            match cell {
                Some(c) => append_value_to_col(&mut columns[ci], blob, col_def.type_code, c)?,
                // `append_value_to_col` encodes a written NULL as exactly this.
                None => push_zero_cell(&mut columns[ci], col_def.type_code),
            }
        }
        batch.nulls.push(null_bits);
    }

    match plan {
        ConflictPlan::Error => {
            // Resolved before the push, so a bad RETURNING list writes nothing.
            // The SERIAL ids are already stamped, so the reply is a projection of
            // the batch just built — no round trip.
            let proj = insert
                .returning
                .as_deref()
                .map(|items| resolve_projection(items, schema, &table_name_str))
                .transpose()?;
            // The engine refuses `Error` on a stream (no unique PK, as above) and
            // leaves `Update` unread: the push appends, so the same row twice is
            // one element at weight 2.
            let mode = if is_stream {
                WireConflictMode::Update
            } else {
                WireConflictMode::Error
            };
            // Split on the projection: `push_owned` gives the batch away when
            // nothing will project it, saving a deep clone inside a transaction.
            match proj {
                Some(proj) => {
                    client.push_with_mode(tid, schema, &batch, mode)?;
                    let (proj_schema, proj_batch) = project(proj, schema, Some(batch));
                    Ok(SqlResult::Rows { schema: proj_schema, batch: proj_batch })
                }
                None => {
                    client.push_owned(tid, schema, batch, mode)?;
                    Ok(SqlResult::RowsAffected { count: n })
                }
            }
        }
        ConflictPlan::DoNothing => {
            // `Update`, not `Error`: the RMW driver's OCC precondition is what
            // settles a stale filter, where `Error` would raise a duplicate-key
            // error out of a statement spelled "do nothing".
            let count = commit_rmw_or_buffer(client, &table_name_str, tid, schema, |client| {
                let filtered = client_side_filter_do_nothing(client, tid, schema, &batch)?;
                let count = filtered.len();
                let write = (!filtered.pks.is_empty()).then_some(RmwWrite {
                    batch: filtered,
                    mode: WireConflictMode::Update,
                });
                Ok(RmwBuild { count, write })
            })?;
            Ok(SqlResult::RowsAffected { count })
        }
        ConflictPlan::DoUpdate { assignments } => {
            // Re-merged per RMW retry, so `SET x = x + 1` reads the freshest `x`.
            // Every row rides at +1 and the worker's `enforce_unique_pk` turns a
            // merged one into the retract-and-insert.
            let count = commit_rmw_or_buffer(client, &table_name_str, tid, schema, |client| {
                let merged = client_side_merge_do_update(client, tid, schema, &batch, &assignments, &payload)?;
                let write = (!merged.pks.is_empty()).then_some(RmwWrite {
                    batch: merged,
                    mode: WireConflictMode::Update,
                });
                Ok(RmwBuild { count: n, write })
            })?;
            Ok(SqlResult::RowsAffected { count })
        }
    }
}

/// Bind `col = expr` assignments for ON CONFLICT DO UPDATE. The
/// incoming-row scope uses the pseudo-qualifier `EXCLUDED.<col>`; bare
/// column names refer to the existing (stored) row.
fn bind_do_update_assignments(
    raw: &[Assignment],
    schema: &Schema,
    alias: &str,
) -> Result<Vec<(usize, SetProgram, Scope)>, GnitzSqlError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen: Vec<usize> = Vec::with_capacity(raw.len());
    for assignment in raw {
        let col_idx = resolve_set_target(assignment, schema, &mut seen, "ON CONFLICT DO UPDATE SET")?;
        let (program, scope) = bind_do_update_rhs(&assignment.value, col_idx, schema, alias)?;
        out.push((col_idx, program, scope));
    }
    Ok(out)
}

fn bind_do_update_rhs(
    expr: &Expr,
    target: usize,
    schema: &Schema,
    alias: &str,
) -> Result<(SetProgram, Scope), GnitzSqlError> {
    if let Some(col_name) = excluded_col(expr) {
        let col_idx = find_unique_column(&schema.columns, col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("EXCLUDED.{col_name}: column not found")))?;
        // Through the same classifier as a bare RHS, so `SET s = EXCLUDED.s`
        // on a string column is a `StrCol` rather than an integer compile, and
        // `SET int_col = EXCLUDED.str_col` is rejected here rather than per row.
        let program = classify_set_rhs(&BoundExpr::ColRef(col_idx), target, schema)?;
        return Ok((program, Scope::Excluded));
    }
    // `col + EXCLUDED.col`. The binder already rejects it (`EXCLUDED` names no
    // relation in scope); this says why, which its message cannot.
    if expr_contains_excluded(expr) {
        return Err(GnitzSqlError::Unsupported(
            "EXCLUDED column references inside compound expressions are not \
             supported; use a simple `col = EXCLUDED.col` assignment"
                .to_string(),
        ));
    }
    let program = classify_set_rhs(&bind_single_table(expr, schema, alias)?, target, schema)?;
    Ok((program, Scope::Existing))
}

/// The column an `EXCLUDED.<col>` reference names. Deliberately unpeeled: the
/// binder resolves `(EXCLUDED.a)` as an ordinary reference, so accepting the
/// parenthesized form here would bind it to the *existing* row's column.
fn excluded_col(e: &Expr) -> Option<&str> {
    match e {
        Expr::CompoundIdentifier(p) if p.len() == 2 && p[0].value.eq_ignore_ascii_case("EXCLUDED") => {
            Some(p[1].value.as_str())
        }
        _ => None,
    }
}

/// True when `expr` references `EXCLUDED.<col>` anywhere the binder would reach.
/// Recognized by [`excluded_col`], the same rule the accept path takes, so the
/// guard cannot miss a form that path would have bound.
fn expr_contains_excluded(expr: &Expr) -> bool {
    crate::ast_util::expr_any(expr, &|e| excluded_col(e).is_some())
}

/// Drop incoming rows whose PK already exists, returning the filtered ZSetBatch.
/// Intra-batch duplicate PKs keep only the first occurrence.
fn client_side_filter_do_nothing(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    batch: &ZSetBatch,
) -> Result<ZSetBatch, GnitzSqlError> {
    let (_, verdicts) = effective_rows(client, tid, schema, &batch.pks)?;
    let mut out = ZSetBatch::with_capacity(schema, verdicts.len());
    let gather = RowGather::new(schema);
    for (i, verdict) in verdicts.iter().enumerate() {
        // `Repeat` and `Existing` alike mean the PK is already claimed.
        if matches!(verdict, Conflict::Fresh) {
            gather.copy(batch, i, &mut out);
        }
    }
    Ok(out)
}

/// The batch an ON CONFLICT DO UPDATE pushes: a conflicting row merged with its
/// assignments, a fresh row passed through, a repeated PK rejected (PostgreSQL's
/// "command cannot affect row a second time").
fn client_side_merge_do_update(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    batch: &ZSetBatch,
    assignments: &[(usize, SetProgram, Scope)],
    payload: &[(usize, usize, &ColumnDef)],
) -> Result<ZSetBatch, GnitzSqlError> {
    // `rows` is both the merge's carry source and the `Existing` scope's
    // evaluation base, so `SET x = x + 1` reads the row a transaction buffered.
    let (rows, verdicts) = effective_rows(client, tid, schema, &batch.pks)?;

    let mut out = ZSetBatch::with_capacity(schema, batch.pks.len());
    let gather = RowGather::new(schema);

    // Built once each: the resolved rows are one batch, so the `existing` view
    // need not be rebuilt per row.
    let excluded_view = ZSetBatchView::new(batch, schema);
    let existing_view = ZSetBatchView::new(&rows, schema);
    // Each RHS is bound to its own scope's view here, which drives every
    // computed one over that whole batch once; the row loop below then reads a
    // buffer instead of paying a single-row drive's prologue per conflict.
    let bound: Vec<SetValues<'_>> = assignments
        .iter()
        .map(|(_, p, scope)| match scope {
            Scope::Existing => bind_set_program(p, &existing_view),
            Scope::Excluded => bind_set_program(p, &excluded_view),
        })
        .collect();
    // Pre-index assignments by column for O(cols) lookup per row.
    let mut asn_by_col: Vec<Option<(&SetValues<'_>, Scope)>> = vec![None; schema.columns.len()];
    for ((ci, _, scope), v) in assignments.iter().zip(&bound) {
        asn_by_col[*ci] = Some((v, *scope));
    }

    for (i, verdict) in verdicts.iter().enumerate() {
        match *verdict {
            Conflict::Repeat => {
                return Err(GnitzSqlError::Bind(
                    "ON CONFLICT DO UPDATE cannot affect row a second time \
                     (duplicate PK in the same batch)"
                        .to_string(),
                ));
            }
            Conflict::Fresh => gather.copy(batch, i, &mut out),
            // The stored row is `row` of the resolved batch; the incoming row is
            // row `i` of the VALUES batch.
            Conflict::Existing(row) => {
                build_merged_row(batch, i, &rows, row, payload, &mut out, |ci| {
                    asn_by_col[ci].map(|(v, scope)| {
                        eval_set_value(
                            v,
                            match scope {
                                Scope::Existing => row,
                                Scope::Excluded => i,
                            },
                        )
                    })
                })?;
            }
        }
    }
    Ok(out)
}

fn extract_values_rows(query: &Query) -> Result<&[Parens<Vec<Expr>>], GnitzSqlError> {
    match query.body.as_ref() {
        // Inert MySQL spellings of `VALUES (…)`: `VALUES ROW(…)` and `VALUE (…)`
        // parse to the same `rows`, so the row inserted is identical.
        SetExpr::Values(Values { rows, explicit_row: _, value_keyword: _ }) => Ok(rows),
        _ => Err(GnitzSqlError::Unsupported(
            "INSERT only supports VALUES (not INSERT INTO ... SELECT)".to_string(),
        )),
    }
}

#[cfg(test)]
#[path = "tests/insert.rs"]
mod tests;
