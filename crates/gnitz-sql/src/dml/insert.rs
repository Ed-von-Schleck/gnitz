//! INSERT, including the `ON CONFLICT` upsert family. The default form pushes
//! with `WireConflictMode::Error`; `DO NOTHING` / `DO UPDATE` are resolved
//! client-side (seek existing PKs, then filter or merge) before a single push.
//! The SET-list binding and evaluation reuse `mutate`'s shared helpers so a
//! `DO UPDATE SET` assignment behaves exactly like an `UPDATE ... SET`.

use std::sync::Arc;

use crate::ast_util::{extract_name, object_name_ident};
use crate::bind::{bind_single_table, find_unique_column, Binder};
use crate::codec::colwrite::{append_value_to_col, check_not_null};
use crate::codec::pk_codec::{extract_pk_value_mapped, is_null_expr};
use crate::dml::mutate::{
    bind_set_program, build_merged_row, classify_set_rhs, eval_set_value, merge_payload_plan, resolve_set_target,
    SetProgram, SetValues,
};
use crate::dml::overlay::effective_rows;
use crate::dml::rmw::{commit_rmw_or_buffer, RmwBuild, RmwWrite};
use crate::error::GnitzSqlError;
use crate::exec::batch::{project, resolve_projection, RowGather};
use crate::ir::BoundExpr;
use crate::validate::reject_unhonored_insert_clauses;
use crate::SqlResult;
use gnitz_core::{
    push_zero_cell, FixedInt, GnitzClient, PkBuf, RelClass, Schema, WireConflictMode, ZSetBatch, ZSetBatchView,
};
use sqlparser::ast::{
    Assignment, ConflictTarget, Expr, Insert, ObjectName, OnConflict, OnConflictAction, OnInsert, Parens, Query,
    SelectItem, SetExpr, TableObject, Values,
};

/// The resolved INSERT disposition after the ON CONFLICT clause (if any) is bound.
/// The conflict target itself is validated and discarded in `validate_conflict_target`;
/// only the action survives into the plan.
enum ConflictPlan {
    /// Default SQL INSERT: push with WireConflictMode::Error.
    Error,
    /// `ON CONFLICT (pk) DO NOTHING` (or `ON CONFLICT DO NOTHING` with
    /// no target): pre-filter conflicting PKs client-side via `seek`,
    /// then push the survivors with WireConflictMode::Error.
    DoNothingPk,
    /// `ON CONFLICT (pk) DO UPDATE SET ...`: seek existing rows, merge
    /// with assignments, push merged batch with WireConflictMode::Update.
    DoUpdatePk { assignments: Vec<(usize, BoundUpdateExpr)> },
}

/// Assignment RHS for `ON CONFLICT DO UPDATE`. Each variant wraps a compiled
/// [`SetProgram`] and a scope — Existing evaluates against the stored row,
/// Excluded against the incoming batch row. `EXCLUDED.col` is the only construct
/// that escapes the existing-row scope.
///
/// Both compile at bind time, against the catalog schema, which is the schema
/// `insert.rs` reads every row through: the VALUES batch is built from it, and
/// `effective_rows`'s buffered branch copies into a batch built from it. Its
/// committed branch returns the seek reply verbatim and drops the reply schema,
/// so a row read back from the store is *assumed* to match — the same assumption
/// `build_merged_row`'s carry path has always made here, not one this compilation
/// introduces. (`mutate.rs` assumes it too: its read is a `ReadSpec` whose reply
/// the client itself authored, so there is no server-echoed schema to prefer.)
enum BoundUpdateExpr {
    Existing(SetProgram),
    Excluded(SetProgram),
}

/// Validate the ON CONFLICT target against the supported subset:
/// either no target (`ON CONFLICT DO ...`) or a single-column target
/// naming the PK. Composite targets and `ON CONSTRAINT` are rejected.
fn validate_conflict_target(target: &Option<ConflictTarget>, schema: &Schema) -> Result<(), GnitzSqlError> {
    match target {
        None => Ok(()),
        Some(ConflictTarget::Columns(cols)) => {
            // Partial-tuple targets like `ON CONFLICT (a) DO NOTHING`
            // against `PRIMARY KEY (a, b)` are out of scope for the
            // compound-PK planner gate.
            let Some(pk_ci) = schema.pk_index_single() else {
                return Err(GnitzSqlError::Unsupported(
                    "ON CONFLICT with target columns is not supported on \
                     compound-PK tables"
                        .to_string(),
                ));
            };
            if cols.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "composite ON CONFLICT targets not supported; \
                     single-column PK target only"
                        .to_string(),
                ));
            }
            let col_name = cols[0].value.as_str();
            let pk_name = schema.columns[pk_ci as usize].name.as_str();
            if !col_name.eq_ignore_ascii_case(pk_name) {
                return Err(GnitzSqlError::Unsupported(format!(
                    "ON CONFLICT ({col_name}) — only the primary key column '{pk_name}' is \
                     supported as a conflict target"
                )));
            }
            Ok(())
        }
        Some(ConflictTarget::OnConstraint(_)) => Err(GnitzSqlError::Unsupported(
            "ON CONFLICT ON CONSTRAINT not supported".to_string(),
        )),
    }
}

pub(crate) fn execute_insert(
    client: &mut GnitzClient,
    insert: &Insert,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_insert_clauses(insert)?;
    // Extract table name, row source, ON CONFLICT action, and RETURNING clause.
    let (table_name_str, rows, columns, on_insert, returning) = extract_insert_parts(insert)?;

    let target = binder.resolve_push_target(client, &table_name_str)?;
    let (tid, schema) = (target.tid, &target.schema);
    let is_stream = target.class == RelClass::Stream;

    // INSERT is positional; reject any column list that isn't every non-SERIAL
    // column in schema order (it would otherwise silently misplace values).
    validate_insert_column_list(columns, schema)?;

    // RETURNING is supported on the plain-INSERT path only; capturing the
    // effective row under ON CONFLICT (which may UPDATE or skip a row) is out of
    // scope.
    if returning.is_some() && on_insert.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "RETURNING with ON CONFLICT is not supported".to_string(),
        ));
    }

    // Resolve the ON CONFLICT clause into a `ConflictPlan`.
    let plan = match on_insert {
        None => ConflictPlan::Error,
        Some(OnInsert::DuplicateKeyUpdate(_)) => {
            return Err(GnitzSqlError::Unsupported(
                "ON DUPLICATE KEY UPDATE not supported — use PostgreSQL-style \
                 ON CONFLICT (col) DO UPDATE"
                    .to_string(),
            ));
        }
        Some(OnInsert::OnConflict(OnConflict { conflict_target, action })) => {
            // Both actions resolve the incoming row against existing rows with a
            // client-side seek before pushing, and a stream has no store to seek.
            if is_stream {
                return Err(GnitzSqlError::Unsupported(format!(
                    "'{table_name_str}' is a stream; ON CONFLICT needs stored rows to resolve against"
                )));
            }
            validate_conflict_target(conflict_target, schema)?;

            match action {
                OnConflictAction::DoNothing => ConflictPlan::DoNothingPk,
                OnConflictAction::DoUpdate(do_update) => {
                    if do_update.selection.is_some() {
                        return Err(GnitzSqlError::Unsupported(
                            "ON CONFLICT ... DO UPDATE WHERE not supported".to_string(),
                        ));
                    }
                    let assignments = bind_do_update_assignments(&do_update.assignments, schema, &table_name_str)?;
                    ConflictPlan::DoUpdatePk { assignments }
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

    // A SERIAL PK is the table's lone single-column PK (enforced at CREATE), so
    // the user omits it: arity excludes it, the PK is drawn from the sequence and
    // stamped, and each VALUES element indexes the payload columns directly. The
    // dense `payload_idx` equals the position in the SERIAL-omitted row precisely
    // because the omitted column is the single PK (the single-PK closed form).
    // `serial_max` is the underlying int type's max positive value
    // (`FixedInt::range`); its presence *is* the SERIAL test, so it both selects the
    // per-row PK source and bounds the sequence. `is_serial` names that presence.
    let serial_max: Option<i128> = schema.columns.iter().find(|c| c.is_serial).map(|c| {
        FixedInt::from_type_code(c.type_code)
            .expect("SERIAL underlying is a fixed int")
            .range()
            .1
    });
    let is_serial = serial_max.is_some();
    // The user supplies one VALUES entry per visible, non-SERIAL column, in schema
    // order; a dropped (hidden) column and the SERIAL PK take no user value. The
    // row-invariant map physical ci → VALUES slot (`None` at SERIAL / hidden
    // slots) drives PK extraction and the payload loop, so a mid-schema hidden
    // column never consumes a user value (positional remap, §6).
    let mut slot_of: Vec<Option<usize>> = vec![None; schema.columns.len()];
    let mut expected = 0usize;
    for (ci, c) in schema.visible_columns() {
        if !c.is_serial {
            slot_of[ci] = Some(expected);
            expected += 1;
        }
    }
    // Built once: `payload_columns` filters on `is_pk_col`, itself a PK-list scan,
    // so leaving it in the row loop pays that scan per row per column.
    let payload = merge_payload_plan(schema);
    // The row count is known here, so the whole statement's ids come from one
    // durable advance and each row stamps `base + i`. A row failing the arity
    // guard below abandons the rest — a wider gap of the same intentional kind.
    let serial = serial_max
        .map(|max| Ok::<_, GnitzSqlError>((client.reserve_serial_ids(tid, n as u64)?, max)))
        .transpose()?;

    for (row_i, row) in rows.iter().enumerate() {
        // Standard SQL rejects a VALUES row whose arity differs from the expected
        // count, in either direction — too few values, or excess trailing ones.
        // This guard makes every per-column index below in-bounds.
        if row.len() != expected {
            let hint = if is_serial {
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
        if let Some((base, max)) = serial {
            // Reject an exhausted sequence (a value past the column type's max),
            // mirroring PostgreSQL. Client-side, so it is a `Bind` error like the
            // arity guard above — `Exec` is for surfaced server `ClientError`s.
            let id = base + row_i as u64;
            if id as i128 > max {
                return Err(GnitzSqlError::Bind(format!(
                    "SERIAL primary key exhausted: next value {id} exceeds the column type maximum {max}"
                )));
            }
            batch.pks.push_u128(schema, id as u128);
        } else {
            batch.pks.push_tuple(&extract_pk_value_mapped(row, &slot_of, schema)?);
        }
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
            let val_expr = &row[slot_of[ci].expect("a visible payload column has a user value")];
            let is_null = is_null_expr(val_expr);
            // `ConflictPlan::DoUpdatePk` pushes only the *merged* batch, so an
            // incoming NULL that survives this point never faces the wire
            // boundary's own check.
            check_not_null(col_def, is_null)?;
            if is_null {
                gnitz_wire::null_word_set(&mut null_bits, payload_idx, true);
            }
            let ZSetBatch { columns, blob, .. } = &mut batch;
            append_value_to_col(&mut columns[ci], blob, col_def.type_code, val_expr)?;
        }
        batch.nulls.push(null_bits);
    }

    match plan {
        ConflictPlan::Error => {
            // SQL-standard INSERT: the server rejects on any PK conflict (at COMMIT
            // in a transaction — the engine's per-family duplicate check).
            //
            // RETURNING (plain-INSERT path only): the assigned SERIAL ids are
            // already stamped into the batch's PK region, so the reply is a
            // projection of the batch we just built — no round-trip. `resolve` runs
            // BEFORE the write, so a bad RETURNING list writes nothing rather than
            // committing the row and then failing; `project` afterwards is
            // infallible and consumes the batch, so nothing is copied.
            let proj = returning
                .map(|items| resolve_projection(items, schema, &table_name_str))
                .transpose()?;
            // A stream has no PK conflict to reject: the engine refuses `Error` on
            // one and leaves `Update` unread, so the push appends. INSERTing the
            // same row twice therefore yields one element at weight 2.
            let mode = if is_stream {
                WireConflictMode::Update
            } else {
                WireConflictMode::Error
            };
            // The batch moves into the push, so a copy is kept only when
            // RETURNING will project it — inside a transaction the push would
            // otherwise deep-clone every buffered row for a projection the common
            // case does not ask for.
            let returned = proj.is_some().then(|| batch.clone());
            client.push_owned(tid, schema, batch, mode)?;
            match proj {
                Some(proj) => {
                    let (proj_schema, proj_batch) = project(proj, schema, returned);
                    Ok(SqlResult::Rows { schema: proj_schema, batch: proj_batch })
                }
                None => Ok(SqlResult::RowsAffected { count: n }),
            }
        }
        ConflictPlan::DoNothingPk => {
            // Client-side filter: drop any row whose PK already exists — buffered
            // or committed (see `effective_rows`). Resolving against the buffer is
            // what stops two DO NOTHING inserts of one new PK from buffering two
            // `+1` Error rows and tripping the commit-time per-family duplicate
            // check. De-duplicate intra-batch (first-wins) before pushing. The
            // filter re-runs per RMW retry against fresh committed state; the
            // incoming VALUES batch (already built, ids drawn) is reused as-is.
            let count = commit_rmw_or_buffer(client, &table_name_str, tid, schema, |client| {
                let filtered = client_side_filter_do_nothing(client, tid, schema, &batch)?;
                let count = filtered.len();
                let write = (count > 0).then_some(RmwWrite {
                    batch: filtered,
                    mode: WireConflictMode::Error,
                });
                Ok(RmwBuild { count, write })
            })?;
            Ok(SqlResult::RowsAffected { count })
        }
        ConflictPlan::DoUpdatePk { assignments } => {
            // Merge each incoming row against the effective existing row (buffered
            // or committed), re-running per RMW retry so `SET x = x + 1` reads the
            // freshest `x`. Update mode: the merged batch carries both +1 merged
            // rows (which may UPSERT) and untouched +1 rows for non-conflicting
            // inserts; workers do the retract-and-insert via enforce_unique_pk.
            let count = commit_rmw_or_buffer(client, &table_name_str, tid, schema, |client| {
                let merged = client_side_merge_do_update(client, tid, schema, &batch, &assignments)?;
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
) -> Result<Vec<(usize, BoundUpdateExpr)>, GnitzSqlError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen: Vec<usize> = Vec::with_capacity(raw.len());
    for assignment in raw {
        let col_idx = resolve_set_target(assignment, schema, &mut seen, "ON CONFLICT DO UPDATE SET")?;
        let value = bind_do_update_rhs(&assignment.value, col_idx, schema, alias)?;
        out.push((col_idx, value));
    }
    Ok(out)
}

fn bind_do_update_rhs(
    expr: &Expr,
    target: usize,
    schema: &Schema,
    alias: &str,
) -> Result<BoundUpdateExpr, GnitzSqlError> {
    if let Some(col_name) = excluded_col(expr) {
        let col_idx = find_unique_column(&schema.columns, col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("EXCLUDED.{col_name}: column not found")))?;
        // Through the same classifier as a bare RHS, so `SET s = EXCLUDED.s`
        // on a string column is a `StrCol` rather than an integer compile.
        return Ok(BoundUpdateExpr::Excluded(classify_set_rhs(
            &BoundExpr::ColRef(col_idx),
            target,
            schema,
        )?));
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
    Ok(BoundUpdateExpr::Existing(classify_set_rhs(
        &bind_single_table(expr, schema, alias)?,
        target,
        schema,
    )?))
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

/// Drop incoming rows whose PK already exists. Returns the filtered ZSetBatch
/// plus the surviving-row count.
///
/// Intra-batch duplicate PKs keep only the first occurrence.
fn client_side_filter_do_nothing(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    batch: &ZSetBatch,
) -> Result<ZSetBatch, GnitzSqlError> {
    let keys: Vec<PkBuf> = (0..batch.pks.len()).map(|i| batch.pks.get_tuple(i)).collect();
    // A PK the transaction buffered as live conflicts; one it buffered as deleted
    // does not; an untouched PK falls through to the committed store.
    let (_, existing) = effective_rows(client, tid, schema, &keys)?;

    let mut seen_pks: std::collections::HashSet<PkBuf> = std::collections::HashSet::with_capacity(keys.len());
    let mut surviving_indices: Vec<usize> = Vec::with_capacity(batch.pks.len());
    for (i, pk) in keys.iter().enumerate() {
        // Intra-batch duplicate: drop everything after the first.
        if !seen_pks.insert(*pk) {
            continue;
        }
        if existing.contains_key(pk) {
            continue;
        }
        surviving_indices.push(i);
    }

    let mut out = ZSetBatch::with_capacity(schema, surviving_indices.len());
    let gather = RowGather::new(schema);
    for &i in &surviving_indices {
        gather.copy(batch, i, &mut out);
    }
    Ok(out)
}

/// Build a merged batch for ON CONFLICT DO UPDATE:
///   - For each incoming row whose PK exists in the store: evaluate
///     assignments against (existing_row, excluded_row) and emit the
///     merged row as +1 (worker's enforce_unique_pk will
///     retract the old payload).
///   - For each incoming row whose PK does NOT exist: pass through as a
///     plain +1 insert.
///
/// Intra-batch duplicate PKs are rejected — this matches PG's
/// "command cannot affect row a second time" behavior.
fn client_side_merge_do_update(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    batch: &ZSetBatch,
    assignments: &[(usize, BoundUpdateExpr)],
) -> Result<ZSetBatch, GnitzSqlError> {
    let keys: Vec<PkBuf> = (0..batch.pks.len()).map(|i| batch.pks.get_tuple(i)).collect();
    // The effective existing rows — a row the transaction buffered is both the
    // merge's carry source AND the `Existing` scope's evaluation base, so
    // `SET x = x + 1` reads the buffered `x`; a buffered delete is no conflict,
    // and an untouched PK falls through to the committed store.
    let (rows, existing) = effective_rows(client, tid, schema, &keys)?;

    let mut seen_pks: std::collections::HashSet<PkBuf> = std::collections::HashSet::with_capacity(keys.len());
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
        .map(|(_, rhs)| match rhs {
            BoundUpdateExpr::Existing(p) => bind_set_program(p, &existing_view),
            BoundUpdateExpr::Excluded(p) => bind_set_program(p, &excluded_view),
        })
        .collect();
    // Pre-index assignments by column for O(cols) lookup per row.
    let mut asn_by_col: Vec<Option<(&BoundUpdateExpr, &SetValues<'_>)>> = vec![None; schema.columns.len()];
    for ((ci, rhs), v) in assignments.iter().zip(&bound) {
        asn_by_col[*ci] = Some((rhs, v));
    }
    let payload = merge_payload_plan(schema);

    for (i, pk) in keys.iter().enumerate() {
        if !seen_pks.insert(*pk) {
            return Err(GnitzSqlError::Bind(
                "ON CONFLICT DO UPDATE cannot affect row a second time \
                 (duplicate PK in the same batch)"
                    .to_string(),
            ));
        }

        match existing.get(pk) {
            None => {
                gather.copy(batch, i, &mut out);
            }
            Some(&row) => {
                // The stored row is `row` of the resolved batch; the incoming row
                // is row `i` of the VALUES batch.
                build_merged_row(batch, i, &rows, row, &payload, &mut out, |ci| {
                    asn_by_col[ci].map(|(rhs, v)| match rhs {
                        BoundUpdateExpr::Existing(_) => eval_set_value(v, &existing_view, row),
                        BoundUpdateExpr::Excluded(_) => eval_set_value(v, &excluded_view, i),
                    })
                })?;
            }
        }
    }
    Ok(out)
}

/// INSERT writes VALUES positionally into `schema.payload_columns()` in schema
/// order, so an explicit column list is correct only when it names every column
/// once, in schema order. A reordered or partial list would silently misplace
/// values — reject it. (Full column-list remapping is a separate feature.)
fn validate_insert_column_list(columns: &[ObjectName], schema: &Schema) -> Result<(), GnitzSqlError> {
    if columns.is_empty() {
        return Ok(());
    }
    // Each explicit INSERT column is a simple identifier (since sqlparser 0.60 the
    // list is `Vec<ObjectName>`); extract the bare name.
    let col_names: Vec<&str> = columns
        .iter()
        .map(|c| {
            object_name_ident(c)
                .map(|i| i.value.as_str())
                .ok_or_else(|| GnitzSqlError::Plan("INSERT column list: column must be a simple identifier".into()))
        })
        .collect::<Result<_, _>>()?;
    // Expected list = every visible (non-hidden), non-SERIAL column, in schema
    // order. For a table with neither a SERIAL nor a dropped column this reduces
    // to the full column list in schema order — not a regression. A hidden
    // (dropped) column is unnameable, so it is excluded from the expected set.
    let expected: Vec<&str> = schema
        .visible_columns()
        .filter(|(_, c)| !c.is_serial)
        .map(|(_, c)| c.name.as_str())
        .collect();
    let matches =
        col_names.len() == expected.len() && col_names.iter().zip(&expected).all(|(c, n)| c.eq_ignore_ascii_case(n));
    if !matches {
        // Naming the SERIAL column at all is a targeted error, since it can never
        // appear in a valid list.
        if col_names.iter().any(|c| {
            schema
                .columns
                .iter()
                .any(|sc| sc.is_serial && sc.name.eq_ignore_ascii_case(c))
        }) {
            return Err(GnitzSqlError::Unsupported(
                "cannot supply a value for a SERIAL column; omit it from the INSERT".to_string(),
            ));
        }
        return Err(GnitzSqlError::Unsupported(
            "INSERT with an explicit column list is only supported when it names all \
             non-SERIAL columns in schema order; reordered or partial column lists are not supported"
                .to_string(),
        ));
    }
    Ok(())
}

/// `(table_name, rows, columns, on_clause, returning)` — return type of
/// [`extract_insert_parts`]. Each row is `Parens<Vec<Expr>>` (the sqlparser 0.60+
/// VALUES row shape), which derefs to its expression list. `columns` is the
/// explicit INSERT column list (empty when omitted); `returning` is the
/// projection list of a RETURNING clause.
type InsertParts<'a> = (
    String,
    &'a [Parens<Vec<Expr>>],
    &'a [ObjectName],
    Option<&'a OnInsert>,
    Option<&'a [SelectItem]>,
);

fn extract_insert_parts(insert: &Insert) -> Result<InsertParts<'_>, GnitzSqlError> {
    let table_name = match &insert.table {
        TableObject::TableName(obj_name) => extract_name(obj_name, "INSERT")?,
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
    Ok((
        table_name,
        rows,
        insert.columns.as_slice(),
        insert.on.as_ref(),
        insert.returning.as_deref(),
    ))
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
