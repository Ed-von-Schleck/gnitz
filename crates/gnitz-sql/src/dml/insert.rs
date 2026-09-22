//! INSERT, including the `ON CONFLICT` upsert family. The default form pushes
//! with `WireConflictMode::Error`; `DO NOTHING` / `DO UPDATE` are resolved
//! client-side (seek existing PKs, then filter or merge) before a single push.
//! `DO UPDATE SET` binds, compiles and applies its list through `mutate`'s SET
//! list (`bind_set_list`, `classify_set_rhs`, `apply_set`), so it behaves exactly
//! like an `UPDATE ... SET`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use crate::ast_util::{extract_object_name, single_part_ident};
use crate::bind::structural::bind_constant;
use crate::bind::{bind_single_table, find_unique_column};
use crate::codec::colwrite::{append_value_to_col, check_not_null, native_value};
use crate::dml::mutate::{apply_set, bind_set_list, classify_set_rhs, Scope, SetCol, SetRhs};
use crate::dml::overlay::{Conflict, KeyProbe};
use crate::dml::plan::{rows_reply, RowsReply};
use crate::dml::rmw::commit_rmw_or_buffer;
use crate::error::GnitzSqlError;
use crate::exec::client_map::ClientMap;
use crate::ir::{BExpr, BoundExpr};
use crate::validate::{reject_unhonored_insert_clauses, require_class, ClassWant};
use crate::SqlResult;
use gnitz_core::{FixedInt, GnitzClient, PkColumn, RelClass, Schema, TypeCode, WireConflictMode, ZSetBatch};
use sqlparser::ast::{
    ConflictTarget, Expr, Insert, ObjectName, OnConflict, OnConflictAction, OnInsert, Parens, Query, SetExpr,
    TableObject, Values,
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
    DoUpdate { set: Vec<SetCol> },
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
        let ident = single_part_ident(name)
            .ok_or_else(|| GnitzSqlError::Plan("INSERT column list: column must be a simple identifier".into()))?;
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
    // from a scan. A SERIAL column is never hidden: it is the table's lone PK,
    // which the engine refuses to hide.
    let serial_ci = schema.columns.iter().position(|c| c.is_serial);
    Ok(RowShape {
        slot_of,
        expected: columns.len(),
        serial_ci,
    })
}

pub(crate) fn execute_insert(
    client: &mut GnitzClient,
    schema_name: &str,
    insert: &Insert,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_insert_clauses(insert)?;
    let table_name_str = match &insert.table {
        TableObject::TableName(obj_name) => extract_object_name(obj_name, schema_name, "INSERT")?,
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

    let target = client.resolve_relation(schema_name, &table_name_str)?;
    require_class(&target, &table_name_str, ClassWant::BaseTableOrStream, "INSERT")?;
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
            // Both actions resolve the incoming row against stored rows, which a
            // stream does not have.
            require_class(&target, &table_name_str, ClassWant::BaseTable, "INSERT … ON CONFLICT")?;
            validate_conflict_target(conflict_target, schema)?;

            match action {
                OnConflictAction::DoNothing => ConflictPlan::DoNothing,
                OnConflictAction::DoUpdate(do_update) => {
                    if do_update.selection.is_some() {
                        return Err(GnitzSqlError::Unsupported(
                            "ON CONFLICT ... DO UPDATE WHERE not supported".to_string(),
                        ));
                    }
                    let set = bind_set_list(&do_update.assignments, schema, "ON CONFLICT DO UPDATE SET", |e, ci| {
                        bind_do_update_rhs(e, ci, schema, &table_name_str)
                    })?;
                    ConflictPlan::DoUpdate { set }
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
    let payload: Vec<_> = schema.payload_columns().collect();
    // One bound cell per VALUES slot, reused across rows and read by both
    // consumers below, so a row's PK slot and payload slot cannot disagree on
    // what a written constant is.
    let mut cells: Vec<BExpr<Infallible>> = Vec::new();
    // The row count is known here, so a SERIAL statement's ids come from one
    // durable advance and each row stamps `base + i`. A row failing the arity
    // guard below abandons the rest — a wider gap of the same intentional kind.
    let pk_plan = match serial_ci {
        Some(ci) => PkPlan::serial(client.reserve_serial_ids(tid, n as u64)?, n, schema.columns[ci].ty.tc)?,
        None => PkPlan::written(&slot_of, schema)?,
    };

    for (row_i, row) in rows.iter().enumerate() {
        // Standard SQL rejects a VALUES row whose arity differs from the expected
        // count, in either direction — too few values, or excess trailing ones.
        // This guard makes every per-column index below in-bounds.
        if row.len() != expected {
            let hint = if serial_ci.is_some() {
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
        pk_plan.push(schema, row_i, &cells, &mut batch.pks)?;
        batch.weights.push(1);

        let mut null_bits: u64 = 0;
        for &(payload_idx, ci, col_def) in &payload {
            if col_def.is_hidden {
                // Logical-dropped column: a zero-filled NOT-NULL filler cell (null
                // bit left unset), keeping the batch rectangular and the table on
                // the FixedIntNonnull comparator. The value is unobservable (§6).
                batch.payload[payload_idx].push_zero();
                continue;
            }
            // Read off the *bound* constant, so `+NULL` is the NULL it spells;
            // a column the list left out is NULL too.
            let cell = slot_of[ci].map(|s| &cells[s]);
            let is_null = cell.is_none_or(|c| matches!(c, BExpr::LitNull));
            // The check is here for the *conflicting* row of an ON CONFLICT DO
            // UPDATE: its incoming NULL is consumed into the merged row and is
            // never pushed, so the wire boundary's own check never sees it.
            check_not_null(col_def, is_null)?;
            if is_null {
                gnitz_wire::null_word_set(&mut null_bits, payload_idx, true);
            }
            let ZSetBatch { payload: cols, blob, .. } = &mut batch;
            match cell {
                Some(c) => append_value_to_col(&mut cols[payload_idx].bytes, blob, col_def, c)?,
                // `append_value_to_col` encodes a written NULL as exactly this.
                None => cols[payload_idx].push_zero(),
            }
        }
        batch.nulls.push(null_bits);
    }

    match plan {
        ConflictPlan::Error => {
            // Before the push, so a RETURNING list that fails to bind writes nothing.
            let returning = insert
                .returning
                .as_deref()
                .map(|items| {
                    let RowsReply { schema: out_schema, program, .. } =
                        rows_reply(items, None, schema, &table_name_str)?;
                    let map = program
                        .map(|p| ClientMap::new(p, schema, Arc::clone(&out_schema)))
                        .transpose()?;
                    Ok::<_, GnitzSqlError>((out_schema, map))
                })
                .transpose()?;
            // The engine refuses `Error` on a stream (its PK is not unique) and
            // leaves `Update` unread: the push appends, so the same row twice is
            // one element at weight 2.
            let mode = if is_stream {
                WireConflictMode::Update
            } else {
                WireConflictMode::Error
            };
            // Split on RETURNING: `push_owned` gives the batch away when nothing
            // will project it, saving a deep clone inside a transaction.
            match returning {
                Some((schema_out, map)) => {
                    client.push_with_mode(tid, schema, &batch, mode)?;
                    Ok(SqlResult::Rows {
                        schema: schema_out,
                        batch: match map {
                            Some(m) => m.apply(batch),
                            None => batch,
                        },
                    })
                }
                None => {
                    client.push_owned(tid, schema, batch, mode)?;
                    Ok(SqlResult::RowsAffected { count: n })
                }
            }
        }
        ConflictPlan::DoNothing => {
            let probe = KeyProbe::keys(schema, &batch.pks)?;
            // The RMW driver pushes `Update`, not `Error`: its OCC precondition is
            // what settles a stale filter, where `Error` would raise a duplicate-key
            // error out of a statement spelled "do nothing".
            let count = commit_rmw_or_buffer(client, &table_name_str, tid, schema, |client| {
                client_side_filter_do_nothing(client, tid, schema, &probe, &batch)
            })?;
            Ok(SqlResult::RowsAffected { count })
        }
        ConflictPlan::DoUpdate { set } => {
            // Re-merged per RMW retry, so `SET x = x + 1` reads the freshest `x`.
            // Every row rides at +1 and the worker's `enforce_unique_pk` turns a
            // merged one into the retract-and-insert.
            let probe = KeyProbe::rows(schema, &batch.pks);
            let count = commit_rmw_or_buffer(client, &table_name_str, tid, schema, |client| {
                client_side_merge_do_update(client, tid, schema, &probe, &batch, &set)
            })?;
            Ok(SqlResult::RowsAffected { count })
        }
    }
}

/// One `DO UPDATE SET` right-hand side. The incoming-row scope uses the
/// pseudo-qualifier `EXCLUDED.<col>`; bare column names refer to the existing
/// (stored) row.
fn bind_do_update_rhs(expr: &Expr, target: usize, schema: &Schema, alias: &str) -> Result<SetRhs, GnitzSqlError> {
    if let Some(col_name) = excluded_col(expr) {
        let col_idx = find_unique_column(&schema.columns, col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("EXCLUDED.{col_name}: column not found")))?;
        // Through the same classifier as a bare RHS, so `SET int_col =
        // EXCLUDED.str_col` is rejected here rather than per row.
        return classify_set_rhs(&BoundExpr::ColRef(col_idx), Scope::Excluded, target, schema);
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
    classify_set_rhs(
        &bind_single_table(expr, schema, alias)?,
        Scope::Existing,
        target,
        schema,
    )
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
/// Intra-batch duplicate PKs keep only the first occurrence. `probe` reads `batch`'s keys.
fn client_side_filter_do_nothing(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    probe: &KeyProbe,
    batch: &ZSetBatch,
) -> Result<ZSetBatch, GnitzSqlError> {
    let (_, _, verdicts) = probe.resolve(client, tid, schema)?;
    let mut out = ZSetBatch::with_capacity(schema, verdicts.len());
    for (i, verdict) in verdicts.iter().enumerate() {
        // `Repeat` and `Existing` alike mean the PK is already claimed.
        if matches!(verdict, Conflict::Fresh) {
            out.copy_row_at(batch, i, batch.weights[i]);
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
    probe: &KeyProbe,
    batch: &ZSetBatch,
    set: &[SetCol],
) -> Result<ZSetBatch, GnitzSqlError> {
    // `rows` is the `Existing` scope, so `SET x = x + 1` reads the row a
    // transaction buffered.
    let (mut rows, buffered, verdicts) = probe.resolve(client, tid, schema)?;
    rows.extend_from_owned(buffered);
    // The VALUES row each existing key collided with.
    let mut src_of: HashMap<&[u8], usize> = HashMap::with_capacity(rows.len());
    let mut out = ZSetBatch::with_capacity(schema, verdicts.len());
    for (i, verdict) in verdicts.iter().enumerate() {
        match *verdict {
            Conflict::Repeat => {
                return Err(GnitzSqlError::Bind(
                    "ON CONFLICT DO UPDATE cannot affect row a second time \
                     (duplicate PK in the same batch)"
                        .to_string(),
                ));
            }
            Conflict::Fresh => out.copy_row_at(batch, i, batch.weights[i]),
            Conflict::Existing => {
                src_of.insert(batch.pks.get_bytes(i), i);
            }
        }
    }
    let mut excluded = ZSetBatch::with_capacity(schema, rows.len());
    for r in 0..rows.len() {
        excluded.copy_row_at(batch, src_of[rows.pks.get_bytes(r)], 1);
    }
    out.extend_from_owned(apply_set(set, rows, Some(&excluded), schema)?);
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

/// Where each INSERT row's PK comes from, resolved once per statement.
pub(crate) enum PkPlan {
    /// SERIAL: row `i` takes `base + i`; exhaustion is checked once, at construction.
    Serial { base: u64 },
    /// Written: per PK column in pk-list order, the VALUES slot it reads.
    Written { slots: Vec<usize> },
}

impl PkPlan {
    /// `n` rows drawn from `base`, refused when the last exceeds the type `tc`.
    pub(crate) fn serial(base: u64, n: usize, tc: TypeCode) -> Result<Self, GnitzSqlError> {
        let max = FixedInt::from_type_code(tc)
            .expect("SERIAL underlying is a fixed int")
            .range()
            .1;
        if i128::from(base) + n as i128 - 1 > max {
            let next = i128::from(base).max(max + 1);
            return Err(GnitzSqlError::Bind(format!(
                "SERIAL primary key exhausted: next value {next} exceeds the column type maximum {max}"
            )));
        }
        Ok(PkPlan::Serial { base })
    }

    /// `slot_of` is the INSERT's physical-column → VALUES-slot map, `None` where
    /// a column takes no user value.
    pub(crate) fn written(slot_of: &[Option<usize>], schema: &Schema) -> Result<Self, GnitzSqlError> {
        let slots = schema
            .pk_cols
            .iter()
            .map(|&pi| {
                slot_of.get(pi as usize).copied().flatten().ok_or_else(|| {
                    GnitzSqlError::Bind(format!(
                        "PK column '{}' missing from INSERT row",
                        schema.columns[pi as usize].name
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(PkPlan::Written { slots })
    }

    /// Append row `row_i`'s primary key to `dst`.
    pub(crate) fn push(
        &self,
        schema: &Schema,
        row_i: usize,
        cells: &[BExpr<Infallible>],
        dst: &mut PkColumn,
    ) -> Result<(), GnitzSqlError> {
        match self {
            PkPlan::Serial { base } => dst.push_natives(schema, &[u128::from(base + row_i as u64)]),
            PkPlan::Written { slots } => {
                let mut natives = [0u128; gnitz_wire::MAX_PK_COLUMNS];
                for (k, (&pi, &slot)) in schema.pk_cols.iter().zip(slots).enumerate() {
                    let def = &schema.columns[pi as usize];
                    check_not_null(def, matches!(cells[slot], BExpr::LitNull))?;
                    natives[k] = native_value(&cells[slot], def)?;
                }
                dst.push_natives(schema, &natives[..slots.len()]);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/insert.rs"]
mod tests;
