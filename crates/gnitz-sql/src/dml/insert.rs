//! INSERT, including the `ON CONFLICT` upsert family. The default form pushes
//! with `WireConflictMode::Error`; `DO NOTHING` / `DO UPDATE` are resolved
//! client-side (seek existing PKs, then filter or merge) before a single push.
//! `DO UPDATE SET` binds, compiles and applies its list through `mutate`'s SET
//! list (`bind_set_list`, `classify_set_rhs`, `apply_set`), so it behaves exactly
//! like an `UPDATE ... SET`.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use crate::ast_util::{extract_object_name, single_part_ident};
use crate::bind::find_unique_column;
use crate::bind::structural::bind_constant;
use crate::codec::colwrite::{append_value_to_col, check_not_null, native_value};
use crate::dml::mutate::{apply_set, bind_set_list, SetClause, SetCol};
use crate::dml::plan::{rows_reply, RowsReply};
use crate::dml::rmw::{commit_rmw, TargetRead};
use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::exec::client_map::ClientMap;
use crate::ir::BExpr;
use crate::validate::{reject_unhonored_insert_clauses, require_class, ClassWant};
use crate::SqlResult;
use gnitz_core::{FixedInt, GnitzClient, PkColumn, RelClass, Schema, TypeCode, WireConflictMode, ZSetBatch};
use gnitz_expr::SchemaFacts;
use gnitz_wire::{PkKeys, ReadBound};
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
    /// `ON CONFLICT`: resolve each incoming row against the rows its key holds —
    /// drop a conflicting row (`DO NOTHING`, no `set`) or merge it with the SET
    /// list (`DO UPDATE`) — and push the rest.
    Resolve { set: Option<Vec<SetCol>> },
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
                    .ok_or_else(|| GnitzSqlError::Rejected(format!("ON CONFLICT ({}): column not found", c.value)))?;
                named.push(ci as u32);
            }
            let mut pk = schema.pk_cols.clone();
            named.sort_unstable();
            pk.sort_unstable();
            if named != pk {
                return Err(GnitzSqlError::Rejected(format!(
                    "ON CONFLICT target must name exactly the primary key ({})",
                    pk.iter()
                        .map(|&ci| schema.columns[ci as usize].name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                )));
            }
            Ok(())
        }
        Some(ConflictTarget::OnConstraint(_)) => Err(unsupported_clause("INSERT … ON CONFLICT", "ON CONSTRAINT")),
    }
}

/// How an INSERT's VALUES rows map onto the table's columns.
struct RowShape {
    /// Physical column → VALUES slot. `None` where the column takes no user
    /// value: SERIAL, hidden, or unnamed by an explicit column list.
    slot_of: Vec<Option<usize>>,
    /// Values every VALUES row must supply.
    expected: usize,
}

/// A column list names the slots and the arity; without one a row supplies every
/// visible column but `serial_ci` in schema order. An unnamed column is written NULL —
/// gnitz has no column DEFAULTs, and `check_not_null` catches the rest.
fn insert_row_shape(
    columns: &[ObjectName],
    schema: &Schema,
    serial_ci: Option<usize>,
) -> Result<RowShape, GnitzSqlError> {
    let mut slot_of: Vec<Option<usize>> = vec![None; schema.columns.len()];
    if columns.is_empty() {
        let mut expected = 0usize;
        for (ci, _) in schema.visible_columns() {
            if Some(ci) != serial_ci {
                slot_of[ci] = Some(expected);
                expected += 1;
            }
        }
        return Ok(RowShape { slot_of, expected });
    }
    for (k, name) in columns.iter().enumerate() {
        let ident = single_part_ident(name)
            .ok_or_else(|| GnitzSqlError::Rejected("INSERT column list: column must be a simple identifier".into()))?;
        let ci = find_unique_column(&schema.columns, ident)?
            .ok_or_else(|| GnitzSqlError::Rejected(format!("column '{ident}' not found in the INSERT column list")))?;
        if Some(ci) == serial_ci {
            return Err(GnitzSqlError::Rejected(
                "cannot supply a value for a SERIAL column; omit it from the INSERT".to_string(),
            ));
        }
        if slot_of[ci].is_some() {
            return Err(GnitzSqlError::Rejected(format!(
                "column '{ident}' specified more than once in the INSERT column list"
            )));
        }
        slot_of[ci] = Some(k);
    }
    Ok(RowShape { slot_of, expected: columns.len() })
}

pub(crate) fn execute_insert(
    client: &mut GnitzClient,
    schema_name: &str,
    insert: &Insert,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_insert_clauses(insert)?;
    let table_name_str = match &insert.table {
        TableObject::TableName(obj_name) => extract_object_name(obj_name, schema_name, "INSERT")?,
        _ => return Err(unsupported_clause("INSERT", "a table function target")),
    };
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| GnitzSqlError::Rejected("INSERT without VALUES not supported".to_string()))?;
    let rows = extract_values_rows(source)?;

    let target = client.resolve_relation(schema_name, &table_name_str)?;
    require_class(&target, &table_name_str, ClassWant::BaseTableOrStream, "INSERT")?;
    let (tid, schema) = (target.tid, &target.schema);
    let is_stream = target.class == RelClass::Stream;

    // RETURNING is supported on the plain-INSERT path only; capturing the
    // effective row under ON CONFLICT (which may UPDATE or skip a row) is out of
    // scope.
    reject_if(
        insert.returning.is_some() && insert.on.is_some(),
        "INSERT … ON CONFLICT",
        "RETURNING",
    )?;

    // Resolve the ON CONFLICT clause into a `ConflictPlan`.
    let plan = match insert.on.as_ref() {
        None => ConflictPlan::Error,
        Some(OnInsert::DuplicateKeyUpdate(_)) => {
            return Err(GnitzSqlError::Rejected(
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
                OnConflictAction::DoNothing => ConflictPlan::Resolve { set: None },
                OnConflictAction::DoUpdate(do_update) => {
                    reject_if(do_update.selection.is_some(), "INSERT … ON CONFLICT DO UPDATE", "WHERE")?;
                    let set = bind_set_list(&do_update.assignments, schema, &table_name_str, SetClause::DoUpdate)?;
                    ConflictPlan::Resolve { set: Some(set) }
                }
            }
        }
        Some(_) => {
            return Err(GnitzSqlError::Rejected("unsupported ON clause in INSERT".to_string()));
        }
    };

    // Build the incoming batch from VALUES rows, sized for the known row count.
    let n = rows.len();
    let mut batch = ZSetBatch::with_capacity(schema, n);

    let serial_ci = schema.pk_index_single().filter(|_| target.serial).map(|c| c as usize);
    let RowShape { slot_of, expected } = insert_row_shape(&insert.columns, schema, serial_ci)?;
    let payload: Vec<_> = schema.payload_columns().collect();
    // What a column the list left out reads as.
    let null: BExpr<Infallible> = BExpr::LitNull;
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
            return Err(GnitzSqlError::Rejected(format!(
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
                // the FixedIntNonnull comparator. The value is unobservable.
                batch.payload[payload_idx].push_zero();
                continue;
            }
            // Read off the *bound* constant, so `+NULL` is the NULL it spells;
            // a column the list left out is NULL too.
            let cell = slot_of[ci].map_or(&null, |s| &cells[s]);
            let is_null = matches!(cell, BExpr::LitNull);
            // The check is here for the *conflicting* row of an ON CONFLICT DO
            // UPDATE: its incoming NULL is consumed into the merged row and is
            // never pushed, so the wire boundary's own check never sees it.
            check_not_null(col_def, is_null)?;
            if is_null {
                gnitz_wire::null_word_set(&mut null_bits, payload_idx, true);
            }
            let ZSetBatch { payload: cols, blob, .. } = &mut batch;
            append_value_to_col(&mut cols[payload_idx].bytes, blob, col_def, cell)?;
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
                        rows_reply(items, None, &target, &table_name_str)?;
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
                            Some(mut m) => m.apply(batch),
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
        ConflictPlan::Resolve { mut set } => {
            // The RMW driver pushes `Update`, not `Error`: its OCC precondition is
            // what settles a stale resolution, where `Error` would raise a
            // duplicate-key error out of a statement spelled "do nothing". Re-resolved
            // per retry, so `SET x = x + 1` reads the freshest `x`; every row rides
            // at +1 and the worker's `enforce_unique_pk` turns a merged one into the
            // retract-and-insert. DO NOTHING reads the held keys alone.
            let keys = PkKeys::from_keys(schema.pk_stride(), (0..batch.len()).map(|i| batch.pks.get_bytes(i)));
            let read = TargetRead::new(&target, ReadBound::PkSet(keys), Vec::new(), set.is_none())?;
            let count = commit_rmw(client, &read, |held| {
                resolve_conflicts(&batch, held, set.as_deref_mut(), schema)
            })?;
            Ok(SqlResult::RowsAffected { count })
        }
    }
}

/// The batch an ON CONFLICT pushes. A key no row holds passes through; a held key
/// is dropped (DO NOTHING) or merged with the SET list (DO UPDATE); a key repeated
/// within the batch is skipped (DO NOTHING) or refused (DO UPDATE, PostgreSQL's
/// "cannot affect row a second time").
fn resolve_conflicts(
    batch: &ZSetBatch,
    held: ZSetBatch,
    set: Option<&mut [SetCol]>,
    schema: &Schema,
) -> Result<ZSetBatch, GnitzSqlError> {
    let is_held: HashSet<&[u8]> = (0..held.len()).map(|r| held.pks.get_bytes(r)).collect();
    // Each key's first incoming row: the dedup and the EXCLUDED source at once.
    let mut first: HashMap<&[u8], usize> = HashMap::with_capacity(batch.len());
    let mut out = ZSetBatch::with_capacity(schema, batch.len());
    for i in 0..batch.len() {
        let key = batch.pks.get_bytes(i);
        match first.entry(key) {
            Entry::Occupied(_) if set.is_some() => {
                return Err(GnitzSqlError::Rejected(
                    "ON CONFLICT DO UPDATE cannot affect row a second time \
                     (duplicate PK in the same batch)"
                        .to_string(),
                ))
            }
            Entry::Occupied(_) => continue,
            Entry::Vacant(v) => {
                v.insert(i);
            }
        }
        if !is_held.contains(key) {
            out.copy_row_at(batch, i, batch.weights[i]);
        }
    }
    if let Some(set) = set {
        // `held` is the `Existing` scope, so `SET x = x + 1` reads the row a
        // transaction buffered; `excluded` is the VALUES row each one collided with.
        let mut excluded = ZSetBatch::with_capacity(schema, held.len());
        for r in 0..held.len() {
            excluded.copy_row_at(batch, first[held.pks.get_bytes(r)], 1);
        }
        out.extend_from_owned(apply_set(set, held, Some(&excluded), schema)?);
    }
    Ok(out)
}

fn extract_values_rows(query: &Query) -> Result<&[Parens<Vec<Expr>>], GnitzSqlError> {
    match query.body.as_ref() {
        // Inert MySQL spellings of `VALUES (…)`: `VALUES ROW(…)` and `VALUE (…)`
        // parse to the same `rows`, so the row inserted is identical.
        SetExpr::Values(Values { rows, explicit_row: _, value_keyword: _ }) => Ok(rows),
        _ => Err(GnitzSqlError::Rejected(
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
            .expect("`validate_serial_key` admits only a fixed int")
            .range()
            .1;
        if i128::from(base) + n as i128 - 1 > max {
            let next = i128::from(base).max(max + 1);
            return Err(GnitzSqlError::Rejected(format!(
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
                    GnitzSqlError::Rejected(format!(
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
