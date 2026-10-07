//! INSERT, including the `ON CONFLICT` upsert family. The default form pushes
//! with `WireConflictMode::Error`; `DO NOTHING` / `DO UPDATE` filter or merge the
//! VALUES against the rows their keys hold, as a read-modify-write. `DO UPDATE
//! SET` runs `mutate`'s SET list, so it behaves exactly like an `UPDATE ... SET`.
//!
//! [`plan_insert`] reads only the catalog, so a refused statement issues no
//! request and consumes no SERIAL id; [`execute_insert`] runs what it planned.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use crate::ast_util::{extract_object_name, single_part_ident};
use crate::bind::bind_constant;
use crate::bind::{require_column, Catalog};
use crate::codec::colwrite::{append_value_to_col, check_not_null, native_value};
use crate::dml::mutate::{apply_set, bind_set_list, SetClause, SetCol};
use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::exec::client_map::ClientMap;
use crate::hir::RowsReply;
use crate::ir::BExpr;
use crate::rules::{require_class, ClassWant};
use crate::validate::{reject_unhonored_query_clauses, QueryEnvelope};
use crate::SqlResult;
use gnitz_core::{GnitzClient, PkColumn, RelDescriptor, ScanReply, Schema, ZSetBatch};
use gnitz_expr::SchemaFacts;
use gnitz_wire::{FixedInt, ReadBound, RelClass, WireConflictMode};
use sqlparser::ast::{
    ConflictTarget, Expr, Insert, ObjectName, OnConflict, OnConflictAction, OnInsert, Parens, Query, SetExpr,
    TableObject, Values,
};

/// What an INSERT does about a key already held. The conflict target is
/// validated and discarded in `validate_conflict_target`; only the action
/// survives into the plan.
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
                let ci = require_column(&schema.columns, c.value.as_str()).map_err(|e| e.in_clause("ON CONFLICT"))?;
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

/// Physical column → VALUES slot, `None` where the column takes no user value:
/// SERIAL, hidden, or unnamed by an explicit column list. A column list names the
/// slots; without one a row supplies every visible column but `serial_ci` in
/// schema order. An unnamed column is written NULL — gnitz has no column DEFAULTs,
/// and `check_not_null` catches the rest.
fn value_slots(
    columns: &[ObjectName],
    schema: &Schema,
    serial_ci: Option<usize>,
) -> Result<Vec<Option<usize>>, GnitzSqlError> {
    let mut slot_of: Vec<Option<usize>> = vec![None; schema.columns.len()];
    if columns.is_empty() {
        let unnamed = schema.visible_columns().filter(|&(ci, _)| Some(ci) != serial_ci);
        for (slot, (ci, _)) in unnamed.enumerate() {
            slot_of[ci] = Some(slot);
        }
        return Ok(slot_of);
    }
    for (k, name) in columns.iter().enumerate() {
        let ident = single_part_ident(name)
            .ok_or_else(|| GnitzSqlError::Rejected("INSERT column list: column must be a simple identifier".into()))?;
        let ci = require_column(&schema.columns, ident).map_err(|e| e.in_clause("INSERT column list"))?;
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
    Ok(slot_of)
}

/// An INSERT, planned: everything but the SERIAL keys, which only the server
/// can hand out.
pub(crate) struct InsertPlan {
    target: Arc<RelDescriptor>,
    /// The VALUES rows at weight +1. Into a SERIAL table the key region is empty
    /// until [`execute_insert`] stamps it.
    rows: ZSetBatch,
    conflict: ConflictPlan,
    /// The RETURNING reply's schema, and the map filling it when it is not the
    /// table's own layout.
    returning: Option<(Arc<Schema>, Option<ClientMap>)>,
}

/// The table's SERIAL column, whose keys the server assigns.
fn serial_col(target: &RelDescriptor) -> Option<usize> {
    target.schema.lone_pk_col().filter(|_| target.serial)
}

/// Reject every `Insert`-statement clause the INSERT planner does not consume. It reads only
/// `table`, `source`, `columns`, `on` (ON CONFLICT) and `returning`; every other field is a
/// conflict / overwrite / partition clause parsed under `GenericDialect` and
/// silently reinterpreted as a plain append. The `source` is a full `Query` whose envelope (LIMIT,
/// ORDER BY, FETCH, a `WITH`, …) an INSERT equally cannot honor, so it is routed through
/// `reject_unhonored_query_clauses` here too.
///
/// Exhaustive destructure (no `..`): a future `sqlparser` `Insert` field stops the build here.
fn reject_unhonored_insert_clauses(insert: &sqlparser::ast::Insert) -> Result<(), GnitzSqlError> {
    const CTX: &str = "INSERT";
    let sqlparser::ast::Insert {
        // Consumed by `plan_insert`; `source`'s `Query` envelope is additionally checked below.
        table: _,
        source,
        columns: _,
        on: _,
        // Inert keyword markers (`INTO` / `TABLE`, the `INSERT` token) and
        // advisory-only comment hints: positional flags, no droppable semantics.
        into: _,
        has_table_keyword: _,
        insert_token: _,
        optimizer_hints: _,
        // Consumed by `plan_insert`: INSERT ... RETURNING is supported (projected
        // client-side from the just-built batch).
        returning: _,
        // Rejected: each is a clause the engine does not implement.
        or,
        ignore,
        overwrite,
        partitioned,
        replace_into,
        priority,
        insert_alias,
        table_alias,
        assignments,
        after_columns,
        settings,
        format_clause,
        output,
        multi_table_insert_type,
        multi_table_into_clauses,
        multi_table_when_clauses,
        multi_table_else_clause,
    } = insert;

    reject_if(or.is_some(), CTX, "OR (conflict clause)")?;
    reject_if(*ignore, CTX, "IGNORE")?;
    reject_if(*overwrite, CTX, "OVERWRITE")?;
    reject_if(*replace_into, CTX, "REPLACE INTO")?;
    reject_if(partitioned.is_some(), CTX, "PARTITION")?;
    reject_if(priority.is_some(), CTX, "priority (LOW_PRIORITY/HIGH_PRIORITY/DELAYED)")?;
    reject_if(insert_alias.is_some(), CTX, "row alias (AS alias)")?;
    reject_if(table_alias.is_some(), CTX, "table alias")?;
    reject_if(!assignments.is_empty(), CTX, "SET")?;
    reject_if(!after_columns.is_empty(), CTX, "AFTER columns")?;
    reject_if(settings.is_some(), CTX, "SETTINGS")?;
    reject_if(format_clause.is_some(), CTX, "FORMAT")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(
        multi_table_insert_type.is_some()
            || !multi_table_into_clauses.is_empty()
            || !multi_table_when_clauses.is_empty()
            || multi_table_else_clause.is_some(),
        CTX,
        "multi-table INSERT (ALL/FIRST)",
    )?;
    // The `source` is a full `Query`; an INSERT honors no envelope clause on it (LIMIT/OFFSET,
    // ORDER BY, FETCH, FOR UPDATE/SHARE, SETTINGS, FORMAT, a `WITH`). Route it through the shared
    // `Query` guard so a dropped envelope clause is a clean error, not a silent full-table insert.
    if let Some(src) = source {
        reject_unhonored_query_clauses(src, QueryEnvelope::Bare, CTX)?;
    }
    Ok(())
}

pub(crate) fn plan_insert(insert: &Insert, cat: &dyn Catalog) -> Result<InsertPlan, GnitzSqlError> {
    reject_unhonored_insert_clauses(insert)?;
    let table_name = match &insert.table {
        TableObject::TableName(obj_name) => extract_object_name(obj_name, cat.schema_name(), "INSERT")?,
        _ => return Err(unsupported_clause("INSERT", "a table function target")),
    };
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| GnitzSqlError::Rejected("INSERT without VALUES not supported".to_string()))?;
    let values = extract_values_rows(source)?;

    let target = cat.probe_relation(&table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTableOrStream, "INSERT")?;
    let schema = &target.schema;

    // RETURNING is supported on the plain-INSERT path only; capturing the
    // effective row under ON CONFLICT (which may UPDATE or skip a row) is out of
    // scope.
    reject_if(
        insert.returning.is_some() && insert.on.is_some(),
        "INSERT … ON CONFLICT",
        "RETURNING",
    )?;

    let conflict = match insert.on.as_ref() {
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
            require_class(&target, &table_name, ClassWant::BaseTable, "INSERT … ON CONFLICT")?;
            validate_conflict_target(conflict_target, schema)?;

            match action {
                OnConflictAction::DoNothing => ConflictPlan::Resolve { set: None },
                OnConflictAction::DoUpdate(do_update) => {
                    reject_if(do_update.selection.is_some(), "INSERT … ON CONFLICT DO UPDATE", "WHERE")?;
                    let set = bind_set_list(
                        &do_update.assignments,
                        schema,
                        table_name.spelled_name(),
                        SetClause::DoUpdate,
                    )?;
                    ConflictPlan::Resolve { set: Some(set) }
                }
            }
        }
        Some(_) => {
            return Err(GnitzSqlError::Rejected("unsupported ON clause in INSERT".to_string()));
        }
    };

    let serial_ci = serial_col(&target);
    let slot_of = value_slots(&insert.columns, schema, serial_ci)?;
    let expected = slot_of.iter().flatten().count();
    // Per PK column in PK-list order, the VALUES slot it reads; a SERIAL key
    // reads none.
    let pk_slots: Vec<usize> = match serial_ci {
        Some(_) => Vec::new(),
        None => schema
            .pk_cols
            .iter()
            .map(|&pi| {
                slot_of[pi as usize].ok_or_else(|| {
                    GnitzSqlError::Rejected(format!(
                        "PK column '{}' missing from INSERT row",
                        schema.columns[pi as usize].name
                    ))
                })
            })
            .collect::<Result<_, _>>()?,
    };
    let payload: Vec<_> = schema.payload_columns().collect();
    // What a column the list left out reads as.
    let null: BExpr<Infallible> = BExpr::LitNull;
    // One bound cell per VALUES slot, reused across rows and read by both
    // consumers below, so a row's PK slot and payload slot cannot disagree on
    // what a written constant is.
    let mut cells: Vec<BExpr<Infallible>> = Vec::new();
    let mut rows = ZSetBatch::with_capacity(schema, values.len());

    for row in values {
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
                table_name,
                expected,
                hint
            )));
        }
        cells.clear();
        for e in row.iter() {
            cells.push(bind_constant(e)?);
        }
        if serial_ci.is_none() {
            let mut natives = [0u128; gnitz_wire::MAX_PK_COLUMNS];
            for (k, (&pi, &slot)) in schema.pk_cols.iter().zip(&pk_slots).enumerate() {
                let def = &schema.columns[pi as usize];
                check_not_null(def, matches!(cells[slot], BExpr::LitNull))?;
                natives[k] = native_value(&cells[slot], def)?;
            }
            rows.pks.push_natives(&natives[..pk_slots.len()]);
        }
        rows.weights.push(1);

        let mut null_bits: u64 = 0;
        for &(payload_idx, ci, col_def) in &payload {
            if col_def.is_hidden {
                // Logical-dropped column: a zero-filled NOT-NULL filler cell (null
                // bit left unset), keeping the batch rectangular and the table on
                // the FixedIntNonnull comparator. The value is unobservable.
                rows.payload[payload_idx].push_zero();
                continue;
            }
            // Read off the *bound* constant, so `+NULL` is the NULL it spells;
            // a column the list left out is NULL too.
            let cell = slot_of[ci].map_or(&null, |s| &cells[s]);
            let is_null = matches!(cell, BExpr::LitNull);
            check_not_null(col_def, is_null)?;
            if is_null {
                gnitz_wire::null_word_set(&mut null_bits, payload_idx, true);
            }
            let ZSetBatch { payload: cols, blob, .. } = &mut rows;
            append_value_to_col(&mut cols[payload_idx].bytes, blob, col_def, cell)?;
        }
        rows.nulls.push(null_bits);
    }

    // SERIAL keys are distinct by construction, and unknown until execute.
    if let (ConflictPlan::Resolve { set }, None) = (&conflict, serial_ci) {
        rows = first_per_key(rows, set.is_some())?;
    }
    let returning = insert
        .returning
        .as_deref()
        .map(|items| {
            let RowsReply { schema: out_schema, program, .. } =
                crate::hir::bind_returning(items, &target, table_name.spelled_name())?.reply(&[])?;
            let map = program
                .map(|p| ClientMap::new(p, schema, Arc::clone(&out_schema)))
                .transpose()?;
            Ok::<_, GnitzSqlError>((out_schema, map))
        })
        .transpose()?;
    Ok(InsertPlan { target, rows, conflict, returning })
}

/// `rows` down to each key's first row, the one an ON CONFLICT resolves; under
/// DO UPDATE (`refuse_repeat`) a second row for a key is refused instead.
fn first_per_key(mut rows: ZSetBatch, refuse_repeat: bool) -> Result<ZSetBatch, GnitzSqlError> {
    let (_, keep) = crate::exec::agg_finish::firsts(&rows);
    if refuse_repeat && keep != [(0, rows.len())] {
        return Err(GnitzSqlError::Rejected(
            "ON CONFLICT DO UPDATE cannot affect row a second time \
             (duplicate PK in the same batch)"
                .to_string(),
        ));
    }
    rows.retain_ranges(&keep);
    Ok(rows)
}

pub(crate) async fn execute_insert(client: &mut GnitzClient, plan: InsertPlan) -> Result<SqlResult, GnitzSqlError> {
    let InsertPlan { target, mut rows, conflict, returning } = plan;
    let schema = &target.schema;
    if serial_col(&target).is_some() {
        // One reservation for the whole statement.
        let n = rows.len();
        rows.pks = serial_keys(client.reserve_serial_ids(&target, n as u64).await?, n, schema)?;
    }
    match conflict {
        ConflictPlan::Error => {
            // The engine refuses `Error` on a stream (its PK is not unique) and
            // leaves `Update` unread: the push appends, so the same row twice is
            // one element at weight 2.
            let mode = if target.class == RelClass::Stream {
                WireConflictMode::Update
            } else {
                WireConflictMode::Error
            };
            // Split on RETURNING: the batch is given away when nothing will
            // project it, saving a deep clone inside a transaction.
            match returning {
                Some((schema_out, map)) => {
                    client.push(&*target, schema, &rows, mode).await?;
                    Ok(SqlResult::Rows(ScanReply {
                        schema: schema_out,
                        batch: match map {
                            Some(mut m) => m.apply(rows),
                            None => rows,
                        },
                        lsn: None,
                    }))
                }
                None => {
                    let count = rows.len();
                    client.push(&*target, schema, rows, mode).await?;
                    Ok(SqlResult::RowsAffected { count })
                }
            }
        }
        ConflictPlan::Resolve { mut set } => {
            let bound = ReadBound::PkSet(rows.pks.keys());
            let keys_only = set.is_none();
            let count = client
                .read_modify_write(&target, bound, Vec::new(), keys_only, |held| {
                    resolve_conflicts(&rows, held, set.as_deref_mut(), schema)
                })
                .await?;
            Ok(SqlResult::RowsAffected { count })
        }
    }
}

/// The delta an ON CONFLICT pushes, given the rows `held` under the keys of
/// `rows` (one row per key): a row whose key nothing holds passes through; a
/// held key is dropped (DO NOTHING) or merged with the SET list (DO UPDATE).
fn resolve_conflicts(
    rows: &ZSetBatch,
    held: ZSetBatch,
    set: Option<&mut [SetCol]>,
    schema: &Schema,
) -> Result<ZSetBatch, GnitzSqlError> {
    let at: HashMap<&[u8], usize> = (0..rows.len()).map(|i| (rows.pks.get_bytes(i), i)).collect();
    let collided: Vec<usize> = (0..held.len()).map(|r| at[held.pks.get_bytes(r)]).collect();
    let mut is_held = vec![false; rows.len()];
    for &i in &collided {
        is_held[i] = true;
    }
    let mut out = ZSetBatch::with_capacity(schema, rows.len());
    for i in (0..rows.len()).filter(|&i| !is_held[i]) {
        out.copy_row_at(rows, i, 1);
    }
    if let Some(set) = set {
        // `held` is the `Existing` scope, so `SET x = x + 1` reads the row a
        // transaction buffered; `excluded` is the VALUES row each one collided with.
        let mut excluded = ZSetBatch::with_capacity(schema, held.len());
        for &i in &collided {
            excluded.copy_row_at(rows, i, 1);
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

/// The keys of `n` SERIAL rows drawn from `base`, refused when the last exceeds
/// the key column's type.
fn serial_keys(base: u64, n: usize, schema: &Schema) -> Result<PkColumn, GnitzSqlError> {
    let tc = schema.columns[schema.pk_cols[0] as usize].ty.tc;
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
    Ok(PkColumn::from_natives(
        schema,
        (0..n as u64).map(|i| u128::from(base + i)),
    ))
}

#[cfg(test)]
#[path = "tests/insert.rs"]
mod tests;
