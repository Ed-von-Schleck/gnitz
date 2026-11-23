//! INSERT, including the `ON CONFLICT` upsert family: [`plan_insert`] encodes the
//! VALUES rows, and [`resolve_conflicts`] is what an `ON CONFLICT` writes for the
//! rows their keys hold. `DO UPDATE SET` runs the SET list `UPDATE` runs, so it
//! behaves exactly like an `UPDATE ... SET`.

use std::convert::Infallible;

use super::colwrite::{check_not_null, native_value, write_value_cell};
use super::set::{SetClause, SetCol, apply_set, bind_set_list};
use super::write::{Write, WritePlan};
use crate::ast_util::single_part_ident;
use crate::bind::bind_constant;
use crate::catalog::Catalog;
use crate::error::{GnitzSqlError, reject_if, unsupported_clause};
use crate::exec::client_map::ClientMap;
use crate::hir::RowsReply;
use crate::ir::BExpr;
use crate::names::extract_object_name;
use crate::rules::{ClassWant, column_names, require_class, require_column};
use crate::validate::{QueryEnvelope, reject_unhonored_query_clauses};
use gnitz_core::{ClientError, ProtocolError, RelDescriptor, RelName, Schema, ZSetBatch};
use gnitz_wire::FixedInt;
use gnitz_zset::algebra::pk_firsts;
use rustc_hash::FxHashMap;
use sqlparser::ast::{
    ConflictTarget, DoUpdate, Expr, Insert, ObjectName, OnConflict, OnConflictAction, OnInsert, Parens, SetExpr,
    TableObject, Values,
};

pub(super) fn plan_insert(insert: &Insert, cat: &dyn Catalog) -> Result<WritePlan, GnitzSqlError> {
    const CTX: &str = "INSERT";
    // Every clause the planner does not consume is rejected, as it would otherwise
    // be read as a plain append.
    let Insert {
        table,
        source,
        columns,
        on,
        returning,
        // Inert keyword markers (`INTO` / `TABLE`, the `INSERT` token) and
        // advisory-only comment hints: positional flags, no droppable semantics.
        into: _,
        has_table_keyword: _,
        insert_token: _,
        optimizer_hints: _,
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
        by_name,
        overriding,
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
    reject_if(*by_name, CTX, "BY NAME")?;
    reject_if(overriding.is_some(), CTX, "OVERRIDING … VALUE")?;
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
    // The `source` is a full `Query`, whose envelope an INSERT honors none of.
    if let Some(src) = source {
        reject_unhonored_query_clauses(src, QueryEnvelope::Bare, CTX)?;
    }

    let table_name = match table {
        TableObject::TableName(obj_name) => extract_object_name(obj_name, cat.schema_name(), CTX)?,
        _ => return Err(unsupported_clause(CTX, "a table function target")),
    };
    let values = match source.as_deref().map(|q| &*q.body) {
        None => return Err(GnitzSqlError::rejected("INSERT without VALUES not supported")),
        // Inert MySQL spellings of `VALUES (…)`: `VALUES ROW(…)` and `VALUE (…)`
        // parse to the same `rows`, so the row inserted is identical.
        Some(SetExpr::Values(Values { rows, explicit_row: _, value_keyword: _ })) => rows,
        Some(_) => {
            return Err(GnitzSqlError::rejected(
                "INSERT only supports VALUES (not INSERT INTO ... SELECT)",
            ));
        }
    };

    let target = cat.probe_relation(&table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTableOrStream, CTX)?;
    let schema = &target.schema;

    reject_if(returning.is_some() && on.is_some(), "INSERT … ON CONFLICT", "RETURNING")?;

    // `Some` under ON CONFLICT, holding DO UPDATE's SET list.
    let on_conflict = match on {
        None => None,
        Some(OnInsert::DuplicateKeyUpdate(_)) => {
            return Err(GnitzSqlError::rejected(
                "ON DUPLICATE KEY UPDATE not supported — use PostgreSQL-style \
                 ON CONFLICT (col) DO UPDATE",
            ));
        }
        Some(OnInsert::OnConflict(OnConflict { conflict_target, action })) => {
            // Both actions resolve the incoming row against stored rows, which a
            // stream does not have.
            require_class(&target, &table_name, ClassWant::BaseTable, "INSERT … ON CONFLICT")?;
            validate_conflict_target(conflict_target.as_ref(), schema)?;
            Some(match action {
                OnConflictAction::DoNothing => None,
                OnConflictAction::DoUpdate(DoUpdate { assignments, selection }) => {
                    reject_if(selection.is_some(), "INSERT … ON CONFLICT DO UPDATE", "WHERE")?;
                    Some(bind_set_list(
                        assignments,
                        schema,
                        table_name.spelled_name(),
                        SetClause::DoUpdate,
                    )?)
                }
            })
        }
        Some(_) => return Err(GnitzSqlError::rejected("unsupported ON clause in INSERT")),
    };

    let mut rows = values_batch(values, columns, &target, &table_name)?;

    let write = match on_conflict {
        None => Write::Insert {
            returning: returning
                .as_deref()
                .map(|items| {
                    let RowsReply { src, schema, program, .. } =
                        crate::hir::bind_returning(items, &target, table_name.spelled_name())?;
                    Ok::<_, GnitzSqlError>(Box::new(ClientMap::new(program, src, schema)?))
                })
                .transpose()?,
            rows,
        },
        Some(set) => {
            // An ON CONFLICT resolves each key's first row; DO UPDATE refuses a
            // second. SERIAL keys are distinct by construction, and unknown until
            // execute.
            if !target.serial {
                let firsts = pk_firsts(&rows.view());
                if set.is_some() && firsts.repeats() {
                    return Err(GnitzSqlError::rejected(
                        "ON CONFLICT DO UPDATE cannot affect row a second time \
                         (duplicate PK in the same batch)",
                    ));
                }
                rows.retain_ranges(&firsts.keep);
            }
            Write::Upsert { rows, set }
        }
    };
    Ok(WritePlan { target, write })
}

/// A conflict target must name exactly the primary key — in any order, as
/// PostgreSQL allows — or be absent, which means the same key. A `UNIQUE` span is
/// not a target: a row duplicating one reaches the engine and raises there.
fn validate_conflict_target(target: Option<&ConflictTarget>, schema: &Schema) -> Result<(), GnitzSqlError> {
    match target {
        None => Ok(()),
        Some(ConflictTarget::Columns { columns, predicate }) => {
            reject_if(predicate.is_some(), "INSERT … ON CONFLICT", "WHERE (index predicate)")?;
            let mut named = columns
                .iter()
                .map(|c| require_column(schema.columns(), c.value.as_str()).map(|ci| ci as u32))
                .collect::<Result<Vec<u32>, _>>()
                .map_err(|e| e.in_clause("ON CONFLICT"))?;
            let mut pk = schema.pk_cols().to_vec();
            named.sort_unstable();
            pk.sort_unstable();
            if named != pk {
                return Err(GnitzSqlError::rejected(format!(
                    "ON CONFLICT target must name exactly the primary key ({})",
                    column_names(schema.columns(), schema.pk_cols()),
                )));
            }
            Ok(())
        }
        Some(ConflictTarget::OnConstraint(_)) => Err(unsupported_clause("INSERT … ON CONFLICT", "ON CONSTRAINT")),
    }
}

/// The VALUES rows as a batch of `target`, each at weight +1. `columns` names the
/// column each value of a row lands in; without a list a row supplies every
/// visible column but a SERIAL one, in schema order. A column no value lands in is
/// written NULL — gnitz has no column DEFAULTs, and `check_not_null` catches the
/// rest. A SERIAL key is planned as 0, for the keys reserved at execute to
/// replace.
fn values_batch(
    values: &[Parens<Vec<Expr>>],
    columns: &[ObjectName],
    target: &RelDescriptor,
    table_name: &RelName,
) -> Result<ZSetBatch, GnitzSqlError> {
    let schema = &target.schema;
    // The server assigns a SERIAL column's keys.
    let serial_ci = schema.layout().lone_pk_col().filter(|_| target.serial);
    // Physical column → VALUES slot, `None` where the column takes no user value:
    // SERIAL, hidden, or unnamed by an explicit column list.
    let mut slot_of: Vec<Option<usize>> = vec![None; schema.columns().len()];
    if columns.is_empty() {
        let unnamed = schema.visible_columns().filter(|&(ci, _)| Some(ci) != serial_ci);
        for (slot, (ci, _)) in unnamed.enumerate() {
            slot_of[ci] = Some(slot);
        }
    }
    for (k, name) in columns.iter().enumerate() {
        let ident = single_part_ident(name)
            .ok_or_else(|| GnitzSqlError::rejected("INSERT column list: column must be a simple identifier"))?;
        let ci = require_column(schema.columns(), ident).map_err(|e| e.in_clause("INSERT column list"))?;
        if Some(ci) == serial_ci {
            return Err(GnitzSqlError::rejected(
                "cannot supply a value for a SERIAL column; omit it from the INSERT",
            ));
        }
        if slot_of[ci].replace(k).is_some() {
            return Err(GnitzSqlError::rejected(format!(
                "column '{ident}' specified more than once in the INSERT column list"
            )));
        }
    }
    let expected = slot_of.iter().flatten().count();
    // The PK columns a row writes, in PK-list order.
    let written_key: &[u32] = if serial_ci.is_some() { &[] } else { schema.pk_cols() };
    if let Some(&pi) = written_key.iter().find(|&&pi| slot_of[pi as usize].is_none()) {
        return Err(GnitzSqlError::rejected(format!(
            "PK column '{}' missing from INSERT row",
            schema.columns()[pi as usize].name
        )));
    }
    // What a column the list left out reads as.
    let null: BExpr<Infallible> = BExpr::LitNull;
    // One bound cell per VALUES slot, reused across rows.
    let mut cells: Vec<BExpr<Infallible>> = Vec::new();
    let mut rows = ZSetBatch::with_capacity(schema, values.len());

    for row in values {
        // Standard SQL rejects a VALUES row whose arity differs from the expected
        // count, in either direction — too few values, or excess trailing ones.
        // This guard makes every slot below in-bounds.
        if row.len() != expected {
            let hint = if serial_ci.is_some() {
                " (its SERIAL primary key is auto-assigned)"
            } else {
                ""
            };
            return Err(GnitzSqlError::rejected(format!(
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
        // Read off the *bound* constant, so `+NULL` is the NULL it spells.
        let cell_of = |ci: usize| slot_of[ci].map_or(&null, |s| &cells[s]);
        let mut natives = [0u128; gnitz_wire::MAX_PK_COLUMNS];
        for (k, &pi) in written_key.iter().enumerate() {
            let (def, cell) = (&schema.columns()[pi as usize], cell_of(pi as usize));
            check_not_null(def, matches!(cell, BExpr::LitNull))?;
            natives[k] = native_value(cell, def)?;
        }
        let mut out = rows.builder();
        out.begin_row_natives(&natives[..schema.pk_cols().len()], 1);
        for (_, ci, col_def) in schema.payload_columns() {
            if col_def.is_hidden {
                // Logical-dropped column: a zero NOT-NULL filler cell, keeping a payload of
                // non-null fixed-width integers non-null. The value is unobservable.
                out.put_zero();
                continue;
            }
            let cell = cell_of(ci);
            let is_null = matches!(cell, BExpr::LitNull);
            check_not_null(col_def, is_null)?;
            if is_null {
                out.put_null();
            } else {
                out.put_cell(|cell_bytes, heap| write_value_cell(cell_bytes, heap, col_def, cell))?;
            }
        }
    }
    Ok(rows)
}

/// The delta an ON CONFLICT pushes, given the rows `held` under the keys of
/// `rows` (one row per key): a row whose key nothing holds passes through; a
/// held key is dropped (DO NOTHING) or merged with the SET list (DO UPDATE). A
/// `held` holding a key outside `rows`, or one key twice, is a malformed reply.
pub(super) fn resolve_conflicts(
    rows: ZSetBatch,
    held: ZSetBatch,
    set: Option<&mut [SetCol]>,
) -> Result<ZSetBatch, GnitzSqlError> {
    let (v, held_rows) = (rows.view(), held.view());
    let at: FxHashMap<&[u8], usize> = (0..v.len()).map(|i| (v.get_pk_bytes(i), i)).collect();
    let mut is_held = vec![false; v.len()];
    let collided = (0..held_rows.len())
        .map(|r| match at.get(held_rows.get_pk_bytes(r)) {
            Some(&i) if !std::mem::replace(&mut is_held[i], true) => Ok(i as u32),
            Some(_) => Err(malformed_reply("a key held twice")),
            None => Err(malformed_reply("a key that was not read")),
        })
        .collect::<Result<Vec<u32>, _>>()?;
    let kept: Vec<u32> = (0..rows.len() as u32).filter(|&i| !is_held[i as usize]).collect();
    let Some(set) = set else {
        return Ok(rows.gather(&kept));
    };
    // `held` is the `Existing` scope, so `SET x = x + 1` reads the row a
    // transaction buffered; `excluded` is the VALUES row each one collided with.
    let excluded = rows.gathered(&collided);
    let mut out = rows.gather(&kept);
    out.extend_from_owned(apply_set(set, held, Some(&excluded))?);
    Ok(out)
}

/// A read reply that does not answer the keys it was asked for.
#[cold]
fn malformed_reply(what: &str) -> GnitzSqlError {
    GnitzSqlError::Client(ClientError::Protocol(ProtocolError::DecodeError(format!(
        "ON CONFLICT read reply carries {what}"
    ))))
}

/// The PK region of `n` SERIAL rows drawn from `base`, in row order, refused when the last
/// exceeds the key column's type.
pub(super) fn serial_keys(base: u64, n: usize, schema: &Schema) -> Result<Vec<u8>, GnitzSqlError> {
    let tc = schema.columns()[schema.pk_cols()[0] as usize].ty.tc;
    let max = FixedInt::from_type_code(tc)
        .expect("the engine admits only a fixed-width integer SERIAL key")
        .range()
        .1;
    if i128::from(base) + n as i128 - 1 > max {
        let next = i128::from(base).max(max + 1);
        return Err(GnitzSqlError::rejected(format!(
            "SERIAL primary key exhausted: next value {next} exceeds the column type maximum {max}"
        )));
    }
    let mut region = Vec::with_capacity(n * tc.wire_stride());
    for id in base..base + n as u64 {
        gnitz_wire::push_opk(&mut region, tc.wire_stride(), gnitz_wire::key_image(tc, u128::from(id)));
    }
    Ok(region)
}

#[cfg(test)]
#[path = "tests/insert.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/insert.rs"]
mod bench;
