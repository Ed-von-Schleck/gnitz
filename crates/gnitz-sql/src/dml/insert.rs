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
use crate::exec::keys::firsts;
use crate::ir::BExpr;
use crate::names::extract_object_name;
use crate::rules::{ClassWant, require_class, require_column};
use crate::validate::{QueryEnvelope, reject_unhonored_query_clauses};
use gnitz_core::{ClientError, ProtocolError, RelDescriptor, Schema, ZSetBatch};
use gnitz_wire::FixedInt;
use rustc_hash::FxHashMap;
use sqlparser::ast::{
    ConflictTarget, DoUpdate, Expr, Insert, ObjectName, OnConflict, OnConflictAction, OnInsert, Parens, Query,
    SelectItem, SetExpr, TableObject, Values,
};

/// A conflict target must name exactly the primary key — in any order, as
/// PostgreSQL allows — or be absent, which means the same key. A `UNIQUE` span is
/// not a target: a row duplicating one reaches the engine and raises there.
fn validate_conflict_target(target: &Option<ConflictTarget>, schema: &Schema) -> Result<(), GnitzSqlError> {
    match target {
        None => Ok(()),
        Some(ConflictTarget::Columns { columns: cols, predicate }) => {
            reject_if(predicate.is_some(), "INSERT … ON CONFLICT", "WHERE (index predicate)")?;
            let mut named: Vec<u32> = Vec::with_capacity(cols.len());
            for c in cols {
                let ci = require_column(schema.columns(), c.value.as_str()).map_err(|e| e.in_clause("ON CONFLICT"))?;
                named.push(ci as u32);
            }
            let mut pk = schema.pk_cols().to_vec();
            named.sort_unstable();
            pk.sort_unstable();
            if named != pk {
                return Err(GnitzSqlError::rejected(format!(
                    "ON CONFLICT target must name exactly the primary key ({})",
                    pk.iter()
                        .map(|&ci| &*schema.columns()[ci as usize].name)
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
    let mut slot_of: Vec<Option<usize>> = vec![None; schema.columns().len()];
    if columns.is_empty() {
        let unnamed = schema.visible_columns().filter(|&(ci, _)| Some(ci) != serial_ci);
        for (slot, (ci, _)) in unnamed.enumerate() {
            slot_of[ci] = Some(slot);
        }
        return Ok(slot_of);
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
        if slot_of[ci].is_some() {
            return Err(GnitzSqlError::rejected(format!(
                "column '{ident}' specified more than once in the INSERT column list"
            )));
        }
        slot_of[ci] = Some(k);
    }
    Ok(slot_of)
}

/// The table's SERIAL column, whose keys the server assigns.
pub(super) fn serial_col(target: &RelDescriptor) -> Option<usize> {
    target.schema.layout().lone_pk_col().filter(|_| target.serial)
}

/// The `Insert` fields [`plan_insert`] reads.
struct InsertClauses<'a> {
    table: &'a TableObject,
    source: Option<&'a Query>,
    columns: &'a [ObjectName],
    on: Option<&'a OnInsert>,
    returning: Option<&'a [SelectItem]>,
}

/// Reject every `Insert` clause the INSERT planner does not consume, and hand
/// back the ones it does. Each rejected one would otherwise be read as a plain
/// append. The `source` is a full `Query`, whose envelope an INSERT honors none of.
fn reject_unhonored_insert_clauses(insert: &Insert) -> Result<InsertClauses<'_>, GnitzSqlError> {
    const CTX: &str = "INSERT";
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
    if let Some(src) = source {
        reject_unhonored_query_clauses(src, QueryEnvelope::Bare, CTX)?;
    }
    Ok(InsertClauses {
        table,
        source: source.as_deref(),
        columns,
        on: on.as_ref(),
        returning: returning.as_deref(),
    })
}

pub(super) fn plan_insert(insert: &Insert, cat: &dyn Catalog) -> Result<WritePlan, GnitzSqlError> {
    let InsertClauses { table, source, columns, on, returning } = reject_unhonored_insert_clauses(insert)?;
    let table_name = match table {
        TableObject::TableName(obj_name) => extract_object_name(obj_name, cat.schema_name(), "INSERT")?,
        _ => return Err(unsupported_clause("INSERT", "a table function target")),
    };
    let source = source.ok_or_else(|| GnitzSqlError::rejected("INSERT without VALUES not supported"))?;
    let values = extract_values_rows(source)?;

    let target = cat.probe_relation(&table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTableOrStream, "INSERT")?;
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
            validate_conflict_target(conflict_target, schema)?;

            match action {
                OnConflictAction::DoNothing => Some(None),
                OnConflictAction::DoUpdate(DoUpdate { assignments, selection }) => {
                    reject_if(selection.is_some(), "INSERT … ON CONFLICT DO UPDATE", "WHERE")?;
                    let set = bind_set_list(assignments, schema, table_name.spelled_name(), SetClause::DoUpdate)?;
                    Some(Some(set))
                }
            }
        }
        Some(_) => {
            return Err(GnitzSqlError::rejected("unsupported ON clause in INSERT"));
        }
    };

    let serial_ci = serial_col(&target);
    let serial_placeholder = vec![0u8; schema.layout().pk_stride()];
    let slot_of = value_slots(columns, schema, serial_ci)?;
    let expected = slot_of.iter().flatten().count();
    // Per PK column in PK-list order, the VALUES slot it reads; a SERIAL key
    // reads none.
    let pk_slots: Vec<usize> = match serial_ci {
        Some(_) => Vec::new(),
        None => schema
            .pk_cols()
            .iter()
            .map(|&pi| {
                slot_of[pi as usize].ok_or_else(|| {
                    GnitzSqlError::rejected(format!(
                        "PK column '{}' missing from INSERT row",
                        schema.columns()[pi as usize].name
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
        // A SERIAL key is left zero, for the keys reserved at execute to replace.
        let mut out = rows.builder();
        match serial_ci {
            Some(_) => out.begin_row_bytes(&serial_placeholder, 1),
            None => {
                let mut natives = [0u128; gnitz_wire::MAX_PK_COLUMNS];
                for (k, (&pi, &slot)) in schema.pk_cols().iter().zip(&pk_slots).enumerate() {
                    let def = &schema.columns()[pi as usize];
                    check_not_null(def, matches!(cells[slot], BExpr::LitNull))?;
                    natives[k] = native_value(&cells[slot], def)?;
                }
                out.begin_row_natives(&natives[..pk_slots.len()], 1)
            }
        };
        for &(_, ci, col_def) in &payload {
            if col_def.is_hidden {
                // Logical-dropped column: a zero NOT-NULL filler cell, keeping a payload of
                // non-null fixed-width integers non-null. The value is unobservable.
                out.put_zero();
                continue;
            }
            // Read off the *bound* constant, so `+NULL` is the NULL it spells;
            // a column the list left out is NULL too.
            let cell = slot_of[ci].map_or(&null, |s| &cells[s]);
            let is_null = matches!(cell, BExpr::LitNull);
            check_not_null(col_def, is_null)?;
            if is_null {
                out.put_null();
            } else {
                out.put_cell(|cell_bytes, heap| write_value_cell(cell_bytes, heap, col_def, cell))?;
            }
        }
    }

    let write = match on_conflict {
        None => Write::Insert {
            returning: returning
                .map(|items| {
                    let rows = crate::hir::bind_returning(items, &target, table_name.spelled_name())?;
                    Ok::<_, GnitzSqlError>(Box::new(ClientMap::over(rows)?.0))
                })
                .transpose()?,
            rows,
        },
        Some(set) => Write::Upsert {
            // SERIAL keys are distinct by construction, and unknown until execute.
            rows: match serial_ci {
                None => first_per_key(rows, set.is_some())?,
                Some(_) => rows,
            },
            set,
        },
    };
    Ok(WritePlan { target, write })
}

/// `rows` down to each key's first row, the one an ON CONFLICT resolves; under
/// DO UPDATE (`refuse_repeat`) a second row for a key is refused instead.
fn first_per_key(mut rows: ZSetBatch, refuse_repeat: bool) -> Result<ZSetBatch, GnitzSqlError> {
    let v = rows.view();
    let firsts = firsts(v.pk(), v.schema().pk_stride());
    if refuse_repeat && firsts.repeats() {
        return Err(GnitzSqlError::rejected(
            "ON CONFLICT DO UPDATE cannot affect row a second time \
             (duplicate PK in the same batch)",
        ));
    }
    rows.retain_ranges(&firsts.keep);
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
    schema: &Schema,
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
    out.extend_from_owned(apply_set(set, held, Some(&excluded), schema)?);
    Ok(out)
}

/// A read reply that does not answer the keys it was asked for.
#[cold]
fn malformed_reply(what: &str) -> GnitzSqlError {
    GnitzSqlError::Client(ClientError::Protocol(ProtocolError::DecodeError(format!(
        "ON CONFLICT read reply carries {what}"
    ))))
}

fn extract_values_rows(query: &Query) -> Result<&[Parens<Vec<Expr>>], GnitzSqlError> {
    match query.body.as_ref() {
        // Inert MySQL spellings of `VALUES (…)`: `VALUES ROW(…)` and `VALUE (…)`
        // parse to the same `rows`, so the row inserted is identical.
        SetExpr::Values(Values { rows, explicit_row: _, value_keyword: _ }) => Ok(rows),
        _ => Err(GnitzSqlError::rejected(
            "INSERT only supports VALUES (not INSERT INTO ... SELECT)",
        )),
    }
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
