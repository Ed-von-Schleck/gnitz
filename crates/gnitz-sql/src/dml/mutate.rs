//! UPDATE and DELETE, as one read-then-write flow: plan the WHERE through the
//! shared access-path ladder (`dml::plan`), read the rows it matches in the
//! transaction's effective state (`dml::overlay`), then write the rewritten rows
//! (UPDATE) or the retraction of their keys (DELETE) under the RMW driver.
//!
//! The SET list — [`bind_set_list`] binds the targets, [`classify_set_rhs`]
//! compiles each value, [`apply_set`] rewrites a batch — is shared with INSERT's
//! `ON CONFLICT DO UPDATE`.

use std::sync::Arc;

use crate::ast_util::{classify_from, extract_table_name_and_alias, single_part_ident, FromShape};
use crate::bind::{bind_single_table, find_unique_column};
use crate::codec::colwrite::{append_value_to_col, check_not_null};
use crate::codec::project_schema::key_reply;
use crate::dml::overlay::resolve_where_matches;
use crate::dml::plan::{bind_where, bound_and_predicate};
use crate::dml::rmw::commit_rmw_or_buffer;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_scalar_evaluator;
use crate::ir::BoundExpr;
use crate::validate::{reject_unhonored_delete_clauses, reject_unhonored_update_clauses, require_class, ClassWant};
use crate::SqlResult;
use gnitz_core::{retraction_batch, ColType, ColumnDef, FixedInt, GnitzClient, Schema, TypeCode, ZSetBatch};
use gnitz_expr::{ExprResults, ScalarEval, SchemaFacts};
use gnitz_wire::{encode_german_string, german_string_content, null_word_get, null_word_set, ReadSink, ReadSpec};
use sqlparser::ast::{Assignment, AssignmentTarget, Delete, Expr, FromTable, TableWithJoins, Update};

// ---------------------------------------------------------------------------
// UPDATE / DELETE
// ---------------------------------------------------------------------------

pub(crate) fn execute_update(
    client: &mut GnitzClient,
    schema_name: &str,
    update: &Update,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_update_clauses(update)?;
    execute_mutation(
        client,
        schema_name,
        "UPDATE",
        std::slice::from_ref(&update.table),
        update.selection.as_ref(),
        Some(&update.assignments),
    )
}

pub(crate) fn execute_delete(
    client: &mut GnitzClient,
    schema_name: &str,
    del: &Delete,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_delete_clauses(del)?;
    let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = &del.from;
    execute_mutation(client, schema_name, "DELETE", from, del.selection.as_ref(), None)
}

/// A single-table UPDATE (`set` present) or DELETE: read the rows the WHERE
/// matches, then write the rewritten rows or the retraction of their keys.
fn execute_mutation(
    client: &mut GnitzClient,
    schema_name: &str,
    verb: &str,
    from: &[TableWithJoins],
    selection: Option<&Expr>,
    set: Option<&[Assignment]>,
) -> Result<SqlResult, GnitzSqlError> {
    // `UPDATE a JOIN b ON … SET v = 1` parses; honoring only the relation would
    // update all of `a`.
    let FromShape::SinglePlainRelation(factor) = classify_from(from) else {
        return Err(GnitzSqlError::Unsupported(format!(
            "{verb}: exactly one simple FROM table required"
        )));
    };
    let (table_name, alias) = extract_table_name_and_alias(factor, schema_name, verb)?;
    let target = client.resolve_relation(schema_name, &table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTable, verb)?;
    let (tid, schema) = (target.tid, &target.schema);
    // Before the read, so a bad SET list errors whether or not a row matches.
    let mut set = set
        .map(|raw| {
            bind_set_list(raw, schema, "UPDATE SET", |e, ci| {
                classify_set_rhs(&bind_single_table(e, schema, &alias)?, Scope::Existing, ci, schema)
            })
        })
        .transpose()?;
    let where_expr = bind_where(schema, &alias, selection)?;
    let (bound, predicate) = bound_and_predicate(schema, &where_expr, &target.indexes)?.into_parts();
    // UPDATE reads whole rows; DELETE the source PK alone, with no blob heap.
    let (reply_schema, sink) = if set.is_some() {
        (Arc::clone(schema), ReadSink::all_rows())
    } else {
        let (reply, map) = key_reply(schema)?;
        (Arc::new(reply), ReadSink { map: Some(map), ..ReadSink::all_rows() })
    };
    let spec = ReadSpec { bound, predicate, sink };
    // The build re-runs per RMW retry, so a conflict re-reads fresh state. Both
    // writes are built under the catalog schema, so an in-transaction DELETE
    // buffers in the layout an INSERT does.
    let count = commit_rmw_or_buffer(client, &table_name, tid, schema, |client| {
        let (mut rows, buffered) = resolve_where_matches(client, tid, schema, &spec, &reply_schema)?;
        Ok(match &mut set {
            Some(set) => {
                rows.extend_from_owned(buffered);
                apply_set(set, rows, None, schema)?
            }
            None => {
                let mut out = retraction_batch(schema, rows.pks);
                out.extend_from_owned(retraction_batch(schema, buffered.pks));
                out
            }
        })
    })?;
    Ok(SqlResult::RowsAffected { count })
}

// ---------------------------------------------------------------------------
// The SET list (shared with INSERT's ON CONFLICT DO UPDATE)
// ---------------------------------------------------------------------------

/// Which row a SET right-hand side reads: the row being rewritten, or (ON
/// CONFLICT DO UPDATE) the incoming VALUES row that collided with it.
#[derive(Clone, Copy)]
pub(crate) enum Scope {
    Existing,
    Excluded,
}

pub(crate) struct SetCol {
    ci: usize,
    rhs: SetRhs,
}

pub(crate) enum SetRhs {
    /// A literal, encoded once by INSERT's cell encoder into one target cell (a
    /// string's spill in `spill`).
    Const { cell: Vec<u8>, spill: Vec<u8>, null: bool },
    /// Payload slot `src` of the scope's row, of exactly the target's column type.
    Copy { scope: Scope, src: usize },
    /// A computed value.
    Expr { scope: Scope, ev: Box<ScalarEval> },
}

/// Each target is one plain identifier naming a non-PK column not already
/// assigned; `rhs` compiles the value for column `ci`.
pub(crate) fn bind_set_list(
    raw: &[Assignment],
    schema: &Schema,
    clause: &str,
    mut rhs: impl FnMut(&Expr, usize) -> Result<SetRhs, GnitzSqlError>,
) -> Result<Vec<SetCol>, GnitzSqlError> {
    let mut set: Vec<SetCol> = Vec::with_capacity(raw.len());
    for a in raw {
        let name = match &a.target {
            AssignmentTarget::ColumnName(name) => single_part_ident(name),
            _ => None,
        }
        .ok_or_else(|| GnitzSqlError::Plan(format!("{clause}: column must be a simple identifier")))?;
        let ci = find_unique_column(&schema.columns, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found in {clause}")))?;
        if schema.is_pk_col(ci) {
            return Err(GnitzSqlError::Unsupported(format!(
                "cannot assign to primary key column in {clause}"
            )));
        }
        if set.iter().any(|s| s.ci == ci) {
            return Err(GnitzSqlError::Bind(format!(
                "multiple assignments to column '{name}' in {clause}"
            )));
        }
        set.push(SetCol { ci, rhs: rhs(&a.value, ci)? });
    }
    Ok(set)
}

/// Compile one SET right-hand side for column `target`, against the schema the
/// rows it reads carry. Every kind rejection happens here, before any row is
/// read; a computed value's range and a NULL into a NOT NULL column are the
/// verdicts [`apply_set`] takes per row.
pub(crate) fn classify_set_rhs(
    expr: &BoundExpr,
    scope: Scope,
    target: usize,
    schema: &Schema,
) -> Result<SetRhs, GnitzSqlError> {
    let col = &schema.columns[target];
    let ty = col.ty;
    if expr.is_literal() {
        let (mut cell, mut spill) = (Vec::new(), Vec::new());
        append_value_to_col(&mut cell, &mut spill, col, expr)?;
        return Ok(SetRhs::Const {
            cell,
            spill,
            null: matches!(expr, BoundExpr::LitNull),
        });
    }
    let src = expr.infer_ty(&schema.columns);
    if let BoundExpr::ColRef(c) = expr {
        if let (true, Some(slot)) = (src == ty, schema.payload_slot(*c)) {
            return Ok(SetRhs::Copy { scope, src: slot });
        }
    }
    // A DECIMAL source into a non-DECIMAL target casts to I64, not the target, so
    // a narrow target's range is checked per row rather than NULLed by the cast.
    let cast_to = if ty.is_decimal() {
        (src != ty).then_some(ty)
    } else if src.is_decimal() {
        Some(ColType::of(TypeCode::I64))
    } else if src.tc.is_temporal() && ty.tc.is_temporal() && src.tc != ty.tc {
        Some(ty)
    } else {
        None
    };
    let expr = match cast_to {
        Some(to) => BoundExpr::Cast { expr: Box::new(expr.clone()), to },
        // An f64 register has no computed destination: nothing downstream can
        // tell its bit pattern from an integer's.
        None if src.tc.is_float() => {
            return Err(GnitzSqlError::Unsupported(
                "SET from a floating-point expression is not supported".to_string(),
            ))
        }
        None => expr.clone(),
    };
    let ev = compile_scalar_evaluator(&expr, schema)?;
    let str_valued = ev.result_is_str();
    let admits = if str_valued {
        col.ty.tc == TypeCode::String
    } else {
        FixedInt::from_type_code(col.ty.tc).is_some()
    };
    if !admits {
        return Err(GnitzSqlError::Bind(format!(
            "cannot assign {} value to column '{}' ({:?})",
            if str_valued { "a string" } else { "an integer" },
            col.name,
            col.ty.tc,
        )));
    }
    Ok(SetRhs::Expr { scope, ev: Box::new(ev) })
}

/// `rows` with every assigned column rewritten and every row at weight +1. Every
/// right-hand side reads the rows as they were. `excluded` holds the incoming
/// VALUES rows aligned with `rows` (ON CONFLICT DO UPDATE only).
///
/// Rewritten columns are built in `new` and swapped in. Assigning a string column
/// also rebuilds every other string column into `new`, whose arena then replaces
/// `rows`'; otherwise nothing is encoded and `rows`' arena is kept.
pub(crate) fn apply_set(
    set: &mut [SetCol],
    mut rows: ZSetBatch,
    excluded: Option<&ZSetBatch>,
    schema: &Schema,
) -> Result<ZSetBatch, GnitzSqlError> {
    let n = rows.len();
    let rebuild = set.iter().any(|a| (schema.columns[a.ci].ty.tc).is_german_string());
    let mut new = ZSetBatch::new(schema);
    let mut nulls = rows.nulls.clone();
    for a in set.iter_mut() {
        let def = &schema.columns[a.ci];
        let pi = schema.payload_slot(a.ci).expect("a SET target is a payload column");
        let tc = def.ty.tc;
        let scoped = |s: Scope| match s {
            Scope::Existing => &rows,
            Scope::Excluded => excluded.expect("only ON CONFLICT DO UPDATE binds EXCLUDED"),
        };
        new.payload[pi].bytes.reserve_exact(n * tc.wire_stride());
        match &mut a.rhs {
            SetRhs::Const { cell, spill, null } => {
                let ZSetBatch { payload, blob, .. } = &mut new;
                // One spill for every row's cell.
                let cell = if tc.is_german_string() {
                    encode_german_string(german_string_content(cell, spill), blob).to_vec()
                } else {
                    cell.clone()
                };
                for w in &mut nulls {
                    set_null(w, pi, *null, def)?;
                    payload[pi].bytes.extend_from_slice(&cell);
                }
            }
            SetRhs::Copy { scope, src } => {
                let b = scoped(*scope);
                for (r, w) in nulls.iter_mut().enumerate() {
                    set_null(w, pi, null_word_get(b.nulls[r], *src), def)?;
                    new.push_cell_from(pi, b, *src, r);
                }
            }
            SetRhs::Expr { scope, ev } => {
                let ZSetBatch { payload, blob, .. } = &mut new;
                match ev.eval_all(scoped(*scope)) {
                    ExprResults::Int(vals) => {
                        let fi = FixedInt::from_type_code(tc)
                            .expect("classify_set_rhs admits a scalar only into a FixedInt column");
                        let (min, max) = fi.range();
                        for (w, v) in nulls.iter_mut().zip(vals) {
                            set_null(w, pi, v.is_none(), def)?;
                            let v = v.unwrap_or(0);
                            if !(min..=max).contains(&v) {
                                return Err(GnitzSqlError::Bind(format!("{tc:?} value out of range: {v}")));
                            }
                            payload[pi]
                                .bytes
                                .extend_from_slice(&fi.pack(v).to_le_bytes()[..fi.width()]);
                        }
                    }
                    ExprResults::Str { bytes, spans } => {
                        for (w, span) in nulls.iter_mut().zip(spans) {
                            set_null(w, pi, span.is_none(), def)?;
                            let content = span.map_or(&[][..], |(o, l)| &bytes[o..o + l]);
                            payload[pi]
                                .bytes
                                .extend_from_slice(&encode_german_string(content, blob));
                        }
                    }
                }
            }
        }
    }
    if rebuild {
        for (pi, ci, def) in schema.payload_columns() {
            if def.ty.tc.is_german_string() && !set.iter().any(|a| a.ci == ci) {
                for r in 0..n {
                    new.push_cell_from(pi, &rows, pi, r);
                }
            }
        }
        rows.blob = std::mem::take(&mut new.blob);
    }
    for (pi, ci, def) in schema.payload_columns() {
        if set.iter().any(|a| a.ci == ci) || (rebuild && def.ty.tc.is_german_string()) {
            std::mem::swap(&mut rows.payload[pi], &mut new.payload[pi]);
        }
    }
    rows.nulls = nulls;
    rows.weights.fill(1);
    Ok(rows)
}

fn set_null(word: &mut u64, pi: usize, null: bool, def: &ColumnDef) -> Result<(), GnitzSqlError> {
    check_not_null(def, null)?;
    null_word_set(word, pi, null);
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/mutate.rs"]
mod tests;
