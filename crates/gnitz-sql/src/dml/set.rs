//! The SET list of UPDATE and of INSERT's `ON CONFLICT DO UPDATE`: [`bind_set_list`]
//! binds the targets, `classify_set_rhs` compiles each value, [`apply_set`]
//! rewrites a batch.

use super::colwrite::{check_not_null, write_value_cell};
use crate::ast_util::{col_ref_parts, expr_any, single_part_ident};
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_scalar_evaluator;
use crate::ir::{BoundExpr, RegClass};
use crate::rules::require_column;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::ScalarEval;
use gnitz_wire::{ColType, FixedInt, Heap, TypeCode, null_word_get, null_word_set, relocate_german_string};
use gnitz_zset::repr::StringDest;
use sqlparser::ast::{Assignment, AssignmentTarget, Expr};

/// Which row a SET right-hand side reads: the row being rewritten, or (ON
/// CONFLICT DO UPDATE) the incoming VALUES row that collided with it.
#[derive(Clone, Copy)]
enum Scope {
    Existing,
    Excluded,
}

pub(super) struct SetCol {
    ci: usize,
    rhs: SetRhs,
}

enum SetRhs {
    /// A literal, encoded once by INSERT's cell encoder into one target cell (a
    /// string's spill in `spill`).
    Const { cell: Vec<u8>, spill: Vec<u8>, null: bool },
    /// Payload slot `src` of the scope's row, of exactly the target's column type.
    Copy { scope: Scope, src: usize },
    /// A computed value.
    Expr { scope: Scope, ev: Box<ScalarEval> },
}

/// Which statement a SET list belongs to: what its error messages name, and
/// whether a value may read the incoming row as `EXCLUDED.<col>`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SetClause {
    Update,
    DoUpdate,
}

impl SetClause {
    fn name(self) -> &'static str {
        match self {
            SetClause::Update => "UPDATE SET",
            SetClause::DoUpdate => "ON CONFLICT DO UPDATE SET",
        }
    }
}

/// Each target is one plain identifier naming a non-PK column not already
/// assigned; each value is bound against `schema` under `alias`, the qualifier a
/// written column reference must name.
pub(super) fn bind_set_list(
    raw: &[Assignment],
    schema: &Schema,
    alias: &str,
    clause: SetClause,
) -> Result<Vec<SetCol>, GnitzSqlError> {
    let clause_name = clause.name();
    let mut set: Vec<SetCol> = Vec::with_capacity(raw.len());
    for a in raw {
        let name = match &a.target {
            AssignmentTarget::ColumnName(name) => single_part_ident(name),
            _ => None,
        }
        .ok_or_else(|| GnitzSqlError::rejected(format!("{clause_name}: column must be a simple identifier")))?;
        let ci = require_column(schema.columns(), name).map_err(|e| e.in_clause(clause_name))?;
        if schema.layout().is_pk_col(ci) {
            return Err(GnitzSqlError::rejected(format!(
                "cannot assign to primary key column in {clause_name}"
            )));
        }
        if set.iter().any(|s| s.ci == ci) {
            return Err(GnitzSqlError::rejected(format!(
                "multiple assignments to column '{name}' in {clause_name}"
            )));
        }
        let rhs = bind_set_rhs(&a.value, ci, schema, alias, clause)?;
        set.push(SetCol { ci, rhs });
    }
    Ok(set)
}

/// One SET right-hand side for column `target`. Under `DoUpdate` the incoming-row
/// scope is the pseudo-qualifier `EXCLUDED.<col>`; a bare column name, as under
/// `Update`, refers to the existing (stored) row.
fn bind_set_rhs(
    expr: &Expr,
    target: usize,
    schema: &Schema,
    alias: &str,
    clause: SetClause,
) -> Result<SetRhs, GnitzSqlError> {
    if clause == SetClause::DoUpdate {
        if let Some(col_name) = excluded_col(expr) {
            let col_idx = require_column(schema.columns(), col_name).map_err(|e| e.in_clause("EXCLUDED"))?;
            // Through the same classifier as a bare RHS, so `SET int_col =
            // EXCLUDED.str_col` is rejected here rather than per row.
            return classify_set_rhs(BoundExpr::ColRef(col_idx), Scope::Excluded, target, schema);
        }
        // `col + EXCLUDED.col`. The binder already rejects it (`EXCLUDED` names no
        // relation in scope); this says why, which its message cannot.
        if expr_any(expr, &|e| excluded_col(e).is_some()) {
            return Err(GnitzSqlError::rejected(
                "EXCLUDED column references inside compound expressions are not \
                 supported; use a simple `col = EXCLUDED.col` assignment",
            ));
        }
    }
    classify_set_rhs(bind_single_table(expr, schema, alias)?, Scope::Existing, target, schema)
}

/// The column an `EXCLUDED.<col>` reference names.
fn excluded_col(e: &Expr) -> Option<&str> {
    match col_ref_parts(e) {
        Some((Some(q), col)) if q.eq_ignore_ascii_case("EXCLUDED") => Some(col),
        _ => None,
    }
}

/// Compile one SET right-hand side for column `target`, against the schema the
/// rows it reads carry. Every kind rejection happens here, before any row is
/// read; a computed value's range and a NULL into a NOT NULL column are the
/// verdicts [`apply_set`] takes over the rows it rewrites.
fn classify_set_rhs(expr: BoundExpr, scope: Scope, target: usize, schema: &Schema) -> Result<SetRhs, GnitzSqlError> {
    let col = &schema.columns()[target];
    let ty = col.ty;
    if expr.is_literal() {
        let (mut cell, mut spill) = (vec![0; ty.tc.wire_stride()], Vec::new());
        write_value_cell(&mut cell, &mut spill, col, &expr)?;
        return Ok(SetRhs::Const {
            cell,
            spill,
            null: matches!(expr, BoundExpr::LitNull),
        });
    }
    let src = expr.infer_ty(schema.columns());
    if let BoundExpr::ColRef(c) = &expr
        && src == ty
        && let Some(slot) = schema.layout().payload_slot(*c)
    {
        return Ok(SetRhs::Copy { scope, src: slot });
    }
    // An f64 register has no computed destination but a DECIMAL's cast: nothing
    // downstream can tell its bit pattern from an integer's.
    let (from, to) = (RegClass::of(src), RegClass::of(ty));
    if from == RegClass::Float && !ty.is_decimal() {
        return Err(GnitzSqlError::rejected(
            "SET from a floating-point expression is not supported",
        ));
    }
    // A string and a BOOLEAN are assigned only to a column of their own class,
    // and a number to any column stored as an integer.
    let admits = match (from, to) {
        (RegClass::Str | RegClass::Bool, _) | (_, RegClass::Str | RegClass::Bool) => from == to,
        _ => FixedInt::from_type_code(ty.tc).is_some(),
    };
    if !admits {
        return Err(GnitzSqlError::rejected(format!(
            "cannot assign a value of type {src} to column '{}' ({ty})",
            col.name
        )));
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
        Some(to) => BoundExpr::Cast { expr: Box::new(expr), to },
        None => expr,
    };
    let ev = compile_scalar_evaluator(&expr, schema)?;
    Ok(SetRhs::Expr { scope, ev: Box::new(ev) })
}

/// `rows` with every assigned column rewritten and every row at weight +1. Every
/// right-hand side reads the rows as they were. `excluded` holds the incoming
/// VALUES rows aligned with `rows` (ON CONFLICT DO UPDATE only).
///
/// Each assigned column is written whole into a copy of `rows`, so no right-hand side reads
/// another's write. Assigning a string column compacts the copy's heap once the values it
/// replaced make it wasteful. A computed value out of its column's
/// range is refused as it is written; a NULL into a NOT NULL column once every column is.
pub(super) fn apply_set(
    set: &mut [SetCol],
    rows: ZSetBatch,
    excluded: Option<&ZSetBatch>,
) -> Result<ZSetBatch, GnitzSqlError> {
    let schema = rows.schema();
    let mut new = ZSetBatch::of(schema, rows.view().to_batch());
    for a in set.iter_mut() {
        let def = &schema.columns()[a.ci];
        let pi = schema
            .layout()
            .payload_slot(a.ci)
            .expect("a SET target is a payload column");
        let scoped = |s: Scope| match s {
            Scope::Existing => &rows,
            Scope::Excluded => excluded.expect("only ON CONFLICT DO UPDATE binds EXCLUDED"),
        };
        match &mut a.rhs {
            SetRhs::Const { cell, spill, null } => new.rewrite(|w| {
                if def.ty.tc.is_german_string() {
                    // One long value for every row's cell.
                    let (cells, heap, _) = w.string_col_mut(pi);
                    let cell =
                        relocate_german_string(cell.first_chunk().expect("a German cell is 16 bytes"), spill, heap);
                    cells.as_chunks_mut::<16>().0.fill(cell);
                } else {
                    gnitz_wire::by_cell_width!(cell.len(), |T| {
                        let cell: [u8; size_of::<T>()] = cell[..].try_into().expect("one cell");
                        w.col_mut(pi).as_chunks_mut::<{ size_of::<T>() }>().0.fill(cell);
                    });
                }
                set_null_bits(w.null_bmp_mut(), pi, |_| *null);
            }),
            SetRhs::Copy { scope, src } => {
                let b = scoped(*scope).view();
                new.rewrite(|w| {
                    w.col_mut(pi).copy_from_slice(b.col_data(*src));
                    // `new` began as a copy of `rows`, its heap at the same offsets, so a cell
                    // of `Existing` already points at its value; one of `Excluded` is moved.
                    if def.ty.tc.is_german_string() && matches!(scope, Scope::Excluded) {
                        w.rebase_string_col(pi, Heap::Plain(b.blob()), StringDest::Placed(None));
                    }
                    set_null_bits(w.null_bmp_mut(), pi, |r| null_word_get(b.get_null_word(r), *src));
                });
            }
            SetRhs::Expr { scope, ev } => new.rewrite(|w| {
                w.col_mut(pi).fill(0);
                set_null_bits(w.null_bmp_mut(), pi, |_| false);
                ev.write_slot(&scoped(*scope).view(), w, pi, def.ty.tc).map_err(|v| {
                    GnitzSqlError::rejected(format!("column '{}': {} value out of range: {v}", def.name, def.ty))
                })
            })?,
        }
    }
    // Bit `pi` set where some rewritten row is NULL in column `pi`.
    let any_null = new.view().null_words().fold(0, |acc, w| acc | w);
    for (pi, ci, def) in schema.payload_columns() {
        if set.iter().any(|a| a.ci == ci) {
            check_not_null(def, null_word_get(any_null, pi))?;
        }
    }
    if set.iter().any(|a| (schema.columns()[a.ci].ty.tc).is_german_string()) {
        new = ZSetBatch::of(schema, new.into_batch().trimmed());
    }
    new.fill_weights(1);
    Ok(new)
}

/// Null bit `pi` of each row's word in `nulls` set to `null` of the row.
fn set_null_bits(nulls: &mut [u8], pi: usize, null: impl Fn(usize) -> bool) {
    for (r, word) in nulls.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        let mut w = u64::from_le_bytes(*word);
        null_word_set(&mut w, pi, null(r));
        *word = w.to_le_bytes();
    }
}

#[cfg(test)]
#[path = "benches/set.rs"]
mod bench;
