//! UPDATE and DELETE, as one read-then-write flow: plan the WHERE through the
//! shared access-path ladder (`dml::plan`), then, as one read-modify-write, read
//! the rows it matches and write the rewritten rows (UPDATE) or the retraction of
//! their keys (DELETE). Planning reads only the catalog, so a refused statement
//! issues no request.
//!
//! The SET list — [`bind_set_list`] binds the targets, [`classify_set_rhs`]
//! compiles each value, [`apply_set`] rewrites a batch — is shared with INSERT's
//! `ON CONFLICT DO UPDATE`.

use crate::ast_util::{
    classify_from, col_ref_parts, expr_any, extract_table_name_and_alias, single_part_ident, FromShape,
};
use crate::bind::{bind_single_table, require_column, Catalog};
use crate::codec::colwrite::{append_value_to_col, check_not_null};
use crate::dml::plan::access_path;
use crate::error::{reject_if, GnitzSqlError};
use crate::expr_lower::compile_scalar_evaluator;
use crate::ir::{BoundExpr, RegClass};
use crate::rules::{require_class, ClassWant};
use crate::SqlResult;
use gnitz_core::{retraction_batch, GnitzClient, RelDescriptor, Schema, ZSetBatch};
use gnitz_expr::{ExprResults, ScalarEval, SchemaFacts};
use gnitz_wire::{encode_german_string, null_word_get, null_word_set, relocate_german_string};
use gnitz_wire::{ColType, ColumnDef, FixedInt, ReadBound, TypeCode};
use sqlparser::ast::{Assignment, AssignmentTarget, Delete, Expr, FromTable, TableWithJoins, Update};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// UPDATE / DELETE
// ---------------------------------------------------------------------------

/// A single-table UPDATE (`set` present) or DELETE, planned: the read that
/// finds its rows and what to write for them.
pub(crate) struct MutationPlan {
    target: Arc<RelDescriptor>,
    bound: ReadBound,
    predicate: Vec<u8>,
    set: Option<Vec<SetCol>>,
}

pub(crate) fn plan_update(update: &Update, cat: &dyn Catalog) -> Result<MutationPlan, GnitzSqlError> {
    reject_unhonored_update_clauses(update)?;
    plan_mutation(
        std::slice::from_ref(&update.table),
        update.selection.as_ref(),
        Some(&update.assignments),
        cat,
    )
}

pub(crate) fn plan_delete(del: &Delete, cat: &dyn Catalog) -> Result<MutationPlan, GnitzSqlError> {
    reject_unhonored_delete_clauses(del)?;
    let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = &del.from;
    plan_mutation(from, del.selection.as_ref(), None, cat)
}

/// Reject every `UPDATE` clause `plan_mutation` does not consume. It reads `table`, `assignments`,
/// `selection`; `from` (UPDATE … FROM join-update), `returning`, and `or` (SQLite conflict) all parse
/// under `GenericDialect` and were dropped — the join-update silently binds SET/WHERE against the
/// wrong relation set.
fn reject_unhonored_update_clauses(update: &sqlparser::ast::Update) -> Result<(), GnitzSqlError> {
    const CTX: &str = "UPDATE";
    let sqlparser::ast::Update {
        // Consumed by `plan_mutation` — `table` whole: it classifies the FROM
        // shape, so a join written there is rejected rather than dropped.
        table: _,
        assignments: _,
        selection: _,
        // Inert: the `UPDATE` token and advisory-only comment hints.
        update_token: _,
        optimizer_hints: _,
        // Rejected: each is a clause `plan_mutation` does not implement.
        from,
        returning,
        output,
        or,
        order_by,
        limit,
    } = update;
    reject_if(from.is_some(), CTX, "FROM (join-update)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(or.is_some(), CTX, "OR (conflict clause)")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
    Ok(())
}

/// Reject every `DELETE` clause `plan_mutation` does not consume. It reads `from` and `selection`;
/// `tables` (multi-table), `using` (join-delete), `returning`, `order_by`, and `limit` all parse
/// under `GenericDialect` and were dropped — a dropped `LIMIT` deletes every matched row (data loss),
/// a dropped `USING` binds WHERE against the wrong relation set.
fn reject_unhonored_delete_clauses(del: &sqlparser::ast::Delete) -> Result<(), GnitzSqlError> {
    const CTX: &str = "DELETE";
    let sqlparser::ast::Delete {
        from: _,
        selection: _,
        // Inert: the `DELETE` token and advisory-only comment hints.
        delete_token: _,
        optimizer_hints: _,
        tables,
        using,
        returning,
        output,
        order_by,
        limit,
    } = del;
    reject_if(!tables.is_empty(), CTX, "multi-table delete")?;
    reject_if(using.is_some(), CTX, "USING (join-delete)")?;
    reject_if(returning.is_some(), CTX, "RETURNING")?;
    reject_if(output.is_some(), CTX, "OUTPUT")?;
    reject_if(!order_by.is_empty(), CTX, "ORDER BY")?;
    reject_if(limit.is_some(), CTX, "LIMIT")?;
    Ok(())
}

fn plan_mutation(
    from: &[TableWithJoins],
    selection: Option<&Expr>,
    set: Option<&[Assignment]>,
    cat: &dyn Catalog,
) -> Result<MutationPlan, GnitzSqlError> {
    let verb = if set.is_some() { "UPDATE" } else { "DELETE" };
    // `UPDATE a JOIN b ON … SET v = 1` parses; honoring only the relation would
    // update all of `a`.
    let FromShape::SinglePlainRelation(factor) = classify_from(from) else {
        return Err(GnitzSqlError::Rejected(format!(
            "{verb}: exactly one simple FROM table required"
        )));
    };
    let (table_name, alias) = extract_table_name_and_alias(factor, cat.schema_name(), verb)?;
    let target = cat.probe_relation(&table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTable, verb)?;
    let schema = &target.schema;
    let set = set
        .map(|raw| bind_set_list(raw, schema, &alias, SetClause::Update))
        .transpose()?;
    let (bound, predicate) = access_path(schema, &alias, selection, &target.indexes)?;
    Ok(MutationPlan { target, bound, predicate, set })
}

/// Read the rows the WHERE matches, then write [`delta`] of them.
pub(crate) async fn execute_mutation(client: &mut GnitzClient, plan: MutationPlan) -> Result<SqlResult, GnitzSqlError> {
    let MutationPlan { target, bound, predicate, mut set } = plan;
    let keys_only = set.is_none();
    let count = client
        .read_modify_write(&target, bound, predicate, keys_only, |rows| {
            delta(set.as_deref_mut(), rows, &target.schema)
        })
        .await?;
    Ok(SqlResult::RowsAffected { count })
}

/// What a mutation writes for the `rows` it read: under a SET list the rewritten
/// rows (UPDATE), without one the retraction of their keys (DELETE).
fn delta(set: Option<&mut [SetCol]>, rows: ZSetBatch, schema: &Schema) -> Result<ZSetBatch, GnitzSqlError> {
    match set {
        Some(set) => apply_set(set, rows, None, schema),
        None => Ok(retraction_batch(schema, rows.pks)),
    }
}

// ---------------------------------------------------------------------------
// The SET list (shared with INSERT's ON CONFLICT DO UPDATE)
// ---------------------------------------------------------------------------

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
        .ok_or_else(|| GnitzSqlError::Rejected(format!("{clause_name}: column must be a simple identifier")))?;
        let ci = require_column(&schema.columns, name).map_err(|e| e.in_clause(clause_name))?;
        if schema.is_pk_col(ci) {
            return Err(GnitzSqlError::Rejected(format!(
                "cannot assign to primary key column in {clause_name}"
            )));
        }
        if set.iter().any(|s| s.ci == ci) {
            return Err(GnitzSqlError::Rejected(format!(
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
            let col_idx = require_column(&schema.columns, col_name).map_err(|e| e.in_clause("EXCLUDED"))?;
            // Through the same classifier as a bare RHS, so `SET int_col =
            // EXCLUDED.str_col` is rejected here rather than per row.
            return classify_set_rhs(&BoundExpr::ColRef(col_idx), Scope::Excluded, target, schema);
        }
        // `col + EXCLUDED.col`. The binder already rejects it (`EXCLUDED` names no
        // relation in scope); this says why, which its message cannot.
        if expr_any(expr, &|e| excluded_col(e).is_some()) {
            return Err(GnitzSqlError::Rejected(
                "EXCLUDED column references inside compound expressions are not \
                 supported; use a simple `col = EXCLUDED.col` assignment"
                    .to_string(),
            ));
        }
    }
    classify_set_rhs(
        &bind_single_table(expr, schema, alias)?,
        Scope::Existing,
        target,
        schema,
    )
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
/// verdicts [`apply_set`] takes per row.
fn classify_set_rhs(expr: &BoundExpr, scope: Scope, target: usize, schema: &Schema) -> Result<SetRhs, GnitzSqlError> {
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
    // An f64 register has no computed destination but a DECIMAL's cast: nothing
    // downstream can tell its bit pattern from an integer's.
    let (from, to) = (RegClass::of(src), RegClass::of(ty));
    if from == RegClass::Float && !ty.is_decimal() {
        return Err(GnitzSqlError::Rejected(
            "SET from a floating-point expression is not supported".to_string(),
        ));
    }
    // A string and a BOOLEAN are assigned only to a column of their own class,
    // and a number to any column stored as an integer.
    let admits = match (from, to) {
        (RegClass::Str | RegClass::Bool, _) | (_, RegClass::Str | RegClass::Bool) => from == to,
        _ => FixedInt::from_type_code(ty.tc).is_some(),
    };
    if !admits {
        return Err(GnitzSqlError::Rejected(format!(
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
        Some(to) => BoundExpr::Cast { expr: Box::new(expr.clone()), to },
        None => expr.clone(),
    };
    let ev = compile_scalar_evaluator(&expr, schema)?;
    Ok(SetRhs::Expr { scope, ev: Box::new(ev) })
}

/// `rows` with every assigned column rewritten and every row at weight +1. Every
/// right-hand side reads the rows as they were. `excluded` holds the incoming
/// VALUES rows aligned with `rows` (ON CONFLICT DO UPDATE only).
///
/// Rewritten columns are built in `new` and swapped in. Assigning a string column
/// also rebuilds every other string column into `new`, whose arena then replaces
/// `rows`'; otherwise nothing is encoded and `rows`' arena is kept.
pub(super) fn apply_set(
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
                    relocate_german_string(cell.first_chunk().expect("a German cell is 16 bytes"), spill, blob).to_vec()
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
                                return Err(GnitzSqlError::Rejected(format!(
                                    "column '{}': {} value out of range: {v}",
                                    def.name, def.ty
                                )));
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
