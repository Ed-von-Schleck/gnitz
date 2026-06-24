//! INSERT, including the `ON CONFLICT` upsert family. The default form pushes
//! with `WireConflictMode::Error`; `DO NOTHING` / `DO UPDATE` are resolved
//! client-side (seek existing PKs, then filter or merge) before a single push.
//! The SET-list binding and evaluation reuse `mutate`'s shared helpers so a
//! `DO UPDATE SET` assignment behaves exactly like an `UPDATE ... SET`.

use crate::ast_util::extract_name;
use crate::bind::{bind_single_table, Binder};
use crate::codec::colwrite::{append_value_to_col, ColumnValue};
use crate::codec::nullmap::null_word_set;
use crate::codec::pk_codec::{extract_pk_value, is_null_expr};
use crate::dml::mutate::{build_merged_row, eval_set_expr, resolve_set_target};
use crate::error::GnitzSqlError;
use crate::exec::batch::copy_batch_row;
use crate::ir::BoundExpr;
use crate::SqlResult;
use gnitz_core::{GnitzClient, PkTuple, Schema, WireConflictMode, ZSetBatch};
use sqlparser::ast::{
    Assignment, ConflictTarget, Expr, Ident, OnConflict, OnConflictAction, OnInsert, Query, SetExpr, Statement,
    TableObject, Values,
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

/// Assignment RHS for `ON CONFLICT DO UPDATE`. Each variant wraps a
/// `BoundExpr` and a scope — Existing evaluates against the stored
/// row, Excluded against the incoming batch row. `EXCLUDED.col` is
/// the only construct that escapes the existing-row scope.
enum BoundUpdateExpr {
    Existing(BoundExpr),
    Excluded(BoundExpr),
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
            // compound-PK planner gate. Reject before any
            // `pk_index_single()` access so the assert turns into a
            // clean SQL error.
            if schema.pk_cols.len() >= 2 {
                return Err(GnitzSqlError::Unsupported(
                    "ON CONFLICT with target columns is not supported on \
                     compound-PK tables"
                        .to_string(),
                ));
            }
            if cols.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "composite ON CONFLICT targets not supported; \
                     single-column PK target only"
                        .to_string(),
                ));
            }
            let col_name = cols[0].value.as_str();
            let pk_name = schema.columns[schema.pk_index_single()].name.as_str();
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
    _schema_name: &str,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Extract table name, row source, and ON CONFLICT action.
    let (table_name_str, rows, columns, on_insert) = extract_insert_parts(stmt)?;

    let (tid, schema) = binder.resolve_base_table(client, &table_name_str)?;

    // INSERT is positional; reject any column list that isn't the full set in
    // schema order (it would otherwise silently misplace values).
    validate_insert_column_list(columns, &schema)?;

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
        Some(OnInsert::OnConflict(OnConflict {
            conflict_target,
            action,
        })) => {
            validate_conflict_target(conflict_target, &schema)?;

            match action {
                OnConflictAction::DoNothing => ConflictPlan::DoNothingPk,
                OnConflictAction::DoUpdate(do_update) => {
                    if do_update.selection.is_some() {
                        return Err(GnitzSqlError::Unsupported(
                            "ON CONFLICT ... DO UPDATE WHERE not supported".to_string(),
                        ));
                    }
                    let assignments = bind_do_update_assignments(&do_update.assignments, &schema)?;
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

    // Build the incoming batch from VALUES rows.
    let mut batch = ZSetBatch::new(&schema);
    let n = rows.len();

    for row in rows {
        // Standard SQL rejects a VALUES row whose arity differs from the column
        // count: too few values, or excess trailing values that were previously
        // discarded silently. INSERT here is full-row positional (any explicit
        // column list was already validated as the full set in schema order),
        // so this guard makes every per-column index below in-bounds.
        if row.len() != schema.columns.len() {
            return Err(GnitzSqlError::Bind(format!(
                "INSERT specifies {} value(s) but table '{}' has {} column(s)",
                row.len(),
                table_name_str,
                schema.columns.len()
            )));
        }
        let pk = extract_pk_value(row, &schema)?;
        batch.pks.push_tuple(&pk);
        batch.weights.push(1);

        let mut null_bits: u64 = 0;
        for (payload_idx, ci, col_def) in schema.payload_columns() {
            let val_expr = &row[ci];
            // Check if this value is NULL and set the null bitmap
            if is_null_expr(val_expr) {
                null_word_set(&mut null_bits, payload_idx, true);
            }
            append_value_to_col(&mut batch.columns[ci], col_def.type_code, val_expr)?;
        }
        batch.nulls.push(null_bits);
    }

    match plan {
        ConflictPlan::Error => {
            // SQL-standard INSERT: server rejects on any PK conflict.
            client.push_with_mode(tid, &schema, &batch, WireConflictMode::Error)?;
            Ok(SqlResult::RowsAffected { count: n })
        }
        ConflictPlan::DoNothingPk => {
            // Client-side filter: drop any row whose PK already exists
            // in the store. De-duplicate intra-batch (first-wins) before
            // pushing.
            let (filtered, surviving_count) = client_side_filter_do_nothing(client, tid, &schema, &batch)?;
            if surviving_count > 0 {
                client.push_with_mode(tid, &schema, &filtered, WireConflictMode::Error)?;
            }
            Ok(SqlResult::RowsAffected { count: surviving_count })
        }
        ConflictPlan::DoUpdatePk { assignments } => {
            let merged = client_side_merge_do_update(client, tid, &schema, &batch, &assignments)?;
            if !merged.pks.is_empty() {
                // Use Update mode: merged batch contains both +1 (new
                // merged rows, which may UPSERT) and untouched +1 rows
                // for non-conflicting inserts. Workers handle the
                // retract-and-insert via enforce_unique_pk.
                client.push_with_mode(tid, &schema, &merged, WireConflictMode::Update)?;
            }
            Ok(SqlResult::RowsAffected { count: n })
        }
    }
}

/// Bind `col = expr` assignments for ON CONFLICT DO UPDATE. The
/// incoming-row scope uses the pseudo-qualifier `EXCLUDED.<col>`; bare
/// column names refer to the existing (stored) row.
fn bind_do_update_assignments(
    raw: &[Assignment],
    schema: &Schema,
) -> Result<Vec<(usize, BoundUpdateExpr)>, GnitzSqlError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen: Vec<usize> = Vec::with_capacity(raw.len());
    for assignment in raw {
        let col_idx = resolve_set_target(assignment, schema, &mut seen, "ON CONFLICT DO UPDATE SET")?;
        // Recognize EXCLUDED.col as a special form. sqlparser parses it
        // as a CompoundIdentifier: `EXCLUDED`.`col`.
        let value = bind_do_update_rhs(&assignment.value, schema)?;
        out.push((col_idx, value));
    }
    Ok(out)
}

fn bind_do_update_rhs(expr: &Expr, schema: &Schema) -> Result<BoundUpdateExpr, GnitzSqlError> {
    // `EXCLUDED.col` — sqlparser produces `CompoundIdentifier`.
    if let Expr::CompoundIdentifier(parts) = expr {
        if parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("EXCLUDED") {
            let col_name = parts[1].value.as_str();
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| GnitzSqlError::Bind(format!("EXCLUDED.{col_name}: column not found")))?;
            return Ok(BoundUpdateExpr::Excluded(BoundExpr::ColRef(col_idx)));
        }
    }
    // Reject expressions that embed EXCLUDED references inside compound
    // expressions (e.g. `col + EXCLUDED.col`): the standard binder strips
    // table qualifiers, so it would silently bind EXCLUDED.col to the
    // existing row's col, producing wrong results.
    if expr_contains_excluded(expr) {
        return Err(GnitzSqlError::Unsupported(
            "EXCLUDED column references inside compound expressions are not \
             supported; use a simple `col = EXCLUDED.col` assignment"
                .to_string(),
        ));
    }
    let bound = bind_single_table(expr, schema)?;
    Ok(BoundUpdateExpr::Existing(bound))
}

/// Returns true if `expr` contains any `EXCLUDED.<col>` compound identifier.
fn expr_contains_excluded(expr: &Expr) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) => parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("EXCLUDED"),
        Expr::BinaryOp { left, right, .. } => expr_contains_excluded(left) || expr_contains_excluded(right),
        Expr::UnaryOp { expr: inner, .. } => expr_contains_excluded(inner),
        Expr::Nested(inner) => expr_contains_excluded(inner),
        _ => false,
    }
}

/// Drop incoming rows whose PK already exists in the store. Returns
/// the filtered ZSetBatch plus the surviving-row count.
///
/// Intra-batch duplicate PKs keep only the first occurrence.
fn client_side_filter_do_nothing(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    batch: &ZSetBatch,
) -> Result<(ZSetBatch, usize), GnitzSqlError> {
    let stride = schema.pk_stride() as u8;
    let mut seen_pks: std::collections::HashSet<PkTuple> = std::collections::HashSet::new();
    let mut surviving_indices: Vec<usize> = Vec::with_capacity(batch.pks.len());

    for i in 0..batch.pks.len() {
        let pk = batch.pks.get_tuple(i, stride);
        // Intra-batch duplicate: drop everything after the first.
        if !seen_pks.insert(pk) {
            continue;
        }
        // Existing-store check.
        let (_sch, found, _lsn) = client.seek(tid, &pk)?;
        let exists = matches!(found, Some(b) if !b.pks.is_empty());
        if exists {
            continue;
        }
        surviving_indices.push(i);
    }

    let mut out = ZSetBatch::new(schema);
    for &i in &surviving_indices {
        copy_batch_row(batch, i, &mut out, schema);
    }
    let n = surviving_indices.len();
    Ok((out, n))
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
    schema: &Schema,
    batch: &ZSetBatch,
    assignments: &[(usize, BoundUpdateExpr)],
) -> Result<ZSetBatch, GnitzSqlError> {
    // Pre-index assignments by column for O(cols) lookup per row.
    let mut asn_by_col: Vec<Option<&BoundUpdateExpr>> = vec![None; schema.columns.len()];
    for (ci, rhs) in assignments {
        asn_by_col[*ci] = Some(rhs);
    }

    let stride = schema.pk_stride() as u8;
    let mut seen_pks: std::collections::HashSet<PkTuple> = std::collections::HashSet::new();
    let mut out = ZSetBatch::new(schema);

    for i in 0..batch.pks.len() {
        let pk = batch.pks.get_tuple(i, stride);
        if !seen_pks.insert(pk) {
            return Err(GnitzSqlError::Bind(
                "ON CONFLICT DO UPDATE cannot affect row a second time \
                 (duplicate PK in the same batch)"
                    .to_string(),
            ));
        }

        let (_sch, existing_opt, _lsn) = client.seek(tid, &pk)?;
        let existing = existing_opt.filter(|b| !b.pks.is_empty());

        match existing {
            None => {
                copy_batch_row(batch, i, &mut out, schema);
            }
            Some(existing_batch) => {
                build_merged_row(batch, i, &existing_batch, 0, schema, &mut out, |ci| {
                    asn_by_col[ci]
                        .map(|rhs| eval_do_update_rhs(rhs, &existing_batch, batch, i, schema))
                        .transpose()
                })?;
            }
        }
    }
    Ok(out)
}

fn eval_do_update_rhs(
    rhs: &BoundUpdateExpr,
    existing: &ZSetBatch,
    excluded: &ZSetBatch,
    excluded_idx: usize,
    schema: &Schema,
) -> Result<ColumnValue, GnitzSqlError> {
    match rhs {
        BoundUpdateExpr::Existing(expr) => eval_set_expr(expr, existing, 0, schema),
        BoundUpdateExpr::Excluded(expr) => eval_set_expr(expr, excluded, excluded_idx, schema),
    }
}

/// INSERT writes VALUES positionally into `schema.payload_columns()` in schema
/// order, so an explicit column list is correct only when it names every column
/// once, in schema order. A reordered or partial list would silently misplace
/// values — reject it. (Full column-list remapping is a separate feature.)
fn validate_insert_column_list(columns: &[Ident], schema: &Schema) -> Result<(), GnitzSqlError> {
    if columns.is_empty() {
        return Ok(());
    }
    let in_schema_order = columns.len() == schema.columns.len()
        && columns
            .iter()
            .zip(&schema.columns)
            .all(|(c, sc)| c.value.eq_ignore_ascii_case(&sc.name));
    if !in_schema_order {
        return Err(GnitzSqlError::Unsupported(
            "INSERT with an explicit column list is only supported when it lists all \
             columns in schema order; reordered or partial column lists are not supported"
                .to_string(),
        ));
    }
    Ok(())
}

/// `(table_name, rows, columns, on_clause)` — return type of [`extract_insert_parts`].
/// `columns` is the explicit INSERT column list (empty when omitted).
type InsertParts<'a> = (String, &'a [Vec<Expr>], &'a [Ident], Option<&'a OnInsert>);

fn extract_insert_parts(stmt: &Statement) -> Result<InsertParts<'_>, GnitzSqlError> {
    match stmt {
        Statement::Insert(insert) => {
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
            Ok((table_name, rows, insert.columns.as_slice(), insert.on.as_ref()))
        }
        _ => Err(GnitzSqlError::Bind("not an INSERT statement".to_string())),
    }
}

fn extract_values_rows(query: &Query) -> Result<&[Vec<Expr>], GnitzSqlError> {
    match query.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => Ok(rows),
        _ => Err(GnitzSqlError::Unsupported(
            "INSERT only supports VALUES (not INSERT INTO ... SELECT)".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::two_col;
    use gnitz_core::TypeCode;

    // `two_col` has columns ["pk", "val"] in schema order.
    fn idents(names: &[&str]) -> Vec<Ident> {
        names.iter().map(|n| Ident::new(*n)).collect()
    }

    #[test]
    fn insert_no_column_list_is_ok() {
        assert!(validate_insert_column_list(&[], &two_col(TypeCode::I64)).is_ok());
    }

    #[test]
    fn insert_in_order_full_list_is_ok() {
        // Full set, schema order, case-insensitive → accepted.
        assert!(validate_insert_column_list(&idents(&["pk", "VAL"]), &two_col(TypeCode::I64)).is_ok());
    }

    #[test]
    fn insert_reordered_list_is_rejected() {
        let err = validate_insert_column_list(&idents(&["val", "pk"]), &two_col(TypeCode::I64)).unwrap_err();
        assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn insert_partial_list_is_rejected() {
        let err = validate_insert_column_list(&idents(&["pk"]), &two_col(TypeCode::I64)).unwrap_err();
        assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn insert_wrong_name_is_rejected() {
        let err = validate_insert_column_list(&idents(&["pk", "nope"]), &two_col(TypeCode::I64)).unwrap_err();
        assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got {err:?}");
    }
}
