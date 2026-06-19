use crate::ast_util::{extract_name, extract_table_factor_name};
use crate::bind::{find_unique_column, Binder};
use crate::codec::colwrite::{append_column_value, append_value_to_col, ColumnValue};
use crate::codec::nullmap::null_word_set;
use crate::codec::pk_codec::{
    extract_pk_value, extract_sql_literal, is_null_expr, parse_literal_i128, parse_pk_literal_packed, parse_uuid_str,
    SqlLiteral,
};
use crate::error::GnitzSqlError;
use crate::exec::batch::{apply_limit, apply_projection, copy_batch_row};
use crate::exec::eval::{eval_expr, eval_pred_row};
use crate::exec::residual::{bind_residuals, residual_filtered, row_passes_residuals};
use crate::ir::BoundExpr;
use crate::SqlResult;
use gnitz_core::GnitzClient;
use gnitz_core::{
    ClientError, ColData, Cut, FixedInt, PkColumn, PkTuple, RangeDescriptor, Schema, TypeCode, WireConflictMode,
    ZSetBatch,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ConflictTarget, Expr, FromTable, OnConflict, OnConflictAction,
    OnInsert, Query, SetExpr, Statement, TableObject, Value, Values,
};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

/// Resolved ON CONFLICT target for v1: either the PK column of the
/// table, or a reserved slot for future ON CONSTRAINT / unique secondary
/// targets (not implemented in v1). `None` = no target specified —
/// DO NOTHING with no target is treated as "any PK conflict".
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
    let (table_name_str, rows, on_insert) = extract_insert_parts(stmt)?;

    let (tid, schema) = binder.resolve_base_table(client, &table_name_str)?;

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
                            "ON CONFLICT ... DO UPDATE WHERE not supported in v1".to_string(),
                        ));
                    }
                    let assignments = bind_do_update_assignments(&do_update.assignments, &schema, binder)?;
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
        // discarded silently. INSERT here is full-row positional (no column
        // list), so this guard makes every per-column index below in-bounds.
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
    binder: &mut Binder<'_>,
) -> Result<Vec<(usize, BoundUpdateExpr)>, GnitzSqlError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen: Vec<usize> = Vec::with_capacity(raw.len());
    for assignment in raw {
        let col_idx = resolve_set_target(assignment, schema, &mut seen, "ON CONFLICT DO UPDATE SET")?;
        // Recognize EXCLUDED.col as a special form. sqlparser parses it
        // as a CompoundIdentifier: `EXCLUDED`.`col`.
        let value = bind_do_update_rhs(&assignment.value, schema, binder)?;
        out.push((col_idx, value));
    }
    Ok(out)
}

fn bind_do_update_rhs(expr: &Expr, schema: &Schema, binder: &mut Binder<'_>) -> Result<BoundUpdateExpr, GnitzSqlError> {
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
    let bound = binder.bind_expr(expr, schema)?;
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
                out.pks.push_from(&batch.pks, i);
                out.weights.push(1);

                // Start with the existing row's null bits; assignments
                // that resolve to NULL will flip their bit below.
                let mut null_bits: u64 = existing_batch.nulls[0];

                for (payload_idx, ci, col_def) in schema.payload_columns() {
                    if let Some(rhs) = asn_by_col[ci] {
                        let cv = eval_do_update_rhs(rhs, &existing_batch, batch, i, schema)?;
                        null_word_set(&mut null_bits, payload_idx, matches!(cv, ColumnValue::Null));
                        append_column_value(&mut out.columns[ci], cv, col_def.type_code)?;
                    } else {
                        let stride = col_def.type_code.wire_stride();
                        existing_batch.columns[ci].push_row_from(0, stride, &mut out.columns[ci]);
                    }
                }
                out.nulls.push(null_bits);
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

/// `(table_name, rows, on_clause)` — return type of [`extract_insert_parts`].
type InsertParts<'a> = (String, &'a [Vec<Expr>], Option<&'a OnInsert>);

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
            Ok((table_name, rows, insert.on.as_ref()))
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

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

pub(crate) fn execute_select(
    client: &mut GnitzClient,
    _schema_name: &str,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    if query.order_by.is_some() {
        return Err(GnitzSqlError::Unsupported("ORDER BY not supported".to_string()));
    }
    let limit = extract_limit(query);

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "only SELECT ... FROM is supported".to_string(),
            ))
        }
    };

    // Exactly one FROM table, no joins
    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "SELECT must have exactly one FROM table".to_string(),
        ));
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "JOINs not supported in direct SELECT".to_string(),
        ));
    }
    let table_name = extract_table_factor_name(&from.relation, "FROM")?;

    let (tid, schema) = binder.resolve(client, &table_name)?;

    // Check WHERE clause
    let (schema_out, batch_opt, _) = if let Some(where_expr) = &select.selection {
        if let Some((pk, residual)) = try_extract_pk_seek_residual(where_expr, &schema) {
            let res = client.seek(tid, &pk)?;
            residual_filtered(binder, &schema, res, &residual)?
        } else {
            // Order: PK equality (above) → range index → equality index → error.
            // Try RANGE candidates first so a composite `a = 5 AND b > 10` uses the
            // (a,b) range scan instead of a bare `a = 5` prefix seek + in-memory
            // `b > 10` residual. A range candidate is never less selective than the
            // equality prefix it extends (a full all-columns-equality match yields
            // no range column, hence no range candidate), so range-first is strict.
            let mut hit = None;

            // One epoch-validated GET_INDICES round-trip serves both collectors:
            // `table_indexes` always hits the wire (its epoch cache only skips
            // re-decoding), so the range→equality fall-through must not fetch
            // the same list twice within one statement.
            let mut idx_memo: Option<Arc<Vec<gnitz_core::IndexMeta>>> = None;
            let range_cands = collect_index_range_candidates(where_expr, &schema, || {
                let list = client.table_indexes(tid)?;
                idx_memo = Some(Arc::clone(&list));
                Ok(list)
            })
            .map_err(GnitzSqlError::Exec)?;
            for cand in range_cands {
                match client.seek_by_index_range(tid, cand.idx_cols.as_slice(), &cand.desc) {
                    Ok(res) => {
                        hit = Some(residual_filtered(binder, &schema, res, &cand.residual)?);
                        break;
                    }
                    Err(ClientError::NoIndex) => continue,
                    Err(e) => return Err(GnitzSqlError::Exec(e)),
                }
            }

            if hit.is_none() {
                let candidates = collect_index_seek_candidates(where_expr, &schema, || match idx_memo {
                    Some(list) => Ok(list),
                    None => client.table_indexes(tid),
                })
                .map_err(GnitzSqlError::Exec)?;
                for (col_indices, key_vals, residual) in candidates {
                    match client.seek_by_index(tid, col_indices.as_slice(), &key_vals) {
                        Ok(res) => {
                            hit = Some(residual_filtered(binder, &schema, res, &residual)?);
                            break;
                        }
                        Err(ClientError::NoIndex) => continue,
                        Err(e) => return Err(GnitzSqlError::Exec(e)),
                    }
                }
            }

            match hit {
                Some(res) => res,
                None => {
                    // No index served this WHERE. Bind it first so an ambiguous or
                    // unknown column raises its exact Bind error (a SELECT * join
                    // view can carry two same-named columns) instead of the generic
                    // "non-indexed" message. Direct SELECT — unlike UPDATE/DELETE —
                    // has no predicate full-scan fallback that would bind it.
                    binder.bind_expr(where_expr, &schema)?;
                    return Err(GnitzSqlError::Unsupported(
                        "WHERE on non-indexed column not supported in direct SELECT; \
                         use CREATE INDEX first, or CREATE VIEW for server-side filtering"
                            .to_string(),
                    ));
                }
            }
        }
    } else {
        client.scan(tid)?
    };

    // Use the resolved schema for column metadata
    let actual_schema = schema_out.as_deref().unwrap_or(&*schema);

    // Apply projection
    let (proj_schema, proj_batch) = apply_projection(&select.projection, actual_schema, batch_opt)?;

    // Apply LIMIT
    let final_batch = if let Some(lim) = limit {
        apply_limit(proj_batch, &proj_schema, lim)
    } else {
        proj_batch
    };

    Ok(SqlResult::Rows {
        schema: proj_schema,
        batch: final_batch,
    })
}

fn extract_limit(query: &Query) -> Option<usize> {
    use sqlparser::ast::LimitClause;
    match &query.limit_clause {
        Some(LimitClause::LimitOffset {
            limit: Some(Expr::Value(vws)),
            ..
        }) => {
            if let Value::Number(n, _) = &vws.value {
                n.parse::<usize>().ok()
            } else {
                None
            }
        }
        Some(LimitClause::OffsetCommaLimit {
            limit: Expr::Value(vws),
            ..
        }) => {
            if let Value::Number(n, _) = &vws.value {
                n.parse::<usize>().ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Flattens an `AND`-tree into its leaf conjuncts, left to right. Descends
/// through `AND` nesting and unwraps parenthesised `Nested` wrappers; any other
/// node (an equality, a range, an `OR`-group, …) is a leaf kept intact.
fn flatten_conjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
    match expr {
        Expr::Nested(inner) => flatten_conjuncts(inner, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            flatten_conjuncts(left, out);
            flatten_conjuncts(right, out);
        }
        _ => out.push(expr),
    }
}

/// `Some((pk_tuple, residual))` when the conjuncts of `expr` bind every PK
/// column to a literal; `residual` holds the leftover conjuncts (non-PK
/// equalities, ranges, an `OR`-group, a repeated PK column) to filter against
/// the seeked row, and is empty when the `WHERE` is exactly the PK equality.
/// `None` when the PK is not fully bound — the caller then tries a secondary
/// index. This is what lets `WHERE pk = 1 AND name = 'x'` take the PK point
/// lookup (residual `name = 'x'`) instead of degrading to a full scan.
fn try_extract_pk_seek_residual<'e>(expr: &'e Expr, schema: &Schema) -> Option<(PkTuple, Vec<&'e Expr>)> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    let stride = schema.pk_stride() as u8;
    let mut tuple = PkTuple::new(stride);
    let mut slot_set = [false; gnitz_core::MAX_PK_COLUMNS];
    let mut bound = 0usize;
    let mut residual = Vec::new();

    for &cand in &conjuncts {
        // Consume `pk_col = literal` for an as-yet-unbound PK slot into the
        // tuple; everything else (non-PK columns, non-equalities, a repeated PK
        // column) routes to the residual.
        let mut consumed = false;
        if let Some((col_idx, val)) = try_col_eq_literal(cand, schema) {
            if let Some(pk_pos) = schema.pk_indices().iter().position(|&pi| pi == col_idx) {
                if !slot_set[pk_pos] {
                    slot_set[pk_pos] = true;
                    bound += 1;
                    let off = schema.pk_byte_offset(col_idx);
                    let w = schema.columns[col_idx].type_code.wire_stride();
                    tuple.buf[off..off + w].copy_from_slice(&val.to_le_bytes()[..w]);
                    consumed = true;
                }
            }
        }
        if !consumed {
            residual.push(cand);
        }
    }

    (bound == schema.pk_count()).then_some((tuple, residual))
}

/// Extracts (col_idx, key) from `col = literal`. Does NOT check index existence.
pub(crate) fn try_col_eq_literal(expr: &Expr, schema: &Schema) -> Option<(usize, u128)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };
    // The column may sit on either side; the literal is whatever
    // `extract_sql_literal` accepts (numbers, optionally negated, or a
    // single-quoted string — but never a double-quoted identifier).
    let (col_id, lit) = match (left.as_ref(), right.as_ref()) {
        (Expr::Identifier(id), r) => (id, extract_sql_literal(r)?),
        (l, Expr::Identifier(id)) => (id, extract_sql_literal(l)?),
        _ => return None,
    };
    // Ambiguous (a dup-named `SELECT *` view) OR absent → None → fast path
    // declined; the WHERE then falls through to a bind that raises the precise
    // error rather than seeking the wrong column.
    let col_idx = find_unique_column(&schema.columns, &col_id.value).ok().flatten()?;
    let col_tc = schema.columns[col_idx].type_code;

    // UUID: accept a single-quoted UUID string or a non-negated numeric u128
    // (equivalent to the old `n.parse::<u128>()` via parse_pk_literal_packed).
    if col_tc == TypeCode::UUID {
        return match lit {
            SqlLiteral::Str(s) => parse_uuid_str(s).ok().map(|v| (col_idx, v)),
            SqlLiteral::Number(n, false) => parse_pk_literal_packed(TypeCode::UUID, n, false).map(|v| (col_idx, v)),
            _ => None,
        };
    }

    // Non-UUID: numeric literal only. Secondary indexes store column values as
    // zero-extended LE bytes cast to u64; for signed types we produce the same
    // bit pattern by casting through the type's unsigned equivalent. Delegated
    // to parse_pk_literal_packed so the three SEEK/INSERT parse sites cannot drift.
    let SqlLiteral::Number(n_str, negated) = lit else {
        return None;
    };
    let key = parse_pk_literal_packed(col_tc, n_str, negated)?;
    Some((col_idx, key))
}

/// One index-servable seek candidate: the index's FULL declared column list,
/// the covered leading key values, and the residual conjuncts to filter after
/// the seek.
type IndexSeekCandidate<'e> = (gnitz_core::PkColList, Vec<u128>, Vec<&'e Expr>);

/// Every index-servable seek candidate among the conjuncts of `expr` —
/// `(index column list, covered leading key values, residual conjuncts)` —
/// best candidate first. The caller attempts each candidate's `seek_by_index`
/// in turn and treats `ClientError::NoIndex` as "try the next candidate".
/// Matching every index against every `col = literal` conjunct lets
/// `WHERE unindexed = 1 AND indexed = 2` find the index whichever conjunct
/// carries it; flattening the whole `AND`-tree extends that to
/// three-or-more-way conjunctions. PK columns are skipped —
/// `try_extract_pk_seek_residual` handles them first — and so are column types
/// that can never carry a secondary index.
///
/// `fetch_indexes` (one epoch-validated GET_INDICES round-trip) is called only
/// when at least one eligible equality exists, so a WHERE that can never seek
/// (`x > 5`, an OR-tree, …) costs no wire traffic here.
fn collect_index_seek_candidates<'e>(
    expr: &'e Expr,
    schema: &Schema,
    fetch_indexes: impl FnOnce() -> Result<Arc<Vec<gnitz_core::IndexMeta>>, ClientError>,
) -> Result<Vec<IndexSeekCandidate<'e>>, ClientError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    let eqs = collect_eq_conjuncts(&conjuncts, schema);
    if eqs.is_empty() {
        return Ok(Vec::new());
    }

    let indexes = fetch_indexes()?;
    let mut out: Vec<IndexSeekCandidate<'e>> = Vec::new();
    for meta in indexes.iter() {
        // The index's FULL declared column list (sent verbatim for the exact
        // circuit match); `vals` covers a leading prefix of it (`<=` arity).
        let idx_cols = meta.cols.as_slice();
        let (vals, consumed) = consume_leading_eq_prefix(idx_cols, &eqs);
        if vals.is_empty() {
            continue;
        } // index does not apply

        // Leading-prefix safety: a row is omitted from the index if ANY indexed
        // column is NULL (`batch_project_index`). The covered columns are
        // non-null (they equal a non-null literal), but an uncovered *trailing*
        // column that is nullable would drop rows whose trailing value is NULL —
        // rows the prefix predicate still matches — so the seek would silently
        // lose them. Reject the prefix; a different index or (for UPDATE/DELETE)
        // the full-scan fallback serves the query correctly, and direct SELECT
        // raises its clean "non-indexed" error rather than returning wrong rows.
        if uncovered_trailing_nullable(idx_cols, vals.len(), schema) {
            continue;
        }

        out.push((meta.cols, vals, residual_conjuncts(&conjuncts, &consumed)));
    }

    // Best first: longer covered prefix wins; on a tie the tighter index (fewer
    // columns — an exact lookup over a leading-prefix scan) wins.
    out.sort_by(|a, b| {
        b.1.len()
            .cmp(&a.1.len())
            .then_with(|| a.0.as_slice().len().cmp(&b.0.as_slice().len()))
    });
    Ok(out)
}

/// Every `col = literal` equality among `conjuncts`, tagged with its conjunct
/// index so the residual can exclude exactly the consumed conjuncts. PK columns
/// and index-ineligible types can never carry a secondary index, so they are
/// never collected. Shared by the equality and range candidate collectors —
/// the eligibility rule must stay identical between them.
fn collect_eq_conjuncts(
    conjuncts: &[&Expr],
    schema: &Schema,
) -> Vec<(usize /*conjunct*/, usize /*col*/, u128 /*key*/)> {
    let mut eqs = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
        if let Some((col, key)) = try_col_eq_literal(cand, schema) {
            let tc = schema.columns[col].type_code;
            if !schema.is_pk_col(col) && tc.is_pk_eligible() {
                eqs.push((ci, col, key));
            }
        }
    }
    eqs
}

/// Consume equality conjuncts as an index's leading columns (leading-prefix
/// rule: stop at the first column with no covering equality). Returns the
/// covered key values and the consumed conjunct indices.
fn consume_leading_eq_prefix(idx_cols: &[u32], eqs: &[(usize, usize, u128)]) -> (Vec<u128>, Vec<usize>) {
    let mut vals = Vec::new();
    let mut consumed = Vec::new();
    for &col in idx_cols {
        match eqs.iter().find(|&&(_, c, _)| c as u32 == col) {
            Some(&(conj, _, key)) => {
                vals.push(key);
                consumed.push(conj);
            }
            None => break,
        }
    }
    (vals, consumed)
}

/// True when an index column past the first `covered` is nullable — the
/// leading-prefix safety rejection both collectors share (a NULL in any
/// indexed column omits the whole row from the index, so an uncovered
/// nullable trailing column would silently drop rows the predicate matches).
fn uncovered_trailing_nullable(idx_cols: &[u32], covered: usize, schema: &Schema) -> bool {
    covered < idx_cols.len()
        && idx_cols[covered..]
            .iter()
            .any(|&c| schema.columns[c as usize].is_nullable)
}

/// The conjuncts a seek/range plan did not consume, kept as post-scan filters.
fn residual_conjuncts<'e>(conjuncts: &[&'e Expr], consumed: &[usize]) -> Vec<&'e Expr> {
    conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, &e)| e)
        .collect()
}

// ---------------------------------------------------------------------------
// Ordered range scans over a secondary index
// ---------------------------------------------------------------------------

/// Which end of the candidate interval a conjunct bounds. This is the only
/// start/end distinction left in range planning — every bound becomes a
/// `Cut`, and the side only says whether that cut starts or ends the interval.
#[derive(Clone, Copy, PartialEq)]
enum RangeSide {
    Start,
    End,
}

/// One end of a range predicate on a column: which interval end it bounds and
/// the cut it induces there.
struct RangeEnd {
    side: RangeSide,
    cut: Cut,
}

/// Parse a range-end literal for column type `tc` into its cut; `mk` is the
/// constructor for an in-range literal — `Cut::After` when the cut falls
/// above the literal's whole duplicate group (`col > v` for a start cut,
/// `col <= v` for an end cut), `Cut::Before` when below (`>=`, `<`). Parses
/// as `i128` so a literal past the type's min/max SATURATES to the matching
/// `Cut::type_edges` edge instead of wrapping into a different in-range value
/// (the cff7c58-class trap one layer up) — and saturation is
/// constructor-independent: below the type minimum cuts at `Before(min)`,
/// above the maximum at `After(max)`. A bound saturating *towards* its
/// interval thereby yields a zero-width interval the engine rejects
/// byte-wise, so there is no empty-range special case anywhere. Returns
/// `None` for a type that cannot carry an ordered range bound here (UUID,
/// float, string) so the predicate stays a residual.
fn parse_range_cut(tc: TypeCode, n_str: &str, negated: bool, mk: fn(u128) -> Cut) -> Option<Cut> {
    // U128: full unsigned range — saturation is impossible (an i128 cannot
    // represent its upper half), and a literal past u128::MAX fails the parse,
    // keeping the conjunct a residual.
    if tc == TypeCode::U128 {
        if negated {
            return None;
        }
        return n_str.parse::<u128>().ok().map(mk);
    }
    let fi = FixedInt::from_type_code(tc)?;
    let (min, max) = fi.range();
    let (below, above) = Cut::type_edges(tc)?;
    let v = parse_literal_i128(n_str, negated)?;
    Some(if v < min {
        below
    } else if v > max {
        above
    } else {
        mk(fi.pack(v))
    })
}

/// Parse a `BETWEEN` endpoint expression to its cut (numeric literal only).
/// Both BETWEEN ends are inclusive: the start cuts before its boundary group
/// (`Cut::Before`), the end after its group (`Cut::After`).
fn between_cut(tc: TypeCode, expr: &Expr, mk: fn(u128) -> Cut) -> Option<Cut> {
    match extract_sql_literal(expr)? {
        SqlLiteral::Number(n, negated) => parse_range_cut(tc, n, negated, mk),
        _ => None,
    }
}

/// Recognize `col OP lit` (and the flipped `lit OP col`) for OP in
/// `>`,`>=`,`<`,`<=`, mapping it to the column index and a `RangeEnd`. Returns
/// `None` for anything else (equality, non-numeric literal, non-range-servable
/// type), leaving it for the equality extractor or the residual.
fn try_col_range_literal(expr: &Expr, schema: &Schema) -> Option<(usize, RangeEnd)> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    // Which side is the column, which is the literal, and is the operator flipped
    // (`lit OP col` ≡ `col FLIP(OP) lit`)?
    let (col_id, lit, flipped) = match (left.as_ref(), right.as_ref()) {
        (Expr::Identifier(id), r) => (id, extract_sql_literal(r)?, false),
        (l, Expr::Identifier(id)) => (id, extract_sql_literal(l)?, true),
        _ => return None,
    };
    use BinaryOperator as B;
    let (side, mk): (RangeSide, fn(u128) -> Cut) = match (op, flipped) {
        (B::Gt, false) | (B::Lt, true) => (RangeSide::Start, Cut::After), // col > lit / lit < col
        (B::GtEq, false) | (B::LtEq, true) => (RangeSide::Start, Cut::Before), // col >= lit / lit <= col
        (B::Lt, false) | (B::Gt, true) => (RangeSide::End, Cut::Before),  // col < lit / lit > col
        (B::LtEq, false) | (B::GtEq, true) => (RangeSide::End, Cut::After), // col <= lit / lit >= col
        _ => return None,
    };
    let col_idx = find_unique_column(&schema.columns, &col_id.value).ok().flatten()?;
    let tc = schema.columns[col_idx].type_code;
    let SqlLiteral::Number(n_str, negated) = lit else {
        return None;
    };
    let cut = parse_range_cut(tc, n_str, negated, mk)?;
    Some((col_idx, RangeEnd { side, cut }))
}

/// One index-servable range candidate: the index's FULL declared column list,
/// the wire descriptor (the equality-pinned leading values plus the half-open
/// cut interval on the next index column), and the residual conjuncts to
/// filter after the scan.
struct IndexRangeCandidate<'e> {
    idx_cols: gnitz_core::PkColList,
    desc: RangeDescriptor,
    residual: Vec<&'e Expr>,
}

/// One collected range end tagged with the conjunct it came from. A simple
/// `col OP lit` contributes one entry; a non-negated `BETWEEN` contributes two
/// (a lower and an upper) sharing the same conjunct index.
struct RangeEndEntry {
    conjunct: usize,
    col: usize,
    end: RangeEnd,
}

/// Recognize a NON-negated `col BETWEEN lo AND hi` (numeric literals only) as
/// the two range ends it desugars to — `col >= lo` and `col <= hi`. BETWEEN is
/// a single AST node (`flatten_conjuncts` keeps it whole). A negated BETWEEN
/// is `col < lo OR col > hi` — not a contiguous interval — and stays a
/// residual, as does any endpoint that fails to parse for the column type.
fn try_col_between(expr: &Expr, schema: &Schema) -> Option<(usize, [RangeEnd; 2])> {
    let Expr::Between {
        expr: be,
        negated: false,
        low,
        high,
    } = expr
    else {
        return None;
    };
    let Expr::Identifier(id) = be.as_ref() else { return None };
    let col = find_unique_column(&schema.columns, &id.value).ok().flatten()?;
    let tc = schema.columns[col].type_code;
    Some((
        col,
        [
            RangeEnd {
                side: RangeSide::Start,
                cut: between_cut(tc, low, Cut::Before)?,
            },
            RangeEnd {
                side: RangeSide::End,
                cut: between_cut(tc, high, Cut::After)?,
            },
        ],
    ))
}

/// Every index-servable range candidate among the conjuncts of `expr`. Mirrors
/// `collect_index_seek_candidates` but for an ordered range scan: collect
/// equality conjuncts (the leading prefix) and range ends (incl. desugared
/// `BETWEEN`); for each index consume the equality conjuncts as the leading `E`
/// columns, then require column `E` to be covered by ≥1 range end. The FIRST
/// start-side and FIRST end-side cut on column `E` become the interval — never
/// a compare of two packed natives to pick the tighter one (the cff7c58 trap one
/// layer up; `apply_residual_filter` trims any redundant same-side end exactly).
/// An unconstrained side widens to the column type's edge cut, and an
/// out-of-type-range literal saturates to the same edges (`parse_range_cut`) —
/// a contradictory bound yields a zero-width interval the engine rejects
/// byte-wise, so there is no empty-range special case.
fn collect_index_range_candidates<'e>(
    expr: &'e Expr,
    schema: &Schema,
    fetch_indexes: impl FnOnce() -> Result<Arc<Vec<gnitz_core::IndexMeta>>, ClientError>,
) -> Result<Vec<IndexRangeCandidate<'e>>, ClientError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    // Equality conjuncts (for the leading prefix) and range ends, both tagged
    // with their conjunct index so the residual excludes exactly the consumed
    // conjuncts.
    let eqs = collect_eq_conjuncts(&conjuncts, schema);
    let mut ends: Vec<RangeEndEntry> = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
        if let Some((col, end)) = try_col_range_literal(cand, schema) {
            ends.push(RangeEndEntry { conjunct: ci, col, end });
        } else if let Some((col, both)) = try_col_between(cand, schema) {
            ends.extend(both.map(|end| RangeEndEntry { conjunct: ci, col, end }));
        }
    }
    // A range candidate needs at least one range end; a pure-equality WHERE is
    // handled by collect_index_seek_candidates, so this costs no wire traffic then.
    if ends.is_empty() {
        return Ok(Vec::new());
    }

    let indexes = fetch_indexes()?;
    let mut out: Vec<IndexRangeCandidate<'e>> = Vec::new();
    for meta in indexes.iter() {
        let idx_cols = meta.cols.as_slice();
        let (eq_vals, eq_consumed) = consume_leading_eq_prefix(idx_cols, &eqs);
        let n_eq = eq_vals.len();
        // The range column is the next index column after the equality prefix.
        if n_eq >= idx_cols.len() {
            continue;
        } // no column left to range over
        let range_col = idx_cols[n_eq];

        // First start-side and first end-side cut on the range column become
        // the interval. Never compare multiple same-side cuts (the cff7c58 trap
        // one layer up); `apply_residual_filter` trims any redundant same-side end.
        let start_idx = ends
            .iter()
            .position(|e| e.col as u32 == range_col && e.end.side == RangeSide::Start);
        let end_idx = ends
            .iter()
            .position(|e| e.col as u32 == range_col && e.end.side == RangeSide::End);
        if start_idx.is_none() && end_idx.is_none() {
            continue;
        } // range column not covered

        // Covered columns are the equality prefix + the range column (n_eq + 1);
        // the leading-prefix safety rejection is shared with the equality path.
        if uncovered_trailing_nullable(idx_cols, n_eq + 1, schema) {
            continue;
        }

        // An unconstrained side widens to the type-edge cut — `Before(min)` /
        // `After(max)` ARE "unbounded", since no index entry lies outside the
        // range column's type. (At least one end on this column parsed into
        // `ends`, so the type is range-servable and the edges exist.)
        let tc = schema.columns[range_col as usize].type_code;
        let Some((edge_start, edge_end)) = Cut::type_edges(tc) else {
            continue;
        };
        let start = start_idx.map_or(edge_start, |i| ends[i].end.cut);
        let end = end_idx.map_or(edge_end, |i| ends[i].end.cut);

        // Consume the range conjuncts whose ends were ALL chosen as bounds. A
        // simple `col OP lit` has one end; a BETWEEN has two sharing one
        // conjunct, and with only one chosen its un-applied half must stay a
        // residual filter (else `b > 5 AND b BETWEEN 10 AND 50` would lose
        // `b >= 10` while seeding lo from `b > 5`).
        let chosen = [start_idx, end_idx];
        let mut consumed = eq_consumed;
        for cj in chosen.iter().flatten().map(|&i| ends[i].conjunct) {
            let all_chosen = ends
                .iter()
                .enumerate()
                .filter(|(_, e)| e.conjunct == cj)
                .all(|(i, _)| chosen.contains(&Some(i)));
            if all_chosen && !consumed.contains(&cj) {
                consumed.push(cj);
            }
        }

        out.push(IndexRangeCandidate {
            idx_cols: meta.cols,
            desc: RangeDescriptor::new(&eq_vals, start, end),
            residual: residual_conjuncts(&conjuncts, &consumed),
        });
    }

    // Best first: more equality-pinned columns first, then the tighter index.
    out.sort_by(|a, b| {
        b.desc
            .eq_vals()
            .len()
            .cmp(&a.desc.eq_vals().len())
            .then_with(|| a.idx_cols.as_slice().len().cmp(&b.idx_cols.as_slice().len()))
    });
    Ok(out)
}

// ---------------------------------------------------------------------------
// UPDATE / DELETE helpers
// ---------------------------------------------------------------------------

fn eval_set_expr(
    expr: &BoundExpr,
    batch: &ZSetBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<ColumnValue, GnitzSqlError> {
    match expr {
        BoundExpr::LitStr(s) => return Ok(ColumnValue::Str(s.clone())),
        BoundExpr::ColRef(c) => {
            if let ColData::Strings(v) = &batch.columns[*c] {
                return Ok(match &v[row_idx] {
                    Some(s) => ColumnValue::Str(s.clone()),
                    None => ColumnValue::Null,
                });
            }
            // fall through: numeric column handled by eval_expr below
        }
        _ => {}
    }
    match eval_expr(expr, batch, row_idx, schema)? {
        None => Ok(ColumnValue::Null),
        Some(v) => Ok(ColumnValue::Int(v)),
    }
}

fn extract_assignment_col_name(assignment: &Assignment, clause: &str) -> Result<String, GnitzSqlError> {
    match &assignment.target {
        AssignmentTarget::ColumnName(obj_name) => extract_name(obj_name, clause),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "only simple column assignments supported in {clause}"
        ))),
    }
}

/// Resolve and validate one `col = expr` SET-list target — shared by UPDATE and
/// ON CONFLICT DO UPDATE, which differ only in the RHS binding and the `clause`
/// label used in messages. Extracts the column name, resolves it to a column
/// index, rejects a PK target, and rejects a column already present in `seen`
/// (recording it there on success so the next duplicate is caught). Both clauses
/// enforce the same SQL rules: no PK writes, no duplicate columns.
fn resolve_set_target(
    assignment: &Assignment,
    schema: &Schema,
    seen: &mut Vec<usize>,
    clause: &str,
) -> Result<usize, GnitzSqlError> {
    let col_name = extract_assignment_col_name(assignment, clause)?;
    let col_idx = schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&col_name))
        .ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found in {clause}")))?;
    if schema.is_pk_col(col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "cannot assign to primary key column in {clause}"
        )));
    }
    if seen.contains(&col_idx) {
        return Err(GnitzSqlError::Bind(format!(
            "multiple assignments to column '{col_name}' in {clause}"
        )));
    }
    seen.push(col_idx);
    Ok(col_idx)
}

fn write_set_columns(
    current: &ZSetBatch,
    row_idx: usize,
    assignments: &[(usize, BoundExpr)],
    schema: &Schema,
    dst: &mut ZSetBatch,
) -> Result<(), GnitzSqlError> {
    dst.pks.push_from(&current.pks, row_idx);
    dst.weights.push(1);
    // Start with the existing row's null bits; each assignment updates only
    // its own column's bit (set for a NULL result, clear for non-NULL).
    let mut null_bits = current.nulls[row_idx];

    for (payload_idx, ci, col_def) in schema.payload_columns() {
        if let Some((_, expr)) = assignments.iter().find(|(idx, _)| *idx == ci) {
            let cv = eval_set_expr(expr, current, row_idx, schema)?;
            null_word_set(&mut null_bits, payload_idx, matches!(cv, ColumnValue::Null));
            append_column_value(&mut dst.columns[ci], cv, col_def.type_code)?;
        } else {
            let stride = col_def.type_code.wire_stride();
            current.columns[ci].push_row_from(row_idx, stride, &mut dst.columns[ci]);
        }
    }
    dst.nulls.push(null_bits);
    Ok(())
}

pub(crate) fn try_extract_pk_in(expr: &Expr, schema: &Schema) -> Option<Vec<u128>> {
    // Compound PK has no IN-list fast path; fall back to a full delta scan.
    if schema.pk_count() != 1 {
        return None;
    }
    if let Expr::InList {
        expr: col_expr,
        list,
        negated,
        ..
    } = expr
    {
        if *negated {
            return None;
        }
        let col_name = match col_expr.as_ref() {
            Expr::Identifier(id) => &id.value,
            _ => return None,
        };
        let pk_col = &schema.columns[schema.pk_indices()[0]];
        if !pk_col.name.eq_ignore_ascii_case(col_name) {
            return None;
        }
        let mut pks = Vec::with_capacity(list.len());
        for item in list {
            // Optionally-negated numerics, plus single-quoted UUID strings for a
            // UUID PK — exactly the literals `try_col_eq_literal` accepts for the
            // `=` seek, so `IN (…)` and `= …` route identically. A NULL, a
            // non-literal, or an unparseable UUID aborts to the slow scan.
            let v = match extract_sql_literal(item)? {
                SqlLiteral::Number(n, negated) => parse_pk_literal_packed(pk_col.type_code, n, negated)?,
                SqlLiteral::Str(s) if pk_col.type_code == TypeCode::UUID => parse_uuid_str(s).ok()?,
                _ => return None,
            };
            pks.push(v);
        }
        Some(pks)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

pub(crate) fn execute_update(
    client: &mut GnitzClient,
    _schema_name: &str,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let (table, assignments_raw, selection) = match stmt {
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => (table, assignments, selection),
        _ => return Err(GnitzSqlError::Bind("not an UPDATE statement".to_string())),
    };

    let table_name = extract_table_factor_name(&table.relation, "UPDATE")?;

    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    // Bind SET assignments; reject PK writes and duplicate columns.
    let mut assignments: Vec<(usize, BoundExpr)> = Vec::new();
    let mut seen: Vec<usize> = Vec::with_capacity(assignments_raw.len());
    for assignment in assignments_raw {
        let col_idx = resolve_set_target(assignment, &schema, &mut seen, "UPDATE SET")?;
        let bound_val = binder.bind_expr(&assignment.value, &schema)?;
        assignments.push((col_idx, bound_val));
    }

    if let Some(where_expr) = selection {
        // Path 1: PK equality, with optional residual on non-PK conjuncts.
        if let Some((pk_tuple, residual)) = try_extract_pk_seek_residual(where_expr, &schema) {
            let (schema_opt, batch_opt, _) = client.seek(table_id, &pk_tuple)?;
            let current = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) if b.pks.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
            let preds = bind_residuals(binder, &residual, &schema)?;
            if !row_passes_residuals(&preds, &current, 0, actual_schema)? {
                return Ok(SqlResult::RowsAffected { count: 0 });
            }
            let mut new_batch = ZSetBatch::new(actual_schema);
            write_set_columns(&current, 0, &assignments, actual_schema, &mut new_batch)?;
            client.push_with_mode(table_id, actual_schema, &new_batch, WireConflictMode::Update)?;
            return Ok(SqlResult::RowsAffected { count: 1 });
        }

        // Path 2: secondary-index seek (any index, unique or not). Applies the
        // residual per row and updates *all* matches. Falls through to Path 3
        // when no candidate column carries an index.
        let candidates = collect_index_seek_candidates(where_expr, &schema, || client.table_indexes(table_id))
            .map_err(GnitzSqlError::Exec)?;
        for (col_indices, key_vals, residual) in candidates {
            let (schema_opt, batch_opt, _) = match client.seek_by_index(table_id, col_indices.as_slice(), &key_vals) {
                Ok(r) => r,
                Err(ClientError::NoIndex) => continue,
                Err(e) => return Err(GnitzSqlError::Exec(e)),
            };
            let current = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) if b.pks.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
            let preds = bind_residuals(binder, &residual, &schema)?;
            let mut new_batch = ZSetBatch::new(actual_schema);
            let mut count = 0usize;
            for i in 0..current.pks.len() {
                if !row_passes_residuals(&preds, &current, i, actual_schema)? {
                    continue;
                }
                write_set_columns(&current, i, &assignments, actual_schema, &mut new_batch)?;
                count += 1;
            }
            if count > 0 {
                client.push_with_mode(table_id, actual_schema, &new_batch, WireConflictMode::Update)?;
            }
            return Ok(SqlResult::RowsAffected { count });
        }

        // Path 3: Full scan with predicate
        let pred = binder.bind_expr(where_expr, &schema)?;
        let (schema_opt, batch_opt, _) = client.scan(table_id)?;
        let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
        let mut updates = ZSetBatch::new(actual_schema);
        let mut count = 0usize;
        if let Some(ref scan_batch) = batch_opt {
            for i in 0..scan_batch.pks.len() {
                if !eval_pred_row(&pred, scan_batch, i, actual_schema)? {
                    continue;
                }
                write_set_columns(scan_batch, i, &assignments, actual_schema, &mut updates)?;
                count += 1;
            }
            if count > 0 {
                client.push_with_mode(table_id, actual_schema, &updates, WireConflictMode::Update)?;
            }
        }
        return Ok(SqlResult::RowsAffected { count });
    }

    // Path 4: No WHERE — update all rows
    let (schema_opt, batch_opt, _) = client.scan(table_id)?;
    let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
    let mut updates = ZSetBatch::new(actual_schema);
    match batch_opt {
        None => Ok(SqlResult::RowsAffected { count: 0 }),
        Some(ref scan_batch) => {
            let n = scan_batch.pks.len();
            for i in 0..n {
                write_set_columns(scan_batch, i, &assignments, actual_schema, &mut updates)?;
            }
            if n > 0 {
                client.push_with_mode(table_id, actual_schema, &updates, WireConflictMode::Update)?;
            }
            Ok(SqlResult::RowsAffected { count: n })
        }
    }
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

pub(crate) fn execute_delete(
    client: &mut GnitzClient,
    _schema_name: &str,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let del = match stmt {
        Statement::Delete(d) => d,
        _ => return Err(GnitzSqlError::Bind("not a DELETE statement".to_string())),
    };

    let tables = match &del.from {
        FromTable::WithFromKeyword(ts) | FromTable::WithoutKeyword(ts) => ts,
    };
    if tables.len() != 1 || !tables[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "DELETE: exactly one simple FROM table required".to_string(),
        ));
    }
    let table_name = extract_table_factor_name(&tables[0].relation, "DELETE")?;

    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    match &del.selection {
        None => {
            // DELETE all rows
            let (schema_opt, batch_opt, _) = client.scan(table_id)?;
            let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
            let batch = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let n = batch.pks.len();
            if n == 0 {
                return Ok(SqlResult::RowsAffected { count: 0 });
            }
            client.delete(table_id, actual_schema, batch.pks)?;
            Ok(SqlResult::RowsAffected { count: n })
        }
        Some(where_expr) => {
            // Path 1: PK equality, with optional residual on non-PK conjuncts.
            if let Some((pk, residual)) = try_extract_pk_seek_residual(where_expr, &schema) {
                let (schema_opt, batch_opt, _) = client.seek(table_id, &pk)?;
                let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
                let batch = match batch_opt {
                    None => return Ok(SqlResult::RowsAffected { count: 0 }),
                    Some(b) if b.pks.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                    Some(b) => b,
                };
                let preds = bind_residuals(binder, &residual, &schema)?;
                if !row_passes_residuals(&preds, &batch, 0, actual_schema)? {
                    return Ok(SqlResult::RowsAffected { count: 0 });
                }
                let pks = PkColumn::one_row(actual_schema, &pk);
                client.delete(table_id, actual_schema, pks)?;
                return Ok(SqlResult::RowsAffected { count: 1 });
            }

            // Path 2: PK IN list (single-PK only; try_extract_pk_in returns None
            // for a compound PK). Seek each key and retract only those that exist,
            // so RowsAffected reports rows actually removed rather than the raw
            // list length, with a repeated key counted once. (Positivity is not at
            // stake here — the engine's enforce_unique_pk already drops
            // a retraction of an absent/tombstoned key; this seek is purely about
            // an accurate count.) PkTuple::from_u128 produces the same seek key
            // Path 1 builds for `pk = v` (low `pk_stride` bytes = the column's
            // native LE bytes).
            if let Some(pks) = try_extract_pk_in(where_expr, &schema) {
                let stride = schema.pk_stride() as u8;
                let mut seen: std::collections::HashSet<u128> = std::collections::HashSet::with_capacity(pks.len());
                let mut pk_col = PkColumn::empty_for_schema(&schema);
                for v in pks {
                    if !seen.insert(v) {
                        continue;
                    } // intra-list dedup
                    let pk = PkTuple::from_u128(stride, v);
                    let (_schema_opt, batch_opt, _) = client.seek(table_id, &pk)?;
                    if batch_opt.as_ref().is_some_and(|b| !b.pks.is_empty()) {
                        pk_col.push_u128(v);
                    }
                }
                let count = pk_col.len();
                if count > 0 {
                    client.delete(table_id, &schema, pk_col)?;
                }
                return Ok(SqlResult::RowsAffected { count });
            }

            // Path 3: secondary-index seek (any index, unique or not). Applies
            // the residual per row and deletes *all* matches. Falls through to
            // Path 4 when no candidate column carries an index.
            let candidates = collect_index_seek_candidates(where_expr, &schema, || client.table_indexes(table_id))
                .map_err(GnitzSqlError::Exec)?;
            for (col_indices, key_vals, residual) in candidates {
                let (schema_opt, batch_opt, _) = match client.seek_by_index(table_id, col_indices.as_slice(), &key_vals)
                {
                    Ok(r) => r,
                    Err(ClientError::NoIndex) => continue,
                    Err(e) => return Err(GnitzSqlError::Exec(e)),
                };
                let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
                let batch = match batch_opt {
                    None => return Ok(SqlResult::RowsAffected { count: 0 }),
                    Some(b) if b.pks.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                    Some(b) => b,
                };
                let preds = bind_residuals(binder, &residual, &schema)?;
                let mut out_pks = PkColumn::empty_for_schema(actual_schema);
                let mut count = 0usize;
                for i in 0..batch.pks.len() {
                    if !row_passes_residuals(&preds, &batch, i, actual_schema)? {
                        continue;
                    }
                    out_pks.push_from(&batch.pks, i);
                    count += 1;
                }
                if count > 0 {
                    client.delete(table_id, actual_schema, out_pks)?;
                }
                return Ok(SqlResult::RowsAffected { count });
            }

            // Path 4: Full scan with predicate
            let pred = binder.bind_expr(where_expr, &schema)?;
            let (schema_opt, batch_opt, _) = client.scan(table_id)?;
            let actual_schema = schema_opt.as_deref().unwrap_or(&*schema);
            let mut out_pks = PkColumn::empty_for_schema(actual_schema);
            let mut n = 0usize;
            if let Some(ref scan_batch) = batch_opt {
                for i in 0..scan_batch.pks.len() {
                    if eval_pred_row(&pred, scan_batch, i, actual_schema)? {
                        out_pks.push_from(&scan_batch.pks, i);
                        n += 1;
                    }
                }
            }
            if n > 0 {
                client.delete(table_id, actual_schema, out_pks)?;
            }
            Ok(SqlResult::RowsAffected { count: n })
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::BoundExpr;
    use gnitz_core::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
    use sqlparser::ast::UnaryOperator;

    fn col_def(name: &str, tc: TypeCode, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_code: tc,
            is_nullable: nullable,
            fk_table_id: 0,
            fk_col_idx: 0,
        }
    }

    /// Two-column schema: pk (U64, pk_index=0) + val (nullable).
    fn two_col(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("pk", TypeCode::U64, false), col_def("val", val_tc, true)],
            pk_cols: vec![0],
        }
    }

    /// Single-row batch for a two-column schema (pk + one payload column).
    fn batch_2col(val_bytes: Vec<u8>, val_tc: TypeCode, null_bits: u64) -> ZSetBatch {
        let schema = two_col(val_tc);
        let mut b = ZSetBatch::new(&schema);
        b.pks.push_u128(1u128);
        b.weights.push(1);
        b.nulls.push(null_bits);
        if let ColData::Fixed(ref mut buf) = b.columns[1] {
            buf.extend(val_bytes);
        }
        b
    }

    // ------------------------------------------------------------------
    // Bug #1 — write_set_columns must update the null bitmap for assignments
    // ------------------------------------------------------------------

    #[test]
    fn test_write_set_clears_null_bit_on_non_null_assignment() {
        // Existing row: val = NULL (null bit set). Assignment: SET val = 99.
        // Expected: val = 99, null bit cleared.
        let schema = two_col(TypeCode::I64);
        let current = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // val is NULL

        let assignments = vec![(1usize, BoundExpr::LitInt(99))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_columns(&current, 0, &assignments, &schema, &mut dst).unwrap();

        assert_eq!(
            dst.nulls[0] & 0b1,
            0,
            "null bit must be cleared after non-null assignment"
        );
        if let ColData::Fixed(ref buf) = dst.columns[1] {
            assert_eq!(i64::from_le_bytes(buf[..8].try_into().unwrap()), 99);
        }
    }

    #[test]
    fn test_write_set_sets_null_bit_when_source_col_is_null() {
        // Three-column schema: pk, a (non-null), b (null).
        // Assignment: SET a = b (b is NULL → a should become NULL).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::I64, true),
                col_def("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0b10); // payload bit 1 (b) is NULL; bit 0 (a) is non-null
        if let ColData::Fixed(ref mut buf) = current.columns[1] {
            buf.extend_from_slice(&5i64.to_le_bytes());
        }
        if let ColData::Fixed(ref mut buf) = current.columns[2] {
            buf.extend_from_slice(&[0u8; 8]);
        }

        // SET a = b  (ColRef(2) = b, which is NULL in current)
        let assignments = vec![(1usize, BoundExpr::ColRef(2))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_columns(&current, 0, &assignments, &schema, &mut dst).unwrap();

        // a's null bit (payload_idx 0 → bit 0) must now be set
        assert_ne!(dst.nulls[0] & 0b01, 0, "a must be null after SET a = NULL_col");
        // b's null bit (payload_idx 1 → bit 1) must remain set (not touched)
        assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null");
    }

    #[test]
    fn test_write_set_preserves_null_bits_for_unassigned_cols() {
        // Unassigned columns must carry their original null status unchanged.
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::I64, true),
                col_def("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0b10); // b is NULL, a is not
        if let ColData::Fixed(ref mut buf) = current.columns[1] {
            buf.extend_from_slice(&5i64.to_le_bytes());
        }
        if let ColData::Fixed(ref mut buf) = current.columns[2] {
            buf.extend_from_slice(&[0u8; 8]);
        }

        // Only assign to a; b is untouched
        let assignments = vec![(1usize, BoundExpr::LitInt(10))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_columns(&current, 0, &assignments, &schema, &mut dst).unwrap();

        assert_eq!(dst.nulls[0] & 0b01, 0, "a must not be null (assigned non-null)");
        assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null (unassigned)");
    }

    fn uuid_schema_pk() -> Schema {
        Schema {
            columns: vec![col_def("id", TypeCode::UUID, false)],
            pk_cols: vec![0],
        }
    }

    fn uuid_schema_payload() -> Schema {
        Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("uid", TypeCode::UUID, true),
            ],
            pk_cols: vec![0],
        }
    }

    fn uuid_str_expr(s: &str) -> Expr {
        Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::SingleQuotedString(s.into()),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    fn num_expr(n: &str) -> Expr {
        Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number(n.into(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    #[test]
    fn test_uuid_non_pk_string_literal_accepted() {
        let schema = uuid_schema_payload();
        let mut batch = ZSetBatch::new(&schema);
        // col 1 is UUID
        append_value_to_col(
            &mut batch.columns[1],
            TypeCode::UUID,
            &uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"),
        )
        .unwrap();
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], 0x550e8400_e29b_41d4_a716_446655440000_u128);
        } else {
            panic!("expected U128s");
        }
    }

    #[test]
    fn test_uuid_decimal_literal_still_accepted() {
        let schema = uuid_schema_payload();
        let mut batch = ZSetBatch::new(&schema);
        let big_val: u128 = 0x550e8400_e29b_41d4_a716_446655440000_u128;
        append_value_to_col(&mut batch.columns[1], TypeCode::UUID, &num_expr(&big_val.to_string())).unwrap();
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], big_val);
        } else {
            panic!("expected U128s");
        }
    }

    #[test]
    fn test_uuid_index_seek_string_literal() {
        let schema = uuid_schema_payload();
        let expr = eq_expr("uid", uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, 0x550e8400_e29b_41d4_a716_446655440000_u128)));
    }

    #[test]
    fn test_uuid_residual_filter_error_names_uuid() {
        let schema = uuid_schema_payload();
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::U128s(v) = &mut batch.columns[1] {
            v.push(0);
        }
        let pred = BoundExpr::ColRef(1);
        let err = super::eval_expr(&pred, &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().contains("UUID"), "error should mention UUID: {err}");
    }

    // ------------------------------------------------------------------
    // try_col_eq_literal — negative signed integer index seek keys
    // ------------------------------------------------------------------

    fn make_schema_col(tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("pk", TypeCode::U64, false), col_def("val", tc, true)],
            pk_cols: vec![0],
        }
    }

    fn neg_num_expr(n: &str) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number(n.into(), false),
                span: sqlparser::tokenizer::Span::empty(),
            })),
        }
    }

    fn eq_expr(col: &str, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            op: BinaryOperator::Eq,
            right: Box::new(rhs),
        }
    }

    #[test]
    fn test_try_col_eq_literal_negative_i64() {
        let schema = make_schema_col(TypeCode::I64);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        // -1i64 as u64 = u64::MAX
        assert_eq!(result, Some((1, ((-1i64) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i32() {
        let schema = make_schema_col(TypeCode::I32);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        // I32 -1 stored as 4 bytes zero-padded → (-1i32 as u32) as u64
        assert_eq!(result, Some((1, ((-1i32 as u32) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i16() {
        let schema = make_schema_col(TypeCode::I16);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, ((-1i16 as u16) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i8() {
        let schema = make_schema_col(TypeCode::I8);
        let expr = eq_expr("val", neg_num_expr("5"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, ((-5i8 as u8) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_positive_i64_still_works() {
        let schema = make_schema_col(TypeCode::I64);
        let expr = eq_expr(
            "val",
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number("42".into(), false),
                span: sqlparser::tokenizer::Span::empty(),
            }),
        );
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, 42u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_u64_returns_none() {
        // Cannot have a negative value for an unsigned column.
        let schema = make_schema_col(TypeCode::U64);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, None);
    }

    fn pk_schema(pk_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("id", pk_tc, false), col_def("v", TypeCode::I64, false)],
            pk_cols: vec![0],
        }
    }

    fn in_list_expr(col: &str, items: Vec<Expr>) -> Expr {
        Expr::InList {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            list: items,
            negated: false,
        }
    }

    // ------------------------------------------------------------------
    // eval_expr: residual filter on PK column reads from batch.pks
    // ------------------------------------------------------------------

    #[test]
    fn eval_expr_pk_i64_negative() {
        let schema = pk_schema(TypeCode::I64);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(((-1i64) as u64) as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        let got = super::eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).unwrap();
        assert_eq!(got, Some(-1i64));
    }

    #[test]
    fn eval_expr_pk_u32_max() {
        // U32 PK = u32::MAX; zero-extended into u128 by extract_pk_value.
        let schema = pk_schema(TypeCode::U32);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(u32::MAX as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        let got = super::eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).unwrap();
        assert_eq!(got, Some(u32::MAX as i64));
    }

    #[test]
    fn eval_expr_pk_u128_unsupported() {
        // U128 PK doesn't fit through eval_expr's i64 projection; must error,
        // not panic.
        let schema = pk_schema(TypeCode::U128);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(u128::MAX);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        let err =
            super::eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).expect_err("U128 PK must return Unsupported");
        assert!(err.to_string().contains("residual filter on PK"), "error: {err}");
    }

    // ------------------------------------------------------------------
    // Compound PK — every per-row byte path on a pk_count >= 2 schema.
    // ------------------------------------------------------------------

    fn compound_schema_u64_u64() -> Schema {
        Schema {
            columns: vec![
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
                col_def("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0, 1],
        }
    }

    fn compound_schema_u64_u64_u128() -> Schema {
        Schema {
            columns: vec![
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
                col_def("c", TypeCode::U128, false),
                col_def("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0, 1, 2],
        }
    }

    #[test]
    fn compound_pk_extract_pk_value_packs_le_bytes() {
        let schema = compound_schema_u64_u64();
        let row = vec![num_expr("1"), num_expr("2"), num_expr("99")];
        let pk = super::extract_pk_value(&row, &schema).unwrap();
        assert_eq!(pk.stride, 16);
        let mut expect = [0u8; 16];
        expect[0..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_extract_pk_value_wide_region() {
        let schema = compound_schema_u64_u64_u128();
        let row = vec![num_expr("1"), num_expr("2"), num_expr("3"), num_expr("99")];
        let pk = super::extract_pk_value(&row, &schema).unwrap();
        // pk_stride = 8 + 8 + 16 = 32 → wide-region path.
        assert_eq!(pk.stride, 32);
        let mut expect = [0u8; 32];
        expect[0..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        expect[16..32].copy_from_slice(&3u128.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_full_binding() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (b = 2) → full bind
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("b", num_expr("2"))),
        };
        let (pk, residual) = super::try_extract_pk_seek_residual(&expr, &schema).expect("must bind");
        assert!(residual.is_empty());
        assert_eq!(pk.stride, 16);
        let mut expect = [0u8; 16];
        expect[..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_reordered_and_tree() {
        let schema = compound_schema_u64_u64();
        // (b = 2) AND (a = 1) — order swapped; tuple must still pack in
        // pk-list order, not source order.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("b", num_expr("2"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("a", num_expr("1"))),
        };
        let (pk, residual) = super::try_extract_pk_seek_residual(&expr, &schema).expect("must bind");
        assert!(residual.is_empty());
        let mut expect = [0u8; 16];
        expect[..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_partial_returns_none() {
        let schema = compound_schema_u64_u64();
        let expr = eq_expr("a", num_expr("1")); // only one of two PK cols
        assert!(super::try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_incomplete_pk_with_payload_returns_none() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (v = 9) — `a` binds, `v` is a payload conjunct that goes
        // to the residual; the PK stays incomplete (`b` unbound) → None.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("v", num_expr("9"))),
        };
        assert!(super::try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_duplicate_binding_returns_none() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (a = 2) — duplicate binding of column 0; the second
        // routes to the residual, so `b` stays unbound → None.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("a", num_expr("2"))),
        };
        assert!(super::try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn pk_seek_residual_keeps_non_pk_conjunct() {
        let schema = pk_schema(TypeCode::U64);
        // id = 1 AND v = 9 → PK binds fully; `v = 9` becomes the residual.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("id", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("v", num_expr("9"))),
        };
        let (pk, residual) = super::try_extract_pk_seek_residual(&expr, &schema).expect("PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert_eq!(residual.len(), 1);
    }

    fn idx_metas(col_lists: &[&[u32]]) -> Arc<Vec<gnitz_core::IndexMeta>> {
        Arc::new(
            col_lists
                .iter()
                .map(|cols| gnitz_core::IndexMeta {
                    cols: gnitz_core::PkColList::from_slice(cols),
                    is_unique: false,
                })
                .collect(),
        )
    }

    #[test]
    fn collect_index_seek_candidates_skips_float_col() {
        // `WHERE val = 1` on a float column emits no candidate even with an index
        // present: a float column is never index-key-eligible, so it is filtered
        // out of the equality set before any index is consulted — fetch_indexes
        // must not even be called (no GET_INDICES round-trip).
        let schema = make_schema_col(TypeCode::F64);
        let expr = eq_expr("val", num_expr("1"));
        let cands = super::collect_index_seek_candidates(&expr, &schema, || {
            panic!("no eligible equality — the index list must not be fetched")
        })
        .unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn collect_index_seek_candidates_flattens_and_tree() {
        // pk(U64) + a,b,c (all U64, pk-eligible non-PK), each with its own
        // single-column index.
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::U64, true),
                col_def("b", TypeCode::U64, true),
                col_def("c", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        // (a = 1 AND b = 2) AND c = 3 — left-assoc nesting.
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(eq_expr("a", num_expr("1"))),
                op: BinaryOperator::And,
                right: Box::new(eq_expr("b", num_expr("2"))),
            }),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("c", num_expr("3"))),
        };
        let indexes = idx_metas(&[&[1], &[2], &[3]]);
        let cands = super::collect_index_seek_candidates(&expr, &schema, || Ok(indexes)).unwrap();
        // One candidate per applicable index; flattening the whole AND-tree finds
        // all three (would regress to just `c` under a two-way-only extractor).
        let mut cols: Vec<Vec<u32>> = cands.iter().map(|(c, _, _)| c.as_slice().to_vec()).collect();
        cols.sort();
        assert_eq!(cols, vec![vec![1], vec![2], vec![3]]);
        // Each candidate binds exactly its one covered value, residual is the
        // other two conjuncts.
        for (_, vals, residual) in &cands {
            assert_eq!(vals.len(), 1);
            assert_eq!(residual.len(), 2);
        }
    }

    #[test]
    fn write_set_columns_carries_blob_column_through() {
        // UPDATE assigns `v` only; the unmodified BLOB column must carry
        // through. Red against the missing `ColData::Bytes` arm (unreachable!).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("b", TypeCode::Blob, true),
                col_def("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0);
        if let ColData::Bytes(v) = &mut current.columns[1] {
            v.push(Some(vec![1, 2, 3]));
        }
        if let ColData::Fixed(buf) = &mut current.columns[2] {
            buf.extend_from_slice(&7i64.to_le_bytes());
        }

        let assignments = vec![(2usize, BoundExpr::LitInt(99))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_columns(&current, 0, &assignments, &schema, &mut dst).unwrap();

        if let ColData::Bytes(v) = &dst.columns[1] {
            assert_eq!(v[0].as_deref(), Some(&[1u8, 2, 3][..]));
        } else {
            panic!("expected Bytes column carried through");
        }
        if let ColData::Fixed(buf) = &dst.columns[2] {
            assert_eq!(i64::from_le_bytes(buf[..8].try_into().unwrap()), 99);
        }
    }

    #[test]
    fn compound_pk_try_extract_pk_in_returns_none() {
        let schema = compound_schema_u64_u64();
        let expr = in_list_expr("a", vec![num_expr("1"), num_expr("2")]);
        assert!(super::try_extract_pk_in(&expr, &schema).is_none());
    }

    #[test]
    fn compound_pk_zset_batch_new_uses_bytes_variant() {
        let schema = compound_schema_u64_u64();
        let batch = ZSetBatch::new(&schema);
        match &batch.pks {
            gnitz_core::PkColumn::Bytes { stride, buf } => {
                assert_eq!(*stride, 16);
                assert!(buf.is_empty());
            }
            other => panic!("expected PkColumn::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn compound_pk_eval_expr_pk_colref_reads_byte_region() {
        let schema = compound_schema_u64_u64();
        // Build a one-row Bytes batch: pk = (a=7, b=9), v = 42.
        let mut batch = ZSetBatch::new(&schema);
        let mut pk_bytes = [0u8; 16];
        pk_bytes[..8].copy_from_slice(&7u64.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&9u64.to_le_bytes());
        batch.pks.push_bytes(&pk_bytes);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[2] {
            buf.extend_from_slice(&42i64.to_le_bytes());
        }
        // Read column 0 (a) and column 1 (b) through eval_expr.
        let a = super::eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).unwrap();
        assert_eq!(a, Some(7));
        let b = super::eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap();
        assert_eq!(b, Some(9));
        // Payload column still works.
        let v = super::eval_expr(&BoundExpr::ColRef(2), &batch, 0, &schema).unwrap();
        assert_eq!(v, Some(42));
    }

    #[test]
    fn compound_pk_copy_batch_row_preserves_compound_pk() {
        let schema = compound_schema_u64_u64();
        let mut src = ZSetBatch::new(&schema);
        let mut pk_bytes = [0u8; 16];
        pk_bytes[..8].copy_from_slice(&11u64.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&22u64.to_le_bytes());
        src.pks.push_bytes(&pk_bytes);
        src.weights.push(1);
        src.nulls.push(0);
        if let ColData::Fixed(buf) = &mut src.columns[2] {
            buf.extend_from_slice(&5i64.to_le_bytes());
        }

        let mut dst = ZSetBatch::new(&schema);
        super::copy_batch_row(&src, 0, &mut dst, &schema);
        assert_eq!(dst.pks.len(), 1);
        match &dst.pks {
            gnitz_core::PkColumn::Bytes { stride: 16, buf } => {
                assert_eq!(buf, &pk_bytes.to_vec());
            }
            _ => panic!("expected Bytes variant after copy"),
        }
    }

    // ------------------------------------------------------------------
    // extract_sql_literal: SingleQuotedString-only (double-quoted is an
    // identifier in GenericDialect, never a seek literal).
    // ------------------------------------------------------------------

    fn dquote_expr(s: &str) -> Expr {
        Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::DoubleQuotedString(s.into()),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    #[test]
    fn test_double_quoted_not_a_uuid_seek_literal() {
        // A single-quoted UUID is a valid seek key (see
        // test_uuid_index_seek_string_literal); a double-quoted token is an
        // identifier in GenericDialect, so it must NOT be treated as a literal.
        let schema = uuid_schema_payload();
        let sq = eq_expr("uid", uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"));
        assert!(
            super::try_col_eq_literal(&sq, &schema).is_some(),
            "single-quoted UUID is a seek key"
        );

        let dq = eq_expr("uid", dquote_expr("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(
            super::try_col_eq_literal(&dq, &schema),
            None,
            "double-quoted token must not be parsed as a UUID seek literal"
        );
    }

    // ------------------------------------------------------------------
    // eval_expr — Site A: F32/F64 payload returns Err(Unsupported);
    // U64 high-bit regression.
    // ------------------------------------------------------------------

    fn float_col_schema(tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("pk", TypeCode::U64, false), col_def("v", tc, false)],
            pk_cols: vec![0],
        }
    }

    #[test]
    fn eval_expr_f32_payload_returns_unsupported() {
        let schema = float_col_schema(TypeCode::F32);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&1.0f32.to_le_bytes());
        }
        let err = super::eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not supported"), "got: {err}");
    }

    #[test]
    fn eval_expr_f64_payload_returns_unsupported() {
        let schema = float_col_schema(TypeCode::F64);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&1.0f64.to_le_bytes());
        }
        let err = super::eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not supported"), "got: {err}");
    }

    #[test]
    fn eval_expr_u64_high_bit_decodes_correctly() {
        // U64 with the high bit set: the new path must produce the same bitcast
        // as the old path. In i64, this is -1.
        let schema = float_col_schema(TypeCode::U64);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&u64::MAX.to_le_bytes());
        }
        let got = super::eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap();
        assert_eq!(got, Some(-1i64), "U64::MAX must bitcast to -1i64 via new path");
    }

    // ------------------------------------------------------------------
    // try_extract_pk_in — UUID string literals take the fast path
    // ------------------------------------------------------------------

    #[test]
    fn try_extract_pk_in_uuid_string_list_fast_path() {
        let schema = uuid_schema_pk();
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let expected = 0x550e8400_e29b_41d4_a716_446655440000_u128;
        let expr = in_list_expr("id", vec![uuid_str_expr(uuid_str)]);
        let got = super::try_extract_pk_in(&expr, &schema).expect("UUID string IN-list should take fast path");
        assert_eq!(got, vec![expected]);
    }

    #[test]
    fn try_extract_pk_in_uuid_invalid_string_falls_back() {
        let schema = uuid_schema_pk();
        let expr = in_list_expr(
            "id",
            vec![
                uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"),
                uuid_str_expr("not-a-uuid"),
            ],
        );
        assert!(
            super::try_extract_pk_in(&expr, &schema).is_none(),
            "invalid UUID in list should fall back to slow scan"
        );
    }

    // ------------------------------------------------------------------
    // Ordered range-scan extraction
    // ------------------------------------------------------------------

    fn parse_expr_sql(src: &str) -> Expr {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        Parser::new(&GenericDialect {})
            .try_with_sql(src)
            .unwrap()
            .parse_expr()
            .unwrap()
    }

    fn idx_list(metas: &[(&[u32], bool)]) -> Arc<Vec<gnitz_core::IndexMeta>> {
        Arc::new(
            metas
                .iter()
                .map(|(cols, uniq)| gnitz_core::IndexMeta {
                    cols: gnitz_core::PkColList::from_slice(cols),
                    is_unique: *uniq,
                })
                .collect(),
        )
    }

    #[test]
    fn parse_range_cut_saturates() {
        use Cut::{After, Before};
        let ck = |tc, s, neg, mk: fn(u128) -> Cut| super::parse_range_cut(tc, s, neg, mk);
        // In-range literals: `mk` picks the cut side, the value packs to the
        // column's native LE u128 (negatives two's-complement at native width).
        assert_eq!(ck(TypeCode::I32, "5", false, Before), Some(Before(5)));
        assert_eq!(ck(TypeCode::I32, "5", false, After), Some(After(5)));
        assert_eq!(
            ck(TypeCode::I32, "5", true, Before),
            Some(Before((-5i32 as u32) as u128))
        );
        // Saturation is constructor-independent: past the type range, the cut
        // lands on the type edge whichever interval end asked for it — a bound
        // saturating towards its interval becomes a zero-width range.
        let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);
        assert_eq!(ck(TypeCode::I32, "3000000000", false, Before), Some(After(max)));
        assert_eq!(ck(TypeCode::I32, "3000000000", false, After), Some(After(max)));
        assert_eq!(ck(TypeCode::I32, "3000000000", true, Before), Some(Before(min)));
        assert_eq!(ck(TypeCode::I32, "3000000000", true, After), Some(Before(min)));
        assert_eq!(ck(TypeCode::U8, "300", false, Before), Some(After(255)));
        // Non-range-servable scalar types decline.
        assert_eq!(ck(TypeCode::String, "5", false, Before), None);
    }

    #[test]
    fn try_col_range_literal_orientations() {
        use super::RangeSide;
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I64, false)],
            pk_cols: vec![0],
        };
        let ck = |sql: &str| super::try_col_range_literal(&parse_expr_sql(sql), &schema);
        // col > lit and the flipped lit < col are the same start cut, falling
        // above the literal's whole duplicate group.
        let (c, e) = ck("x > 5").unwrap();
        assert_eq!(c, 1);
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        let (_, e) = ck("5 < x").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        // col <= lit / lit >= col → the end side, cutting above the same group
        // (same cut as `x > 5`; only the side differs).
        let (_, e) = ck("x <= 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        let (_, e) = ck("5 >= x").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        // col >= lit / col < lit cut below the group.
        let (_, e) = ck("x >= 5").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == Before(5));
        let (_, e) = ck("x < 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == Before(5));
        // Equality is not a range end.
        assert!(ck("x = 5").is_none());
    }

    /// schema: (id U64 pk, a U64, b U64) → indexable cols a=1, b=2.
    fn abc_schema() -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        }
    }

    #[test]
    fn range_candidate_composite_eq_prefix() {
        use Cut::Before;
        let schema = abc_schema();
        let expr = parse_expr_sql("a = 7 AND b < 50");
        let cands =
            super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1, 2], false)]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!(c.desc.eq_vals(), &[7u128]);
        // No start conjunct → the U64 edge cut; `b < 50` cuts below 50's group.
        assert_eq!(c.desc.start, Before(0));
        assert_eq!(c.desc.end, Before(50));
        assert!(c.residual.is_empty(), "both conjuncts consumed");
    }

    #[test]
    fn range_candidate_between_desugars() {
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I64, false)],
            pk_cols: vec![0],
        };
        // BETWEEN → inclusive lower + inclusive upper, both consumed:
        // [Before(10), After(20)) keeps both boundary groups whole.
        let expr = parse_expr_sql("x BETWEEN 10 AND 20");
        let cands = super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!((c.desc.start, c.desc.end), (Before(10), After(20)));
        assert!(c.residual.is_empty());

        // NOT BETWEEN is non-contiguous → contributes no range end → no candidate.
        let expr = parse_expr_sql("x NOT BETWEEN 10 AND 20");
        let cands = super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn range_candidate_redundant_same_side_keeps_first() {
        use Cut::After;
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        // First lower end becomes the start cut; the second stays a residual
        // (the planner never compares two packed natives to pick the tighter
        // one).
        for sql in ["x > 5 AND x > 10", "x > 10 AND x > 5"] {
            let expr = parse_expr_sql(sql);
            let cands =
                super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
            assert_eq!(cands.len(), 1, "{sql}");
            let c = &cands[0];
            // The FIRST conjunct's cut is taken; the other is residual.
            let first_val: u128 = if sql.starts_with("x > 5") { 5 } else { 10 };
            assert_eq!(c.desc.start, After(first_val), "{sql}");
            assert_eq!(c.residual.len(), 1, "{sql}: other same-side end stays residual");
        }
    }

    #[test]
    fn range_candidate_saturates_out_of_range() {
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I32, false)],
            pk_cols: vec![0],
        };
        let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);

        // Lower above the type max ⇒ a zero-width interval (start == end) the
        // engine rejects byte-wise — no planner-side empty special case.
        let expr = parse_expr_sql("x > 3000000000");
        let c = super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].desc.start, c[0].desc.end), (After(max), After(max)));

        // Upper above the type max ⇒ saturates to the edge (and no lower
        // conjunct widens to the other edge) → full scan.
        let expr = parse_expr_sql("x < 3000000000");
        let c = super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].desc.start, c[0].desc.end), (Before(min), After(max)));
    }

    #[test]
    fn range_candidate_residual_non_range_conjunct() {
        use Cut::After;
        let schema = abc_schema();
        // `b` indexed, `a` not part of the index → `a = 7` stays residual.
        let expr = parse_expr_sql("b > 10 AND a = 7");
        let cands = super::collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[2], false)]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert!(c.desc.eq_vals().is_empty());
        assert_eq!(c.desc.start, After(10));
        assert_eq!(c.residual.len(), 1, "`a = 7` is a residual conjunct");
    }
}
