use sqlparser::ast::{
    Query, SetExpr, Values, Expr, Value, SelectItem,
    Statement, TableObject, BinaryOperator, UnaryOperator,
    FromTable, Assignment, AssignmentTarget,
    OnInsert, OnConflict, OnConflictAction, ConflictTarget,
};
use gnitz_core::{Schema, ZSetBatch, ColData, TypeCode, WireConflictMode};
use gnitz_core::GnitzClient;
use crate::error::{GnitzSqlError, extract_name, extract_table_factor_name};
use crate::binder::Binder;
use crate::logical_plan::BoundExpr;
use crate::SqlResult;

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
fn validate_conflict_target(
    target: &Option<ConflictTarget>,
    schema: &Schema,
) -> Result<(), GnitzSqlError> {
    match target {
        None => Ok(()),
        Some(ConflictTarget::Columns(cols)) => {
            if cols.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "composite ON CONFLICT targets not supported; \
                     single-column PK target only".to_string()
                ));
            }
            let col_name = cols[0].value.as_str();
            let pk_name = schema.columns[schema.pk_index].name.as_str();
            if !col_name.eq_ignore_ascii_case(pk_name) {
                return Err(GnitzSqlError::Unsupported(format!(
                    "ON CONFLICT ({}) — only the primary key column '{}' is \
                     supported as a conflict target",
                    col_name, pk_name
                )));
            }
            Ok(())
        }
        Some(ConflictTarget::OnConstraint(_)) => Err(GnitzSqlError::Unsupported(
            "ON CONFLICT ON CONSTRAINT not supported".to_string()
        )),
    }
}

pub fn execute_insert(
    client:       &GnitzClient,
    _schema_name: &str,
    stmt:         &Statement,
    binder:       &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Extract table name, row source, and ON CONFLICT action.
    let (table_name_str, rows, on_insert) = extract_insert_parts(stmt)?;

    let (tid, schema) = binder.resolve(&table_name_str)?;

    // Resolve the ON CONFLICT clause into a `ConflictPlan`.
    let plan = match on_insert {
        None => ConflictPlan::Error,
        Some(OnInsert::DuplicateKeyUpdate(_)) => {
            return Err(GnitzSqlError::Unsupported(
                "ON DUPLICATE KEY UPDATE not supported — use PostgreSQL-style \
                 ON CONFLICT (col) DO UPDATE".to_string()
            ));
        }
        Some(OnInsert::OnConflict(OnConflict { conflict_target, action })) => {
            validate_conflict_target(conflict_target, &schema)?;

            match action {
                OnConflictAction::DoNothing => ConflictPlan::DoNothingPk,
                OnConflictAction::DoUpdate(do_update) => {
                    if do_update.selection.is_some() {
                        return Err(GnitzSqlError::Unsupported(
                            "ON CONFLICT ... DO UPDATE WHERE not supported in v1".to_string()
                        ));
                    }
                    let assignments = bind_do_update_assignments(
                        &do_update.assignments, &schema, binder,
                    )?;
                    ConflictPlan::DoUpdatePk { assignments }
                }
            }
        }
        Some(_) => {
            return Err(GnitzSqlError::Unsupported(
                "unsupported ON clause in INSERT".to_string()
            ));
        }
    };

    // Build the incoming batch from VALUES rows.
    let mut batch = ZSetBatch::new(&schema);
    let n = rows.len();

    for row in rows {
        let (pk_lo, pk_hi) = extract_pk_value(row, &schema)?;
        batch.pk_lo.push(pk_lo);
        batch.pk_hi.push(pk_hi);
        batch.weights.push(1);

        let mut null_bits: u64 = 0;
        let mut col_data_idx = 0usize; // index into row (skipping PK col)
        let mut payload_idx = 0usize;
        for (ci, col_def) in schema.columns.iter().enumerate() {
            if ci == schema.pk_index {
                // pk column: the actual data is in pk_lo/pk_hi, ColData stays empty
                // we still skip one item from the VALUES row for the PK
                col_data_idx += 1;
                continue;
            }
            if col_data_idx >= row.len() {
                return Err(GnitzSqlError::Bind(
                    format!("not enough values in INSERT row (column {})", ci)
                ));
            }
            let val_expr = &row[col_data_idx];
            col_data_idx += 1;
            // Check if this value is NULL and set the null bitmap
            if is_null_expr(val_expr) {
                null_bits |= 1u64 << payload_idx;
            }
            append_value_to_col(&mut batch.columns[ci], col_def.type_code, val_expr)?;
            payload_idx += 1;
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
            let (filtered, surviving_count) =
                client_side_filter_do_nothing(client, tid, &schema, &batch)?;
            if surviving_count > 0 {
                client.push_with_mode(tid, &schema, &filtered, WireConflictMode::Error)?;
            }
            Ok(SqlResult::RowsAffected { count: surviving_count })
        }
        ConflictPlan::DoUpdatePk { assignments } => {
            let merged =
                client_side_merge_do_update(client, tid, &schema, &batch, &assignments)?;
            if !merged.pk_lo.is_empty() {
                // Use Update mode: merged batch contains both +1 (new
                // merged rows, which may UPSERT) and untouched +1 rows
                // for non-conflicting inserts. Workers handle the
                // retract-and-insert via enforce_unique_pk_partitioned.
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
    for assignment in raw {
        let col_name = extract_assignment_col_name(assignment)?;
        let col_idx = schema.columns.iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col_name))
            .ok_or_else(|| GnitzSqlError::Bind(format!(
                "column '{}' not found in ON CONFLICT DO UPDATE SET", col_name
            )))?;
        if col_idx == schema.pk_index {
            return Err(GnitzSqlError::Unsupported(
                "cannot assign to primary key column in ON CONFLICT DO UPDATE".to_string()
            ));
        }
        // Recognize EXCLUDED.col as a special form. sqlparser parses it
        // as a CompoundIdentifier: `EXCLUDED`.`col`.
        let value = bind_do_update_rhs(&assignment.value, schema, binder)?;
        out.push((col_idx, value));
    }
    Ok(out)
}

fn bind_do_update_rhs(
    expr: &Expr,
    schema: &Schema,
    binder: &mut Binder<'_>,
) -> Result<BoundUpdateExpr, GnitzSqlError> {
    // `EXCLUDED.col` — sqlparser produces `CompoundIdentifier`.
    if let Expr::CompoundIdentifier(parts) = expr {
        if parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("EXCLUDED") {
            let col_name = parts[1].value.as_str();
            let col_idx = schema.columns.iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| GnitzSqlError::Bind(format!(
                    "EXCLUDED.{}: column not found", col_name
                )))?;
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
             supported; use a simple `col = EXCLUDED.col` assignment".to_string()
        ));
    }
    let bound = binder.bind_expr(expr, schema)?;
    Ok(BoundUpdateExpr::Existing(bound))
}

/// Returns true if `expr` contains any `EXCLUDED.<col>` compound identifier.
fn expr_contains_excluded(expr: &Expr) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) =>
            parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("EXCLUDED"),
        Expr::BinaryOp { left, right, .. } =>
            expr_contains_excluded(left) || expr_contains_excluded(right),
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
    client: &GnitzClient,
    tid: u64,
    schema: &Schema,
    batch: &ZSetBatch,
) -> Result<(ZSetBatch, usize), GnitzSqlError> {
    let mut seen_pks: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();
    let mut surviving_indices: Vec<usize> = Vec::with_capacity(batch.pk_lo.len());

    for i in 0..batch.pk_lo.len() {
        let pk = (batch.pk_lo[i], batch.pk_hi[i]);
        // Intra-batch duplicate: drop everything after the first.
        if !seen_pks.insert(pk) { continue; }
        // Existing-store check.
        let (_sch, found, _lsn) = client.seek(tid, pk.0, pk.1)?;
        let exists = matches!(found, Some(b) if !b.pk_lo.is_empty());
        if exists { continue; }
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
///     merged row as +1 (worker's enforce_unique_pk_partitioned will
///     retract the old payload).
///   - For each incoming row whose PK does NOT exist: pass through as a
///     plain +1 insert.
///
/// Intra-batch duplicate PKs are rejected — this matches PG's
/// "command cannot affect row a second time" behavior.
fn client_side_merge_do_update(
    client: &GnitzClient,
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

    let mut seen_pks: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();
    let mut out = ZSetBatch::new(schema);

    for i in 0..batch.pk_lo.len() {
        let pk = (batch.pk_lo[i], batch.pk_hi[i]);
        if !seen_pks.insert(pk) {
            return Err(GnitzSqlError::Bind(
                "ON CONFLICT DO UPDATE cannot affect row a second time \
                 (duplicate PK in the same batch)".to_string()
            ));
        }

        let (_sch, existing_opt, _lsn) = client.seek(tid, pk.0, pk.1)?;
        let existing = existing_opt.filter(|b| !b.pk_lo.is_empty());

        match existing {
            None => {
                copy_batch_row(batch, i, &mut out, schema);
            }
            Some(existing_batch) => {
                out.pk_lo.push(pk.0);
                out.pk_hi.push(pk.1);
                out.weights.push(1);

                // Start with the existing row's null bits; assignments
                // that resolve to NULL will flip their bit below.
                let mut null_bits: u64 = existing_batch.nulls[0];

                for (ci, col_def) in schema.columns.iter().enumerate() {
                    if ci == schema.pk_index { continue; }
                    let payload_idx = if ci < schema.pk_index { ci } else { ci - 1 };

                    if let Some(rhs) = asn_by_col[ci] {
                        let cv = eval_do_update_rhs(
                            rhs, &existing_batch, batch, i, schema,
                        )?;
                        if matches!(cv, ColumnValue::Null) {
                            null_bits |= 1u64 << payload_idx;
                        } else {
                            null_bits &= !(1u64 << payload_idx);
                        }
                        append_column_value(&mut out.columns[ci], cv, col_def.type_code)?;
                    } else {
                        let stride = col_def.type_code.wire_stride();
                        match (&existing_batch.columns[ci], &mut out.columns[ci]) {
                            (ColData::Fixed(s), ColData::Fixed(d)) => {
                                d.extend_from_slice(&s[0..stride]);
                            }
                            (ColData::Strings(s), ColData::Strings(d)) => {
                                d.push(s[0].clone());
                            }
                            (ColData::U128s(s), ColData::U128s(d)) => {
                                d.push(s[0]);
                            }
                            _ => unreachable!("mismatched ColData variants for column {}", ci),
                        }
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


fn extract_insert_parts(
    stmt: &Statement,
) -> Result<(String, &[Vec<Expr>], Option<&OnInsert>), GnitzSqlError> {
    match stmt {
        Statement::Insert(insert) => {
            let table_name = match &insert.table {
                TableObject::TableName(obj_name) => extract_name(obj_name, "INSERT")?,
                _ => return Err(GnitzSqlError::Unsupported("INSERT with table function not supported".to_string())),
            };

            let source = insert.source.as_ref()
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
            "INSERT only supports VALUES (not INSERT INTO ... SELECT)".to_string()
        )),
    }
}

/// Split a u128 into (lo, hi) halves for the pk_lo/pk_hi representation.
#[inline]
fn u128_lo_hi(v: u128) -> (u64, u64) {
    (v as u64, (v >> 64) as u64)
}

/// Extract the primary key from a VALUES row, returning (pk_lo, pk_hi).
///
/// For U128 PKs the 128-bit value is split into (lo, hi) halves.
/// For all other numeric PK types hi is always 0.
fn extract_pk_value(row: &[Expr], schema: &Schema) -> Result<(u64, u64), GnitzSqlError> {
    let pk_expr = row.get(schema.pk_index).ok_or_else(|| {
        GnitzSqlError::Bind("PK column missing from INSERT row".to_string())
    })?;
    match pk_expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => {
                if schema.columns[schema.pk_index].type_code == TypeCode::U128 {
                    let val = n.parse::<u128>().map_err(|_| {
                        GnitzSqlError::Bind(format!("PK value is not a valid u128: {}", n))
                    })?;
                    Ok(u128_lo_hi(val))
                } else {
                    let val = n.parse::<u64>().map_err(|_| {
                        GnitzSqlError::Bind(format!("PK value is not a valid u64: {}", n))
                    })?;
                    Ok((val, 0))
                }
            }
            _ => Err(GnitzSqlError::Bind("PK value must be a numeric literal".to_string())),
        },
        _ => Err(GnitzSqlError::Bind("PK value must be a numeric literal".to_string())),
    }
}

fn is_null_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(vws) if matches!(vws.value, Value::Null))
}

fn append_value_to_col(
    col:      &mut ColData,
    tc:       TypeCode,
    val_expr: &Expr,
) -> Result<(), GnitzSqlError> {
    // sqlparser parses negative number literals as UnaryOp(Minus, Number(...)).
    // Unwrap here so the rest of the function sees a plain Value::Number.
    let (val_expr, negated) = match val_expr {
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => (expr.as_ref(), true),
        e => (e, false),
    };

    match val_expr {
        Expr::Value(vws) => match &vws.value {
            Value::Null => {
                match col {
                    ColData::Strings(v) => { v.push(None); }
                    ColData::Fixed(buf) => {
                        let stride = tc.wire_stride();
                        buf.extend(std::iter::repeat(0u8).take(stride));
                    }
                    ColData::U128s(v) => { v.push(0u128); }
                }
                Ok(())
            }
            Value::Number(n, _) => {
                // Build the effective numeric string with optional leading minus.
                let neg_buf;
                let n: &str = if negated {
                    neg_buf = format!("-{}", n);
                    &neg_buf
                } else {
                    n.as_str()
                };
                match col {
                    ColData::Fixed(buf) => {
                        match tc {
                            TypeCode::U8  => { let v = n.parse::<u8>().map_err(|_| GnitzSqlError::Bind(format!("invalid u8: {}", n)))?; buf.push(v); }
                            TypeCode::I8  => { let v = n.parse::<i8>().map_err(|_| GnitzSqlError::Bind(format!("invalid i8: {}", n)))?; buf.push(v as u8); }
                            TypeCode::U16 => { let v = n.parse::<u16>().map_err(|_| GnitzSqlError::Bind(format!("invalid u16: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::I16 => { let v = n.parse::<i16>().map_err(|_| GnitzSqlError::Bind(format!("invalid i16: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::U32 => { let v = n.parse::<u32>().map_err(|_| GnitzSqlError::Bind(format!("invalid u32: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::I32 => { let v = n.parse::<i32>().map_err(|_| GnitzSqlError::Bind(format!("invalid i32: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::U64 => { let v = n.parse::<u64>().map_err(|_| GnitzSqlError::Bind(format!("invalid u64: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::I64 => { let v = n.parse::<i64>().map_err(|_| GnitzSqlError::Bind(format!("invalid i64: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::F32 => { let v = n.parse::<f32>().map_err(|_| GnitzSqlError::Bind(format!("invalid f32: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            TypeCode::F64 => { let v = n.parse::<f64>().map_err(|_| GnitzSqlError::Bind(format!("invalid f64: {}", n)))?; buf.extend_from_slice(&v.to_le_bytes()); }
                            _ => return Err(GnitzSqlError::Bind(format!("unexpected type {:?} for number literal", tc))),
                        }
                        Ok(())
                    }
                    ColData::U128s(v) => {
                        let val = n.parse::<u128>().map_err(|_| GnitzSqlError::Bind(format!("invalid u128: {}", n)))?;
                        v.push(val);
                        Ok(())
                    }
                    ColData::Strings(_) => Err(GnitzSqlError::Bind(
                        "number literal for string column".to_string()
                    )),
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                match col {
                    ColData::Strings(v) => { v.push(Some(s.clone())); Ok(()) }
                    _ => Err(GnitzSqlError::Bind("string literal for non-string column".to_string())),
                }
            }
            _ => Err(GnitzSqlError::Unsupported(
                format!("unsupported value in INSERT: {:?}", vws.value)
            )),
        },
        _ => Err(GnitzSqlError::Unsupported(
            format!("unsupported value expression in INSERT: {:?}", val_expr)
        )),
    }
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

pub fn execute_select(
    client:       &GnitzClient,
    _schema_name: &str,
    query:        &Query,
    binder:       &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    if query.order_by.is_some() {
        return Err(GnitzSqlError::Unsupported("ORDER BY not supported".to_string()));
    }
    let limit = extract_limit(query);

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(GnitzSqlError::Unsupported("only SELECT ... FROM is supported".to_string())),
    };

    // Exactly one FROM table, no joins
    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported("SELECT must have exactly one FROM table".to_string()));
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(GnitzSqlError::Unsupported("JOINs not supported in direct SELECT".to_string()));
    }
    let table_name = extract_table_factor_name(&from.relation, "FROM")?;

    let (tid, schema) = binder.resolve(&table_name)?;

    // Check WHERE clause
    let (schema_out, batch_opt, _) = if let Some(where_expr) = &select.selection {
        if let Some((pk_lo, pk_hi)) = try_extract_pk_seek(where_expr, &schema) {
            client.seek(tid, pk_lo, pk_hi)?
        } else if let Some((col_idx, key_lo, key_hi, residual)) =
            try_extract_index_seek(where_expr, &schema, binder, tid, false)?
        {
            let (s, b, lsn) = client.seek_by_index(tid, col_idx as u64, key_lo, key_hi)?;
            let (s2, b2) = apply_residual_filter((s, b), residual.as_ref(), &schema)?;
            (s2, b2, lsn)
        } else {
            return Err(GnitzSqlError::Unsupported(
                "WHERE on non-indexed column not supported in direct SELECT; \
                 use CREATE INDEX first, or CREATE VIEW for server-side filtering".to_string()
            ));
        }
    } else {
        client.scan(tid)?
    };

    // Use the resolved schema for column metadata
    let actual_schema = schema_out.unwrap_or_else(|| (*schema).clone());

    // Apply projection
    let (proj_schema, proj_batch) = apply_projection(
        &select.projection, &actual_schema, batch_opt, &binder
    )?;

    // Apply LIMIT
    let final_batch = if let Some(lim) = limit {
        apply_limit(proj_batch, &proj_schema, lim)
    } else {
        proj_batch
    };

    Ok(SqlResult::Rows { schema: proj_schema, batch: final_batch })
}

fn extract_limit(query: &Query) -> Option<usize> {
    use sqlparser::ast::LimitClause;
    match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit: Some(Expr::Value(vws)), .. }) => {
            if let Value::Number(n, _) = &vws.value { n.parse::<usize>().ok() } else { None }
        }
        Some(LimitClause::OffsetCommaLimit { limit: Expr::Value(vws), .. }) => {
            if let Value::Number(n, _) = &vws.value { n.parse::<usize>().ok() } else { None }
        }
        _ => None,
    }
}

/// Returns Some((pk_lo, pk_hi)) if the expression is `pk_col = literal`, else None.
fn try_extract_pk_seek(expr: &Expr, schema: &Schema) -> Option<(u64, u64)> {
    let (col_idx, key_lo, key_hi) = try_col_eq_literal(expr, schema)?;
    if col_idx == schema.pk_index { Some((key_lo, key_hi)) } else { None }
}

/// Extracts (col_idx, key_lo, key_hi) from `col = integer_literal`. Does NOT check index existence.
///
/// For U128 columns the 128-bit literal is split into (lo, hi); for all other types hi is 0.
fn try_col_eq_literal(expr: &Expr, schema: &Schema) -> Option<(usize, u64, u64)> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            // sqlparser represents negative number literals as UnaryOp(Minus, Number("N")).
            let is_num_or_neg = |e: &Expr| match e {
                Expr::Value(vws) => matches!(vws.value, Value::Number(_, _)),
                Expr::UnaryOp { op: UnaryOperator::Minus, expr } =>
                    matches!(expr.as_ref(), Expr::Value(vws) if matches!(vws.value, Value::Number(_, _))),
                _ => false,
            };
            let (col_expr, lit_expr) = match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(_), r) if is_num_or_neg(r) => (left.as_ref(), right.as_ref()),
                (l, Expr::Identifier(_)) if is_num_or_neg(l) => (right.as_ref(), left.as_ref()),
                _ => return None,
            };
            let col_name = if let Expr::Identifier(id) = col_expr { &id.value } else { return None; };
            // Extract (negated, numeric_string) from the literal side.
            let (negated, n_str): (bool, &str) = match lit_expr {
                Expr::Value(vws) => {
                    if let Value::Number(n, _) = &vws.value { (false, n.as_str()) } else { return None; }
                }
                Expr::UnaryOp { op: UnaryOperator::Minus, expr } => {
                    if let Expr::Value(vws) = expr.as_ref() {
                        if let Value::Number(n, _) = &vws.value { (true, n.as_str()) } else { return None; }
                    } else { return None; }
                }
                _ => return None,
            };
            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;
            if schema.columns[col_idx].type_code == TypeCode::U128 {
                if negated { return None; }
                let val = n_str.parse::<u128>().ok()?;
                let (lo, hi) = u128_lo_hi(val);
                Some((col_idx, lo, hi))
            } else {
                // Secondary indexes store column values as zero-extended LE bytes cast to u64.
                // For signed types, we must produce the same bit pattern by casting through
                // the type's unsigned equivalent (not sign-extending to 64 bits).
                let key_lo = match schema.columns[col_idx].type_code {
                    TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
                        let v = n_str.parse::<i64>().ok()?;
                        let v = if negated { v.checked_neg()? } else { v };
                        match schema.columns[col_idx].type_code {
                            TypeCode::I8  => (v as i8  as u8)  as u64,
                            TypeCode::I16 => (v as i16 as u16) as u64,
                            TypeCode::I32 => (v as i32 as u32) as u64,
                            _             => v as u64,
                        }
                    }
                    _ => {
                        if negated { return None; }
                        n_str.parse::<u64>().ok()?
                    }
                };
                Some((col_idx, key_lo, 0u64))
            }
        }
        _ => None,
    }
}

/// Returns Some((col_idx, key_lo, key_hi, residual)) when WHERE contains an equality on an indexed column.
/// When `require_unique` is true, only matches indexes where `is_unique` is set.
fn try_extract_index_seek(
    expr:           &Expr,
    schema:         &Schema,
    binder:         &mut Binder<'_>,
    table_id:       u64,
    require_unique: bool,
) -> Result<Option<(usize, u64, u64, Option<BoundExpr>)>, GnitzSqlError> {
    // Case 1: simple `col = literal`
    if let Some((col_idx, key_lo, key_hi)) = try_col_eq_literal(expr, schema) {
        if col_idx != schema.pk_index {
            if let Some((_, is_unique)) = binder.find_index(table_id, col_idx)? {
                if !require_unique || is_unique {
                    return Ok(Some((col_idx, key_lo, key_hi, None)));
                }
            }
        }
    }
    // Case 2: `(indexed_col = literal) AND rest`
    if let Expr::BinaryOp { left, op: BinaryOperator::And, right } = expr {
        for (candidate, other) in [
            (left.as_ref(), right.as_ref()),
            (right.as_ref(), left.as_ref()),
        ] {
            if let Some((col_idx, key_lo, key_hi)) = try_col_eq_literal(candidate, schema) {
                if col_idx != schema.pk_index {
                    if let Some((_, is_unique)) = binder.find_index(table_id, col_idx)? {
                        if !require_unique || is_unique {
                            let residual = binder.bind_expr(other, schema)?;
                            return Ok(Some((col_idx, key_lo, key_hi, Some(residual))));
                        }
                    }
                }
            }
        }
    }
    Ok(None)
}

fn apply_residual_filter(
    result:  (Option<Schema>, Option<ZSetBatch>),
    pred:    Option<&BoundExpr>,
    schema:  &Schema,
) -> Result<(Option<Schema>, Option<ZSetBatch>), GnitzSqlError> {
    let pred = match pred {
        None => return Ok(result),
        Some(p) => p,
    };
    let (schema_opt, batch_opt) = result;
    let batch = match batch_opt {
        None => return Ok((schema_opt, None)),
        Some(b) => b,
    };
    let actual_schema = schema_opt.as_ref().unwrap_or(schema);
    let mut new_batch = ZSetBatch::new(actual_schema);

    for i in 0..batch.pk_lo.len() {
        if eval_pred_row(pred, &batch, i, actual_schema)? {
            copy_batch_row(&batch, i, &mut new_batch, actual_schema);
        }
    }
    Ok((schema_opt, Some(new_batch)))
}

enum ColumnValue { Int(i64), #[allow(dead_code)] Float(f64), Str(String), Null }

fn eval_pred_row(
    pred:   &BoundExpr,
    batch:  &ZSetBatch,
    i:      usize,
    schema: &Schema,
) -> Result<bool, GnitzSqlError> {
    // SQL NULL in a predicate position is UNKNOWN → treated as false (row excluded).
    Ok(eval_expr(pred, batch, i, schema)?.map_or(false, |v| v != 0))
}

/// Evaluate a bound expression against a single row.
///
/// Returns `None` when the result is SQL NULL. A NULL column operand propagates
/// through all arithmetic and comparison operators. Callers that need a boolean
/// predicate should treat `None` as `false` (UNKNOWN → row excluded).
fn eval_expr(
    expr:   &BoundExpr,
    batch:  &ZSetBatch,
    i:      usize,
    schema: &Schema,
) -> Result<Option<i64>, GnitzSqlError> {
    use crate::logical_plan::BinOp;
    match expr {
        BoundExpr::ColRef(c) => {
            // PK is never null. Payload columns must be checked via the null bitmap.
            if *c != schema.pk_index {
                let payload_idx = if *c < schema.pk_index { *c } else { *c - 1 };
                if (batch.nulls[i] & (1u64 << payload_idx)) != 0 {
                    return Ok(None);
                }
            }
            let col_def = &schema.columns[*c];
            match &batch.columns[*c] {
                ColData::Fixed(buf) => {
                    let stride = col_def.type_code.wire_stride();
                    let start  = i * stride;
                    let slice  = &buf[start..start + stride];
                    // Decode using the actual type code so unsigned columns
                    // are not sign-extended through an intermediate signed cast.
                    let v: i64 = match col_def.type_code {
                        TypeCode::U8  => slice[0] as i64,
                        TypeCode::I8  => slice[0] as i8 as i64,
                        TypeCode::U16 => u16::from_le_bytes(slice.try_into().unwrap()) as i64,
                        TypeCode::I16 => i16::from_le_bytes(slice.try_into().unwrap()) as i64,
                        TypeCode::U32 => u32::from_le_bytes(slice.try_into().unwrap()) as i64,
                        TypeCode::I32 => i32::from_le_bytes(slice.try_into().unwrap()) as i64,
                        _             => i64::from_le_bytes(slice.try_into().unwrap()),
                    };
                    Ok(Some(v))
                }
                ColData::Strings(_) => Err(GnitzSqlError::Unsupported(
                    "residual filter on string column not supported".to_string()
                )),
                ColData::U128s(_) => Err(GnitzSqlError::Unsupported(
                    "residual filter on U128 column not supported; \
                     use a primary-key seek or CREATE INDEX for equality lookups".to_string()
                )),
            }
        }
        BoundExpr::LitInt(v) => Ok(Some(*v)),
        BoundExpr::LitFloat(_) => Err(GnitzSqlError::Unsupported(
            "float literals in residual filter not supported".to_string()
        )),
        BoundExpr::LitStr(_) => Err(GnitzSqlError::Unsupported(
            "string literals in WHERE predicate not supported; \
             use CREATE INDEX or CREATE VIEW".to_string()
        )),
        BoundExpr::BinOp(l, op, r) => {
            let lv = match eval_expr(l, batch, i, schema)? {
                None => return Ok(None),
                Some(v) => v,
            };
            let rv = match eval_expr(r, batch, i, schema)? {
                None => return Ok(None),
                Some(v) => v,
            };
            Ok(Some(match op {
                BinOp::Add => lv.wrapping_add(rv),
                BinOp::Sub => lv.wrapping_sub(rv),
                BinOp::Mul => lv.wrapping_mul(rv),
                // wrapping_div/rem prevent the i64::MIN / -1 overflow panic.
                BinOp::Div => if rv == 0 { 0 } else { lv.wrapping_div(rv) },
                BinOp::Mod => if rv == 0 { 0 } else { lv.wrapping_rem(rv) },
                BinOp::Eq  => (lv == rv) as i64,
                BinOp::Ne  => (lv != rv) as i64,
                BinOp::Gt  => (lv >  rv) as i64,
                BinOp::Ge  => (lv >= rv) as i64,
                BinOp::Lt  => (lv <  rv) as i64,
                BinOp::Le  => (lv <= rv) as i64,
                BinOp::And => ((lv != 0) && (rv != 0)) as i64,
                BinOp::Or  => ((lv != 0) || (rv != 0)) as i64,
            }))
        }
        BoundExpr::UnaryOp(op, e) => {
            let v = match eval_expr(e, batch, i, schema)? {
                None => return Ok(None),
                Some(v) => v,
            };
            use crate::logical_plan::UnaryOp;
            Ok(Some(match op {
                UnaryOp::Neg => v.wrapping_neg(),
                UnaryOp::Not => (v == 0) as i64,
            }))
        }
        BoundExpr::IsNull(c) => {
            if *c == schema.pk_index {
                return Ok(Some(0)); // PK is never null → IS NULL is always false
            }
            let payload_idx = if *c < schema.pk_index { *c } else { *c - 1 };
            let is_null = (batch.nulls[i] & (1u64 << payload_idx)) != 0;
            Ok(Some(is_null as i64))
        }
        BoundExpr::IsNotNull(c) => {
            if *c == schema.pk_index {
                return Ok(Some(1)); // PK is never null → IS NOT NULL is always true
            }
            let payload_idx = if *c < schema.pk_index { *c } else { *c - 1 };
            let is_null = (batch.nulls[i] & (1u64 << payload_idx)) != 0;
            Ok(Some(!is_null as i64))
        }
        BoundExpr::AggCall { .. } => Err(GnitzSqlError::Unsupported(
            "aggregate functions not allowed in this context".to_string()
        )),
    }
}

fn copy_batch_row(src: &ZSetBatch, i: usize, dst: &mut ZSetBatch, schema: &Schema) {
    dst.pk_lo.push(src.pk_lo[i]);
    dst.pk_hi.push(src.pk_hi[i]);
    dst.weights.push(src.weights[i]);
    dst.nulls.push(src.nulls[i]);
    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index { continue; }
        let stride = col_def.type_code.wire_stride();
        match (&src.columns[ci], &mut dst.columns[ci]) {
            (ColData::Fixed(s), ColData::Fixed(d)) => {
                d.extend_from_slice(&s[i * stride..(i + 1) * stride]);
            }
            (ColData::Strings(s), ColData::Strings(d)) => {
                d.push(s[i].clone());
            }
            (ColData::U128s(s), ColData::U128s(d)) => {
                d.push(s[i]);
            }
            _ => unreachable!("mismatched ColData variants for column {}", ci),
        }
    }
}

fn apply_projection(
    projection: &[SelectItem],
    schema:     &Schema,
    batch:      Option<ZSetBatch>,
    _binder:    &Binder<'_>,
) -> Result<(Schema, ZSetBatch), GnitzSqlError> {
    // Check if projection is wildcard
    let is_wildcard = projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));

    if is_wildcard {
        let b = batch.unwrap_or_else(|| ZSetBatch::new(schema));
        return Ok((schema.clone(), b));
    }

    // Extract named columns; dedup preserving first-occurrence order
    let mut col_indices: Vec<usize> = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("column '{}' not found in projection", ident.value)
                    ))?;
                if !col_indices.contains(&idx) { col_indices.push(idx); }
            }
            SelectItem::Wildcard(_) => {
                for i in 0..schema.columns.len() {
                    if !col_indices.contains(&i) { col_indices.push(i); }
                }
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "only simple column references supported in SELECT projection".to_string()
            )),
        }
    }

    let src_batch = batch.unwrap_or_else(|| ZSetBatch::new(schema));
    let new_cols: Vec<gnitz_core::ColumnDef> = col_indices.iter()
        .map(|&i| schema.columns[i].clone())
        .collect();
    let new_pk_idx = col_indices.iter().position(|&i| i == schema.pk_index).unwrap_or(0);
    let new_schema = Schema { columns: new_cols, pk_index: new_pk_idx };

    // Destructure src_batch so system columns can be moved (not cloned)
    let ZSetBatch { pk_lo, pk_hi, weights, nulls, columns: src_columns } = src_batch;
    let mut new_batch = ZSetBatch::new(&new_schema);
    new_batch.pk_lo   = pk_lo;
    new_batch.pk_hi   = pk_hi;
    new_batch.weights = weights;
    new_batch.nulls   = nulls;

    for (new_ci, &old_ci) in col_indices.iter().enumerate() {
        if new_ci == new_pk_idx { continue; }
        if old_ci == schema.pk_index { continue; }
        match (&src_columns[old_ci], &mut new_batch.columns[new_ci]) {
            (ColData::Fixed(src), ColData::Fixed(dst)) => {
                dst.extend_from_slice(src);
            }
            (ColData::Strings(src), ColData::Strings(dst)) => {
                dst.extend(src.iter().cloned());
            }
            (ColData::U128s(src), ColData::U128s(dst)) => {
                dst.extend(src.iter().copied());
            }
            _ => {}
        }
    }

    Ok((new_schema, new_batch))
}

fn apply_limit(mut batch: ZSetBatch, schema: &Schema, limit: usize) -> ZSetBatch {
    let n = batch.pk_lo.len();
    if n <= limit { return batch; }

    batch.pk_lo.truncate(limit);
    batch.pk_hi.truncate(limit);
    batch.weights.truncate(limit);
    batch.nulls.truncate(limit);

    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index { continue; }
        match &mut batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = col_def.type_code.wire_stride();
                buf.truncate(limit * stride);
            }
            ColData::Strings(v) => { v.truncate(limit); }
            ColData::U128s(v) => { v.truncate(limit); }
        }
    }
    batch
}

// ---------------------------------------------------------------------------
// UPDATE / DELETE helpers
// ---------------------------------------------------------------------------

fn eval_set_expr(
    expr:    &BoundExpr,
    batch:   &ZSetBatch,
    row_idx: usize,
    schema:  &Schema,
) -> Result<ColumnValue, GnitzSqlError> {
    match expr {
        BoundExpr::LitStr(s) => return Ok(ColumnValue::Str(s.clone())),
        BoundExpr::ColRef(c) => {
            if let ColData::Strings(v) = &batch.columns[*c] {
                return Ok(match &v[row_idx] {
                    Some(s) => ColumnValue::Str(s.clone()),
                    None    => ColumnValue::Null,
                });
            }
            // fall through: numeric column handled by eval_expr below
        }
        _ => {}
    }
    match eval_expr(expr, batch, row_idx, schema)? {
        None    => Ok(ColumnValue::Null),
        Some(v) => Ok(ColumnValue::Int(v)),
    }
}

fn append_column_value(col: &mut ColData, cv: ColumnValue, tc: TypeCode) -> Result<(), GnitzSqlError> {
    match cv {
        ColumnValue::Null => {
            match col {
                ColData::Fixed(buf) => buf.extend(std::iter::repeat(0u8).take(tc.wire_stride())),
                ColData::Strings(v) => v.push(None),
                ColData::U128s(v)   => v.push(0u128),
            }
        }
        ColumnValue::Int(i) => {
            match col {
                ColData::Fixed(buf) => match tc {
                    TypeCode::U8  => buf.push(i as u8),
                    TypeCode::I8  => buf.push(i as u8),
                    TypeCode::U16 => buf.extend_from_slice(&(i as u16).to_le_bytes()),
                    TypeCode::I16 => buf.extend_from_slice(&(i as i16).to_le_bytes()),
                    TypeCode::U32 => buf.extend_from_slice(&(i as u32).to_le_bytes()),
                    TypeCode::I32 => buf.extend_from_slice(&(i as i32).to_le_bytes()),
                    TypeCode::U64 => buf.extend_from_slice(&(i as u64).to_le_bytes()),
                    TypeCode::I64 => buf.extend_from_slice(&i.to_le_bytes()),
                    _ => return Err(GnitzSqlError::Bind(format!("cannot assign Int to {:?}", tc))),
                },
                _ => return Err(GnitzSqlError::Bind("Int value for non-numeric column".to_string())),
            }
        }
        ColumnValue::Float(f) => {
            match col {
                ColData::Fixed(buf) => match tc {
                    TypeCode::F32 => buf.extend_from_slice(&(f as f32).to_le_bytes()),
                    TypeCode::F64 => buf.extend_from_slice(&f.to_le_bytes()),
                    _ => return Err(GnitzSqlError::Bind(format!("cannot assign Float to {:?}", tc))),
                },
                _ => return Err(GnitzSqlError::Bind("Float value for non-float column".to_string())),
            }
        }
        ColumnValue::Str(s) => {
            match col {
                ColData::Strings(v) => v.push(Some(s)),
                _ => return Err(GnitzSqlError::Bind("String value for non-string column".to_string())),
            }
        }
    }
    Ok(())
}

fn extract_assignment_col_name(assignment: &Assignment) -> Result<String, GnitzSqlError> {
    match &assignment.target {
        AssignmentTarget::ColumnName(obj_name) => extract_name(obj_name, "UPDATE SET"),
        _ => Err(GnitzSqlError::Unsupported(
            "only simple column assignments supported in UPDATE SET".to_string()
        )),
    }
}

fn write_set_columns(
    current:     &ZSetBatch,
    row_idx:     usize,
    assignments: &[(usize, BoundExpr)],
    schema:      &Schema,
    dst:         &mut ZSetBatch,
) -> Result<(), GnitzSqlError> {
    dst.pk_lo.push(current.pk_lo[row_idx]);
    dst.pk_hi.push(current.pk_hi[row_idx]);
    dst.weights.push(1);
    // Start with the existing row's null bits; each assignment updates only
    // its own column's bit (set for a NULL result, clear for non-NULL).
    let mut null_bits = current.nulls[row_idx];

    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index { continue; }
        let payload_idx = if ci < schema.pk_index { ci } else { ci - 1 };
        if let Some((_, expr)) = assignments.iter().find(|(idx, _)| *idx == ci) {
            let cv = eval_set_expr(expr, current, row_idx, schema)?;
            if matches!(cv, ColumnValue::Null) {
                null_bits |= 1u64 << payload_idx;
            } else {
                null_bits &= !(1u64 << payload_idx);
            }
            append_column_value(&mut dst.columns[ci], cv, col_def.type_code)?;
        } else {
            let stride = col_def.type_code.wire_stride();
            match (&current.columns[ci], &mut dst.columns[ci]) {
                (ColData::Fixed(s), ColData::Fixed(d)) => {
                    d.extend_from_slice(&s[row_idx * stride..(row_idx + 1) * stride]);
                }
                (ColData::Strings(s), ColData::Strings(d)) => { d.push(s[row_idx].clone()); }
                (ColData::U128s(s), ColData::U128s(d))     => { d.push(s[row_idx]); }
                _ => unreachable!("mismatched ColData variants for column {}", ci),
            }
        }
    }
    dst.nulls.push(null_bits);
    Ok(())
}

fn try_extract_pk_in(expr: &Expr, schema: &Schema) -> Option<Vec<(u64, u64)>> {
    if let Expr::InList { expr: col_expr, list, negated, .. } = expr {
        if *negated { return None; }
        let col_name = match col_expr.as_ref() {
            Expr::Identifier(id) => &id.value,
            _ => return None,
        };
        let pk_col = &schema.columns[schema.pk_index];
        if !pk_col.name.eq_ignore_ascii_case(col_name) { return None; }
        let mut pks = Vec::with_capacity(list.len());
        for item in list {
            if let Expr::Value(vws) = item {
                if let Value::Number(n, _) = &vws.value {
                    pks.push((n.parse::<u64>().ok()?, 0u64));
                    continue;
                }
            }
            return None;
        }
        Some(pks)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

pub fn execute_update(
    client:       &GnitzClient,
    _schema_name: &str,
    stmt:         &Statement,
    binder:       &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let (table, assignments_raw, selection) = match stmt {
        Statement::Update { table, assignments, selection, .. } => (table, assignments, selection),
        _ => return Err(GnitzSqlError::Bind("not an UPDATE statement".to_string())),
    };

    let table_name = extract_table_factor_name(&table.relation, "UPDATE")?;

    let (table_id, schema) = binder.resolve(&table_name)?;

    // Bind SET assignments; reject any assignment to the PK column
    let mut assignments: Vec<(usize, BoundExpr)> = Vec::new();
    for assignment in assignments_raw {
        let col_name = extract_assignment_col_name(assignment)?;
        let col_idx = schema.columns.iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col_name))
            .ok_or_else(|| GnitzSqlError::Bind(
                format!("column '{}' not found in UPDATE SET", col_name)
            ))?;
        if col_idx == schema.pk_index {
            return Err(GnitzSqlError::Unsupported(
                "cannot UPDATE primary key column".to_string()
            ));
        }
        let bound_val = binder.bind_expr(&assignment.value, &schema)?;
        assignments.push((col_idx, bound_val));
    }

    if let Some(where_expr) = selection {
        // Path 1: PK equality
        if let Some((pk_lo, pk_hi)) = try_extract_pk_seek(where_expr, &schema) {
            let (schema_opt, batch_opt, _) = client.seek(table_id, pk_lo, pk_hi)?;
            let current = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) if b.pk_lo.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
            let mut new_batch = ZSetBatch::new(actual_schema);
            write_set_columns(&current, 0, &assignments, actual_schema, &mut new_batch)?;
            client.push_with_mode(table_id, actual_schema, &new_batch, WireConflictMode::Update)?;
            return Ok(SqlResult::RowsAffected { count: 1 });
        }

        // Path 2: Unique index seek
        if let Some((col_idx, key_lo, key_hi, residual)) =
            try_extract_index_seek(where_expr, &schema, binder, table_id, true)?
        {
            let (schema_opt, batch_opt, _) =
                client.seek_by_index(table_id, col_idx as u64, key_lo, key_hi)?;
            let current = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) if b.pk_lo.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
            if let Some(pred) = residual {
                if !eval_pred_row(&pred, &current, 0, actual_schema)? {
                    return Ok(SqlResult::RowsAffected { count: 0 });
                }
            }
            let mut new_batch = ZSetBatch::new(actual_schema);
            write_set_columns(&current, 0, &assignments, actual_schema, &mut new_batch)?;
            client.push_with_mode(table_id, actual_schema, &new_batch, WireConflictMode::Update)?;
            return Ok(SqlResult::RowsAffected { count: 1 });
        }

        // Path 3: Full scan with predicate
        let pred = binder.bind_expr(where_expr, &schema)?;
        let (schema_opt, batch_opt, _) = client.scan(table_id)?;
        let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
        let mut updates = ZSetBatch::new(actual_schema);
        let mut count = 0usize;
        if let Some(ref scan_batch) = batch_opt {
            for i in 0..scan_batch.pk_lo.len() {
                if !eval_pred_row(&pred, scan_batch, i, actual_schema)? { continue; }
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
    let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
    let mut updates = ZSetBatch::new(actual_schema);
    match batch_opt {
        None => Ok(SqlResult::RowsAffected { count: 0 }),
        Some(ref scan_batch) => {
            let n = scan_batch.pk_lo.len();
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

pub fn execute_delete(
    client:       &GnitzClient,
    _schema_name: &str,
    stmt:         &Statement,
    binder:       &mut Binder<'_>,
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
            "DELETE: exactly one simple FROM table required".to_string()
        ));
    }
    let table_name = extract_table_factor_name(&tables[0].relation, "DELETE")?;

    let (table_id, schema) = binder.resolve(&table_name)?;

    match &del.selection {
        None => {
            // DELETE all rows
            let (schema_opt, batch_opt, _) = client.scan(table_id)?;
            let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
            let batch = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let n = batch.pk_lo.len();
            if n == 0 { return Ok(SqlResult::RowsAffected { count: 0 }); }
            let pks: Vec<(u64, u64)> =
                (0..n).map(|i| (batch.pk_lo[i], batch.pk_hi[i])).collect();
            client.delete(table_id, actual_schema, &pks)?;
            Ok(SqlResult::RowsAffected { count: n })
        }
        Some(where_expr) => {
            // Path 1: PK equality — seek first for accurate count
            if let Some((pk_lo, pk_hi)) = try_extract_pk_seek(where_expr, &schema) {
                let (schema_opt, batch_opt, _) = client.seek(table_id, pk_lo, pk_hi)?;
                let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
                let exists = batch_opt.map_or(false, |b| !b.pk_lo.is_empty());
                if !exists { return Ok(SqlResult::RowsAffected { count: 0 }); }
                client.delete(table_id, actual_schema, &[(pk_lo, pk_hi)])?;
                return Ok(SqlResult::RowsAffected { count: 1 });
            }

            // Path 2: PK IN list
            if let Some(pks) = try_extract_pk_in(where_expr, &schema) {
                let n = pks.len();
                if n > 0 { client.delete(table_id, &schema, &pks)?; }
                return Ok(SqlResult::RowsAffected { count: n });
            }

            // Path 3: Unique index seek
            if let Some((col_idx, key_lo, key_hi, residual)) =
                try_extract_index_seek(where_expr, &schema, binder, table_id, true)?
            {
                let (schema_opt, batch_opt, _) =
                    client.seek_by_index(table_id, col_idx as u64, key_lo, key_hi)?;
                let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
                let batch = match batch_opt {
                    None => return Ok(SqlResult::RowsAffected { count: 0 }),
                    Some(b) if b.pk_lo.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                    Some(b) => b,
                };
                if let Some(pred) = residual {
                    if !eval_pred_row(&pred, &batch, 0, actual_schema)? {
                        return Ok(SqlResult::RowsAffected { count: 0 });
                    }
                }
                client.delete(table_id, actual_schema, &[(batch.pk_lo[0], batch.pk_hi[0])])?;
                return Ok(SqlResult::RowsAffected { count: 1 });
            }

            // Path 4: Full scan with predicate
            let pred = binder.bind_expr(where_expr, &schema)?;
            let (schema_opt, batch_opt, _) = client.scan(table_id)?;
            let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
            let mut pks: Vec<(u64, u64)> = Vec::new();
            if let Some(ref scan_batch) = batch_opt {
                for i in 0..scan_batch.pk_lo.len() {
                    if eval_pred_row(&pred, scan_batch, i, actual_schema)? {
                        pks.push((scan_batch.pk_lo[i], scan_batch.pk_hi[i]));
                    }
                }
            }
            let n = pks.len();
            if n > 0 { client.delete(table_id, actual_schema, &pks)?; }
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
    use gnitz_core::{ColumnDef, TypeCode, ZSetBatch, ColData, Schema};
    use crate::logical_plan::{BoundExpr, BinOp};

    fn col_def(name: &str, tc: TypeCode, nullable: bool) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: tc, is_nullable: nullable,
                    fk_table_id: 0, fk_col_idx: 0 }
    }

    /// Two-column schema: pk (U64, pk_index=0) + val (nullable).
    fn two_col(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("pk", TypeCode::U64, false), col_def("val", val_tc, true)],
            pk_index: 0,
        }
    }

    /// Single-row batch for a two-column schema (pk + one payload column).
    fn batch_2col(val_bytes: Vec<u8>, val_tc: TypeCode, null_bits: u64) -> ZSetBatch {
        let schema = two_col(val_tc);
        let mut b = ZSetBatch::new(&schema);
        b.pk_lo.push(1); b.pk_hi.push(0); b.weights.push(1); b.nulls.push(null_bits);
        if let ColData::Fixed(ref mut buf) = b.columns[1] { buf.extend(val_bytes); }
        b
    }

    // ------------------------------------------------------------------
    // Bug #4 — unsigned integers must not be sign-extended
    // ------------------------------------------------------------------

    #[test]
    fn test_u8_200_not_sign_extended() {
        let schema = two_col(TypeCode::U8);
        let batch  = batch_2col(vec![200u8], TypeCode::U8, 0);
        assert_eq!(eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(), Some(200));
    }

    #[test]
    fn test_u16_60000_not_sign_extended() {
        let schema = two_col(TypeCode::U16);
        let batch  = batch_2col(60000u16.to_le_bytes().to_vec(), TypeCode::U16, 0);
        assert_eq!(eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(), Some(60000));
    }

    #[test]
    fn test_u32_3b_not_sign_extended() {
        let schema = two_col(TypeCode::U32);
        let batch  = batch_2col(3_000_000_000u32.to_le_bytes().to_vec(), TypeCode::U32, 0);
        assert_eq!(eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(), Some(3_000_000_000));
    }

    // ------------------------------------------------------------------
    // Bug #5 — NULL must propagate through predicates
    // ------------------------------------------------------------------

    #[test]
    fn test_null_col_returns_none() {
        let schema = two_col(TypeCode::I64);
        let batch  = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // bit 0 set → val is NULL
        assert_eq!(eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(), None);
    }

    #[test]
    fn test_null_col_eq_zero_predicate_is_false() {
        // WHERE val = 0 must NOT match a NULL row (SQL: NULL = 0 → UNKNOWN → false)
        let schema = two_col(TypeCode::I64);
        let batch  = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1);
        let pred = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Eq,
            Box::new(BoundExpr::LitInt(0)),
        );
        assert!(!eval_pred_row(&pred, &batch, 0, &schema).unwrap());
    }

    // ------------------------------------------------------------------
    // Bug #6 — IS NULL / IS NOT NULL on the PK column must not panic
    // ------------------------------------------------------------------

    #[test]
    fn test_isnull_on_pk_is_false_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch  = batch_2col(vec![0u8; 8], TypeCode::I64, 0);
        // pk_index = 0; previously `*c - 1` would underflow when *c == 0
        assert_eq!(eval_expr(&BoundExpr::IsNull(0), &batch, 0, &schema).unwrap(), Some(0));
    }

    #[test]
    fn test_isnotnull_on_pk_is_true_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch  = batch_2col(vec![0u8; 8], TypeCode::I64, 0);
        assert_eq!(eval_expr(&BoundExpr::IsNotNull(0), &batch, 0, &schema).unwrap(), Some(1));
    }

    // ------------------------------------------------------------------
    // Bug #7 — i64::MIN / -1 and i64::MIN % -1 must not panic
    // ------------------------------------------------------------------

    #[test]
    fn test_div_i64_min_by_neg1_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch  = batch_2col(i64::MIN.to_le_bytes().to_vec(), TypeCode::I64, 0);
        let expr = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Div,
            Box::new(BoundExpr::LitInt(-1)),
        );
        assert!(eval_expr(&expr, &batch, 0, &schema).is_ok());
    }

    #[test]
    fn test_mod_i64_min_by_neg1_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch  = batch_2col(i64::MIN.to_le_bytes().to_vec(), TypeCode::I64, 0);
        let expr = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Mod,
            Box::new(BoundExpr::LitInt(-1)),
        );
        assert!(eval_expr(&expr, &batch, 0, &schema).is_ok());
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

        assert_eq!(dst.nulls[0] & 0b1, 0, "null bit must be cleared after non-null assignment");
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
                col_def("a",  TypeCode::I64, true),
                col_def("b",  TypeCode::I64, true),
            ],
            pk_index: 0,
        };
        let mut current = ZSetBatch::new(&schema);
        current.pk_lo.push(1); current.pk_hi.push(0);
        current.weights.push(1);
        current.nulls.push(0b10); // payload bit 1 (b) is NULL; bit 0 (a) is non-null
        if let ColData::Fixed(ref mut buf) = current.columns[1] { buf.extend_from_slice(&5i64.to_le_bytes()); }
        if let ColData::Fixed(ref mut buf) = current.columns[2] { buf.extend_from_slice(&[0u8; 8]); }

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
                col_def("a",  TypeCode::I64, true),
                col_def("b",  TypeCode::I64, true),
            ],
            pk_index: 0,
        };
        let mut current = ZSetBatch::new(&schema);
        current.pk_lo.push(1); current.pk_hi.push(0);
        current.weights.push(1);
        current.nulls.push(0b10); // b is NULL, a is not
        if let ColData::Fixed(ref mut buf) = current.columns[1] { buf.extend_from_slice(&5i64.to_le_bytes()); }
        if let ColData::Fixed(ref mut buf) = current.columns[2] { buf.extend_from_slice(&[0u8; 8]); }

        // Only assign to a; b is untouched
        let assignments = vec![(1usize, BoundExpr::LitInt(10))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_columns(&current, 0, &assignments, &schema, &mut dst).unwrap();

        assert_eq!(dst.nulls[0] & 0b01, 0, "a must not be null (assigned non-null)");
        assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null (unassigned)");
    }

    // ------------------------------------------------------------------
    // try_col_eq_literal — negative signed integer index seek keys
    // ------------------------------------------------------------------

    fn make_schema_col(tc: TypeCode) -> Schema {
        Schema {
            columns: vec![
                col_def("pk",  TypeCode::U64, false),
                col_def("val", tc, true),
            ],
            pk_index: 0,
        }
    }

    fn neg_num_expr(n: &str) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: Value::Number(n.into(), false),
                span:  sqlparser::tokenizer::Span::empty(),
            })),
        }
    }

    fn eq_expr(col: &str, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            op:   BinaryOperator::Eq,
            right: Box::new(rhs),
        }
    }

    #[test]
    fn test_try_col_eq_literal_negative_i64() {
        let schema = make_schema_col(TypeCode::I64);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        // -1i64 as u64 = u64::MAX
        assert_eq!(result, Some((1, (-1i64) as u64, 0)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i32() {
        let schema = make_schema_col(TypeCode::I32);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        // I32 -1 stored as 4 bytes zero-padded → (-1i32 as u32) as u64
        assert_eq!(result, Some((1, (-1i32 as u32) as u64, 0)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i16() {
        let schema = make_schema_col(TypeCode::I16);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, (-1i16 as u16) as u64, 0)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i8() {
        let schema = make_schema_col(TypeCode::I8);
        let expr = eq_expr("val", neg_num_expr("5"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, (-5i8 as u8) as u64, 0)));
    }

    #[test]
    fn test_try_col_eq_literal_positive_i64_still_works() {
        let schema = make_schema_col(TypeCode::I64);
        let expr = eq_expr("val", Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("42".into(), false),
            span:  sqlparser::tokenizer::Span::empty(),
        }));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, 42u64, 0)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_u64_returns_none() {
        // Cannot have a negative value for an unsigned column.
        let schema = make_schema_col(TypeCode::U64);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = super::try_col_eq_literal(&expr, &schema);
        assert_eq!(result, None);
    }
}
