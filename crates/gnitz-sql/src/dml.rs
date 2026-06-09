use sqlparser::ast::{
    Query, SetExpr, Values, Expr, Value, SelectItem,
    Statement, TableObject, BinaryOperator, UnaryOperator,
    FromTable, Assignment, AssignmentTarget,
    OnInsert, OnConflict, OnConflictAction, ConflictTarget,
};
use std::sync::Arc;
use gnitz_core::{Schema, ZSetBatch, ColData, PkColumn, PkTuple, TypeCode, WireConflictMode, ClientError};
use gnitz_core::GnitzClient;
use crate::error::{GnitzSqlError, extract_name, extract_table_factor_name};
use crate::binder::{Binder, find_unique_column};
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
            // Partial-tuple targets like `ON CONFLICT (a) DO NOTHING`
            // against `PRIMARY KEY (a, b)` are out of scope for the
            // compound-PK planner gate. Reject before any
            // `pk_index_single()` access so the assert turns into a
            // clean SQL error.
            if schema.pk_cols.len() >= 2 {
                return Err(GnitzSqlError::Unsupported(
                    "ON CONFLICT with target columns is not supported on \
                     compound-PK tables".to_string()
                ));
            }
            if cols.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "composite ON CONFLICT targets not supported; \
                     single-column PK target only".to_string()
                ));
            }
            let col_name = cols[0].value.as_str();
            let pk_name = schema.columns[schema.pk_index_single()].name.as_str();
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
    client:       &mut GnitzClient,
    _schema_name: &str,
    stmt:         &Statement,
    binder:       &mut Binder<'_>,
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
        let pk = extract_pk_value(row, &schema)?;
        batch.pks.push_tuple(&pk);
        batch.weights.push(1);

        let mut null_bits: u64 = 0;
        for (payload_idx, ci, col_def) in schema.payload_columns() {
            if ci >= row.len() {
                return Err(GnitzSqlError::Bind(
                    format!("not enough values in INSERT row (column {})", ci)
                ));
            }
            let val_expr = &row[ci];
            // Check if this value is NULL and set the null bitmap
            if is_null_expr(val_expr) {
                null_bits |= 1u64 << payload_idx;
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
            if !merged.pks.is_empty() {
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
        if schema.is_pk_col(col_idx) {
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
        if !seen_pks.insert(pk) { continue; }
        // Existing-store check.
        let (_sch, found, _lsn) = client.seek(tid, &pk)?;
        let exists = matches!(found, Some(b) if !b.pks.is_empty());
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
                 (duplicate PK in the same batch)".to_string()
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

fn parse_uuid_str(s: &str) -> Result<u128, GnitzSqlError> {
    let s = s.trim();
    let hex: String = if s.len() == 36
        && s.as_bytes()[8]  == b'-'
        && s.as_bytes()[13] == b'-'
        && s.as_bytes()[18] == b'-'
        && s.as_bytes()[23] == b'-'
    {
        format!("{}{}{}{}{}", &s[..8], &s[9..13], &s[14..18], &s[19..23], &s[24..])
    } else if s.len() == 32 {
        s.to_string()
    } else {
        return Err(GnitzSqlError::Bind(format!("invalid UUID literal: {:?}", s)));
    };
    u128::from_str_radix(&hex, 16)
        .map_err(|_| GnitzSqlError::Bind(format!("invalid UUID literal: {:?}", s)))
}

/// Parse a numeric SQL literal into its packed-u128 PK form. The returned
/// u128 holds the on-disk LE bytes in its low `pk_stride` bytes: signed
/// types pass through `(v as iN as uN) as u128` so the low `stride` bytes
/// match the column's native LE encoding (e.g. `-1_i8` → `0xFF`, not
/// `0xFFFF_FFFF_FFFF_FFFF`).
///
/// `negated` is true when the literal sits under `Expr::UnaryOp(Minus, _)`;
/// the prepend-`-`-then-parse rule accepts `i64::MIN`'s digit string
/// (which would overflow a plain `i64::parse` followed by `checked_neg`).
///
/// This helper is the single source of truth for INSERT/SEEK PK routing —
/// `extract_pk_value`, `try_col_eq_literal`, and `try_extract_pk_in` all
/// dispatch through it so the master cannot send INSERT and DELETE for the
/// same key to different workers.
fn parse_pk_literal_packed(tc: TypeCode, n_str: &str, negated: bool) -> Option<u128> {
    let s_owned;
    let s: &str = if negated { s_owned = format!("-{}", n_str); &s_owned } else { n_str };
    match tc {
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
            let v = s.parse::<i64>().ok()?;
            Some(match tc {
                TypeCode::I8  => (v as i8  as u8)  as u128,
                TypeCode::I16 => (v as i16 as u16) as u128,
                TypeCode::I32 => (v as i32 as u32) as u128,
                _             => (v as u64) as u128,
            })
        }
        TypeCode::U128 | TypeCode::UUID => {
            if negated { return None; }
            s.parse::<u128>().ok()
        }
        _ => {
            if negated { return None; }
            s.parse::<u64>().ok().map(|v| v as u128)
        }
    }
}

/// A SQL literal extracted from an `Expr` for PK/seek routing. `Number`'s
/// second field is `negated` (the literal sat under `UnaryOp(Minus, _)`);
/// `Str` carries the unescaped single-quoted contents.
enum SqlLiteral<'a> { Number(&'a str, bool), Str(&'a str), Null }

/// Centralizes the `Expr::Value` / `UnaryOp(Minus, Number)` unwrap shared by
/// the SEEK/INSERT parse sites (`parse_one_pk_literal`, `try_col_eq_literal`,
/// `try_extract_pk_in`).
///
/// Matches `SingleQuotedString` only — NOT `DoubleQuotedString`: in
/// `GenericDialect` a double-quoted token is an identifier, so treating
/// `col = "x"` as a UUID seek literal would silently change which queries
/// take the index fast path.
fn extract_sql_literal(expr: &Expr) -> Option<SqlLiteral<'_>> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Null                  => Some(SqlLiteral::Null),
            Value::Number(n, _)          => Some(SqlLiteral::Number(n, false)),
            Value::SingleQuotedString(s) => Some(SqlLiteral::Str(s)),
            _ => None,
        },
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => match expr.as_ref() {
            Expr::Value(vws) => match &vws.value {
                Value::Number(n, _) => Some(SqlLiteral::Number(n, true)),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Parse one PK column literal at `pk_expr` into its packed u128 form.
/// Routes through `parse_pk_literal_packed` (numerics) or `parse_uuid_str`
/// (UUID); the returned u128's low `wire_stride` bytes carry the column's
/// native LE bytes.
fn parse_one_pk_literal(
    pk_expr: &Expr,
    tc: TypeCode,
    col_name: &str,
) -> Result<u128, GnitzSqlError> {
    match extract_sql_literal(pk_expr) {
        Some(SqlLiteral::Number(n, negated)) => {
            parse_pk_literal_packed(tc, n, negated).ok_or_else(|| {
                if negated && matches!(tc,
                    TypeCode::U8 | TypeCode::U16 | TypeCode::U32
                    | TypeCode::U64 | TypeCode::U128 | TypeCode::UUID
                ) {
                    GnitzSqlError::Bind(format!(
                        "PK column '{}' of type {:?} does not accept negative literals",
                        col_name, tc))
                } else {
                    let s_disp = if negated { format!("-{}", n) } else { n.to_string() };
                    GnitzSqlError::Bind(format!(
                        "PK column '{}' value is not a valid {:?}: {}",
                        col_name, tc, s_disp))
                }
            })
        }
        // UUID accepts a single-quoted UUID string; non-UUID PKs are numeric only.
        Some(SqlLiteral::Str(s)) if tc == TypeCode::UUID => parse_uuid_str(s),
        _ => Err(GnitzSqlError::Bind(format!(
            "PK column '{}' value must be a numeric literal", col_name
        ))),
    }
}

/// Extract the primary key from a VALUES row as a `PkTuple`. Walks the PK
/// columns in pk-list order, dispatches each through `parse_one_pk_literal`,
/// and copies the column's native LE bytes into the tuple buffer.
fn extract_pk_value(row: &[Expr], schema: &Schema) -> Result<PkTuple, GnitzSqlError> {
    let stride = schema.pk_stride() as u8;
    let mut tuple = PkTuple::new(stride);
    let mut off = 0usize;
    for &pi in schema.pk_indices() {
        let pk_expr = row.get(pi).ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "PK column '{}' missing from INSERT row", schema.columns[pi].name
            ))
        })?;
        let tc = schema.columns[pi].type_code;
        let v = parse_one_pk_literal(pk_expr, tc, &schema.columns[pi].name)?;
        let w = tc.wire_stride();
        tuple.buf[off..off + w].copy_from_slice(&v.to_le_bytes()[..w]);
        off += w;
    }
    debug_assert_eq!(off, stride as usize);
    Ok(tuple)
}

fn is_null_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(vws) if matches!(vws.value, Value::Null))
}

/// Append a NULL for column `col` of wire type `tc`. The NULL encoding is
/// uniform across all four `ColData` variants, so INSERT and UPDATE share it.
fn append_null(col: &mut ColData, tc: TypeCode) {
    match col {
        ColData::Fixed(buf) => buf.extend(std::iter::repeat_n(0u8, tc.wire_stride())),
        ColData::Strings(v) => v.push(None),
        ColData::Bytes(v)   => v.push(None),
        ColData::U128s(v)   => v.push(0u128),
    }
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
                append_null(col, tc);
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
                    ColData::Bytes(_) => Err(GnitzSqlError::Bind(
                        "number literal for blob column".to_string()
                    )),
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                match col {
                    ColData::Strings(v) => { v.push(Some(s.clone())); Ok(()) }
                    ColData::U128s(v) if tc == TypeCode::UUID => {
                        v.push(parse_uuid_str(s)?);
                        Ok(())
                    }
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
    client:       &mut GnitzClient,
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

    let (tid, schema) = binder.resolve(client, &table_name)?;

    // Check WHERE clause
    let (schema_out, batch_opt, _) = if let Some(where_expr) = &select.selection {
        if let Some((pk, residual)) = try_extract_pk_seek_residual(where_expr, &schema) {
            let (s, b, lsn) = client.seek(tid, &pk)?;
            let preds = bind_residuals(binder, &residual, &schema)?;
            let (s2, b2) = apply_residual_filter((s, b), &preds, &schema)?;
            (s2, b2, lsn)
        } else {
            let mut hit = None;
            for (col_idx, key, residual) in collect_index_seek_candidates(where_expr, &schema) {
                match client.seek_by_index(tid, col_idx as u64, key) {
                    Ok((s, b, lsn)) => {
                        let preds = bind_residuals(binder, &residual, &schema)?;
                        let (s2, b2) = apply_residual_filter((s, b), &preds, &schema)?;
                        hit = Some((s2, b2, lsn));
                        break;
                    }
                    Err(ClientError::NoIndex) => continue,
                    Err(e) => return Err(GnitzSqlError::Exec(e)),
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
                         use CREATE INDEX first, or CREATE VIEW for server-side filtering".to_string()
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
    let (proj_schema, proj_batch) = apply_projection(
        &select.projection, actual_schema, batch_opt, binder
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

/// Flattens an `AND`-tree into its leaf conjuncts, left to right. Descends
/// through `AND` nesting and unwraps parenthesised `Nested` wrappers; any other
/// node (an equality, a range, an `OR`-group, …) is a leaf kept intact.
fn flatten_conjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
    match expr {
        Expr::Nested(inner) => flatten_conjuncts(inner, out),
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
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
fn try_extract_pk_seek_residual<'e>(
    expr:   &'e Expr,
    schema: &Schema,
) -> Option<(PkTuple, Vec<&'e Expr>)> {
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
fn try_col_eq_literal(expr: &Expr, schema: &Schema) -> Option<(usize, u128)> {
    let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr else { return None; };
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
            SqlLiteral::Number(n, false) =>
                parse_pk_literal_packed(TypeCode::UUID, n, false).map(|v| (col_idx, v)),
            _ => None,
        };
    }

    // Non-UUID: numeric literal only. Secondary indexes store column values as
    // zero-extended LE bytes cast to u64; for signed types we produce the same
    // bit pattern by casting through the type's unsigned equivalent. Delegated
    // to parse_pk_literal_packed so the three SEEK/INSERT parse sites cannot drift.
    let SqlLiteral::Number(n_str, negated) = lit else { return None; };
    let key = parse_pk_literal_packed(col_tc, n_str, negated)?;
    Some((col_idx, key))
}

/// Every non-PK `col = literal` seek candidate among the conjuncts of `expr`,
/// each paired with the residual conjuncts (all the *other* conjuncts) to
/// evaluate after the seek.
///
/// Pure syntax: no catalog probe, no binding, no I/O. The caller attempts each
/// candidate's `seek_by_index` in turn and treats `ClientError::NoIndex` as
/// "this column has no index — try the next candidate". Returning *all*
/// candidates lets `WHERE unindexed = 1 AND indexed = 2` find the index whichever
/// conjunct carries it; flattening the whole `AND`-tree extends that to
/// three-or-more-way conjunctions. PK columns are skipped —
/// `try_extract_pk_seek_residual` handles them first — and so are column types
/// that can never carry a secondary index.
fn collect_index_seek_candidates<'e>(
    expr:   &'e Expr,
    schema: &Schema,
) -> Vec<(usize, u128, Vec<&'e Expr>)> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    let mut out = Vec::new();
    for (i, &cand) in conjuncts.iter().enumerate() {
        if let Some((col_idx, key)) = try_col_eq_literal(cand, schema) {
            // A secondary index is only creatable on an `is_pk_eligible` integer
            // scalar (validate_index_col_type), so a float/string/blob
            // `col = <integer-literal>` — which try_col_eq_literal still matches,
            // since parse_pk_literal_packed accepts an integer literal for any
            // type — could only ever answer NoIndex. Skipping it (and the PK
            // columns, handled first) avoids a guaranteed-futile round trip.
            let tc = schema.columns[col_idx].type_code;
            if !schema.is_pk_col(col_idx) && tc.is_pk_eligible() {
                let residual = conjuncts.iter()
                    .enumerate()
                    .filter(|&(j, _)| j != i)
                    .map(|(_, &e)| e)
                    .collect();
                out.push((col_idx, key, residual));
            }
        }
    }
    out
}

/// Bind the residual conjuncts collected as raw `&Expr` by the seek extractors
/// into `BoundExpr`s, ready for per-row evaluation after a seek succeeds. Kept
/// separate from extraction so candidates that never seek (wrong index, NoIndex)
/// are never bound.
fn bind_residuals(
    binder:   &Binder<'_>,
    residual: &[&Expr],
    schema:   &Schema,
) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    residual.iter().map(|&e| binder.bind_expr(e, schema)).collect()
}

/// True when row `i` of `batch` satisfies every residual predicate. An empty
/// slice trivially passes; short-circuits on the first failing conjunct. Binding
/// each conjunct independently and ANDing the results is equivalent to evaluating
/// one `AND`-chain — `BinOp::And` returns `None` (→ `eval_pred_row` false) the
/// moment either operand is NULL, so a NULL conjunct excludes the row either way
/// — but needs no temporary `AND`-tree to be cloned and bound.
fn row_passes_residuals(
    preds:  &[BoundExpr],
    batch:  &ZSetBatch,
    i:      usize,
    schema: &Schema,
) -> Result<bool, GnitzSqlError> {
    for p in preds {
        if !eval_pred_row(p, batch, i, schema)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn apply_residual_filter(
    result: (Option<Arc<Schema>>, Option<ZSetBatch>),
    preds:  &[BoundExpr],
    schema: &Schema,
) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), GnitzSqlError> {
    if preds.is_empty() {
        return Ok(result);
    }
    let (schema_opt, batch_opt) = result;
    let batch = match batch_opt {
        None => return Ok((schema_opt, None)),
        Some(b) => b,
    };
    let actual_schema = schema_opt.as_deref().unwrap_or(schema);
    let mut new_batch = ZSetBatch::new(actual_schema);
    for i in 0..batch.pks.len() {
        if row_passes_residuals(preds, &batch, i, actual_schema)? {
            copy_batch_row(&batch, i, &mut new_batch, actual_schema);
        }
    }
    Ok((schema_opt, Some(new_batch)))
}

enum ColumnValue { Int(i64), Str(String), Null }

/// Decode the native LE bytes of a PK column into an i64. U128/UUID don't fit
/// through the i64 return shape and are rejected.
fn decode_pk_bytes_to_i64(tc: TypeCode, slice: &[u8]) -> Result<i64, GnitzSqlError> {
    Ok(match tc {
        TypeCode::I8  => slice[0] as i8 as i64,
        TypeCode::U8  => slice[0] as i64,
        TypeCode::I16 => i16::from_le_bytes(slice[..2].try_into().unwrap()) as i64,
        TypeCode::U16 => u16::from_le_bytes(slice[..2].try_into().unwrap()) as i64,
        TypeCode::I32 => i32::from_le_bytes(slice[..4].try_into().unwrap()) as i64,
        TypeCode::U32 => u32::from_le_bytes(slice[..4].try_into().unwrap()) as i64,
        TypeCode::I64 => i64::from_le_bytes(slice[..8].try_into().unwrap()),
        TypeCode::U64 => u64::from_le_bytes(slice[..8].try_into().unwrap()) as i64,
        _ => return Err(GnitzSqlError::Unsupported(format!(
            "residual filter on PK column of type {:?} not supported; \
             use `pk = literal` to seek instead", tc
        ))),
    })
}

fn eval_pred_row(
    pred:   &BoundExpr,
    batch:  &ZSetBatch,
    i:      usize,
    schema: &Schema,
) -> Result<bool, GnitzSqlError> {
    // SQL NULL in a predicate position is UNKNOWN → treated as false (row excluded).
    Ok(eval_expr(pred, batch, i, schema)?.is_some_and(|v| v != 0))
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
            let col_def = &schema.columns[*c];
            // The PK column's slot in `batch.columns` is `Fixed(vec![])`
            // (placeholder set by ZSetBatch::new). The PK value lives in
            // `batch.pks`; decode the bytes for column `*c` under the
            // column's declared signedness. U128/UUID don't fit through the
            // i64 return shape — reject explicitly.
            if schema.is_pk_col(*c) {
                let stride = col_def.type_code.wire_stride();
                match &batch.pks {
                    PkColumn::Bytes { stride: s, buf } => {
                        let s   = *s as usize;
                        let off = schema.pk_byte_offset(*c);
                        let slice = &buf[i * s + off .. i * s + off + stride];
                        return decode_pk_bytes_to_i64(col_def.type_code, slice).map(Some);
                    }
                    _ => {
                        let bytes = batch.pks.get(i).to_le_bytes();
                        return decode_pk_bytes_to_i64(col_def.type_code, &bytes[..stride])
                            .map(Some);
                    }
                }
            }
            // Payload column: check the null bitmap first.
            let payload_idx = schema.payload_idx(*c);
            if (batch.nulls[i] & (1u64 << payload_idx)) != 0 {
                return Ok(None);
            }
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
                ColData::Bytes(_) => Err(GnitzSqlError::Unsupported(
                    "residual filter on blob column not supported".to_string()
                )),
                ColData::U128s(_) => {
                    let type_name = if schema.columns[*c].type_code == TypeCode::UUID { "UUID" } else { "U128" };
                    Err(GnitzSqlError::Unsupported(format!(
                        "residual filter on {} column not supported; \
                         use a primary-key seek or CREATE INDEX for equality lookups",
                        type_name
                    )))
                }
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
            if schema.is_pk_col(*c) {
                return Ok(Some(0)); // PK is never null → IS NULL is always false
            }
            let payload_idx = schema.payload_idx(*c);
            let is_null = (batch.nulls[i] & (1u64 << payload_idx)) != 0;
            Ok(Some(is_null as i64))
        }
        BoundExpr::IsNotNull(c) => {
            if schema.is_pk_col(*c) {
                return Ok(Some(1)); // PK is never null → IS NOT NULL is always true
            }
            let payload_idx = schema.payload_idx(*c);
            let is_null = (batch.nulls[i] & (1u64 << payload_idx)) != 0;
            Ok(Some(!is_null as i64))
        }
        BoundExpr::AggCall { .. } => Err(GnitzSqlError::Unsupported(
            "aggregate functions not allowed in this context".to_string()
        )),
    }
}

fn copy_batch_row(src: &ZSetBatch, i: usize, dst: &mut ZSetBatch, schema: &Schema) {
    dst.pks.push_from(&src.pks, i);
    dst.weights.push(src.weights[i]);
    dst.nulls.push(src.nulls[i]);
    for (_pi, ci, col_def) in schema.payload_columns() {
        let stride = col_def.type_code.wire_stride();
        src.columns[ci].push_row_from(i, stride, &mut dst.columns[ci]);
    }
}

fn apply_projection(
    projection: &[SelectItem],
    schema:     &Schema,
    batch:      Option<ZSetBatch>,
    _binder:    &Binder<'_>,
) -> Result<(Schema, ZSetBatch), GnitzSqlError> {
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
                let idx = find_unique_column(&schema.columns, &ident.value)?
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

    // Identity fast-path: every source column projected in source order.
    // Checked before allocating new_cols/new_schema — on the common case
    // (named projection that names every column) those allocations would
    // be dead. The identity case projects every column, so PK columns are
    // included and the no-PK-projected guard below is satisfied implicitly.
    let is_identity = col_indices.len() == schema.columns.len()
        && col_indices.iter().enumerate().all(|(i, &ci)| ci == i);
    if is_identity {
        let b = batch.unwrap_or_else(|| ZSetBatch::new(schema));
        return Ok((schema.clone(), b));
    }

    // 1. Build new PK column set; every projected source-PK becomes a new PK.
    let new_pk_cols: Vec<usize> = col_indices.iter().enumerate()
        .filter(|(_, &old_ci)| schema.is_pk_col(old_ci))
        .map(|(new_ci, _)| new_ci)
        .collect();

    if new_pk_cols.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "SELECT must project at least one PRIMARY KEY column".to_string()
        ));
    }

    let new_cols: Vec<gnitz_core::ColumnDef> = col_indices.iter()
        .map(|&i| schema.columns[i].clone())
        .collect();
    let new_schema = Schema { columns: new_cols, pk_cols: new_pk_cols };

    let src_batch = batch.unwrap_or_else(|| ZSetBatch::new(schema));
    let row_count = src_batch.len();
    let ZSetBatch { pks: src_pks, weights, nulls: src_nulls, columns: mut src_columns } = src_batch;
    let mut new_batch = ZSetBatch::new(&new_schema);
    new_batch.weights = weights;

    // 3. PK region: move when layout is byte-identical; else rebuild from
    //    the packed source bytes. `pk_preserved` covers compound→compound
    //    when columns and order match; the single-PK source can only land
    //    here (see invariants above).
    let pk_preserved = new_schema.pk_cols.len() == schema.pk_cols.len()
        && new_schema.pk_cols.iter().enumerate()
            .all(|(i, &new_pk_ci)| col_indices[new_pk_ci] == schema.pk_cols[i]);

    if pk_preserved {
        new_batch.pks = src_pks;
    } else {
        // Single-PK source can't reach here: if the lone PK is projected
        // then pk_preserved is true; if not, new_pk_cols is empty and we
        // returned the no-PK-projected error above. So the source PK
        // column is always compound (Bytes).
        let src_bytes = match &src_pks {
            PkColumn::Bytes { buf, .. } => buf.as_slice(),
            _ => unreachable!("non-preserved PK rebuild requires compound source"),
        };
        let src_stride = schema.pk_stride();

        // Per-PK (col_off, stride) is invariant across rows — hoist.
        let pk_mappings: Vec<(usize, usize)> = new_schema.pk_cols.iter()
            .map(|&new_pk_ci| {
                let old_ci = col_indices[new_pk_ci];
                (schema.pk_byte_offset(old_ci), schema.columns[old_ci].type_code.wire_stride())
            })
            .collect();

        match &mut new_batch.pks {
            PkColumn::Bytes { buf, .. } => {
                buf.reserve(row_count * new_schema.pk_stride());
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    for &(col_off, stride) in &pk_mappings {
                        buf.extend_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    }
                }
            }
            PkColumn::U64s(v) => {
                // Destination is single-PK → exactly one mapping.
                let (col_off, stride) = pk_mappings[0];
                v.reserve(row_count);
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    let mut b = [0u8; 8];
                    b[..stride].copy_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    v.push(u64::from_le_bytes(b));
                }
            }
            PkColumn::U128s(v) => {
                let (col_off, stride) = pk_mappings[0];
                v.reserve(row_count);
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    let mut b = [0u8; 16];
                    b[..stride].copy_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    v.push(u128::from_le_bytes(b));
                }
            }
        }
    }

    // 4. Null bitmap: move when payload layout matches the source's;
    //    else rebuild bit-by-bit using a hoisted (new_pi, old_pi) mapping.
    let payload_preserved = new_schema.num_payload_cols() == schema.num_payload_cols()
        && new_schema.payload_columns().zip(schema.payload_columns())
            .all(|((_, new_ci, _), (_, old_ci, _))| col_indices[new_ci] == old_ci);

    if payload_preserved {
        new_batch.nulls = src_nulls;
    } else {
        // Projected PKs become new PKs (not payload), so old_ci is a
        // source payload column by construction — payload_idx is safe.
        let pi_mappings: Vec<(usize, usize)> = new_schema.payload_columns()
            .map(|(new_pi, new_ci, _)| (new_pi, schema.payload_idx(col_indices[new_ci])))
            .collect();

        new_batch.nulls.reserve(row_count);
        for &old_word in &src_nulls {
            let mut new_word = 0u64;
            for &(new_pi, old_pi) in &pi_mappings {
                if (old_word >> old_pi) & 1 == 1 {
                    new_word |= 1u64 << new_pi;
                }
            }
            new_batch.nulls.push(new_word);
        }
    }

    // 5. Payload columns: strictly payload→payload. `src_columns` is owned,
    //    so swap whole vectors instead of cloning per element — avoids
    //    per-row Option<String>/Option<Vec<u8>> allocations on string/blob
    //    result sets.
    for (_, new_ci, _) in new_schema.payload_columns() {
        let old_ci = col_indices[new_ci];
        let src_col = std::mem::replace(&mut src_columns[old_ci], ColData::Fixed(Vec::new()));
        match (src_col, &mut new_batch.columns[new_ci]) {
            (ColData::Fixed(s),   ColData::Fixed(d))   => *d = s,
            (ColData::Strings(s), ColData::Strings(d)) => *d = s,
            (ColData::Bytes(s),   ColData::Bytes(d))   => *d = s,
            (ColData::U128s(s),   ColData::U128s(d))   => *d = s,
            _ => unreachable!("mismatched ColData variants for column {new_ci}"),
        }
    }

    Ok((new_schema, new_batch))
}

fn apply_limit(mut batch: ZSetBatch, schema: &Schema, limit: usize) -> ZSetBatch {
    let n = batch.pks.len();
    if n <= limit { return batch; }

    batch.pks.truncate(limit);
    batch.weights.truncate(limit);
    batch.nulls.truncate(limit);

    for (_pi, ci, col_def) in schema.payload_columns() {
        match &mut batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = col_def.type_code.wire_stride();
                buf.truncate(limit * stride);
            }
            ColData::Strings(v) => { v.truncate(limit); }
            ColData::Bytes(v) => { v.truncate(limit); }
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
        ColumnValue::Null => append_null(col, tc),
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
    dst.pks.push_from(&current.pks, row_idx);
    dst.weights.push(1);
    // Start with the existing row's null bits; each assignment updates only
    // its own column's bit (set for a NULL result, clear for non-NULL).
    let mut null_bits = current.nulls[row_idx];

    for (payload_idx, ci, col_def) in schema.payload_columns() {
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
            current.columns[ci].push_row_from(row_idx, stride, &mut dst.columns[ci]);
        }
    }
    dst.nulls.push(null_bits);
    Ok(())
}

fn try_extract_pk_in(expr: &Expr, schema: &Schema) -> Option<Vec<u128>> {
    // Compound PK has no IN-list fast path; fall back to a full delta scan.
    if schema.pk_count() != 1 { return None; }
    if let Expr::InList { expr: col_expr, list, negated, .. } = expr {
        if *negated { return None; }
        let col_name = match col_expr.as_ref() {
            Expr::Identifier(id) => &id.value,
            _ => return None,
        };
        let pk_col = &schema.columns[schema.pk_indices()[0]];
        if !pk_col.name.eq_ignore_ascii_case(col_name) { return None; }
        let mut pks = Vec::with_capacity(list.len());
        for item in list {
            // Only optionally-negated numeric literals take the fast path; a
            // string or NULL in the IN-list aborts to the slow scan. Routing
            // through extract_sql_literal also unwraps `UnaryOp(Minus, Number)`
            // so signed lists like `IN (-1, -2)` match without the slow path.
            let SqlLiteral::Number(n, negated) = extract_sql_literal(item)? else { return None; };
            pks.push(parse_pk_literal_packed(pk_col.type_code, n, negated)?);
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
    client:       &mut GnitzClient,
    _schema_name: &str,
    stmt:         &Statement,
    binder:       &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let (table, assignments_raw, selection) = match stmt {
        Statement::Update { table, assignments, selection, .. } => (table, assignments, selection),
        _ => return Err(GnitzSqlError::Bind("not an UPDATE statement".to_string())),
    };

    let table_name = extract_table_factor_name(&table.relation, "UPDATE")?;

    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    // Bind SET assignments; reject any assignment to the PK column
    let mut assignments: Vec<(usize, BoundExpr)> = Vec::new();
    for assignment in assignments_raw {
        let col_name = extract_assignment_col_name(assignment)?;
        let col_idx = schema.columns.iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col_name))
            .ok_or_else(|| GnitzSqlError::Bind(
                format!("column '{}' not found in UPDATE SET", col_name)
            ))?;
        if schema.is_pk_col(col_idx) {
            return Err(GnitzSqlError::Unsupported(
                "cannot UPDATE primary key column".to_string()
            ));
        }
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
        for (col_idx, key, residual) in collect_index_seek_candidates(where_expr, &schema) {
            let (schema_opt, batch_opt, _) = match client.seek_by_index(table_id, col_idx as u64, key) {
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
                if !row_passes_residuals(&preds, &current, i, actual_schema)? { continue; }
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

pub fn execute_delete(
    client:       &mut GnitzClient,
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
            if n == 0 { return Ok(SqlResult::RowsAffected { count: 0 }); }
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

            // Path 2: PK IN list (single-PK only; try_extract_pk_in returns
            // None for compound PK, so we never reach the push_u128 path).
            if let Some(pks) = try_extract_pk_in(where_expr, &schema) {
                let n = pks.len();
                if n > 0 {
                    let mut pk_col = PkColumn::empty_for_schema(&schema);
                    for v in pks { pk_col.push_u128(v); }
                    client.delete(table_id, &schema, pk_col)?;
                }
                return Ok(SqlResult::RowsAffected { count: n });
            }

            // Path 3: secondary-index seek (any index, unique or not). Applies
            // the residual per row and deletes *all* matches. Falls through to
            // Path 4 when no candidate column carries an index.
            for (col_idx, key, residual) in collect_index_seek_candidates(where_expr, &schema) {
                let (schema_opt, batch_opt, _) = match client.seek_by_index(table_id, col_idx as u64, key) {
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
                    if !row_passes_residuals(&preds, &batch, i, actual_schema)? { continue; }
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
            if n > 0 { client.delete(table_id, actual_schema, out_pks)?; }
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
            pk_cols: vec![0],
        }
    }

    /// Single-row batch for a two-column schema (pk + one payload column).
    fn batch_2col(val_bytes: Vec<u8>, val_tc: TypeCode, null_bits: u64) -> ZSetBatch {
        let schema = two_col(val_tc);
        let mut b = ZSetBatch::new(&schema);
        b.pks.push_u128(1u128); b.weights.push(1); b.nulls.push(null_bits);
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
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
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
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
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
    // UUID parse_uuid_str
    // ------------------------------------------------------------------

    #[test]
    fn test_parse_uuid_str_standard_format() {
        let v = super::parse_uuid_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(v, 0x550e8400_e29b_41d4_a716_446655440000_u128);
    }

    #[test]
    fn test_parse_uuid_str_no_hyphens() {
        let v = super::parse_uuid_str("550e8400e29b41d4a716446655440000").unwrap();
        assert_eq!(v, 0x550e8400_e29b_41d4_a716_446655440000_u128);
    }

    #[test]
    fn test_parse_uuid_str_invalid_rejected() {
        assert!(super::parse_uuid_str("not-a-uuid").is_err());
        assert!(super::parse_uuid_str("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz").is_err());
        assert!(super::parse_uuid_str("").is_err());
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
            span:  sqlparser::tokenizer::Span::empty(),
        })
    }

    fn num_expr(n: &str) -> Expr {
        Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number(n.into(), false),
            span:  sqlparser::tokenizer::Span::empty(),
        })
    }

    #[test]
    fn test_uuid_pk_string_literal_accepted() {
        let schema = uuid_schema_pk();
        let row = vec![uuid_str_expr("550e8400-e29b-41d4-a716-446655440000")];
        let pk = super::extract_pk_value(&row, &schema).unwrap();
        // UUID PK has stride 16; the parsed u128 lives in the low 16 bytes.
        assert_eq!(pk.to_u128_narrow(), 0x550e8400_e29b_41d4_a716_446655440000_u128);
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
        ).unwrap();
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], 0x550e8400_e29b_41d4_a716_446655440000_u128);
        } else { panic!("expected U128s"); }
    }

    #[test]
    fn test_uuid_decimal_literal_still_accepted() {
        let schema = uuid_schema_payload();
        let mut batch = ZSetBatch::new(&schema);
        let big_val: u128 = 0x550e8400_e29b_41d4_a716_446655440000_u128;
        append_value_to_col(
            &mut batch.columns[1],
            TypeCode::UUID,
            &num_expr(&big_val.to_string()),
        ).unwrap();
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], big_val);
        } else { panic!("expected U128s"); }
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
        batch.pks.push_u128(1); batch.weights.push(1); batch.nulls.push(0);
        if let ColData::U128s(v) = &mut batch.columns[1] { v.push(0); }
        let pred = BoundExpr::ColRef(1);
        let err = super::eval_expr(&pred, &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().contains("UUID"), "error should mention UUID: {}", err);
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
            pk_cols: vec![0],
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
        let expr = eq_expr("val", Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Number("42".into(), false),
            span:  sqlparser::tokenizer::Span::empty(),
        }));
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

    // ------------------------------------------------------------------
    // Native-PK literal parsing: extract_pk_value / try_col_eq_literal
    // / try_extract_pk_in must produce the SAME u128 for the same
    // (type, literal) pair. Master partition routing depends on this.
    // ------------------------------------------------------------------

    fn pk_schema(pk_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![
                col_def("id", pk_tc, false),
                col_def("v",  TypeCode::I64, false),
            ],
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

    /// All three parser entry points must agree byte-for-byte on the u128
    /// produced by a given (PK type, literal) pair. Master routes SEEK via
    /// `partition_for_key(pk)`; drift between any pair sends INSERT and
    /// DELETE to different workers.
    fn check_pk_parity(pk_tc: TypeCode, literal: Expr, expected: u128) {
        let schema = pk_schema(pk_tc);

        // 1. extract_pk_value (INSERT row).
        let row = vec![literal.clone(), num_expr("0")];
        let got_insert = super::extract_pk_value(&row, &schema)
            .unwrap_or_else(|e| panic!("extract_pk_value({pk_tc:?}): {e}"));
        assert_eq!(got_insert.to_u128_narrow(), expected, "extract_pk_value");

        // 2. try_col_eq_literal (WHERE pk = literal).
        let where_expr = eq_expr("id", literal.clone());
        let got_eq = super::try_col_eq_literal(&where_expr, &schema)
            .expect("try_col_eq_literal returned None");
        assert_eq!(got_eq, (0, expected), "try_col_eq_literal");

        // 3. try_extract_pk_in (WHERE pk IN (literal)).
        let in_expr = in_list_expr("id", vec![literal]);
        let got_in = super::try_extract_pk_in(&in_expr, &schema)
            .expect("try_extract_pk_in returned None");
        assert_eq!(got_in, vec![expected], "try_extract_pk_in");
    }

    #[test]
    fn pk_parity_i8_neg1() {
        check_pk_parity(TypeCode::I8, neg_num_expr("1"), (-1i8 as u8) as u128);
    }

    #[test]
    fn pk_parity_i16_neg1() {
        check_pk_parity(TypeCode::I16, neg_num_expr("1"), (-1i16 as u16) as u128);
    }

    #[test]
    fn pk_parity_i32_neg1() {
        check_pk_parity(TypeCode::I32, neg_num_expr("1"), (-1i32 as u32) as u128);
    }

    #[test]
    fn pk_parity_i64_neg1() {
        check_pk_parity(TypeCode::I64, neg_num_expr("1"), ((-1i64) as u64) as u128);
    }

    #[test]
    fn pk_parity_i64_min() {
        // Regression for the prepend-`-` parse rule: `(i64::MIN as u64) as u128`.
        check_pk_parity(
            TypeCode::I64,
            neg_num_expr("9223372036854775808"),
            (i64::MIN as u64) as u128,
        );
    }

    #[test]
    fn pk_parity_u16_max() {
        check_pk_parity(TypeCode::U16, num_expr("65535"), 65535u128);
    }

    #[test]
    fn pk_parity_u32_max() {
        check_pk_parity(TypeCode::U32, num_expr("4294967295"), 4294967295u128);
    }

    #[test]
    fn extract_pk_value_u64_rejects_negative() {
        let schema = pk_schema(TypeCode::U64);
        let row = vec![neg_num_expr("1"), num_expr("0")];
        let err = super::extract_pk_value(&row, &schema)
            .expect_err("U64 PK must reject negative literal");
        assert!(err.to_string().contains("negative"), "error: {err}");
    }

    #[test]
    fn extract_pk_value_u128_rejects_negative() {
        let schema = pk_schema(TypeCode::U128);
        let row = vec![neg_num_expr("1"), num_expr("0")];
        assert!(super::extract_pk_value(&row, &schema).is_err());
    }

    #[test]
    fn try_extract_pk_in_negative_i32_list() {
        // Today's u64-only body returned None for negative items, falling
        // through to the slow full-scan path. Native-PK fast path must hit.
        let schema = pk_schema(TypeCode::I32);
        let expr = in_list_expr("id", vec![neg_num_expr("1"), neg_num_expr("2")]);
        let got = super::try_extract_pk_in(&expr, &schema).expect("should match fast path");
        assert_eq!(got, vec![(-1i32 as u32) as u128, (-2i32 as u32) as u128]);
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
        let err = super::eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema)
            .expect_err("U128 PK must return Unsupported");
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
                col_def("a", TypeCode::U64,  false),
                col_def("b", TypeCode::U64,  false),
                col_def("c", TypeCode::U128, false),
                col_def("v", TypeCode::I64,  true),
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
            op:   BinaryOperator::And,
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
            left:  Box::new(eq_expr("b", num_expr("2"))),
            op:    BinaryOperator::And,
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
        let expr = eq_expr("a", num_expr("1"));   // only one of two PK cols
        assert!(super::try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_incomplete_pk_with_payload_returns_none() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (v = 9) — `a` binds, `v` is a payload conjunct that goes
        // to the residual; the PK stays incomplete (`b` unbound) → None.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op:   BinaryOperator::And,
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
            op:   BinaryOperator::And,
            right: Box::new(eq_expr("a", num_expr("2"))),
        };
        assert!(super::try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn pk_seek_residual_keeps_non_pk_conjunct() {
        let schema = pk_schema(TypeCode::U64);
        // id = 1 AND v = 9 → PK binds fully; `v = 9` becomes the residual.
        let expr = Expr::BinaryOp {
            left:  Box::new(eq_expr("id", num_expr("1"))),
            op:    BinaryOperator::And,
            right: Box::new(eq_expr("v",  num_expr("9"))),
        };
        let (pk, residual) = super::try_extract_pk_seek_residual(&expr, &schema).expect("PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert_eq!(residual.len(), 1);
    }

    #[test]
    fn collect_index_seek_candidates_skips_float_col() {
        // `WHERE val = 1` on a float column emits no candidate: a secondary
        // index can never cover it, so issuing a seek would be a guaranteed
        // NoIndex round trip.
        let schema = make_schema_col(TypeCode::F64);
        let expr = eq_expr("val", num_expr("1"));
        assert!(super::collect_index_seek_candidates(&expr, &schema).is_empty());
    }

    #[test]
    fn collect_index_seek_candidates_flattens_and_tree() {
        // pk(U64) + a,b,c (all U64, pk-eligible non-PK).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a",  TypeCode::U64, true),
                col_def("b",  TypeCode::U64, true),
                col_def("c",  TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        // (a = 1 AND b = 2) AND c = 3 — left-assoc nesting.
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left:  Box::new(eq_expr("a", num_expr("1"))),
                op:    BinaryOperator::And,
                right: Box::new(eq_expr("b", num_expr("2"))),
            }),
            op:    BinaryOperator::And,
            right: Box::new(eq_expr("c", num_expr("3"))),
        };
        let cands = super::collect_index_seek_candidates(&expr, &schema);
        // All three conjuncts are candidates — would regress to just `c` under a
        // two-way-only extractor.
        let cols: Vec<usize> = cands.iter().map(|(c, _, _)| *c).collect();
        assert_eq!(cols, vec![1, 2, 3]);
        // Each candidate's residual is the other two conjuncts.
        for (_, _, residual) in &cands {
            assert_eq!(residual.len(), 2);
        }
    }

    #[test]
    fn write_set_columns_carries_blob_column_through() {
        // UPDATE assigns `v` only; the unmodified BLOB column must carry
        // through. Red against the missing `ColData::Bytes` arm (unreachable!).
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64,  false),
                col_def("b",  TypeCode::Blob, true),
                col_def("v",  TypeCode::I64,  true),
            ],
            pk_cols: vec![0],
        };
        let mut current = ZSetBatch::new(&schema);
        current.pks.push_u128(1u128);
        current.weights.push(1);
        current.nulls.push(0);
        if let ColData::Bytes(v) = &mut current.columns[1] { v.push(Some(vec![1, 2, 3])); }
        if let ColData::Fixed(buf) = &mut current.columns[2] { buf.extend_from_slice(&7i64.to_le_bytes()); }

        let assignments = vec![(2usize, BoundExpr::LitInt(99))];
        let mut dst = ZSetBatch::new(&schema);
        write_set_columns(&current, 0, &assignments, &schema, &mut dst).unwrap();

        if let ColData::Bytes(v) = &dst.columns[1] {
            assert_eq!(v[0].as_deref(), Some(&[1u8, 2, 3][..]));
        } else { panic!("expected Bytes column carried through"); }
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
            other => panic!("expected PkColumn::Bytes, got {:?}", other),
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
            span:  sqlparser::tokenizer::Span::empty(),
        })
    }

    #[test]
    fn test_double_quoted_not_a_uuid_seek_literal() {
        // A single-quoted UUID is a valid seek key (see
        // test_uuid_index_seek_string_literal); a double-quoted token is an
        // identifier in GenericDialect, so it must NOT be treated as a literal.
        let schema = uuid_schema_payload();
        let sq = eq_expr("uid", uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"));
        assert!(super::try_col_eq_literal(&sq, &schema).is_some(), "single-quoted UUID is a seek key");

        let dq = eq_expr("uid", dquote_expr("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(super::try_col_eq_literal(&dq, &schema), None,
                   "double-quoted token must not be parsed as a UUID seek literal");
    }

    #[test]
    fn test_double_quoted_pk_value_rejected() {
        // INSERT path: a double-quoted value in a UUID PK slot is not a literal.
        let err = super::parse_one_pk_literal(
            &dquote_expr("550e8400-e29b-41d4-a716-446655440000"),
            TypeCode::UUID, "id",
        ).expect_err("double-quoted UUID PK literal must be rejected");
        assert!(err.to_string().contains("numeric literal"), "error: {err}");
    }

    // ------------------------------------------------------------------
    // append_null: INSERT and UPDATE produce the byte-identical NULL
    // encoding across every ColData variant.
    // ------------------------------------------------------------------

    #[test]
    fn test_null_append_insert_update_identical_all_variants() {
        let null_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
            value: Value::Null,
            span:  sqlparser::tokenizer::Span::empty(),
        });
        // (fresh empty ColData, wire type, expected NULL encoding) per variant.
        let cases: [(ColData, TypeCode, ColData); 4] = [
            (ColData::Fixed(Vec::new()),   TypeCode::U32,    ColData::Fixed(vec![0u8; 4])),
            (ColData::Strings(Vec::new()), TypeCode::String, ColData::Strings(vec![None])),
            (ColData::Bytes(Vec::new()),   TypeCode::Blob,   ColData::Bytes(vec![None])),
            (ColData::U128s(Vec::new()),   TypeCode::UUID,   ColData::U128s(vec![0u128])),
        ];
        for (empty, tc, expected) in cases {
            let mut via_insert = empty.clone();
            append_value_to_col(&mut via_insert, tc, &null_expr).unwrap();
            let mut via_update = empty.clone();
            append_column_value(&mut via_update, ColumnValue::Null, tc).unwrap();
            assert_eq!(via_insert, expected, "INSERT NULL encoding for {tc:?}");
            assert_eq!(via_update, expected, "UPDATE NULL encoding for {tc:?}");
            assert_eq!(via_insert, via_update, "INSERT vs UPDATE NULL must match for {tc:?}");
        }
    }
}
