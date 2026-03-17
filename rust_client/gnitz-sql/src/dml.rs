use sqlparser::ast::{
    Query, SetExpr, Values, Expr, Value, SelectItem,
    TableFactor, Statement, TableObject, BinaryOperator,
    FromTable, Assignment, AssignmentTarget,
};
use gnitz_protocol::{Schema, ZSetBatch, ColData, TypeCode};
use gnitz_core::GnitzClient;
use crate::error::GnitzSqlError;
use crate::binder::Binder;
use crate::logical_plan::BoundExpr;
use crate::SqlResult;

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

pub fn execute_insert(
    client:       &GnitzClient,
    _schema_name: &str,
    stmt:         &Statement,
    binder:       &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Extract table name and source from the INSERT statement
    let (table_name_str, rows) = extract_insert_parts(stmt)?;

    let (tid, schema) = binder.resolve(&table_name_str)?;
    let mut batch = ZSetBatch::new(&schema);
    let n = rows.len();

    for row in &rows {
        let pk_val = extract_pk_value(row, &schema)?;
        batch.pk_lo.push(pk_val);
        batch.pk_hi.push(0);
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

    client.push(tid, &schema, &batch)?;
    Ok(SqlResult::RowsAffected { count: n })
}

fn extract_insert_parts(stmt: &Statement) -> Result<(String, Vec<Vec<Expr>>), GnitzSqlError> {
    match stmt {
        Statement::Insert(insert) => {
            let table_name = match &insert.table {
                TableObject::TableName(obj_name) => obj_name.0.last()
                    .and_then(|p| p.as_ident())
                    .map(|i| i.value.clone())
                    .ok_or_else(|| GnitzSqlError::Bind("empty table name in INSERT".to_string()))?,
                _ => return Err(GnitzSqlError::Unsupported("INSERT with table function not supported".to_string())),
            };

            let source = insert.source.as_ref()
                .ok_or_else(|| GnitzSqlError::Unsupported("INSERT without VALUES not supported".to_string()))?;

            let rows = extract_values_rows(source)?;
            Ok((table_name, rows))
        }
        _ => Err(GnitzSqlError::Bind("not an INSERT statement".to_string())),
    }
}

fn extract_values_rows(query: &Query) -> Result<Vec<Vec<Expr>>, GnitzSqlError> {
    match query.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => Ok(rows.clone()),
        _ => Err(GnitzSqlError::Unsupported(
            "INSERT only supports VALUES (not INSERT INTO ... SELECT)".to_string()
        )),
    }
}

fn extract_pk_value(row: &[Expr], schema: &Schema) -> Result<u64, GnitzSqlError> {
    let pk_expr = row.get(schema.pk_index).ok_or_else(|| {
        GnitzSqlError::Bind("PK column missing from INSERT row".to_string())
    })?;
    match pk_expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse::<u64>().map_err(|_| {
                GnitzSqlError::Bind(format!("PK value is not a valid u64: {}", n))
            }),
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
    let table_name = match &from.relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty table name in FROM".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported("only simple table references supported in FROM".to_string())),
    };

    let (tid, schema) = binder.resolve(&table_name)?;

    // Check WHERE clause
    let (schema_out, batch_opt) = if let Some(where_expr) = &select.selection {
        if let Some((pk_lo, pk_hi)) = try_extract_pk_seek(where_expr, &schema) {
            client.seek(tid, pk_lo, pk_hi)?
        } else if let Some((col_idx, key_lo, key_hi, residual)) =
            try_extract_index_seek(where_expr, &schema, binder, tid, false)?
        {
            let result = client.seek_by_index(tid, col_idx as u64, key_lo, key_hi)?;
            apply_residual_filter(result, residual.as_ref(), &schema)?
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
    let actual_schema = schema_out.unwrap_or(schema);

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
    match expr {
        Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } => {
            // left = col_name, right = number literal (or swapped)
            let is_num = |e: &Expr| matches!(e, Expr::Value(vws) if matches!(vws.value, Value::Number(_, _)));
            let (col_expr, lit_expr) = match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(_), r) if is_num(r) => (left.as_ref(), right.as_ref()),
                (l, Expr::Identifier(_)) if is_num(l) => (right.as_ref(), left.as_ref()),
                _ => return None,
            };
            let col_name = if let Expr::Identifier(ident) = col_expr { &ident.value } else { return None; };
            let n_str = if let Expr::Value(vws) = lit_expr {
                if let Value::Number(n, _) = &vws.value { n } else { return None; }
            } else { return None; };

            // Check that col_name is the PK column
            let pk_col = &schema.columns[schema.pk_index];
            if !pk_col.name.eq_ignore_ascii_case(col_name) { return None; }

            // Parse literal as u64 (pk_lo), pk_hi = 0
            let pk_lo = n_str.parse::<u64>().ok()?;
            Some((pk_lo, 0u64))
        }
        _ => None,
    }
}

/// Extracts (col_idx, key_lo, key_hi) from `col = integer_literal`. Does NOT check index existence.
fn try_col_eq_literal(expr: &Expr, schema: &Schema) -> Option<(usize, u64, u64)> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let is_num = |e: &Expr| matches!(e, Expr::Value(vws) if matches!(vws.value, Value::Number(_, _)));
            let (col_expr, lit_expr) = match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(_), r) if is_num(r) => (left.as_ref(), right.as_ref()),
                (l, Expr::Identifier(_)) if is_num(l) => (right.as_ref(), left.as_ref()),
                _ => return None,
            };
            let col_name = if let Expr::Identifier(id) = col_expr { &id.value } else { return None; };
            let n_str = if let Expr::Value(vws) = lit_expr {
                if let Value::Number(n, _) = &vws.value { n } else { return None; }
            } else { return None; };
            let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))?;
            let key_lo = n_str.parse::<u64>().ok()?;
            Some((col_idx, key_lo, 0u64))
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

#[derive(Debug)]
#[allow(dead_code)]
enum ColumnValue { Int(i64), Float(f64), Str(String), Null }

fn eval_pred_row(
    pred:   &BoundExpr,
    batch:  &ZSetBatch,
    i:      usize,
    schema: &Schema,
) -> Result<bool, GnitzSqlError> {
    Ok(eval_expr(pred, batch, i, schema)? != 0)
}

fn eval_expr(
    expr:   &BoundExpr,
    batch:  &ZSetBatch,
    i:      usize,
    schema: &Schema,
) -> Result<i64, GnitzSqlError> {
    use crate::logical_plan::BinOp;
    match expr {
        BoundExpr::ColRef(c) => {
            let col_def = &schema.columns[*c];
            match &batch.columns[*c] {
                ColData::Fixed(buf) => {
                    let stride = col_def.type_code.wire_stride();
                    let start  = i * stride;
                    let slice  = &buf[start..start + stride];
                    let v = match stride {
                        1 => slice[0] as i8  as i64,
                        2 => i16::from_le_bytes(slice.try_into().unwrap()) as i64,
                        4 => i32::from_le_bytes(slice.try_into().unwrap()) as i64,
                        8 => i64::from_le_bytes(slice.try_into().unwrap()),
                        _ => 0,
                    };
                    Ok(v)
                }
                ColData::Strings(_) => Err(GnitzSqlError::Unsupported(
                    "residual filter on string column not supported".to_string()
                )),
                ColData::U128s(_) => Err(GnitzSqlError::Unsupported(
                    "residual filter on U128 column not supported".to_string()
                )),
            }
        }
        BoundExpr::LitInt(v) => Ok(*v),
        BoundExpr::LitFloat(_) => Err(GnitzSqlError::Unsupported(
            "float literals in residual filter not supported".to_string()
        )),
        BoundExpr::LitStr(_) => Err(GnitzSqlError::Unsupported(
            "string literals in WHERE predicate not supported; \
             use CREATE INDEX or CREATE VIEW".to_string()
        )),
        BoundExpr::BinOp(l, op, r) => {
            let lv = eval_expr(l, batch, i, schema)?;
            let rv = eval_expr(r, batch, i, schema)?;
            Ok(match op {
                BinOp::Add => lv.wrapping_add(rv),
                BinOp::Sub => lv.wrapping_sub(rv),
                BinOp::Mul => lv.wrapping_mul(rv),
                BinOp::Div => if rv == 0 { 0 } else { lv / rv },
                BinOp::Mod => if rv == 0 { 0 } else { lv % rv },
                BinOp::Eq  => (lv == rv) as i64,
                BinOp::Ne  => (lv != rv) as i64,
                BinOp::Gt  => (lv >  rv) as i64,
                BinOp::Ge  => (lv >= rv) as i64,
                BinOp::Lt  => (lv <  rv) as i64,
                BinOp::Le  => (lv <= rv) as i64,
                BinOp::And => ((lv != 0) && (rv != 0)) as i64,
                BinOp::Or  => ((lv != 0) || (rv != 0)) as i64,
            })
        }
        BoundExpr::UnaryOp(op, e) => {
            let v = eval_expr(e, batch, i, schema)?;
            use crate::logical_plan::UnaryOp;
            Ok(match op {
                UnaryOp::Neg => v.wrapping_neg(),
                UnaryOp::Not => (v == 0) as i64,
            })
        }
        BoundExpr::IsNull(c) => {
            // nulls are stored as a bitmask per row; simplified: always false for now
            let _ = c;
            Ok(0)
        }
        BoundExpr::IsNotNull(c) => {
            let _ = c;
            Ok(1)
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
    let new_cols: Vec<gnitz_protocol::ColumnDef> = col_indices.iter()
        .map(|&i| schema.columns[i].clone())
        .collect();
    let new_pk_idx = col_indices.iter().position(|&i| i == schema.pk_index).unwrap_or(0);
    let new_schema = Schema { columns: new_cols, pk_index: new_pk_idx };

    // Build new batch
    let mut new_batch = ZSetBatch::new(&new_schema);
    new_batch.pk_lo  = src_batch.pk_lo.clone();
    new_batch.pk_hi  = src_batch.pk_hi.clone();
    new_batch.weights = src_batch.weights.clone();
    new_batch.nulls   = src_batch.nulls.clone();

    for (new_ci, &old_ci) in col_indices.iter().enumerate() {
        if new_ci == new_pk_idx { continue; }
        if old_ci == schema.pk_index { continue; }
        match (&src_batch.columns[old_ci], &mut new_batch.columns[new_ci]) {
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

fn apply_limit(batch: ZSetBatch, schema: &Schema, limit: usize) -> ZSetBatch {
    let n = batch.pk_lo.len();
    if n <= limit { return batch; }

    let mut new_batch = ZSetBatch::new(schema);
    new_batch.pk_lo  = batch.pk_lo[..limit].to_vec();
    new_batch.pk_hi  = batch.pk_hi[..limit].to_vec();
    new_batch.weights = batch.weights[..limit].to_vec();
    new_batch.nulls   = batch.nulls[..limit].to_vec();

    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index { continue; }
        match (&batch.columns[ci], &mut new_batch.columns[ci]) {
            (ColData::Fixed(src), ColData::Fixed(dst)) => {
                let stride = col_def.type_code.wire_stride();
                dst.extend_from_slice(&src[..limit * stride]);
            }
            (ColData::Strings(src), ColData::Strings(dst)) => {
                dst.extend(src[..limit].iter().cloned());
            }
            (ColData::U128s(src), ColData::U128s(dst)) => {
                dst.extend(src[..limit].iter().copied());
            }
            _ => {}
        }
    }
    new_batch
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
    eval_expr(expr, batch, row_idx, schema).map(ColumnValue::Int)
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
        AssignmentTarget::ColumnName(obj_name) => obj_name.0.last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty column name in SET".to_string())),
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
    dst.nulls.push(current.nulls[row_idx]);

    for (ci, col_def) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index { continue; }
        if let Some((_, expr)) = assignments.iter().find(|(idx, _)| *idx == ci) {
            let cv = eval_set_expr(expr, current, row_idx, schema)?;
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

    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident()).map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty table name in UPDATE".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported(
            "UPDATE: only simple table references supported".to_string()
        )),
    };

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
            let (schema_opt, batch_opt) = client.seek(table_id, pk_lo, pk_hi)?;
            let current = match batch_opt {
                None => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) if b.pk_lo.is_empty() => return Ok(SqlResult::RowsAffected { count: 0 }),
                Some(b) => b,
            };
            let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
            let mut new_batch = ZSetBatch::new(actual_schema);
            write_set_columns(&current, 0, &assignments, actual_schema, &mut new_batch)?;
            client.push(table_id, actual_schema, &new_batch)?;
            return Ok(SqlResult::RowsAffected { count: 1 });
        }

        // Path 2: Unique index seek
        if let Some((col_idx, key_lo, key_hi, residual)) =
            try_extract_index_seek(where_expr, &schema, binder, table_id, true)?
        {
            let (schema_opt, batch_opt) =
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
            client.push(table_id, actual_schema, &new_batch)?;
            return Ok(SqlResult::RowsAffected { count: 1 });
        }

        // Path 3: Full scan with predicate
        let pred = binder.bind_expr(where_expr, &schema)?;
        let (schema_opt, batch_opt) = client.scan(table_id)?;
        let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
        let mut updates = ZSetBatch::new(actual_schema);
        let mut count = 0usize;
        if let Some(ref scan_batch) = batch_opt {
            for i in 0..scan_batch.pk_lo.len() {
                if !eval_pred_row(&pred, scan_batch, i, actual_schema)? { continue; }
                write_set_columns(scan_batch, i, &assignments, actual_schema, &mut updates)?;
                count += 1;
            }
            if count > 0 { client.push(table_id, actual_schema, &updates)?; }
        }
        return Ok(SqlResult::RowsAffected { count });
    }

    // Path 4: No WHERE — update all rows
    let (schema_opt, batch_opt) = client.scan(table_id)?;
    let actual_schema = schema_opt.as_ref().unwrap_or(&schema);
    let mut updates = ZSetBatch::new(actual_schema);
    match batch_opt {
        None => Ok(SqlResult::RowsAffected { count: 0 }),
        Some(ref scan_batch) => {
            let n = scan_batch.pk_lo.len();
            for i in 0..n {
                write_set_columns(scan_batch, i, &assignments, actual_schema, &mut updates)?;
            }
            if n > 0 { client.push(table_id, actual_schema, &updates)?; }
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
    let table_name = match &tables[0].relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident()).map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty table name in DELETE".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported(
            "DELETE: only simple table references supported".to_string()
        )),
    };

    let (table_id, schema) = binder.resolve(&table_name)?;

    match &del.selection {
        None => {
            // DELETE all rows
            let (schema_opt, batch_opt) = client.scan(table_id)?;
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
                let (schema_opt, batch_opt) = client.seek(table_id, pk_lo, pk_hi)?;
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
                let (schema_opt, batch_opt) =
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
            let (schema_opt, batch_opt) = client.scan(table_id)?;
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
