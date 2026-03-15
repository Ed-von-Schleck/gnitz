use sqlparser::ast::{
    Query, SetExpr, Values, Expr, Value, SelectItem,
    TableFactor, Statement, TableObject,
};
use gnitz_protocol::{Schema, ZSetBatch, ColData, TypeCode};
use gnitz_core::GnitzClient;
use crate::error::GnitzSqlError;
use crate::binder::Binder;
use crate::SqlResult;

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

pub fn execute_insert(
    client:      &GnitzClient,
    schema_name: &str,
    stmt:        &Statement,
    binder:      &mut Binder<'_>,
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
        batch.nulls.push(0);

        let mut col_data_idx = 0usize; // index into row (skipping PK col)
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
            append_value_to_col(&mut batch.columns[ci], col_def.type_code, val_expr)?;
        }
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
    client:      &GnitzClient,
    schema_name: &str,
    query:       &Query,
    binder:      &mut Binder<'_>,
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
        // Only accept WHERE pk_col = literal (point lookup via seek)
        if let Some((pk_lo, pk_hi)) = try_extract_pk_seek(where_expr, &schema) {
            client.seek(tid, pk_lo, pk_hi)?
        } else {
            return Err(GnitzSqlError::Plan(
                "Non-indexed WHERE in SELECT; use CREATE VIEW for server-side filtering".to_string()
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

    // Extract named columns
    let mut col_indices: Vec<usize> = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("column '{}' not found in projection", ident.value)
                    ))?;
                col_indices.push(idx);
            }
            SelectItem::Wildcard(_) => {
                for i in 0..schema.columns.len() { col_indices.push(i); }
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
    let n = src_batch.pk_lo.len();
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
