use std::collections::HashMap;
use sqlparser::ast::{
    Statement, ObjectType, ColumnOption, TableConstraint, Query, SetExpr, Expr,
    SelectItem, TableFactor, JoinOperator, JoinConstraint,
};
use gnitz_protocol::{ColumnDef, Schema, TypeCode};
use gnitz_core::{GnitzClient, CircuitBuilder, ExprBuilder};
use crate::error::GnitzSqlError;
use crate::binder::{Binder, resolve_qualified_column, resolve_unqualified_column};
use crate::types::sql_type_to_typecode;
use crate::expr::compile_bound_expr;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp};
use crate::dml;
use crate::SqlResult;

pub fn execute_statement(
    client:      &GnitzClient,
    schema_name: &str,
    stmt:        &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    let mut binder = Binder::new(client, schema_name);

    match stmt {
        Statement::CreateTable(create) => {
            execute_create_table(client, schema_name, create, &mut binder)
        }
        Statement::Drop { object_type, names, .. } => {
            execute_drop(client, schema_name, object_type, names)
        }
        Statement::CreateView { name, query, .. } => {
            execute_create_view(client, schema_name, name, query, stmt, &mut binder)
        }
        Statement::Insert(_) => {
            dml::execute_insert(client, schema_name, stmt, &mut binder)
        }
        Statement::Query(query) => {
            dml::execute_select(client, schema_name, query, &mut binder)
        }
        Statement::CreateIndex(ci) => {
            execute_create_index(client, schema_name, ci)
        }
        Statement::Update { .. } => dml::execute_update(client, schema_name, stmt, &mut binder),
        Statement::Delete(_)     => dml::execute_delete(client, schema_name, stmt, &mut binder),
        _ => Err(GnitzSqlError::Unsupported(
            format!("unsupported SQL statement: {:?}", stmt)
        )),
    }
}

fn execute_create_table(
    client:      &GnitzClient,
    schema_name: &str,
    create:      &sqlparser::ast::CreateTable,
    _binder:     &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = create.name.0.last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind("empty table name in CREATE TABLE".to_string()))?;

    let sql_cols = &create.columns;
    let mut pk_idx = 0usize;
    let mut cols: Vec<ColumnDef> = Vec::new();

    for (i, col) in sql_cols.iter().enumerate() {
        let tc = sql_type_to_typecode(&col.data_type)?;
        let is_nullable = !col.options.iter().any(|o| {
            matches!(o.option, ColumnOption::NotNull)
        });
        cols.push(ColumnDef {
            name:        col.name.value.clone(),
            type_code:   tc,
            is_nullable,
        });

        // Detect inline PRIMARY KEY
        for opt in &col.options {
            if matches!(opt.option, ColumnOption::Unique { is_primary: true, .. }) {
                pk_idx = i;
            }
        }
    }

    // Table-level PRIMARY KEY constraint
    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey { columns: pk_cols, .. } = constraint {
            if let Some(pk_col) = pk_cols.first() {
                if let Some(i) = sql_cols.iter().position(|c| c.name.value == pk_col.value) {
                    pk_idx = i;
                }
            }
        }
    }

    // Coerce PK column to unsigned (server requires U64 or U128)
    cols[pk_idx].type_code = match cols[pk_idx].type_code {
        TypeCode::I8  => TypeCode::U8,
        TypeCode::I16 => TypeCode::U16,
        TypeCode::I32 => TypeCode::U32,
        TypeCode::I64 => TypeCode::U64,
        tc => tc,
    };

    let tid = client.create_table(schema_name, &table_name, &cols, pk_idx, true)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::TableCreated { table_id: tid })
}

fn execute_drop(
    client:      &GnitzClient,
    schema_name: &str,
    object_type: &ObjectType,
    names:       &[sqlparser::ast::ObjectName],
) -> Result<SqlResult, GnitzSqlError> {
    for obj_name in names {
        let name = obj_name.0.last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty name in DROP".to_string()))?;

        match object_type {
            ObjectType::Table => {
                client.drop_table(schema_name, &name).map_err(GnitzSqlError::Exec)?;
            }
            ObjectType::View => {
                client.drop_view(schema_name, &name).map_err(GnitzSqlError::Exec)?;
            }
            ObjectType::Index => {
                client.drop_index_by_name(&name).map_err(GnitzSqlError::Exec)?;
            }
            _ => return Err(GnitzSqlError::Unsupported(
                format!("DROP {:?} not supported", object_type)
            )),
        }
    }
    Ok(SqlResult::Dropped)
}

fn execute_create_view(
    client:      &GnitzClient,
    schema_name: &str,
    view_name_obj: &sqlparser::ast::ObjectName,
    query:       &Query,
    stmt:        &Statement,
    binder:      &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = view_name_obj.0.last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind("empty view name in CREATE VIEW".to_string()))?;

    let sql_text = format!("{}", stmt);

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(GnitzSqlError::Unsupported("CREATE VIEW only supports SELECT".to_string())),
    };

    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported("CREATE VIEW: only single FROM item supported".to_string()));
    }
    // Delegate to join handler if JOINs present
    if !select.from[0].joins.is_empty() {
        return execute_create_join_view(
            client, schema_name, &view_name, &sql_text, select, binder, query,
        );
    }
    if select.group_by != sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]) {
        return Err(GnitzSqlError::Unsupported("CREATE VIEW: GROUP BY not supported".to_string()));
    }

    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty source table name".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported("CREATE VIEW: only simple table in FROM".to_string())),
    };

    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    // Build filter expression (if any)
    let expr_prog = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        Some(eb.build(result_reg))
    } else {
        None
    };

    // Build projection column list
    let (items, out_cols) = build_projection(&select.projection, &source_schema, binder)?;

    let has_computed = items.iter().any(|i| matches!(i, ProjectionItem::Computed { .. }));

    // Allocate view_id
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;

    // Build circuit
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None    => inp,
    };

    let out_node = if has_computed {
        // Expr-map: compile all payload items into one ExprProgram
        let mut eb = ExprBuilder::new();
        let mut payload_idx = 0u32;
        for item in &items {
            if is_pk_item(item, &source_schema) { continue; }  // PK handled by commit_row
            match item {
                ProjectionItem::PassThrough { src_col } => {
                    let tc = source_schema.columns[*src_col].type_code as u32;
                    eb.copy_col(tc, *src_col as u32, payload_idx);
                }
                ProjectionItem::Computed { bound_expr, .. } => {
                    let (reg, is_float) = compile_bound_expr(bound_expr, &source_schema, &mut eb)?;
                    if is_float {
                        // EMIT writes via append_int which stores raw bits — correct for float
                    }
                    eb.emit_col(reg, payload_idx);
                }
            }
            payload_idx += 1;
        }
        let program = eb.build(0);  // result_reg unused — EMIT/COPY_COL write directly
        cb.map_expr(filtered, program)
    } else if items.len() < source_schema.columns.len()
           || items.iter().enumerate().any(|(i, item)| match item {
               ProjectionItem::PassThrough { src_col } => *src_col != i,
               _ => false,
           }) {
        // Pure column reorder/subset — use existing projection map
        let cols: Vec<usize> = items.iter().filter_map(|i| match i {
            ProjectionItem::PassThrough { src_col } => Some(*src_col),
            _ => None,
        }).collect();
        cb.map(filtered, &cols)
    } else {
        // Identity — no map needed
        filtered
    };

    cb.sink(out_node, view_id);
    let circuit = cb.build();

    client.create_view_with_circuit(schema_name, &view_name, &sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

fn execute_create_index(
    client:      &GnitzClient,
    schema_name: &str,
    ci:          &sqlparser::ast::CreateIndex,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = ci.table_name.0.last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .ok_or_else(|| GnitzSqlError::Bind("empty table name in CREATE INDEX".to_string()))?;

    if ci.columns.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "CREATE INDEX: only single-column indices supported".to_string()
        ));
    }
    let col_name = match &ci.columns[0].column.expr {
        Expr::Identifier(id) => id.value.clone(),
        _ => return Err(GnitzSqlError::Bind(
            "CREATE INDEX: column must be a simple identifier".to_string()
        )),
    };
    let is_unique = ci.unique;

    let index_id = client.create_index(schema_name, &table_name, &col_name, is_unique)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::IndexCreated { index_id })
}

enum ProjectionItem {
    PassThrough { src_col: usize },
    Computed { bound_expr: BoundExpr, out_type: TypeCode },
}

fn is_pk_item(item: &ProjectionItem, schema: &Schema) -> bool {
    matches!(item, ProjectionItem::PassThrough { src_col } if *src_col == schema.pk_index)
}

fn infer_expr_type(expr: &BoundExpr, schema: &Schema) -> TypeCode {
    match expr {
        BoundExpr::ColRef(idx) => schema.columns[*idx].type_code,
        BoundExpr::LitInt(_) => TypeCode::I64,
        BoundExpr::LitFloat(_) => TypeCode::F64,
        BoundExpr::LitStr(_) => TypeCode::String,
        BoundExpr::BinOp(l, op, r) => {
            let lt = infer_expr_type(l, schema);
            let rt = infer_expr_type(r, schema);
            match op {
                BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Ge |
                BinOp::Lt | BinOp::Le | BinOp::And | BinOp::Or => TypeCode::I64,
                _ => if matches!(lt, TypeCode::F32 | TypeCode::F64)
                     || matches!(rt, TypeCode::F32 | TypeCode::F64)
                     { TypeCode::F64 } else { TypeCode::I64 },
            }
        }
        BoundExpr::UnaryOp(UnaryOp::Neg, inner) => infer_expr_type(inner, schema),
        BoundExpr::UnaryOp(UnaryOp::Not, _) => TypeCode::I64,
        BoundExpr::IsNull(_) | BoundExpr::IsNotNull(_) => TypeCode::I64,
    }
}

fn build_projection(
    projection:    &[SelectItem],
    source_schema: &Schema,
    binder:        &Binder<'_>,
) -> Result<(Vec<ProjectionItem>, Vec<ColumnDef>), GnitzSqlError> {
    let is_wildcard = projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    if is_wildcard {
        let items: Vec<ProjectionItem> = (0..source_schema.columns.len())
            .map(|i| ProjectionItem::PassThrough { src_col: i })
            .collect();
        return Ok((items, source_schema.columns.clone()));
    }

    let mut items: Vec<ProjectionItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();

    for (idx, item) in projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let col_idx = source_schema.columns.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("column '{}' not found", ident.value)
                    ))?;
                items.push(ProjectionItem::PassThrough { src_col: col_idx });
                out_cols.push(source_schema.columns[col_idx].clone());
            }
            SelectItem::Wildcard(_) => {
                for i in 0..source_schema.columns.len() {
                    items.push(ProjectionItem::PassThrough { src_col: i });
                    out_cols.push(source_schema.columns[i].clone());
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let bound = binder.bind_expr(expr, source_schema)?;
                let out_type = infer_expr_type(&bound, source_schema);
                items.push(ProjectionItem::Computed { bound_expr: bound, out_type });
                out_cols.push(ColumnDef {
                    name: alias.value.clone(),
                    type_code: out_type,
                    is_nullable: true,
                });
            }
            SelectItem::UnnamedExpr(expr) => {
                // Try as column reference first (already handled above for Identifier),
                // otherwise treat as computed expression
                let bound = binder.bind_expr(expr, source_schema)?;
                let out_type = infer_expr_type(&bound, source_schema);
                let col_name = format!("_expr{}", idx);
                items.push(ProjectionItem::Computed { bound_expr: bound, out_type });
                out_cols.push(ColumnDef {
                    name: col_name,
                    type_code: out_type,
                    is_nullable: true,
                });
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "unsupported SELECT item in CREATE VIEW projection".to_string()
            )),
        }
    }

    // Ensure PK is present (as PassThrough) and first
    let pk = source_schema.pk_index;
    let pk_pos = items.iter().position(|i| matches!(i, ProjectionItem::PassThrough { src_col } if *src_col == pk));

    match pk_pos {
        Some(0) => { /* already first, good */ }
        Some(pos) => {
            // Move PK to front
            items.swap(0, pos);
            out_cols.swap(0, pos);
        }
        None => {
            // PK not present — check if any computed item would shadow it
            let pk_computed = items.iter().any(|i| {
                if let ProjectionItem::Computed { bound_expr, .. } = i {
                    matches!(bound_expr, BoundExpr::ColRef(c) if *c == pk)
                } else { false }
            });
            if pk_computed {
                return Err(GnitzSqlError::Unsupported(
                    "PK column cannot be a computed expression in CREATE VIEW".to_string()
                ));
            }
            // Auto-prepend PK
            items.insert(0, ProjectionItem::PassThrough { src_col: pk });
            out_cols.insert(0, source_schema.columns[pk].clone());
        }
    }

    Ok((items, out_cols))
}

// ---------------------------------------------------------------------------
// Equijoin support
// ---------------------------------------------------------------------------

fn execute_create_join_view(
    client:      &GnitzClient,
    schema_name: &str,
    view_name:   &str,
    sql_text:    &str,
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
    _query:      &Query,
) -> Result<SqlResult, GnitzSqlError> {
    // Reject GROUP BY
    if select.group_by != sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]) {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW with JOIN: GROUP BY not supported".to_string()
        ));
    }

    // Extract left table
    let left_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident()).map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty left table name".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW JOIN: only simple table in FROM".to_string()
        )),
    };
    let left_alias = match &select.from[0].relation {
        TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
        _ => left_name.clone(),
    };

    // Only support one join
    if select.from[0].joins.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW: only single JOIN supported".to_string()
        ));
    }
    let join = &select.from[0].joins[0];

    // Extract right table
    let (right_name, right_alias) = match &join.relation {
        TableFactor::Table { name, alias, .. } => {
            let n = name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.clone())
                .ok_or_else(|| GnitzSqlError::Bind("empty right table name".to_string()))?;
            let a = alias.as_ref().map(|al| al.name.value.clone()).unwrap_or_else(|| n.clone());
            (n, a)
        }
        _ => return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW JOIN: only simple table reference".to_string()
        )),
    };

    // Only INNER JOIN ON (sqlparser emits Join for bare "JOIN", Inner for "INNER JOIN")
    let on_expr = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr))
        | JoinOperator::Join(JoinConstraint::On(expr)) => expr,
        _ => return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW: only INNER JOIN ... ON supported".to_string()
        )),
    };

    // Resolve both tables
    let (left_tid, left_schema) = binder.resolve(&left_name)?;
    let (right_tid, right_schema) = binder.resolve(&right_name)?;

    // Build alias map for qualified column resolution
    let mut alias_map: HashMap<String, (u64, Schema, usize)> = HashMap::new();
    alias_map.insert(left_alias.to_lowercase(), (left_tid, left_schema.clone(), 0));
    alias_map.insert(right_alias.to_lowercase(), (right_tid, right_schema.clone(), left_schema.columns.len()));

    // Extract equijoin keys
    let (left_join_col, right_join_col) = extract_equijoin_keys(
        on_expr, &left_schema, &right_schema, &alias_map,
    )?;

    // Allocate view_id
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;

    // Build the reindex ExprProgram for each side.
    // Each side: COPY_COL all columns as payload, reindex by join key column.
    let left_reindex_prog = build_reindex_program(&left_schema);
    let right_reindex_prog = build_reindex_program(&right_schema);

    // Build circuit
    let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
    let input_a = cb.input_delta_tagged(left_tid);
    let input_b = cb.input_delta_tagged(right_tid);
    let reindex_a = cb.map_reindex(input_a, left_join_col, left_reindex_prog);
    let reindex_b = cb.map_reindex(input_b, right_join_col, right_reindex_prog);
    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))

    // After JOIN_DELTA_TRACE, CompositeAccessor produces:
    //   [delta_PK(U128), delta_payloads..., trace_payloads...]
    // For AB: [A_PK, A_cols..., B_cols...]
    // For BA: [B_PK, B_cols..., A_cols...]
    //
    // We need both paths to produce the same canonical output:
    //   [PK(U128), left_cols..., right_cols...]
    //
    // The join output schema after CompositeAccessor has:
    //   col 0: PK (U128) — the delta side's reindexed PK
    //   col 1..N: delta side's payload columns (all original cols as payload from reindex)
    //   col N+1..M: trace side's payload columns
    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();
    // Total composite cols = 1 (PK) + left_n (A payload) + right_n (B payload)
    // For path AB: col 0=PK, 1..left_n=A, left_n+1..=B → already canonical
    // For path BA: col 0=PK, 1..right_n=B, right_n+1..=A → need reorder

    // Path AB projection: identity (already in canonical order)
    // PK (col 0) is auto-copied by op_map, so skip it in projection indices
    let proj_ab: Vec<usize> = (1..1 + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);

    // Path BA projection: reorder [A_cols, B_cols] — skip PK (col 0)
    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n {
        proj_ba.push(1 + right_n + i); // A cols come after B cols in BA
    }
    for i in 0..right_n {
        proj_ba.push(1 + i); // B cols are first payload in BA
    }
    let proj_ba_node = cb.map(join_ba, &proj_ba);

    let merged = cb.union(proj_ab_node, proj_ba_node);
    cb.sink(merged, view_id);
    let circuit = cb.build();

    // Build output schema: U128 PK + all left cols + all right cols
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    out_cols.push(ColumnDef {
        name: "_join_pk".into(),
        type_code: TypeCode::U128,
        is_nullable: false,
    });
    for col in &left_schema.columns {
        out_cols.push(col.clone());
    }
    for col in &right_schema.columns {
        out_cols.push(col.clone());
    }

    // Now apply user-specified projection (SELECT columns)
    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let final_cols = if is_wildcard {
        out_cols
    } else {
        let mut cols = Vec::new();
        // Always include join PK as first column
        cols.push(out_cols[0].clone());
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let idx = resolve_unqualified_column(&ident.value, &alias_map)?;
                    cols.push(out_cols[1 + idx].clone()); // +1 for join PK
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let idx = resolve_qualified_column(
                        &parts[0].value, &parts[1].value, &alias_map,
                    )?;
                    cols.push(out_cols[1 + idx].clone());
                }
                SelectItem::Wildcard(_) => {
                    for i in 1..out_cols.len() {
                        cols.push(out_cols[i].clone());
                    }
                }
                _ => return Err(GnitzSqlError::Unsupported(
                    "unsupported SELECT item in JOIN view".to_string()
                )),
            }
        }
        cols
    };

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Build a reindex ExprProgram that copies all columns as payload.
fn build_reindex_program(schema: &Schema) -> gnitz_core::ExprProgram {
    let mut eb = ExprBuilder::new();
    let mut payload_idx = 0u32;
    for (ci, col) in schema.columns.iter().enumerate() {
        let tc = col.type_code as u32;
        eb.copy_col(tc, ci as u32, payload_idx);
        payload_idx += 1;
    }
    eb.build(0) // result_reg unused — COPY_COL writes directly
}

/// Extract equijoin key columns from an ON expression.
/// Returns (left_col_idx, right_col_idx) for a single-column equijoin.
fn extract_equijoin_keys(
    on_expr:      &Expr,
    left_schema:  &Schema,
    _right_schema: &Schema,
    alias_map:    &HashMap<String, (u64, Schema, usize)>,
) -> Result<(usize, usize), GnitzSqlError> {
    match on_expr {
        Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } => {
            let l_col = resolve_join_col_ref(left, alias_map)?;
            let r_col = resolve_join_col_ref(right, alias_map)?;
            // l_col and r_col are global indices; convert to per-table indices
            let left_n = left_schema.columns.len();
            let (left_idx, right_idx) = if l_col < left_n && r_col >= left_n {
                (l_col, r_col - left_n)
            } else if r_col < left_n && l_col >= left_n {
                (r_col, l_col - left_n)
            } else {
                return Err(GnitzSqlError::Bind(
                    "JOIN ON: each side of = must reference a different table".to_string()
                ));
            };
            Ok((left_idx, right_idx))
        }
        _ => Err(GnitzSqlError::Unsupported(
            "JOIN ON: only simple equijoin (col = col) supported".to_string()
        )),
    }
}

/// Resolve a column reference in a JOIN ON clause to a global column index.
fn resolve_join_col_ref(
    expr:         &Expr,
    alias_map:    &HashMap<String, (u64, Schema, usize)>,
) -> Result<usize, GnitzSqlError> {
    match expr {
        Expr::Identifier(ident) => {
            resolve_unqualified_column(&ident.value, alias_map)
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)
        }
        _ => Err(GnitzSqlError::Unsupported(
            "JOIN ON: only column references supported".to_string()
        )),
    }
}
