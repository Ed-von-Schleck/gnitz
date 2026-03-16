use sqlparser::ast::{
    Statement, ObjectType, ColumnOption, TableConstraint, Query, SetExpr, Expr,
    SelectItem, TableFactor,
};
use gnitz_protocol::{ColumnDef, Schema, TypeCode};
use gnitz_core::{GnitzClient, CircuitBuilder, ExprBuilder};
use crate::error::GnitzSqlError;
use crate::binder::Binder;
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

    // Reject JOINs and GROUP BY
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported("CREATE VIEW: only single-table queries supported; JOINs not supported".to_string()));
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
