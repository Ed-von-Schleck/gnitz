use sqlparser::ast::{
    Statement, ObjectType, ColumnOption, TableConstraint, Query, SetExpr, Values, Expr, Value,
    SelectItem, TableFactor,
};
use gnitz_protocol::{ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_core::{GnitzClient, CircuitBuilder, ExprBuilder};
use crate::error::GnitzSqlError;
use crate::binder::Binder;
use crate::types::sql_type_to_typecode;
use crate::expr::compile_bound_expr;
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
        _ => Err(GnitzSqlError::Unsupported(
            format!("statement type not supported in Phase 1: {:?}", stmt)
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

    // Reject JOINs and GROUP BY for Phase 1
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported("CREATE VIEW: only single-table, no JOIN in Phase 1".to_string()));
    }
    if select.group_by != sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]) {
        return Err(GnitzSqlError::Unsupported("CREATE VIEW: GROUP BY not supported in Phase 1".to_string()));
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
        let result_reg = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        Some(eb.build(result_reg))
    } else {
        None
    };

    // Build projection column list
    let (proj_cols, out_cols): (Option<Vec<usize>>, Vec<ColumnDef>) =
        build_projection(&select.projection, &source_schema)?;

    // Allocate view_id
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;

    // Build circuit
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None    => inp,
    };
    let out_node = match proj_cols {
        Some(ref cols) => cb.map(filtered, cols),
        None           => filtered,
    };
    cb.sink(out_node, view_id);
    let circuit = cb.build();

    client.create_view_with_circuit(schema_name, &view_name, &sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

fn build_projection(
    projection:    &[SelectItem],
    source_schema: &Schema,
) -> Result<(Option<Vec<usize>>, Vec<ColumnDef>), GnitzSqlError> {
    let is_wildcard = projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    if is_wildcard {
        return Ok((None, source_schema.columns.clone()));
    }

    let mut col_indices: Vec<usize> = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = source_schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("column '{}' not found", ident.value)
                    ))?;
                col_indices.push(idx);
            }
            SelectItem::Wildcard(_) => {
                for i in 0..source_schema.columns.len() { col_indices.push(i); }
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "only simple column names in CREATE VIEW projection".to_string()
            )),
        }
    }

    // Always ensure the source PK column is first in the output (server requires U64/U128 PK).
    let pk = source_schema.pk_index;
    if !col_indices.contains(&pk) {
        col_indices.insert(0, pk);
    } else if col_indices[0] != pk {
        // Move PK to front
        let pos = col_indices.iter().position(|&i| i == pk).unwrap();
        col_indices.swap(0, pos);
    }

    let out_cols: Vec<ColumnDef> = col_indices.iter()
        .map(|&i| source_schema.columns[i].clone())
        .collect();

    Ok((Some(col_indices), out_cols))
}
