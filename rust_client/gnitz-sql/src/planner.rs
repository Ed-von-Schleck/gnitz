use std::collections::HashMap;
use sqlparser::ast::{
    Statement, ObjectType, ColumnOption, TableConstraint, Query, SetExpr, Expr,
    SelectItem, TableFactor, JoinOperator, JoinConstraint, GroupByExpr,
    SetOperator, SetQuantifier,
    FunctionArguments, FunctionArg, FunctionArgExpr,
};
use gnitz_protocol::{ColumnDef, Schema, TypeCode};
use gnitz_core::{
    GnitzClient, CircuitBuilder, ExprBuilder,
    AGG_COUNT, AGG_COUNT_NON_NULL, AGG_SUM, AGG_MIN, AGG_MAX,
};
use crate::error::GnitzSqlError;
use crate::binder::{Binder, resolve_qualified_column, resolve_unqualified_column};
use crate::types::sql_type_to_typecode;
use crate::expr::compile_bound_expr;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp, AggFunc};
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

    // Process CTEs (WITH clause) — inline them into the binder cache
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(GnitzSqlError::Unsupported(
                "recursive CTEs not supported".to_string()
            ));
        }
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            // Resolve the CTE's SELECT to find its source table and schema
            let cte_select = match cte.query.body.as_ref() {
                SetExpr::Select(s) => s,
                _ => return Err(GnitzSqlError::Unsupported(
                    format!("CTE '{}': only SELECT supported", cte_name)
                )),
            };
            if cte_select.from.len() != 1 || !cte_select.from[0].joins.is_empty() {
                return Err(GnitzSqlError::Unsupported(
                    format!("CTE '{}': only single table without JOINs", cte_name)
                ));
            }
            let cte_table_name = match &cte_select.from[0].relation {
                TableFactor::Table { name, .. } => name.0.last()
                    .and_then(|p| p.as_ident()).map(|i| i.value.clone())
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("CTE '{}': empty source table name", cte_name)
                    ))?,
                _ => return Err(GnitzSqlError::Unsupported(
                    format!("CTE '{}': only simple table in FROM", cte_name)
                )),
            };
            // Resolve the CTE's source table and cache the CTE name as an alias
            let resolved = binder.resolve(&cte_table_name)?;
            binder.cache_alias(cte_name, resolved);
        }
    }

    // Handle set operations (UNION, INTERSECT, EXCEPT)
    match query.body.as_ref() {
        SetExpr::SetOperation { op, set_quantifier, left, right } => {
            return execute_create_set_op_view(
                client, schema_name, &view_name, &sql_text, *op, *set_quantifier,
                left, right, binder, query,
            );
        }
        SetExpr::Select(_) => { /* fall through */ }
        _ => return Err(GnitzSqlError::Unsupported("CREATE VIEW only supports SELECT".to_string())),
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };

    // Handle SELECT DISTINCT
    if select.distinct.is_some() {
        return execute_create_distinct_view(
            client, schema_name, &view_name, &sql_text, select, binder,
        );
    }

    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported("CREATE VIEW: only single FROM item supported".to_string()));
    }
    // Delegate to join handler if JOINs present
    if !select.from[0].joins.is_empty() {
        return execute_create_join_view(
            client, schema_name, &view_name, &sql_text, select, binder, query,
        );
    }

    // Check for GROUP BY and delegate
    let has_group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        _ => false,
    };
    if has_group_by {
        return execute_create_group_by_view(
            client, schema_name, &view_name, &sql_text, select, binder,
        );
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
    Computed { bound_expr: BoundExpr, #[allow(dead_code)] out_type: TypeCode },
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
        BoundExpr::AggCall { func, arg } => match func {
            AggFunc::Avg => TypeCode::F64,
            AggFunc::Min | AggFunc::Max => {
                if let Some(inner) = arg {
                    infer_expr_type(inner, schema)
                } else {
                    TypeCode::I64
                }
            }
            _ => TypeCode::I64,
        },
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

    // Determine join type
    let (on_expr, is_left_join) = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr))
        | JoinOperator::Join(JoinConstraint::On(expr)) => (expr, false),
        JoinOperator::LeftOuter(JoinConstraint::On(expr))
        | JoinOperator::Left(JoinConstraint::On(expr)) => (expr, true),
        _ => return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW: only INNER JOIN / LEFT JOIN ... ON supported".to_string()
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

    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    // Path AB projection: identity (already canonical: [PK, A_cols, B_cols])
    let proj_ab: Vec<usize> = (1..1 + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);

    // Path BA projection: reorder [A_cols, B_cols]
    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n {
        proj_ba.push(1 + right_n + i);
    }
    for i in 0..right_n {
        proj_ba.push(1 + i);
    }
    let proj_ba_node = cb.map(join_ba, &proj_ba);

    let inner_merged = cb.union(proj_ab_node, proj_ba_node);

    let merged = if is_left_join {
        // Single-pass LEFT OUTER JOIN: outer_ab handles ΔA (inner rows + null-fill for
        // unmatched); proj_ba_node handles ΔB updates via the inner join path.
        // Do NOT union with inner_merged — that would double-count proj_ab_node.
        let _ = inner_merged; // suppress unused-variable warning
        let outer_ab = cb.left_join_with_trace_node(reindex_a, trace_b);
        cb.union(outer_ab, proj_ba_node)
    } else {
        inner_merged
    };

    // Build virtual combined output schema: U128 PK + all left cols + all right cols.
    // After proj_ab/proj_ba, the UNION output has this layout (union col indices):
    //   col 0: U128_pk (PK)
    //   col 1..left_n: all A columns (in A schema order)
    //   col left_n+1..left_n+right_n: all B columns (in B schema order)
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
        let mut c = col.clone();
        if is_left_join { c.is_nullable = true; }
        out_cols.push(c);
    }

    // Compute user-specified projection and view schema.
    // `combined_idx` is the 0-based index in [A_cols..B_cols]; union output col = combined_idx+1.
    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let (final_cols, final_projection) = if is_wildcard {
        let proj: Vec<usize> = (1..1 + left_n + right_n).collect();
        (out_cols, proj)
    } else {
        let mut cols = Vec::new();
        let mut proj = Vec::new();
        // Always include join PK as first column
        cols.push(out_cols[0].clone());
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let idx = resolve_unqualified_column(&ident.value, &alias_map)?;
                    cols.push(out_cols[1 + idx].clone());
                    proj.push(idx + 1); // union output col index = combined_idx + 1
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let idx = resolve_qualified_column(
                        &parts[0].value, &parts[1].value, &alias_map,
                    )?;
                    cols.push(out_cols[1 + idx].clone());
                    proj.push(idx + 1);
                }
                SelectItem::Wildcard(_) => {
                    for i in 1..out_cols.len() {
                        cols.push(out_cols[i].clone());
                        proj.push(i);
                    }
                }
                _ => return Err(GnitzSqlError::Unsupported(
                    "unsupported SELECT item in JOIN view".to_string()
                )),
            }
        }
        (cols, proj)
    };

    // Apply final column projection before sink when not identity.
    // Identity = selecting all left+right cols in canonical order [1..left_n+right_n].
    let is_identity = final_projection.len() == left_n + right_n
        && final_projection.iter().enumerate().all(|(i, &p)| p == i + 1);
    let sink_input = if is_identity {
        merged
    } else {
        cb.map(merged, &final_projection)
    };
    cb.sink(sink_input, view_id);
    let circuit = cb.build();

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

// ---------------------------------------------------------------------------
// GROUP BY support
// ---------------------------------------------------------------------------

/// Tracks how a user-level aggregate maps to reduce agg_specs.
struct AggMapping {
    specs_start: usize,     // index into agg_specs
    specs_count: usize,     // 1 for normal, 2 for AVG
    is_avg: bool,
    output_name: String,
    output_type: TypeCode,
}

/// What each SELECT item represents in a GROUP BY query.
enum GroupBySelectItem {
    GroupCol { src_col: usize, name: String },
    Aggregate { agg_idx: usize },
}

fn execute_create_group_by_view(
    client:      &GnitzClient,
    schema_name: &str,
    view_name:   &str,
    sql_text:    &str,
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // 1. Resolve source table
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident()).map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty source table name".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported("GROUP BY: only simple table in FROM".to_string())),
    };
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    // 2. Parse GROUP BY → group column indices
    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        _ => return Err(GnitzSqlError::Unsupported("GROUP BY: only expression list supported".to_string())),
    };
    let mut group_col_indices: Vec<usize> = Vec::new();
    for ge in group_exprs {
        match ge {
            Expr::Identifier(id) => {
                let idx = source_schema.columns.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&id.value))
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("GROUP BY column '{}' not found", id.value)
                    ))?;
                group_col_indices.push(idx);
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "GROUP BY: only simple column references supported".to_string()
            )),
        }
    }

    // 3. Analyze SELECT items → group cols + aggregates
    let mut agg_mappings: Vec<AggMapping> = Vec::new();
    let mut select_items: Vec<GroupBySelectItem> = Vec::new();
    let mut agg_specs: Vec<(u64, usize)> = Vec::new();

    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::UnnamedExpr(expr) => (expr, None),
            _ => return Err(GnitzSqlError::Unsupported(
                "GROUP BY: unsupported SELECT item".to_string()
            )),
        };

        let bound = binder.bind_expr(expr, &source_schema)?;
        match &bound {
            BoundExpr::ColRef(col_idx) => {
                if !group_col_indices.contains(col_idx) {
                    return Err(GnitzSqlError::Plan(format!(
                        "column '{}' must appear in GROUP BY or an aggregate function",
                        source_schema.columns[*col_idx].name
                    )));
                }
                let name = alias.unwrap_or_else(|| source_schema.columns[*col_idx].name.clone());
                select_items.push(GroupBySelectItem::GroupCol { src_col: *col_idx, name });
            }
            BoundExpr::AggCall { func, arg } => {
                let src_col = match arg {
                    Some(a) => match a.as_ref() {
                        BoundExpr::ColRef(c) => Some(*c),
                        _ => return Err(GnitzSqlError::Unsupported(
                            "aggregate on computed expression not supported".to_string()
                        )),
                    },
                    None => None,
                };
                let agg_idx = agg_mappings.len();
                let start = agg_specs.len();
                let (out_name, out_type, is_avg) = match func {
                    AggFunc::Count => {
                        agg_specs.push((AGG_COUNT, 0));
                        (alias.unwrap_or_else(|| format!("_count{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::CountNonNull => {
                        agg_specs.push((AGG_COUNT_NON_NULL, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_count{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Sum => {
                        agg_specs.push((AGG_SUM, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_sum{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Min => {
                        agg_specs.push((AGG_MIN, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_min{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Max => {
                        agg_specs.push((AGG_MAX, src_col.unwrap()));
                        (alias.unwrap_or_else(|| format!("_max{}", idx)), TypeCode::I64, false)
                    }
                    AggFunc::Avg => {
                        let col = src_col.unwrap();
                        agg_specs.push((AGG_SUM, col));
                        agg_specs.push((AGG_COUNT_NON_NULL, col));
                        (alias.unwrap_or_else(|| format!("_avg{}", idx)), TypeCode::F64, true)
                    }
                };
                let count = agg_specs.len() - start;
                agg_mappings.push(AggMapping {
                    specs_start: start, specs_count: count, is_avg,
                    output_name: out_name, output_type: out_type,
                });
                select_items.push(GroupBySelectItem::Aggregate { agg_idx });
            }
            _ => return Err(GnitzSqlError::Plan(
                "GROUP BY SELECT: only column refs and aggregates supported".to_string()
            )),
        }
    }

    // 4. Determine reduce output schema layout
    let use_natural_pk = group_col_indices.len() == 1 && matches!(
        source_schema.columns[group_col_indices[0]].type_code,
        TypeCode::U64 | TypeCode::U128
    );

    // agg_col_offset: index of first aggregate column in reduce output
    let agg_col_offset = if use_natural_pk { 1 } else { 1 + group_col_indices.len() };

    // Build the reduce output schema (mirrors server's _build_reduce_output_schema)
    let mut reduce_schema_cols: Vec<ColumnDef> = Vec::new();
    if use_natural_pk {
        reduce_schema_cols.push(source_schema.columns[group_col_indices[0]].clone());
    } else {
        reduce_schema_cols.push(ColumnDef {
            name: "_group_pk".into(), type_code: TypeCode::U128, is_nullable: false,
        });
        for &gi in &group_col_indices {
            reduce_schema_cols.push(source_schema.columns[gi].clone());
        }
    }
    for _ in 0..agg_specs.len() {
        reduce_schema_cols.push(ColumnDef {
            name: "_agg".into(), type_code: TypeCode::I64, is_nullable: false,
        });
    }
    let reduce_schema = Schema { columns: reduce_schema_cols, pk_index: 0 };

    // 5. Build circuit
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();

    // Optional WHERE filter
    let filtered = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        let prog = eb.build(result_reg);
        cb.filter(inp, Some(prog))
    } else {
        inp
    };

    // REDUCE — always use multi-agg path
    let reduced = cb.reduce_multi(filtered, &group_col_indices, &agg_specs);

    // 6. Post-reduce MAP: project group cols + compute aggregates (AVG = SUM/COUNT)
    //    Reduce output: [pk, (group_cols...), agg0, agg1, ...]
    //    MAP inherits PK from input; ExprProgram writes payload columns only.
    //    Output:        [pk(inherited), named_group_col0, ..., named_agg0, ...]
    let mut post_map_eb = ExprBuilder::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    let mut payload_idx: u32 = 0;

    // PK column in output schema (inherited by MAP, not written by ExprProgram)
    out_cols.push(reduce_schema.columns[0].clone());

    for si in &select_items {
        match si {
            GroupBySelectItem::GroupCol { src_col, name } => {
                // Find group col position in reduce output
                let reduce_col = if use_natural_pk {
                    0 // natural PK: group col is at index 0 (same as PK)
                } else {
                    // synthetic PK: group cols start at index 1
                    1 + group_col_indices.iter().position(|&gi| gi == *src_col).unwrap()
                };
                let tc = reduce_schema.columns[reduce_col].type_code;
                post_map_eb.copy_col(tc as u32, reduce_col as u32, payload_idx);
                out_cols.push(ColumnDef {
                    name: name.clone(), type_code: tc, is_nullable: false,
                });
                payload_idx += 1;
            }
            GroupBySelectItem::Aggregate { agg_idx } => {
                let m = &agg_mappings[*agg_idx];
                if m.is_avg {
                    // AVG = SUM / COUNT: two agg_specs were pushed (SUM, COUNT)
                    let sum_col = (agg_col_offset + m.specs_start) as u32;
                    let cnt_col = (agg_col_offset + m.specs_start + 1) as u32;
                    let sum_reg = post_map_eb.load_col_int(sum_col as usize);
                    let cnt_reg = post_map_eb.load_col_int(cnt_col as usize);
                    let sum_f = post_map_eb.int_to_float(sum_reg);
                    let cnt_f = post_map_eb.int_to_float(cnt_reg);
                    let avg_reg = post_map_eb.float_div(sum_f, cnt_f);
                    post_map_eb.emit_col(avg_reg, payload_idx);
                    out_cols.push(ColumnDef {
                        name: m.output_name.clone(), type_code: TypeCode::F64,
                        is_nullable: false,
                    });
                    payload_idx += 1;
                } else {
                    // Direct aggregate: single spec
                    let agg_col = (agg_col_offset + m.specs_start) as u32;
                    let tc = reduce_schema.columns[agg_col as usize].type_code;
                    post_map_eb.copy_col(tc as u32, agg_col, payload_idx);
                    out_cols.push(ColumnDef {
                        name: m.output_name.clone(), type_code: m.output_type,
                        is_nullable: false,
                    });
                    payload_idx += 1;
                }
            }
        }
    }

    // The result_reg for a MAP program is typically 0 (true = pass through)
    let post_map_prog = post_map_eb.build(0);
    let mapped = cb.map_expr(reduced, post_map_prog);

    let post_map_schema = Schema { columns: out_cols.clone(), pk_index: 0 };

    // 7. Optional HAVING filter (applied after MAP so column indices match output)
    let having_input = if let Some(having_expr) = &select.having {
        let bound = bind_having_expr(
            having_expr, &post_map_schema, &source_schema,
            &group_col_indices, &agg_mappings, &select_items, use_natural_pk,
        )?;
        let mut heb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &post_map_schema, &mut heb)?;
        let prog = heb.build(result_reg);
        cb.filter(mapped, Some(prog))
    } else {
        mapped
    };

    // 8. Sink
    cb.sink(having_input, view_id);
    let circuit = cb.build();

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Bind a HAVING expression against the post-reduce schema.
/// Aggregate function calls are resolved to their output column indices.
fn bind_having_expr(
    expr:             &Expr,
    post_map_schema:  &Schema,
    source_schema:    &Schema,
    group_col_indices: &[usize],
    agg_mappings:     &[AggMapping],
    select_items:     &[GroupBySelectItem],
    use_natural_pk:   bool,
) -> Result<BoundExpr, GnitzSqlError> {
    match expr {
        Expr::Identifier(ident) => {
            // Column name → must be a group column
            let col_name = &ident.value;
            let idx = post_map_schema.columns.iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| GnitzSqlError::Bind(
                    format!("HAVING: column '{}' not found in output", col_name)
                ))?;
            Ok(BoundExpr::ColRef(idx))
        }
        Expr::Function(func) => {
            // Aggregate function → find matching agg_mapping by function name + arg
            let name = func.name.to_string().to_lowercase();
            let arg_col = extract_func_arg_col(func, source_schema)?;
            let agg_func = match name.as_str() {
                "count" => if arg_col.is_none() { AggFunc::Count } else { AggFunc::CountNonNull },
                "sum" => AggFunc::Sum,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                "avg" => AggFunc::Avg,
                _ => return Err(GnitzSqlError::Unsupported(
                    format!("HAVING: function '{}' not supported", name)
                )),
            };
            // Find the matching aggregate in select_items
            for si in select_items {
                if let GroupBySelectItem::Aggregate { agg_idx } = si {
                    let m = &agg_mappings[*agg_idx];
                    // Match by output name and type
                    let matches = match (agg_func, m.is_avg) {
                        (AggFunc::Avg, true) => true,
                        (AggFunc::Count, false) if m.output_type == TypeCode::I64 => {
                            m.specs_count == 1
                        }
                        _ => !m.is_avg,
                    };
                    if matches {
                        // Find this agg's position in the output schema
                        let out_idx = post_map_schema.columns.iter()
                            .position(|c| c.name == m.output_name)
                            .unwrap();
                        return Ok(BoundExpr::ColRef(out_idx));
                    }
                }
            }
            Err(GnitzSqlError::Bind(
                format!("HAVING: aggregate {}({}) not found in SELECT", name,
                    arg_col.map_or("*".to_string(), |c| source_schema.columns[c].name.clone()))
            ))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = bind_having_expr(left, post_map_schema, source_schema,
                group_col_indices, agg_mappings, select_items, use_natural_pk)?;
            let r = bind_having_expr(right, post_map_schema, source_schema,
                group_col_indices, agg_mappings, select_items, use_natural_pk)?;
            let bop = match op {
                sqlparser::ast::BinaryOperator::Plus  => BinOp::Add,
                sqlparser::ast::BinaryOperator::Minus => BinOp::Sub,
                sqlparser::ast::BinaryOperator::Eq    => BinOp::Eq,
                sqlparser::ast::BinaryOperator::NotEq => BinOp::Ne,
                sqlparser::ast::BinaryOperator::Gt    => BinOp::Gt,
                sqlparser::ast::BinaryOperator::GtEq  => BinOp::Ge,
                sqlparser::ast::BinaryOperator::Lt    => BinOp::Lt,
                sqlparser::ast::BinaryOperator::LtEq  => BinOp::Le,
                sqlparser::ast::BinaryOperator::And   => BinOp::And,
                sqlparser::ast::BinaryOperator::Or    => BinOp::Or,
                op => return Err(GnitzSqlError::Unsupported(
                    format!("HAVING: operator {:?} not supported", op)
                )),
            };
            Ok(BoundExpr::BinOp(Box::new(l), bop, Box::new(r)))
        }
        Expr::Value(vws) => {
            match &vws.value {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(BoundExpr::LitInt(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(BoundExpr::LitFloat(f))
                    } else {
                        Err(GnitzSqlError::Bind(format!("HAVING: invalid number {}", n)))
                    }
                }
                _ => Err(GnitzSqlError::Unsupported("HAVING: unsupported value type".to_string())),
            }
        }
        Expr::Nested(inner) => bind_having_expr(
            inner, post_map_schema, source_schema,
            group_col_indices, agg_mappings, select_items, use_natural_pk,
        ),
        _ => Err(GnitzSqlError::Unsupported(
            format!("HAVING: unsupported expression {:?}", expr)
        )),
    }
}

/// Extract the column index from a function argument (for HAVING binding).
fn extract_func_arg_col(func: &sqlparser::ast::Function, schema: &Schema) -> Result<Option<usize>, GnitzSqlError> {
    match &func.args {
        FunctionArguments::List(list) if list.args.len() == 1 => {
            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                    let idx = schema.columns.iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&id.value))
                        .ok_or_else(|| GnitzSqlError::Bind(
                            format!("column '{}' not found", id.value)
                        ))?;
                    Ok(Some(idx))
                }
                _ => Err(GnitzSqlError::Unsupported("HAVING: unsupported function argument".to_string())),
            }
        }
        _ => Err(GnitzSqlError::Unsupported("HAVING: function requires one argument".to_string())),
    }
}

// ---------------------------------------------------------------------------
// Set operations: UNION ALL, UNION, INTERSECT, EXCEPT
// ---------------------------------------------------------------------------

fn compile_set_op_side(
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
    cb:          &mut CircuitBuilder,
) -> Result<(gnitz_core::NodeId, Vec<ColumnDef>), GnitzSqlError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "set operation: each side must be a simple SELECT from one table".to_string()
        ));
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident()).map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty table name in set operation".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported("set operation: only simple table in FROM".to_string())),
    };
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    let inp = cb.input_delta_tagged(source_tid);

    // Optional WHERE
    let filtered = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        let prog = eb.build(result_reg);
        cb.filter(inp, Some(prog))
    } else {
        inp
    };

    // Reindex by PK column with all columns as payload
    let prog = build_reindex_program(&source_schema);
    let reindexed = cb.map_reindex(filtered, source_schema.pk_index, prog);

    // Determine output columns (from projection)
    let out_cols: Vec<ColumnDef> = source_schema.columns.clone();
    Ok((reindexed, out_cols))
}

fn execute_create_set_op_view(
    client:          &GnitzClient,
    schema_name:     &str,
    view_name:       &str,
    sql_text:        &str,
    op:              SetOperator,
    set_quantifier:  SetQuantifier,
    left:            &SetExpr,
    right:           &SetExpr,
    binder:          &mut Binder<'_>,
    _query:          &Query,
) -> Result<SqlResult, GnitzSqlError> {
    let left_select = match left {
        SetExpr::Select(s) => s,
        _ => return Err(GnitzSqlError::Unsupported("set operation: left side must be a SELECT".to_string())),
    };
    let right_select = match right {
        SetExpr::Select(s) => s,
        _ => return Err(GnitzSqlError::Unsupported("set operation: right side must be a SELECT".to_string())),
    };

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, 0);

    let (left_node, left_cols) = compile_set_op_side(left_select, binder, &mut cb)?;
    let (right_node, right_cols) = compile_set_op_side(right_select, binder, &mut cb)?;

    // Schema compatibility check
    if left_cols.len() != right_cols.len() {
        return Err(GnitzSqlError::Plan(format!(
            "set operation: column count mismatch ({} vs {})", left_cols.len(), right_cols.len()
        )));
    }

    let out_node = match (op, set_quantifier) {
        (SetOperator::Union, SetQuantifier::All) => {
            cb.union(left_node, right_node)
        }
        (SetOperator::Union, _) => {
            // UNION (distinct): union → distinct
            let merged = cb.union(left_node, right_node);
            cb.distinct(merged)
        }
        (SetOperator::Intersect, _) => {
            // INTERSECT: integrate both sides, bidirectional semi-join
            let trace_l = cb.integrate_trace(left_node);
            let trace_r = cb.integrate_trace(right_node);
            let semi_lr = cb.semi_join_with_trace_node(left_node, trace_r);
            let semi_rl = cb.semi_join_with_trace_node(right_node, trace_l);
            cb.union(semi_lr, semi_rl)
        }
        (SetOperator::Except, _) => {
            // EXCEPT: anti-join left against right trace
            let trace_r = cb.integrate_trace(right_node);
            cb.anti_join_with_trace_node(left_node, trace_r)
        }
        _ => return Err(GnitzSqlError::Unsupported(
            format!("set operation {:?} not supported", op)
        )),
    };

    cb.sink(out_node, view_id);
    let circuit = cb.build();

    // Output schema: U128 PK + all payload columns
    let mut out_cols_final: Vec<ColumnDef> = Vec::new();
    out_cols_final.push(ColumnDef {
        name: "_set_pk".into(), type_code: TypeCode::U128, is_nullable: false,
    });
    for col in &left_cols {
        out_cols_final.push(col.clone());
    }

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols_final)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

// ---------------------------------------------------------------------------
// SELECT DISTINCT
// ---------------------------------------------------------------------------

fn execute_create_distinct_view(
    client:      &GnitzClient,
    schema_name: &str,
    view_name:   &str,
    sql_text:    &str,
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "SELECT DISTINCT: only single table without JOINs".to_string()
        ));
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.0.last()
            .and_then(|p| p.as_ident()).map(|i| i.value.clone())
            .ok_or_else(|| GnitzSqlError::Bind("empty source table name".to_string()))?,
        _ => return Err(GnitzSqlError::Unsupported("SELECT DISTINCT: only simple table in FROM".to_string())),
    };
    let (source_tid, source_schema) = binder.resolve(&table_name)?;

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();

    // Optional WHERE
    let filtered = if let Some(where_expr) = &select.selection {
        let bound = binder.bind_expr(where_expr, &source_schema)?;
        let mut eb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
        let prog = eb.build(result_reg);
        cb.filter(inp, Some(prog))
    } else {
        inp
    };

    // Reindex by PK → distinct
    let prog = build_reindex_program(&source_schema);
    let reindexed = cb.map_reindex(filtered, source_schema.pk_index, prog);
    let distinct_node = cb.distinct(reindexed);

    cb.sink(distinct_node, view_id);
    let circuit = cb.build();

    // Output schema: U128 PK + all source columns
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    out_cols.push(ColumnDef {
        name: "_distinct_pk".into(), type_code: TypeCode::U128, is_nullable: false,
    });
    for col in &source_schema.columns {
        out_cols.push(col.clone());
    }

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}
