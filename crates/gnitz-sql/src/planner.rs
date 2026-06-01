use std::collections::HashMap;
use sqlparser::ast::{
    Statement, ObjectType, ColumnOption, TableConstraint, Query, SetExpr, Expr,
    SelectItem, TableFactor, JoinOperator, JoinConstraint, GroupByExpr,
    SetOperator, SetQuantifier,
    FunctionArguments, FunctionArg, FunctionArgExpr,
};
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_core::{
    GnitzClient, CircuitBuilder, ExprBuilder,
    AGG_COUNT, AGG_COUNT_NON_NULL, AGG_SUM, AGG_MIN, AGG_MAX,
};
use crate::error::{GnitzSqlError, extract_name, extract_table_factor_name};
use crate::binder::{Binder, resolve_qualified_column, resolve_unqualified_column};
use crate::types::sql_type_to_typecode;
use crate::expr::compile_bound_expr;
use crate::logical_plan::{BoundExpr, BinOp, AggFunc};
use crate::dml;
use crate::SqlResult;

pub fn execute_statement(
    client:      &mut GnitzClient,
    schema_name: &str,
    stmt:        &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    let mut binder = Binder::new(schema_name);

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

fn is_integer_type(tc: TypeCode) -> bool {
    matches!(tc,
        TypeCode::I8  | TypeCode::I16 | TypeCode::I32 | TypeCode::I64
        | TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64
        | TypeCode::U128
    )
}

/// Reject any CREATE VIEW form that reads from a compound-PK source.
/// Lifting this needs the view storage layer (which registers every view
/// with a hardcoded `pk_cols = &[0]`) and every planner reindex site
/// (which calls `pk_index_single()`) to widen first — a separate plan.
fn reject_compound_pk_view_source(schema: &Schema) -> Result<(), GnitzSqlError> {
    if schema.pk_count() >= 2 {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW over a compound-PK table is not yet supported".into()
        ));
    }
    Ok(())
}

/// Resolve a REFERENCES clause to (fk_table_id, ref_col_idx, parent_col_type).
/// The referenced column is a legal target iff it is the parent's lone PK
/// column or it carries its own active UNIQUE index. Validates that
/// fk_col_type is compatible with the referenced column type and returns that
/// type so the caller can widen the child column.
/// Resolve a self-referencing FK against the in-flight column list (the table
/// is not yet registered in the catalog). The referenced column must be the
/// table's lone PK column; returns table id 0 — the sentinel the FK
/// registration path uses for "same table".
fn resolve_fk_target_inline(
    current_cols:     &[ColumnDef],
    current_pk_cols:  &[u32],
    ref_table:        &str,
    referred_columns: &[sqlparser::ast::Ident],
    fk_col_type:      TypeCode,
) -> Result<(u64, u64, TypeCode), GnitzSqlError> {
    if referred_columns.len() > 1 {
        return Err(GnitzSqlError::Unsupported(
            "multi-column FOREIGN KEY references are not supported".into()
        ));
    }
    let ref_col_idx: usize = if let Some(ident) = referred_columns.first() {
        current_cols.iter()
            .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
            .ok_or_else(|| GnitzSqlError::Bind(format!(
                "FK references column '{}' not found in table '{}'",
                ident.value, ref_table,
            )))?
    } else if current_pk_cols.len() == 1 {
        current_pk_cols[0] as usize
    } else {
        return Err(GnitzSqlError::Bind(format!(
            "self-referencing FK against '{}' must name the referenced column",
            ref_table,
        )));
    };

    if !(current_pk_cols.len() == 1 && current_pk_cols[0] as usize == ref_col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "self-referencing FK must reference the primary key of '{}'; \
             column '{}' is not the lone PK",
            ref_table, current_cols[ref_col_idx].name,
        )));
    }

    let parent_col_type = current_cols[ref_col_idx].type_code;
    let is_compat = if is_integer_type(fk_col_type) && is_integer_type(parent_col_type) {
        true
    } else {
        fk_col_type == parent_col_type
    };
    if !is_compat {
        return Err(GnitzSqlError::Bind(format!(
            "FK type mismatch: column type {:?} is not compatible with referenced \
             column type {:?}",
            fk_col_type, parent_col_type,
        )));
    }
    Ok((0, ref_col_idx as u64, parent_col_type))
}

#[allow(clippy::too_many_arguments)]
fn resolve_fk_target(
    client:           &mut GnitzClient,
    schema_name:      &str,
    foreign_table:    &sqlparser::ast::ObjectName,
    referred_columns: &[sqlparser::ast::Ident],
    fk_col_type:      TypeCode,
    current_table_name: &str,
    current_cols:       &[ColumnDef],
    current_pk_cols:    &[u32],
) -> Result<(u64, u64, TypeCode), GnitzSqlError> {
    let ref_table = extract_name(foreign_table, "REFERENCES")?;

    // Self-referencing FK: the table being created is not yet in the catalog,
    // so resolve the referenced column against the in-flight column list.
    if ref_table.eq_ignore_ascii_case(current_table_name) {
        return resolve_fk_target_inline(
            current_cols, current_pk_cols, &ref_table, referred_columns, fk_col_type,
        );
    }

    let (ref_tid, ref_schema) = client.resolve_table_id(schema_name, &ref_table)
        .map_err(|e| GnitzSqlError::Bind(format!("FK target '{}': {}", ref_table, e)))?;

    if referred_columns.len() > 1 {
        return Err(GnitzSqlError::Unsupported(
            "multi-column FOREIGN KEY references are not supported".into()
        ));
    }

    // Referenced parent column. An omitted column list defaults to the PK and
    // is only well-defined for a single-column PK.
    let ref_col_idx: usize = if let Some(ident) = referred_columns.first() {
        ref_schema.columns.iter()
            .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
            .ok_or_else(|| GnitzSqlError::Bind(format!(
                "FK references column '{}' not found in table '{}'",
                ident.value, ref_table,
            )))?
    } else if ref_schema.pk_count() == 1 {
        ref_schema.pk_index_single()
    } else {
        return Err(GnitzSqlError::Bind(format!(
            "FK against compound-PK table '{}' must name the referenced column",
            ref_table,
        )));
    };

    // Legal target iff the referenced column is the parent's lone PK, or it
    // carries an active UNIQUE index. The `&&` short-circuits, so
    // `pk_index_single()` is never reached for a compound parent.
    let is_lone_pk = ref_schema.pk_count() == 1
        && ref_col_idx == ref_schema.pk_index_single();
    if !is_lone_pk {
        match client.find_index_for_column(ref_tid, ref_col_idx)
            .map_err(GnitzSqlError::Exec)?
        {
            Some((_, true)) => {}
            _ => return Err(GnitzSqlError::Unsupported(format!(
                "FK against table '{}' must reference the primary key or a column \
                 with a UNIQUE index; column '{}' has neither",
                ref_table, ref_schema.columns[ref_col_idx].name,
            ))),
        }
    }

    // Child column widens to the referenced parent column's type.
    let parent_col_type = ref_schema.columns[ref_col_idx].type_code;
    let is_compat = if is_integer_type(fk_col_type) && is_integer_type(parent_col_type) {
        true
    } else {
        fk_col_type == parent_col_type
    };
    if !is_compat {
        return Err(GnitzSqlError::Bind(format!(
            "FK type mismatch: column type {:?} is not compatible with referenced \
             column type {:?}",
            fk_col_type, parent_col_type,
        )));
    }

    Ok((ref_tid, ref_col_idx as u64, parent_col_type))
}

fn execute_create_table(
    client:      &mut GnitzClient,
    schema_name: &str,
    create:      &sqlparser::ast::CreateTable,
    _binder:     &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&create.name, "CREATE TABLE")?;

    let sql_cols = &create.columns;

    // Reject duplicate column names up front: the PK binder's name->index
    // lookup silently binds to the first match, so a duplicate would produce a
    // schema with two identically-named columns that bind ambiguously later.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for col in sql_cols.iter() {
        if !seen.insert(col.name.value.to_lowercase()) {
            return Err(GnitzSqlError::Plan(format!(
                "duplicate column name '{}' in table definition", col.name.value
            )));
        }
    }

    // Phase 1 — build column defs (name, type, nullability only).
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(sql_cols.len());
    for col in sql_cols.iter() {
        cols.push(ColumnDef {
            name:        col.name.value.clone(),
            type_code:   sql_type_to_typecode(&col.data_type)?,
            is_nullable: !col.options.iter().any(|o| matches!(o.option, ColumnOption::NotNull)),
            fk_table_id: 0,
            fk_col_idx:  0,
        });
    }

    // Phase 2 — gather PK column indices.
    //   * One table-level `PRIMARY KEY (a, b, ...)` clause OR one inline
    //     `col PRIMARY KEY`; mixing the two is rejected.
    //   * Unknown column names raise a Bind error.
    //   * Duplicate columns inside a `PRIMARY KEY (...)` list are rejected
    //     before the engine catalog reports the same.
    let mut pk_indices: Vec<u32> = Vec::new();
    let mut pk_decl_seen = false;

    // Table-level PRIMARY KEY (...). Done before the inline pass so an
    // unknown column name produces a Bind error rather than being eclipsed
    // by a duplicate-PK error from a separate inline `PRIMARY KEY` clause.
    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey { columns: pk_cols, .. } = constraint {
            if pk_decl_seen {
                return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
            }
            pk_decl_seen = true;
            for col_ident in pk_cols {
                let idx = sql_cols.iter()
                    .position(|c| c.name.value.eq_ignore_ascii_case(&col_ident.value))
                    .ok_or_else(|| GnitzSqlError::Bind(format!(
                        "PRIMARY KEY column '{}' not found", col_ident.value
                    )))?;
                if pk_indices.contains(&(idx as u32)) {
                    return Err(GnitzSqlError::Plan(format!(
                        "Duplicate column '{}' in PRIMARY KEY", col_ident.value
                    )));
                }
                pk_indices.push(idx as u32);
            }
        }
    }

    // Phase 3 — inline column PRIMARY KEY + FOREIGN KEY.
    for (i, col) in sql_cols.iter().enumerate() {
        for opt in &col.options {
            match &opt.option {
                ColumnOption::Unique { is_primary: true, .. } => {
                    if pk_decl_seen {
                        return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
                    }
                    pk_decl_seen = true;
                    pk_indices.push(i as u32);
                }
                ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                    let (tid, idx, parent_pk_type) = resolve_fk_target(
                        client, schema_name, foreign_table, referred_columns, cols[i].type_code,
                        &table_name, &cols, &pk_indices,
                    )?;
                    cols[i].fk_table_id = tid;
                    cols[i].fk_col_idx  = idx;
                    cols[i].type_code   = parent_pk_type;
                }
                _ => {}
            }
        }
    }

    // Phase 4 — table-level FOREIGN KEY constraints.
    for constraint in &create.constraints {
        if let TableConstraint::ForeignKey { columns, foreign_table, referred_columns, .. } = constraint {
            if columns.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "multi-column FOREIGN KEY constraints are not supported".into()
                ));
            }
            let local_col_name = &columns[0].value;
            let col_idx = cols.iter()
                .position(|c| c.name.eq_ignore_ascii_case(local_col_name))
                .ok_or_else(|| GnitzSqlError::Bind(format!(
                    "FOREIGN KEY column '{}' not found in table definition", local_col_name
                )))?;
            let (tid, idx, parent_pk_type) = resolve_fk_target(
                client, schema_name, foreign_table, referred_columns, cols[col_idx].type_code,
                &table_name, &cols, &pk_indices,
            )?;
            cols[col_idx].fk_table_id = tid;
            cols[col_idx].fk_col_idx  = idx;
            cols[col_idx].type_code   = parent_pk_type;
        }
    }

    // Admission rule — every base table must satisfy these conditions.
    // The order here matches the order of error messages a user would
    // expect to see: missing PK → count cap → type allow-list → stride.
    if pk_indices.is_empty() {
        return Err(GnitzSqlError::Plan(
            "CREATE TABLE requires at least one PRIMARY KEY column".into()
        ));
    }
    if pk_indices.len() > 4 {
        return Err(GnitzSqlError::Unsupported(
            "PRIMARY KEY supports at most 4 columns".into()
        ));
    }
    for &i in &pk_indices {
        let tc = cols[i as usize].type_code;
        // Pre-check before the DDL reaches the engine (which re-validates via
        // `validate_pk_cols`); naming the offending column here gives a clearer
        // error. Same eligibility rule, shared via `TypeCode::is_pk_eligible`.
        if !tc.is_pk_eligible() {
            return Err(GnitzSqlError::Unsupported(format!(
                "PRIMARY KEY column '{}' of type {:?} is not supported \
                 (PK must be a fixed-width integer, U128, or UUID column; \
                 String, Blob, and float columns cannot be PK)",
                cols[i as usize].name, tc,
            )));
        }
    }
    // The PK region must fit MAX_PK_BYTES. Strides ≤ 16 widen to a `u128` fast
    // key; wider compound PKs (stride > 16, e.g. three `U64`s = 24) route
    // through the byte-path cursor/merge accessors. The 4-column cap above
    // bounds a valid PK at 64 bytes; MAX_PK_BYTES is the ceiling. The engine
    // re-validates the same bound in `validate_pk_cols`.
    let pk_stride: usize = pk_indices.iter()
        .map(|&i| cols[i as usize].type_code.wire_stride())
        .sum();
    if pk_stride == 0 || pk_stride > gnitz_core::MAX_PK_BYTES {
        return Err(GnitzSqlError::Unsupported(format!(
            "PRIMARY KEY total stride must be 1..={} bytes, got {pk_stride}",
            gnitz_core::MAX_PK_BYTES
        )));
    }

    // PK columns keep their declared type. The null bitmap excludes the PK
    // region, so a nullable PK has no place to carry the null — enforce
    // non-nullable here regardless of type.
    for &i in &pk_indices {
        cols[i as usize].is_nullable = false;
    }

    let tid = client.create_table(schema_name, &table_name, &cols, &pk_indices, true)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::TableCreated { table_id: tid })
}

fn execute_drop(
    client:      &mut GnitzClient,
    schema_name: &str,
    object_type: &ObjectType,
    names:       &[sqlparser::ast::ObjectName],
) -> Result<SqlResult, GnitzSqlError> {
    for obj_name in names {
        let name = extract_name(obj_name, "DROP")?;

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
    client:      &mut GnitzClient,
    schema_name: &str,
    view_name_obj: &sqlparser::ast::ObjectName,
    query:       &Query,
    stmt:        &Statement,
    binder:      &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = extract_name(view_name_obj, "CREATE VIEW")?;

    if query.order_by.is_some() {
        return Err(GnitzSqlError::Unsupported("ORDER BY not supported".to_string()));
    }
    // An incremental circuit has no meaningful LIMIT/OFFSET semantics; a silently
    // ignored LIMIT would stream the full table.
    if query.limit_clause.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "LIMIT and OFFSET are not supported in VIEW definitions".into(),
        ));
    }

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
            let cte_table_name = extract_table_factor_name(
                &cte_select.from[0].relation,
                &format!("CTE '{}'", cte_name),
            )?;
            // Resolve the CTE's source table and cache the CTE name as an alias.
            let (cte_tid, cte_schema) = binder.resolve(client, &cte_table_name)?;
            // A CTE is inlined only as a plain column pass-through of its source
            // table. Anything that changes the row set or shape (WHERE, GROUP BY,
            // HAVING, DISTINCT, ORDER BY, LIMIT, or a non-identity projection)
            // would be silently discarded by the alias mechanism and return wrong
            // rows, so reject it explicitly. (Compiling an arbitrary CTE body as a
            // sub-plan is a separate, unimplemented feature.)
            let proj_is_identity = cte_select.projection.iter()
                    .all(|p| matches!(p, SelectItem::Wildcard(_)))
                || (cte_select.projection.len() == cte_schema.columns.len()
                    && cte_select.projection.iter().enumerate().all(|(i, item)| {
                        matches!(item, SelectItem::UnnamedExpr(Expr::Identifier(id))
                            if cte_schema.columns[i].name.eq_ignore_ascii_case(&id.value))
                    }));
            let has_group_by = matches!(&cte_select.group_by,
                GroupByExpr::Expressions(e, _) if !e.is_empty());
            if cte_select.selection.is_some()
                || cte_select.having.is_some()
                || cte_select.distinct.is_some()
                || has_group_by
                || cte.query.order_by.is_some()
                || cte.query.limit_clause.is_some()
                || !proj_is_identity
            {
                return Err(GnitzSqlError::Unsupported(format!(
                    "CTE '{}': only a plain column pass-through of a single table is \
                     supported; WHERE, a subset/reordered/computed projection, GROUP BY, \
                     HAVING, DISTINCT, ORDER BY and LIMIT inside a CTE are not yet supported",
                    cte_name
                )));
            }
            // Apply CTE column aliases (`WITH cte(a, b) AS ...`): rename the
            // resolved columns so outer references to the aliases bind.
            let cte_schema = if !cte.alias.columns.is_empty() {
                if cte.alias.columns.len() != cte_schema.columns.len() {
                    return Err(GnitzSqlError::Plan(format!(
                        "CTE '{}' defines {} column aliases but query returns {} columns",
                        cte_name, cte.alias.columns.len(), cte_schema.columns.len()
                    )));
                }
                let mut s = (*cte_schema).clone();
                for (col, alias) in s.columns.iter_mut().zip(&cte.alias.columns) {
                    col.name = alias.name.value.clone();
                }
                std::rc::Rc::new(s)
            } else {
                cte_schema
            };
            binder.cache_alias(cte_name, (cte_tid, cte_schema));
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

    let table_name = extract_table_factor_name(&select.from[0].relation, "CREATE VIEW")?;

    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;
    reject_compound_pk_view_source(&source_schema)?;

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

    cb.sink(out_node);
    let circuit = cb.build();

    client.create_view_with_circuit(schema_name, &view_name, &sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

fn execute_create_index(
    client:      &mut GnitzClient,
    schema_name: &str,
    ci:          &sqlparser::ast::CreateIndex,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&ci.table_name, "CREATE INDEX")?;

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
    Computed { bound_expr: BoundExpr, _out_type: TypeCode },
}

fn is_pk_item(item: &ProjectionItem, schema: &Schema) -> bool {
    matches!(item, ProjectionItem::PassThrough { src_col } if schema.is_pk_col(*src_col))
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
                // A direct column reference with an alias is a PassThrough with a
                // renamed output column, not a computation — otherwise an aliased
                // PK column would trip the "PK cannot be computed" guard below.
                if let BoundExpr::ColRef(ci) = bound {
                    items.push(ProjectionItem::PassThrough { src_col: ci });
                    let mut col = source_schema.columns[ci].clone();
                    col.name = alias.value.clone();
                    out_cols.push(col);
                } else {
                    let out_type = bound.infer_type(source_schema);
                    items.push(ProjectionItem::Computed { bound_expr: bound, _out_type: out_type });
                    out_cols.push(ColumnDef {
                        name: alias.value.clone(),
                        type_code: out_type,
                        is_nullable: true, fk_table_id: 0, fk_col_idx: 0,
                    });
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                // Try as column reference first (already handled above for Identifier),
                // otherwise treat as computed expression
                let bound = binder.bind_expr(expr, source_schema)?;
                let out_type = bound.infer_type(source_schema);
                let col_name = format!("_expr{}", idx);
                items.push(ProjectionItem::Computed { bound_expr: bound, _out_type: out_type });
                out_cols.push(ColumnDef {
                    name: col_name,
                    type_code: out_type,
                    is_nullable: true, fk_table_id: 0, fk_col_idx: 0,
                });
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "unsupported SELECT item in CREATE VIEW projection".to_string()
            )),
        }
    }

    // Ensure PK is present (as PassThrough) and first
    let pk = source_schema.pk_index_single();
    let pk_pos = items.iter().position(|i| matches!(i, ProjectionItem::PassThrough { src_col } if *src_col == pk));

    match pk_pos {
        Some(0) => { /* already first, good */ }
        Some(pos) => {
            // Move PK to front. A swap would relocate the old column-0 to `pos`,
            // scrambling the remaining order; remove+insert shifts the rest left.
            let pk_item = items.remove(pos);
            items.insert(0, pk_item);
            let pk_col = out_cols.remove(pos);
            out_cols.insert(0, pk_col);
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
    client:      &mut GnitzClient,
    schema_name: &str,
    view_name:   &str,
    sql_text:    &str,
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
    _query:      &Query,
) -> Result<SqlResult, GnitzSqlError> {
    // Extract left table
    let left_name = extract_table_factor_name(&select.from[0].relation, "CREATE VIEW JOIN")?;
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
    let (left_tid, left_schema) = binder.resolve(client, &left_name)?;
    reject_compound_pk_view_source(&left_schema)?;
    let (right_tid, right_schema) = binder.resolve(client, &right_name)?;
    reject_compound_pk_view_source(&right_schema)?;

    // The 2-term DBSP join formula assumes the left and right input deltas are
    // never simultaneously active. A self-join feeds the same delta to both
    // sides in one epoch, dropping the bilinear ΔT⋈ΔT cross-term — silent data
    // loss whenever same-key rows arrive in one batch.
    if left_tid == right_tid {
        return Err(GnitzSqlError::Unsupported(
            "self-join (joining a table with itself) is not supported".into()
        ));
    }

    // The merged join output is [_join_pk, left cols..., right cols...]. Reject
    // before the server's schema build hits its hard column-count assertion.
    let combined_cols = 1 + left_schema.columns.len() + right_schema.columns.len();
    if combined_cols > gnitz_core::MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN view output has {} columns, exceeding the {}-column limit",
            combined_cols, gnitz_core::MAX_COLUMNS,
        )));
    }

    // Build alias map for qualified column resolution
    let mut alias_map: HashMap<String, (u64, Schema, usize)> = HashMap::new();
    alias_map.insert(left_alias.to_lowercase(), (left_tid, (*left_schema).clone(), 0));
    alias_map.insert(right_alias.to_lowercase(), (right_tid, (*right_schema).clone(), left_schema.columns.len()));

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
    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    // Right-side type codes for null-extend (used by LEFT-join null fills).
    let right_col_tcs: Vec<u64> =
        right_schema.columns.iter().map(|c| c.type_code as u64).collect();

    // A NULL equi-join key must match nothing (SQL 3VL: NULL = anything, including
    // NULL = NULL, is unknown). map_reindex would promote a NULL integer key to
    // synthetic PK 0 and a NULL string to the empty-content hash 0, colliding with
    // a real 0/"" key and with every other NULL. Gate NULL keys out of the match.
    // A NOT NULL key leaves its side untouched (no filter node) — byte-identical to
    // the original plan, so the common case has zero overhead.
    let left_key_nullable  = left_schema.columns[left_join_col].is_nullable;
    let right_key_nullable = right_schema.columns[right_join_col].is_nullable;

    let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
    let input_a = cb.input_delta_tagged(left_tid);
    let input_b = cb.input_delta_tagged(right_tid);

    // Inner (right) side: NULL keys can never match — drop them before reindex.
    let input_b = if right_key_nullable {
        cb.filter(input_b, Some(null_filter_prog(right_join_col, &right_schema, false)?))
    } else {
        input_b
    };
    let reindex_b = cb.map_reindex(input_b, right_join_col, right_reindex_prog);

    // Preserved (left) side.
    //   INNER join: a NULL left key matches nothing — drop it.
    //   LEFT  join: a NULL left key must still be emitted with NULL right columns
    //               but must bypass the match (else it collides with a right 0/""
    //               key and pollutes trace_a, corrupting join_ba and the ΔB
    //               null-fill correction). Split it out, reindex it to a U128 PK
    //               (so it is layout-compatible with the join output — a bare Filter
    //               would keep the left table's native PK stride and corrupt the
    //               downstream Union, which merges by raw PK bytes into one stride),
    //               null-extend it, and union it into the unmatched-left stream.
    //               Its synthetic PK is 0, which is harmless: the left PK is among
    //               the copied payload columns and compare_rows treats NULL ≠ 0, so
    //               it never merges with a real-0-key row.
    let (input_a_match, left_null_filled) = if !left_key_nullable {
        (input_a, None)
    } else {
        let left_not_null = cb.filter(
            input_a, Some(null_filter_prog(left_join_col, &left_schema, false)?));
        if !is_left_join {
            (left_not_null, None)
        } else {
            let left_null = cb.filter(
                input_a, Some(null_filter_prog(left_join_col, &left_schema, true)?));
            let left_null_ri = cb.map_reindex(
                left_null, left_join_col, build_reindex_program(&left_schema));
            (left_not_null, Some(cb.null_extend(left_null_ri, &right_col_tcs)))
        }
    };
    let reindex_a = cb.map_reindex(input_a_match, left_join_col, left_reindex_prog);

    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))

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
        // Decomposed LEFT OUTER JOIN: inner ∪ (anti_join × null_right) ∪ null-key bypass.
        // This handles both ΔA and ΔB correctly.

        // Key-only B: strip payload, keep only join key PK for distinct tracking
        let key_only_b = cb.map_key_only(reindex_b);
        let distinct_b = cb.distinct(key_only_b);
        let trace_db = cb.integrate_trace(distinct_b);

        // ΔA null-fill path: left rows whose join key has no match in I(distinct(B))
        let antijoin_a = cb.anti_join_with_trace_node(reindex_a, trace_db);
        let null_filled_a = cb.null_extend(antijoin_a, &right_col_tcs);

        // ΔB correction path: when B key appears/disappears, adjust null-fills
        // join_dt(distinct_b, trace_a) gives (key, A_payload) for affected left rows
        // distinct_b emits +1 when key appears → negate → -1 → retract null-fill
        // distinct_b emits -1 when key disappears → negate → +1 → emit null-fill
        let correction_raw = cb.join_with_trace_node(distinct_b, trace_a);
        let correction = cb.negate(correction_raw);
        let null_filled_correction = cb.null_extend(correction, &right_col_tcs);

        let mut all_null_fills = cb.union(null_filled_a, null_filled_correction);
        // NULL-key left rows bypass the match entirely (SQL 3VL) but are still
        // emitted once as (left payload, NULL right) via this reindexed branch.
        if let Some(f) = left_null_filled {
            all_null_fills = cb.union(all_null_fills, f);
        }
        cb.union(inner_merged, all_null_fills)
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
        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
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
                SelectItem::ExprWithAlias { expr, alias } => {
                    let idx = match expr {
                        Expr::Identifier(ident) =>
                            resolve_unqualified_column(&ident.value, &alias_map)?,
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 =>
                            resolve_qualified_column(&parts[0].value, &parts[1].value, &alias_map)?,
                        _ => return Err(GnitzSqlError::Unsupported(
                            "JOIN view: only column references supported in AS clause".to_string()
                        )),
                    };
                    let mut col = out_cols[1 + idx].clone();
                    col.name = alias.value.clone();
                    cols.push(col);
                    proj.push(idx + 1);
                }
                SelectItem::Wildcard(_) => {
                    for (i, col) in out_cols.iter().enumerate().skip(1) {
                        cols.push(col.clone());
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
    cb.sink(sink_input);
    let circuit = cb.build();

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Single-column null predicate for a Filter, reusing the same
/// bound-expr → ExprProgram path the planner uses for WHERE clauses (so the
/// column index is correctly mapped to its payload byte). `want_null` selects
/// IS NULL vs IS NOT NULL. Used to gate NULL equi-join keys out of the match.
fn null_filter_prog(
    col_idx:   usize,
    schema:    &Schema,
    want_null: bool,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    let bound = if want_null {
        BoundExpr::IsNull(col_idx)
    } else {
        BoundExpr::IsNotNull(col_idx)
    };
    let mut eb = ExprBuilder::new();
    let (r, _) = compile_bound_expr(&bound, schema, &mut eb)?;
    Ok(eb.build(r))
}

/// Build a reindex ExprProgram that copies all columns as payload.
fn build_reindex_program(schema: &Schema) -> gnitz_core::ExprProgram {
    let mut eb = ExprBuilder::new();
    for (ci, col) in schema.columns.iter().enumerate() {
        let tc = col.type_code as u32;
        eb.copy_col(tc, ci as u32, ci as u32);
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
    is_avg: bool,
    output_name: String,
    output_type: TypeCode,
    agg_func: AggFunc,
    arg_col: Option<usize>,
}

/// Mirror the engine's `agg_output_type`: SUM/MIN/MAX over a float column
/// resolve to F64, COUNT/COUNT_NON_NULL to I64. A planner/compiler mismatch
/// silently scrambles the view's output column positions.
fn agg_result_type(func: AggFunc, src_col: Option<usize>, schema: &Schema) -> TypeCode {
    match func {
        AggFunc::Count | AggFunc::CountNonNull => TypeCode::I64,
        AggFunc::Avg => TypeCode::F64,
        AggFunc::Sum | AggFunc::Min | AggFunc::Max => {
            match src_col {
                Some(c) if schema.columns[c].type_code.is_float() => TypeCode::F64,
                _ => TypeCode::I64,
            }
        }
    }
}

/// What each SELECT item represents in a GROUP BY query.
enum GroupBySelectItem {
    GroupCol { src_col: usize, name: String },
    Aggregate { agg_idx: usize },
}

fn execute_create_group_by_view(
    client:      &mut GnitzClient,
    schema_name: &str,
    view_name:   &str,
    sql_text:    &str,
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // 1. Resolve source table
    let table_name = extract_table_factor_name(&select.from[0].relation, "GROUP BY")?;
    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;
    reject_compound_pk_view_source(&source_schema)?;

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
                // SUM/MIN/MAX/COUNT_NON_NULL/AVG require an argument column. The
                // binder normally rejects no-argument calls, but guard against a
                // None reaching the unwraps below (panic → dead server thread).
                if matches!(func, AggFunc::Sum | AggFunc::Min | AggFunc::Max
                        | AggFunc::CountNonNull | AggFunc::Avg)
                    && src_col.is_none()
                {
                    return Err(GnitzSqlError::Plan(format!(
                        "{:?} requires an argument column", func
                    )));
                }
                // Reject aggregates whose source column type is meaningless for
                // the function: numeric aggregates need a numeric column, and
                // MIN/MAX need an orderable column (the engine's decode_signed
                // marks U128/UUID unreachable and reads STRING/BLOB as garbage).
                if let Some(c) = src_col {
                    let tc = source_schema.columns[c].type_code;
                    let is_numeric = is_integer_type(tc) || tc.is_float();
                    match func {
                        AggFunc::Sum | AggFunc::Avg | AggFunc::CountNonNull => {
                            // SUM/AVG accumulate into a 64-bit slot; U128/UUID
                            // overflow it and the engine's decode_signed marks
                            // them unreachable. CountNonNull only counts
                            // presence, so it accepts any type.
                            if matches!(func, AggFunc::Sum | AggFunc::Avg)
                                && (!is_numeric || matches!(tc, TypeCode::U128 | TypeCode::UUID))
                            {
                                return Err(GnitzSqlError::Bind(format!(
                                    "{:?} is not supported on column type {:?} ('{}')",
                                    func, tc, source_schema.columns[c].name,
                                )));
                            }
                        }
                        AggFunc::Min | AggFunc::Max => {
                            if matches!(tc, TypeCode::String | TypeCode::Blob
                                | TypeCode::U128 | TypeCode::UUID) {
                                return Err(GnitzSqlError::Bind(format!(
                                    "{:?} is not supported on column type {:?} ('{}')",
                                    func, tc, source_schema.columns[c].name,
                                )));
                            }
                        }
                        AggFunc::Count => {}
                    }
                }

                let out_type = agg_result_type(*func, src_col, &source_schema);
                let agg_idx = agg_mappings.len();
                let start = agg_specs.len();
                let is_avg = push_agg_specs(*func, src_col, &mut agg_specs);
                let out_name = alias.unwrap_or_else(|| {
                    let prefix = match func {
                        AggFunc::Count | AggFunc::CountNonNull => "_count",
                        AggFunc::Sum => "_sum",
                        AggFunc::Min => "_min",
                        AggFunc::Max => "_max",
                        AggFunc::Avg => "_avg",
                    };
                    format!("{}{}", prefix, idx)
                });
                agg_mappings.push(AggMapping {
                    specs_start: start, is_avg,
                    output_name: out_name, output_type: out_type,
                    agg_func: *func, arg_col: src_col,
                });
                select_items.push(GroupBySelectItem::Aggregate { agg_idx });
            }
            _ => return Err(GnitzSqlError::Plan(
                "GROUP BY SELECT: only column refs and aggregates supported".to_string()
            )),
        }
    }

    // 3b. Materialise aggregates referenced only by HAVING. HAVING is evaluated
    //     over the grouped relation, so every aggregate it references needs an
    //     agg_spec and a reduce-output column even when the SELECT list omits
    //     it. Done before reduce_schema is built so the new columns are included.
    if let Some(having_expr) = &select.having {
        collect_having_aggs(
            having_expr, &source_schema, &mut agg_specs, &mut agg_mappings,
        )?;
    }

    // 4. Determine reduce output schema layout.
    //    A nullable group column cannot become the natural PK — the PK region
    //    has no null bitmap, so we fall through to the synthetic _group_pk
    //    (U128, non-nullable) path which is always safe.
    //
    //    The two natural-PK cases must mirror the compiler's
    //    `build_reduce_output_schema` decision (compiler.rs:695-697):
    //      (a) `group_set_eq_pk`: group cols are a permutation of the source
    //          PK cols, regardless of PK type — even native I64.
    //      (b) `is_single_col_natural_pk`: a single non-nullable U64/U128/UUID
    //          group col. Narrow unsigned/signed group cols still take the
    //          synthetic path because the engine helper rejects them.
    //    A divergence between planner and compiler here scrambles the view's
    //    output column positions silently.
    let group_set_eq_pk = source_schema.group_cols_eq_pk(&group_col_indices);
    let single_col_natural_pk = source_schema.is_single_col_natural_pk(&group_col_indices);
    let use_natural_pk = group_set_eq_pk || single_col_natural_pk;

    // Build the reduce output schema (mirrors server's _build_reduce_output_schema).
    let mut reduce_schema_cols: Vec<ColumnDef> = Vec::new();
    let mut reduce_pk_cols: Vec<usize> = Vec::new();
    if group_set_eq_pk {
        for &pi in &source_schema.pk_cols {
            reduce_pk_cols.push(reduce_schema_cols.len());
            reduce_schema_cols.push(source_schema.columns[pi].clone());
        }
    } else if single_col_natural_pk {
        reduce_pk_cols.push(0);
        reduce_schema_cols.push(source_schema.columns[group_col_indices[0]].clone());
    } else {
        reduce_pk_cols.push(0);
        reduce_schema_cols.push(ColumnDef {
            name: "_group_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
        for &gi in &group_col_indices {
            reduce_schema_cols.push(source_schema.columns[gi].clone());
        }
    }
    // First aggregate column lives immediately after the PK + (synthetic)
    // group cols — the synthetic path adds the group cols as payload, the
    // natural paths don't.
    let agg_col_offset = if use_natural_pk {
        reduce_schema_cols.len()
    } else {
        1 + group_col_indices.len()
    };
    for &(op, col) in &agg_specs {
        // Mirror the engine's agg_output_type so the planner's virtual schema
        // matches the compiler's physical reduce schema (F64 for float SUM/MIN/MAX).
        let type_code = if (op == AGG_SUM || op == AGG_MIN || op == AGG_MAX)
            && source_schema.columns[col].type_code.is_float()
        {
            TypeCode::F64
        } else {
            TypeCode::I64
        };
        reduce_schema_cols.push(ColumnDef {
            name: "_agg".into(), type_code, is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
    }
    let reduce_schema = Schema { columns: reduce_schema_cols, pk_cols: reduce_pk_cols };

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
                    name: name.clone(),
                    type_code: tc,
                    // Natural-PK path: source col is non-nullable (asserted by use_natural_pk).
                    // Synthetic-PK path: propagate source nullability — nothing forces NOT NULL.
                    is_nullable: source_schema.columns[*src_col].is_nullable,
                    fk_table_id: 0,
                    fk_col_idx:  0,
                });
                payload_idx += 1;
            }
            GroupBySelectItem::Aggregate { agg_idx } => {
                let m = &agg_mappings[*agg_idx];
                if m.is_avg {
                    // AVG = SUM / COUNT: two agg_specs were pushed (SUM, COUNT)
                    let sum_col = (agg_col_offset + m.specs_start) as u32;
                    let cnt_col = (agg_col_offset + m.specs_start + 1) as u32;
                    // For a float source, the SUM accumulator stores IEEE-754
                    // bits; loading those as an int and casting numerically would
                    // produce a wildly wrong value. Load them directly as float.
                    let is_float_src = m.arg_col
                        .map(|c| source_schema.columns[c].type_code.is_float())
                        .unwrap_or(false);
                    let sum_f = if is_float_src {
                        post_map_eb.load_col_float(sum_col as usize)
                    } else {
                        let sum_reg = post_map_eb.load_col_int(sum_col as usize);
                        post_map_eb.int_to_float(sum_reg)
                    };
                    let cnt_reg = post_map_eb.load_col_int(cnt_col as usize);
                    let cnt_f = post_map_eb.int_to_float(cnt_reg);
                    let avg_reg = post_map_eb.float_div(sum_f, cnt_f);
                    post_map_eb.emit_col(avg_reg, payload_idx);
                    out_cols.push(ColumnDef {
                        // AVG of an empty / all-NULL group is NULL (COUNT_NON_NULL=0
                        // → float_div by zero marks the result NULL), so the column
                        // must be nullable to match what the circuit can emit.
                        name: m.output_name.clone(), type_code: TypeCode::F64,
                        is_nullable: true, fk_table_id: 0, fk_col_idx: 0,
                    });
                    payload_idx += 1;
                } else {
                    // Direct aggregate: single spec
                    let agg_col = (agg_col_offset + m.specs_start) as u32;
                    let tc = reduce_schema.columns[agg_col as usize].type_code;
                    post_map_eb.copy_col(tc as u32, agg_col, payload_idx);
                    out_cols.push(ColumnDef {
                        name: m.output_name.clone(), type_code: m.output_type,
                        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
                    });
                    payload_idx += 1;
                }
            }
        }
    }

    // The result_reg for a MAP program is typically 0 (true = pass through)
    let post_map_prog = post_map_eb.build(0);

    // 7. Optional HAVING filter, applied to the grouped relation *before* the
    //    SELECT projection — the relational order standard SQL specifies. Filter
    //    and map are both row-wise linear operators and commute, so this is
    //    semantically and incrementally sound. Binding against reduce_schema lets
    //    HAVING reference group columns by their source name (unaffected by
    //    SELECT aliases or omission) and aggregates that are not projected.
    let filtered_reduced = if let Some(having_expr) = &select.having {
        let bound = bind_having_expr(having_expr, &HavingCtx {
            source_schema:     &source_schema,
            group_col_indices: &group_col_indices,
            use_natural_pk,
            agg_mappings:      &agg_mappings,
            agg_col_offset,
        })?;
        let mut heb = ExprBuilder::new();
        let (result_reg, _) = compile_bound_expr(&bound, &reduce_schema, &mut heb)?;
        let prog = heb.build(result_reg);
        cb.filter(reduced, Some(prog))
    } else {
        reduced
    };

    let mapped = cb.map_expr(filtered_reduced, post_map_prog);

    // 8. Sink
    cb.sink(mapped);
    let circuit = cb.build();

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Resolve a HAVING function call to its aggregate function selector + argument
/// column, shared by collection and binding so both agree on what an aggregate
/// reference means.
fn having_agg_func(
    func: &sqlparser::ast::Function,
    source_schema: &Schema,
) -> Result<(AggFunc, Option<usize>), GnitzSqlError> {
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
    Ok((agg_func, arg_col))
}

/// True iff `m` is the reduce mapping for the aggregate `(agg_func, arg_col)`.
/// COUNT(*) ignores arg_col (it has none); every other form matches the source
/// column too, so `MAX(c2)` never binds to `SUM(c1)`.
fn agg_mapping_matches(m: &AggMapping, agg_func: AggFunc, arg_col: Option<usize>) -> bool {
    match agg_func {
        AggFunc::Avg => m.agg_func == AggFunc::Avg && m.is_avg && m.arg_col == arg_col,
        AggFunc::Count => m.agg_func == AggFunc::Count,
        AggFunc::CountNonNull => m.agg_func == AggFunc::CountNonNull && m.arg_col == arg_col,
        AggFunc::Sum => m.agg_func == AggFunc::Sum && m.arg_col == arg_col,
        AggFunc::Min => m.agg_func == AggFunc::Min && m.arg_col == arg_col,
        AggFunc::Max => m.agg_func == AggFunc::Max && m.arg_col == arg_col,
    }
}

/// Push the engine `agg_specs` for one aggregate and return whether it is an
/// AVG (which materialises two specs — SUM then COUNT_NON_NULL). The single
/// source of truth for the spec layout, shared by the SELECT projection and the
/// HAVING-only materialisation so the two stay in lockstep — notably the
/// AVG-emits-two-specs invariant, on which the reduce-output column positions
/// and `AggMapping::specs_start` both depend.
fn push_agg_specs(
    agg_func: AggFunc,
    arg_col: Option<usize>,
    agg_specs: &mut Vec<(u64, usize)>,
) -> bool {
    match agg_func {
        AggFunc::Count => { agg_specs.push((AGG_COUNT, 0)); false }
        AggFunc::CountNonNull => { agg_specs.push((AGG_COUNT_NON_NULL, arg_col.unwrap())); false }
        AggFunc::Sum => { agg_specs.push((AGG_SUM, arg_col.unwrap())); false }
        AggFunc::Min => { agg_specs.push((AGG_MIN, arg_col.unwrap())); false }
        AggFunc::Max => { agg_specs.push((AGG_MAX, arg_col.unwrap())); false }
        AggFunc::Avg => {
            let c = arg_col.unwrap();
            agg_specs.push((AGG_SUM, c));
            agg_specs.push((AGG_COUNT_NON_NULL, c));
            true
        }
    }
}

/// Push the agg_specs + AggMapping for one HAVING-only aggregate (one absent
/// from the SELECT list). Reuses `push_agg_specs` so the reduce-output column
/// positions line up with the SELECT projection. The mapping is found later by
/// `agg_mapping_matches`.
fn append_having_agg(
    agg_func: AggFunc,
    arg_col: Option<usize>,
    source_schema: &Schema,
    agg_specs: &mut Vec<(u64, usize)>,
    agg_mappings: &mut Vec<AggMapping>,
) {
    let out_type = agg_result_type(agg_func, arg_col, source_schema);
    let agg_idx = agg_mappings.len();
    let start = agg_specs.len();
    let is_avg = push_agg_specs(agg_func, arg_col, agg_specs);
    agg_mappings.push(AggMapping {
        specs_start: start, is_avg,
        output_name: format!("_having_agg{}", agg_idx),
        output_type: out_type, agg_func, arg_col,
    });
}

/// Recursively collect aggregate calls referenced in a HAVING expression,
/// appending any not already present in `agg_mappings`.
fn collect_having_aggs(
    expr: &Expr,
    source_schema: &Schema,
    agg_specs: &mut Vec<(u64, usize)>,
    agg_mappings: &mut Vec<AggMapping>,
) -> Result<(), GnitzSqlError> {
    match expr {
        Expr::Function(func) => {
            let (agg_func, arg_col) = having_agg_func(func, source_schema)?;
            if !agg_mappings.iter().any(|m| agg_mapping_matches(m, agg_func, arg_col)) {
                append_having_agg(agg_func, arg_col, source_schema, agg_specs, agg_mappings);
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_having_aggs(left, source_schema, agg_specs, agg_mappings)?;
            collect_having_aggs(right, source_schema, agg_specs, agg_mappings)
        }
        Expr::UnaryOp { expr, .. } => collect_having_aggs(expr, source_schema, agg_specs, agg_mappings),
        Expr::Nested(inner) => collect_having_aggs(inner, source_schema, agg_specs, agg_mappings),
        _ => Ok(()),
    }
}

/// Invariant context for `bind_having_expr`'s recursion: everything needed to
/// resolve a HAVING identifier or aggregate call against the reduce-output
/// (grouped) relation. Bundled so the recursion threads one `&self` instead of
/// re-passing five unchanging arguments at every node. (The reduce schema
/// itself is not needed for binding — the caller compiles the bound expression
/// against it separately.)
struct HavingCtx<'a> {
    source_schema:     &'a Schema,
    group_col_indices: &'a [usize],
    use_natural_pk:    bool,
    agg_mappings:      &'a [AggMapping],
    agg_col_offset:    usize,
}

/// Bind a HAVING expression against the reduce-output (grouped) relation —
/// before the SELECT projection, as standard SQL specifies. Group-column
/// identifiers resolve by their source name (unaffected by SELECT aliases or
/// omission); aggregate calls resolve to their reduce-output column.
fn bind_having_expr(expr: &Expr, ctx: &HavingCtx) -> Result<BoundExpr, GnitzSqlError> {
    match expr {
        Expr::Identifier(ident) => {
            // HAVING references the grouped relation by source column name. Map
            // the name to a GROUP BY column, then to its reduce-output position
            // (natural-PK grouping puts the lone group col at the PK index 0; the
            // synthetic path lays group cols out after the U128 _group_pk).
            let col_name = &ident.value;
            let src = ctx.source_schema.columns.iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| GnitzSqlError::Bind(
                    format!("HAVING: column '{}' not found", col_name)
                ))?;
            let gpos = ctx.group_col_indices.iter().position(|&gi| gi == src)
                .ok_or_else(|| GnitzSqlError::Bind(format!(
                    "HAVING: column '{}' must appear in GROUP BY or an aggregate function",
                    col_name,
                )))?;
            let reduce_col = if ctx.use_natural_pk { 0 } else { 1 + gpos };
            Ok(BoundExpr::ColRef(reduce_col))
        }
        Expr::Function(func) => {
            let (agg_func, arg_col) = having_agg_func(func, ctx.source_schema)?;
            let m = ctx.agg_mappings.iter()
                .find(|m| agg_mapping_matches(m, agg_func, arg_col))
                .ok_or_else(|| GnitzSqlError::Bind(format!(
                    "HAVING: aggregate {:?}({}) could not be resolved", agg_func,
                    arg_col.map_or("*".to_string(), |c| ctx.source_schema.columns[c].name.clone()),
                )))?;
            if m.is_avg {
                // AVG = SUM / COUNT, both materialised as reduce columns. Force
                // float division (an int-source SUM/COUNT would otherwise
                // truncate) by lifting SUM to float via `* 1.0`.
                let sum_col = ctx.agg_col_offset + m.specs_start;
                let cnt_col = ctx.agg_col_offset + m.specs_start + 1;
                let sum_f = BoundExpr::BinOp(
                    Box::new(BoundExpr::ColRef(sum_col)),
                    BinOp::Mul,
                    Box::new(BoundExpr::LitFloat(1.0)),
                );
                Ok(BoundExpr::BinOp(
                    Box::new(sum_f), BinOp::Div, Box::new(BoundExpr::ColRef(cnt_col)),
                ))
            } else {
                Ok(BoundExpr::ColRef(ctx.agg_col_offset + m.specs_start))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = bind_having_expr(left, ctx)?;
            let r = bind_having_expr(right, ctx)?;
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
        Expr::Nested(inner) => bind_having_expr(inner, ctx),
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
    client:      &mut GnitzClient,
    select:      &sqlparser::ast::Select,
    binder:      &mut Binder<'_>,
    cb:          &mut CircuitBuilder,
    branch_id:   u8,
) -> Result<(gnitz_core::NodeId, Vec<ColumnDef>, u64), GnitzSqlError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "set operation: each side must be a simple SELECT from one table".to_string()
        ));
    }
    let table_name = extract_table_factor_name(&select.from[0].relation, "set operation")?;
    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;
    reject_compound_pk_view_source(&source_schema)?;

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

    // Resolve the projection to a set of source column indices (in SELECT
    // order). Unlike `build_projection`, the source PK is NOT force-included:
    // set membership is over exactly the projected columns.
    let (proj_indices, out_cols) = resolve_set_projection(&select.projection, &source_schema, binder)?;

    // Reindex by a hash of the projected columns, so set membership
    // (EXCEPT/INTERSECT/UNION-distinct) is decided by the projected row content,
    // not by the source table's PK: two rows from different tables sharing a PK
    // but differing in payload must not match.
    let reindexed = cb.map_hash_row(filtered, &proj_indices, branch_id);
    // Repartition by the synthetic hash PK (column 0) so that under
    // multiple workers each row lands on the worker that owns its new PK's
    // shard, co-locating matching rows for the downstream semi/anti-join and
    // placing each output row on its owning worker for the sink/scan. The hash
    // is computed in-circuit, so the master cannot pre-shard the source by it;
    // this in-circuit exchange is mandatory. Single-worker mode elides the IPC.
    let sharded = cb.shard(reindexed, &[0]);
    Ok((sharded, out_cols, source_tid))
}

/// Resolve a set-operation side's projection to source column indices plus
/// output column definitions. Supports `SELECT *`, bare column references, and
/// aliased column references; rejects computed expressions (which have no
/// meaningful set identity here) with a clean error rather than silently
/// dropping them.
fn resolve_set_projection(
    projection:    &[SelectItem],
    source_schema: &Schema,
    binder:        &Binder<'_>,
) -> Result<(Vec<usize>, Vec<ColumnDef>), GnitzSqlError> {
    if projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_))) {
        let indices: Vec<usize> = (0..source_schema.columns.len()).collect();
        return Ok((indices, source_schema.columns.clone()));
    }
    let mut indices: Vec<usize> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                for i in 0..source_schema.columns.len() {
                    indices.push(i);
                    out_cols.push(source_schema.columns[i].clone());
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                match binder.bind_expr(expr, source_schema)? {
                    BoundExpr::ColRef(ci) => {
                        indices.push(ci);
                        out_cols.push(source_schema.columns[ci].clone());
                    }
                    _ => return Err(GnitzSqlError::Unsupported(
                        "set operation: computed expressions in a SELECT side are not supported".into()
                    )),
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                match binder.bind_expr(expr, source_schema)? {
                    BoundExpr::ColRef(ci) => {
                        indices.push(ci);
                        let mut col = source_schema.columns[ci].clone();
                        col.name = alias.value.clone();
                        out_cols.push(col);
                    }
                    _ => return Err(GnitzSqlError::Unsupported(
                        "set operation: computed expressions in a SELECT side are not supported".into()
                    )),
                }
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "set operation: unsupported SELECT item".into()
            )),
        }
    }
    Ok((indices, out_cols))
}

#[allow(clippy::too_many_arguments)]
fn execute_create_set_op_view(
    client:          &mut GnitzClient,
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

    // UNION ALL keeps both copies of an identical row (weight +2), so the two
    // branches must hash to distinct PKs — give the right side branch_id = 1.
    // Deduplicating set-ops (UNION/EXCEPT/INTERSECT) intentionally coalesce
    // identical rows, so both sides use branch_id = 0.
    let right_branch_id = matches!(
        (op, set_quantifier),
        (SetOperator::Union, SetQuantifier::All)
    ) as u8;

    let (left_node, left_cols, left_tid) = compile_set_op_side(client, left_select, binder, &mut cb, 0)?;
    let (right_node, right_cols, right_tid) = compile_set_op_side(client, right_select, binder, &mut cb, right_branch_id)?;

    // INTERSECT/EXCEPT inline both branches and semi/anti-join each against the
    // other's delayed trace with no same-epoch cross-correction. When both
    // branches are the same relation, one delta drives both sides in a single
    // pass and the correction term is dropped — silently wrong results. Mirror
    // of the self-join guard. UNION / UNION ALL are linear merges with no
    // correction term, exempt. Two *different* views over the same base table
    // produce two distinct dependency edges (two passes), so the discriminator
    // is source-id equality, not base-table overlap.
    if matches!(op, SetOperator::Intersect | SetOperator::Except) && left_tid == right_tid {
        return Err(GnitzSqlError::Unsupported(
            "INTERSECT/EXCEPT whose two inputs are the same relation is not supported".into()
        ));
    }

    // Schema compatibility check
    if left_cols.len() != right_cols.len() {
        return Err(GnitzSqlError::Plan(format!(
            "set operation: column count mismatch ({} vs {})", left_cols.len(), right_cols.len()
        )));
    }
    // The merge operators read payload bytes using the left schema's column
    // sizes; a STRING column read as an 8-byte integer (or vice versa) produces
    // garbage, so both sides must agree on column types.
    for (i, (l, r)) in left_cols.iter().zip(&right_cols).enumerate() {
        if l.type_code != r.type_code {
            return Err(GnitzSqlError::Plan(format!(
                "set operation: column {} type mismatch ({:?} vs {:?})",
                i, l.type_code, r.type_code,
            )));
        }
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
            // Lift both sides through `distinct`: set membership flips once per
            // projected row regardless of how many source rows carry it, and the
            // integrated traces then hold only weight-1 entries (semi-join tests
            // existence, not weight, so storing raw multiplicities only bloats
            // the trace tables for no effect). Build each `integrate_trace`
            // before its `distinct`'s other consumer so the Kahn schedule
            // (ascending node-id tie-break) keeps the non-destructive reader
            // ahead of any destructive co-consumer; here both readers of each
            // `distinct` are semi-joins (no trace-absent destructive branch).
            let distinct_l = cb.distinct(left_node);
            let distinct_r = cb.distinct(right_node);
            let trace_l = cb.integrate_trace(distinct_l);
            let trace_r = cb.integrate_trace(distinct_r);
            let semi_lr = cb.semi_join_with_trace_node(distinct_l, trace_r);
            let semi_rl = cb.semi_join_with_trace_node(distinct_r, trace_l);
            cb.union(semi_lr, semi_rl)
        }
        (SetOperator::Except, _) => {
            // EXCEPT DISTINCT: difference of the two projected sets. Lifting the
            // left through `distinct` before the anti-join caps a value carried
            // by multiple source rows at weight 1 (the raw `left_node` would emit
            // weight n, surviving the difference once the right side covers it);
            // the integrated traces then hold only weight-1 entries.
            let distinct_l = cb.distinct(left_node);
            let distinct_r = cb.distinct(right_node);
            // `trace_l` is created before `except_lr`: whenever `trace_r` is still
            // empty the anti-join takes its trace-absent branch and drains
            // `distinct_l`'s register, so the non-destructive `integrate_trace`
            // (lower node id) must — and does — schedule first.
            let trace_l = cb.integrate_trace(distinct_l);
            let trace_r = cb.integrate_trace(distinct_r);
            // ΔA path: left set-members not in I(B).
            let except_lr = cb.anti_join_with_trace_node(distinct_l, trace_r);
            // ΔB path: when B's set membership flips, retract/emit the matching
            // left row. Reading `distinct_r` makes a second insert of an
            // already-present projected B row a no-op.
            let semi_rl = cb.semi_join_with_trace_node(distinct_r, trace_l);
            let correction = cb.negate(semi_rl);
            cb.union(except_lr, correction)
        }
        _ => return Err(GnitzSqlError::Unsupported(
            format!("set operation {:?} not supported", op)
        )),
    };

    cb.sink(out_node);
    let circuit = cb.build();

    // Output schema: U128 PK + all payload columns
    let mut out_cols_final: Vec<ColumnDef> = Vec::new();
    out_cols_final.push(ColumnDef {
        name: "_set_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    });
    for (l, r) in left_cols.iter().zip(&right_cols) {
        let mut col = l.clone();
        // Output nullability is operator-specific. EXCEPT emits only left-side
        // values (right-side corrections must match a left set-pk to fire, and a
        // NULL-in-c right tuple has a set-pk a NOT NULL left column never
        // produces), so it is nullable iff the left input is. INTERSECT emits a
        // tuple only when it is in both sides (set membership matches NULLs as
        // equal), so a column is nullable iff BOTH inputs admit NULL there.
        // UNION{,ALL} may emit from either side. Tightening only ever removes the
        // flag, and only when the operator's tuple algebra forbids NULL, so it
        // cannot mislabel a nullable column. Type equality is checked above.
        col.is_nullable = match op {
            SetOperator::Intersect => l.is_nullable && r.is_nullable,
            SetOperator::Except    => l.is_nullable,
            _                      => l.is_nullable || r.is_nullable,
        };
        out_cols_final.push(col);
    }

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols_final)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

// ---------------------------------------------------------------------------
// SELECT DISTINCT
// ---------------------------------------------------------------------------

fn execute_create_distinct_view(
    client:      &mut GnitzClient,
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
    let table_name = extract_table_factor_name(&select.from[0].relation, "SELECT DISTINCT")?;
    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;
    reject_compound_pk_view_source(&source_schema)?;

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

    // Project the requested columns, then reindex by a hash of just those
    // columns so `distinct` deduplicates on the projected content. Without the
    // projection, reindexing by the source PK (unique by definition) makes
    // `distinct` a no-op and leaks the unselected columns.
    let (proj_indices, proj_cols) = resolve_set_projection(&select.projection, &source_schema, binder)?;
    let reindexed = cb.map_hash_row(filtered, &proj_indices, 0);
    // Repartition by the synthetic hash PK so `distinct` deduplicates across
    // workers: every copy of a projected row must land on the same worker.
    let sharded = cb.shard(reindexed, &[0]);
    let distinct_node = cb.distinct(sharded);

    cb.sink(distinct_node);
    let circuit = cb.build();

    // Output schema: synthetic U128 PK + the projected columns.
    let mut out_cols: Vec<ColumnDef> = Vec::with_capacity(proj_cols.len() + 1);
    out_cols.push(ColumnDef {
        name: "_distinct_pk".into(), type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    });
    out_cols.extend(proj_cols);

    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}
