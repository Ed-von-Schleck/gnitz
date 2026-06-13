use std::collections::HashMap;
use std::rc::Rc;
use sqlparser::ast::{
    Statement, ObjectType, ColumnOption, TableConstraint, Query, SetExpr, Expr,
    SelectItem, TableFactor, JoinOperator, JoinConstraint, GroupByExpr,
    SetOperator, SetQuantifier, BinaryOperator,
    FunctionArguments, FunctionArg, FunctionArgExpr,
};
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_core::{
    GnitzClient, IndexMeta, CircuitBuilder, ExprBuilder, RangeRel,
    AGG_COUNT, AGG_COUNT_NON_NULL, AGG_SUM, AGG_MIN, AGG_MAX,
};
use crate::error::{GnitzSqlError, extract_name, extract_table_factor_name};
use crate::binder::{Binder, resolve_qualified_column, resolve_unqualified_column, find_unique_column};
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
            execute_create_index(client, schema_name, ci, &mut binder)
        }
        Statement::Update { .. } => dml::execute_update(client, schema_name, stmt, &mut binder),
        Statement::Delete(_)     => dml::execute_delete(client, schema_name, stmt, &mut binder),
        _ => Err(GnitzSqlError::Unsupported(
            format!("unsupported SQL statement: {stmt:?}")
        )),
    }
}

fn is_integer_type(tc: TypeCode) -> bool {
    matches!(tc,
        TypeCode::I8  | TypeCode::I16 | TypeCode::I32 | TypeCode::I64
        | TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64
        | TypeCode::U128 | TypeCode::I128
    )
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
            "self-referencing FK against '{ref_table}' must name the referenced column",
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
            "FK type mismatch: column type {fk_col_type:?} is not compatible with referenced \
             column type {parent_col_type:?}",
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
        .map_err(|e| GnitzSqlError::Bind(format!("FK target '{ref_table}': {e}")))?;

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
            "FK against compound-PK table '{ref_table}' must name the referenced column",
        )));
    };

    // Legal target iff the referenced column is the parent's lone PK, or it
    // carries an active UNIQUE index. The `&&` short-circuits, so
    // `pk_index_single()` is never reached for a compound parent.
    let is_lone_pk = ref_schema.pk_count() == 1
        && ref_col_idx == ref_schema.pk_index_single();
    if !is_lone_pk {
        match client.index_for_column(ref_tid, ref_col_idx).map_err(GnitzSqlError::Exec)? {
            Some(IndexMeta { is_unique: true, .. }) => {}
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
            "FK type mismatch: column type {fk_col_type:?} is not compatible with referenced \
             column type {parent_col_type:?}",
        )));
    }

    Ok((ref_tid, ref_col_idx as u64, parent_col_type))
}

/// Reject an output column list that names the same column twice. The binder's
/// name->index lookup silently binds to the first match, so a duplicate would
/// bind ambiguously for any downstream query. `context` names the DDL surface
/// for the error message (e.g. "table definition", "CREATE VIEW projection").
fn reject_duplicate_column_names<'a>(
    names:   impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(name.to_lowercase()) {
            return Err(GnitzSqlError::Plan(format!(
                "duplicate column name '{name}' in {context}"
            )));
        }
    }
    Ok(())
}

/// Validate a user-supplied name destined to become an index name — a CREATE
/// INDEX name or a UNIQUE/constraint name that maps to a secondary index.
/// Beyond the general identifier rules, the reserved `__fk_` infix is rejected:
/// such a name would collide with internal FK-backing index names and persist as
/// undroppable (`drop_index` refuses it). The infix check is scoped here rather
/// than in `validate_user_identifier`, which also guards table/column/schema
/// names that may legitimately contain `__fk_`.
fn validate_user_index_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_core::validate_user_identifier(name).map_err(GnitzSqlError::Plan)?;
    if name.contains(gnitz_core::FK_INDEX_INFIX) {
        return Err(GnitzSqlError::Plan(format!(
            "Index/constraint names cannot contain the reserved '{}' infix",
            gnitz_core::FK_INDEX_INFIX)));
    }
    Ok(())
}

fn execute_create_table(
    client:      &mut GnitzClient,
    schema_name: &str,
    create:      &sqlparser::ast::CreateTable,
    _binder:     &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&create.name, "CREATE TABLE")?;

    let sql_cols = &create.columns;

    // Reject duplicate column names up front, before the PK binder runs.
    reject_duplicate_column_names(
        sql_cols.iter().map(|c| c.name.value.as_str()),
        "table definition",
    )?;

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
    // Column lists carrying a UNIQUE constraint, paired with the user-specified
    // constraint name (if any). A column-level `UNIQUE` is a 1-element list and
    // carries no name (always `None`); a table-level `UNIQUE (a, b, …)` is the
    // full ordered list and may carry a `CONSTRAINT <name>`.
    let mut unique_cols: Vec<(Vec<u32>, Option<String>)> = Vec::new();

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

    // Phase 3a — gather all inline column-level PRIMARY KEYs first, so that
    // `pk_indices` is fully populated before any FK is resolved. A
    // self-referencing FK declared before its inline PK column would otherwise
    // see an empty `pk_indices` and fail spuriously.
    for (i, col) in sql_cols.iter().enumerate() {
        for opt in &col.options {
            if let ColumnOption::Unique { is_primary: true, .. } = &opt.option {
                if pk_decl_seen {
                    return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
                }
                pk_decl_seen = true;
                pk_indices.push(i as u32);
            }
        }
    }

    // Phase 3b — resolve inline FOREIGN KEYs and collect column-level UNIQUE.
    for (i, col) in sql_cols.iter().enumerate() {
        for opt in &col.options {
            match &opt.option {
                ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                    let (tid, idx, parent_pk_type) = resolve_fk_target(
                        client, schema_name, foreign_table, referred_columns, cols[i].type_code,
                        &table_name, &cols, &pk_indices,
                    )?;
                    cols[i].fk_table_id = tid;
                    cols[i].fk_col_idx  = idx;
                    cols[i].type_code   = parent_pk_type;
                }
                ColumnOption::Unique { is_primary: false, .. }
                    if !unique_cols.iter().any(|(c, _)| c.as_slice() == [i as u32]) =>
                {
                    unique_cols.push((vec![i as u32], None));
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
                    "FOREIGN KEY column '{local_col_name}' not found in table definition"
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

    // Phase 5 — table-level UNIQUE constraints, single- or multi-column. Each
    // named column is resolved to its index in declared order (order is
    // significant: it drives the composite index's leading-key span and prefix
    // seeks).
    for constraint in &create.constraints {
        if let TableConstraint::Unique { name: name_ident, columns, .. } = constraint {
            if columns.is_empty() {
                return Err(GnitzSqlError::Plan("UNIQUE constraint cannot be empty".into()));
            }
            let mut col_indices: Vec<u32> = Vec::with_capacity(columns.len());
            for col_ident in columns {
                let col_name = &col_ident.value;
                let idx = cols.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| GnitzSqlError::Bind(format!(
                        "UNIQUE column '{col_name}' not found in table definition")))?;
                if col_indices.contains(&(idx as u32)) {
                    return Err(GnitzSqlError::Plan(format!(
                        "duplicate column '{col_name}' in UNIQUE constraint")));
                }
                col_indices.push(idx as u32);
            }
            if unique_cols.iter().any(|(c, _)| c.as_slice() == col_indices.as_slice()) {
                return Err(GnitzSqlError::Plan(format!(
                    "duplicate UNIQUE constraint on column(s) ({})",
                    columns.iter().map(|c| c.value.as_str()).collect::<Vec<_>>().join(", "))));
            }
            let constraint_name = name_ident.as_ref().map(|n| n.value.clone());
            if let Some(ref name) = constraint_name {
                validate_user_index_name(name)?;
            }
            unique_cols.push((col_indices, constraint_name));
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
    if pk_indices.len() > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(format!(
            "PRIMARY KEY supports at most {} columns", gnitz_core::PK_LIST_MAX_COLS
        )));
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

    // A lone single-column PK is already unique; drop a redundant secondary
    // unique index equal to it. A compound-PK member declared UNIQUE is NOT
    // individually unique, and a composite UNIQUE (len > 1) never equals a
    // single-element lone PK, so both are kept (the engine supports unique
    // indices on PK columns, and the engine's trivial-uniqueness short-circuit
    // skips the pre-flight scan for a composite UNIQUE equal to a compound PK).
    let lone_pk: &[u32] = if pk_indices.len() == 1 { &pk_indices } else { &[] };
    unique_cols.retain(|(c, _)| c.as_slice() != lone_pk);

    // Pre-validate index-eligibility BEFORE create_table: DDL is not
    // transactional, so a type error after the table is created would leave an
    // orphan table. `is_pk_eligible` is the exact index-eligible allow-list, so
    // this one gate covers every rejected type. A UNIQUE+FK column always
    // passes — its type was rewritten to the parent's (integer) PK type above.
    for (col_indices, _) in &unique_cols {
        for &c in col_indices {
            let tc = cols[c as usize].type_code;
            if !tc.is_pk_eligible() {
                return Err(GnitzSqlError::Unsupported(format!(
                    "UNIQUE column '{}' of type {:?} is not supported \
                     (UNIQUE requires a fixed-width integer, U128, or UUID column; \
                     String, Blob, and float columns cannot carry a UNIQUE index)",
                    cols[c as usize].name, tc)));
            }
        }
    }

    let tid = client.create_table(schema_name, &table_name, &cols, &pk_indices, true)
        .map_err(GnitzSqlError::Exec)?;

    // Unique secondary indices: created after the table exists (create_index
    // requires the table id). Types were pre-validated, and the base table is
    // empty, so create_index here cannot fail on a bad type or duplicate data —
    // only on transport-level errors. On failure, best-effort drop_table to
    // avoid an orphaned table with missing constraints (DDL is not
    // transactional across the create_table / create_index boundary).
    for (col_indices, constraint_name) in &unique_cols {
        let col_types: Vec<TypeCode> =
            col_indices.iter().map(|&c| cols[c as usize].type_code).collect();
        let index_name = constraint_name.clone().unwrap_or_else(|| {
            let col_names: Vec<&str> = col_indices.iter()
                .map(|&c| cols[c as usize].name.as_str()).collect();
            default_index_name(schema_name, &table_name, &col_names)
        });
        if let Err(e) = client.create_index(tid, col_indices, &col_types, &index_name, true) {
            let _ = client.drop_table(schema_name, &table_name);
            return Err(GnitzSqlError::Exec(e));
        }
    }

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
                format!("DROP {object_type:?} not supported")
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

    let sql_text = format!("{stmt}");

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
                    format!("CTE '{cte_name}': only SELECT supported")
                )),
            };
            if cte_select.from.len() != 1 || !cte_select.from[0].joins.is_empty() {
                return Err(GnitzSqlError::Unsupported(
                    format!("CTE '{cte_name}': only single table without JOINs")
                ));
            }
            let cte_table_name = extract_table_factor_name(
                &cte_select.from[0].relation,
                &format!("CTE '{cte_name}'"),
            )?;
            // Resolve the CTE's source table and cache the CTE name as an alias.
            let (cte_tid, cte_schema) = binder.resolve(client, &cte_table_name)?;
            // A CTE is inlined only as a plain column pass-through of its source
            // table. Anything that changes the row set or shape (WHERE, GROUP BY,
            // HAVING, DISTINCT, ORDER BY, LIMIT, or a non-identity projection)
            // would be silently discarded by the alias mechanism and return wrong
            // rows, so reject it explicitly. (Compiling an arbitrary CTE body as a
            // sub-plan is a separate, unimplemented feature.)
            // Positional identity: `*`, or one identifier per source column in
            // order. The qualified form (`SELECT t.a, t.b FROM t`) parses as
            // `CompoundIdentifier` and is the same positional pass-through — match
            // `parts[1]` against each source column. This stays a positional
            // identity test (never a name→index lookup), so a dup-named source
            // simply fails the per-position compare and is rejected.
            let proj_is_identity = cte_select.projection.iter()
                    .all(|p| matches!(p, SelectItem::Wildcard(_)))
                || (cte_select.projection.len() == cte_schema.columns.len()
                    && cte_select.projection.iter().enumerate().all(|(i, item)| {
                        let want = &cte_schema.columns[i].name;
                        match item {
                            SelectItem::UnnamedExpr(Expr::Identifier(id)) =>
                                id.value.eq_ignore_ascii_case(want),
                            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 =>
                                parts[1].value.eq_ignore_ascii_case(want),
                            _ => false,
                        }
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
                    "CTE '{cte_name}': only a plain column pass-through of a single table is \
                     supported; WHERE, a subset/reordered/computed projection, GROUP BY, \
                     HAVING, DISTINCT, ORDER BY and LIMIT inside a CTE are not yet supported"
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
    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let (items, out_cols) = build_projection(&select.projection, &source_schema, binder)?;

    // The pass-through generalization can now emit the same name twice (SELECT
    // name, name; or an alias landing on the auto-prepended PK). This keys on
    // output *names*, so a duplicate PK *value* under distinct names (SELECT id,
    // id AS id2) is unaffected and accepted.
    //
    // A `SELECT *` over a dup-named source (a join view surfacing both sides'
    // `id`) legitimately carries the duplicate names through positionally — the
    // same wildcard contract as `execute_create_join_view`. Gate the guard on
    // `!is_wildcard` so only an *explicit* projection that produces a duplicate
    // (`SELECT name, name`) errors.
    if !is_wildcard {
        reject_duplicate_column_names(
            out_cols.iter().map(|c| c.name.as_str()),
            "CREATE VIEW projection",
        )?;
    }

    // Slots 0..k are the view's physical PK (carried verbatim by commit_row). A
    // payload slot (>= k) that is a PK PassThrough is a duplicate PK value; a
    // Computed is a derived column. Either forces the expr-map, which trusts the
    // planner's declared schema (node_schema = out_schema) and writes/derives
    // each payload slot explicitly — the pure projection path would silently drop
    // a PK value requested as a payload column.
    let k = source_schema.pk_count();
    let needs_expr_map = items[k..].iter().any(|item| match item {
        ProjectionItem::Computed { .. }         => true,
        ProjectionItem::PassThrough { src_col }  => source_schema.is_pk_col(*src_col),
    });

    // Allocate view_id
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;

    // Build circuit
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();
    let filtered = match expr_prog {
        Some(p) => cb.filter(inp, Some(p)),
        None    => inp,
    };

    let out_node = if needs_expr_map {
        // Emit only the payload slots (k..); the k physical PK columns are carried
        // by commit_row and must not appear in the program. payload_idx is the
        // dense output payload position, matching out_cols[k + payload_idx].
        let mut eb = ExprBuilder::new();
        for (payload_idx, item) in items[k..].iter().enumerate() {
            let payload_idx = payload_idx as u32;
            match item {
                ProjectionItem::PassThrough { src_col } => {
                    let tc = source_schema.columns[*src_col].type_code as u32;
                    eb.copy_col(tc, *src_col as u32, payload_idx);
                }
                ProjectionItem::Computed { bound_expr, .. } => {
                    let (reg, _) = compile_bound_expr(bound_expr, &source_schema, &mut eb)?;
                    // EMIT writes the raw register bits via append_int — correct for float.
                    eb.emit_col(reg, payload_idx);
                }
            }
        }
        let program = eb.build(0);  // result_reg unused — EMIT/COPY_COL write directly
        cb.map_expr(filtered, program)
    } else if items.len() < source_schema.columns.len()
           || items.iter().enumerate().any(|(i, item)| match item {
               ProjectionItem::PassThrough { src_col } => *src_col != i,
               _ => false,
           }) {
        // Pure column reorder/subset — every payload item is a non-PK
        // pass-through, so build_map_output_schema reproduces out_cols (PK region
        // in pk_indices() order, then non-PK cols in projection order). Pass only
        // the payload items (`items[k..]`): the k PK slots are inherited verbatim
        // by execute_map's bulk PK copy / build_map_output_schema's PK prepend.
        // Including them would emit one ColMove per PK index with dst_payload set
        // to the enumeration index, shifting every payload destination out of range
        // (single-PK: payload OOB; compound-PK: SENTINEL stride past num_payload).
        let cols: Vec<usize> = items[k..].iter().filter_map(|i| match i {
            ProjectionItem::PassThrough { src_col } => Some(*src_col),
            _ => None,
        }).collect();
        cb.map(filtered, &cols)
    } else {
        // Identity — PK already at front, full width, no map needed.
        filtered
    };

    cb.sink(out_node);
    let circuit = cb.build();

    // The view's physical PK is the leading k columns (the source PK passed
    // through in pk_indices() order). Persist exactly that.
    let view_pk: Vec<u32> = (0..k as u32).collect();
    client.create_view_with_circuit(schema_name, &view_name, &sql_text, circuit, &out_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Catalog name for an auto-generated (unnamed) secondary index:
/// `{schema}__{table}__idx_{col1}_{col2}…` (column names joined with `_`).
/// `DROP INDEX <name>` resolves this exact string, so the format is a stable
/// contract (the drop-by-name tests in `planner_create_table` pin it); this is
/// its single definition, shared by CREATE INDEX and CREATE TABLE … UNIQUE.
fn default_index_name(schema_name: &str, table_name: &str, col_names: &[&str]) -> String {
    format!("{schema_name}__{table_name}__idx_{}", col_names.join("_"))
}

fn execute_create_index(
    client:      &mut GnitzClient,
    schema_name: &str,
    ci:          &sqlparser::ast::CreateIndex,
    binder:      &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&ci.table_name, "CREATE INDEX")?;

    // A user-supplied index name flows through to the IDX_TAB row so
    // `DROP INDEX <name>` resolves it. Validate it before anything else: a
    // malformed or `__fk_`-infixed name would persist and be undroppable.
    let explicit_name = ci.name.as_ref()
        .map(|n| extract_name(n, "CREATE INDEX"))
        .transpose()?;
    if let Some(ref name) = explicit_name {
        validate_user_index_name(name)?;
    }

    if ci.columns.is_empty() {
        return Err(GnitzSqlError::Bind(
            "CREATE INDEX: at least one column required".to_string()
        ));
    }
    let is_unique = ci.unique;

    // Resolve the target as a base table: this rejects a view (read-only — a
    // view's store is maintained solely by its circuit, so indexing a snapshot of
    // derived data has no defined semantics) with a precise error, and yields the
    // schema used to resolve the indexed columns. The client write below takes the
    // already-resolved (table_id, col_indices), so the name is resolved once.
    let (table_id, schema) = binder.resolve_base_table(client, &table_name)?;

    // Resolve each indexed column to its index, in declared order; each must be a
    // simple identifier. Reject duplicate columns.
    let mut col_names: Vec<String> = Vec::with_capacity(ci.columns.len());
    let mut col_indices: Vec<u32> = Vec::with_capacity(ci.columns.len());
    for c in &ci.columns {
        let col_name = match &c.column.expr {
            Expr::Identifier(id) => id.value.clone(),
            _ => return Err(GnitzSqlError::Bind(
                "CREATE INDEX: column must be a simple identifier".to_string()
            )),
        };
        let col_idx = find_unique_column(&schema.columns, &col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(
                format!("column '{col_name}' not found")
            ))?;
        if col_indices.contains(&(col_idx as u32)) {
            return Err(GnitzSqlError::Unsupported(format!(
                "CREATE INDEX: duplicate column '{col_name}' in index list")));
        }
        col_names.push(col_name);
        col_indices.push(col_idx as u32);
    }

    // Limit pre-check — runs before the client push so an over-limit index
    // raises a clean planner error here, not after the IDX_TAB row already
    // committed. `index_key_types` is the same promotion + arity/stride
    // validation the engine's `make_index_schema` runs, so the friendly error
    // here and the engine backstop can never disagree.
    let col_types: Vec<TypeCode> = col_indices.iter()
        .map(|&c| schema.columns[c as usize].type_code).collect();
    let raw_types: Vec<u8> = col_types.iter().map(|&tc| tc as u8).collect();
    gnitz_core::index_key_types(&raw_types, schema.pk_count(), schema.pk_stride())
        .map_err(GnitzSqlError::Unsupported)?;   // also rejects String/Blob/float

    let index_name = explicit_name.unwrap_or_else(|| {
        let names: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        default_index_name(schema_name, &table_name, &names)
    });

    let index_id = client.create_index(
        table_id, &col_indices, &col_types, &index_name, is_unique,
    ).map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::IndexCreated { index_id })
}

enum ProjectionItem {
    PassThrough { src_col: usize },
    Computed { bound_expr: BoundExpr, _out_type: TypeCode },
}

fn build_projection(
    projection:    &[SelectItem],
    source_schema: &Schema,
    binder:        &Binder<'_>,
) -> Result<(Vec<ProjectionItem>, Vec<ColumnDef>), GnitzSqlError> {
    let mut items: Vec<ProjectionItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();

    for (idx, item) in projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let col_idx = find_unique_column(&source_schema.columns, &ident.value)?
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("column '{}' not found", ident.value)
                    ))?;
                items.push(ProjectionItem::PassThrough { src_col: col_idx });
                out_cols.push(source_schema.columns[col_idx].clone());
            }
            SelectItem::Wildcard(_) => {
                // No early return: `SELECT *` expands here and then flows through
                // the PK-placement loop below, so the source PK is pinned to slots
                // 0..k even when it is not the table's leading column. A PK already
                // at the front degenerates to the verbatim identity order.
                for i in 0..source_schema.columns.len() {
                    items.push(ProjectionItem::PassThrough { src_col: i });
                    out_cols.push(source_schema.columns[i].clone());
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let bound = binder.bind_expr(expr, source_schema)?;
                // A direct column reference with an alias is a PassThrough with a
                // renamed output column, not a computation — so an aliased PK
                // column is found by the placement scan and moved like any other.
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
                let bound = binder.bind_expr(expr, source_schema)?;
                // A qualified (t.col) or parenthesized ((col)) reference binds to
                // a bare ColRef — a verbatim pass-through, like the aliased arm.
                // Without this, a qualified PK column would be wrapped as Computed
                // (and formerly rejected), and a qualified non-PK column would be
                // renamed to _exprN and needlessly recomputed.
                if let BoundExpr::ColRef(ci) = bound {
                    items.push(ProjectionItem::PassThrough { src_col: ci });
                    out_cols.push(source_schema.columns[ci].clone());
                } else {
                    let out_type = bound.infer_type(source_schema);
                    items.push(ProjectionItem::Computed { bound_expr: bound, _out_type: out_type });
                    out_cols.push(ColumnDef {
                        name: format!("_expr{idx}"),
                        type_code: out_type,
                        is_nullable: true, fk_table_id: 0, fk_col_idx: 0,
                    });
                }
            }
            _ => return Err(GnitzSqlError::Unsupported(
                "unsupported SELECT item in CREATE VIEW projection".to_string()
            )),
        }
    }

    // Pass the full source PK through to the view output: every source PK column
    // occupies output slots 0..k in pk_indices() order, matching the engine's
    // build_map_output_schema (which copies all PK columns to the front via
    // copy_pk_columns_into). One loop serves every PK arity — k == 1 reduces to a
    // single-column move-to-front.
    for (target, &pk) in source_schema.pk_indices().iter().enumerate() {
        // First occurrence is the canonical physical-PK slot; any later duplicate
        // (SELECT pk, pk AS x) stays in the payload region and is materialized by
        // the expr-map COPY_COL path.
        let cur = items.iter().position(|i|
            matches!(i, ProjectionItem::PassThrough { src_col } if *src_col == pk));
        match cur {
            Some(pos) if pos == target => { /* already in place */ }
            Some(pos) => {
                // pos > target here (slots 0..target already hold earlier PK
                // columns); remove+insert shifts the spanned non-PK columns right
                // by one and preserves their relative order — a swap would not.
                let it  = items.remove(pos);
                let col = out_cols.remove(pos);
                items.insert(target, it);
                out_cols.insert(target, col);
            }
            None => {
                // The projection omits this PK column, or references it only
                // through a computed expression (SELECT pk + 1). Either way the
                // view carries the full source PK verbatim: auto-prepend it.
                items.insert(target, ProjectionItem::PassThrough { src_col: pk });
                out_cols.insert(target, source_schema.columns[pk].clone());
            }
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
    let (right_tid, right_schema) = binder.resolve(client, &right_name)?;

    // The 2-term DBSP join formula assumes the left and right input deltas are
    // never simultaneously active. A self-join feeds the same delta to both
    // sides in one epoch, dropping the bilinear ΔT⋈ΔT cross-term — silent data
    // loss whenever same-key rows arrive in one batch.
    if left_tid == right_tid {
        return Err(GnitzSqlError::Unsupported(
            "self-join (joining a table with itself) is not supported".into()
        ));
    }

    // Build alias map for qualified column resolution
    let mut alias_map: JoinAliasMap = HashMap::new();
    alias_map.insert(left_alias.to_lowercase(), (left_tid, Rc::clone(&left_schema), 0));
    alias_map.insert(right_alias.to_lowercase(), (right_tid, Rc::clone(&right_schema), left_schema.columns.len()));

    // Classify the ON clause: equality prefix pairs + an optional range conjunct.
    let (left_join_cols, right_join_cols, target_tcs, range_conjunct) = extract_join_predicates(
        on_expr, &left_schema, &right_schema, &alias_map,
    )?;

    // Range (band) join: one range conjunct (optionally behind equality
    // conjuncts) routes to the broadcast / re-key / output-exchange circuit.
    if let Some(range) = range_conjunct {
        if is_left_join {
            return Err(GnitzSqlError::Unsupported(
                "LEFT JOIN with a range predicate is not supported".into()));
        }
        return build_range_join_view(
            client, schema_name, view_name, sql_text, select, &alias_map,
            left_tid, right_tid, &left_schema, &right_schema,
            &left_join_cols, &right_join_cols, &target_tcs, range,
        );
    }

    // Equi-join path (zero range conjuncts) — byte-identical to before. The
    // reindex-slot arity cap was already enforced in extract_join_predicates.
    let k = left_join_cols.len();   // == right_join_cols.len(), 1..=PK_LIST_MAX_COLS

    // Per-side carried target tc for each key pair: `T_i` only when the side's own
    // self-derived reindex output type differs from `T_i` (a cross-width promotion
    // on that side), else `0` (self-derive). The `0` keeps same-type / U128-vs-UUID
    // / string circuits byte-identical to the pre-promotion serialization. The
    // encode rule lives in `carried_reindex_tc`, the round-trip inverse of the
    // engine's `resolve_reindex_type`.
    let side_tcs = |cols: &[usize], schema: &Schema| -> Vec<u8> {
        cols.iter().zip(&target_tcs).map(|(&c, &t)| {
            schema.columns[c].type_code.carried_reindex_tc(TypeCode::from_validated_u8(t))
        }).collect()
    };
    let left_target_tcs  = side_tcs(&left_join_cols,  &left_schema);
    let right_target_tcs = side_tcs(&right_join_cols, &right_schema);

    // The merged join output is [k _join_pk cols, left cols..., right cols...].
    // Reject before the server's schema build hits its hard column-count assertion.
    let combined_cols = k + left_schema.columns.len() + right_schema.columns.len();
    if combined_cols > gnitz_core::MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN view output has {} columns, exceeding the {}-column limit",
            combined_cols, gnitz_core::MAX_COLUMNS,
        )));
    }

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
    let left_key_nullable  = left_join_cols.iter().any(|&c| left_schema.columns[c].is_nullable);
    let right_key_nullable = right_join_cols.iter().any(|&c| right_schema.columns[c].is_nullable);

    let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
    let input_a = cb.input_delta_tagged(left_tid);
    let input_b = cb.input_delta_tagged(right_tid);

    // Inner (right) side: a key with any NULL component can never match — drop it.
    let input_b = if right_key_nullable {
        cb.filter(input_b, Some(multi_null_filter_prog(&right_join_cols, &right_schema, false)?))
    } else {
        input_b
    };
    let reindex_b = cb.map_reindex(input_b, &right_join_cols, &right_target_tcs, right_reindex_prog);

    // Preserved (left) side.
    //   INNER join: a left row with any NULL key component matches nothing — drop it.
    //   LEFT  join: such a row must still be emitted with NULL right columns but must
    //               bypass the match (else it collides with a right 0/"" key and
    //               pollutes trace_a, corrupting join_ba and the ΔB null-fill
    //               correction). Split it out, reindex it to the synthetic join PK
    //               (so it is layout-compatible with the join output — a bare Filter
    //               would keep the left table's native PK stride and corrupt the
    //               downstream Union, which merges by raw PK bytes into one stride),
    //               null-extend it, and union it into the unmatched-left stream.
    //               Its NULL components pack to 0 bytes, which is harmless: the source
    //               columns are among the copied payload and compare_rows treats
    //               NULL ≠ 0, so it never merges with a real-0-key row.
    let (input_a_match, left_null_filled) = if !left_key_nullable {
        (input_a, None)
    } else {
        let left_not_null = cb.filter(
            input_a, Some(multi_null_filter_prog(&left_join_cols, &left_schema, false)?));
        if !is_left_join {
            (left_not_null, None)
        } else {
            let left_null = cb.filter(
                input_a, Some(multi_null_filter_prog(&left_join_cols, &left_schema, true)?));
            // MUST use the same left_target_tcs as reindex_a, else this branch
            // reindexes at the source width and the downstream UNION merges two
            // different `_join_pk` strides.
            let left_null_ri = cb.map_reindex(
                left_null, &left_join_cols, &left_target_tcs, build_reindex_program(&left_schema));
            (left_not_null, Some(cb.null_extend(left_null_ri, &right_col_tcs)))
        }
    };
    let reindex_a = cb.map_reindex(input_a_match, &left_join_cols, &left_target_tcs, left_reindex_prog);

    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))

    // Path AB projection: identity (already canonical: [PK cols, A_cols, B_cols])
    let proj_ab: Vec<usize> = (k..k + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);

    // Path BA projection: reorder [A_cols, B_cols]
    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n {
        proj_ba.push(k + right_n + i);
    }
    for i in 0..right_n {
        proj_ba.push(k + i);
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

    // Build virtual combined output schema: k synthetic join PK cols + all left cols
    // + all right cols. After proj_ab/proj_ba, the UNION output has this layout (union
    // col indices):
    //   col 0..k: _join_pk[_i] (PK region, one slot per key pair)
    //   col k..k+left_n: all A columns (in A schema order)
    //   col k+left_n..k+left_n+right_n: all B columns (in B schema order)
    //
    // Each synthetic PK column's type is the pair's `join_key_common_type` `T_i`
    // (returned by validate_join_key_pair): the single persisted stride that both
    // sides' reindex Maps and every cross-process consumer re-derive. For a
    // same-type pair `T_i` equals the source's reindex_output_type, so the catalog
    // value is unchanged; for a cross-width pair it is the wider promoted type.
    //
    // The first key column keeps the name `_join_pk` at k = 1 (the catalog name is not
    // referenced by any code, but preserving it keeps the shippable single-key view
    // byte-identical); composite keys use `_join_pk_{i}`.
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for (i, &t) in target_tcs.iter().enumerate() {
        let name = if k == 1 { "_join_pk".to_string() } else { format!("_join_pk_{i}") };
        out_cols.push(ColumnDef {
            name,
            // The pair's common type T_i — the single persisted stride both sides'
            // reindex Maps and every cross-process consumer re-derive.
            type_code: TypeCode::from_validated_u8(t),
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
    }
    for col in &left_schema.columns {
        out_cols.push(col.clone());
    }
    for col in &right_schema.columns {
        let mut c = col.clone();
        if is_left_join { c.is_nullable = true; }
        out_cols.push(c);
    }

    // Compute the user projection + view schema via the shared join-projection
    // helper. A lone `SELECT *` flows through its Wildcard arm and stays identity
    // (the projection map is then skipped below), so no wildcard fast path here.
    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let (final_cols, final_projection) = build_join_view_projection(
        &select.projection, &alias_map, &out_cols[..k], left_n + right_n, k,
        |idx| out_cols[k + idx].clone(), "JOIN view",
    )?;

    // Apply final column projection before sink when not identity.
    // Identity = selecting all left+right cols in canonical order [k..k+left_n+right_n].
    let is_identity = final_projection.len() == left_n + right_n
        && final_projection.iter().enumerate().all(|(i, &p)| p == i + k);
    let sink_input = if is_identity {
        merged
    } else {
        cb.map(merged, &final_projection)
    };
    cb.sink(sink_input);
    let circuit = cb.build();

    // The view's physical PK is the k synthetic `_join_pk` columns at slots 0..k
    // (final_cols lists them first). At k = 1 this is the existing single `[0]`.
    let view_pk: Vec<u32> = (0..k as u32).collect();
    // Reject duplicate output names from explicit user aliases (e.g.
    // `SELECT l.a AS x, r.b AS x`). A `SELECT *` join legitimately surfaces
    // same-named columns from both sides (both tables' `id`); that is the
    // established wildcard contract, so the guard applies only to explicit
    // projections.
    if !is_wildcard {
        reject_duplicate_column_names(
            final_cols.iter().map(|c| c.name.as_str()),
            "join view",
        )?;
    }
    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Build a non-equi (range / band) join view: `n_eq` equality conjuncts (possibly
/// zero) plus exactly one range conjunct. Both sides reindex onto
/// `[eq slots…, range slot]` at the pair's common promoted type, so each side's
/// trace is an ordered arrangement by the range key; the active delta is
/// broadcast and probed against the other side's owned trace by an ordered range
/// walk. Because the two terms emit with different delta-side keys, the output is
/// re-keyed onto the **source-PK pair** `(a.pk…, b.pk…)` — the only identity
/// under which a `+1` and its later `-1` (from opposite terms, on different
/// workers) are byte-identical — then exchanged by that pair-PK, so the view is
/// PK-partitioned like every other view. INNER join only.
#[allow(clippy::too_many_arguments)]
fn build_range_join_view(
    client:          &mut GnitzClient,
    schema_name:     &str,
    view_name:       &str,
    sql_text:        &str,
    select:          &sqlparser::ast::Select,
    alias_map:       &JoinAliasMap,
    left_tid:        u64,
    right_tid:       u64,
    left_schema:     &Schema,
    right_schema:    &Schema,
    left_join_cols:  &[usize],
    right_join_cols: &[usize],
    eq_tcs:          &[u8],
    range:           RangeConjunct,
) -> Result<SqlResult, GnitzSqlError> {
    let left_n  = left_schema.columns.len();
    let right_n = right_schema.columns.len();
    let n_eq = left_join_cols.len();
    let k = n_eq + 1;                       // reindex slots: eq prefix + range slot
    let pa = left_schema.pk_cols.len();
    let pb = right_schema.pk_cols.len();
    let pair_pk = pa + pb;                  // output PK arity

    // The reindex-slot arity cap (k ≤ PK_LIST_MAX_COLS) was enforced in
    // extract_join_predicates. Here we additionally cap the output pair-PK.
    //
    // Output pair-PK arity cap (a.pk_count + b.pk_count ≤ PK_LIST_MAX_COLS) — the
    // binding constraint on the synthesized output PK; the stride ceiling is
    // non-binding (≤ 4·16 = 64 ≤ MAX_PK_BYTES). The engine's validate_pk_cols is
    // the backstop; this is the friendly planner error.
    if pair_pk > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(format!(
            "range JOIN output PK has {pair_pk} columns (a.pk {pa} + b.pk {pb}), \
             exceeding the {}-column limit", gnitz_core::PK_LIST_MAX_COLS)));
    }
    // The widest intermediate is the re-key output: pair-PK + k `_join_pk` slots +
    // every A and B column. Reject before the server's hard column-count assertion.
    let rekey_cols = pair_pk + k + left_n + right_n;
    if rekey_cols > gnitz_core::MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "range JOIN view has {rekey_cols} intermediate columns, exceeding the \
             {}-column limit", gnitz_core::MAX_COLUMNS)));
    }

    // Reindex columns and per-pair common type T: eq pairs first, range slot last.
    let left_reindex_cols:  Vec<usize> = left_join_cols.iter().copied()
        .chain(std::iter::once(range.left_col)).collect();
    let right_reindex_cols: Vec<usize> = right_join_cols.iter().copied()
        .chain(std::iter::once(range.right_col)).collect();
    let all_tcs: Vec<u8> = eq_tcs.iter().copied().chain(std::iter::once(range.tc)).collect();

    // Per-side carried target tc per slot (0 = self-derive); see the equi path.
    let side_tcs = |cols: &[usize], schema: &Schema| -> Vec<u8> {
        cols.iter().zip(&all_tcs).map(|(&c, &t)| {
            schema.columns[c].type_code.carried_reindex_tc(TypeCode::from_validated_u8(t))
        }).collect()
    };
    let left_target_tcs  = side_tcs(&left_reindex_cols,  left_schema);
    let right_target_tcs = side_tcs(&right_reindex_cols, right_schema);

    // §3 table: term AB ({y : x OP y}) wants trace y `converse(OP)` delta x; term
    // BA ({x : x OP y}) wants trace x `OP` delta y.
    let rel_ab = converse_rel(range.op);
    let rel_ba = range.op;

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a = cb.input_delta_tagged(left_tid);
    let input_b = cb.input_delta_tagged(right_tid);

    // NULL exclusion (SQL 3VL) over ALL key cols (eq + range) when any is nullable.
    // INNER join only, so no null-fill branches.
    let left_key_nullable  = left_reindex_cols.iter().any(|&c| left_schema.columns[c].is_nullable);
    let right_key_nullable = right_reindex_cols.iter().any(|&c| right_schema.columns[c].is_nullable);
    let input_a = if left_key_nullable {
        cb.filter(input_a, Some(multi_null_filter_prog(&left_reindex_cols, left_schema, false)?))
    } else { input_a };
    let input_b = if right_key_nullable {
        cb.filter(input_b, Some(multi_null_filter_prog(&right_reindex_cols, right_schema, false)?))
    } else { input_b };

    let reindex_a = cb.map_reindex(input_a, &left_reindex_cols, &left_target_tcs, build_reindex_program(left_schema));
    let reindex_b = cb.map_reindex(input_b, &right_reindex_cols, &right_target_tcs, build_reindex_program(right_schema));

    // Trace = the worker-owned slice only (PartitionFilter between reindex and
    // integrate); the join terms probe the UNFILTERED (full, broadcast) reindex.
    let filt_a = cb.partition_filter(reindex_a);
    let trace_a = cb.integrate_trace(filt_a);
    let filt_b = cb.partition_filter(reindex_b);
    let trace_b = cb.integrate_trace(filt_b);
    let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab); // ΔA ⋈θ I(B)
    let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba); // ΔB ⋈θ I(A)

    // Per-term normalize payload to [A cols, B cols] (verbatim from the equi path,
    // with k = n_eq + 1).
    let proj_ab: Vec<usize> = (k..k + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);
    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n  { proj_ba.push(k + right_n + i); }
    for i in 0..right_n { proj_ba.push(k + i); }
    let proj_ba_node = cb.map(join_ba, &proj_ba);
    let merged = cb.union(proj_ab_node, proj_ba_node);

    // Re-key onto the source-PK pair `[a.pk…, b.pk…]`. The merged layout is
    // `[_join_pk × k, A cols, B cols]`; A's PK col j sits at `k + j`, B's at
    // `k + left_n + j`. Targets all 0 (self-derive — each slot keeps its source PK
    // type, no cross-side promotion).
    let mut union_cols: Vec<ColumnDef> = Vec::with_capacity(k + left_n + right_n);
    for (i, &t) in all_tcs.iter().enumerate() {
        union_cols.push(ColumnDef {
            name: format!("_join_pk_{i}"), type_code: TypeCode::from_validated_u8(t),
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
    }
    for col in &left_schema.columns  { union_cols.push(col.clone()); }
    for col in &right_schema.columns { union_cols.push(col.clone()); }
    let union_schema = Schema { columns: union_cols, pk_cols: (0..k).collect() };

    let mut pair_pk_cols: Vec<usize> = Vec::with_capacity(pair_pk);
    for &a_pk in &left_schema.pk_cols  { pair_pk_cols.push(k + a_pk); }
    for &b_pk in &right_schema.pk_cols { pair_pk_cols.push(k + left_n + b_pk); }
    let zero_tcs = vec![0u8; pair_pk];
    let rekey = cb.map_reindex(merged, &pair_pk_cols, &zero_tcs, build_reindex_program(&union_schema));

    // Re-key output layout: `[_pair_pk × pair_pk (PK), _join_pk × k, A cols, B cols]`.
    // The user projection drops the k `_join_pk` slots (they DIFFER per term and
    // must not survive into the exchanged/consolidated output) and selects user
    // columns from A/B. A/B columns start at this payload offset.
    let payload_offset = pair_pk + k;
    let combined_coldef = |idx: usize| -> ColumnDef {
        if idx < left_n { left_schema.columns[idx].clone() }
        else { right_schema.columns[idx - left_n].clone() }
    };

    // Leading output PK columns: A's then B's source-PK column types (the re-key
    // self-derive output type), non-nullable, `_pair_pk_{slot}` numbered across
    // both sides.
    let pair_pk_coldefs: Vec<ColumnDef> = left_schema.pk_cols.iter().map(|&c| (left_schema, c))
        .chain(right_schema.pk_cols.iter().map(|&c| (right_schema, c)))
        .enumerate()
        .map(|(slot, (schema, c))| ColumnDef {
            name: format!("_pair_pk_{slot}"),
            type_code: schema.columns[c].type_code.reindex_output_type(),
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        })
        .collect();

    // User projection via the shared helper. Always applied (it must drop the
    // `_join_pk` slots, which DIFFER per term); keeps the pair-PK region and
    // selects user columns from A/B as the payload.
    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let (final_cols, final_projection) = build_join_view_projection(
        &select.projection, alias_map, &pair_pk_coldefs, left_n + right_n, payload_offset,
        combined_coldef, "range JOIN view",
    )?;

    let projected = cb.map(rekey, &final_projection);
    // ExchangeShard on EXACTLY the pair-PK columns in order — must equal view_pk,
    // so the GroupKey scatter routes by `partition_for_pk_bytes` identically to
    // the view scan/seek (the compound-key alignment invariant).
    let pair_pk_idxs: Vec<usize> = (0..pair_pk).collect();
    let sharded = cb.shard(projected, &pair_pk_idxs);
    cb.sink(sharded);
    let circuit = cb.build();

    let view_pk: Vec<u32> = (0..pair_pk as u32).collect();
    debug_assert_eq!(pair_pk_idxs, view_pk.iter().map(|&c| c as usize).collect::<Vec<_>>(),
        "range join: ExchangeShard cols must equal view_pk in strict order");
    if !is_wildcard {
        reject_duplicate_column_names(
            final_cols.iter().map(|c| c.name.as_str()), "range join view")?;
    }
    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Build a join view's user projection: the leading PK columns followed by the
/// selected payload columns, plus the parallel union-column index list the
/// output `Map` projects. Shared by the equi (`execute_create_join_view`) and
/// range (`build_range_join_view`) builders, which differ only in their PK region
/// and where the A‖B payload columns sit in the union layout:
///   - `leading_cols`: the synthesized PK columns, cloned verbatim into the output
///     schema (`_join_pk` slots for equi, `_pair_pk` slots for range).
///   - `coldef(i)`: maps a combined A‖B column index (`0..n_combined`) to its
///     output `ColumnDef` (equi reads the pre-built, LEFT-join-nullable-adjusted
///     list; range reads the raw source schemas — INNER only).
///   - `payload_offset`: the union-column index of combined column 0.
///
/// A lone `SELECT *` flows through the `Wildcard` arm, so neither caller needs a
/// separate wildcard fast path. `label` names the view kind in error messages.
fn build_join_view_projection(
    projection:     &[SelectItem],
    alias_map:      &JoinAliasMap,
    leading_cols:   &[ColumnDef],
    n_combined:     usize,
    payload_offset: usize,
    coldef:         impl Fn(usize) -> ColumnDef,
    label:          &str,
) -> Result<(Vec<ColumnDef>, Vec<usize>), GnitzSqlError> {
    let mut cols: Vec<ColumnDef> = leading_cols.to_vec();
    let mut proj: Vec<usize> = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = resolve_unqualified_column(&ident.value, alias_map)?;
                cols.push(coldef(idx));
                proj.push(payload_offset + idx);
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                let idx = resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)?;
                cols.push(coldef(idx));
                proj.push(payload_offset + idx);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let idx = match expr {
                    Expr::Identifier(ident) =>
                        resolve_unqualified_column(&ident.value, alias_map)?,
                    Expr::CompoundIdentifier(parts) if parts.len() == 2 =>
                        resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)?,
                    _ => return Err(GnitzSqlError::Unsupported(
                        format!("{label}: only column references supported in AS clause"))),
                };
                let mut col = coldef(idx);
                col.name = alias.value.clone();
                cols.push(col);
                proj.push(payload_offset + idx);
            }
            SelectItem::Wildcard(_) => {
                for i in 0..n_combined {
                    cols.push(coldef(i));
                    proj.push(payload_offset + i);
                }
            }
            _ => return Err(GnitzSqlError::Unsupported(
                format!("unsupported SELECT item in {label}"))),
        }
    }
    Ok((cols, proj))
}

/// Multi-column NULL predicate for a Filter over a composite equijoin key,
/// reusing the WHERE-clause bound-expr → ExprProgram path so each column index
/// maps to its payload byte. A composite key is NULL — and matches nothing
/// (SQL 3VL) — iff ANY component is NULL.
///   want_null == false: keep rows whose key is fully defined →
///                       `c0 IS NOT NULL AND … AND ck IS NOT NULL`.
///   want_null == true : keep rows whose key is NULL (LEFT-join bypass) →
///                       `c0 IS NULL OR … OR ck IS NULL`.
/// The two are exact De Morgan complements, so the LEFT-join match/bypass split
/// partitions the preserved side with no gap and no double-count. `cols` is
/// non-empty (k ≥ 1 is guaranteed by extract_join_predicates). At k = 1 this emits
/// exactly the single-column IsNotNull/IsNull program, so existing single-key
/// plans are byte-identical.
fn multi_null_filter_prog(
    cols:      &[usize],
    schema:    &Schema,
    want_null: bool,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    // Caller invariant: cols is non-empty (at least one join key column) AND at
    // least one entry is nullable (the outer guard in execute_create_join_view
    // ensures both). Guard here so future callers fail loudly rather than
    // panicking at cols[0].
    if cols.is_empty() {
        return Err(GnitzSqlError::Plan(
            "multi_null_filter_prog: column list cannot be empty".into(),
        ));
    }
    // Only nullable columns can ever satisfy IsNull or fail IsNotNull, so drop the
    // NOT NULL columns to elide tautological (want_null=false) / contradictory
    // (want_null=true) filter instructions. The caller (execute_create_join_view)
    // only reaches this with ≥ 1 nullable key column, so `nullable` is non-empty on
    // every real path; the `is_empty` fallback to the unfiltered `cols` still
    // degrades correctly should that ever change — with every key NOT NULL,
    // `c IS NOT NULL` is a tautology (keep all rows) and `c IS NULL` a contradiction
    // (drop all), exactly right when no key can be NULL.
    let nullable: Vec<usize> = cols.iter()
        .copied()
        .filter(|&c| schema.columns[c].is_nullable)
        .collect();
    let cols = if nullable.is_empty() { cols } else { &nullable[..] };

    let leaf = |c: usize| if want_null { BoundExpr::IsNull(c) } else { BoundExpr::IsNotNull(c) };
    let op = if want_null { BinOp::Or } else { BinOp::And };
    let mut expr = leaf(cols[0]);
    for &c in &cols[1..] {
        expr = BoundExpr::BinOp(Box::new(expr), op, Box::new(leaf(c)));
    }
    let mut eb = ExprBuilder::new();
    let (r, _) = compile_bound_expr(&expr, schema, &mut eb)?;
    Ok(eb.build(r))
}

/// Build a reindex ExprProgram that copies all columns as payload. Arity-
/// independent: it copies every source column to payload offsets `0..n`, and
/// `reindex_output_schema` places those payload columns at physical indices
/// `k..k+n` regardless of the key arity `k` (the `k` PK slots precede them), so
/// the payload offsets never shift with the number of key columns.
fn build_reindex_program(schema: &Schema) -> gnitz_core::ExprProgram {
    let mut eb = ExprBuilder::new();
    for (ci, col) in schema.columns.iter().enumerate() {
        let tc = col.type_code as u32;
        eb.copy_col(tc, ci as u32, ci as u32);
    }
    eb.build(0) // result_reg unused — COPY_COL writes directly
}

/// Reject a float column used as any hashed key — a GROUP BY grouping key, a
/// DISTINCT/set-op row identity, or an equijoin key. All of these hash the
/// column's raw IEEE-754 bytes, so -0.0/+0.0 and distinct-NaN bit patterns split
/// values that are numerically equal (and route them to distinct workers). `role`
/// names the offending clause for the error message.
fn reject_float_key(col: &ColumnDef, role: &str) -> Result<(), GnitzSqlError> {
    if col.type_code.is_float() {
        return Err(GnitzSqlError::Unsupported(format!(
            "{role}: float column '{}' cannot be a key \
             (IEEE-754 -0.0/+0.0 and NaN break key equality)",
            col.name)));
    }
    Ok(())
}

/// Validate one equijoin key pair and return the pair's common reindex output
/// type `T`. Float keys are rejected outright (IEEE-754 -0.0/+0.0 compare
/// byte-unequal and NaN has no canonical form). A German string (STRING/BLOB) may
/// only join another German string: it reindexes to a 16-byte content hash, which
/// is byte-incompatible with the native U128/UUID encoding even though both
/// collapse to the U128 output type. The remaining pairs are resolved by
/// `join_key_common_type`, which promotes integers of different widths (and
/// different sign classes, as long as the unsigned side is ≤ 8 bytes (`U64`)) to a
/// common type that faithfully holds both ranges, so the two reindex sides
/// co-partition byte-for-byte and the `_join_pk` catalog stride matches both. Only
/// a cross-sign pair whose unsigned side is 128-bit (`U128`/`UUID`) stays
/// rejected — its faithful common type is a signed-256 type that does not exist.
fn validate_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<u8, GnitzSqlError> {
    for col in [left, right] {
        reject_float_key(col, "JOIN ON")?;
    }
    // STRING/BLOB reindex to a 16-byte XXH3 content hash; U128/UUID reindex to the
    // 16-byte native value. Both collapse to the U128 output type, so
    // `join_key_common_type` cannot tell them apart — but a content hash never
    // equals a native integer, so the join would silently match nothing.
    if left.type_code.is_german_string() != right.type_code.is_german_string() {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN ON: cannot equijoin string/blob column '{}' ({:?}) with non-string \
             column '{}' ({:?}); a string content hash never matches a native key",
            left.name, left.type_code, right.name, right.type_code)));
    }
    left.type_code.join_key_common_type(right.type_code)
        .map(|t| t as u8)
        .ok_or_else(|| GnitzSqlError::Unsupported(format!(
            "JOIN ON: join key columns '{}' ({:?}) and '{}' ({:?}) cannot co-partition; \
             a cross-sign pair whose unsigned side is 128-bit (e.g. DECIMAL(38,0)/UUID \
             joined with a signed integer) needs a signed-256 type that does not exist",
            left.name, left.type_code, right.name, right.type_code)))
}

/// Validate the range conjunct's key pair and return its common reindex output
/// type `T`. A range bound must be order-preserving: STRING/BLOB reindex to a
/// 16-byte content hash that is equality-correct but NOT order-preserving, so
/// they are rejected here (they remain legal in the equality prefix). Floats are
/// rejected by `validate_join_key_pair`, which then resolves the common integer
/// type via `join_key_common_type` (cross-sign promotion included).
fn validate_range_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<u8, GnitzSqlError> {
    for col in [left, right] {
        if col.type_code.is_german_string() {
            return Err(GnitzSqlError::Unsupported(format!(
                "range join key column '{}' ({:?}): a string/blob content hash is not \
                 order-preserving and cannot bound a range conjunct",
                col.name, col.type_code)));
        }
    }
    validate_join_key_pair(left, right)
}

/// Map a SQL comparison operator to its `RangeRel`, or `None` for a non-range
/// operator (`=`, `AND`, arithmetic, …).
fn sql_binop_to_range_rel(op: &BinaryOperator) -> Option<RangeRel> {
    match op {
        BinaryOperator::Lt   => Some(RangeRel::Lt),
        BinaryOperator::LtEq => Some(RangeRel::Le),
        BinaryOperator::Gt   => Some(RangeRel::Gt),
        BinaryOperator::GtEq => Some(RangeRel::Ge),
        _ => None,
    }
}

/// The order-reversing converse of a `RangeRel` (`x OP y` ⟺ `y converse(OP) x`).
/// Used both to canonicalize a right-table-first range conjunct to left-first and
/// to derive term AB's rel from the canonical OP (§3 table).
fn converse_rel(r: RangeRel) -> RangeRel {
    match r {
        RangeRel::Lt => RangeRel::Gt,
        RangeRel::Le => RangeRel::Ge,
        RangeRel::Gt => RangeRel::Lt,
        RangeRel::Ge => RangeRel::Le,
    }
}

/// The single range conjunct of a band/range join, canonicalized to
/// `left_col OP right_col` (both table-relative). `op` is the canonical OP as a
/// `RangeRel`; `tc` is the pair's common reindex output type.
#[derive(Debug)]
struct RangeConjunct {
    left_col:  usize,
    right_col: usize,
    op:        RangeRel,
    tc:        u8,
}

/// The full ON-clause classification: per-position-paired equality-prefix key
/// columns (`left_cols`, `right_cols`) with their per-pair common reindex output
/// type (`target_tcs`), plus an optional single range conjunct.
type JoinPredicates = (Vec<usize>, Vec<usize>, Vec<u8>, Option<RangeConjunct>);

/// Alias map for JOIN column resolution: alias/name → (table_id, schema,
/// global column offset). `Rc<Schema>` so the per-side schemas resolved by the
/// binder are shared, not deep-cloned, into the map.
type JoinAliasMap = HashMap<String, (u64, Rc<Schema>, usize)>;

/// Classify a JOIN ON clause into its equality-prefix pairs and an optional
/// single range conjunct. A pure superset of the old equi-only extraction: with
/// zero range conjuncts the equality pairs (and therefore the equi circuit and
/// its serialization) are byte-for-byte unchanged. Returns
/// `(left_eq, right_eq, eq_tcs, range)`, all table-relative and position-paired.
fn extract_join_predicates(
    on_expr:      &Expr,
    left_schema:  &Schema,
    right_schema: &Schema,
    alias_map:    &JoinAliasMap,
) -> Result<JoinPredicates, GnitzSqlError> {
    let mut left_cols  = Vec::new();
    let mut right_cols = Vec::new();
    let mut target_tcs = Vec::new();
    let mut range: Option<RangeConjunct> = None;
    collect_join_predicates(on_expr, left_schema, right_schema, alias_map,
                            &mut left_cols, &mut right_cols, &mut target_tcs, &mut range)?;
    if left_cols.is_empty() && range.is_none() {
        return Err(GnitzSqlError::Bind(
            "JOIN ON must have at least one equijoin or range predicate".into()));
    }
    // Reindex-slot arity cap: each equality pair plus the optional range slot
    // becomes one synthetic `_join_pk` PK-list slot, and the codec holds at most
    // PK_LIST_MAX_COLS. Reject a wider ON here as a clean planner error rather than
    // a `pack_pk_cols` panic at registration. (The output pair-PK has its own cap,
    // checked in the range circuit builder.)
    let slots = left_cols.len() + range.is_some() as usize;
    if slots > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(if range.is_none() {
            format!("JOIN ON: at most {} equijoin key columns are supported (got {})",
                gnitz_core::PK_LIST_MAX_COLS, left_cols.len())
        } else {
            format!("range JOIN ON: at most {} join key columns (equality prefix + \
                     range) are supported (got {})", gnitz_core::PK_LIST_MAX_COLS, slots)
        }));
    }
    Ok((left_cols, right_cols, target_tcs, range))
}

#[allow(clippy::too_many_arguments)]
fn collect_join_predicates(
    expr:         &Expr,
    left_schema:  &Schema,
    right_schema: &Schema,
    alias_map:    &JoinAliasMap,
    left_cols:    &mut Vec<usize>,
    right_cols:   &mut Vec<usize>,
    target_tcs:   &mut Vec<u8>,
    range:        &mut Option<RangeConjunct>,
) -> Result<(), GnitzSqlError> {
    // The one-range-conjunct rule, named in the rejection of anything that is not
    // an AND of column equijoins plus at most one column range conjunct.
    let unsupported = || GnitzSqlError::Unsupported(
        "JOIN ON: only an AND-conjunction of column equijoins plus at most one \
         range conjunct (<, <=, >, >=) between the two tables is supported".into());
    match expr {
        // Parentheses: `ON (a.x = b.x) AND a.y = b.y`, `ON (a.x = b.x AND a.y = b.y)`.
        // sqlparser wraps a parenthesized sub-expression in Expr::Nested; unwrap it
        // so grouping never changes which predicates are extracted. This mirrors the
        // WHERE binder (`binder.rs` Expr::Nested arm) and the HAVING path
        // (`bind_having_expr`), both of which already unwrap Nested.
        Expr::Nested(inner) => {
            collect_join_predicates(inner, left_schema, right_schema, alias_map,
                                    left_cols, right_cols, target_tcs, range)?;
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                collect_join_predicates(left,  left_schema, right_schema, alias_map,
                                        left_cols, right_cols, target_tcs, range)?;
                collect_join_predicates(right, left_schema, right_schema, alias_map,
                                        left_cols, right_cols, target_tcs, range)?;
            }
            BinaryOperator::Eq => {
                let l = resolve_join_col_ref(left,  alias_map)?;  // global index
                let r = resolve_join_col_ref(right, alias_map)?;  // global index
                let left_n = left_schema.columns.len();
                let (li, ri) = if l < left_n && r >= left_n {
                    (l, r - left_n)
                } else if r < left_n && l >= left_n {
                    (r, l - left_n)
                } else {
                    return Err(GnitzSqlError::Bind(
                        "JOIN ON: each side of = must reference a different table".into()));
                };
                // Drop an exact-duplicate pair (`a.x = b.x AND a.x = b.x`, or the
                // same pair with the sides swapped). Both produce byte-identical key
                // slots; keeping them only widens the synthetic PK and can spuriously
                // trip the arity cap. A pair sharing one column but not the other
                // (`a.x = b.x AND a.x = b.y`) is distinct — different `ri` — and kept.
                if left_cols.iter().zip(right_cols.iter()).any(|(&pl, &pr)| pl == li && pr == ri) {
                    return Ok(());
                }
                // Push T only on the same path that pushes (li, ri) — after the dup
                // early-return — so the three vectors stay parallel.
                let t = validate_join_key_pair(&left_schema.columns[li], &right_schema.columns[ri])?;
                left_cols.push(li);
                right_cols.push(ri);
                target_tcs.push(t);
            }
            _ => {
                // Range conjunct (`<`, `<=`, `>`, `>=`) — exactly one allowed.
                let Some(rel) = sql_binop_to_range_rel(op) else { return Err(unsupported()); };
                let l = resolve_join_col_ref(left,  alias_map)?;
                let r = resolve_join_col_ref(right, alias_map)?;
                let left_n = left_schema.columns.len();
                // Canonicalize to `left_col OP right_col`: if the right table's
                // column is the LEFT operand (`b.y > a.x`), swap operands and take
                // the converse operator (`a.x < b.y`).
                let (li, ri, canon_op) = if l < left_n && r >= left_n {
                    (l, r - left_n, rel)
                } else if r < left_n && l >= left_n {
                    (r, l - left_n, converse_rel(rel))
                } else {
                    return Err(GnitzSqlError::Bind(
                        "JOIN ON: each side of a range comparison must reference a different table".into()));
                };
                if range.is_some() {
                    return Err(GnitzSqlError::Unsupported(
                        "JOIN ON: at most one range conjunct (<, <=, >, >=) is supported".into()));
                }
                let t = validate_range_join_key_pair(
                    &left_schema.columns[li], &right_schema.columns[ri])?;
                *range = Some(RangeConjunct { left_col: li, right_col: ri, op: canon_op, tc: t });
            }
        },
        _ => return Err(unsupported()),
    }
    Ok(())
}

/// Resolve a column reference in a JOIN ON clause to a global column index.
fn resolve_join_col_ref(
    expr:         &Expr,
    alias_map:    &JoinAliasMap,
) -> Result<usize, GnitzSqlError> {
    match expr {
        Expr::Nested(inner) => resolve_join_col_ref(inner, alias_map),
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

/// Reduce-output column index for a group column `src_col`, mirroring the
/// reduce output schema and the engine's `build_reduce_output_schema`:
///
/// * `group_set_eq_pk` — the PK region holds the source PK columns in source-PK
///   order; locate `src_col` there.
/// * `single_col_natural_pk` — the lone group col is the PK at index 0.
/// * synthetic `_group_pk` — group cols follow the U128 PK, in GROUP BY order.
///
/// The branch order mirrors `build_reduce_output_schema`'s decision tree
/// (`group_set_eq_pk` → `single_col_natural_pk` → synthetic) so the helper and
/// the reduce-schema build cannot drift. When both natural-PK flags hold
/// (grouping by a single-column PK) both branches return 0, so correctness does
/// not depend on the order.
fn group_col_reduce_pos(
    src_col:               usize,
    group_set_eq_pk:       bool,
    single_col_natural_pk: bool,
    source_schema:         &Schema,
    group_col_indices:     &[usize],
) -> usize {
    if group_set_eq_pk {
        source_schema.pk_cols.iter().position(|&pi| pi == src_col)
            .expect("group_set_eq_pk: every group col is a source PK col")
    } else if single_col_natural_pk {
        0
    } else {
        1 + group_col_indices.iter().position(|&gi| gi == src_col).unwrap()
    }
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
    // The compound-PK source guard is intentionally NOT applied here: the engine
    // reduce output already emits a full compound natural-PK region
    // (`build_reduce_output_schema` walks `pk_columns()` for `group_set_eq_pk`),
    // the helpers below map group columns through `group_col_reduce_pos`, and the
    // co-partition analyzers now compare the full PK sequence — so a reduce that
    // shards by one component of a compound PK gets the exchange it needs.

    // 2. Parse GROUP BY → group column indices
    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        _ => return Err(GnitzSqlError::Unsupported("GROUP BY: only expression list supported".to_string())),
    };
    let mut group_col_indices: Vec<usize> = Vec::new();
    for ge in group_exprs {
        match ge {
            Expr::Identifier(id) => {
                let idx = find_unique_column(&source_schema.columns, &id.value)?
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("GROUP BY column '{}' not found", id.value)
                    ))?;
                reject_float_key(&source_schema.columns[idx], "GROUP BY")?;
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
                                && (!is_numeric || matches!(tc,
                                    TypeCode::U128 | TypeCode::UUID | TypeCode::I128))
                            {
                                return Err(GnitzSqlError::Bind(format!(
                                    "{:?} is not supported on column type {:?} ('{}')",
                                    func, tc, source_schema.columns[c].name,
                                )));
                            }
                        }
                        AggFunc::Min | AggFunc::Max => {
                            if matches!(tc, TypeCode::String | TypeCode::Blob
                                | TypeCode::U128 | TypeCode::UUID | TypeCode::I128) {
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
                let is_avg = push_agg_specs(*func, src_col, &mut agg_specs)?;
                let out_name = alias.unwrap_or_else(|| {
                    let prefix = match func {
                        AggFunc::Count | AggFunc::CountNonNull => "_count",
                        AggFunc::Sum => "_sum",
                        AggFunc::Min => "_min",
                        AggFunc::Max => "_max",
                        AggFunc::Avg => "_avg",
                    };
                    format!("{prefix}{idx}")
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
    // The aggregate columns are pushed next, so they start at the current width
    // of the reduce schema: the PK region plus, on the synthetic path only, the
    // group cols carried as payload.
    let agg_col_offset = reduce_schema_cols.len();
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

    // REDUCE — always use multi-agg path.
    //
    // For `group_set_eq_pk`, shard/reindex the reduce by the group columns in
    // source-PK (schema) order, not the user's GROUP BY order. The groups are
    // identical under any permutation of the PK (each group is a PK singleton),
    // and `build_reduce_output_schema` emits the output PK in source-PK order
    // regardless — so this only normalizes the shard key. Without it a permuted
    // grouping (e.g. `GROUP BY pk1, pk0`) shards by a non-PK-order key: the
    // co-partition analyzer (correctly) declines to skip the exchange, the
    // shuffle hash-routes by `[pk1, pk0]`, and the reduce output lands
    // partitioned by `hash(pk1, pk0)` rather than by the view's declared PK
    // `(pk0, pk1)` — so the multi-worker gather drops the rows that hashed to a
    // different worker. Sharding in PK order keeps the reduce co-partitioned with
    // the source (the exchange is skipped, or routes by `partition_for_pk_bytes`),
    // so the view stays partitioned by its real PK. The non-eq-pk paths keep the
    // user order: their synthetic/single-natural PK and reduce layout depend on
    // it (`group_col_reduce_pos`'s synthetic branch indexes by GROUP BY order).
    let reduce_group_cols: Vec<usize> = if group_set_eq_pk {
        source_schema.pk_cols.clone()
    } else {
        group_col_indices.clone()
    };
    let reduced = cb.reduce_multi(filtered, &reduce_group_cols, &agg_specs);

    // 6. Post-reduce MAP: project group cols + compute aggregates (AVG = SUM/COUNT)
    //    Reduce output: [pk, (group_cols...), agg0, agg1, ...]
    //    MAP inherits PK from input; ExprProgram writes payload columns only.
    //    Natural-PK group cols are part of that inherited PK region — the alias
    //    renames the PK slot in place (no payload copy). Synthetic-PK group cols
    //    are written to payload. Output: [pk(inherited, renamed), …payload…].
    let mut post_map_eb = ExprBuilder::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    let mut payload_idx: u32 = 0;

    // PK region in the output schema (inherited by MAP, not written by ExprProgram):
    // the full source PK in source-PK order for a compound natural PK; the lone
    // leading column for the single-natural / synthetic paths. `reduce_schema.pk_cols`
    // is dense (`0..pk_count`), so the leading columns are exactly the PK region.
    out_cols.extend(
        reduce_schema.columns[..reduce_schema.pk_cols.len()].iter().cloned()
    );

    // Tracks which PK slots a natural-PK group col has already renamed in place,
    // so a group column selected twice with a different alias falls back to a
    // payload COPY_COL copy rather than silently overwriting the first alias.
    let mut pk_renamed = vec![false; reduce_schema.pk_cols.len()];

    for si in &select_items {
        match si {
            GroupBySelectItem::GroupCol { src_col, name } => {
                // Find group col position in reduce output (routed through the
                // shared helper so SELECT and HAVING cannot drift on the
                // source-PK-order mapping a permuted `group_set_eq_pk` needs).
                let reduce_col = group_col_reduce_pos(
                    *src_col, group_set_eq_pk, single_col_natural_pk,
                    &source_schema, &group_col_indices);
                let tc = reduce_schema.columns[reduce_col].type_code;
                // On both natural-PK paths `group_col_reduce_pos` returns a position
                // within the dense PK region, so on the `is_natural` branch
                // `reduce_col` always indexes a real PK slot in `out_cols` /
                // `pk_renamed` (asserted below). `&&` short-circuits, so the index
                // is only evaluated once `is_natural` holds — the synthetic path's
                // larger `reduce_col` never reaches it.
                let is_natural = group_set_eq_pk || single_col_natural_pk;
                if is_natural && !pk_renamed[reduce_col] {
                    // First projection of this group column: rename the PK slot
                    // in-place. The MAP inherits the PK region verbatim — no
                    // COPY_COL needed, and the column must not be re-pushed to
                    // out_cols (it is already there from the PK-region extend).
                    debug_assert!(
                        reduce_col < reduce_schema.pk_cols.len(),
                        "GROUP BY natural-PK rename: reduce_col {reduce_col} is not a PK slot \
                         (pk_cols.len() = {})", reduce_schema.pk_cols.len(),
                    );
                    out_cols[reduce_col].name = name.clone();
                    pk_renamed[reduce_col] = true;
                } else {
                    // Synthetic-PK path, or second projection of the same group column.
                    post_map_eb.copy_col(tc as u32, reduce_col as u32, payload_idx);
                    out_cols.push(ColumnDef {
                        name: name.clone(),
                        type_code: tc,
                        // Natural-PK path (group_set_eq_pk / single_col_natural_pk):
                        // the source col is non-nullable. Synthetic-PK path: propagate
                        // source nullability — nothing forces NOT NULL.
                        is_nullable: source_schema.columns[*src_col].is_nullable,
                        fk_table_id: 0,
                        fk_col_idx:  0,
                    });
                    payload_idx += 1;
                }
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
                        // SUM/MIN/MAX emit NULL for an all-NULL group (emit.rs sets
                        // the null bit when the accumulator is_zero(), since it skips
                        // NULL inputs). COUNT/COUNT_NON_NULL always return an integer
                        // (0, never NULL). Match emit.rs so a schema-driven decoder
                        // reads NULL instead of raw zero bytes.
                        is_nullable: matches!(m.agg_func, AggFunc::Sum | AggFunc::Min | AggFunc::Max),
                        fk_table_id: 0, fk_col_idx: 0,
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
            source_schema:         &source_schema,
            group_col_indices:     &group_col_indices,
            group_set_eq_pk,
            single_col_natural_pk,
            agg_mappings:          &agg_mappings,
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

    // A SELECT that names the same group column twice (e.g. `k, k AS k2` is fine,
    // but `k, k` collides) must be caught cleanly rather than registering a view
    // with duplicate column names.
    reject_duplicate_column_names(
        out_cols.iter().map(|c| c.name.as_str()),
        "GROUP BY view",
    )?;

    // The view's physical PK is the reduce output's PK region: the full source
    // PK (source-PK order) for a compound natural PK, else the lone leading
    // column. `reduce_schema.pk_cols` is dense (`0..pk_count`).
    let view_pk: Vec<u32> = (0..reduce_schema.pk_cols.len() as u32).collect();
    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols, &view_pk)
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
            format!("HAVING: function '{name}' not supported")
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
) -> Result<bool, GnitzSqlError> {
    // Every aggregate except COUNT(*) needs a column argument, which the specs
    // below unwrap. Validating here — the single source of truth for spec
    // layout — covers both the SELECT-list and HAVING callers, so neither needs
    // its own wildcard guard and a future caller cannot reintroduce the panic.
    if !matches!(agg_func, AggFunc::Count) && arg_col.is_none() {
        return Err(GnitzSqlError::Plan(format!(
            "{agg_func:?} requires an argument column; only COUNT(*) accepts a wildcard"
        )));
    }
    Ok(match agg_func {
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
    })
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
) -> Result<(), GnitzSqlError> {
    let out_type = agg_result_type(agg_func, arg_col, source_schema);
    let agg_idx = agg_mappings.len();
    let start = agg_specs.len();
    let is_avg = push_agg_specs(agg_func, arg_col, agg_specs)?;
    agg_mappings.push(AggMapping {
        specs_start: start, is_avg,
        output_name: format!("_having_agg{agg_idx}"),
        output_type: out_type, agg_func, arg_col,
    });
    Ok(())
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
                append_having_agg(agg_func, arg_col, source_schema, agg_specs, agg_mappings)?;
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
    source_schema:         &'a Schema,
    group_col_indices:     &'a [usize],
    group_set_eq_pk:       bool,
    single_col_natural_pk: bool,
    agg_mappings:          &'a [AggMapping],
    agg_col_offset:        usize,
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
            let src = find_unique_column(&ctx.source_schema.columns, col_name)?
                .ok_or_else(|| GnitzSqlError::Bind(
                    format!("HAVING: column '{col_name}' not found")
                ))?;
            // HAVING may only reference grouped columns. The old `position()`
            // bound a local solely to compute `1 + gpos`, which now lives inside
            // `group_col_reduce_pos`; binding it here would be a dead local.
            if !ctx.group_col_indices.contains(&src) {
                return Err(GnitzSqlError::Bind(format!(
                    "HAVING: column '{col_name}' must appear in GROUP BY or an aggregate function")));
            }
            let reduce_col = group_col_reduce_pos(
                src, ctx.group_set_eq_pk, ctx.single_col_natural_pk,
                ctx.source_schema, ctx.group_col_indices);
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
                    format!("HAVING: operator {op:?} not supported")
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
                        Err(GnitzSqlError::Bind(format!("HAVING: invalid number {n}")))
                    }
                }
                _ => Err(GnitzSqlError::Unsupported("HAVING: unsupported value type".to_string())),
            }
        }
        Expr::Nested(inner) => bind_having_expr(inner, ctx),
        _ => Err(GnitzSqlError::Unsupported(
            format!("HAVING: unsupported expression {expr:?}")
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
                    let idx = find_unique_column(&schema.columns, &id.value)?
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
        reject_float_keys(source_schema, &indices)?;
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
    // Single chokepoint: every projected column lands in `indices`, so one pass
    // here rejects a float row-identity key regardless of which SELECT-item arm
    // produced it (a new arm is covered automatically).
    reject_float_keys(source_schema, &indices)?;
    Ok((indices, out_cols))
}

/// Reject any float column among `indices` as a row-identity key. See
/// [`reject_float_key`] for why floats break set/DISTINCT membership.
fn reject_float_keys(source_schema: &Schema, indices: &[usize]) -> Result<(), GnitzSqlError> {
    for &ci in indices {
        reject_float_key(&source_schema.columns[ci], "SELECT DISTINCT / set operation")?;
    }
    Ok(())
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
            format!("set operation {op:?} not supported")
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
    reject_duplicate_column_names(
        out_cols_final.iter().map(|c| c.name.as_str()),
        "set operation view",
    )?;

    // Set-op views emit a synthetic single-column content-hash PK at slot 0.
    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols_final, &[0])
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
    reject_duplicate_column_names(
        out_cols.iter().map(|c| c.name.as_str()),
        "SELECT DISTINCT view",
    )?;

    // DISTINCT views emit a synthetic single-column content-hash PK at slot 0.
    client.create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols, &[0])
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Multi-column equijoin key extraction / validation ────────────────────
    //
    // These exercise the k ≥ 2 logic directly: extract_join_predicates and
    // validate_join_key_pair return their result *before* the `k > 1` planner
    // gate in execute_create_join_view, so a composite key is fully testable
    // here even though CREATE VIEW still rejects it end-to-end.

    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn col(name: &str, tc: TypeCode, nullable: bool) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: tc, is_nullable: nullable,
                    fk_table_id: 0, fk_col_idx: 0 }
    }

    fn parse_on(src: &str) -> Expr {
        Parser::new(&GenericDialect {})
            .try_with_sql(src).unwrap()
            .parse_expr().unwrap()
    }

    /// Build the (left_schema, right_schema, alias_map) trio the way
    /// `execute_create_join_view` does, for aliases `a` (left) / `b` (right).
    fn join_ctx(
        left:  Vec<ColumnDef>,
        right: Vec<ColumnDef>,
    ) -> (Schema, Schema, JoinAliasMap) {
        let left_n = left.len();
        let left_schema  = Schema { columns: left,  pk_cols: vec![0] };
        let right_schema = Schema { columns: right, pk_cols: vec![0] };
        let mut am = HashMap::new();
        am.insert("a".to_string(), (1u64, Rc::new(left_schema.clone()), 0usize));
        am.insert("b".to_string(), (2u64, Rc::new(right_schema.clone()), left_n));
        (left_schema, right_schema, am)
    }

    fn extract(
        on: &str,
        left:  Vec<ColumnDef>,
        right: Vec<ColumnDef>,
    ) -> Result<(Vec<usize>, Vec<usize>), GnitzSqlError> {
        let (ls, rs, am) = join_ctx(left, right);
        extract_join_predicates(&parse_on(on), &ls, &rs, &am).map(|(l, r, _, _)| (l, r))
    }

    /// The equi-join half of `JoinPredicates` (no range conjunct): paired key
    /// columns + per-pair common reindex type. Named for legible helper returns.
    type EquiKeys = (Vec<usize>, Vec<usize>, Vec<u8>);

    /// Like `extract` but also returns the per-pair common reindex output type
    /// `T`, for cross-width promotion assertions. Asserts no range conjunct.
    fn extract_with_tcs(
        on: &str,
        left:  Vec<ColumnDef>,
        right: Vec<ColumnDef>,
    ) -> Result<EquiKeys, GnitzSqlError> {
        let (ls, rs, am) = join_ctx(left, right);
        extract_join_predicates(&parse_on(on), &ls, &rs, &am).map(|(l, r, t, range)| {
            assert!(range.is_none(), "expected no range conjunct");
            (l, r, t)
        })
    }

    /// Full classification including the optional range conjunct.
    fn extract_full(
        on: &str,
        left:  Vec<ColumnDef>,
        right: Vec<ColumnDef>,
    ) -> Result<JoinPredicates, GnitzSqlError> {
        let (ls, rs, am) = join_ctx(left, right);
        extract_join_predicates(&parse_on(on), &ls, &rs, &am)
    }

    fn two_u64() -> Vec<ColumnDef> {
        vec![col("x", TypeCode::U64, false), col("y", TypeCode::U64, false)]
    }

    #[test]
    fn extract_keys_per_table_order_side_independent() {
        // Each `=` is resolved regardless of which side the left/right ref appears
        // on: `b.y = a.y` keys the same as `a.y = b.y`.
        let r = extract("a.x = b.x AND b.y = a.y", two_u64(), two_u64()).unwrap();
        assert_eq!(r, (vec![0, 1], vec![0, 1]));
    }

    #[test]
    fn extract_keys_parentheses_unwrap() {
        // Expr::Nested grouping never changes which equijoins are extracted.
        let base = extract("a.x = b.x AND a.y = b.y", two_u64(), two_u64()).unwrap();
        assert_eq!(base, (vec![0, 1], vec![0, 1]));
        assert_eq!(extract("(a.x = b.x) AND a.y = b.y", two_u64(), two_u64()).unwrap(), base);
        assert_eq!(extract("(a.x = b.x AND a.y = b.y)", two_u64(), two_u64()).unwrap(), base);
        // Parenthesized column ref on one side resolves too (single pair).
        assert_eq!(extract("(a.x) = b.x", two_u64(), two_u64()).unwrap(), (vec![0], vec![0]));
    }

    #[test]
    fn extract_keys_dedup_exact_duplicate() {
        // Exact duplicate and sides-swapped duplicate both collapse to one column.
        assert_eq!(extract("a.x = b.x AND a.x = b.x", two_u64(), two_u64()).unwrap(),
                   (vec![0], vec![0]));
        assert_eq!(extract("a.x = b.x AND b.x = a.x", two_u64(), two_u64()).unwrap(),
                   (vec![0], vec![0]));
        // Overlapping-but-distinct (shares left col, differs on right) keeps both.
        assert_eq!(extract("a.x = b.x AND a.x = b.y", two_u64(), two_u64()).unwrap(),
                   (vec![0, 0], vec![0, 1]));
    }

    #[test]
    fn extract_keys_single_pair() {
        assert_eq!(extract("a.x = b.y", two_u64(), two_u64()).unwrap(), (vec![0], vec![1]));
    }

    #[test]
    fn extract_keys_rejections() {
        // Float key → Unsupported (from validate_join_key_pair).
        let fl = vec![col("x", TypeCode::F64, false), col("y", TypeCode::U64, false)];
        assert!(matches!(extract("a.x = b.x", fl.clone(), fl).unwrap_err(),
                         GnitzSqlError::Unsupported(_)));
        // Cross-sign pair U128 = I64 → Unsupported (needs a signed-256 type that
        // does not exist). (U64 = I64 now promotes to I128 — see
        // `extract_keys_cross_sign_u64_promotes`.)
        let l = vec![col("x", TypeCode::U128, false)];
        let r = vec![col("x", TypeCode::I64, false)];
        assert!(matches!(extract("a.x = b.x", l, r).unwrap_err(),
                         GnitzSqlError::Unsupported(_)));
        // One past the codec cap → Unsupported (arity cap). Built relative to
        // PK_LIST_MAX_COLS so a future bump doesn't silently slacken the test.
        let n = gnitz_core::PK_LIST_MAX_COLS + 1;
        let wide: Vec<ColumnDef> = (0..n)
            .map(|i| col(&format!("c{i}"), TypeCode::U64, false)).collect();
        let on_wide = (0..n).map(|i| format!("a.c{i}=b.c{i}"))
            .collect::<Vec<_>>().join(" AND ");
        assert!(matches!(extract(&on_wide, wide.clone(), wide).unwrap_err(),
                         GnitzSqlError::Unsupported(_)));
        // Both refs on the same table → Bind.
        assert!(matches!(extract("a.x = a.y", two_u64(), two_u64()).unwrap_err(),
                         GnitzSqlError::Bind(_)));
        // A non-eq, non-and, non-range operator (`<>`) → Unsupported.
        assert!(matches!(extract("a.x <> b.x", two_u64(), two_u64()).unwrap_err(),
                         GnitzSqlError::Unsupported(_)));
        // OR is not an AND-conjunction → Unsupported.
        assert!(matches!(extract("a.x = b.x OR a.y = b.y", two_u64(), two_u64()).unwrap_err(),
                         GnitzSqlError::Unsupported(_)));
    }

    #[test]
    fn validate_pair_accepts_compatible() {
        // Same-type pairs: T == the source reindex output type.
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U64, false),
                               &col("b", TypeCode::U64, false)).unwrap(), TypeCode::U64 as u8);
        validate_join_key_pair(&col("a", TypeCode::String, false),
                               &col("b", TypeCode::String, false)).unwrap();
        validate_join_key_pair(&col("a", TypeCode::String, false),
                               &col("b", TypeCode::Blob, false)).unwrap();
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U128, false),
                               &col("b", TypeCode::UUID, false)).unwrap(), TypeCode::U128 as u8);
        // Cross-width same-sign pairs promote to the wider type.
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::I32, false),
                               &col("b", TypeCode::I64, false)).unwrap(), TypeCode::I64 as u8);
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U32, false),
                               &col("b", TypeCode::U64, false)).unwrap(), TypeCode::U64 as u8);
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U8, false),
                               &col("b", TypeCode::U64, false)).unwrap(), TypeCode::U64 as u8);
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U32, false),
                               &col("b", TypeCode::U128, false)).unwrap(), TypeCode::U128 as u8);
        // Cross-sign pairs whose unsigned side is ≤ 8 bytes (U64) promote to the
        // narrowest signed type holding both ranges.
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U32, false),
                               &col("b", TypeCode::I64, false)).unwrap(), TypeCode::I64 as u8);
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U8, false),
                               &col("b", TypeCode::I16, false)).unwrap(), TypeCode::I16 as u8);
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U16, false),
                               &col("b", TypeCode::I32, false)).unwrap(), TypeCode::I32 as u8);
        // U32 = I32 needs the wider I64 (a signed type of equal width cannot hold
        // U32's full range).
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U32, false),
                               &col("b", TypeCode::I32, false)).unwrap(), TypeCode::I64 as u8);
        // U64 cross-sign with any signed integer ⇒ the signed-128 common type.
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U64, false),
                               &col("b", TypeCode::I64, false)).unwrap(), TypeCode::I128 as u8);
        assert_eq!(validate_join_key_pair(&col("a", TypeCode::U64, false),
                               &col("b", TypeCode::I8, false)).unwrap(), TypeCode::I128 as u8);
    }

    #[test]
    fn validate_pair_rejects_incompatible() {
        // Float column.
        assert!(validate_join_key_pair(&col("a", TypeCode::F64, false),
                                       &col("b", TypeCode::U64, false)).is_err());
        // Cross-sign pair whose unsigned side is 128-bit (would need a signed-256
        // type that does not exist). (U64 = I64 now promotes to I128.)
        assert!(validate_join_key_pair(&col("a", TypeCode::U128, false),
                                       &col("b", TypeCode::I64, false)).is_err());
        assert!(validate_join_key_pair(&col("a", TypeCode::UUID, false),
                                       &col("b", TypeCode::I64, false)).is_err());
        // Mixed string/native — STRING = U128 both reindex to U128, so this is
        // caught *only* by the german-string check, not the common-type check.
        assert!(validate_join_key_pair(&col("a", TypeCode::String, false),
                                       &col("b", TypeCode::U128, false)).is_err());
        assert!(validate_join_key_pair(&col("a", TypeCode::String, false),
                                       &col("b", TypeCode::I64, false)).is_err());
    }

    /// Cross-width promotion threads `T` into both per-side carried tcs and the
    /// `_join_pk` stamp: extract returns the promoted common type per pair.
    #[test]
    fn extract_keys_cross_width_promotes() {
        // INT (I32) = BIGINT (I64) → T = I64.
        let l = vec![col("x", TypeCode::I32, false)];
        let r = vec![col("x", TypeCode::I64, false)];
        let (lc, rc, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
        assert_eq!((lc, rc), (vec![0], vec![0]));
        assert_eq!(tcs, vec![TypeCode::I64 as u8]);

        // U32 = U64 → T = U64.
        let l = vec![col("x", TypeCode::U32, false)];
        let r = vec![col("x", TypeCode::U64, false)];
        let (_, _, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
        assert_eq!(tcs, vec![TypeCode::U64 as u8]);

        // Cross-sign: INT UNSIGNED (U32) = BIGINT (I64) → T = I64.
        let l = vec![col("x", TypeCode::U32, false)];
        let r = vec![col("x", TypeCode::I64, false)];
        let (_, _, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
        assert_eq!(tcs, vec![TypeCode::I64 as u8]);
    }

    /// BIGINT UNSIGNED (U64) cross-sign joined with any signed integer promotes to
    /// the signed-128 common type `I128`, the new capability of this feature.
    #[test]
    fn extract_keys_cross_sign_u64_promotes() {
        for signed in [TypeCode::I8, TypeCode::I16, TypeCode::I32, TypeCode::I64] {
            let l = vec![col("x", TypeCode::U64, false)];
            let r = vec![col("x", signed, false)];
            let (lc, rc, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
            assert_eq!((lc, rc), (vec![0], vec![0]));
            assert_eq!(tcs, vec![TypeCode::I128 as u8],
                "U64 = {signed:?} must promote to I128");
        }
    }

    #[test]
    fn multi_null_filter_builds_at_every_arity() {
        let schema = Schema {
            columns: vec![
                col("c0", TypeCode::U64, true),
                col("c1", TypeCode::U64, true),
                col("c2", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        // k = 1 (byte-identical to the old single-column null_filter_prog).
        multi_null_filter_prog(&[0], &schema, false).unwrap();
        multi_null_filter_prog(&[0], &schema, true).unwrap();
        // k ≥ 2: a multi-leaf And for want_null=false, a multi-leaf Or for true.
        multi_null_filter_prog(&[0, 1, 2], &schema, false).unwrap();
        multi_null_filter_prog(&[0, 1, 2], &schema, true).unwrap();
    }

    // ── Range / band join extraction ─────────────────────────────────────────

    /// An equi-only ON yields no range conjunct (the pure superset property — the
    /// equi path is byte-identical because nothing about its inputs changed).
    #[test]
    fn extract_equi_only_has_no_range() {
        let (_, _, _, range) = extract_full("a.x = b.x AND a.y = b.y", two_u64(), two_u64()).unwrap();
        assert!(range.is_none());
    }

    /// The §3 rel mapping for all four operators in BOTH operand orders. The
    /// canonical form is always `left_col OP right_col`; a right-table-first
    /// conjunct flips the operator to its converse.
    #[test]
    fn range_rel_mapping_both_operand_orders() {
        // (ON clause, canonical op, left_col, right_col). two_u64 = [x=0, y=1].
        let cases: &[(&str, RangeRel)] = &[
            ("a.x < b.y",  RangeRel::Lt),
            ("a.x <= b.y", RangeRel::Le),
            ("a.x > b.y",  RangeRel::Gt),
            ("a.x >= b.y", RangeRel::Ge),
            // Right-table-first: `b.y > a.x` ⟺ `a.x < b.y`, etc.
            ("b.y > a.x",  RangeRel::Lt),
            ("b.y >= a.x", RangeRel::Le),
            ("b.y < a.x",  RangeRel::Gt),
            ("b.y <= a.x", RangeRel::Ge),
        ];
        for (on, want_op) in cases {
            let (eq_l, _, _, range) = extract_full(on, two_u64(), two_u64()).unwrap();
            assert!(eq_l.is_empty(), "{on}: pure range join has no eq prefix");
            let rc = range.unwrap_or_else(|| panic!("{on}: expected a range conjunct"));
            assert_eq!(rc.op, *want_op, "{on}");
            assert_eq!((rc.left_col, rc.right_col), (0, 1), "{on}: canonicalized to a.x / b.y");
        }
    }

    /// A band join (equality prefix + one range conjunct): the eq pair and the
    /// range conjunct are both extracted.
    #[test]
    fn extract_band_join_eq_prefix_plus_range() {
        // a.k = b.k AND a.lo <= b.t. cols: a=[k=0, lo=1], b=[k=0, t=1].
        let (eq_l, eq_r, eq_tcs, range) =
            extract_full("a.x = b.x AND a.y <= b.y", two_u64(), two_u64()).unwrap();
        assert_eq!((eq_l, eq_r), (vec![0], vec![0]));
        assert_eq!(eq_tcs, vec![TypeCode::U64 as u8]);
        let rc = range.expect("range conjunct");
        assert_eq!((rc.left_col, rc.right_col, rc.op), (1, 1, RangeRel::Le));
    }

    /// Rejections at extraction: two range conjuncts, OR, and a string/blob range
    /// pair (the content hash is not order-preserving).
    #[test]
    fn range_extraction_rejections() {
        // Two range conjuncts → Unsupported.
        assert!(matches!(
            extract_full("a.x < b.x AND a.y < b.y", two_u64(), two_u64()).unwrap_err(),
            GnitzSqlError::Unsupported(_)));
        // String range pair → Unsupported (not order-preserving).
        let s = vec![col("x", TypeCode::String, false), col("y", TypeCode::String, false)];
        assert!(matches!(
            extract_full("a.x < b.x", s.clone(), s).unwrap_err(),
            GnitzSqlError::Unsupported(_)));
        // BLOB range pair → Unsupported.
        let bl = vec![col("x", TypeCode::Blob, false), col("y", TypeCode::Blob, false)];
        assert!(matches!(
            extract_full("a.x > b.x", bl.clone(), bl).unwrap_err(),
            GnitzSqlError::Unsupported(_)));
        // Float range pair → Unsupported (via validate_join_key_pair).
        let f = vec![col("x", TypeCode::F64, false), col("y", TypeCode::F64, false)];
        assert!(matches!(
            extract_full("a.x < b.x", f.clone(), f).unwrap_err(),
            GnitzSqlError::Unsupported(_)));
        // Both range operands on the same table → Bind.
        assert!(matches!(
            extract_full("a.x < a.y", two_u64(), two_u64()).unwrap_err(),
            GnitzSqlError::Bind(_)));
    }

    /// A signed range pair via cross-sign promotion resolves to the common type
    /// (U32 vs I64 → I64), the same ladder the equi path uses.
    #[test]
    fn range_pair_cross_sign_promotes() {
        let l = vec![col("id", TypeCode::U64, false), col("x", TypeCode::U32, false)];
        let r = vec![col("id", TypeCode::U64, false), col("y", TypeCode::I64, false)];
        let (_, _, _, range) = extract_full("a.x < b.y", l, r).unwrap();
        assert_eq!(range.unwrap().tc, TypeCode::I64 as u8);
    }
}
