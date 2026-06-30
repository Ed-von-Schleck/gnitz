//! DDL: CREATE TABLE (with FK resolution, UNIQUE, CLUSTER BY, REPLICATED), DROP,
//! and CREATE INDEX. The compile side's only non-view surface.

use crate::ast_util::extract_name;
use crate::bind::{find_unique_column, Binder};
use crate::error::GnitzSqlError;
use crate::plan::validate::{
    default_index_name, reject_duplicate_column_names, reject_non_key_eligible, reject_unhonored_column_options,
    reject_unhonored_table_constraints, validate_user_index_name,
};
use crate::types::{int_domain_fits, is_integer_type, sql_type_to_typecode};
use crate::SqlResult;
use gnitz_core::{ColumnDef, GnitzClient, IndexMeta, TypeCode};
use sqlparser::ast::{
    ColumnOption, Expr, ObjectType, SqlOption, TableConstraint, Value, ValueWithSpan, WrappedCollection,
};

/// FK child/parent type compatibility: an integer child widens to an integer
/// parent whose domain covers it; otherwise the types must match exactly. Returns
/// the standard mismatch error so both resolver paths reject identically.
fn check_fk_type_compat(fk_col_type: TypeCode, parent_col_type: TypeCode) -> Result<(), GnitzSqlError> {
    let is_compat = if is_integer_type(fk_col_type) && is_integer_type(parent_col_type) {
        int_domain_fits(fk_col_type, parent_col_type)
    } else {
        fk_col_type == parent_col_type
    };
    if !is_compat {
        return Err(GnitzSqlError::Bind(format!(
            "FK type mismatch: column type {fk_col_type:?} cannot reference column type \
             {parent_col_type:?} — the child column adopts the referenced type, which would \
             narrow or re-sign {fk_col_type:?}; declare the child with a type whose range \
             fits within {parent_col_type:?}",
        )));
    }
    Ok(())
}

/// Resolve a self-referencing FK against the in-flight column list (the table
/// is not yet registered in the catalog). The referenced column must be the
/// table's lone PK column; returns table id 0 — the sentinel the FK
/// registration path uses for "same table".
fn resolve_fk_target_inline(
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
    ref_table: &str,
    referred_columns: &[sqlparser::ast::Ident],
    fk_col_type: TypeCode,
) -> Result<(u64, u64, TypeCode), GnitzSqlError> {
    if referred_columns.len() > 1 {
        return Err(GnitzSqlError::Unsupported(
            "multi-column FOREIGN KEY references are not supported".into(),
        ));
    }
    let ref_col_idx: usize = if let Some(ident) = referred_columns.first() {
        current_cols
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
            .ok_or_else(|| {
                GnitzSqlError::Bind(format!(
                    "FK references column '{}' not found in table '{}'",
                    ident.value, ref_table,
                ))
            })?
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
    check_fk_type_compat(fk_col_type, parent_col_type)?;
    Ok((0, ref_col_idx as u64, parent_col_type))
}

/// Resolve a REFERENCES clause to (fk_table_id, ref_col_idx, parent_col_type).
/// The referenced column is a legal target iff it is the parent's lone PK
/// column or it carries its own active UNIQUE index. Validates that
/// fk_col_type is compatible with the referenced column type and returns that
/// type so the caller can widen the child column.
#[allow(clippy::too_many_arguments)]
fn resolve_fk_target(
    client: &mut GnitzClient,
    schema_name: &str,
    foreign_table: &sqlparser::ast::ObjectName,
    referred_columns: &[sqlparser::ast::Ident],
    fk_col_type: TypeCode,
    current_table_name: &str,
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
) -> Result<(u64, u64, TypeCode), GnitzSqlError> {
    let ref_table = extract_name(foreign_table, "REFERENCES")?;

    // Self-referencing FK: the table being created is not yet in the catalog,
    // so resolve the referenced column against the in-flight column list.
    if ref_table.eq_ignore_ascii_case(current_table_name) {
        return resolve_fk_target_inline(current_cols, current_pk_cols, &ref_table, referred_columns, fk_col_type);
    }

    let (ref_tid, ref_schema) = client
        .resolve_table_id(schema_name, &ref_table)
        .map_err(|e| GnitzSqlError::Bind(format!("FK target '{ref_table}': {e}")))?;

    if referred_columns.len() > 1 {
        return Err(GnitzSqlError::Unsupported(
            "multi-column FOREIGN KEY references are not supported".into(),
        ));
    }

    // Referenced parent column. An omitted column list defaults to the PK and
    // is only well-defined for a single-column PK.
    let ref_col_idx: usize = if let Some(ident) = referred_columns.first() {
        ref_schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
            .ok_or_else(|| {
                GnitzSqlError::Bind(format!(
                    "FK references column '{}' not found in table '{}'",
                    ident.value, ref_table,
                ))
            })?
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
    let is_lone_pk = ref_schema.pk_count() == 1 && ref_col_idx == ref_schema.pk_index_single();
    if !is_lone_pk {
        match client
            .index_for_column(ref_tid, ref_col_idx)
            .map_err(GnitzSqlError::Exec)?
        {
            Some(IndexMeta { is_unique: true, .. }) => {}
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "FK against table '{}' must reference the primary key or a column \
                 with a UNIQUE index; column '{}' has neither",
                    ref_table, ref_schema.columns[ref_col_idx].name,
                )))
            }
        }
    }

    // Child column widens to the referenced parent column's type.
    let parent_col_type = ref_schema.columns[ref_col_idx].type_code;
    check_fk_type_compat(fk_col_type, parent_col_type)?;

    Ok((ref_tid, ref_col_idx as u64, parent_col_type))
}

/// Extract the `REPLICATED` table property from a `CREATE TABLE … WITH (…)`
/// option list. Surface: `CREATE TABLE t (…) WITH (replicated = true)`. A
/// replicated table keeps a full copy on every worker (broadcast writes,
/// single-source reads). Only the `replicated` key is recognized; any other
/// `WITH` option is rejected so a typo cannot be silently ignored (gnitz has no
/// other table-level `WITH` options today).
fn parse_replicated_option(with_options: &[SqlOption]) -> Result<bool, GnitzSqlError> {
    let mut replicated = false;
    for opt in with_options {
        match opt {
            SqlOption::KeyValue { key, value } if key.value.eq_ignore_ascii_case("replicated") => {
                let Expr::Value(ValueWithSpan {
                    value: Value::Boolean(b),
                    ..
                }) = value
                else {
                    return Err(GnitzSqlError::Plan(
                        "WITH (replicated = …) expects a boolean (true/false)".into(),
                    ));
                };
                replicated = *b;
            }
            other => {
                return Err(GnitzSqlError::Plan(format!(
                    "unsupported CREATE TABLE option in WITH (…): {other:?}"
                )))
            }
        }
    }
    Ok(replicated)
}

pub(crate) fn execute_create_table(
    client: &mut GnitzClient,
    schema_name: &str,
    create: &sqlparser::ast::CreateTable,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&create.name, "CREATE TABLE")?;

    let sql_cols = &create.columns;

    // Reject duplicate column names up front: the PK/UNIQUE column lookups
    // below resolve by name and would silently bind to the first match.
    reject_duplicate_column_names(sql_cols.iter().map(|c| c.name.value.as_str()), "table definition")?;

    // Reject every column option / table constraint the table builder does not
    // honor (DEFAULT, CHECK, GENERATED, FK referential actions, inline INDEX, …)
    // before column/PK processing, so an unsupported clause errors here rather
    // than being silently dropped or masked by the "requires at least one
    // PRIMARY KEY column" admission error below.
    for col in sql_cols {
        reject_unhonored_column_options(col, "column definition")?;
    }
    reject_unhonored_table_constraints(&create.constraints, "table constraint")?;

    // Phase 1 — build column defs (name, type, nullability only).
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(sql_cols.len());
    for col in sql_cols.iter() {
        cols.push(ColumnDef::new(
            col.name.value.clone(),
            sql_type_to_typecode(&col.data_type)?,
            !col.options.iter().any(|o| matches!(o.option, ColumnOption::NotNull)),
        ));
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
                let idx = sql_cols
                    .iter()
                    .position(|c| c.name.value.eq_ignore_ascii_case(&col_ident.value))
                    .ok_or_else(|| {
                        GnitzSqlError::Bind(format!("PRIMARY KEY column '{}' not found", col_ident.value))
                    })?;
                if pk_indices.contains(&(idx as u32)) {
                    return Err(GnitzSqlError::Plan(format!(
                        "Duplicate column '{}' in PRIMARY KEY",
                        col_ident.value
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
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    let (tid, idx, parent_pk_type) = resolve_fk_target(
                        client,
                        schema_name,
                        foreign_table,
                        referred_columns,
                        cols[i].type_code,
                        &table_name,
                        &cols,
                        &pk_indices,
                    )?;
                    cols[i].fk_table_id = tid;
                    cols[i].fk_col_idx = idx;
                    cols[i].type_code = parent_pk_type;
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
        if let TableConstraint::ForeignKey {
            columns,
            foreign_table,
            referred_columns,
            ..
        } = constraint
        {
            if columns.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "multi-column FOREIGN KEY constraints are not supported".into(),
                ));
            }
            let local_col_name = &columns[0].value;
            let col_idx = cols
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(local_col_name))
                .ok_or_else(|| {
                    GnitzSqlError::Bind(format!(
                        "FOREIGN KEY column '{local_col_name}' not found in table definition"
                    ))
                })?;
            let (tid, idx, parent_pk_type) = resolve_fk_target(
                client,
                schema_name,
                foreign_table,
                referred_columns,
                cols[col_idx].type_code,
                &table_name,
                &cols,
                &pk_indices,
            )?;
            cols[col_idx].fk_table_id = tid;
            cols[col_idx].fk_col_idx = idx;
            cols[col_idx].type_code = parent_pk_type;
        }
    }

    // Phase 5 — table-level UNIQUE constraints, single- or multi-column. Each
    // named column is resolved to its index in declared order (order is
    // significant: it drives the composite index's leading-key span and prefix
    // seeks).
    for constraint in &create.constraints {
        if let TableConstraint::Unique {
            name: name_ident,
            columns,
            ..
        } = constraint
        {
            if columns.is_empty() {
                return Err(GnitzSqlError::Plan("UNIQUE constraint cannot be empty".into()));
            }
            let mut col_indices: Vec<u32> = Vec::with_capacity(columns.len());
            for col_ident in columns {
                let col_name = &col_ident.value;
                let idx = cols
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| {
                        GnitzSqlError::Bind(format!("UNIQUE column '{col_name}' not found in table definition"))
                    })?;
                if col_indices.contains(&(idx as u32)) {
                    return Err(GnitzSqlError::Plan(format!(
                        "duplicate column '{col_name}' in UNIQUE constraint"
                    )));
                }
                col_indices.push(idx as u32);
            }
            if unique_cols.iter().any(|(c, _)| c.as_slice() == col_indices.as_slice()) {
                return Err(GnitzSqlError::Plan(format!(
                    "duplicate UNIQUE constraint on column(s) ({})",
                    columns.iter().map(|c| c.value.as_str()).collect::<Vec<_>>().join(", ")
                )));
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
            "CREATE TABLE requires at least one PRIMARY KEY column".into(),
        ));
    }
    if pk_indices.len() > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(format!(
            "PRIMARY KEY supports at most {} columns",
            gnitz_core::PK_LIST_MAX_COLS
        )));
    }
    // Pre-check before the DDL reaches the engine (which re-validates via
    // `validate_pk_cols`); naming the offending column here gives a clearer
    // error. Same eligibility rule, shared via `TypeCode::is_pk_eligible`.
    for &i in &pk_indices {
        let tc = cols[i as usize].type_code;
        reject_non_key_eligible(&cols[i as usize].name, tc, "PRIMARY KEY")?;
    }
    // The PK region must fit MAX_PK_BYTES. Strides ≤ 16 widen to a `u128` fast
    // key; wider compound PKs (stride > 16, e.g. three `U64`s = 24) route
    // through the byte-path cursor/merge accessors. The 4-column cap above
    // bounds a valid PK at 64 bytes; MAX_PK_BYTES is the ceiling. The engine
    // re-validates the same bound in `validate_pk_cols`.
    let pk_stride: usize = pk_indices
        .iter()
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
            reject_non_key_eligible(&cols[c as usize].name, tc, "UNIQUE")?;
        }
    }

    // Phase 6 — CLUSTER BY (hash distribution key). The named columns must be the
    // PK's leading prefix in PK order; the prefix length `k` is persisted in
    // `TABLE_TAB.flags` and drives write-side routing and co-partition detection.
    // No clause ⇒ `k = 0` ⇒ default full-PK distribution (byte-identical to before
    // this feature). `GenericDialect` parses `CLUSTER BY a, b` into `cluster_by`.
    let dist_prefix_len = if let Some(cluster) = &create.cluster_by {
        let (WrappedCollection::NoWrapping(idents) | WrappedCollection::Parentheses(idents)) = cluster;
        let mut cluster_indices: Vec<u32> = Vec::with_capacity(idents.len());
        for col_ident in idents {
            let idx = find_unique_column(&cols, &col_ident.value)?
                .ok_or_else(|| GnitzSqlError::Bind(format!("CLUSTER BY column '{}' not found", col_ident.value)))?;
            cluster_indices.push(idx as u32);
        }
        gnitz_core::validate_dist_prefix(&pk_indices, &cluster_indices).map_err(GnitzSqlError::Plan)?
    } else {
        0
    };

    // Phase 7 — REPLICATED (full copy on every worker), via `WITH (replicated = true)`.
    // Mutually exclusive with CLUSTER BY: a hash-distribution prefix is meaningless
    // when every worker already holds the whole table. The flags packing cannot make
    // the conflict unrepresentable (replicated is a boolean bit, k a byte), so reject
    // it here.
    let replicated = parse_replicated_option(&create.with_options)?;
    if replicated && dist_prefix_len != 0 {
        return Err(GnitzSqlError::Plan(
            "REPLICATED and CLUSTER BY are mutually exclusive: a replicated table keeps \
             a full copy on every worker, so a hash-distribution prefix is meaningless"
                .into(),
        ));
    }

    let tid = client
        .create_table(
            schema_name,
            &table_name,
            &cols,
            &pk_indices,
            true,
            replicated,
            dist_prefix_len,
        )
        .map_err(GnitzSqlError::Exec)?;

    // Unique secondary indices: created after the table exists (create_index
    // requires the table id). Types were pre-validated, and the base table is
    // empty, so create_index here cannot fail on a bad type or duplicate data —
    // only on transport-level errors. On failure, best-effort drop_table to
    // avoid an orphaned table with missing constraints (DDL is not
    // transactional across the create_table / create_index boundary).
    for (col_indices, constraint_name) in &unique_cols {
        let col_types: Vec<TypeCode> = col_indices.iter().map(|&c| cols[c as usize].type_code).collect();
        let index_name = constraint_name.clone().unwrap_or_else(|| {
            let col_names: Vec<&str> = col_indices.iter().map(|&c| cols[c as usize].name.as_str()).collect();
            default_index_name(schema_name, &table_name, &col_names)
        });
        if let Err(e) = client.create_index(tid, col_indices, &col_types, &index_name, true) {
            let _ = client.drop_table(schema_name, &table_name);
            return Err(GnitzSqlError::Exec(e));
        }
    }

    Ok(SqlResult::TableCreated { table_id: tid })
}

pub(crate) fn execute_drop(
    client: &mut GnitzClient,
    schema_name: &str,
    object_type: &ObjectType,
    names: &[sqlparser::ast::ObjectName],
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
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "DROP {object_type:?} not supported"
                )))
            }
        }
    }
    Ok(SqlResult::Dropped)
}

pub(crate) fn execute_create_index(
    client: &mut GnitzClient,
    schema_name: &str,
    ci: &sqlparser::ast::CreateIndex,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&ci.table_name, "CREATE INDEX")?;

    // A user-supplied index name flows through to the IDX_TAB row so
    // `DROP INDEX <name>` resolves it. Validate it before anything else: a
    // malformed or `__fk_`-infixed name would persist and be undroppable.
    let explicit_name = ci.name.as_ref().map(|n| extract_name(n, "CREATE INDEX")).transpose()?;
    if let Some(ref name) = explicit_name {
        validate_user_index_name(name)?;
    }

    if ci.columns.is_empty() {
        return Err(GnitzSqlError::Bind(
            "CREATE INDEX: at least one column required".to_string(),
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
            _ => {
                return Err(GnitzSqlError::Bind(
                    "CREATE INDEX: column must be a simple identifier".to_string(),
                ))
            }
        };
        let col_idx = find_unique_column(&schema.columns, &col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found")))?;
        if col_indices.contains(&(col_idx as u32)) {
            return Err(GnitzSqlError::Unsupported(format!(
                "CREATE INDEX: duplicate column '{col_name}' in index list"
            )));
        }
        col_names.push(col_name);
        col_indices.push(col_idx as u32);
    }

    // Limit pre-check — runs before the client push so an over-limit index
    // raises a clean planner error here, not after the IDX_TAB row already
    // committed. `index_key_types` is the same promotion + arity/stride
    // validation the engine's `make_index_schema` runs, so the friendly error
    // here and the engine backstop can never disagree.
    let col_types: Vec<TypeCode> = col_indices
        .iter()
        .map(|&c| schema.columns[c as usize].type_code)
        .collect();
    let raw_types: Vec<u8> = col_types.iter().map(|&tc| tc as u8).collect();
    gnitz_core::index_key_types(&raw_types, schema.pk_count(), schema.pk_stride())
        .map_err(GnitzSqlError::Unsupported)?; // also rejects String/Blob/float

    let index_name = explicit_name.unwrap_or_else(|| {
        let names: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        default_index_name(schema_name, &table_name, &names)
    });

    let index_id = client
        .create_index(table_id, &col_indices, &col_types, &index_name, is_unique)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::IndexCreated { index_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fk_widening_and_identity_accepted() {
        assert!(check_fk_type_compat(TypeCode::I32, TypeCode::I64).is_ok()); // safe widen
        assert!(check_fk_type_compat(TypeCode::U32, TypeCode::I64).is_ok()); // unsigned → wider signed
        assert!(check_fk_type_compat(TypeCode::I32, TypeCode::I32).is_ok()); // identity
        assert!(check_fk_type_compat(TypeCode::U64, TypeCode::U64).is_ok()); // identity
        assert!(check_fk_type_compat(TypeCode::UUID, TypeCode::UUID).is_ok()); // non-integer exact match
    }

    #[test]
    fn fk_narrowing_resigning_or_cross_type_rejected() {
        for (child, parent) in [
            (TypeCode::U64, TypeCode::I32),   // narrow + re-sign
            (TypeCode::U64, TypeCode::I64),   // same width, unsigned → signed
            (TypeCode::I64, TypeCode::U64),   // signed → unsigned
            (TypeCode::U128, TypeCode::I128), // 16→16, unsigned → signed
            (TypeCode::U64, TypeCode::UUID),  // integer child → non-integer parent
        ] {
            assert!(
                matches!(check_fk_type_compat(child, parent), Err(GnitzSqlError::Bind(_))),
                "{child:?} → {parent:?} must be rejected"
            );
        }
    }
}
