//! DDL: CREATE TABLE (with FK resolution, UNIQUE, CLUSTER BY, REPLICATED), DROP,
//! and CREATE INDEX. The compile side's only non-view surface.

use crate::ast_util::{extract_name, index_column_ident, simple_ident_expr};
use crate::bind::{find_unique_column, Binder};
use crate::error::GnitzSqlError;
use crate::types::{int_domain_fits, is_integer_type, serial_underlying, sql_type_to_typecode};
use crate::validate::{
    canonical_user_name, default_index_name, disambiguate_index_name, non_key_eligible_error, reject_duplicate_names,
    reject_unhonored_column_options, reject_unhonored_table_constraints, reject_unindexable_columns,
    validate_user_index_name, validate_user_name, ColumnOptionSite,
};
use crate::SqlResult;
use gnitz_core::{ColumnDef, GnitzClient, IndexMeta, InlineUniqueIndex, TableProps, TypeCode};
use sqlparser::ast::{
    ColumnOption, CreateTableOptions, Expr, ForeignKeyConstraint, ObjectType, PrimaryKeyConstraint, TableConstraint,
    UniqueConstraint, Value, ValueWithSpan, WrappedCollection,
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

/// Reject a UNIQUE constraint whose backing index the engine could not build.
/// A secondary index's record key is the indexed columns *plus the source PK*, so
/// the two rules are the indexed columns' own key-eligibility and the combined
/// key's arity and stride. Both gates are the ones `create_index_core` runs, in
/// the same order, so the UNIQUE and CREATE INDEX surfaces cannot disagree.
fn reject_unbuildable_unique_index(
    unique_cols: &[(Vec<u32>, Option<String>)],
    cols: &[ColumnDef],
    pk_indices: &[u32],
) -> Result<(), GnitzSqlError> {
    let src_pk_stride: usize = pk_indices
        .iter()
        .map(|&c| cols[c as usize].type_code.wire_stride())
        .sum();
    for (col_indices, _) in unique_cols {
        let names: Vec<&str> = col_indices.iter().map(|&c| cols[c as usize].name.as_str()).collect();
        let types: Vec<TypeCode> = col_indices.iter().map(|&c| cols[c as usize].type_code).collect();
        reject_unindexable_columns(&names, &types, "UNIQUE")?;
        let raw_types: Vec<u8> = types.iter().map(|&tc| tc as u8).collect();
        gnitz_core::index_key_types(&raw_types, pk_indices.len(), src_pk_stride).map_err(GnitzSqlError::Unsupported)?;
    }
    Ok(())
}

/// Resolve a PRIMARY KEY / UNIQUE / CREATE INDEX column list against `cols`, in
/// declared order — order drives the composite index's leading-key span and its
/// prefix seeks. `ctx` names the surface in both messages.
fn resolve_index_columns<'c>(
    columns: &'c [sqlparser::ast::IndexColumn],
    cols: &[ColumnDef],
    ctx: &str,
) -> Result<(Vec<&'c str>, Vec<u32>), GnitzSqlError> {
    let mut names: Vec<&str> = Vec::with_capacity(columns.len());
    let mut indices: Vec<u32> = Vec::with_capacity(columns.len());
    for c in columns {
        let name = index_column_ident(c, ctx)?;
        let idx = find_unique_column(cols, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("{ctx} column '{name}' not found")))? as u32;
        if indices.contains(&idx) {
            return Err(GnitzSqlError::Plan(format!("{ctx}: duplicate column '{name}'")));
        }
        names.push(name);
        indices.push(idx);
    }
    Ok((names, indices))
}

/// Resolve a `REFERENCES` clause's column list to the parent column it names.
/// `pk_single` is the parent's lone PK column when its PK is single-column — an
/// omitted column list defaults to it, and is undefined otherwise.
fn resolve_referred_column(
    referred_columns: &[sqlparser::ast::Ident],
    ref_table: &str,
    cols: &[ColumnDef],
    pk_single: Option<usize>,
) -> Result<usize, GnitzSqlError> {
    if referred_columns.len() > 1 {
        return Err(GnitzSqlError::Unsupported(
            "multi-column FOREIGN KEY references are not supported".into(),
        ));
    }
    match (referred_columns.first(), pk_single) {
        (Some(ident), _) => find_unique_column(cols, &ident.value)?.ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "FK references column '{}' not found in table '{}'",
                ident.value, ref_table,
            ))
        }),
        (None, Some(pk)) => Ok(pk),
        (None, None) => Err(GnitzSqlError::Bind(format!(
            "FK against '{ref_table}' must name the referenced column (its primary key is not a single column)",
        ))),
    }
}

/// Resolve a self-referencing FK against the in-flight column list (the table
/// is not yet registered in the catalog). The referenced column must be the
/// table's lone PK column, and may not be `fk_col_idx` itself. Returns
/// [`ColumnDef::SELF_FK_TABLE_ID`] as the table id: the planner cannot name an
/// id that is allocated only when the table is created, so it marks the column
/// and the `COL_TAB` writer rewrites the marker to the owner id.
fn resolve_fk_target_inline(
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
    ref_table: &str,
    referred_columns: &[sqlparser::ast::Ident],
    fk_col_idx: usize,
) -> Result<(u64, u64, TypeCode), GnitzSqlError> {
    let pk_single = (current_pk_cols.len() == 1).then(|| current_pk_cols[0] as usize);
    let ref_col_idx = resolve_referred_column(referred_columns, ref_table, current_cols, pk_single)?;

    if pk_single != Some(ref_col_idx) {
        return Err(GnitzSqlError::Unsupported(format!(
            "self-referencing FK must reference the primary key of '{}'; \
             column '{}' is not the lone PK",
            ref_table, current_cols[ref_col_idx].name,
        )));
    }

    // A column referencing itself is a tautology every row satisfies by
    // construction, and it has no index to validate against: the referenced
    // column is the lone PK, so the child column would be a PK column too, and
    // the FK auto-index skips PK columns (the PK region already
    // stores them). Every parent delete would then fail on a missing child
    // index. Rejecting it keeps the self-FK column non-PK and its index present.
    if ref_col_idx == fk_col_idx {
        return Err(GnitzSqlError::Bind(
            "a self-referencing FK column must not be the referenced column itself".into(),
        ));
    }

    let parent_col_type = current_cols[ref_col_idx].type_code;
    check_fk_type_compat(current_cols[fk_col_idx].type_code, parent_col_type)?;
    Ok((ColumnDef::SELF_FK_TABLE_ID, ref_col_idx as u64, parent_col_type))
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
    fk_col_idx: usize,
    current_table_name: &str,
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
) -> Result<(u64, u64, TypeCode), GnitzSqlError> {
    let ref_table = extract_name(foreign_table, "REFERENCES")?;

    // Self-referencing FK: the table being created is not yet in the catalog,
    // so resolve the referenced column against the in-flight column list.
    if ref_table.eq_ignore_ascii_case(current_table_name) {
        return resolve_fk_target_inline(current_cols, current_pk_cols, &ref_table, referred_columns, fk_col_idx);
    }

    // The one relation-name catalog probe outside the Binder funnels: hold the
    // FK target to the same reserved-prefix rule they enforce.
    validate_user_name(&ref_table)?;
    let ref_rel = client
        .resolve_relation(schema_name, &ref_table)
        .map_err(|e| GnitzSqlError::Bind(format!("FK target '{ref_table}': {e}")))?;
    let ref_schema = &ref_rel.schema;
    // The PK/UNIQUE tests below read only the schema, and both a view's and a
    // stream's PK look exactly like a base table's without being the unique, stored
    // key the parent probe reads.
    if ref_rel.class != gnitz_core::RelClass::Table {
        return Err(GnitzSqlError::Bind(format!(
            "FK target '{ref_table}' is a {}; a FOREIGN KEY must reference a base table",
            ref_rel.class.noun()
        )));
    }
    let ref_tid = ref_rel.tid;

    let pk_single = (ref_schema.pk_count() == 1).then(|| ref_schema.pk_index_single());
    let ref_col_idx = resolve_referred_column(referred_columns, &ref_table, &ref_schema.columns, pk_single)?;

    // Legal target iff the referenced column is the parent's lone PK, or it
    // carries an active UNIQUE index.
    if pk_single != Some(ref_col_idx) {
        match client.index_for_column(ref_tid, ref_col_idx)? {
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
    check_fk_type_compat(current_cols[fk_col_idx].type_code, parent_col_type)?;

    Ok((ref_tid, ref_col_idx as u64, parent_col_type))
}

/// The boolean properties of `CREATE TABLE … WITH (…)`. `dist_prefix_len` is left
/// at its default; the CLUSTER BY phase fills it. An unknown key is rejected so a
/// typo cannot be silently ignored.
fn parse_table_options(table_options: &CreateTableOptions) -> Result<TableProps, GnitzSqlError> {
    let mut props = TableProps::default();
    for opt in crate::validate::with_options(table_options)? {
        let (key, value) = crate::validate::require_kv_option(opt, "CREATE TABLE")?;
        let slot = if key.value.eq_ignore_ascii_case("replicated") {
            &mut props.replicated
        } else if key.value.eq_ignore_ascii_case("stream") {
            &mut props.stream
        } else {
            return Err(GnitzSqlError::Plan(format!(
                "unknown CREATE TABLE option '{}'; the supported options are `replicated` and `stream`",
                key.value
            )));
        };
        let Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) = value
        else {
            return Err(GnitzSqlError::Plan(format!(
                "WITH ({} = …) expects a boolean (true/false)",
                key.value
            )));
        };
        *slot = *b;
    }
    Ok(props)
}

pub(crate) fn execute_create_table(
    client: &mut GnitzClient,
    schema_name: &str,
    create: &sqlparser::ast::CreateTable,
) -> Result<SqlResult, GnitzSqlError> {
    let table_name = extract_name(&create.name, "CREATE TABLE")?;
    validate_user_name(&table_name)?;

    let sql_cols = &create.columns;

    // Reject duplicate column names up front: the PK/UNIQUE column lookups
    // below resolve by name and would silently bind to the first match.
    reject_duplicate_names(sql_cols.iter().map(|c| c.name.value.as_str()), "table definition")?;

    // Reject every column option / table constraint the table builder does not
    // honor (DEFAULT, CHECK, GENERATED, FK referential actions, inline INDEX, …)
    // before column/PK processing, so an unsupported clause errors here rather
    // than being silently dropped or masked by the "requires at least one
    // PRIMARY KEY column" admission error below.
    for col in sql_cols {
        reject_unhonored_column_options(col, "column definition", ColumnOptionSite::CreateTable)?;
    }
    reject_unhonored_table_constraints(&create.constraints, "table constraint")?;

    // Phase 1 — build column defs (name, type, nullability only). A SERIAL
    // column resolves to its underlying signed int, is always NOT NULL, and
    // carries the `is_serial` marker; at most one per table. The lone-PK
    // constraint is enforced after PK gathering (Phase 2).
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(sql_cols.len());
    for col in sql_cols.iter() {
        let cd = if let Some(tc) = serial_underlying(&col.data_type) {
            ColumnDef::new(col.name.value.clone(), tc, false).serial() // NOT NULL
        } else {
            ColumnDef::new(
                col.name.value.clone(),
                sql_type_to_typecode(&col.data_type)?,
                !col.options.iter().any(|o| matches!(o.option, ColumnOption::NotNull)),
            )
        };
        cols.push(cd);
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
        if let TableConstraint::PrimaryKey(PrimaryKeyConstraint { columns: pk_cols, .. }) = constraint {
            if pk_decl_seen {
                return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
            }
            pk_decl_seen = true;
            pk_indices = resolve_index_columns(pk_cols, &cols, "PRIMARY KEY")?.1;
        }
    }

    // Phase 3a — gather all inline column-level PRIMARY KEYs first, so that
    // `pk_indices` is fully populated before any FK is resolved. A
    // self-referencing FK declared before its inline PK column would otherwise
    // see an empty `pk_indices` and fail spuriously.
    for (i, col) in sql_cols.iter().enumerate() {
        for opt in &col.options {
            if let ColumnOption::PrimaryKey(_) = &opt.option {
                if pk_decl_seen {
                    return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
                }
                pk_decl_seen = true;
                pk_indices.push(i as u32);
            }
        }
    }

    // Phase 3b — gather the FOREIGN KEY sites and collect column-level UNIQUE.
    // Order is load-bearing: `resolve_fk_target` REWRITES the child column's type
    // to the parent's and a later resolution reads `&cols`, so inline sites must
    // stay ahead of table-level ones.
    let mut fk_sites: Vec<(usize, &sqlparser::ast::ObjectName, &[sqlparser::ast::Ident])> = Vec::new();
    for (i, col) in sql_cols.iter().enumerate() {
        for opt in &col.options {
            match &opt.option {
                ColumnOption::ForeignKey(ForeignKeyConstraint {
                    foreign_table,
                    referred_columns,
                    ..
                }) => fk_sites.push((i, foreign_table, referred_columns)),
                ColumnOption::Unique(_) if !unique_cols.iter().any(|(c, _)| c.as_slice() == [i as u32]) => {
                    unique_cols.push((vec![i as u32], None));
                }
                _ => {}
            }
        }
    }

    // Phase 4 — table-level FOREIGN KEY constraints. Same sites, child column
    // named rather than positional.
    for constraint in &create.constraints {
        if let TableConstraint::ForeignKey(ForeignKeyConstraint {
            columns,
            foreign_table,
            referred_columns,
            ..
        }) = constraint
        {
            if columns.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "multi-column FOREIGN KEY constraints are not supported".into(),
                ));
            }
            let local_col_name = &columns[0].value;
            let col_idx = find_unique_column(&cols, local_col_name)?.ok_or_else(|| {
                GnitzSqlError::Bind(format!(
                    "FOREIGN KEY column '{local_col_name}' not found in table definition"
                ))
            })?;
            fk_sites.push((col_idx, foreign_table, referred_columns));
        }
    }

    for (col_idx, foreign_table, referred_columns) in fk_sites {
        let (tid, idx, parent_pk_type) = resolve_fk_target(
            client,
            schema_name,
            foreign_table,
            referred_columns,
            col_idx,
            &table_name,
            &cols,
            &pk_indices,
        )?;
        cols[col_idx].fk_table_id = tid;
        cols[col_idx].fk_col_idx = idx;
        cols[col_idx].type_code = parent_pk_type;
    }

    // Phase 5 — table-level UNIQUE constraints, single- or multi-column. Each
    // named column is resolved to its index in declared order (order is
    // significant: it drives the composite index's leading-key span and prefix
    // seeks).
    for constraint in &create.constraints {
        if let TableConstraint::Unique(UniqueConstraint {
            name: name_ident,
            columns,
            ..
        }) = constraint
        {
            if columns.is_empty() {
                return Err(GnitzSqlError::Plan("UNIQUE constraint cannot be empty".into()));
            }
            let (col_names, col_indices) = resolve_index_columns(columns, &cols, "UNIQUE")?;
            if unique_cols.iter().any(|(c, _)| c.as_slice() == col_indices.as_slice()) {
                return Err(GnitzSqlError::Plan(format!(
                    "duplicate UNIQUE constraint on column(s) ({})",
                    col_names.join(", ")
                )));
            }
            let constraint_name = name_ident.as_ref().map(|n| n.value.clone());
            if let Some(ref name) = constraint_name {
                validate_user_index_name(name)?;
            }
            unique_cols.push((col_indices, constraint_name));
        }
    }

    // PK columns keep their declared type. The null bitmap excludes the PK
    // region, so a nullable PK has no place to carry the null — PRIMARY KEY
    // implies NOT NULL, so coerce before validating rather than rejecting.
    for &i in &pk_indices {
        cols[i as usize].is_nullable = false;
    }

    // Admission rule — every base table must satisfy these conditions. The rule
    // set is `gnitz-wire`'s, shared with the client's `validate_parts` and the
    // engine catalog's `validate_pk_cols`, so this pre-check and the engine
    // backstop cannot disagree on what a legal PK is. Only the wording is the
    // planner's: it names the offending column, which the engine cannot.
    gnitz_wire::validate_pk_tuple(&pk_indices, cols.len(), |c| {
        let cd = &cols[c as usize];
        (cd.type_code as u8, cd.is_nullable)
    })
    .map_err(|rule| match rule {
        gnitz_wire::PkRule::Empty => {
            GnitzSqlError::Plan("CREATE TABLE requires at least one PRIMARY KEY column".into())
        }
        gnitz_wire::PkRule::TooManyColumns { .. } => GnitzSqlError::Unsupported(format!(
            "PRIMARY KEY supports at most {} columns",
            gnitz_core::PK_LIST_MAX_COLS
        )),
        gnitz_wire::PkRule::NotEligible { col, .. } => {
            let cd = &cols[col as usize];
            non_key_eligible_error(&cd.name, cd.type_code, "PRIMARY KEY")
        }
        gnitz_wire::PkRule::StrideOutOfRange { stride } => GnitzSqlError::Unsupported(format!(
            "PRIMARY KEY total stride must be 1..={} bytes, got {stride}",
            gnitz_core::MAX_PK_BYTES
        )),
        other => GnitzSqlError::Unsupported(other.to_string()),
    })?;

    // A SERIAL column must be the table's sole, single-column PRIMARY KEY. This
    // is load-bearing, not just a simplification: the INSERT path spells the
    // generated id as `PkTuple::from_u128(stride, id)`, which has no compound
    // form. Reject a SERIAL column that is part of a compound PK, is not the PK
    // at all, or shares the table with a second SERIAL column.
    if cols.iter().filter(|c| c.is_serial).count() > 1 {
        return Err(GnitzSqlError::Unsupported("at most one SERIAL column per table".into()));
    }
    if let Some(sci) = cols.iter().position(|c| c.is_serial) {
        if pk_indices.as_slice() != [sci as u32] {
            return Err(GnitzSqlError::Unsupported(
                "a SERIAL column must be the table's single-column PRIMARY KEY".into(),
            ));
        }
    }

    // A lone single-column PK is already unique; drop a redundant secondary
    // unique index equal to it. A compound-PK member declared UNIQUE is NOT
    // individually unique, and a composite UNIQUE (len > 1) never equals a
    // single-element lone PK, so both are kept (the engine supports unique
    // indices on PK columns, and the engine's trivial-uniqueness short-circuit
    // skips the pre-flight scan for a composite UNIQUE equal to a compound PK).
    let lone_pk: &[u32] = if pk_indices.len() == 1 { &pk_indices } else { &[] };
    unique_cols.retain(|(c, _)| c.as_slice() != lone_pk);

    // Before `create_table`, since DDL is not transactional: a rejection after the
    // table exists would leave an orphan.
    reject_unbuildable_unique_index(&unique_cols, &cols, &pk_indices)?;

    // Phase 6 — CLUSTER BY (hash distribution key). The named columns must be the
    // PK's leading prefix in PK order; the prefix length `k` is persisted in
    // `TABLE_TAB.flags` and drives write-side routing and co-partition detection.
    // No clause ⇒ `k = 0` ⇒ default full-PK distribution (byte-identical to before
    // this feature). `GenericDialect` parses `CLUSTER BY a, b` into `cluster_by`.
    let dist_prefix_len = if let Some(cluster) = &create.cluster_by {
        let (WrappedCollection::NoWrapping(exprs) | WrappedCollection::Parentheses(exprs)) = cluster;
        let mut cluster_indices: Vec<u32> = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let col_name = simple_ident_expr(expr, "CLUSTER BY")?;
            let idx = find_unique_column(&cols, col_name)?
                .ok_or_else(|| GnitzSqlError::Bind(format!("CLUSTER BY column '{col_name}' not found")))?;
            cluster_indices.push(idx as u32);
        }
        gnitz_core::validate_dist_prefix(&pk_indices, &cluster_indices).map_err(GnitzSqlError::Plan)?
    } else {
        0
    };

    // Phase 7 — the boolean `WITH (…)` properties. REPLICATED is mutually exclusive
    // with CLUSTER BY: a hash-distribution prefix is meaningless when every worker
    // already holds the whole table. The flags packing cannot make the conflict
    // unrepresentable (replicated is a boolean bit, k a byte), so reject it here.
    // STREAM composes freely with both.
    let mut props = parse_table_options(&create.table_options)?;
    props.dist_prefix_len = dist_prefix_len;
    if props.replicated && dist_prefix_len != 0 {
        return Err(GnitzSqlError::Plan(
            "REPLICATED and CLUSTER BY are mutually exclusive: a replicated table keeps \
             a full copy on every worker, so a hash-distribution prefix is meaningless"
                .into(),
        ));
    }

    // Fold every inline UNIQUE constraint into the CREATE TABLE bundle so the
    // table and its unique indices commit — or roll back — as one atomic DDL
    // zone. No more create_table-then-create_index window that could leave a
    // committed table missing its constraint. Resolve each index's catalog name
    // here (SQL-level naming), disambiguating auto-names that collide (distinct
    // column sets can render the same `…__idx_a_b_c` base when a column name
    // contains `_`); `create_table` allocates the index ids, derives column types
    // from `cols` (so a UNIQUE+FK column's rewritten type is used), and assembles
    // the IDX_TAB family. Types were pre-validated above. The table is new, so no
    // *pre-existing* index can collide (index names embed the table name); only
    // auto-names within this bundle can collide with each other or with an
    // explicit constraint name.
    let mut taken: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Explicit constraint names first — fixed points that auto-names route around.
    // Two of them under one canonical name would write two IDX_TAB rows the
    // catalog cannot both reach by name, so the collision is an error here rather
    // than a silently-shadowed index.
    for (_, constraint_name) in &unique_cols {
        if let Some(n) = constraint_name {
            if !taken.insert(canonical_user_name(n)?) {
                return Err(GnitzSqlError::Plan(format!("duplicate constraint name '{n}'")));
            }
        }
    }
    // Both `unique_cols` push sites reject a set already present, so an auto-name
    // collision here is always between *distinct* sets rendering the same base.
    let mut index_names: Vec<String> = Vec::with_capacity(unique_cols.len());
    for (col_indices, constraint_name) in &unique_cols {
        let name = match constraint_name {
            Some(n) => canonical_user_name(n)?,
            None => {
                let col_names: Vec<&str> = col_indices.iter().map(|&c| cols[c as usize].name.as_str()).collect();
                let base = default_index_name(schema_name, &table_name, &col_names);
                let name = disambiguate_index_name(base, &taken);
                taken.insert(name.clone());
                name
            }
        };
        index_names.push(name);
    }
    let unique_indexes: Vec<InlineUniqueIndex> = unique_cols
        .iter()
        .zip(&index_names)
        .map(|((col_indices, _), name)| InlineUniqueIndex {
            col_indices: col_indices.as_slice(),
            name: name.as_str(),
        })
        .collect();

    let tid = client.create_table(schema_name, &table_name, &cols, &pk_indices, props, &unique_indexes)?;

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
                // Reject a leading-`_` target planner-side: no user object can
                // carry such a name (creation validates it too), and this keeps
                // synthesized hidden views (`__h…`) undroppable by name — they
                // are only removed by the owning view's cascade.
                validate_user_name(&name)?;
                client.drop_table(schema_name, &name)?;
            }
            ObjectType::View => {
                validate_user_name(&name)?;
                client.drop_view(schema_name, &name)?;
            }
            ObjectType::Index => {
                // Mirror CREATE INDEX's name rule: a clearer planner-side
                // error, one fewer round-trip — the engine's own internal-index
                // drop refusal stays the backstop.
                validate_user_index_name(&name)?;
                // Plain DROP INDEX drops loudly (like DROP TABLE/VIEW); only
                // ALTER TABLE ... DROP CONSTRAINT IF EXISTS passes `true`.
                client.drop_index_by_name(&name, false)?;
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
    let explicit_name = ci.name.as_ref().map(|n| extract_name(n, "CREATE INDEX")).transpose()?;
    create_index_core(
        client,
        schema_name,
        binder,
        &table_name,
        &ci.columns,
        ci.unique,
        explicit_name,
        "CREATE INDEX",
    )
}

/// The shared CREATE INDEX / `ALTER TABLE … ADD CONSTRAINT UNIQUE` core: validate
/// the optional explicit name, resolve the base-table target and its indexed
/// columns, generate or disambiguate the auto name, and create the index. `ctx`
/// names the surface for error messages. Returns `IndexCreated { index_id }` — so
/// ADD CONSTRAINT UNIQUE reuses the uniform create result surface.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_index_core(
    client: &mut GnitzClient,
    schema_name: &str,
    binder: &mut Binder<'_>,
    table_name: &str,
    columns: &[sqlparser::ast::IndexColumn],
    is_unique: bool,
    explicit_name: Option<String>,
    ctx: &str,
) -> Result<SqlResult, GnitzSqlError> {
    // A user-supplied index/constraint name flows through to the IDX_TAB row so
    // `DROP INDEX`/`DROP CONSTRAINT <name>` resolves it. Validate it before
    // anything else: a malformed name would persist and be undroppable.
    if let Some(ref name) = explicit_name {
        validate_user_index_name(name)?;
    }

    if columns.is_empty() {
        return Err(GnitzSqlError::Bind(format!("{ctx}: at least one column required")));
    }

    // Resolve the target as a base table: this rejects a view (read-only — a
    // view's store is maintained solely by its circuit, so indexing a snapshot of
    // derived data has no defined semantics) with a precise error, and yields the
    // schema used to resolve the indexed columns. The client write below takes the
    // already-resolved (table_id, col_indices), so the name is resolved once.
    let target = binder.resolve_base_table(client, table_name)?;
    let (table_id, schema) = (target.tid, &target.schema);

    let (col_names, col_indices) = resolve_index_columns(columns, &schema.columns, ctx)?;

    // Limit pre-check — runs before the client push so an over-limit index
    // raises a clean planner error here, not after the IDX_TAB row already
    // committed. `index_key_types` is the same promotion + arity/stride
    // validation the engine's `make_index_schema` runs, so the friendly error
    // here and the engine backstop can never disagree.
    let col_types: Vec<TypeCode> = col_indices
        .iter()
        .map(|&c| schema.columns[c as usize].type_code)
        .collect();
    // Per-column eligibility first, so an ineligible column is named rather than
    // reaching `index_key_types`, whose verdict is a bare type code.
    reject_unindexable_columns(&col_names, &col_types, ctx)?;
    let raw_types: Vec<u8> = col_types.iter().map(|&tc| tc as u8).collect();
    gnitz_core::index_key_types(&raw_types, schema.pk_count(), schema.pk_stride())
        .map_err(GnitzSqlError::Unsupported)?; // arity + combined-key stride

    let index_name = match explicit_name {
        Some(name) => name, // explicit: an exact-name collision still errors in `create_index`
        None => {
            let base = default_index_name(schema_name, table_name, &col_names);
            let existing = client.index_name_cols()?;
            // A prior *auto-named* index on this exact column set is the same index.
            // An FK-backing or explicitly-named index on these columns carries a
            // different name, so it never blocks a distinct auto-name.
            if existing
                .iter()
                .any(|(name, cols)| name == &base && cols.as_slice() == col_indices.as_slice())
            {
                return Err(GnitzSqlError::Plan(format!(
                    "an index on these columns already exists as '{base}'"
                )));
            }
            let taken: std::collections::HashSet<String> = existing.into_iter().map(|(n, _)| n).collect();
            disambiguate_index_name(base, &taken)
        }
    };

    let index_id = client.create_index(table_id, &col_indices, &col_types, &index_name, is_unique)?;

    Ok(SqlResult::IndexCreated { index_id })
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
