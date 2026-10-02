//! DDL: CREATE TABLE (with FK resolution, UNIQUE, CLUSTER BY, REPLICATED), DROP,
//! and CREATE INDEX.

use super::guard::{
    kv_options, reject_index_type_and_options, reject_unhonored_column_options, reject_unhonored_fk_fields,
    reject_unhonored_pk_fields, reject_unhonored_unique_fields, ColumnOptionSite,
};
use crate::ast_util::{extract_index_name, extract_object_name, index_column_ident, simple_ident_expr};
use crate::bind::{find_unique_column, Catalog};
use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::rules::{canonical_user_name, reject_duplicate_names, require_class, ClassWant};
use crate::types::column_def;
use crate::SqlResult;
use gnitz_core::{FkTarget, GnitzClient, InlineForeignKey, InlineUniqueIndex, Schema};
use gnitz_expr::SchemaFacts;
use gnitz_wire::sys_rows::FkRef;
use gnitz_wire::TableDistribution;
use gnitz_wire::{ColType, ColumnDef, TableProps, TypeCode};
use sqlparser::ast::{
    ColumnOption, CreateTableOptions, Expr, ForeignKeyConstraint, ObjectType, PrimaryKeyConstraint, TableConstraint,
    UniqueConstraint, Value, ValueWithSpan, WrappedCollection,
};
use std::collections::HashSet;

/// The canonical catalog name of an unnamed index:
/// `{schema}__{table}__idx_{col1}_{col2}…`.
fn default_index_name(schema_name: &str, table_name: &str, col_names: &[&str]) -> String {
    let name = format!("{schema_name}__{table_name}__idx_{}", col_names.join("_"));
    // A quoted column name holds any byte; an index name holds identifier bytes.
    name.bytes()
        .map(|b| {
            if gnitz_wire::is_valid_ident_char(b) {
                b.to_ascii_lowercase() as char
            } else {
                '_'
            }
        })
        .collect()
}

/// `base` if free, else the first `{base}_{n}` (n ≥ 2) not in `taken`.
fn disambiguate_index_name(base: String, taken: &HashSet<String>) -> String {
    if !taken.contains(&base) {
        return base;
    }
    for n in 2u32.. {
        let candidate = format!("{base}_{n}");
        if !taken.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!("u32 range exhausted")
}

/// Whether [`disambiguate_index_name`] can have named an index `name` from `base`.
fn is_auto_name(name: &str, base: &str) -> bool {
    match name.strip_prefix(base) {
        Some("") => true,
        Some(rest) => rest
            .strip_prefix('_')
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
        None => false,
    }
}

/// Reject `key`, column indices into `cols`, as an index key the engine could not
/// build, naming the column.
fn reject_unbuildable_index_key(
    cols: &[ColumnDef],
    key: &[u32],
    src_pk_count: usize,
    role: &str,
) -> Result<(), GnitzSqlError> {
    let types: Vec<TypeCode> = key.iter().map(|&c| cols[c as usize].ty.tc).collect();
    match gnitz_wire::index_key_types(&types, src_pk_count) {
        Ok(_) => Ok(()),
        Err(gnitz_wire::IndexKeyRule::NotEligible { col, type_code }) => Err(GnitzSqlError::Rejected(format!(
            "{role}: column '{}' of type {type_code} cannot be an index key",
            cols[key[col] as usize].name
        ))),
        Err(arity @ gnitz_wire::IndexKeyRule::ArityOutOfRange { .. }) => {
            Err(GnitzSqlError::Rejected(arity.to_string()))
        }
    }
}

/// The FK child-type rule: a child adopts the referenced type, so its type must
/// equal it or hold a domain that fits inside it, and under DECIMAL adopting
/// another scale would restate its values.
fn check_fk_type_compat(fk_col_type: ColType, parent_col_type: ColType) -> Result<(), GnitzSqlError> {
    let fits = fk_col_type.decimal_domains_match(parent_col_type)
        && (fk_col_type.tc == parent_col_type.tc || fk_col_type.tc.int_domain_fits(parent_col_type.tc));
    if !fits {
        return Err(GnitzSqlError::Rejected(format!(
            "FK type mismatch: column type {fk_col_type} cannot reference column type \
             {parent_col_type} — the child column adopts the referenced type, which would \
             narrow or re-sign {fk_col_type}; declare the child with a type whose range \
             fits within {parent_col_type}",
        )));
    }
    Ok(())
}

/// One `REFERENCES` site: the child column and the target the clause named.
/// Both spellings — the column option and the table-level constraint — collapse
/// to this before either is resolved.
struct FkSite<'a> {
    col_idx: usize,
    foreign_table: &'a sqlparser::ast::ObjectName,
    referred_columns: &'a [sqlparser::ast::Ident],
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
            .ok_or_else(|| GnitzSqlError::Rejected(format!("{ctx} column '{name}' not found")))?
            as u32;
        if indices.contains(&idx) {
            return Err(GnitzSqlError::Rejected(format!("{ctx}: duplicate column '{name}'")));
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
        return Err(GnitzSqlError::Rejected(
            "multi-column FOREIGN KEY references are not supported".into(),
        ));
    }
    match referred_columns.first() {
        Some(ident) => find_unique_column(cols, &ident.value)?.ok_or_else(|| {
            GnitzSqlError::Rejected(format!(
                "FK references column '{}' not found in table '{}'",
                ident.value, ref_table,
            ))
        }),
        None => pk_single.ok_or_else(|| {
            GnitzSqlError::Rejected(format!(
                "FK against '{ref_table}' must name the referenced column (its primary key is not a single column)",
            ))
        }),
    }
}

/// Resolve a self-referencing FK against the in-flight column list (the table
/// is not yet registered in the catalog). The referenced column must be the
/// table's lone PK column.
fn resolve_fk_target_inline(
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
    ref_table: &str,
    site: &FkSite<'_>,
) -> Result<(FkTarget, ColType), GnitzSqlError> {
    let pk_single = (current_pk_cols.len() == 1).then(|| current_pk_cols[0] as usize);
    let ref_col_idx = resolve_referred_column(site.referred_columns, ref_table, current_cols, pk_single)?;

    if pk_single != Some(ref_col_idx) {
        return Err(GnitzSqlError::Rejected(format!(
            "self-referencing FK must reference the primary key of '{}'; \
             column '{}' is not the lone PK",
            ref_table, current_cols[ref_col_idx].name,
        )));
    }

    check_fk_type_compat(current_cols[site.col_idx].ty, current_cols[ref_col_idx].ty)?;
    Ok((
        FkTarget::SelfTable { col: ref_col_idx as u32 },
        current_cols[ref_col_idx].ty,
    ))
}

/// Resolve a REFERENCES clause naming `ref_table` to its target and the parent
/// column's type. The referenced column is a legal target iff it is the
/// parent's lone PK column or it carries its own single-column UNIQUE index.
/// Validates that the child's type is compatible with the referenced column's
/// and returns that type so the caller can widen the child column.
fn resolve_fk_target(
    cat: &Catalog<'_>,
    site: &FkSite<'_>,
    ref_table: &str,
    current_table_name: &str,
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
) -> Result<(FkTarget, ColType), GnitzSqlError> {
    // Self-referencing FK: the table being created is not yet in the catalog,
    // so resolve the referenced column against the in-flight column list.
    if ref_table.eq_ignore_ascii_case(current_table_name) {
        return resolve_fk_target_inline(current_cols, current_pk_cols, ref_table, site);
    }

    let ref_rel = cat.probe_relation(ref_table)?;
    let ref_schema = &ref_rel.schema;
    // The PK/UNIQUE tests below read only the schema, and both a view's and a
    // stream's PK look exactly like a base table's without being the unique, stored
    // key the parent probe reads.
    require_class(&ref_rel, ref_table, ClassWant::BaseTable, "a FOREIGN KEY target")?;
    let ref_tid = ref_rel.tid;

    let pk_single = ref_schema.lone_pk_col();
    let ref_col_idx = resolve_referred_column(site.referred_columns, ref_table, &ref_schema.columns, pk_single)?;

    // Legal target iff the referenced column is the parent's lone PK or carries a
    // UNIQUE index of its own. A composite unique `(a, b)` does not make `a`
    // unique, so the column list is matched exactly, as the engine's gate does.
    if pk_single != Some(ref_col_idx) {
        let unique = ref_rel
            .indexes
            .iter()
            .any(|m| m.cols.as_slice() == [ref_col_idx as u32] && m.is_unique);
        if !unique {
            return Err(GnitzSqlError::Rejected(format!(
                "FK against table '{}' must reference the primary key or a column \
                 with a UNIQUE index; column '{}' has neither",
                ref_table, ref_schema.columns[ref_col_idx].name,
            )));
        }
    }

    // Child column widens to the referenced parent column's type.
    check_fk_type_compat(current_cols[site.col_idx].ty, ref_schema.columns[ref_col_idx].ty)?;

    Ok((
        FkTarget::Table(FkRef {
            table_id: ref_tid,
            col: ref_col_idx as u32,
        }),
        ref_schema.columns[ref_col_idx].ty,
    ))
}

/// The boolean properties of `CREATE TABLE … WITH (…)`. A `Keyed` distribution's
/// prefix is left at its default; the CLUSTER BY phase fills it.
fn parse_table_options(table_options: &CreateTableOptions) -> Result<TableProps, GnitzSqlError> {
    let [replicated, stream] = kv_options(table_options, "CREATE TABLE", ["replicated", "stream"])?;
    let flag = |key: &str, value: Option<&Expr>| match value {
        None => Ok(false),
        Some(Expr::Value(ValueWithSpan { value: Value::Boolean(b), .. })) => Ok(*b),
        Some(_) => Err(GnitzSqlError::Rejected(format!(
            "WITH ({key} = …) expects a boolean (true/false)"
        ))),
    };
    let distribution = match flag("replicated", replicated)? {
        true => TableDistribution::Replicated,
        false => TableDistribution::default(),
    };
    Ok(TableProps {
        stream: flag("stream", stream)?,
        serial: false,
        distribution,
    })
}

/// One UNIQUE constraint as written: the column list — one element for a
/// column-level `UNIQUE`, the whole ordered list for a table-level one — and the
/// `CONSTRAINT <name>` it carried, canonical.
struct UniqueDecl {
    cols: Vec<u32>,
    name: Option<String>,
}

/// Record a UNIQUE constraint, refusing a column set already recorded:
/// `a INT UNIQUE UNIQUE` and `UNIQUE(a), UNIQUE(a)` are one constraint written
/// twice, and one index is all either could create.
fn push_unique(
    unique: &mut Vec<UniqueDecl>,
    cols: Vec<u32>,
    col_names: &[&str],
    name: Option<&sqlparser::ast::Ident>,
) -> Result<(), GnitzSqlError> {
    if unique.iter().any(|u| u.cols == cols) {
        return Err(GnitzSqlError::Rejected(format!(
            "duplicate UNIQUE constraint on column(s) ({})",
            col_names.join(", ")
        )));
    }
    let name = name.map(|n| canonical_user_name(&n.value)).transpose()?;
    unique.push(UniqueDecl { cols, name });
    Ok(())
}

/// Everything `CREATE TABLE`'s own text declares.
struct Declared<'a> {
    cols: Vec<ColumnDef>,
    pk: Vec<u32>,
    /// The columns declared SERIAL.
    serial: Vec<u32>,
    fk_sites: Vec<FkSite<'a>>,
    unique: Vec<UniqueDecl>,
}

/// Read `create` into a [`Declared`]. Takes neither a catalog nor a schema name,
/// so nothing here can resolve to a relation: a name is a column of the in-flight
/// list, or it is rejected.
fn collect_declarations(create: &sqlparser::ast::CreateTable) -> Result<Declared<'_>, GnitzSqlError> {
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(create.columns.len());
    let mut inline_pk: Vec<u32> = Vec::new();
    let mut fk_sites: Vec<FkSite<'_>> = Vec::new();
    let mut unique: Vec<UniqueDecl> = Vec::new();
    let mut serial: Vec<u32> = Vec::new();

    // The column defs and every site a column option declares.
    for (i, col) in create.columns.iter().enumerate() {
        let (def, is_serial) = column_def(col)?;
        cols.push(def);
        if is_serial {
            serial.push(i as u32);
        }
        for opt in &col.options {
            match &opt.option {
                ColumnOption::PrimaryKey(_) => inline_pk.push(i as u32),
                ColumnOption::ForeignKey(ForeignKeyConstraint { foreign_table, referred_columns, .. }) => fk_sites
                    .push(FkSite {
                        col_idx: i,
                        foreign_table,
                        referred_columns,
                    }),
                ColumnOption::Unique(_) => push_unique(
                    &mut unique,
                    vec![i as u32],
                    &[col.name.value.as_str()],
                    opt.name.as_ref(),
                )?,
                _ => {}
            }
        }
    }
    if inline_pk.len() > 1 {
        return Err(GnitzSqlError::Rejected("Multiple PRIMARY KEYs defined".into()));
    }

    // The same three kinds again, table-level and in declaration order, with the
    // columns named rather than positional.
    let mut table_pk: Option<Vec<u32>> = None;
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(PrimaryKeyConstraint { columns: pk_cols, .. }) => {
                if table_pk.is_some() {
                    return Err(GnitzSqlError::Rejected("Multiple PRIMARY KEYs defined".into()));
                }
                // Resolved before the inline conflict is reported, so an unknown
                // column name is named rather than eclipsed by it.
                let resolved = resolve_index_columns(pk_cols, &cols, "PRIMARY KEY")?.1;
                if !inline_pk.is_empty() {
                    return Err(GnitzSqlError::Rejected("Multiple PRIMARY KEYs defined".into()));
                }
                table_pk = Some(resolved);
            }
            TableConstraint::ForeignKey(ForeignKeyConstraint {
                columns, foreign_table, referred_columns, ..
            }) => {
                if columns.len() != 1 {
                    return Err(GnitzSqlError::Rejected(
                        "multi-column FOREIGN KEY constraints are not supported".into(),
                    ));
                }
                let local_col_name = &columns[0].value;
                let col_idx = find_unique_column(&cols, local_col_name)?.ok_or_else(|| {
                    GnitzSqlError::Rejected(format!(
                        "FOREIGN KEY column '{local_col_name}' not found in table definition"
                    ))
                })?;
                fk_sites.push(FkSite { col_idx, foreign_table, referred_columns });
            }
            TableConstraint::Unique(UniqueConstraint { name: name_ident, columns, .. }) => {
                if columns.is_empty() {
                    return Err(GnitzSqlError::Rejected("UNIQUE constraint cannot be empty".into()));
                }
                let (col_names, col_indices) = resolve_index_columns(columns, &cols, "UNIQUE")?;
                push_unique(&mut unique, col_indices, &col_names, name_ident.as_ref())?;
            }
            _ => {}
        }
    }

    Ok(Declared {
        cols,
        pk: table_pk.unwrap_or(inline_pk),
        serial,
        fk_sites,
        unique,
    })
}

/// The PK column list when a UNIQUE on it would be redundant — a lone
/// single-column PK. Empty otherwise, which no UNIQUE column list equals.
fn pk_covered_unique(pk: &[u32]) -> &[u32] {
    if pk.len() == 1 {
        pk
    } else {
        &[]
    }
}

/// A stream holds no rows: nothing to index, and nothing for a referential action
/// to check against. The engine refuses an index or an FK naming the relation by
/// id; this names the column.
fn reject_stream_constraints(d: &Declared<'_>) -> Result<(), GnitzSqlError> {
    if let Some(site) = d.fk_sites.first() {
        return Err(GnitzSqlError::Rejected(format!(
            "a stream cannot carry a FOREIGN KEY (column '{}'): it holds no rows to check against",
            d.cols[site.col_idx].name
        )));
    }
    // A UNIQUE the PK already covers builds no index, so it is no more refused
    // here than it is on a table.
    if let Some(u) = d.unique.iter().find(|u| u.cols != pk_covered_unique(&d.pk)) {
        let names: Vec<&str> = u.cols.iter().map(|&c| d.cols[c as usize].name.as_str()).collect();
        return Err(GnitzSqlError::Rejected(format!(
            "a stream cannot carry a UNIQUE constraint (column(s) {}): it holds no rows to index",
            names.join(", ")
        )));
    }
    Ok(())
}

/// Drop a UNIQUE the PK already covers: a second index on a lone single-column PK
/// would only duplicate the PK region. A *named* one is rejected instead —
/// nothing would be left to carry the name for `DROP CONSTRAINT` to resolve.
fn drop_unique_covered_by_pk(
    unique: &mut Vec<UniqueDecl>,
    cols: &[ColumnDef],
    pk: &[u32],
) -> Result<(), GnitzSqlError> {
    let covered = pk_covered_unique(pk);
    if let Some(name) = unique
        .iter()
        .find(|u| u.cols == covered)
        .and_then(|u| u.name.as_deref())
    {
        return Err(GnitzSqlError::Rejected(format!(
            "UNIQUE constraint '{name}' on the primary-key column '{}' names no index: the primary \
             key already provides the constraint",
            cols[covered[0] as usize].name
        )));
    }
    unique.retain(|u| u.cols != covered);
    Ok(())
}

/// Give each UNIQUE constraint the catalog name its index will carry: the written
/// `CONSTRAINT <name>`, which no two may share, or an auto-name free in this bundle.
fn name_unique_indexes(
    unique: Vec<UniqueDecl>,
    cols: &[ColumnDef],
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<InlineUniqueIndex>, GnitzSqlError> {
    // Written names first: they are the fixed points auto-names route around.
    let mut taken: HashSet<String> = HashSet::new();
    for u in &unique {
        if let Some(name) = &u.name {
            if !taken.insert(name.clone()) {
                return Err(GnitzSqlError::Rejected(format!("duplicate constraint name '{name}'")));
            }
        }
    }
    Ok(unique
        .into_iter()
        .map(|u| {
            let name = match u.name {
                Some(name) => name,
                None => {
                    let col_names: Vec<&str> = u.cols.iter().map(|&c| cols[c as usize].name.as_str()).collect();
                    let base = default_index_name(schema_name, table_name, &col_names);
                    let name = disambiguate_index_name(base, &taken);
                    taken.insert(name.clone());
                    name
                }
            };
            InlineUniqueIndex { col_indices: u.cols, name }
        })
        .collect())
}

/// A `CREATE TABLE`'s bundle: the table and its inline unique indexes.
pub(crate) struct TablePlan {
    pub(crate) name: String,
    pub(crate) schema: Schema,
    pub(crate) fks: Vec<InlineForeignKey>,
    pub(crate) props: TableProps,
    /// Each inline UNIQUE constraint's columns and the catalog name its index
    /// takes — auto-names already disambiguated within the bundle.
    pub(crate) unique_indexes: Vec<InlineUniqueIndex>,
}

/// Reject every `CREATE TABLE` clause the planner does not consume. Exhaustive
/// destructure (no `..`): a future `sqlparser` field stops the build.
fn reject_unhonored_create_table_clauses(create: &sqlparser::ast::CreateTable) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE TABLE";
    let sqlparser::ast::CreateTable {
        // Consumed (column/constraint contents further guarded by the column-option and table-constraint guards).
        name: _,
        columns: _,
        constraints: _,
        cluster_by: _,
        table_options: _,
        // Consumed: `plan_create_table`'s skip route.
        if_not_exists: _,
        // Rejected: each silently changes the result if dropped.
        or_replace,
        temporary,
        global,
        query,
        like,
        clone,
        inherits,
        on_commit,
        primary_key,
        partition_of,
        for_values,
        // No gnitz-honorable semantics — storage/engine/vendor metadata accepted as no-ops.
        external: _,
        dynamic: _,
        transient: _,
        volatile: _,
        iceberg: _,
        snapshot: _,
        hive_distribution: _,
        hive_formats: _,
        file_format: _,
        location: _,
        version: _,
        without_rowid: _,
        comment: _,
        on_cluster: _,
        order_by: _,
        partition_by: _,
        clustered_by: _,
        strict: _,
        copy_grants: _,
        enable_schema_evolution: _,
        change_tracking: _,
        data_retention_time_in_days: _,
        max_data_extension_time_in_days: _,
        default_ddl_collation: _,
        with_aggregation_policy: _,
        with_row_access_policy: _,
        with_storage_lifecycle_policy: _,
        with_tags: _,
        external_volume: _,
        base_location: _,
        catalog: _,
        catalog_sync: _,
        storage_serialization_policy: _,
        target_lag: _,
        warehouse: _,
        refresh_mode: _,
        initialize: _,
        require_user: _,
        diststyle: _,
        distkey: _,
        sortkey: _,
        backup: _,
    } = create;

    reject_if(query.is_some(), CTX, "AS SELECT (CTAS)")?;
    reject_if(*or_replace, CTX, "OR REPLACE (it would discard the table's rows)")?;
    reject_if(*temporary, CTX, "TEMPORARY")?;
    reject_if(global.is_some(), CTX, "GLOBAL/LOCAL")?;
    reject_if(like.is_some(), CTX, "LIKE")?;
    reject_if(clone.is_some(), CTX, "CLONE")?;
    reject_if(inherits.is_some(), CTX, "INHERITS")?;
    reject_if(on_commit.is_some(), CTX, "ON COMMIT")?;
    reject_if(primary_key.is_some(), CTX, "PRIMARY KEY expression")?;
    reject_if(partition_of.is_some() || for_values.is_some(), CTX, "PARTITION OF")?;
    Ok(())
}

/// Reject every table constraint CREATE TABLE does not honor. Honored: PRIMARY KEY, UNIQUE,
/// FOREIGN KEY target — each honored variant is descended into
/// (`reject_unhonored_{pk,unique,fk}_fields`) so an unimplemented field inside it (a referential
/// action, DEFERRABLE, NULLS NOT DISTINCT, …) is rejected too. `CHECK`, inline `INDEX`,
/// `FULLTEXT`/`SPATIAL` indexes, and the Postgres `{PRIMARY KEY,UNIQUE} USING INDEX` promotions (no
/// pre-existing index at CREATE TABLE) are rejected. Exhaustive over all 8 `TableConstraint`
/// variants (no `_`).
fn reject_unhonored_table_constraints(constraints: &[sqlparser::ast::TableConstraint]) -> Result<(), GnitzSqlError> {
    const CTX: &str = "table constraint";
    use sqlparser::ast::TableConstraint as C;
    for c in constraints {
        match c {
            // Consumed, but only the column list / constraint name — descend so an
            // unimplemented field is rejected, not silently dropped.
            C::PrimaryKey(pk) => reject_unhonored_pk_fields(pk, CTX)?,
            C::Unique(u) => reject_unhonored_unique_fields(u, CTX)?,
            C::ForeignKey(fk) => reject_unhonored_fk_fields(fk, CTX)?,
            C::Check(_) => return Err(unsupported_clause(CTX, "CHECK constraint")),
            C::Index(_) => return Err(unsupported_clause(CTX, "INDEX in table definition")),
            C::FulltextOrSpatial(_) => return Err(unsupported_clause(CTX, "FULLTEXT/SPATIAL index")),
            C::PrimaryKeyUsingIndex(_) => return Err(unsupported_clause(CTX, "PRIMARY KEY USING INDEX")),
            C::UniqueUsingIndex(_) => return Err(unsupported_clause(CTX, "UNIQUE USING INDEX")),
        }
    }
    Ok(())
}

/// Plan a `CREATE TABLE` into the bundle its commit writes; `None` when
/// `IF NOT EXISTS` finds the name taken. [`collect_declarations`] reads the statement; everything below it
/// resolves, admits and names.
/// `Err` unless `cluster` is exactly `pk`'s leading prefix, in PK order: the
/// distribution key is a leading PK prefix so write-side routing is a byte-slice
/// of the OPK region. `cols` names a column the PK does not hold.
fn validate_dist_prefix(cols: &[ColumnDef], pk: &[u32], cluster: &[u32]) -> Result<(), GnitzSqlError> {
    if cluster.is_empty() || cluster.len() > pk.len() {
        return Err(GnitzSqlError::Rejected(format!(
            "CLUSTER BY expects 1..={} leading PRIMARY KEY columns, got {}",
            pk.len(),
            cluster.len()
        )));
    }
    if let Some(&c) = cluster.iter().find(|c| !pk.contains(c)) {
        return Err(GnitzSqlError::Rejected(format!(
            "CLUSTER BY column '{}' is not a PRIMARY KEY column",
            cols[c as usize].name
        )));
    }
    if cluster != &pk[..cluster.len()] {
        return Err(GnitzSqlError::Rejected(
            "CLUSTER BY columns must be a leading prefix of the PRIMARY KEY, \
             in PK order; reorder the PK so the distribution column(s) lead"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn plan_create_table(
    create: &sqlparser::ast::CreateTable,
    cat: &Catalog<'_>,
) -> Result<Option<TablePlan>, GnitzSqlError> {
    reject_unhonored_create_table_clauses(create)?;
    let schema_name = cat.schema_name();
    let table_name = extract_object_name(&create.name, schema_name, "CREATE TABLE")?;

    // The `WITH (…)` keys and values are decidable from the statement's own text.
    // A `Keyed` distribution's prefix is the other half of `props`, and needs
    // CLUSTER BY.
    let mut props = parse_table_options(&create.table_options)?;

    // `IF NOT EXISTS` tests the NAME, not the definition — no dialect compares the
    // two — so any relation standing under it ends the statement, whatever its
    // kind. Probed only under the clause, so a plain CREATE TABLE resolves nothing.
    if create.if_not_exists && cat.probe(&table_name)?.is_some() {
        return Ok(None);
    }

    // Before any column list is resolved: a repeat would otherwise surface as an
    // ambiguous *reference*, when it is the definition that is wrong.
    reject_duplicate_names(create.columns.iter().map(|c| c.name.value.as_str()), "table definition")?;
    // Before the column defs are built, so an unsupported clause errors as itself
    // rather than as a downstream PK admission failure.
    for col in &create.columns {
        reject_unhonored_column_options(col, ColumnOptionSite::CreateTable)?;
    }
    reject_unhonored_table_constraints(&create.constraints)?;

    let declared = collect_declarations(create)?;
    if props.stream {
        reject_stream_constraints(&declared)?;
    }
    let Declared {
        mut cols,
        pk: pk_indices,
        serial,
        fk_sites,
        mut unique,
    } = declared;

    // A self-FK resolves against the whole PK, and each resolve REWRITES its child
    // column's type to the parent's — so this runs after collection, not inside it.
    // The self-references go last: one adopts the PK column's type, which a
    // cross-table FK on that column rewrites.
    let mut sites: Vec<(String, &FkSite<'_>)> = fk_sites
        .iter()
        .map(|site| {
            Ok((
                extract_object_name(site.foreign_table, schema_name, "REFERENCES")?,
                site,
            ))
        })
        .collect::<Result<_, GnitzSqlError>>()?;
    sites.sort_by_key(|(ref_table, _)| ref_table.eq_ignore_ascii_case(&table_name));
    let mut fks: Vec<InlineForeignKey> = Vec::with_capacity(sites.len());
    for (ref_table, site) in sites {
        if fks.iter().any(|fk| fk.col_idx as usize == site.col_idx) {
            return Err(GnitzSqlError::Rejected(format!(
                "column '{}' carries more than one FOREIGN KEY",
                cols[site.col_idx].name
            )));
        }
        let (fk, parent_pk_type) = resolve_fk_target(cat, site, &ref_table, &table_name, &cols, &pk_indices)?;
        fks.push(InlineForeignKey { col_idx: site.col_idx as u32, target: fk });
        cols[site.col_idx].ty = parent_pk_type;
    }

    // The null bitmap excludes the PK region, so a nullable PK has no place to
    // carry the null: PRIMARY KEY implies NOT NULL, coerced rather than rejected.
    for &i in &pk_indices {
        cols[i as usize].is_nullable = false;
    }

    let schema = Schema::from_parts(cols, pk_indices).map_err(GnitzSqlError::Rejected)?;
    let (cols, pk_indices) = (&schema.columns, &schema.pk_cols);

    // The generated id has no compound form: a column spelled SERIAL is the table's
    // whole primary key.
    if let Some(&c) = serial.iter().find(|&&c| pk_indices.as_slice() != [c]) {
        return Err(GnitzSqlError::Rejected(format!(
            "SERIAL column '{}' must be the table's only SERIAL column and its single-column PRIMARY KEY",
            cols[c as usize].name
        )));
    }
    props.serial = !serial.is_empty();

    drop_unique_covered_by_pk(&mut unique, cols, pk_indices)?;

    for u in &unique {
        reject_unbuildable_index_key(cols, &u.cols, pk_indices.len(), "UNIQUE")?;
    }
    // Every FK column carries an index of its own.
    for fk in &fks {
        reject_unbuildable_index_key(cols, &[fk.col_idx], pk_indices.len(), "FOREIGN KEY")?;
    }

    // CLUSTER BY (hash distribution key). The named columns must be the PK's
    // leading prefix in PK order; the prefix length `k` is persisted in
    // `TABLE_TAB.flags` and drives write-side routing and co-partition detection.
    // No clause ⇒ `k = 0` ⇒ default full-PK distribution.
    if let Some(cluster) = &create.cluster_by {
        let (WrappedCollection::NoWrapping(exprs) | WrappedCollection::Parentheses(exprs)) = cluster;
        let mut cluster_indices: Vec<u32> = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let col_name = simple_ident_expr(expr, "CLUSTER BY")?;
            let idx = find_unique_column(cols, col_name)?
                .ok_or_else(|| GnitzSqlError::Rejected(format!("CLUSTER BY column '{col_name}' not found")))?;
            cluster_indices.push(idx as u32);
        }
        validate_dist_prefix(cols, pk_indices, &cluster_indices)?;
        // `validate_dist_prefix` bounded the prefix by the PK arity, so the `u8`
        // is lossless.
        let prefix_len = cluster_indices.len() as u8;
        if props.distribution == TableDistribution::Replicated {
            return Err(GnitzSqlError::Rejected(gnitz_wire::replicated_with_prefix(prefix_len)));
        }
        props.distribution = TableDistribution::Keyed { prefix_len };
    }
    props.validate(pk_indices.len()).map_err(GnitzSqlError::Rejected)?;

    let unique_indexes = name_unique_indexes(unique, cols, schema_name, &table_name)?;
    Ok(Some(TablePlan {
        name: table_name,
        schema,
        fks,
        props,
        unique_indexes,
    }))
}
/// Commit a planned `CREATE TABLE`: one bundle carrying the table, its columns
/// and its inline unique indexes.
pub(crate) fn execute_create_table(
    client: &mut GnitzClient,
    schema_name: &str,
    plan: TablePlan,
) -> Result<SqlResult, GnitzSqlError> {
    let TablePlan { name, schema, fks, props, unique_indexes } = plan;
    client.create_table(schema_name, &name, &schema, &fks, props, &unique_indexes)?;
    Ok(SqlResult::Ddl)
}

pub(crate) fn execute_drop(
    client: &mut GnitzClient,
    schema_name: &str,
    object_type: &ObjectType,
    names: &[sqlparser::ast::ObjectName],
    if_exists: bool,
) -> Result<SqlResult, GnitzSqlError> {
    // The kind is the statement's, not each name's, so it is settled once.
    if !matches!(object_type, ObjectType::Table | ObjectType::View | ObjectType::Index) {
        return Err(unsupported_clause("DROP", &object_type.to_string()));
    }
    let mut targets: Vec<String> = Vec::with_capacity(names.len());
    for obj_name in names {
        // An index name is global, so a qualifier on one means nothing; a table
        // or view name is schema-scoped, and the active schema is the session's.
        targets.push(match object_type {
            ObjectType::Index => extract_index_name(obj_name, "DROP")?,
            _ => extract_object_name(obj_name, schema_name, "DROP")?,
        });
    }
    let targets: Vec<&str> = targets.iter().map(String::as_str).collect();

    // `IF EXISTS` rides the verb, which resolves its own targets — the one place
    // that can answer "no such object" without a second lookup. It softens nothing
    // else: one refusal still fails the whole statement.
    match object_type {
        ObjectType::View => client.drop_view(schema_name, &targets, if_exists)?,
        ObjectType::Index => client.drop_indexes_by_name(&targets, if_exists)?,
        _ => client.drop_table(schema_name, &targets, if_exists)?,
    }
    Ok(SqlResult::Ddl)
}

/// Which surface asked for the index, and the options only that surface can
/// spell. `ADD CONSTRAINT` is always unique and has no IF NOT EXISTS spelling,
/// so neither is representable on it.
#[derive(Clone, Copy)]
pub(crate) enum IndexSite {
    CreateIndex { unique: bool, if_not_exists: bool },
    AddConstraint,
}

impl IndexSite {
    /// How the site names itself in a rejection. Derived rather than passed
    /// beside it: the two are 1:1, and a second parameter is a hole a caller can
    /// spell the wrong way round.
    fn context(self) -> &'static str {
        match self {
            IndexSite::CreateIndex { .. } => "CREATE INDEX",
            IndexSite::AddConstraint => "ADD CONSTRAINT",
        }
    }

    fn is_unique(self) -> bool {
        match self {
            IndexSite::CreateIndex { unique, .. } => unique,
            IndexSite::AddConstraint => true,
        }
    }

    fn if_not_exists(self) -> bool {
        matches!(self, IndexSite::CreateIndex { if_not_exists: true, .. })
    }
}

/// One index the statement asks for, as its own text describes it.
pub(crate) struct IndexRequest<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) columns: &'a [sqlparser::ast::IndexColumn],
    /// The written index or constraint name, canonical.
    pub(crate) explicit_name: Option<String>,
    pub(crate) site: IndexSite,
}

/// Reject every `CreateIndex` field `execute_create_index` does not consume (`name`, `table_name`,
/// `columns`, `unique`). `using` is accepted only for the BTree default (gnitz's index is ordered /
/// range-scannable); any other type, plus `predicate` (partial index → full index), `concurrently`
/// (no non-blocking-build guarantee), `include`/`nulls_distinct`/`with` (silent default semantics)
/// are rejected. `if_not_exists` is consumed (the dispatcher's skip route).
fn reject_unhonored_create_index_clauses(ci: &sqlparser::ast::CreateIndex) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE INDEX";
    let sqlparser::ast::CreateIndex {
        name: _,
        table_name: _,
        columns: _,
        unique: _,
        if_not_exists: _, // consumed: the dispatcher's skip route
        using,
        concurrently,
        include,
        nulls_distinct,
        with,
        predicate,
        index_options,
        alter_options,
    } = ci;
    reject_if(predicate.is_some(), CTX, "WHERE (partial index)")?;
    reject_if(!alter_options.is_empty(), CTX, "ALTER options (ALGORITHM / LOCK)")?;
    reject_index_type_and_options(using, index_options, CTX)?;
    reject_if(*concurrently, CTX, "CONCURRENTLY")?;
    reject_if(!include.is_empty(), CTX, "INCLUDE (covering columns)")?;
    reject_if(nulls_distinct.is_some(), CTX, "NULLS [NOT] DISTINCT")?;
    reject_if(!with.is_empty(), CTX, "WITH (storage parameters)")?;
    Ok(())
}

pub(crate) fn execute_create_index(
    client: &mut GnitzClient,
    schema_name: &str,
    ci: &sqlparser::ast::CreateIndex,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_create_index_clauses(ci)?;
    let table_name = extract_object_name(&ci.table_name, schema_name, "CREATE INDEX")?;
    let explicit_name = ci
        .name
        .as_ref()
        .map(|n| extract_index_name(n, "CREATE INDEX"))
        .transpose()?;
    // The one resolve on this path: `create_index_core` takes the descriptor.
    let target = client.resolve_relation(schema_name, &table_name)?;
    require_class(&target, &table_name, ClassWant::BaseTable, "CREATE INDEX")?;
    create_index_core(
        client,
        schema_name,
        &target,
        &IndexRequest {
            table_name: &table_name,
            columns: &ci.columns,
            explicit_name,
            site: IndexSite::CreateIndex {
                unique: ci.unique,
                if_not_exists: ci.if_not_exists,
            },
        },
    )
}

/// The shared CREATE INDEX / `ALTER TABLE … ADD CONSTRAINT UNIQUE` core. `target`
/// is the caller's already-resolved base table, so a statement resolves its name
/// and asserts the base-table rule exactly once.
pub(crate) fn create_index_core(
    client: &mut GnitzClient,
    schema_name: &str,
    target: &gnitz_core::RelDescriptor,
    req: &IndexRequest<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let ctx = req.site.context();
    if req.columns.is_empty() {
        return Err(GnitzSqlError::Rejected(format!("{ctx}: at least one column required")));
    }

    let (table_id, schema) = (target.tid, &target.schema);
    let (col_names, col_indices) = resolve_index_columns(req.columns, &schema.columns, ctx)?;

    reject_unbuildable_index_key(&schema.columns, &col_indices, schema.pk_cols.len(), ctx)?;

    let index_name = match req.explicit_name.clone() {
        Some(name) => {
            // Index names are globally unique, so a name already standing — on
            // this table or any other — means this CREATE would fail; the clause
            // says skip it. Without it the collision errors in `create_index`.
            if req.site.if_not_exists() && client.index_rows()?.iter().any(|r| r.name == name) {
                return Ok(SqlResult::Ddl);
            }
            name
        }
        None => {
            let base = default_index_name(schema_name, req.table_name, &col_names);
            let existing = client.index_rows()?;
            let serves_request = |r: &gnitz_core::IndexRow| {
                r.owner == table_id
                    && r.cols.as_slice() == col_indices.as_slice()
                    && (r.is_unique || !req.site.is_unique())
            };
            if let Some(same) = existing
                .iter()
                .find(|r| is_auto_name(&r.name, &base) && serves_request(r))
            {
                return Err(GnitzSqlError::Rejected(format!(
                    "an index on these columns already exists as '{}'",
                    same.name
                )));
            }
            let taken: HashSet<String> = existing.into_iter().map(|r| r.name).collect();
            disambiguate_index_name(base, &taken)
        }
    };

    client.create_index(table_id, &col_indices, &index_name, req.site.is_unique())?;
    Ok(SqlResult::Ddl)
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
