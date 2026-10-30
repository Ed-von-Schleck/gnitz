//! DDL: CREATE TABLE (with FK resolution, UNIQUE, CLUSTER BY, REPLICATED), DROP,
//! and CREATE INDEX.

use super::guard::{
    fk_on_delete, read_column, reject_index_type_and_options, reject_unhonored_pk_fields,
    reject_unhonored_unique_fields, ColumnConstraint, ColumnDecl, ColumnOptionSite,
};
use super::options::parse_table_options;
use crate::catalog::Catalog;
use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::names::{extract_index_name, extract_object_name};
use crate::rules::{canonical_user_name, reject_duplicate_names, require_class, require_column, ClassWant};
use gnitz_core::{FkTarget, GnitzClient, InlineUniqueIndex, RelName, Schema, TableDef};
use gnitz_wire::{ColType, ColumnDef, FkAction, FkRef, PkColList, PkListRole};
use sqlparser::ast::{Expr, ForeignKeyConstraint, IndexColumn, ObjectType, TableConstraint, WrappedCollection};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};

/// The canonical catalog name of an unnamed index:
/// `{schema}__{table}__idx_{col1}_{col2}…`.
fn default_index_name(table: &RelName, col_names: &[&str]) -> String {
    let name = format!("{}__{}__idx_{}", table.schema(), table.name(), col_names.join("_"));
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

/// The column list of an index over `key`, or why no index can be built on it.
fn index_key(cols: &[ColumnDef], key: &[u32], src_pk_count: usize, role: &str) -> Result<PkColList, GnitzSqlError> {
    let rejected = |rule: gnitz_wire::PkRule| {
        let named = rule.named(PkListRole::ColumnList, |c| {
            cols.get(c as usize).map(|d| d.name.as_str())
        });
        GnitzSqlError::Rejected(format!("{role}: {named}"))
    };
    gnitz_wire::validate_index_key(key, cols.len(), src_pk_count, |c| cols[c as usize].ty.tc).map_err(rejected)
}

/// The FK child-type rule: a child adopts the referenced type, so its type must
/// equal it or be an integer whose domain fits inside it.
fn check_fk_type_compat(fk_col_type: ColType, parent_col_type: ColType) -> Result<(), GnitzSqlError> {
    if fk_col_type != parent_col_type && !fk_col_type.tc.int_domain_fits(parent_col_type.tc) {
        return Err(GnitzSqlError::Rejected(format!(
            "FK type mismatch: column type {fk_col_type} cannot reference column type \
             {parent_col_type} — the child column adopts the referenced type, which would \
             narrow, re-sign or restate {fk_col_type}; declare the child with a type whose range \
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
    on_delete: FkAction,
}

/// Resolve a PRIMARY KEY / UNIQUE / CREATE INDEX column list against `cols`, in
/// declared order — order drives the composite index's leading-key span and its
/// prefix seeks. `ctx` names the surface in both messages.
fn resolve_index_columns<'c>(
    columns: &'c [IndexColumn],
    cols: &[ColumnDef],
    ctx: &str,
) -> Result<(Vec<&'c str>, Vec<u32>), GnitzSqlError> {
    let mut names: Vec<&str> = Vec::with_capacity(columns.len());
    let mut indices: Vec<u32> = Vec::with_capacity(columns.len());
    for c in columns {
        let name = index_column_ident(c, ctx)?;
        let idx = require_column(cols, name).map_err(|e| e.in_clause(ctx))? as u32;
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
    ref_table: &RelName,
    cols: &[ColumnDef],
    pk_single: Option<usize>,
) -> Result<usize, GnitzSqlError> {
    if referred_columns.len() > 1 {
        return Err(GnitzSqlError::Rejected(
            "multi-column FOREIGN KEY references are not supported".into(),
        ));
    }
    match referred_columns.first() {
        Some(ident) => {
            require_column(cols, &ident.value).map_err(|e| e.in_clause(&format!("FOREIGN KEY REFERENCES {ref_table}")))
        }
        None => pk_single.ok_or_else(|| {
            GnitzSqlError::Rejected(format!(
                "FK against '{ref_table}' must name the referenced column (its primary key is not a single column)",
            ))
        }),
    }
}

/// Resolve the REFERENCES clause of `site` to its target and the parent column's
/// type, which the child column adopts once checked compatible. A
/// self-reference (`ref_table` is the table being created) resolves against the
/// in-flight `cols` and `pk`, the table not being in the catalog yet; any other
/// against the registered relation. The engine holds the target to a base
/// table's lone PK or a column with a single-column UNIQUE index.
fn resolve_fk_target(
    cat: &dyn Catalog,
    site: &FkSite<'_>,
    ref_table: &RelName,
    self_ref: bool,
    cols: &[ColumnDef],
    pk: &[u32],
) -> Result<(FkTarget, ColType), GnitzSqlError> {
    let probed;
    let (parent_cols, pk_single, parent) = match self_ref {
        true => (cols, (pk.len() == 1).then(|| pk[0] as usize), None),
        false => {
            probed = cat.probe_relation(ref_table)?;
            (
                probed.schema.columns(),
                probed.schema.layout().lone_pk_col(),
                Some(probed.tid),
            )
        }
    };
    let col = resolve_referred_column(site.referred_columns, ref_table, parent_cols, pk_single)?;
    let parent_ty = parent_cols[col].ty;
    check_fk_type_compat(cols[site.col_idx].ty, parent_ty)?;
    let (col, on_delete) = (col as u32, site.on_delete);
    let target = match parent {
        None => FkTarget::SelfTable { col, on_delete },
        Some(table_id) => FkTarget::Table(FkRef { table_id, col, on_delete }),
    };
    Ok((target, parent_ty))
}

/// A strictly simple identifier expression's name, or the shared
/// "column must be a simple identifier" rejection. Backs [`index_column_ident`]
/// and the CLUSTER BY column list (a `Vec<Expr>` in sqlparser).
fn simple_ident_expr<'a>(e: &'a Expr, context: &str) -> Result<&'a str, GnitzSqlError> {
    match e {
        Expr::Identifier(id) => Ok(&id.value),
        _ => Err(GnitzSqlError::Rejected(format!(
            "{context}: column must be a simple identifier"
        ))),
    }
}

/// The bare column name of an `IndexColumn` (a PRIMARY KEY / UNIQUE constraint
/// column or a CREATE INDEX column), which gnitz requires to be a simple
/// identifier. Any expression, operator class, or ordering qualifier is rejected
/// — gnitz indexes are ordered ascending by key bytes and carry no per-column
/// ordering. Shared by the CREATE INDEX loop and the table-level PK/UNIQUE loops.
fn index_column_ident<'a>(c: &'a IndexColumn, context: &str) -> Result<&'a str, GnitzSqlError> {
    reject_if(
        c.operator_class.is_some(),
        context,
        "an operator class on an index column",
    )?;
    reject_if(
        c.column.options.asc.is_some() || c.column.options.nulls_first.is_some() || c.column.with_fill.is_some(),
        context,
        "per-column ordering (ASC/DESC, NULLS FIRST/LAST, WITH FILL)",
    )?;
    simple_ident_expr(&c.column.expr, context)
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
        let ColumnDecl { def, serial: is_serial, constraints } = read_column(col, ColumnOptionSite::CreateTable)?;
        cols.push(def);
        if is_serial {
            serial.push(i as u32);
        }
        for constraint in constraints {
            match constraint {
                ColumnConstraint::PrimaryKey => inline_pk.push(i as u32),
                ColumnConstraint::ForeignKey { fk, on_delete } => fk_sites.push(FkSite {
                    col_idx: i,
                    foreign_table: &fk.foreign_table,
                    referred_columns: &fk.referred_columns,
                    on_delete,
                }),
                ColumnConstraint::Unique(name) => {
                    push_unique(&mut unique, vec![i as u32], &[col.name.value.as_str()], name)?
                }
            }
        }
    }
    if inline_pk.len() > 1 {
        return Err(GnitzSqlError::Rejected("Multiple PRIMARY KEYs defined".into()));
    }

    // The same three kinds again, table-level and in declaration order, with the
    // columns named rather than positional. Exhaustive over `TableConstraint`: a
    // consumed variant is descended into (`reject_unhonored_{pk,unique}_fields`,
    // `fk_on_delete`) so an unimplemented field inside it (ON UPDATE, DEFERRABLE,
    // NULLS [NOT] DISTINCT, …) is rejected rather than dropped; CHECK, inline
    // INDEX, FULLTEXT/SPATIAL and the `{PRIMARY KEY,UNIQUE} USING INDEX`
    // promotions (no pre-existing index at CREATE TABLE) are rejected.
    const CTX: &str = "table constraint";
    let mut table_pk: Option<Vec<u32>> = None;
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(pk) => {
                reject_unhonored_pk_fields(pk, CTX)?;
                // Resolved before the conflict is reported, so an unknown column
                // name is named rather than eclipsed by it.
                let resolved = resolve_index_columns(&pk.columns, &cols, "PRIMARY KEY")?.1;
                if table_pk.is_some() || !inline_pk.is_empty() {
                    return Err(GnitzSqlError::Rejected("Multiple PRIMARY KEYs defined".into()));
                }
                table_pk = Some(resolved);
            }
            TableConstraint::ForeignKey(fk) => {
                let on_delete = fk_on_delete(fk, CTX)?;
                let ForeignKeyConstraint {
                    columns, foreign_table, referred_columns, ..
                } = fk;
                if columns.len() != 1 {
                    return Err(GnitzSqlError::Rejected(
                        "multi-column FOREIGN KEY constraints are not supported".into(),
                    ));
                }
                let local_col_name = &columns[0].value;
                let col_idx = require_column(&cols, local_col_name).map_err(|e| e.in_clause("FOREIGN KEY"))?;
                fk_sites.push(FkSite {
                    col_idx,
                    foreign_table,
                    referred_columns,
                    on_delete,
                });
            }
            TableConstraint::Unique(u) => {
                reject_unhonored_unique_fields(u, CTX)?;
                if u.columns.is_empty() {
                    return Err(GnitzSqlError::Rejected("UNIQUE constraint cannot be empty".into()));
                }
                let (col_names, col_indices) = resolve_index_columns(&u.columns, &cols, "UNIQUE")?;
                push_unique(&mut unique, col_indices, &col_names, u.name.as_ref())?;
            }
            TableConstraint::Check(_) => return Err(unsupported_clause(CTX, "CHECK constraint")),
            TableConstraint::Index(_) => return Err(unsupported_clause(CTX, "INDEX in table definition")),
            TableConstraint::FulltextOrSpatial(_) => return Err(unsupported_clause(CTX, "FULLTEXT/SPATIAL index")),
            TableConstraint::PrimaryKeyUsingIndex(_) => return Err(unsupported_clause(CTX, "PRIMARY KEY USING INDEX")),
            TableConstraint::UniqueUsingIndex(_) => return Err(unsupported_clause(CTX, "UNIQUE USING INDEX")),
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

/// Drop a UNIQUE the PK already covers: a second index on a lone single-column PK
/// would only duplicate the PK region. A *named* one is rejected instead —
/// nothing would be left to carry the name for `DROP CONSTRAINT` to resolve.
fn drop_unique_covered_by_pk(
    unique: &mut Vec<UniqueDecl>,
    cols: &[ColumnDef],
    pk: &[u32],
) -> Result<(), GnitzSqlError> {
    let &[pk_col] = pk else {
        return Ok(());
    };
    if let Some(name) = unique
        .iter()
        .find(|u| u.cols == [pk_col])
        .and_then(|u| u.name.as_deref())
    {
        return Err(GnitzSqlError::Rejected(format!(
            "UNIQUE constraint '{name}' on the primary-key column '{}' names no index: the primary \
             key already provides the constraint",
            cols[pk_col as usize].name
        )));
    }
    unique.retain(|u| u.cols != [pk_col]);
    Ok(())
}

/// The catalog name a UNIQUE constraint's index will carry.
enum IndexName {
    /// The written `CONSTRAINT <name>`.
    Written(String),
    /// The auto-name base, which [`PlannedTable::named`] frees of live names.
    Auto(String),
}

/// Key each UNIQUE constraint and settle its name: the written one, which no two
/// may share, or the auto-name base. `pk_count` is the table's PK arity.
fn name_unique_indexes(
    unique: Vec<UniqueDecl>,
    cols: &[ColumnDef],
    pk_count: usize,
    table: &RelName,
) -> Result<Vec<(PkColList, IndexName)>, GnitzSqlError> {
    let mut written: HashSet<&str> = HashSet::new();
    for u in &unique {
        if let Some(name) = &u.name {
            if !written.insert(name) {
                return Err(GnitzSqlError::Rejected(format!("duplicate constraint name '{name}'")));
            }
        }
    }
    unique
        .into_iter()
        .map(|u| {
            let key = index_key(cols, &u.cols, pk_count, "UNIQUE")?;
            let name = match u.name {
                Some(name) => IndexName::Written(name),
                None => {
                    let col_names: Vec<&str> = u.cols.iter().map(|&c| cols[c as usize].name.as_str()).collect();
                    IndexName::Auto(default_index_name(table, &col_names))
                }
            };
            Ok((key, name))
        })
        .collect()
}

/// A planned `CREATE TABLE` whose unnamed UNIQUE indexes still hold their base
/// auto-name: index names are global, so only the live catalog says which suffix
/// is free.
pub(crate) struct PlannedTable {
    /// The bundle, its `unique_indexes` empty until [`PlannedTable::named`].
    def: TableDef,
    unique: Vec<(PkColList, IndexName)>,
}

impl PlannedTable {
    /// The bundle with each auto-name moved to the first `{base}` / `{base}_{n}`
    /// free of `live` and of every other name in the bundle. A written name is
    /// left as written: a live collision with one is the engine's refusal.
    pub(crate) fn named(self, live: impl IntoIterator<Item = String>) -> TableDef {
        let mut taken: HashSet<String> = live.into_iter().collect();
        taken.extend(self.unique.iter().filter_map(|(_, name)| match name {
            IndexName::Written(name) => Some(name.clone()),
            IndexName::Auto(_) => None,
        }));
        let unique_indexes = self
            .unique
            .into_iter()
            .map(|(cols, name)| {
                let name = match name {
                    IndexName::Written(name) => name,
                    IndexName::Auto(base) => {
                        let name = disambiguate_index_name(base, &taken);
                        taken.insert(name.clone());
                        name
                    }
                };
                InlineUniqueIndex { cols, name }
            })
            .collect();
        TableDef { unique_indexes, ..self.def }
    }

    fn has_auto_name(&self) -> bool {
        self.unique.iter().any(|(_, name)| matches!(name, IndexName::Auto(_)))
    }
}

/// Write a planned `CREATE TABLE`, reading the live index names only when an
/// unnamed UNIQUE needs one.
pub(crate) async fn create_planned_table(client: &mut GnitzClient, plan: PlannedTable) -> Result<(), GnitzSqlError> {
    let live: Vec<String> = if plan.has_auto_name() {
        client.index_rows().await?.into_iter().map(|r| r.name).collect()
    } else {
        Vec::new()
    };
    client.create_table(&plan.named(live)).await?;
    Ok(())
}

/// Reject every `CREATE TABLE` clause the planner does not consume.
fn reject_unhonored_create_table_clauses(create: &sqlparser::ast::CreateTable) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE TABLE";
    let sqlparser::ast::CreateTable {
        // Consumed; the columns and constraints are read by `collect_declarations`.
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

/// The distribution prefix length `cluster` declares, or `Err` unless it is
/// exactly `pk`'s leading prefix, in PK order: the distribution key is a leading
/// PK prefix so write-side routing is a byte-slice of the OPK region. `cols`
/// names a column the PK does not hold.
fn dist_prefix_len(cols: &[ColumnDef], pk: &[u32], cluster: &[u32]) -> Result<u8, GnitzSqlError> {
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
    Ok(u8::try_from(cluster.len()).expect("a PRIMARY KEY prefix fits a u8"))
}

/// Plan a `CREATE TABLE` into the bundle its commit writes, its UNIQUE auto-names
/// settled at write time; `None` when `IF NOT EXISTS` finds the name taken.
/// [`collect_declarations`] reads the statement; everything below it resolves,
/// admits and names.
pub(crate) fn plan_create_table(
    create: &sqlparser::ast::CreateTable,
    cat: &dyn Catalog,
) -> Result<Option<PlannedTable>, GnitzSqlError> {
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

    let Declared {
        mut cols,
        pk: pk_indices,
        serial,
        fk_sites,
        mut unique,
    } = collect_declarations(create)?;

    // A self-FK resolves against the whole PK, and each resolve REWRITES its child
    // column's type to the parent's — so this runs after collection, not inside it.
    // The self-references go last: one adopts the PK column's type, which a
    // cross-table FK on that column rewrites.
    let mut sites: Vec<(RelName, &FkSite<'_>)> = fk_sites
        .iter()
        .map(|site| {
            Ok((
                extract_object_name(site.foreign_table, schema_name, "REFERENCES")?,
                site,
            ))
        })
        .collect::<Result<_, GnitzSqlError>>()?;
    sites.sort_by_key(|(ref_table, _)| *ref_table == table_name);
    let mut fks = BTreeMap::new();
    for (ref_table, site) in sites {
        let Entry::Vacant(slot) = fks.entry(site.col_idx) else {
            return Err(GnitzSqlError::Rejected(format!(
                "column '{}' carries more than one FOREIGN KEY",
                cols[site.col_idx].name
            )));
        };
        let self_ref = ref_table == table_name;
        let (fk, parent_pk_type) = resolve_fk_target(cat, site, &ref_table, self_ref, &cols, &pk_indices)?;
        slot.insert(fk);
        cols[site.col_idx].ty = parent_pk_type;
    }

    // The null bitmap excludes the PK region, so a nullable PK has no place to
    // carry the null: PRIMARY KEY implies NOT NULL, coerced rather than rejected.
    for &i in &pk_indices {
        cols[i as usize].is_nullable = false;
    }

    let schema = Schema::from_parts(cols, &pk_indices).map_err(GnitzSqlError::Rejected)?;
    let (cols, pk_indices) = (schema.columns(), schema.pk_cols());

    // The generated id has no compound form: a column spelled SERIAL is the table's
    // whole primary key.
    if let Some(&c) = serial.iter().find(|&&c| pk_indices != [c]) {
        return Err(GnitzSqlError::Rejected(format!(
            "SERIAL column '{}' must be the table's only SERIAL column and its single-column PRIMARY KEY",
            cols[c as usize].name
        )));
    }
    props.serial = !serial.is_empty();

    drop_unique_covered_by_pk(&mut unique, cols, pk_indices)?;
    // A stream holds no rows to index. A UNIQUE the PK covers was dropped above,
    // so it is no more refused here than it is on a table.
    if props.stream {
        if let Some(u) = unique.first() {
            let names: Vec<&str> = u.cols.iter().map(|&c| cols[c as usize].name.as_str()).collect();
            return Err(GnitzSqlError::Rejected(format!(
                "a stream cannot carry a UNIQUE constraint (column(s) {}): it holds no rows to index",
                names.join(", ")
            )));
        }
    }

    let unique = name_unique_indexes(unique, cols, pk_indices.len(), &table_name)?;
    // Every FK column carries an index of its own.
    for &i in fks.keys() {
        index_key(cols, &[i as u32], pk_indices.len(), "FOREIGN KEY")?;
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
            let idx = require_column(cols, col_name).map_err(|e| e.in_clause("CLUSTER BY"))?;
            cluster_indices.push(idx as u32);
        }
        let prefix_len = dist_prefix_len(cols, pk_indices, &cluster_indices)?;
        props.distribution = props
            .distribution
            .cluster_by(prefix_len)
            .map_err(GnitzSqlError::Rejected)?;
    }
    props.validate(pk_indices.len()).map_err(GnitzSqlError::Rejected)?;

    Ok(Some(PlannedTable {
        def: TableDef {
            name: table_name,
            schema,
            fks,
            props,
            unique_indexes: Vec::new(),
        },
        unique,
    }))
}

/// `DROP TABLE | VIEW | INDEX`; returns the statement's command tag.
pub(crate) async fn execute_drop(
    client: &mut GnitzClient,
    schema_name: &str,
    object_type: &ObjectType,
    names: &[sqlparser::ast::ObjectName],
    if_exists: bool,
) -> Result<&'static str, GnitzSqlError> {
    // The kind is the statement's, not each name's, so it is settled once.
    if !matches!(object_type, ObjectType::Table | ObjectType::View | ObjectType::Index) {
        return Err(unsupported_clause("DROP", &object_type.to_string()));
    }
    // `IF EXISTS` rides the verb, which resolves its own targets. It softens
    // nothing else: one refusal still fails the whole statement.
    if let ObjectType::Index = object_type {
        // An index name is global, so a qualifier on one means nothing.
        let targets = names
            .iter()
            .map(|n| extract_index_name(n, "DROP"))
            .collect::<Result<Vec<String>, _>>()?;
        let targets: Vec<&str> = targets.iter().map(String::as_str).collect();
        client.drop_indexes_by_name(&targets, if_exists).await?;
        return Ok("DROP INDEX");
    }
    let targets = names
        .iter()
        .map(|n| extract_object_name(n, schema_name, "DROP"))
        .collect::<Result<Vec<RelName>, GnitzSqlError>>()?;
    match object_type {
        ObjectType::View => {
            client.drop_view(&targets, if_exists).await?;
            Ok("DROP VIEW")
        }
        _ => {
            client.drop_table(&targets, if_exists).await?;
            Ok("DROP TABLE")
        }
    }
}

/// Which surface asked for the index. `ADD CONSTRAINT` is always unique, so
/// uniqueness is representable only on `CreateIndex`.
#[derive(Clone, Copy)]
pub(super) enum IndexSite {
    CreateIndex { unique: bool },
    AddConstraint,
}

impl IndexSite {
    /// How the site names itself in a rejection.
    fn context(self) -> &'static str {
        match self {
            IndexSite::CreateIndex { .. } => "CREATE INDEX",
            IndexSite::AddConstraint => "ADD CONSTRAINT",
        }
    }

    fn is_unique(self) -> bool {
        match self {
            IndexSite::CreateIndex { unique } => unique,
            IndexSite::AddConstraint => true,
        }
    }
}

/// Reject every `CreateIndex` field `execute_create_index` does not consume (`name`, `table_name`,
/// `columns`, `unique`). `using` is accepted only for the BTree default (gnitz's index is ordered /
/// range-scannable); any other type, plus `predicate` (partial index → full index), `concurrently`
/// (no non-blocking-build guarantee), `include`/`nulls_distinct`/`with` (silent default semantics)
/// are rejected. `if_not_exists` is consumed by `execute_create_index`.
fn reject_unhonored_create_index_clauses(ci: &sqlparser::ast::CreateIndex) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE INDEX";
    let sqlparser::ast::CreateIndex {
        name: _,
        table_name: _,
        columns: _,
        unique: _,
        if_not_exists: _, // consumed by `execute_create_index`
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

pub(crate) async fn execute_create_index(
    client: &mut GnitzClient,
    schema_name: &str,
    ci: &sqlparser::ast::CreateIndex,
) -> Result<(), GnitzSqlError> {
    reject_unhonored_create_index_clauses(ci)?;
    let owner_name = extract_object_name(&ci.table_name, schema_name, "CREATE INDEX")?;
    let explicit_name = ci
        .name
        .as_ref()
        .map(|n| extract_index_name(n, "CREATE INDEX"))
        .transpose()?;
    // `IF NOT EXISTS` tests the name alone: a standing index under it ends the
    // statement before the owner or the column list is resolved. Index
    // names are global, so the index may stand on any relation. sqlparser
    // requires a name under the clause.
    if ci.if_not_exists {
        if let Some(name) = &explicit_name {
            if client.index_rows().await?.iter().any(|r| &r.name == name) {
                return Ok(());
            }
        }
    }
    // The one resolve on this path: `create_index_core` takes the descriptor.
    let target = client.resolve_relation(&owner_name).await?;
    require_class(&target, &owner_name, ClassWant::Indexable, "CREATE INDEX")?;
    create_index_core(
        client,
        &target,
        &owner_name,
        &ci.columns,
        explicit_name,
        IndexSite::CreateIndex { unique: ci.unique },
    )
    .await
}

/// The shared CREATE INDEX / `ALTER TABLE … ADD CONSTRAINT UNIQUE` core over the
/// index `columns` of `owner_name` asks for. `target` is the caller's
/// already-resolved owner, so a statement resolves its name and asserts the owner
/// class exactly once. `explicit_name` is the written index or constraint name,
/// canonical.
pub(super) async fn create_index_core(
    client: &mut GnitzClient,
    target: &gnitz_core::RelDescriptor,
    owner_name: &RelName,
    columns: &[sqlparser::ast::IndexColumn],
    explicit_name: Option<String>,
    site: IndexSite,
) -> Result<(), GnitzSqlError> {
    let ctx = site.context();
    if columns.is_empty() {
        return Err(GnitzSqlError::Rejected(format!("{ctx}: at least one column required")));
    }

    let (owner_id, schema) = (target.tid, &target.schema);
    let (col_names, col_indices) = resolve_index_columns(columns, schema.columns(), ctx)?;

    let cols = index_key(schema.columns(), &col_indices, schema.pk_cols().len(), ctx)?;

    let index_name = match explicit_name {
        // A collision with a standing index errors in `create_index`.
        Some(name) => name,
        None => {
            let base = default_index_name(owner_name, &col_names);
            let existing = client.index_rows().await?;
            let serves_request =
                |r: &gnitz_core::IndexRow| r.owner == owner_id && r.cols == cols && (r.is_unique || !site.is_unique());
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

    client
        .create_index(owner_id, cols, &index_name, site.is_unique())
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
