//! DDL: CREATE TABLE (with FK resolution, UNIQUE, CLUSTER BY, REPLICATED),
//! DROP, and CREATE INDEX.

use super::guard::{
    ColumnDecl, KeyConstraint, index_column_names, read_column, read_table_constraint, reject_index_type_and_options,
    simple_ident_expr,
};
use super::options::parse_table_options;
use crate::catalog::Catalog;
use crate::error::{GnitzSqlError, reject_if, unsupported_clause};
use crate::names::{extract_global_name, extract_object_name};
use crate::rules::{ClassWant, column_names, reject_duplicate_names, require_class, require_column};
use gnitz_core::{ClientError, GnitzClient, InlineUniqueIndex, RelName, Schema, TableDef};
use gnitz_wire::{ColType, ColumnDef, FkAction, FkRef, PkColList, PkListRole};
use sqlparser::ast::{ObjectType, WrappedCollection};
use std::collections::BTreeMap;

/// The column list of an index on `key` of `schema`, or why no index can be
/// built on it.
fn index_key(schema: &Schema, key: &[u32], role: &str) -> Result<PkColList, GnitzSqlError> {
    let cols = schema.columns();
    gnitz_wire::validate_index_key(key, cols.len(), schema.pk_cols().len(), |c| cols[c as usize].ty.tc).map_err(
        |rule| {
            let named = rule.named(PkListRole::ColumnList, |c| cols.get(c as usize).map(|d| &*d.name));
            GnitzSqlError::rejected(format!("{role}: {named}"))
        },
    )
}

/// The FK child-type rule: a child adopts the referenced type, so its type must
/// equal it or be an integer whose domain fits inside it.
fn check_fk_type_compat(fk_col_type: ColType, parent_col_type: ColType) -> Result<(), GnitzSqlError> {
    if fk_col_type != parent_col_type && !fk_col_type.tc.int_domain_fits(parent_col_type.tc) {
        return Err(GnitzSqlError::rejected(format!(
            "FK type mismatch: column type {fk_col_type} cannot reference column type \
             {parent_col_type} — the child column adopts the referenced type, which would \
             narrow, re-sign or restate {fk_col_type}; declare the child with a type whose range \
             fits within {parent_col_type}",
        )));
    }
    Ok(())
}

/// Resolve a PRIMARY KEY / UNIQUE / CREATE INDEX / CLUSTER BY column list against `cols`, in
/// declared order — order drives the composite index's leading-key span and its
/// prefix seeks. `ctx` names the surface in both messages.
fn resolve_index_columns(names: &[&str], cols: &[ColumnDef], ctx: &str) -> Result<Vec<u32>, GnitzSqlError> {
    let mut indices: Vec<u32> = Vec::with_capacity(names.len());
    for &name in names {
        let idx = require_column(cols, name).map_err(|e| e.in_clause(ctx))? as u32;
        if indices.contains(&idx) {
            return Err(GnitzSqlError::rejected(format!("{ctx}: duplicate column '{name}'")));
        }
        indices.push(idx);
    }
    Ok(indices)
}

/// One `REFERENCES` site, qualified but not yet resolved.
struct FkSite {
    /// The child column.
    child: usize,
    parent: RelName,
    /// The parent column the clause names; `None` defaults to the parent's lone
    /// PK column.
    referred: Option<String>,
    on_delete: FkAction,
}

/// One UNIQUE constraint as written: its ordered column list and the
/// `CONSTRAINT <name>` it carried, canonical.
struct UniqueDecl {
    cols: Vec<u32>,
    name: Option<String>,
}

/// Everything `CREATE TABLE`'s own text declares. Built from the statement and
/// the session's schema alone, so it is complete before any catalog read: a
/// column name is a column of the in-flight list or it is rejected, and a
/// `REFERENCES` target is only qualified.
#[derive(Default)]
struct Declared {
    cols: Vec<ColumnDef>,
    /// Empty where no PRIMARY KEY is written: a written one names a column.
    pk: Vec<u32>,
    /// The columns declared SERIAL.
    serial: Vec<u32>,
    fks: Vec<FkSite>,
    unique: Vec<UniqueDecl>,
}

impl Declared {
    /// Declare `c` over the in-flight columns. Each kind holds one rule whichever
    /// spelling it is written in.
    fn constrain(&mut self, c: KeyConstraint<'_>, schema_name: &str) -> Result<(), GnitzSqlError> {
        match c {
            KeyConstraint::PrimaryKey(names) => {
                // Resolved before the conflict is reported, so an unknown column
                // name is named rather than eclipsed by it.
                let key = resolve_index_columns(&names, &self.cols, "PRIMARY KEY")?;
                if !self.pk.is_empty() {
                    return Err(GnitzSqlError::rejected("Multiple PRIMARY KEYs defined"));
                }
                // The null bitmap excludes the PK region, so a nullable PK has no
                // place to carry the null: PRIMARY KEY implies NOT NULL, coerced
                // rather than rejected.
                for &c in &key {
                    self.cols[c as usize].is_nullable = false;
                }
                self.pk = key;
            }
            // The rule CREATE INDEX holds an unnamed index to: an unnamed UNIQUE
            // may not repeat a column set another declares. `a INT UNIQUE UNIQUE`
            // is one constraint written twice; two written names on one column
            // list are two indexes, as two CREATE INDEX statements make. No two
            // may share a written name.
            KeyConstraint::Unique(names, name) => {
                let cols = resolve_index_columns(&names, &self.cols, "UNIQUE")?;
                if self
                    .unique
                    .iter()
                    .any(|d| d.cols == cols && (d.name.is_none() || name.is_none()))
                {
                    return Err(GnitzSqlError::rejected(format!(
                        "duplicate UNIQUE constraint on column(s) ({})",
                        names.join(", ")
                    )));
                }
                if let Some(name) = &name
                    && self.unique.iter().any(|d| d.name.as_ref() == Some(name))
                {
                    return Err(GnitzSqlError::rejected(format!("duplicate constraint name '{name}'")));
                }
                self.unique.push(UniqueDecl { cols, name });
            }
            KeyConstraint::ForeignKey { child, parent, referred, on_delete } => {
                let child = require_column(&self.cols, child).map_err(|e| e.in_clause("FOREIGN KEY"))?;
                if self.fks.iter().any(|s| s.child == child) {
                    return Err(GnitzSqlError::rejected(format!(
                        "column '{}' carries more than one FOREIGN KEY",
                        self.cols[child].name
                    )));
                }
                self.fks.push(FkSite {
                    child,
                    parent: extract_object_name(parent, schema_name, "REFERENCES")?,
                    referred: referred.map(str::to_owned),
                    on_delete,
                });
            }
        }
        Ok(())
    }
}

/// Read `create`'s columns and constraints into a [`Declared`]: every column
/// option, then every table constraint, each in declaration order.
fn collect_declarations(create: &sqlparser::ast::CreateTable, schema_name: &str) -> Result<Declared, GnitzSqlError> {
    let mut d = Declared::default();
    for (i, col) in create.columns.iter().enumerate() {
        let ColumnDecl { def, serial, constraints } = read_column(col, "column definition")?;
        d.cols.push(def);
        if serial {
            d.serial.push(i as u32);
        }
        for c in constraints {
            d.constrain(c, schema_name)?;
        }
    }
    for constraint in &create.constraints {
        d.constrain(read_table_constraint(constraint, "table constraint")?, schema_name)?;
    }
    Ok(d)
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
        multiset,
        // No gnitz-honorable semantics — storage/engine/vendor metadata accepted as
        // no-ops. `with_data` is read only by CTAS, rejected below.
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
        sqlite_table_options: _,
        unlogged: _,
        with_connection: _,
        fallback: _,
        with_data: _,
        comment: _,
        on_cluster: _,
        order_by: _,
        partition_by: _,
        clustered_by: _,
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
    reject_if(multiset.is_some(), CTX, "MULTISET/SET table kind")?;
    Ok(())
}

/// The distribution prefix length `cluster` declares, or `Err` unless it is a
/// non-empty leading prefix of `pk`, in PK order: the distribution key is a
/// leading PK prefix so write-side routing is a byte-slice of the OPK region.
/// `cols` names a column the PK does not hold.
fn dist_prefix_len(cols: &[ColumnDef], pk: &[u32], cluster: &[u32]) -> Result<u8, GnitzSqlError> {
    if let Some(&c) = cluster.iter().find(|c| !pk.contains(c)) {
        return Err(GnitzSqlError::rejected(format!(
            "CLUSTER BY column '{}' is not a PRIMARY KEY column",
            cols[c as usize].name
        )));
    }
    if cluster.is_empty() || !pk.starts_with(cluster) {
        return Err(GnitzSqlError::rejected(
            "CLUSTER BY columns must be a leading prefix of the PRIMARY KEY, \
             in PK order; reorder the PK so the distribution column(s) lead",
        ));
    }
    Ok(u8::try_from(cluster.len()).expect("a PRIMARY KEY prefix fits a u8"))
}

/// Plan a `CREATE TABLE` into the bundle its commit writes, an unnamed UNIQUE
/// left for the writer to name; `None` when `IF NOT EXISTS` finds the name taken.
/// [`collect_declarations`] and the checks after it read the statement alone;
/// everything past the `IF NOT EXISTS` probe resolves against the catalog.
pub(crate) fn plan_create_table(
    create: &sqlparser::ast::CreateTable,
    cat: &dyn Catalog,
) -> Result<Option<TableDef>, GnitzSqlError> {
    reject_unhonored_create_table_clauses(create)?;
    let schema_name = cat.schema_name();
    let table_name = extract_object_name(&create.name, schema_name, "CREATE TABLE")?;

    // The `WITH (…)` keys and values are decidable from the statement's own text.
    // A `Keyed` distribution's prefix is the other half of `props`, and needs
    // CLUSTER BY.
    let mut props = parse_table_options(&create.table_options)?;

    // Before any column list is resolved: a repeat would otherwise surface as an
    // ambiguous *reference*, when it is the definition that is wrong.
    reject_duplicate_names(create.columns.iter().map(|c| c.name.value.as_str()), "table definition")?;

    let Declared { mut cols, pk, serial, mut fks, unique } = collect_declarations(create, schema_name)?;

    // The generated id has no compound form: a column spelled SERIAL is the table's
    // whole primary key.
    if let Some(&c) = serial.iter().find(|&&c| pk != [c]) {
        return Err(GnitzSqlError::rejected(format!(
            "SERIAL column '{}' must be the table's only SERIAL column and its single-column PRIMARY KEY",
            cols[c as usize].name
        )));
    }
    props.serial = !serial.is_empty();

    // A stream holds no rows to index.
    if props.stream
        && let Some(u) = unique.first()
    {
        return Err(GnitzSqlError::rejected(format!(
            "a stream cannot carry a UNIQUE constraint (column(s) {}): it holds no rows to index",
            column_names(&cols, &u.cols)
        )));
    }

    // CLUSTER BY (hash distribution key). The named columns must be the PK's
    // leading prefix in PK order; the prefix length `k` is persisted in
    // `TABLE_TAB.dist_prefix` and drives write-side routing and co-partition detection.
    // No clause ⇒ `k = 0` ⇒ default full-PK distribution.
    if let Some(cluster) = &create.cluster_by {
        let (WrappedCollection::NoWrapping(exprs) | WrappedCollection::Parentheses(exprs)) = cluster;
        let names = exprs
            .iter()
            .map(|e| simple_ident_expr(e, "CLUSTER BY"))
            .collect::<Result<Vec<_>, _>>()?;
        let cluster_indices = resolve_index_columns(&names, &cols, "CLUSTER BY")?;
        let prefix_len = dist_prefix_len(&cols, &pk, &cluster_indices)?;
        props.distribution = props
            .distribution
            .cluster_by(prefix_len)
            .map_err(GnitzSqlError::rejected)?;
    }
    props.validate(pk.len()).map_err(GnitzSqlError::rejected)?;

    // `IF NOT EXISTS` tests the NAME, not the definition — no dialect compares the
    // two — so any relation standing under it ends the statement, whatever its
    // kind. Probed only under the clause, so a plain CREATE TABLE resolves nothing;
    // probed after every rejection the statement's text earns, which outrank it.
    if create.if_not_exists && cat.probe(&table_name).is_some() {
        return Ok(None);
    }

    // Each FK REWRITES its child column's type to the parent's, so the
    // self-references go last: one adopts the PK column's type, which a
    // cross-table FK on that column rewrites. A self-reference resolves against
    // the in-flight columns and the whole PK, the table not being in the catalog
    // yet; any other against the registered relation. The engine holds the target
    // to a base table's lone PK or a column with a single-column UNIQUE index.
    fks.sort_by_key(|site| site.parent == table_name);
    let fks = fks
        .into_iter()
        .map(|site| {
            let probed;
            let (parent_cols, parent_pk, table_id) = match site.parent == table_name {
                true => (
                    &cols[..],
                    match pk[..] {
                        [c] => Some(c as usize),
                        _ => None,
                    },
                    gnitz_core::pending_id(0),
                ),
                false => {
                    probed = cat.probe_relation(&site.parent)?;
                    (
                        probed.schema.columns(),
                        probed.schema.layout().lone_pk_col(),
                        probed.tid,
                    )
                }
            };
            let col = match &site.referred {
                Some(name) => require_column(parent_cols, name)
                    .map_err(|e| e.in_clause(&format!("FOREIGN KEY REFERENCES {}", site.parent)))?,
                None => parent_pk.ok_or_else(|| {
                    GnitzSqlError::rejected(format!(
                        "FK against '{}' must name the referenced column (its primary key is not a single column)",
                        site.parent
                    ))
                })?,
            };
            let parent_ty = parent_cols[col].ty;
            check_fk_type_compat(cols[site.child].ty, parent_ty)?;
            cols[site.child].ty = parent_ty;
            let fk = FkRef {
                table_id,
                col: col as u32,
                on_delete: site.on_delete,
            };
            Ok((site.child, fk))
        })
        .collect::<Result<BTreeMap<_, _>, GnitzSqlError>>()?;

    let schema = Schema::from_parts(cols, &pk).map_err(GnitzSqlError::rejected)?;
    // Keyed after FK resolution: an index key's admissibility reads the adopted
    // column types.
    let unique_indexes = unique
        .into_iter()
        .map(|u| {
            let cols = index_key(&schema, &u.cols, "UNIQUE")?;
            Ok(InlineUniqueIndex { cols, name: u.name })
        })
        .collect::<Result<_, GnitzSqlError>>()?;

    Ok(Some(TableDef {
        name: table_name,
        schema,
        fks,
        props,
        unique_indexes,
    }))
}

/// `DROP SCHEMA | TABLE | VIEW | INDEX`; returns the statement's command tag.
pub(crate) async fn execute_drop(
    client: &mut GnitzClient,
    schema_name: &str,
    object_type: &ObjectType,
    names: &[sqlparser::ast::ObjectName],
    if_exists: bool,
    cascade: bool,
) -> Result<&'static str, GnitzSqlError> {
    const CTX: &str = "DROP";
    // CASCADE widens a schema's drop to the relations in it; no other kind
    // has members to take along.
    reject_if(cascade && !matches!(object_type, ObjectType::Schema), CTX, "CASCADE")?;
    // A schema or index name is global, so a qualifier on one means nothing.
    let global = |noun: &str| {
        names
            .iter()
            .map(|n| extract_global_name(n, noun, CTX))
            .collect::<Result<Vec<String>, _>>()
    };
    let relations = || {
        names
            .iter()
            .map(|n| extract_object_name(n, schema_name, CTX))
            .collect::<Result<Vec<RelName>, _>>()
    };
    // `IF EXISTS` rides the verb, which resolves its own targets. It softens
    // nothing else: one refusal still fails the whole statement.
    match object_type {
        ObjectType::Schema => {
            let targets = global("schema")?;
            let targets: Vec<&str> = targets.iter().map(String::as_str).collect();
            client.drop_schema(&targets, cascade, if_exists).await?;
            Ok("DROP SCHEMA")
        }
        ObjectType::Index => {
            let targets = global("index")?;
            let targets: Vec<&str> = targets.iter().map(String::as_str).collect();
            client.drop_indexes_by_name(&targets, if_exists).await?;
            Ok("DROP INDEX")
        }
        ObjectType::View => {
            client.drop_view(&relations()?, if_exists).await?;
            Ok("DROP VIEW")
        }
        ObjectType::Table => {
            client.drop_table(&relations()?, if_exists).await?;
            Ok("DROP TABLE")
        }
        _ => Err(unsupported_clause(CTX, &object_type.to_string())),
    }
}

/// Reject every `CreateIndex` field `execute_create_index` does not consume (`name`, `table_name`,
/// `columns`, `unique`). `using` is accepted only for the BTree default (gnitz's index is ordered /
/// range-scannable); any other type, plus `predicate` (partial index → full index), `concurrently`/`async`
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
        r#async,
        include,
        nulls_distinct,
        with,
        predicate,
        index_options,
        alter_options,
    } = ci;
    reject_if(predicate.is_some(), CTX, "WHERE (partial index)")?;
    reject_if(!alter_options.is_empty(), CTX, "ALTER options (ALGORITHM / LOCK)")?;
    reject_index_type_and_options(using, index_options, include, CTX)?;
    reject_if(*concurrently, CTX, "CONCURRENTLY")?;
    reject_if(*r#async, CTX, "ASYNC")?;
    reject_if(nulls_distinct.is_some(), CTX, "NULLS [NOT] DISTINCT")?;
    reject_if(!with.is_empty(), CTX, "WITH (storage parameters)")?;
    Ok(())
}

pub(crate) async fn execute_create_index(
    client: &mut GnitzClient,
    schema_name: &str,
    ci: &sqlparser::ast::CreateIndex,
) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE INDEX";
    reject_unhonored_create_index_clauses(ci)?;
    let columns = index_column_names(&ci.columns, CTX)?;
    let owner_name = extract_object_name(&ci.table_name, schema_name, CTX)?;
    let explicit_name = ci
        .name
        .as_ref()
        .map(|n| extract_global_name(n, "index", CTX))
        .transpose()?;
    let created = async {
        // The one resolve on this path: `create_index` takes the descriptor.
        let target = client.resolve_relation(&owner_name).await?;
        require_class(&target, &owner_name, ClassWant::Indexable, CTX)?;
        let key = index_cols(&target.schema, &columns, CTX)?;
        client
            .create_index(&owner_name, &target, key, explicit_name.as_deref(), ci.unique)
            .await?;
        Ok(())
    }
    .await;
    // `IF NOT EXISTS` tests the name alone: a standing index under it ends the
    // statement whatever its owner or column list would have refused, and
    // whoever created it first — the server refuses a taken name, so the test
    // runs on a refusal. Index names are global, so the index may stand on any
    // relation. sqlparser requires a name under the clause.
    if ci.if_not_exists
        && let Err(GnitzSqlError::Client(ClientError::Refused(_))) = created
        && let Some(name) = &explicit_name
        && client
            .catalog()
            .await?
            .relations
            .iter()
            .flat_map(|r| &r.indexes)
            .any(|i| &i.name == name)
    {
        return Ok(());
    }
    created
}

/// The key of an index on `columns` of `schema`, in that order, as CREATE INDEX
/// and `ALTER TABLE … ADD CONSTRAINT UNIQUE` (which `ctx` names) ask for it.
pub(super) fn index_cols(schema: &Schema, columns: &[&str], ctx: &str) -> Result<PkColList, GnitzSqlError> {
    let col_indices = resolve_index_columns(columns, schema.columns(), ctx)?;
    index_key(schema, &col_indices, ctx)
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
