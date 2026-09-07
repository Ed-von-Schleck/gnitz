//! DDL: CREATE TABLE (with FK resolution, UNIQUE, CLUSTER BY, REPLICATED), DROP,
//! and CREATE INDEX. The compile side's only non-view surface.

use crate::ast_util::{extract_index_name, extract_object_name, index_column_ident, simple_ident_expr};
use crate::bind::{find_unique_column, probe, Binder};
use crate::error::GnitzSqlError;
use crate::types::{serial_underlying, sql_type_to_typecode};
use crate::validate::{
    canonical_user_name, kv_options, non_key_eligible_error, reject_column_overflow, reject_duplicate_names,
    reject_repeated_object, reject_unbuildable_index_key, reject_unhonored_column_options,
    reject_unhonored_create_index_clauses, reject_unhonored_create_table_clauses, reject_unhonored_table_constraints,
    require_class, validate_user_name, ClassWant, ColumnOptionSite,
};
use crate::SqlResult;
use gnitz_core::{CatalogSnapshot, ColumnDef, FkTarget, GnitzClient, InlineUniqueIndex, TableProps, TypeCode};
use sqlparser::ast::{
    ColumnOption, CreateTableOptions, Expr, ForeignKeyConstraint, ObjectType, PrimaryKeyConstraint, TableConstraint,
    UniqueConstraint, Value, ValueWithSpan, WrappedCollection,
};
use std::collections::HashSet;

/// Catalog name for an auto-generated (unnamed) secondary index:
/// `{schema}__{table}__idx_{col1}_{col2}…`. `DROP INDEX <name>` resolves this
/// exact string, so the format is a stable contract. Lowercased, so the base is
/// canonical and [`disambiguate_index_name`] compares like with like.
fn default_index_name(schema_name: &str, table_name: &str, col_names: &[&str]) -> String {
    format!("{schema_name}__{table_name}__idx_{}", col_names.join("_")).to_ascii_lowercase()
}

/// Return `base` if free, else the first `{base}_{n}` (n ≥ 2) not in `taken` —
/// PostgreSQL's scheme, keeping the readable base for the common non-colliding
/// case. Both are canonical: `base` comes from [`default_index_name`], `taken`
/// from the catalog.
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

/// [`gnitz_wire::fk_child_fits`]'s rejection message. The engine's own pre-create
/// gate reads the same predicate, so the two cannot disagree on what is legal.
fn check_fk_type_compat(fk_col_type: TypeCode, parent_col_type: TypeCode) -> Result<(), GnitzSqlError> {
    if !gnitz_wire::fk_child_fits(fk_col_type as u8, parent_col_type as u8) {
        return Err(GnitzSqlError::Bind(format!(
            "FK type mismatch: column type {fk_col_type:?} cannot reference column type \
             {parent_col_type:?} — the child column adopts the referenced type, which would \
             narrow or re-sign {fk_col_type:?}; declare the child with a type whose range \
             fits within {parent_col_type:?}",
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
    match referred_columns.first() {
        Some(ident) => find_unique_column(cols, &ident.value)?.ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "FK references column '{}' not found in table '{}'",
                ident.value, ref_table,
            ))
        }),
        None => pk_single.ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "FK against '{ref_table}' must name the referenced column (its primary key is not a single column)",
            ))
        }),
    }
}

/// Resolve a self-referencing FK against the in-flight column list (the table
/// is not yet registered in the catalog). The referenced column must be the
/// table's lone PK column, and may not be the child column itself.
fn resolve_fk_target_inline(
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
    ref_table: &str,
    site: &FkSite<'_>,
) -> Result<(FkTarget, TypeCode), GnitzSqlError> {
    let pk_single = (current_pk_cols.len() == 1).then(|| current_pk_cols[0] as usize);
    let ref_col_idx = resolve_referred_column(site.referred_columns, ref_table, current_cols, pk_single)?;

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
    if ref_col_idx == site.col_idx {
        return Err(GnitzSqlError::Bind(
            "a self-referencing FK column must not be the referenced column itself".into(),
        ));
    }

    let parent_col_type = current_cols[ref_col_idx].type_code;
    check_fk_type_compat(current_cols[site.col_idx].type_code, parent_col_type)?;
    Ok((FkTarget::SelfTable { col: ref_col_idx as u32 }, parent_col_type))
}

/// Resolve a REFERENCES clause to its target and the parent column's type.
/// The referenced column is a legal target iff it is the parent's lone PK
/// column or it carries its own single-column UNIQUE index. Validates that the
/// child's type is compatible with the referenced column's and returns that
/// type so the caller can widen the child column.
fn resolve_fk_target(
    cat: &CatalogSnapshot,
    schema_name: &str,
    site: &FkSite<'_>,
    current_table_name: &str,
    current_cols: &[ColumnDef],
    current_pk_cols: &[u32],
) -> Result<(FkTarget, TypeCode), GnitzSqlError> {
    let ref_table = extract_object_name(site.foreign_table, schema_name, "REFERENCES")?;

    // Self-referencing FK: the table being created is not yet in the catalog,
    // so resolve the referenced column against the in-flight column list.
    if ref_table.eq_ignore_ascii_case(current_table_name) {
        return resolve_fk_target_inline(current_cols, current_pk_cols, &ref_table, site);
    }

    // The one relation-name catalog probe outside the Binder funnels: hold the
    // FK target to the same reserved-prefix rule they enforce.
    validate_user_name(&ref_table)?;
    let ref_rel = probe(cat, schema_name, &ref_table)?
        .ok_or_else(|| crate::error::missing_relation("FK target", schema_name, &ref_table))?;
    let ref_schema = &ref_rel.schema;
    // The PK/UNIQUE tests below read only the schema, and both a view's and a
    // stream's PK look exactly like a base table's without being the unique, stored
    // key the parent probe reads.
    require_class(&ref_rel, &ref_table, ClassWant::BaseTable, "a FOREIGN KEY target")?;
    let ref_tid = ref_rel.tid;

    let pk_single = ref_schema.pk_index_single().map(|c| c as usize);
    let ref_col_idx = resolve_referred_column(site.referred_columns, &ref_table, &ref_schema.columns, pk_single)?;

    // Legal target iff the referenced column is the parent's lone PK or carries a
    // UNIQUE index of its own. A composite unique `(a, b)` does not make `a`
    // unique, so the column list is matched exactly, as the engine's gate does.
    if pk_single != Some(ref_col_idx) {
        let unique = ref_rel
            .indexes
            .iter()
            .any(|m| m.cols.as_slice() == [ref_col_idx as u32] && m.is_unique);
        if !unique {
            return Err(GnitzSqlError::Unsupported(format!(
                "FK against table '{}' must reference the primary key or a column \
                 with a UNIQUE index; column '{}' has neither",
                ref_table, ref_schema.columns[ref_col_idx].name,
            )));
        }
    }

    // Child column widens to the referenced parent column's type.
    let parent_col_type = ref_schema.columns[ref_col_idx].type_code;
    check_fk_type_compat(current_cols[site.col_idx].type_code, parent_col_type)?;

    Ok((
        FkTarget::Table { id: ref_tid, col: ref_col_idx as u32 },
        parent_col_type,
    ))
}

/// The boolean properties of `CREATE TABLE … WITH (…)`. `dist_prefix_len` is left
/// at its default; the CLUSTER BY phase fills it. An unknown key is rejected so a
/// typo cannot be silently ignored.
fn parse_table_options(table_options: &CreateTableOptions) -> Result<TableProps, GnitzSqlError> {
    let mut props = TableProps::default();
    for (key, value) in kv_options(table_options, "CREATE TABLE")? {
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
        let Expr::Value(ValueWithSpan { value: Value::Boolean(b), .. }) = value else {
            return Err(GnitzSqlError::Plan(format!(
                "WITH ({} = …) expects a boolean (true/false)",
                key.value
            )));
        };
        *slot = *b;
    }
    Ok(props)
}

/// One UNIQUE constraint as written: the column list — one element for a
/// column-level `UNIQUE`, the whole ordered list for a table-level one — and the
/// `CONSTRAINT <name>` it carried, still in the user's own spelling.
struct UniqueDecl {
    cols: Vec<u32>,
    raw_name: Option<String>,
}

/// Record a UNIQUE constraint, refusing a column set already recorded:
/// `a INT UNIQUE UNIQUE` and `UNIQUE(a), UNIQUE(a)` are one constraint written
/// twice, and one index is all either could create.
fn push_unique(
    unique: &mut Vec<UniqueDecl>,
    cols: Vec<u32>,
    col_names: &[&str],
    raw_name: Option<String>,
) -> Result<(), GnitzSqlError> {
    if unique.iter().any(|u| u.cols == cols) {
        return Err(GnitzSqlError::Plan(format!(
            "duplicate UNIQUE constraint on column(s) ({})",
            col_names.join(", ")
        )));
    }
    unique.push(UniqueDecl { cols, raw_name });
    Ok(())
}

/// Everything `CREATE TABLE`'s own text declares.
struct Declared<'a> {
    cols: Vec<ColumnDef>,
    pk: Vec<u32>,
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

    // The column defs (name, type, nullability) and every site a column option
    // declares. A SERIAL column resolves to its underlying signed int, is always
    // NOT NULL, and carries the `is_serial` marker.
    for (i, col) in create.columns.iter().enumerate() {
        cols.push(if let Some(tc) = serial_underlying(&col.data_type) {
            ColumnDef::new(col.name.value.clone(), tc, false).serial() // NOT NULL
        } else {
            ColumnDef::new(
                col.name.value.clone(),
                sql_type_to_typecode(&col.data_type)?,
                !col.options.iter().any(|o| matches!(o.option, ColumnOption::NotNull)),
            )
        });
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
                    opt.name.as_ref().map(|n| n.value.clone()),
                )?,
                _ => {}
            }
        }
    }
    if inline_pk.len() > 1 {
        return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
    }

    // The same three kinds again, table-level and in declaration order, with the
    // columns named rather than positional.
    let mut table_pk: Option<Vec<u32>> = None;
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(PrimaryKeyConstraint { columns: pk_cols, .. }) => {
                if table_pk.is_some() {
                    return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
                }
                // Resolved before the inline conflict is reported, so an unknown
                // column name is named rather than eclipsed by it.
                let resolved = resolve_index_columns(pk_cols, &cols, "PRIMARY KEY")?.1;
                if !inline_pk.is_empty() {
                    return Err(GnitzSqlError::Plan("Multiple PRIMARY KEYs defined".into()));
                }
                table_pk = Some(resolved);
            }
            TableConstraint::ForeignKey(ForeignKeyConstraint {
                columns, foreign_table, referred_columns, ..
            }) => {
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
                fk_sites.push(FkSite { col_idx, foreign_table, referred_columns });
            }
            TableConstraint::Unique(UniqueConstraint { name: name_ident, columns, .. }) => {
                if columns.is_empty() {
                    return Err(GnitzSqlError::Plan("UNIQUE constraint cannot be empty".into()));
                }
                let (col_names, col_indices) = resolve_index_columns(columns, &cols, "UNIQUE")?;
                push_unique(
                    &mut unique,
                    col_indices,
                    &col_names,
                    name_ident.as_ref().map(|n| n.value.clone()),
                )?;
            }
            _ => {}
        }
    }

    Ok(Declared {
        cols,
        pk: table_pk.unwrap_or(inline_pk),
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

/// A stream holds no rows: nothing to index, nothing to seed a SERIAL generator
/// from, and nothing for a referential action to check against. The engine
/// refuses all three naming the relation by id; this names the column.
fn reject_stream_constraints(d: &Declared<'_>) -> Result<(), GnitzSqlError> {
    if let Some(c) = d.cols.iter().find(|c| c.is_serial) {
        return Err(GnitzSqlError::Unsupported(format!(
            "a stream cannot carry a SERIAL column ('{}'): it holds no rows to seed the generator from",
            c.name
        )));
    }
    if let Some(site) = d.fk_sites.first() {
        return Err(GnitzSqlError::Unsupported(format!(
            "a stream cannot carry a FOREIGN KEY (column '{}'): it holds no rows to check against",
            d.cols[site.col_idx].name
        )));
    }
    // A UNIQUE the PK already covers builds no index, so it is no more refused
    // here than it is on a table.
    if let Some(u) = d.unique.iter().find(|u| u.cols != pk_covered_unique(&d.pk)) {
        let names: Vec<&str> = u.cols.iter().map(|&c| d.cols[c as usize].name.as_str()).collect();
        return Err(GnitzSqlError::Unsupported(format!(
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
    if let Some(u) = unique.iter().find(|u| u.cols == covered && u.raw_name.is_some()) {
        return Err(GnitzSqlError::Plan(format!(
            "UNIQUE constraint '{}' on the primary-key column '{}' names no index: the primary \
             key already provides the constraint",
            u.raw_name.as_deref().unwrap_or_default(),
            cols[covered[0] as usize].name
        )));
    }
    unique.retain(|u| u.cols != covered);
    Ok(())
}

/// Give each UNIQUE constraint the catalog name its index will carry: the written
/// `CONSTRAINT <name>`, or [`default_index_name`] disambiguated against the rest
/// of this bundle. Two written names folding to one canonical name are rejected —
/// only one of the two IDX_TAB rows would be reachable by name.
fn name_unique_indexes(
    unique: Vec<UniqueDecl>,
    cols: &[ColumnDef],
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<(Vec<u32>, String)>, GnitzSqlError> {
    // Written names first: they are the fixed points auto-names route around.
    let mut taken: HashSet<String> = HashSet::new();
    for u in &unique {
        if let Some(raw) = &u.raw_name {
            if !taken.insert(canonical_user_name(raw)?) {
                return Err(GnitzSqlError::Plan(format!("duplicate constraint name '{raw}'")));
            }
        }
    }
    unique
        .into_iter()
        .map(|u| {
            let name = match u.raw_name {
                Some(raw) => canonical_user_name(&raw)?,
                None => {
                    let col_names: Vec<&str> = u.cols.iter().map(|&c| cols[c as usize].name.as_str()).collect();
                    let base = default_index_name(schema_name, table_name, &col_names);
                    let name = disambiguate_index_name(base, &taken);
                    taken.insert(name.clone());
                    name
                }
            };
            Ok((u.cols, name))
        })
        .collect()
}

/// What a `CREATE TABLE` statement asks the connection to do — the two shapes
/// [`crate::ViewPlan`] carries, and for the same reason: `IF NOT EXISTS` tests
/// the name, and a name already standing ends the statement.
pub enum TablePlan {
    /// Register the table and its inline unique indexes as one bundle.
    Create {
        name: String,
        cols: Vec<ColumnDef>,
        pk_indices: Vec<u32>,
        props: TableProps,
        /// Each inline UNIQUE constraint's columns and the catalog name its index
        /// takes — auto-names already disambiguated within the bundle.
        unique_indexes: Vec<(Vec<u32>, String)>,
    },
    /// `IF NOT EXISTS`, and the name is taken: nothing to register. Carries the
    /// id of the relation already standing there.
    Skip { existing_id: u64 },
}

/// Plan a `CREATE TABLE` into the bundle its commit writes: a pure function of
/// `(create, cat)`, reaching no server. [`collect_declarations`] reads the
/// statement; everything below it resolves, admits and names.
pub fn plan_create_table(
    create: &sqlparser::ast::CreateTable,
    cat: &CatalogSnapshot,
    schema_name: &str,
) -> Result<TablePlan, GnitzSqlError> {
    reject_unhonored_create_table_clauses(create)?;
    let table_name = extract_object_name(&create.name, schema_name, "CREATE TABLE")?;
    validate_user_name(&table_name)?;

    // The `WITH (…)` keys and values are decidable from the statement's own text.
    // `dist_prefix_len` is the other half of `props`, and needs CLUSTER BY.
    let mut props = parse_table_options(&create.table_options)?;

    // `IF NOT EXISTS` tests the NAME, not the definition — no dialect compares the
    // two — so any relation standing under it ends the statement, whatever its
    // kind. Probed only under the clause, so a plain CREATE TABLE resolves nothing.
    if create.if_not_exists {
        if let Some(rel) = probe(cat, schema_name, &table_name)? {
            return Ok(TablePlan::Skip { existing_id: rel.tid });
        }
    }

    // The column cap is O(1) over the AST; the fold below is per column.
    reject_column_overflow("table definition", create.columns.len())?;
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
        fk_sites,
        mut unique,
    } = declared;

    // A self-FK resolves against the whole PK, and each resolve REWRITES its child
    // column's type to the parent's — so this runs after collection, not inside it.
    for site in &fk_sites {
        if cols[site.col_idx].fk.is_some() {
            return Err(GnitzSqlError::Plan(format!(
                "column '{}' carries more than one FOREIGN KEY",
                cols[site.col_idx].name
            )));
        }
        let (fk, parent_pk_type) = resolve_fk_target(cat, schema_name, site, &table_name, &cols, &pk_indices)?;
        cols[site.col_idx].fk = Some(fk);
        cols[site.col_idx].type_code = parent_pk_type;
    }

    // The null bitmap excludes the PK region, so a nullable PK has no place to
    // carry the null: PRIMARY KEY implies NOT NULL, coerced rather than rejected.
    for &i in &pk_indices {
        cols[i as usize].is_nullable = false;
    }

    // The admission rule is `gnitz-wire`'s, shared with the client gateway and the
    // engine catalog. Only the wording is the planner's: it names the offending
    // column, which the engine cannot.
    let pk_stride = gnitz_wire::validate_pk_tuple(&pk_indices, cols.len(), |c| {
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

    // A SERIAL column must be the table's sole, single-column PRIMARY KEY: the
    // INSERT path spells the generated id as `PkBuf::from_u128(stride, id)`, which
    // has no compound form.
    let mut serials = cols.iter().enumerate().filter(|(_, c)| c.is_serial);
    if let Some((sci, _)) = serials.next() {
        if serials.next().is_some() {
            return Err(GnitzSqlError::Unsupported("at most one SERIAL column per table".into()));
        }
        if pk_indices.as_slice() != [sci as u32] {
            return Err(GnitzSqlError::Unsupported(
                "a SERIAL column must be the table's single-column PRIMARY KEY".into(),
            ));
        }
    }

    drop_unique_covered_by_pk(&mut unique, &cols, &pk_indices)?;

    // The same `gnitz-wire` rule the engine applies, run here to name the
    // offending column — which the engine's id-only message cannot.
    for u in &unique {
        let names: Vec<&str> = u.cols.iter().map(|&c| cols[c as usize].name.as_str()).collect();
        let types: Vec<TypeCode> = u.cols.iter().map(|&c| cols[c as usize].type_code).collect();
        reject_unbuildable_index_key(&names, &types, pk_indices.len(), pk_stride, "UNIQUE")?;
    }

    // CLUSTER BY (hash distribution key). The named columns must be the PK's
    // leading prefix in PK order; the prefix length `k` is persisted in
    // `TABLE_TAB.flags` and drives write-side routing and co-partition detection.
    // No clause ⇒ `k = 0` ⇒ default full-PK distribution.
    props.dist_prefix_len = if let Some(cluster) = &create.cluster_by {
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
    // The one rule the flags packing cannot represent rides on `TableProps`.
    props.validate().map_err(GnitzSqlError::Plan)?;

    let unique_indexes = name_unique_indexes(unique, &cols, schema_name, &table_name)?;
    Ok(TablePlan::Create {
        name: table_name,
        cols,
        pk_indices,
        props,
        unique_indexes,
    })
}
/// Commit a planned `CREATE TABLE`: one bundle carrying the table, its columns
/// and its inline unique indexes. A `Skip` answers with the id already standing
/// under the name, since that is the name the caller asked to be taken.
pub(crate) fn execute_create_table(
    client: &mut GnitzClient,
    schema_name: &str,
    plan: TablePlan,
) -> Result<SqlResult, GnitzSqlError> {
    let (name, cols, pk_indices, props, unique_indexes) = match plan {
        TablePlan::Skip { existing_id } => return Ok(SqlResult::TableCreated { table_id: existing_id }),
        TablePlan::Create {
            name,
            cols,
            pk_indices,
            props,
            unique_indexes,
        } => (name, cols, pk_indices, props, unique_indexes),
    };
    let unique_indexes: Vec<InlineUniqueIndex> = unique_indexes
        .iter()
        .map(|(col_indices, name)| InlineUniqueIndex {
            col_indices: col_indices.as_slice(),
            name: name.as_str(),
        })
        .collect();
    let tid = client.create_table(schema_name, &name, &cols, &pk_indices, props, &unique_indexes)?;
    Ok(SqlResult::TableCreated { table_id: tid })
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
        return Err(GnitzSqlError::Unsupported(format!(
            "DROP {object_type:?} not supported"
        )));
    }
    let mut targets: Vec<String> = Vec::with_capacity(names.len());
    for obj_name in names {
        // An index name is global, so a qualifier on one means nothing; a table
        // or view name is schema-scoped, and the active schema is the session's.
        let name = match object_type {
            ObjectType::Index => extract_index_name(obj_name, "DROP")?,
            _ => extract_object_name(obj_name, schema_name, "DROP")?,
        };
        // No user object can carry a leading `_`, so this keeps a synthesized hidden
        // view and an engine-internal index undroppable by name — a clearer error
        // than the engine's own refusal, which stays the backstop.
        validate_user_name(&name)?;
        targets.push(name);
    }
    reject_repeated_object(targets.iter().map(String::as_str), "DROP")?;
    let targets: Vec<&str> = targets.iter().map(String::as_str).collect();

    // `IF EXISTS` rides the verb, which resolves its own targets — the one place
    // that can answer "no such object" without a second lookup. It softens nothing
    // else: one refusal still fails the whole statement.
    match object_type {
        ObjectType::View => client.drop_view(schema_name, &targets, if_exists)?,
        ObjectType::Index => client.drop_indexes_by_name(&targets, if_exists)?,
        _ => client.drop_table(schema_name, &targets, if_exists)?,
    }
    Ok(SqlResult::Dropped)
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
    pub(crate) explicit_name: Option<String>,
    pub(crate) site: IndexSite,
}

pub(crate) fn execute_create_index(
    client: &mut GnitzClient,
    schema_name: &str,
    ci: &sqlparser::ast::CreateIndex,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    reject_unhonored_create_index_clauses(ci)?;
    let table_name = extract_object_name(&ci.table_name, schema_name, "CREATE INDEX")?;
    let explicit_name = ci
        .name
        .as_ref()
        .map(|n| extract_index_name(n, "CREATE INDEX"))
        .transpose()?;
    // The one resolve on this path: `create_index_core` takes the descriptor.
    let target = binder.resolve_base_table(client, &table_name, "CREATE INDEX")?;
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
    // The name reaches the IDX_TAB row, so a malformed one would persist and be
    // undroppable. Folded here because the catalog stores the folded form, which
    // the `IF NOT EXISTS` test below compares against.
    let explicit_name = req.explicit_name.as_deref().map(canonical_user_name).transpose()?;

    if req.columns.is_empty() {
        return Err(GnitzSqlError::Plan(format!("{ctx}: at least one column required")));
    }

    let (table_id, schema) = (target.tid, &target.schema);
    let (col_names, col_indices) = resolve_index_columns(req.columns, &schema.columns, ctx)?;

    // The same rule the engine applies, run here for the message: it names the
    // offending column, which the engine cannot.
    let col_types: Vec<TypeCode> = col_indices
        .iter()
        .map(|&c| schema.columns[c as usize].type_code)
        .collect();
    reject_unbuildable_index_key(&col_names, &col_types, schema.pk_count(), schema.pk_stride(), ctx)?;

    let index_name = match explicit_name {
        Some(name) => {
            // Index names are globally unique, so a name already standing — on
            // this table or any other — means this CREATE would fail; the clause
            // says skip it. Without it the collision errors in `create_index`.
            if req.site.if_not_exists() {
                let standing = client
                    .index_rows()?
                    .into_iter()
                    .find(|(_, n, _)| n == &name)
                    .map(|(id, _, _)| id);
                if let Some(index_id) = standing {
                    return Ok(SqlResult::IndexCreated { index_id });
                }
            }
            name
        }
        None => {
            let base = default_index_name(schema_name, req.table_name, &col_names);
            let existing = client.index_rows()?;
            // A prior *auto-named* index on this exact column set is the same index.
            // An FK-backing or explicitly-named index on these columns carries a
            // different name, so it never blocks a distinct auto-name.
            if existing
                .iter()
                .any(|(_, name, cols)| name == &base && cols.as_slice() == col_indices.as_slice())
            {
                return Err(GnitzSqlError::Plan(format!(
                    "an index on these columns already exists as '{base}'"
                )));
            }
            let taken: HashSet<String> = existing.into_iter().map(|(_, n, _)| n).collect();
            disambiguate_index_name(base, &taken)
        }
    };

    let index_id = client.create_index(table_id, &col_indices, &col_types, &index_name, req.site.is_unique())?;

    Ok(SqlResult::IndexCreated { index_id })
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
