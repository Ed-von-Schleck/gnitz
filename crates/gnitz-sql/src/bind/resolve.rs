use crate::ast_util::{classify_from, extract_table_name_and_alias, is_bare_wildcard_projection, FromShape};
use crate::error::GnitzSqlError;
use gnitz_core::{CatalogSnapshot, ClientError, ColumnDef, GnitzClient, RelClass, RelDescriptor, Schema};
use sqlparser::ast::{Expr, Ident, Select, SelectItem, TableAliasColumnDef};
use std::collections::HashMap;
use std::sync::Arc;

/// Find the column named `col_name` in `columns`, case-insensitively.
///
/// - `Ok(Some(idx))` — exactly one column matches.
/// - `Ok(None)` — no column matches (callers attach their own "not found" text).
/// - `Err(Bind)` — more than one column matches. A `SELECT *` join view carries
///   same-named columns from both sides; an unqualified reference to such a name
///   is ambiguous (standard SQL) and must be rejected, not silently bound to the
///   first match.
///
/// Hidden columns (`is_hidden`) are skipped as match candidates but keep their
/// physical position, so the returned index is always the real offset into
/// `columns` (every caller adds a physical `col_offset` to it). Every
/// readable-relation (table or view) name→index resolution funnels through
/// here, so a synthetic view key (`_join_pk`, `_group_pk`, …) or an unprojected
/// passthrough PK is unresolvable by name everywhere, without pruning any
/// schema or shifting any offset — and a visible name shared with a hidden
/// column is not spuriously ambiguous. (The DML sites that resolve names by
/// hand — INSERT/UPDATE column lists — target base tables, whose only hidden
/// columns come from DROP COLUMN and are excluded here by the `!c.is_hidden`
/// filter, so a dropped column is unnameable.)
pub(crate) fn find_unique_column<'a>(
    columns: impl IntoIterator<Item = &'a ColumnDef>,
    col_name: &str,
) -> Result<Option<usize>, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for (i, c) in columns.into_iter().enumerate() {
        if !c.is_hidden && c.name.eq_ignore_ascii_case(col_name) {
            if found.is_some() {
                return Err(GnitzSqlError::Bind(format!(
                    "column reference '{col_name}' is ambiguous"
                )));
            }
            found = Some(i);
        }
    }
    Ok(found)
}

/// One Binder cache entry: a name's resolved id and schema, plus the catalog
/// descriptor the id came from.
///
/// `desc` is `None` for a chain-minted CTE/derived-table segment id: its catalog
/// rows are not published until the whole chain is created at statement end, so
/// there is no descriptor to carry. It lives in the entry so an alias that
/// shadows an earlier resolution (`FROM (SELECT * FROM t) t` re-caching `t` as a
/// minted segment) replaces id and descriptor together.
///
/// The schema rides beside the descriptor rather than being read out of it: a
/// CTE's positional column aliases rename the schema the alias resolves through,
/// while the descriptor keeps the source relation's own names.
struct CachedRelation {
    table_id: u64,
    schema: Arc<Schema>,
    desc: Option<Arc<RelDescriptor>>,
}

/// A resolved relation: its id, the schema references resolve through, and its
/// catalog descriptor — `None` if the id was chain-minted rather than
/// catalog-issued.
pub(crate) type Resolved = (u64, Arc<Schema>, Option<Arc<RelDescriptor>>);

pub(crate) struct Binder<'a> {
    schema_name: &'a str,
    cache: HashMap<String, CachedRelation>,
    /// This binder is binding a view *body* (CREATE VIEW / ALTER VIEW), where
    /// the leaf rule applies: a capacity-bounded view may not be a source.
    view_body: bool,
}

impl<'a> Binder<'a> {
    pub(crate) fn new(schema_name: &'a str) -> Self {
        Binder {
            schema_name,
            cache: HashMap::new(),
            view_body: false,
        }
    }

    /// Bind in view-body mode: every relation this binder resolves becomes a
    /// source of a new view, so the leaf rule in [`Self::resolve`] applies. Set by
    /// `plan_view` for CREATE VIEW / ALTER VIEW.
    pub(crate) fn for_view_body(mut self) -> Self {
        self.view_body = true;
        self
    }

    /// Cache a `name → relation` entry, keyed by the canonical ASCII-lowercase
    /// form — the single fold site, so a case-varying reference (`WITH Cc … FROM
    /// cc`) hits regardless of which caller inserted (SQL identifiers are
    /// case-insensitive).
    fn cache_relation(&mut self, name: &str, table_id: u64, schema: Arc<Schema>, desc: Option<Arc<RelDescriptor>>) {
        self.cache
            .insert(name.to_ascii_lowercase(), CachedRelation { table_id, schema, desc });
    }

    /// Resolve `name` to its id, schema and descriptor against the statement's
    /// catalog snapshot. The descriptor rides the same cache entry as the id, so
    /// the two cannot disagree.
    ///
    /// A name the snapshot does not hold raises [`GnitzSqlError::CatalogMiss`],
    /// which `dispatch::plan_resolving` answers by resolving it and re-running the
    /// pass. A name it holds as a recorded absence is the ordinary "not found".
    pub(crate) fn resolve(&mut self, cat: &CatalogSnapshot, name: &str) -> Result<Resolved, GnitzSqlError> {
        // Probe with the canonical key — the cache holds base-table resolutions
        // *and* CTE/derived-table aliases (which never reach the catalog).
        if let Some(entry) = self.cache.get(&name.to_ascii_lowercase()) {
            return Ok((entry.table_id, Arc::clone(&entry.schema), entry.desc.clone()));
        }
        // Referenced relations obey the same reserved-prefix rule as created
        // ones: a fresh catalog probe of a leading-`_` name can only be a user
        // naming internal plumbing (a chain segment), and honoring it leaks a
        // dependency that makes the owner view undroppable. Placed after the
        // cache check — every cached name passed the same rule on insert
        // (`cache_alias` validates; `cache_relation` is fed from these
        // already-validated probes), never a raw internal catalog name.
        crate::validate::validate_user_name(name)?;
        let rel = cat
            .get(self.schema_name, name)
            .ok_or_else(|| GnitzSqlError::CatalogMiss(name.to_string()))?
            .ok_or_else(|| {
                GnitzSqlError::Exec(ClientError::ServerError(format!(
                    "Table or view '{}' not found",
                    gnitz_core::qualified_name(self.schema_name, name)
                )))
            })?;
        let schema = Arc::clone(&rel.schema);
        // Leaf rule. A bounded view's store keeps only skeleton rows past its
        // capacity, and hydrating them replays *sources* — so a view over one
        // would have to hydrate through it, and its own store would be a second
        // derived copy of already-lossy state. One funnel, one check: this covers
        // direct FROM, join sides, subquery inners, set-op sides and CTE bodies
        // alike. A chain-minted segment id never reaches here (it is served from
        // the alias cache) and a hidden segment never carries a capacity.
        if self.view_body && rel.class == RelClass::BoundedView {
            return Err(GnitzSqlError::Unsupported(format!(
                "'{name}' is a capacity-bounded view; views cannot be created over it"
            )));
        }
        // The opposite polarity to the leaf rule above, through the same funnel: a
        // view body is exactly where reading a stream is the point.
        if !self.view_body && rel.class == RelClass::Stream {
            return Err(GnitzSqlError::Unsupported(format!(
                "'{name}' is a stream; it holds no rows and can only be read inside a view body"
            )));
        }
        self.cache_relation(name, rel.tid, Arc::clone(&schema), Some(Arc::clone(&rel)));
        Ok((rel.tid, schema, Some(rel)))
    }

    /// Resolve a write/index target that must be a base table: UPDATE, DELETE and
    /// CREATE INDEX all read stored rows back before writing, and a view's store is
    /// maintained solely by its circuit while a stream has none at all. `resolve`
    /// (used by SELECT and view definitions) still accepts views.
    ///
    /// Same reserved-prefix rule as the read funnel, for the same reason: a write
    /// target naming a leading-`_` relation can only be a user reaching for system
    /// plumbing (a hidden segment). Rejecting before the catalog probe also closes
    /// the existence side-channel a two-probe fallback would open — one resolve
    /// answers id, schema and class together, so "is a view" and "does not exist" are
    /// distinguished without a second probe.
    pub(crate) fn resolve_base_table(
        &mut self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        crate::validate::validate_user_name(name)?;
        let rel = client.resolve_relation(self.schema_name, name)?;
        if rel.class != RelClass::Table {
            return Err(GnitzSqlError::Unsupported(format!(
                "'{name}' is a {}; UPDATE, DELETE and CREATE INDEX require a base table",
                rel.class.noun()
            )));
        }
        self.cache_relation(name, rel.tid, Arc::clone(&rel.schema), Some(Arc::clone(&rel)));
        Ok(rel)
    }

    /// Resolve an INSERT target, which may be a base table or a stream — the one
    /// writable-target caller that admits a stream. Same name rule as
    /// [`Self::resolve_base_table`]. Deliberately does not cache: [`Self::resolve`]
    /// returns on a cache hit before its class rules run, so a cached stream would
    /// let a later reference to the name pass as an ordinary readable relation.
    pub(crate) fn resolve_push_target(
        &mut self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        crate::validate::validate_user_name(name)?;
        let rel = client.resolve_relation(self.schema_name, name)?;
        if rel.class.is_view() {
            return Err(GnitzSqlError::Unsupported(format!(
                "'{name}' is a {}; INSERT requires a base table or a stream",
                rel.class.noun()
            )));
        }
        Ok(rel)
    }

    /// Cache a CTE / derived-table alias as resolving to the given
    /// (table_id, schema). The alias is a user-chosen relation name that later
    /// references resolve *ahead of* the funnel guard in `resolve` (the cache is
    /// probed before validation), so it is held to the same reserved-prefix rule
    /// here — the one gate every alias passes to become resolvable.
    ///
    /// The descriptor is the caller's: a pass-through CTE passes its source's, a
    /// compiled derived table / CTE segment passes `None` (see [`CachedRelation`]).
    pub(crate) fn cache_alias(&mut self, name: &str, resolved: Resolved) -> Result<(), GnitzSqlError> {
        crate::validate::validate_user_name(name)?;
        let (table_id, schema, desc) = resolved;
        self.cache_relation(name, table_id, schema, desc);
        Ok(())
    }
}

/// Apply positional column aliases (`WITH d(a, b) AS …` / `(subquery) AS d(a, b)`
/// / `CREATE VIEW v (a, b) AS …`) to the *visible* columns of a body's output, in
/// order — a rename only, leaving `ColId`s and layout untouched. Hidden columns
/// (a JOIN body's synthetic-PK region) are skipped, so an alias list names exactly
/// what a downstream query sees.
///
/// A rename can collide where the original names did not, so the duplicate check
/// belongs here rather than at each caller.
pub(crate) fn apply_positional_aliases<'a>(
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    defs: Vec<&mut ColumnDef>,
    ctx: &str,
) -> Result<(), GnitzSqlError> {
    let n_aliases = aliases.len();
    if n_aliases == 0 {
        return Ok(());
    }
    let mut visible: Vec<&mut ColumnDef> = defs.into_iter().filter(|c| !c.is_hidden).collect();
    if n_aliases != visible.len() {
        return Err(GnitzSqlError::Plan(format!(
            "{ctx} defines {n_aliases} column aliases but body returns {} columns",
            visible.len(),
        )));
    }
    for (col, alias) in visible.iter_mut().zip(aliases) {
        col.name = alias.value.clone();
    }
    crate::validate::reject_duplicate_column_names(visible.iter().map(|c| &**c), &format!("{ctx} column aliases"))
}

/// The pure pass-through predicate shared by the CTE binding (`bind_ctes`) and the
/// ad-hoc read route (`dml::select`): a CTE body that is a bare single-table (or
/// view) identity/positional projection resolves directly to its source relation,
/// with any column aliases applied. Returns `Some(resolved)` for such an aliasable
/// pass-through, `None` for everything else (a joined / multi-FROM / derived-table
/// FROM or a non-identity projection); `Err` only on a hard bind failure (unknown
/// source relation). The caller decides what `None` means: the CTE binding compiles
/// a hidden segment; the read route rejects the whole query as a derivation. Both
/// callers reject a WHERE'd / grouped / DISTINCT / exotic-clause body BEFORE
/// calling (each with its own verdict), so such a body never reaches this
/// predicate.
///
/// The source's provenance rides the result: a pass-through over an earlier CTE
/// that compiled to a chain-minted segment inherits that segment's `None`, so the
/// alias never claims a catalog id it does not have.
pub(crate) fn cte_passthrough(
    cat: &CatalogSnapshot,
    cte_select: &Select,
    column_aliases: &[TableAliasColumnDef],
    binder: &mut Binder<'_>,
) -> Result<Option<Resolved>, GnitzSqlError> {
    // A single plain table/view FROM, no joins, no derived table.
    if !matches!(classify_from(&cte_select.from), FromShape::SinglePlainRelation) {
        return Ok(None);
    }
    let (cte_table_name, cte_alias) = extract_table_name_and_alias(&cte_select.from[0].relation, "CTE")?;
    let (cte_tid, cte_schema, cte_kind) = binder.resolve(cat, &cte_table_name)?;
    // Positional identity projection: a *bare* `*`, or one identifier per source
    // column in order. A qualified identifier (`SELECT t.a FROM t`) is the same
    // pass-through, but only under the source's own alias — the compiled path
    // rejects any other qualifier, so this fast path must too.
    let proj_is_identity = is_bare_wildcard_projection(&cte_select.projection)
        || (cte_select.projection.len() == cte_schema.columns.len()
            && cte_select.projection.iter().enumerate().all(|(i, item)| {
                let want = &cte_schema.columns[i].name;
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.eq_ignore_ascii_case(want),
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                        parts[0].value.eq_ignore_ascii_case(&cte_alias) && parts[1].value.eq_ignore_ascii_case(want)
                    }
                    _ => false,
                }
            }));
    if !proj_is_identity {
        return Ok(None);
    }
    // Apply CTE column aliases (`WITH cte(a, b) AS ...`).
    let cte_schema = if !column_aliases.is_empty() {
        let mut s = (*cte_schema).clone();
        apply_positional_aliases(
            column_aliases.iter().map(|a| &a.name),
            s.columns.iter_mut().collect(),
            "CTE",
        )?;
        Arc::new(s)
    } else {
        cte_schema
    };
    Ok(Some((cte_tid, cte_schema, cte_kind)))
}

#[cfg(test)]
#[path = "tests/resolve.rs"]
mod tests;
