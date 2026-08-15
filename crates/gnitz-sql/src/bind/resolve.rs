use crate::ast_util::{classify_from, extract_table_factor_name, is_bare_wildcard_projection, FromShape};
use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, GnitzClient, Schema};
use sqlparser::ast::{Expr, Select, SelectItem, TableAliasColumnDef};
use std::collections::HashMap;
use std::rc::Rc;

/// Find the column named `col_name` in `columns`, case-insensitively.
///
/// - `Ok(Some(idx))` — exactly one column matches.
/// - `Ok(None)` — no column matches (callers attach their own "not found" text).
/// - `Err(Bind)` — more than one column matches. A `SELECT *` join view carries
///   same-named columns from both sides; an unqualified reference to such a name
///   is ambiguous (standard SQL) and must be rejected, not silently bound to the
///   first match.
///
/// Single home for the name→index lookup that was previously
/// `columns.iter().position(...)` (first-match) at every call site.
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

/// What the catalog issued an id as. `None` wherever this appears means the id
/// was not catalog-issued at all — see [`CachedRelation::catalog_kind`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RelationKind {
    Table,
    View,
}

/// One Binder cache entry: a name's resolved id and schema, plus how that id was
/// issued.
///
/// `catalog_kind` is `None` for a chain-minted CTE/derived-table segment id.
/// Those ids are minted `1, 2, 3, …` per chain, so they alias real relation ids
/// (`SCHEMA_TAB = 1`, `TABLE_TAB = 2`, … `FIRST_USER_TABLE_ID = 16`): asking the
/// catalog for segment 1's indexes probes the system schema table, and at `>= 16`
/// it would match a *foreign* user table's index column indices against this
/// segment's schema by pure numeric coincidence — a bogus bound, and a wasted
/// round-trip per segment. The field lives IN the entry so an alias that shadows
/// an earlier resolution (`FROM (SELECT * FROM t) t` re-caching `t` as a minted
/// segment) atomically replaces id and provenance together.
struct CachedRelation {
    table_id: u64,
    schema: Rc<Schema>,
    catalog_kind: Option<RelationKind>,
}

/// A resolved relation: its id, its schema, and `None` if the id was chain-minted
/// rather than catalog-issued.
pub(crate) type Resolved = (u64, Rc<Schema>, Option<RelationKind>);

pub(crate) struct Binder<'a> {
    schema_name: &'a str,
    cache: HashMap<String, CachedRelation>,
}

impl<'a> Binder<'a> {
    pub(crate) fn new(schema_name: &'a str) -> Self {
        Binder {
            schema_name,
            cache: HashMap::new(),
        }
    }

    /// Cache a `name → relation` entry, keyed by the canonical ASCII-lowercase
    /// form — the single fold site, so a case-varying reference (`WITH Cc … FROM
    /// cc`) hits regardless of which caller inserted (SQL identifiers are
    /// case-insensitive).
    fn cache_relation(&mut self, name: &str, table_id: u64, schema: Rc<Schema>, catalog_kind: Option<RelationKind>) {
        self.cache.insert(
            name.to_ascii_lowercase(),
            CachedRelation {
                table_id,
                schema,
                catalog_kind,
            },
        );
    }

    /// Resolve `name` to its id, schema and provenance. The provenance rides the
    /// same entry as the id, so a caller that needs it makes no second lookup and
    /// the two can never disagree.
    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<Resolved, GnitzSqlError> {
        // Probe with the canonical key — the cache holds base-table resolutions
        // *and* CTE/derived-table aliases (which never reach the catalog).
        if let Some(entry) = self.cache.get(&name.to_ascii_lowercase()) {
            return Ok((entry.table_id, Rc::clone(&entry.schema), entry.catalog_kind));
        }
        // Referenced relations obey the same reserved-prefix rule as created
        // ones: a fresh catalog probe of a leading-`_` name can only be a user
        // naming internal plumbing (a hidden chain segment `__h{vid}_{idx}`), and
        // honoring it leaks a dependency that makes the owner view undroppable.
        // Placed after the cache check — every cached name passed the same rule
        // on insert (`cache_alias` validates; `cache_relation` is fed from these
        // already-validated probes), never a raw `__h…` catalog name.
        crate::validate::validate_user_name(name)?;
        let (tid, schema, is_view) = client
            .resolve_relation(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        let kind = Some(if is_view {
            RelationKind::View
        } else {
            RelationKind::Table
        });
        let rc = Rc::new(schema);
        self.cache_relation(name, tid, Rc::clone(&rc), kind);
        Ok((tid, rc, kind))
    }

    /// Resolve a write/index target that must be a base table. INSERT, UPDATE,
    /// DELETE and CREATE INDEX may only act on base tables: a view is a read-only
    /// derived relation whose store is maintained solely by its circuit, so
    /// writing to it or indexing it corrupts that state. `resolve` (used by
    /// SELECT and view definitions) still accepts views; this is the
    /// writable-target variant.
    pub(crate) fn resolve_base_table(
        &mut self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        // Same reserved-prefix rule as the read funnel: a write/index target
        // naming a leading-`_` relation can only be a user reaching for system
        // plumbing (a hidden segment). Rejecting here — before the catalog probe —
        // also closes the existence side-channel the two-probe fallback below
        // would otherwise open (a hidden segment returns "is a view", a missing
        // name returns "not found").
        crate::validate::validate_user_name(name)?;
        // `resolve_table_id` consults TABLE_TAB only, so a view name misses it.
        match client.resolve_table_id(self.schema_name, name) {
            Ok((tid, schema)) => {
                let rc = Rc::new(schema);
                self.cache_relation(name, tid, Rc::clone(&rc), Some(RelationKind::Table));
                Ok((tid, rc))
            }
            // Miss: kind-probe (id-only, no schema assembly) to tell "is a view"
            // (reject as read-only) from "does not exist" (propagate the
            // original error).
            Err(not_found) => match client.resolve_relation_kind(self.schema_name, name) {
                Ok(Some((_, true))) => Err(GnitzSqlError::Unsupported(format!(
                    "'{name}' is a view; INSERT, UPDATE, DELETE and CREATE INDEX \
                     require a base table"
                ))),
                _ => Err(GnitzSqlError::Exec(not_found)),
            },
        }
    }

    /// Cache a CTE / derived-table alias as resolving to the given
    /// (table_id, schema). The alias is a user-chosen relation name that later
    /// references resolve *ahead of* the funnel guard in `resolve` (the cache is
    /// probed before validation), so it is held to the same reserved-prefix rule
    /// here — the one gate every alias passes to become resolvable.
    ///
    /// `catalog_kind` is the caller's: a pass-through CTE passes its source's
    /// kind, a compiled derived table / CTE segment passes `None` (see
    /// [`CachedRelation`]).
    pub(crate) fn cache_alias(&mut self, name: &str, resolved: Resolved) -> Result<(), GnitzSqlError> {
        crate::validate::validate_user_name(name)?;
        let (table_id, schema, catalog_kind) = resolved;
        self.cache_relation(name, table_id, schema, catalog_kind);
        Ok(())
    }
}

/// Apply positional column aliases (`WITH d(a, b) AS …` / `(subquery) AS d(a, b)`)
/// to the *visible* columns of a body's output, in order — a rename only (`ColId`s
/// and layout are untouched). A JOIN body's leading synthetic-PK region is hidden
/// and skipped automatically, so positional aliases name exactly the visible output
/// columns — the same columns a downstream query sees — with no misalignment.
///
/// One home for both column shapes this runs over: a hidden segment's registered
/// `ColumnDef`s, and the `HirCol` scope/env cols a derived table resolves
/// `d.col` / wildcard against (the caller projects to `&mut c.def`).
pub(crate) fn apply_positional_aliases(
    aliases: &[TableAliasColumnDef],
    defs: Vec<&mut ColumnDef>,
    ctx: &str,
) -> Result<(), GnitzSqlError> {
    if aliases.is_empty() {
        return Ok(());
    }
    let mut visible: Vec<&mut ColumnDef> = defs.into_iter().filter(|c| !c.is_hidden).collect();
    if aliases.len() != visible.len() {
        return Err(GnitzSqlError::Plan(format!(
            "{ctx} defines {} column aliases but body returns {} columns",
            aliases.len(),
            visible.len(),
        )));
    }
    for (col, alias) in visible.iter_mut().zip(aliases) {
        col.name = alias.name.value.clone();
    }
    Ok(())
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
    client: &mut GnitzClient,
    cte_select: &Select,
    column_aliases: &[TableAliasColumnDef],
    binder: &mut Binder<'_>,
) -> Result<Option<Resolved>, GnitzSqlError> {
    // A single plain table/view FROM, no joins, no derived table.
    if !matches!(classify_from(&cte_select.from), FromShape::SinglePlainRelation) {
        return Ok(None);
    }
    let cte_table_name = extract_table_factor_name(&cte_select.from[0].relation, "CTE")?;
    let (cte_tid, cte_schema, cte_kind) = binder.resolve(client, &cte_table_name)?;
    // Positional identity projection: `*`, or one identifier per source column in
    // order. The qualified form (`SELECT t.a, t.b FROM t`) parses as `CompoundIdentifier`
    // and is the same positional pass-through; a dup-named source fails the per-position
    // compare and is not identity. Only a *bare* `*` is identity — a
    // `* EXCEPT/EXCLUDE/RENAME` (or a rejected `* REPLACE/ILIKE`) CTE body is not.
    let proj_is_identity = is_bare_wildcard_projection(&cte_select.projection)
        || (cte_select.projection.len() == cte_schema.columns.len()
            && cte_select.projection.iter().enumerate().all(|(i, item)| {
                let want = &cte_schema.columns[i].name;
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.eq_ignore_ascii_case(want),
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                        parts[1].value.eq_ignore_ascii_case(want)
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
        apply_positional_aliases(column_aliases, s.columns.iter_mut().collect(), "CTE")?;
        Rc::new(s)
    } else {
        cte_schema
    };
    Ok(Some((cte_tid, cte_schema, cte_kind)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, TypeCode};

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef::new(name, tc, false)
    }

    /// The index-bound gate: a cached alias reports back the provenance it was
    /// cached with, keyed case-insensitively, and an unseen name resolves to
    /// nothing. A chain-minted id aliases real relation ids, so treating it as
    /// catalog-issued would bound a scan against a foreign table's index columns.
    #[test]
    fn catalog_provenance_tracks_the_cache_alias_kind() {
        let schema = std::rc::Rc::new(Schema {
            columns: vec![col("a", TypeCode::U64)],
            pk_cols: vec![0],
        });
        let kind = |b: &Binder<'_>, n: &str| b.cache.get(&n.to_ascii_lowercase()).map(|e| e.catalog_kind);
        let mut b = Binder::new("public");
        b.cache_alias("minted", (1, Rc::clone(&schema), None)).unwrap();
        b.cache_alias("real", (16, Rc::clone(&schema), Some(RelationKind::Table)))
            .unwrap();
        b.cache_alias("aview", (17, Rc::clone(&schema), Some(RelationKind::View)))
            .unwrap();

        assert_eq!(
            kind(&b, "minted"),
            Some(None),
            "a chain-minted id is not catalog-issued"
        );
        assert_eq!(kind(&b, "real"), Some(Some(RelationKind::Table)));
        assert_eq!(kind(&b, "aview"), Some(Some(RelationKind::View)));
        assert_eq!(kind(&b, "unseen"), None);
        // Provenance keys on the same lowercased string as the resolution.
        assert_eq!(kind(&b, "REAL"), Some(Some(RelationKind::Table)));
        assert_eq!(kind(&b, "MINTED"), Some(None));

        // Shadowing: a minted alias overwriting a catalog resolution must drop
        // the provenance with it — `FROM (SELECT * FROM t) t` re-caches `t` as a
        // chain-minted segment, and a stale `Some(_)` here would probe a foreign
        // table's indexes for the segment's bound.
        b.cache_alias("real", (2, Rc::clone(&schema), None)).unwrap();
        assert_eq!(
            kind(&b, "real"),
            Some(None),
            "an alias shadowing a catalog resolution must shed its provenance"
        );
    }

    #[test]
    fn test_find_unique_column_unique() {
        let cols = vec![col("a", TypeCode::U64), col("b", TypeCode::I64)];
        assert_eq!(find_unique_column(&cols, "a").unwrap(), Some(0));
        assert_eq!(find_unique_column(&cols, "B").unwrap(), Some(1)); // case-insensitive
    }

    #[test]
    fn test_find_unique_column_absent() {
        let cols = vec![col("a", TypeCode::U64)];
        assert_eq!(find_unique_column(&cols, "missing").unwrap(), None);
    }

    #[test]
    fn test_find_unique_column_duplicate_is_ambiguous() {
        // Two case-insensitively equal names (as a `SELECT *` join view produces).
        let cols = vec![col("Id", TypeCode::U64), col("ID", TypeCode::U64)];
        match find_unique_column(&cols, "id") {
            Err(GnitzSqlError::Bind(s)) => assert!(s.contains("ambiguous"), "got: {s}"),
            other => panic!("expected Bind(ambiguous), got {other:?}"),
        }
    }
}
