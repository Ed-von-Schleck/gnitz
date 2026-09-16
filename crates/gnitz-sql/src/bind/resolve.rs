use crate::ast_util::col_ref_parts;
use crate::error::GnitzSqlError;
use gnitz_core::{CatalogSnapshot, ColumnDef, GnitzClient, RelClass, RelDescriptor};
use sqlparser::ast::{Expr, Ident};
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

/// [`find_unique_column`] for a name that must resolve.
pub(crate) fn require_column<'a>(
    columns: impl IntoIterator<Item = &'a ColumnDef>,
    col_name: &str,
) -> Result<usize, GnitzSqlError> {
    find_unique_column(columns, col_name)?.ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found")))
}

/// The output column `e` names, if it names one. Matched by name alone: an
/// output column carries no qualifier to check against.
pub(crate) fn output_column<'a>(
    e: &Expr,
    cols: impl IntoIterator<Item = &'a ColumnDef>,
) -> Result<Option<usize>, GnitzSqlError> {
    match col_ref_parts(e) {
        Some((_, name)) => find_unique_column(cols, name),
        None => Ok(None),
    }
}

pub(crate) struct Binder<'a> {
    schema_name: &'a str,
}

impl<'a> Binder<'a> {
    pub(crate) fn new(schema_name: &'a str) -> Self {
        Binder { schema_name }
    }

    /// The session schema every name this binder resolves is scoped to.
    pub(crate) fn schema_name(&self) -> &'a str {
        self.schema_name
    }

    /// Resolve `name` as an ad-hoc read's source.
    pub(crate) fn resolve(&self, cat: &CatalogSnapshot, name: &str) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        let rel = probe_relation(cat, self.schema_name, name)?;
        if rel.class == RelClass::Stream {
            return Err(GnitzSqlError::Unsupported(format!(
                "'{name}' is a stream; it holds no rows and can only be read inside a view body"
            )));
        }
        Ok(rel)
    }

    /// Resolve the base table an UPDATE, DELETE or CREATE INDEX (`op`) writes.
    pub(crate) fn resolve_base_table(
        &self,
        client: &mut GnitzClient,
        name: &str,
        op: &str,
    ) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        crate::validate::validate_user_name(name)?;
        let rel = client.resolve_relation(self.schema_name, name)?;
        crate::validate::require_class(&rel, name, crate::validate::ClassWant::BaseTable, op)?;
        Ok(rel)
    }

    /// Resolve an INSERT target, which may be a base table or a stream — the one
    /// writable-target caller that admits a stream. Same name rule as
    /// [`Self::resolve_base_table`].
    pub(crate) fn resolve_push_target(
        &self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        crate::validate::validate_user_name(name)?;
        let rel = client.resolve_relation(self.schema_name, name)?;
        crate::validate::require_class(&rel, name, crate::validate::ClassWant::BaseTableOrStream, "INSERT")?;
        Ok(rel)
    }
}

/// The relation `name` a statement reads: [`probe`], with absence an error.
pub(crate) fn probe_relation(
    cat: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
    // A leading `_` names a hidden chain segment, which no statement may read.
    crate::validate::validate_user_name(name)?;
    probe(cat, schema_name, name)?.ok_or_else(|| crate::error::missing_relation("Table or view", schema_name, name))
}

/// The relation `name` resolves to in the statement's snapshot, `None` for a free
/// name, and [`GnitzSqlError::CatalogMiss`] for one the snapshot has not probed —
/// the signal `dispatch::plan_resolving` answers by resolving and re-running.
pub(crate) fn probe(
    cat: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
    cat.get(schema_name, name)
        .ok_or_else(|| GnitzSqlError::CatalogMiss(name.to_string()))
}

/// Apply positional column aliases (`WITH d(a, b) AS …` / `(subquery) AS d(a, b)`
/// / `CREATE VIEW v (a, b) AS …`) to the *visible* columns of a body's output, in
/// order — a rename only, leaving `ColId`s and layout untouched. Hidden columns
/// (a JOIN body's synthetic-PK region) are skipped, so an alias list names exactly
/// what a downstream query sees.
///
/// A rename can collide where the original names did not, so the duplicate check
/// belongs here rather than at each caller.
pub(crate) fn apply_positional_aliases<'a, 'b>(
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    defs: impl IntoIterator<Item = &'b mut ColumnDef>,
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

#[cfg(test)]
#[path = "tests/resolve.rs"]
mod tests;
