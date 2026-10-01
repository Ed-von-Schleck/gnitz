use crate::ast_util::col_ref_parts;
use crate::error::GnitzSqlError;
use gnitz_core::{qualified_name, RelDescriptor};
use gnitz_wire::ColumnDef;
use sqlparser::ast::{Expr, Ident};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// The one visible column of `columns` named `col_name`, matched
/// case-insensitively, as its physical index into `columns`.
///
/// - `Ok(Some(idx))` — exactly one visible column matches.
/// - `Ok(None)` — no visible column matches (callers attach their own "not
///   found" text).
/// - `Err(Bind)` — more than one matches. A `SELECT *` join view carries
///   same-named columns from both sides; an unqualified reference to such a name
///   is ambiguous (standard SQL) and must be rejected, not silently bound to the
///   first match.
///
/// Hidden columns are never candidates but keep their position, so a synthetic
/// view key is unnameable and a visible name shared with a hidden column is not
/// ambiguous.
pub(crate) fn find_unique_column<'a>(
    columns: impl IntoIterator<Item = &'a ColumnDef>,
    col_name: &str,
) -> Result<Option<usize>, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for (i, c) in columns.into_iter().enumerate() {
        if !c.is_hidden && c.name.eq_ignore_ascii_case(col_name) {
            if found.is_some() {
                return Err(GnitzSqlError::Rejected(format!(
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
    find_unique_column(columns, col_name)?
        .ok_or_else(|| GnitzSqlError::Rejected(format!("column '{col_name}' not found")))
}

/// The output column `e` names, if it names one. Only an unqualified name can:
/// an output column belongs to no relation, so a qualified reference is left to
/// the caller's scope, which checks the qualifier.
pub(crate) fn output_column<'a>(
    e: &Expr,
    cols: impl IntoIterator<Item = &'a ColumnDef>,
) -> Result<Option<usize>, GnitzSqlError> {
    match col_ref_parts(e) {
        Some((None, name)) => find_unique_column(cols, name),
        _ => Ok(None),
    }
}

/// What a statement's resolver answers for one relation name.
pub(crate) type Resolve<'r> = &'r dyn Fn(&str) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError>;

/// The relations one statement's plan reads under `schema_name`: each name is
/// resolved on its first probe and remembered, absence included.
pub(crate) struct Catalog<'r> {
    schema_name: &'r str,
    resolve: Resolve<'r>,
    known: RefCell<HashMap<String, Option<Arc<RelDescriptor>>>>,
}

impl<'r> Catalog<'r> {
    pub(crate) fn new(schema_name: &'r str, resolve: Resolve<'r>) -> Self {
        Catalog {
            schema_name,
            resolve,
            known: RefCell::new(HashMap::new()),
        }
    }

    /// The schema every name of the statement resolves under.
    pub(crate) fn schema_name(&self) -> &'r str {
        self.schema_name
    }

    /// The relation `name` resolves to, `None` for a free name.
    pub(crate) fn probe(&self, name: &str) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
        let key = qualified_name(self.schema_name, name);
        let hit = self.known.borrow().get(&key).cloned();
        if let Some(hit) = hit {
            return Ok(hit);
        }
        let found = (self.resolve)(name)?;
        self.known.borrow_mut().insert(key, found.clone());
        Ok(found)
    }

    /// The relation `name` a statement reads: [`Self::probe`], with absence an
    /// error.
    pub(crate) fn probe_relation(&self, name: &str) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        self.probe(name)?
            .ok_or_else(|| crate::error::missing_relation(self.schema_name, name))
    }

    /// Record `name`'s verdict without asking the resolver.
    #[cfg(test)]
    pub(crate) fn insert(&self, name: &str, desc: Option<Arc<RelDescriptor>>) {
        self.known
            .borrow_mut()
            .insert(qualified_name(self.schema_name, name), desc);
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
        return Err(GnitzSqlError::Rejected(format!(
            "{ctx} defines {n_aliases} column aliases but body returns {} columns",
            visible.len(),
        )));
    }
    for (col, alias) in visible.iter_mut().zip(aliases) {
        col.name = alias.value.clone();
    }
    crate::rules::reject_duplicate_column_names(visible.iter().map(|c| &**c), &format!("{ctx} column aliases"))
}

#[cfg(test)]
#[path = "tests/resolve.rs"]
mod tests;
