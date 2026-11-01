//! SELECT * and its EXCEPT / EXCLUDE / RENAME modifiers, expanded over a scope's columns.

use crate::ast_util::single_part_ident;
use crate::error::{unsupported_clause, GnitzSqlError};
use crate::rules::has_visible_column;
use gnitz_wire::ColumnDef;
use sqlparser::ast::{ExcludeSelectItem, ObjectName, RenameSelectItem, SelectItem, WildcardAdditionalOptions};
use std::sync::Arc;

/// One wildcard's modifiers, classified. The **only** place a
/// `WildcardAdditionalOptions` is read, so a field a future sqlparser adds
/// cannot reach one reader and miss another — the destructure is exhaustive and
/// every reader goes through this struct.
struct WildcardMods<'a> {
    /// A modifier gnitz does not honor, spelled for the rejection.
    refused: Option<&'static str>,
    /// `EXCEPT` ∪ `EXCLUDE` — synonyms (ClickHouse/BigQuery vs Snowflake).
    drop: Vec<&'a str>,
    /// `RENAME` as (from, to) pairs.
    rename: Vec<(&'a str, &'a str)>,
}

fn wildcard_mods(o: &WildcardAdditionalOptions) -> WildcardMods<'_> {
    let WildcardAdditionalOptions {
        wildcard_token: _, // span only
        opt_ilike,
        opt_exclude,
        opt_except,
        opt_replace,
        opt_rename,
        // Redshift `SELECT * AS x`; `GenericDialect` leaves
        // `supports_select_wildcard_with_alias()` false, so the parser never fills it.
        opt_alias,
    } = o;
    let mut drop: Vec<&str> = Vec::new();
    if let Some(e) = opt_except {
        drop.push(e.first_element.value.as_str());
        drop.extend(e.additional_elements.iter().map(|i| i.value.as_str()));
    }
    // `EXCLUDE` columns parse as `ObjectName`s (sqlparser 0.62). A single plain
    // identifier names a column; a qualified name is refused, since the
    // exclusion drops a name from every relation the wildcard expands.
    let exclude: &[ObjectName] = match opt_exclude {
        Some(ExcludeSelectItem::Single(n)) => std::slice::from_ref(n),
        Some(ExcludeSelectItem::Multiple(v)) => v,
        None => &[],
    };
    let mut qualified_exclude = false;
    for n in exclude {
        match single_part_ident(n) {
            Some(s) => drop.push(s),
            None => qualified_exclude = true,
        }
    }
    let mut rename: Vec<(&str, &str)> = Vec::new();
    match opt_rename {
        Some(RenameSelectItem::Single(a)) => rename.push((a.ident.value.as_str(), a.alias.value.as_str())),
        Some(RenameSelectItem::Multiple(v)) => {
            rename.extend(v.iter().map(|a| (a.ident.value.as_str(), a.alias.value.as_str())))
        }
        None => {}
    }
    WildcardMods {
        // gnitz honors neither a computed value substitution nor a name-pattern
        // filter, so expanding a plain `*` in their place would answer a
        // different query than the one written.
        refused: opt_replace
            .is_some()
            .then_some("SELECT * REPLACE")
            .or(opt_ilike.is_some().then_some("SELECT * ILIKE"))
            .or(opt_alias.is_some().then_some("SELECT * AS"))
            .or(qualified_exclude.then_some("a qualified SELECT * EXCLUDE column")),
        drop,
        rename,
    }
}

/// Expand one wildcard over `cols` into `(source index, output def)` pairs, in
/// source order, dropping and renaming per its modifiers. Hidden columns are
/// skipped: a synthetic view key (`_join_pk`, `_set_pk`, …) must never re-enter
/// a payload or a row identity through a wildcard. Matching is case-insensitive,
/// like [`crate::rules::find_unique_column`]. `ctx` names the surface for the messages.
pub(super) fn expand_wildcard_item<'a, I>(
    o: &WildcardAdditionalOptions,
    cols: I,
    ctx: &str,
) -> Result<Vec<(usize, ColumnDef)>, GnitzSqlError>
where
    I: IntoIterator<Item = &'a ColumnDef> + Clone,
{
    let WildcardMods { refused, drop, rename } = wildcard_mods(o);
    if let Some(what) = refused {
        return Err(unsupported_clause(ctx, what));
    }
    let excludes = |name: &str| drop.iter().any(|d| d.eq_ignore_ascii_case(name));
    let output_name = |name: &str| {
        rename
            .iter()
            .find(|(f, _)| f.eq_ignore_ascii_case(name))
            .map(|&(_, t)| t)
    };
    // Each rejection below covers a rewrite that would otherwise do nothing and
    // say nothing.
    for &n in drop.iter().chain(rename.iter().map(|(f, _)| f)) {
        if !has_visible_column(cols.clone(), n) {
            return Err(GnitzSqlError::Rejected(format!(
                "{ctx}: SELECT * EXCEPT/EXCLUDE/RENAME names unknown column '{n}'"
            )));
        }
    }
    if let Some((f, _)) = rename.iter().find(|(f, _)| excludes(f)) {
        return Err(GnitzSqlError::Rejected(format!(
            "{ctx}: SELECT * RENAME names excluded column '{f}'"
        )));
    }
    if let Some(f) = gnitz_wire::first_duplicate(rename.iter().map(|&(f, _)| f)) {
        return Err(GnitzSqlError::Rejected(format!(
            "{ctx}: SELECT * RENAME names column '{f}' twice"
        )));
    }
    Ok(cols
        .into_iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden && !excludes(&c.name))
        .map(|(i, c)| {
            let mut out = c.clone();
            if let Some(new) = output_name(&out.name) {
                out.name = Arc::from(new);
            }
            (i, out)
        })
        .collect())
}

/// True when the projection is one wildcard naming no output column of its own,
/// so the output names are the source's: a duplicate among them belongs to the
/// source (a join surfacing both sides' `val`) and is carried through rather than
/// rejected. `RENAME` names its output, so it gets the duplicate check.
pub(super) fn is_name_preserving_wildcard_projection(projection: &[SelectItem]) -> bool {
    matches!(projection, [SelectItem::Wildcard(o)] if wildcard_mods(o).rename.is_empty())
}

#[cfg(test)]
#[path = "tests/wildcard.rs"]
mod tests;
