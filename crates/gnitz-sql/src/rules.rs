//! Schema and naming rules that read no SQL text: what a key, a name or a
//! relation's class must satisfy, and which visible column a name names.

use crate::error::GnitzSqlError;
use gnitz_core::RelDescriptor;
use gnitz_wire::{ColumnDef, RelClass, TypeCode};

/// Reject a list naming one column twice. `context` names the list in the error.
pub(crate) fn reject_duplicate_names<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    match gnitz_wire::first_duplicate(names) {
        Some(name) => Err(GnitzSqlError::Rejected(format!(
            "duplicate column name '{name}' in {context}"
        ))),
        None => Ok(()),
    }
}

/// Reject a name the user may not give a relation, an index or a constraint;
/// otherwise return its canonical form.
pub(crate) fn canonical_user_name(name: &str) -> Result<String, GnitzSqlError> {
    gnitz_wire::canonical_identifier(name).map_err(GnitzSqlError::Rejected)
}

/// Reject a float as a hashed key of clause `role`. `name` is the column the user
/// wrote that holds the key, if one does.
pub(crate) fn reject_float_key(tc: TypeCode, name: Option<&str>, role: &str) -> Result<(), GnitzSqlError> {
    if !tc.is_float() {
        return Ok(());
    }
    let what = match name {
        Some(name) => format!("float column '{name}'"),
        None => "a float-valued expression".to_string(),
    };
    Err(GnitzSqlError::Rejected(format!(
        "{role}: {what} cannot be a key (IEEE-754 -0.0/+0.0 and NaN break key equality)"
    )))
}

/// [`reject_float_key`] over every column of one element's identity.
pub(crate) fn reject_float_keys<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    role: &str,
) -> Result<(), GnitzSqlError> {
    cols.into_iter()
        .try_for_each(|c| reject_float_key(c.ty.tc, (!c.is_hidden).then_some(c.name.as_str()), role))
}

/// What a surface requires of the relation a name resolved to, and how a
/// refusal words it.
#[derive(Clone, Copy)]
pub(crate) enum ClassWant {
    /// A stored, writable base table.
    BaseTable,
    /// A base table or a stream — the INSERT target rule, the one surface a
    /// storeless relation is a legal write target for.
    BaseTableOrStream,
    /// Any view, bounded or fed included.
    View,
    /// What `ALTER VIEW` may retarget: a view with no options, since the statement
    /// states none and would drop them.
    PlainView,
    /// What an ad-hoc read may name: anything holding rows, so not a stream.
    Readable,
    /// What a view body may read: anything but a capacity-bounded view, whose
    /// skeleton rows hydrate from its own sources, which a view over it cannot reach.
    ViewSource,
    /// What may carry a secondary index.
    Indexable,
}

impl ClassWant {
    /// Each arm lists the wants that admit its class, so a new `RelClass` is a
    /// compile error until its row is decided, and a new `ClassWant` admits
    /// nothing until it is listed.
    fn accepts(self, class: RelClass) -> bool {
        match class {
            RelClass::Table => matches!(
                self,
                ClassWant::BaseTable
                    | ClassWant::BaseTableOrStream
                    | ClassWant::Readable
                    | ClassWant::ViewSource
                    | ClassWant::Indexable
            ),
            RelClass::Stream => matches!(self, ClassWant::BaseTableOrStream | ClassWant::ViewSource),
            RelClass::View => matches!(
                self,
                ClassWant::View
                    | ClassWant::PlainView
                    | ClassWant::Readable
                    | ClassWant::ViewSource
                    | ClassWant::Indexable
            ),
            RelClass::FedView => matches!(
                self,
                ClassWant::View | ClassWant::Readable | ClassWant::ViewSource | ClassWant::Indexable
            ),
            RelClass::BoundedView => matches!(self, ClassWant::View | ClassWant::Readable),
            RelClass::Catalog => matches!(self, ClassWant::Readable | ClassWant::ViewSource),
        }
    }

    /// How the requirement names itself in a rejection.
    fn noun(self) -> &'static str {
        match self {
            ClassWant::BaseTable => "a base table",
            ClassWant::BaseTableOrStream => "a base table or a stream",
            ClassWant::View => "a view",
            ClassWant::PlainView => "a view created without WITH options (DROP and CREATE the view instead)",
            ClassWant::Readable => "a table or a view (a stream is read only inside a view body)",
            ClassWant::ViewSource => "a relation a view can be created over (a capacity-bounded view is a leaf)",
            ClassWant::Indexable => "a base table or a view without a capacity",
        }
    }
}

/// Reject a relation of the wrong class for `op`.
pub(crate) fn require_class(
    rel: &RelDescriptor,
    name: impl std::fmt::Display,
    want: ClassWant,
    op: &str,
) -> Result<(), GnitzSqlError> {
    if want.accepts(rel.class) {
        return Ok(());
    }
    Err(GnitzSqlError::Rejected(format!(
        "'{name}' is a {}; {op} requires {}",
        rel.class.describe(),
        want.noun()
    )))
}

/// Reject a counted column list wider than `cap`. `what` names the list.
pub(crate) fn reject_arity(what: &str, cols: usize, cap: usize) -> Result<(), GnitzSqlError> {
    if cols > cap {
        return Err(GnitzSqlError::Rejected(format!(
            "{what} has {cols} columns, exceeding the {cap}-column limit"
        )));
    }
    Ok(())
}

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

/// The column of the one relation in scope, under `alias`, that a reference
/// names: a written qualifier must name `alias`, and it disambiguates nothing
/// more.
pub(crate) fn single_relation_col_idx<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    alias: &str,
    qual: Option<&str>,
    name: &str,
) -> Result<usize, GnitzSqlError> {
    if let Some(qual) = qual.filter(|q| !q.eq_ignore_ascii_case(alias)) {
        return Err(GnitzSqlError::Rejected(format!(
            "table alias '{qual}' not found (the relation in scope is '{alias}')"
        )));
    }
    require_column(cols, name)
}

/// Whether `cols` holds a *visible* (non-hidden) column named `name`, matched
/// case-insensitively. Unlike [`find_unique_column`], two matches are not an
/// error: `SELECT * EXCEPT (id)` over a two-`id` join deliberately drops both.
pub(crate) fn has_visible_column<'a>(cols: impl IntoIterator<Item = &'a ColumnDef>, name: &str) -> bool {
    cols.into_iter()
        .any(|c| !c.is_hidden && c.name.eq_ignore_ascii_case(name))
}

#[cfg(test)]
#[path = "tests/rules.rs"]
mod tests;
