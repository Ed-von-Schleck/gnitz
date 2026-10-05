//! Schema and naming rules that read no SQL text: what a key, a name or a
//! relation's class must satisfy.

use crate::error::GnitzSqlError;
use gnitz_core::RelDescriptor;
use gnitz_wire::{ColumnDef, RelClass, TypeCode};

/// The first name `names` repeats, folded case-insensitively as SQL identifiers
/// are, and returned as the user spelled it at the repeat.
pub(crate) fn first_duplicate<'a>(mut names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    names.find(|n| !seen.insert(n.to_ascii_lowercase()))
}

/// Reject a list naming one column twice. `context` names the list in the error.
pub(crate) fn reject_duplicate_names<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    match first_duplicate(names) {
        Some(name) => Err(GnitzSqlError::Rejected(format!(
            "duplicate column name '{name}' in {context}"
        ))),
        None => Ok(()),
    }
}

/// Reject a name the user may not give a relation, an index or a constraint.
pub(crate) fn validate_user_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_wire::validate_user_identifier(name).map_err(GnitzSqlError::Rejected)
}

/// [`validate_user_name`], returning the name's canonical form.
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

/// [`reject_float_key`] over every column of one hashed row identity.
pub(crate) fn reject_float_keys<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    role: &str,
) -> Result<(), GnitzSqlError> {
    cols.into_iter()
        .try_for_each(|c| reject_float_key(c.ty.tc, (!c.is_hidden).then_some(c.name.as_str()), role))
}

/// What a surface requires of the relation a name resolved to. The wording rides
/// the variant rather than a parameter beside it, which a caller could pair wrongly.
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

/// How a rejection names the class a relation has: the wire noun, with a view's
/// options spelled out where a requirement can turn on them.
fn class_noun(class: RelClass) -> &'static str {
    match class {
        RelClass::BoundedView => "capacity-bounded view",
        RelClass::FedView => "view with a delta feed",
        RelClass::Table | RelClass::Stream | RelClass::View => class.noun(),
    }
}

/// Reject a relation of the wrong class for `op`.
pub(crate) fn require_class(rel: &RelDescriptor, name: &str, want: ClassWant, op: &str) -> Result<(), GnitzSqlError> {
    if want.accepts(rel.class) {
        return Ok(());
    }
    Err(GnitzSqlError::Rejected(format!(
        "'{name}' is a {}; {op} requires {}",
        class_noun(rel.class),
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
