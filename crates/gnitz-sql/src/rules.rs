//! Schema and naming rules that read no SQL text: what a computed column, a key,
//! a name or a relation's class must satisfy. `gnitz-wire` decides each rule the
//! engine shares; these name the offending column or relation, which wire cannot.

use crate::error::GnitzSqlError;
use crate::ir::check_decimal_scale;
use gnitz_core::RelDescriptor;
use gnitz_wire::{ColType, ColumnDef, RelClass, TypeCode};

/// The column def of a *computed* projection item, from the expression's
/// nominal type. One home for the three rules every computed column obeys, so
/// the ad-hoc and CREATE VIEW binders (which each build their own projection
/// schema) cannot drift:
/// - `_expr{idx}` when the item has no alias;
/// - always nullable — an expression over a NOT NULL column can still be NULL
///   (division by zero, an unmatched CASE);
/// - typed by the register image the engine's register sink stores whole, not
///   the nominal type: a narrowing integer cast types as its target and a window
///   placeholder as the narrow value it stands in for, yet each rides a full
///   8-byte register. STRING maps to itself, which `register_image` already
///   accounts for;
/// - a DECIMAL scale a register can hold.
pub(crate) fn computed_column(alias: Option<String>, idx: usize, nominal: ColType) -> Result<ColumnDef, GnitzSqlError> {
    check_decimal_scale(nominal.scale)?;
    Ok(ColumnDef::typed(
        alias.unwrap_or_else(|| computed_column_name(idx)),
        nominal.register_image(),
        true,
    ))
}

/// The name of the unaliased computed item at SELECT position `idx`.
pub(crate) fn computed_column_name(idx: usize) -> String {
    format!("_expr{idx}")
}

/// A hidden ordering column's label, for dumps and EXPLAIN. Placement travels
/// as a column index, not by this name.
pub(crate) fn order_column_name(i: usize) -> String {
    format!("_order{i}")
}

/// Reject an output column list that names the same *visible* column twice.
/// Hidden key slots are skipped — they are excluded from name resolution, so
/// they cannot bind ambiguously. `context` names the surface in the error message.
pub(crate) fn reject_duplicate_column_names<'a>(
    cols: impl Iterator<Item = &'a ColumnDef>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_duplicate_names(cols.filter(|c| !c.is_hidden).map(|c| c.name.as_str()), context)
}

/// The first name `names` repeats, folded case-insensitively as SQL identifiers
/// are, and returned as the user spelled it at the repeat.
pub(crate) fn first_duplicate<'a>(names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    names.into_iter().find(|n| !seen.insert(n.to_ascii_lowercase()))
}

/// Raw-name form of [`reject_duplicate_column_names`], for surfaces that have
/// only parser-AST names (CREATE TABLE — a freshly created column is never
/// hidden; a base table gains a hidden slot only later, via DROP COLUMN).
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

/// Reject a statement naming the same catalog object twice — `DROP TABLE a, a`.
/// The two retractions would land on one catalog PK, which the engine refuses in
/// terms of the catalog rather than of the SQL the user wrote.
pub(crate) fn reject_repeated_object<'a>(
    names: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    match first_duplicate(names) {
        Some(name) => Err(GnitzSqlError::Rejected(format!(
            "{context}: '{name}' is named more than once"
        ))),
        None => Ok(()),
    }
}

/// Validate a user-supplied table/view/schema/index/constraint name: reject the
/// empty string, a leading `_` (reserved for the engine's own internal relation
/// and index names), and any character outside `[A-Za-z0-9_]`.
///
/// The leading-`_` reservation is *policy* the engine cannot enforce for a
/// relation or index name — it must accept exactly the rows it synthesizes
/// itself. It does enforce it for a schema name, which nothing synthesizes.
pub(crate) fn validate_user_name(name: &str) -> Result<(), GnitzSqlError> {
    gnitz_wire::validate_user_identifier(name).map_err(GnitzSqlError::Rejected)
}

/// [`validate_user_name`] returning the canonical stored form. The one fold a
/// user-supplied name gets in this crate; the catalog gateway applies the same
/// one, so the two cannot spell canonicalization differently.
pub(crate) fn canonical_user_name(name: &str) -> Result<String, GnitzSqlError> {
    gnitz_wire::canonical_identifier(name).map_err(GnitzSqlError::Rejected)
}

/// [`reject_float_key`] for a key with no column name to report — a computed one.
pub(crate) fn reject_float_key_of(what: &str, role: &str) -> GnitzSqlError {
    GnitzSqlError::Rejected(format!(
        "{role}: {what} cannot be a key (IEEE-754 -0.0/+0.0 and NaN break key equality)"
    ))
}

/// Reject a float column used as any hashed key — a GROUP BY grouping key, a
/// DISTINCT/set-op row identity, or an equijoin key. All of these hash the
/// column's raw IEEE-754 bytes, so -0.0/+0.0 and distinct-NaN bit patterns split
/// values that are numerically equal (and route them to distinct workers). `role`
/// names the offending clause for the error message.
///
/// A hidden slot carries no name the user wrote — it is a synthetic key a
/// pre-map minted — so it is described rather than named.
pub(crate) fn reject_float_key(col: &ColumnDef, role: &str) -> Result<(), GnitzSqlError> {
    if col.ty.tc.is_float() {
        let what = if col.is_hidden {
            "a float-valued expression".to_string()
        } else {
            format!("float column '{}'", col.name)
        };
        return Err(reject_float_key_of(&what, role));
    }
    Ok(())
}

/// [`reject_float_key`] over every column of one hashed row identity — a
/// DISTINCT/set-op row, a join-key pair — so a caller assembling the key from
/// several columns has one call, not a loop.
pub(crate) fn reject_float_keys<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    role: &str,
) -> Result<(), GnitzSqlError> {
    cols.into_iter().try_for_each(|c| reject_float_key(c, role))
}

/// Reject an index key the engine could not build — the whole gate both
/// index-creating surfaces (CREATE INDEX and inline UNIQUE) run. `IndexKeyRule`
/// carries the failing position so this layer can name the column wire cannot.
pub(crate) fn reject_unbuildable_index_key(
    names: &[&str],
    types: &[TypeCode],
    src_pk_count: usize,
    role: &str,
) -> Result<(), GnitzSqlError> {
    gnitz_wire::index_key_types(types, src_pk_count).map_err(|rule| match rule {
        gnitz_wire::IndexKeyRule::NotEligible { col, .. } => non_key_eligible_error(names[col], types[col], role),
        arity @ gnitz_wire::IndexKeyRule::ArityOutOfRange { .. } => GnitzSqlError::Rejected(arity.to_string()),
    })?;
    Ok(())
}

/// The named-column rendering of "this type cannot be a key column" — the
/// planner's half of `gnitz_wire::PkRule::NotEligible`, split out so the
/// CREATE TABLE path can raise the identical message from the shared rule's
/// verdict instead of re-testing eligibility itself.
pub(crate) fn non_key_eligible_error(name: &str, tc: TypeCode, role: &str) -> GnitzSqlError {
    GnitzSqlError::Rejected(format!("{role} column '{name}' of type {tc} cannot be a {role} key"))
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
    /// What an ad-hoc read may name: anything holding rows, so not a stream.
    Readable,
}

impl ClassWant {
    /// Each arm lists the wants that admit its class, so a new `RelClass` is a
    /// compile error until its row is decided, and a new `ClassWant` admits
    /// nothing until it is listed.
    fn accepts(self, class: RelClass) -> bool {
        match class {
            RelClass::Table => matches!(
                self,
                ClassWant::BaseTable | ClassWant::BaseTableOrStream | ClassWant::Readable
            ),
            RelClass::Stream => matches!(self, ClassWant::BaseTableOrStream),
            RelClass::View | RelClass::BoundedView | RelClass::FedView => {
                matches!(self, ClassWant::View | ClassWant::Readable)
            }
        }
    }

    /// How the requirement names itself in a rejection.
    fn noun(self) -> &'static str {
        match self {
            ClassWant::BaseTable => "a base table",
            ClassWant::BaseTableOrStream => "a base table or a stream",
            ClassWant::View => "a view",
            ClassWant::Readable => "a table or a view (a stream is read only inside a view body)",
        }
    }
}

/// Reject a relation of the wrong class for `op`. Takes the descriptor rather
/// than resolving one: the resolve is surface-specific, the verdict on what came
/// back is not.
pub(crate) fn require_class(rel: &RelDescriptor, name: &str, want: ClassWant, op: &str) -> Result<(), GnitzSqlError> {
    if want.accepts(rel.class) {
        return Ok(());
    }
    Err(GnitzSqlError::Rejected(format!(
        "'{name}' is a {}; {op} requires {}",
        rel.class.noun(),
        want.noun()
    )))
}

/// Reject a counted column list wider than `cap`, before the wire encoder's
/// assertion. `what` names the list; the two caps below are the only ones.
fn reject_arity(what: &str, cols: usize, cap: usize) -> Result<(), GnitzSqlError> {
    if cols > cap {
        return Err(GnitzSqlError::Rejected(format!(
            "{what} has {cols} columns, exceeding the {cap}-column limit"
        )));
    }
    Ok(())
}

/// A relation's column list, against the engine's column limit.
pub(crate) fn reject_column_overflow(what: &str, cols: usize) -> Result<(), GnitzSqlError> {
    reject_arity(what, cols, gnitz_wire::MAX_COLUMNS)
}

/// A synthesized key list — a join's reindex slots, or a join output's pair PK —
/// against the width a registered PK may have.
pub(crate) fn reject_pk_list_arity(what: &str, cols: usize) -> Result<(), GnitzSqlError> {
    reject_arity(what, cols, gnitz_wire::PK_LIST_MAX_COLS)
}

#[cfg(test)]
#[path = "tests/rules.rs"]
mod tests;
