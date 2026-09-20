//! The admission rules for a join step and for a decorrelated EXISTS/IN, shared
//! by `bind`, `place`, `decorrelate` and `lower::join`.

use super::{HirExpr, JoinShape, JoinType, SubqueryKind, SubqueryRef};
use crate::error::GnitzSqlError;
use crate::validate::reject_float_keys;
use gnitz_core::{ColumnDef, TypeCode};

/// `residual`, what a join's ON left unkeyed, applies only as a filter over an
/// INNER product: `JoinClass` has nowhere to hold one, so `place()` drops it into
/// `Placed.above`, where nothing tells it from a WHERE.
pub(crate) fn reject_outer_with_residual(kind: JoinType, residual: &[HirExpr]) -> Result<(), GnitzSqlError> {
    if residual.is_empty() {
        return Ok(());
    }
    match kind {
        JoinType::Inner => Ok(()),
        JoinType::Left | JoinType::Right | JoinType::Full => Err(GnitzSqlError::Unsupported(
            "LEFT/RIGHT/FULL JOIN with a residual ON predicate (a non-equi/non-range \
             conjunct, or a second range conjunct) is not supported; the residual \
             would have to participate in the outer null-fill. Use INNER JOIN, or \
             move the predicate to a WHERE over a wrapping view."
                .into(),
        )),
        JoinType::Semi | JoinType::Anti | JoinType::Mark(_) => Err(GnitzSqlError::Unsupported(
            "EXISTS/IN correlation contains a conjunct the semi-join cannot consume \
             (a non-equality/non-range comparison, a second range conjunct, or an OR \
             group spanning both relations); it would have to participate in the \
             match-existence decision. Filter inside the subquery or a wrapping view."
                .into(),
        )),
    }
}

/// Which (shape, kind) combinations a join step exists in. Both refusals are
/// ordinary SQL with a buildable lowering; neither emitter was written.
pub(crate) fn reject_join_shape(kind: JoinType, shape: JoinShape) -> Result<(), GnitzSqlError> {
    match (shape, kind) {
        (JoinShape::Cross, k) if k != JoinType::Inner => Err(GnitzSqlError::Unsupported(
            "a LEFT/RIGHT/FULL JOIN or an EXISTS/IN correlation needs at least one equijoin \
             or range predicate between its two sides; only an INNER step (CROSS JOIN, a \
             comma-separated FROM, or JOIN … ON with no cross-table comparison) may be keyless."
                .into(),
        )),
        (JoinShape::PureRange, k) if k.preserves_right() => Err(GnitzSqlError::Unsupported(
            "pure-range RIGHT/FULL JOIN (a sole inequality range conjunct with no \
             equality prefix) is not supported; its mirror null-fill has no inner-join \
             witness on the preserved side. Use INNER/LEFT JOIN, or add an equality \
             conjunct to make it a band join."
                .into(),
        )),
        _ => Ok(()),
    }
}

/// Validate one equijoin key pair and return the pair's common reindex output
/// type `T`. Float keys are rejected outright (IEEE-754 -0.0/+0.0 compare
/// byte-unequal and NaN has no canonical form). A German string (STRING/BLOB) may
/// only join another German string: it reindexes to a 16-byte content hash, which
/// is byte-incompatible with the native U128/UUID encoding even though both
/// collapse to the U128 output type. Everything else goes to
/// [`TypeCode::join_key_common_type`].
pub(crate) fn validate_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<TypeCode, GnitzSqlError> {
    reject_float_keys([left, right], "JOIN ON")?;
    if !left.ty().decimal_domains_match(right.ty()) {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN ON: join key columns '{}' ({}) and '{}' ({}) differ; a DECIMAL joins only a \
             DECIMAL of the same scale",
            left.name,
            left.ty(),
            right.name,
            right.ty()
        )));
    }
    // STRING/BLOB reindex to a 16-byte XXH3 content hash; U128/UUID reindex to the
    // 16-byte native value. Both collapse to the U128 output type, so
    // `join_key_common_type` cannot tell them apart — but a content hash never
    // equals a native integer, so the join would silently match nothing.
    if left.type_code.is_german_string() != right.type_code.is_german_string() {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN ON: cannot equijoin string/blob column '{}' ({:?}) with non-string \
             column '{}' ({:?}); a string content hash never matches a native key",
            left.name, left.type_code, right.name, right.type_code
        )));
    }
    left.type_code.join_key_common_type(right.type_code).ok_or_else(|| {
        GnitzSqlError::Unsupported(format!(
            "JOIN ON: join key columns '{}' ({:?}) and '{}' ({:?}) cannot co-partition; \
             a cross-sign pair whose unsigned side is 128-bit (e.g. DECIMAL(38,0)/UUID \
             joined with a signed integer) needs a signed-256 type that does not exist",
            left.name, left.type_code, right.name, right.type_code
        ))
    })
}

/// Validate the range conjunct's key pair and return its common reindex output
/// type `T`. A range bound must be order-preserving: STRING/BLOB reindex to a
/// 16-byte content hash that is equality-correct but NOT order-preserving, so
/// they are rejected here (they remain legal in the equality prefix).
pub(crate) fn validate_range_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<TypeCode, GnitzSqlError> {
    for col in [left, right] {
        if col.type_code.is_german_string() {
            return Err(GnitzSqlError::Unsupported(format!(
                "range join key column '{}' ({:?}): a string/blob content hash is not \
                 order-preserving and cannot bound a range conjunct",
                col.name, col.type_code
            )));
        }
    }
    validate_join_key_pair(left, right)
}

/// An IN over a nullable operand is three-valued; only a top-level conjunct — a
/// semi-join, where WHERE reads NULL as false — computes it exactly.
pub(crate) fn reject_nullable_in(kind: JoinType, s: &SubqueryRef) -> Result<(), GnitzSqlError> {
    if !matches!(s.kind, SubqueryKind::Exists { nullable: true }) {
        return Ok(());
    }
    match kind {
        JoinType::Anti => Err(GnitzSqlError::Unsupported(
            "NOT IN (SELECT …) requires the outer operand and the subquery column to be \
             NOT NULL (SQL's NULL semantics diverge from the anti-join otherwise); \
             use NOT EXISTS with an explicit equality instead"
                .into(),
        )),
        JoinType::Mark(_) => Err(GnitzSqlError::Unsupported(
            "IN (SELECT …) in a mark position (under OR/NOT, in CASE, or projected) requires \
             the outer operand and the subquery column to be NOT NULL — SQL's 3VL diverges \
             from the two-valued mark; use a top-level AND `x IN (SELECT …)` conjunct, or \
             NOT EXISTS with an explicit equality"
                .into(),
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "tests/guards.rs"]
mod tests;
