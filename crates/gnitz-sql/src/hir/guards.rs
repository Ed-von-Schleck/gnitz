//! The pass-neutral guards: what the HIR *rejects*, independent of which pass
//! notices. Bind classifies a FROM-clause `JOIN … ON` here, the classification
//! rewrite validates each key pair and the ON arity here, and lowering checks the
//! range-join output caps here — so no pass has to reach into another for a rule,
//! and `hir::lower` depends only downward.

use super::JoinType;
use crate::error::GnitzSqlError;
use crate::validate::reject_float_key;
use gnitz_core::{ColumnDef, RangeRel, TypeCode};
use sqlparser::ast::{Expr, JoinConstraint, JoinOperator};

/// The ON expression and type of one join step. Each step supports
/// INNER / LEFT / RIGHT / FULL, left-deep in syntactic order (no reordering),
/// so `a LEFT JOIN b JOIN c` is `(a LEFT JOIN b) JOIN c` — each step's emit is the
/// standard 2-way emit, outer null-fill included.
///
/// sqlparser 0.56 spells the bare/`OUTER` forms as separate variants
/// (`Left`/`LeftOuter`, `Right`/`RightOuter`); FULL has only `FullOuter`.
pub(crate) fn join_on_and_type(join: &sqlparser::ast::Join) -> Result<(&Expr, JoinType), GnitzSqlError> {
    match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::Join(JoinConstraint::On(e)) => {
            Ok((e, JoinType::Inner))
        }
        JoinOperator::LeftOuter(JoinConstraint::On(e)) | JoinOperator::Left(JoinConstraint::On(e)) => {
            Ok((e, JoinType::Left))
        }
        JoinOperator::RightOuter(JoinConstraint::On(e)) | JoinOperator::Right(JoinConstraint::On(e)) => {
            Ok((e, JoinType::Right))
        }
        JoinOperator::FullOuter(JoinConstraint::On(e)) => Ok((e, JoinType::Full)),
        _ => Err(GnitzSqlError::Unsupported(
            "CREATE VIEW JOIN: only INNER / LEFT / RIGHT / FULL JOIN ... ON supported".into(),
        )),
    }
}

/// Outer + residual is unsupported: an outer preserved-side row's null-fill
/// decides match existence from the inner output (or the MAX/MIN threshold
/// witness), independently of the residual, and so would not retro-null-fill a row
/// matched only by residual-failing pairs. A consistent boundary across all join
/// shapes beats an inconsistent partial one; INNER residuals are fully supported.
/// One home, so both join emitters and the decorrelated Semi/Anti/Mark joins
/// reject identically. `Inner` alone allows a residual (a post-join filter);
/// `Left/Right/Full` reject it (it would have to participate in the null-fill);
/// `Semi/Anti/Mark` reject it with the EXISTS/IN correlation message (a residual
/// correlation conjunct cannot participate in the match-existence decision).
pub(crate) fn reject_outer_with_residual(kind: JoinType, residual_empty: bool) -> Result<(), GnitzSqlError> {
    if residual_empty {
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
        JoinType::Semi | JoinType::Anti | JoinType::Mark => Err(GnitzSqlError::Unsupported(
            "EXISTS/IN correlation contains a conjunct the semi-join cannot consume \
             (a non-equality/non-range comparison, a second range conjunct, or an OR \
             group spanning both relations); it would have to participate in the \
             match-existence decision. Filter inside the subquery or a wrapping view."
                .into(),
        )),
    }
}

/// The range-join output pair-PK arity cap (`a.pk_count + b.pk_count ≤
/// PK_LIST_MAX_COLS`) — the binding constraint on the synthesized output PK; the
/// stride ceiling is non-binding (≤ 4·16 = 64 ≤ MAX_PK_BYTES). The engine's
/// `validate_pk_cols` is the backstop; this is the friendly planner error. One
/// home, so both range-join emitters reject identically.
pub(crate) fn reject_pair_pk_overflow(pa: usize, pb: usize) -> Result<(), GnitzSqlError> {
    let pair_pk = pa + pb;
    if pair_pk > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(format!(
            "range JOIN output PK has {pair_pk} columns (a.pk {pa} + b.pk {pb}), \
             exceeding the {}-column limit",
            gnitz_core::PK_LIST_MAX_COLS
        )));
    }
    Ok(())
}

/// The pure-range (`n_eq == 0`) outer-join restrictions. RIGHT/FULL is checked
/// **first** so a FULL join reports the mirror-null-fill limitation rather than
/// the narrower LEFT type rule.
///
/// The broadcast range join scatters the inner output by the OTHER side's range
/// key, so gathering it onto the preserved-key worker would need a second
/// sequential exchange the compiler forbids — the very reason LEFT uses a
/// threshold. A RIGHT/FULL pure-range null-fill would need a full second
/// threshold pipeline (a B-side `m_A = MAX/MIN(a.range)`), a standalone effort.
///
/// LEFT: the inline threshold null-fill reduces the range column with MIN/MAX (an
/// 8-byte accumulator), then reindexes the result onto the range slot type.
/// MIN/MAX preserves the source integer type, so any ≤8-byte integer range column
/// yields a result the reindex consumes directly. A 16-byte U128/UUID/I128 range
/// column has no 8-byte accumulator — reject up front rather than failing the
/// compile.
pub(crate) fn reject_pure_range_outer(kind: JoinType, range_tc: TypeCode) -> Result<(), GnitzSqlError> {
    if kind.preserves_right() {
        return Err(GnitzSqlError::Unsupported(
            "pure-range RIGHT/FULL JOIN (a sole inequality range conjunct with no \
             equality prefix) is not supported; its mirror null-fill has no inner-join \
             witness on the preserved side. Use INNER/LEFT JOIN, or add an equality \
             conjunct to make it a band join."
                .into(),
        ));
    }
    if kind.preserves_left() {
        reject_pure_range_threshold_tc(
            range_tc,
            "pure-range LEFT JOIN",
            "use a narrower range column, INNER JOIN, or a band join",
        )?;
    }
    Ok(())
}

/// The threshold null-fill's range-column type rule, shared by every pure-range
/// (`n_eq == 0`) shape that builds one: the threshold `m = MAX/MIN(other.range)` is
/// an inline MIN/MAX reduce, which has only an 8-byte accumulator, so a 16-byte
/// U128/UUID/I128 range column has no reduce that can produce it — reject up front
/// rather than failing the compile. `surface` names the SQL shape and `remedy` the
/// way out; the rule itself has one home, so a change to the accumulator width is
/// one edit rather than three.
pub(crate) fn reject_pure_range_threshold_tc(
    range_tc: TypeCode,
    surface: &str,
    remedy: &str,
) -> Result<(), GnitzSqlError> {
    if !gnitz_wire::is_fixed_int(range_tc as u8) {
        return Err(GnitzSqlError::Unsupported(format!(
            "{surface} needs a ≤8-byte integer range column (got {range_tc:?}); its threshold \
             null-fill reduces the range column with MIN/MAX, which has no 16-byte accumulator \
             — {remedy}"
        )));
    }
    Ok(())
}

/// Validate one equijoin key pair and return the pair's common reindex output
/// type `T`. Float keys are rejected outright (IEEE-754 -0.0/+0.0 compare
/// byte-unequal and NaN has no canonical form). A German string (STRING/BLOB) may
/// only join another German string: it reindexes to a 16-byte content hash, which
/// is byte-incompatible with the native U128/UUID encoding even though both
/// collapse to the U128 output type. The remaining pairs are resolved by
/// `join_key_common_type`, which promotes integers of different widths (and
/// different sign classes, as long as the unsigned side is ≤ 8 bytes (`U64`)) to a
/// common type that faithfully holds both ranges, so the two reindex sides
/// co-partition byte-for-byte and the `_join_pk` catalog stride matches both. Only
/// a cross-sign pair whose unsigned side is 128-bit (`U128`/`UUID`) stays
/// rejected — its faithful common type is a signed-256 type that does not exist.
pub(crate) fn validate_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<TypeCode, GnitzSqlError> {
    for col in [left, right] {
        reject_float_key(col, "JOIN ON")?;
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
/// they are rejected here (they remain legal in the equality prefix). Floats are
/// rejected by `validate_join_key_pair`, which then resolves the common integer
/// type via `join_key_common_type` (cross-sign promotion included).
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

/// The order-reversing converse of a `RangeRel` (`x OP y` ⟺ `y converse(OP) x`).
/// Used both to canonicalize a right-table-first range conjunct to left-first and
/// to derive term AB's rel from the canonical OP (§3 table).
pub(crate) fn converse_rel(r: RangeRel) -> RangeRel {
    match r {
        RangeRel::Lt => RangeRel::Gt,
        RangeRel::Le => RangeRel::Ge,
        RangeRel::Gt => RangeRel::Lt,
        RangeRel::Ge => RangeRel::Le,
    }
}

/// The JOIN ON key-arity rules, shared by both join planners.
///
/// A residual cannot stand alone: a residual-only ON (`ON a.r <> b.s`) would be an
/// incremental cross-join, which the engine cannot build. Residuals are only ever
/// evaluated alongside a physical equi/range anchor (§3).
///
/// Reindex-slot arity cap: each equality pair plus the optional range slot becomes
/// one synthetic `_join_pk` PK-list slot, and the codec holds at most
/// `PK_LIST_MAX_COLS`. Reject a wider ON here as a clean planner error rather than
/// a `pack_pk_cols` panic at registration. (The output pair-PK has its own cap,
/// checked in the range circuit builder.)
pub(crate) fn reject_join_key_arity(n_eq: usize, has_range: bool) -> Result<(), GnitzSqlError> {
    if n_eq == 0 && !has_range {
        return Err(GnitzSqlError::Bind(
            "JOIN ON must have at least one equijoin or range predicate".into(),
        ));
    }
    let slots = n_eq + has_range as usize;
    if slots > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(if !has_range {
            format!(
                "JOIN ON: at most {} equijoin key columns are supported (got {n_eq})",
                gnitz_core::PK_LIST_MAX_COLS,
            )
        } else {
            format!(
                "range JOIN ON: at most {} join key columns (equality prefix + \
                 range) are supported (got {slots})",
                gnitz_core::PK_LIST_MAX_COLS,
            )
        }));
    }
    Ok(())
}

/// Per-column common type for a set-op pair, or `None` to keep the exact-match
/// type-mismatch error. Same types pass through; a cross-width integer pair
/// promotes to the join-key ladder's common type (same-sign → the wider type;
/// cross-sign with the unsigned operand ≤ U32 → the narrowest strictly-wider
/// signed type, e.g. `U32` vs `I32` → `I64`) but only when the result is a
/// concrete ≤8-byte integer. `None`, the `I128` collapse (`U64` vs `I64`), and
/// every 16-byte / non-integer / string pair are rejected: the widening path
/// loads a value into one i64 register and cannot represent a 16-byte extremum.
pub(crate) fn set_op_common_type(l: TypeCode, r: TypeCode) -> Option<TypeCode> {
    if l == r {
        return Some(l);
    }
    let t = l.join_key_common_type(r)?;
    gnitz_wire::is_fixed_int(t as u8).then_some(t)
}

#[cfg(test)]
#[path = "tests/guards.rs"]
mod tests;
