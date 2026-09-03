//! The pass-neutral guards: what the HIR *rejects*, independent of which pass
//! notices. Bind classifies a FROM-clause join step here, the classification
//! rewrite validates each key pair and the key arity here, and lowering checks the
//! range-join output caps here — so no pass has to reach into another for a rule,
//! and `hir::lower` depends only downward.

use super::{JoinShape, JoinType};
use crate::error::GnitzSqlError;
use crate::validate::reject_float_keys;
use gnitz_core::{ColumnDef, RangeRel, TypeCode};
use sqlparser::ast::{Expr, JoinConstraint, JoinOperator};

/// What supplies one join step's key columns, and the step's type — orthogonal in
/// the grammar, so `NATURAL LEFT JOIN` is `(JoinKeys::Natural, JoinType::Left)`.
///
/// A `CROSS JOIN` is an INNER step stating no keys, which is what a comma between
/// FROM items is too; `reject_keyless_non_inner` is the one place such a step is
/// refused for any other join type.
pub(crate) fn join_keys_and_type(join: &sqlparser::ast::Join) -> Result<(JoinKeys<'_>, JoinType), GnitzSqlError> {
    let sqlparser::ast::Join {
        relation: _, // resolved by the caller as the step's right input
        // Inert: ClickHouse's `GLOBAL` asks for evaluation against the whole
        // right relation, which a DBSP bilinear join already does.
        global: _,
        join_operator,
    } = join;
    let (constraint, kind) = match join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => (c, JoinType::Inner),
        JoinOperator::LeftOuter(c) | JoinOperator::Left(c) => (c, JoinType::Left),
        JoinOperator::RightOuter(c) | JoinOperator::Right(c) => (c, JoinType::Right),
        JoinOperator::FullOuter(c) => (c, JoinType::Full),
        JoinOperator::CrossJoin(c) => (c, JoinType::Inner),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "JOIN: only INNER / LEFT / RIGHT / FULL JOIN, with ON / USING / \
                 NATURAL, are supported"
                    .into(),
            ))
        }
    };
    let keys = match constraint {
        JoinConstraint::On(e) => JoinKeys::On(e),
        JoinConstraint::Using(cols) => {
            reject_full_join_column_merge(kind, "USING")?;
            JoinKeys::Using(cols)
        }
        JoinConstraint::Natural => {
            reject_full_join_column_merge(kind, "NATURAL")?;
            JoinKeys::Natural
        }
        // No constraint at all: `CROSS JOIN b`, `INNER JOIN b` with nothing after
        // it, and the comma between FROM items are one shape, and bind as one.
        JoinConstraint::None => JoinKeys::None,
    };
    Ok((keys, kind))
}

/// Where one join step's key columns come from.
pub(crate) enum JoinKeys<'a> {
    /// `ON <expr>` — the conjuncts are classified as written.
    On(&'a Expr),
    /// `USING (c, …)` — each named column is equated across the two sides and the
    /// two copies merge into one output column.
    Using(&'a [sqlparser::ast::ObjectName]),
    /// `NATURAL` — `USING` over every column name the two sides share.
    Natural,
    /// No constraint: a `CROSS JOIN`, a comma-separated FROM item, or `JOIN b`
    /// with nothing after it. Keyless on its own; the WHERE is what may key it.
    None,
}

/// The merged column is the preserved side's copy, which INNER / LEFT / RIGHT can
/// pass through. FULL preserves both, so its merged column would be
/// `COALESCE(l.c, r.c)` — a value neither side carries, and the join emitter
/// projects columns of its two inputs, never a computed one.
fn reject_full_join_column_merge(kind: JoinType, clause: &str) -> Result<(), GnitzSqlError> {
    if kind != JoinType::Full {
        return Ok(());
    }
    Err(GnitzSqlError::Unsupported(format!(
        "FULL JOIN … {clause} is not supported: the merged column would be \
         COALESCE(left, right), which a join projection cannot compute. Write the \
         equality as `ON …` and project the two columns you want."
    )))
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

/// The pair-PK output arity cap — the binding constraint on a synthesized output
/// PK (its stride cannot reach `MAX_PK_BYTES` first). `validate_pk_cols` is the
/// engine-side backstop; this is the planner error naming the surface written.
pub(crate) fn reject_pair_pk_overflow(surface: &str, pa: usize, pb: usize) -> Result<(), GnitzSqlError> {
    let pair_pk = pa + pb;
    if pair_pk > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(format!(
            "{surface} output PK has {pair_pk} columns (a.pk {pa} + b.pk {pb}), \
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
    reject_float_keys([left, right], "JOIN ON")?;
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

/// Only an INNER step may be keyless: it is the cross join, whose residual (if
/// any) filters the product. An outer or decorrelated step decides its null-fill
/// or match existence from a key, and a keyless one would need a global "is the
/// other side empty" witness no emitter builds.
pub(crate) fn reject_keyless_non_inner(kind: JoinType, shape: JoinShape) -> Result<(), GnitzSqlError> {
    if shape == JoinShape::Cross && kind != JoinType::Inner {
        return Err(GnitzSqlError::Unsupported(
            "a LEFT/RIGHT/FULL JOIN or an EXISTS/IN correlation needs at least one equijoin \
             or range predicate between its two sides; only an INNER step (CROSS JOIN, a \
             comma-separated FROM, or JOIN … ON with no cross-table comparison) may be keyless."
                .into(),
        ));
    }
    Ok(())
}

/// The JOIN ON reindex-slot arity cap: each equality pair plus the optional range
/// slot becomes one `_join_pk` PK-list slot, and the codec holds at most
/// `PK_LIST_MAX_COLS`. A planner error here rather than a `pack_pk_cols` panic at
/// registration. The output pair-PK has its own cap above.
pub(crate) fn reject_join_key_arity(n_eq: usize, has_range: bool) -> Result<(), GnitzSqlError> {
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
