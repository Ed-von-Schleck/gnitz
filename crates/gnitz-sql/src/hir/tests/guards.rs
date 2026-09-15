use super::*;

fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// The equijoin key-pair promotion ladder: same-type pairs keep their reindex
/// output type; cross-width same-sign pairs widen; a cross-sign pair whose
/// unsigned side is ≤ 8 bytes takes the narrowest signed type holding both.
#[test]
fn join_key_pair_promotion_ladder() {
    let ok = |l, r| validate_join_key_pair(&col("a", l), &col("b", r)).unwrap();
    // Same type ⇒ the source reindex output type.
    assert_eq!(ok(TypeCode::U64, TypeCode::U64), TypeCode::U64);
    assert_eq!(ok(TypeCode::U128, TypeCode::UUID), TypeCode::U128);
    // A German string may join another German string (both content-hash).
    validate_join_key_pair(&col("a", TypeCode::String), &col("b", TypeCode::String)).unwrap();
    validate_join_key_pair(&col("a", TypeCode::String), &col("b", TypeCode::Blob)).unwrap();
    // Cross-width, same sign ⇒ the wider type.
    assert_eq!(ok(TypeCode::I32, TypeCode::I64), TypeCode::I64);
    assert_eq!(ok(TypeCode::U8, TypeCode::U64), TypeCode::U64);
    assert_eq!(ok(TypeCode::U32, TypeCode::U128), TypeCode::U128);
    // Cross-sign, unsigned side ≤ U64 ⇒ the narrowest signed type holding both.
    assert_eq!(ok(TypeCode::U32, TypeCode::I64), TypeCode::I64);
    assert_eq!(ok(TypeCode::U8, TypeCode::I16), TypeCode::I16);
    assert_eq!(ok(TypeCode::U16, TypeCode::I32), TypeCode::I32);
    // U32 vs I32 needs the wider I64 — an equal-width signed type cannot hold
    // U32's full range.
    assert_eq!(ok(TypeCode::U32, TypeCode::I32), TypeCode::I64);
    // U64 cross-sign with any signed integer ⇒ the signed-128 common type.
    assert_eq!(ok(TypeCode::U64, TypeCode::I64), TypeCode::I128);
    assert_eq!(ok(TypeCode::U64, TypeCode::I8), TypeCode::I128);
}

#[test]
fn join_key_pair_rejects_incompatible() {
    let bad = |l, r| validate_join_key_pair(&col("a", l), &col("b", r)).is_err();
    // Float keys: IEEE-754 breaks the byte-equal key contract.
    assert!(bad(TypeCode::F64, TypeCode::U64));
    // Cross-sign with a 128-bit unsigned side would need a signed-256 type.
    assert!(bad(TypeCode::U128, TypeCode::I64));
    assert!(bad(TypeCode::UUID, TypeCode::I64));
    // String vs native: both collapse to U128, so only the german-string check
    // catches it — a content hash never equals a native key.
    assert!(bad(TypeCode::String, TypeCode::U128));
    assert!(bad(TypeCode::String, TypeCode::I64));
}

/// A range bound must be order-preserving, so a string/blob content hash is
/// rejected even though it is a legal *equality* key.
#[test]
fn range_key_pair_rejects_strings_but_promotes_integers() {
    assert!(validate_range_join_key_pair(&col("a", TypeCode::String), &col("b", TypeCode::String)).is_err());
    assert_eq!(
        validate_range_join_key_pair(&col("a", TypeCode::U32), &col("b", TypeCode::I64)).unwrap(),
        TypeCode::I64
    );
}

/// The reindex-slot arity is capped by the PK-list width; the keyless case is
/// no concern of this guard's, at any width.
#[test]
fn join_key_arity_bounds() {
    reject_join_key_arity(0, false).unwrap();
    reject_join_key_arity(gnitz_core::PK_LIST_MAX_COLS, false).unwrap();
    assert!(reject_join_key_arity(gnitz_core::PK_LIST_MAX_COLS + 1, false).is_err());
    // The range slot counts toward the same cap.
    assert!(reject_join_key_arity(gnitz_core::PK_LIST_MAX_COLS, true).is_err());
}

/// Only an INNER step may be keyless (the cross join); every other kind needs an
/// equi or range key, and no kind is refused once it has one.
#[test]
fn only_an_inner_step_may_be_keyless() {
    reject_keyless_non_inner(JoinType::Inner, JoinShape::Cross).unwrap();
    for kind in [
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::Semi,
        JoinType::Anti,
        JoinType::Mark(crate::hir::ColId::NONE),
    ] {
        assert!(reject_keyless_non_inner(kind, JoinShape::Cross).is_err(), "{kind:?}");
        reject_keyless_non_inner(kind, JoinShape::Band).unwrap();
        reject_keyless_non_inner(kind, JoinShape::PureRange).unwrap();
        reject_keyless_non_inner(kind, JoinShape::Equi).unwrap();
    }
}

/// INNER alone tolerates a residual; the outer and the decorrelation kinds each
/// reject with their own surface's wording.
#[test]
fn outer_with_residual_rejects_per_surface() {
    for k in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Full,
        JoinType::Semi,
        JoinType::Mark(crate::hir::ColId::NONE),
    ] {
        reject_outer_with_residual(k, &[]).unwrap();
    }
    let residual = [crate::ir::BExpr::LitInt(1)];
    reject_outer_with_residual(JoinType::Inner, &residual).unwrap();
    let msg = |k| match reject_outer_with_residual(k, &residual).unwrap_err() {
        GnitzSqlError::Unsupported(s) => s,
        e => panic!("expected Unsupported, got {e:?}"),
    };
    assert!(msg(JoinType::Left).contains("LEFT/RIGHT/FULL JOIN"));
    assert!(msg(JoinType::Full).contains("LEFT/RIGHT/FULL JOIN"));
    assert!(msg(JoinType::Semi).contains("EXISTS/IN correlation"));
    assert!(msg(JoinType::Anti).contains("EXISTS/IN correlation"));
    assert!(msg(JoinType::Mark(crate::hir::ColId::NONE)).contains("EXISTS/IN correlation"));
}

/// Set-op columns promote on the same integer ladder, but only to a concrete
/// ≤ 8-byte integer — the widening path loads through one i64 register.
#[test]
fn set_op_common_type_caps_at_eight_bytes() {
    let common = |l: TypeCode, r: TypeCode| set_op_common_type(l.into(), r.into()).map(|t| t.tc);
    assert_eq!(common(TypeCode::U64, TypeCode::U64), Some(TypeCode::U64));
    assert_eq!(common(TypeCode::U32, TypeCode::I32), Some(TypeCode::I64));
    assert_eq!(common(TypeCode::I32, TypeCode::I64), Some(TypeCode::I64));
    // The I128 collapse and every 16-byte / non-integer pair are rejected.
    assert_eq!(common(TypeCode::U64, TypeCode::I64), None);
    assert_eq!(common(TypeCode::U128, TypeCode::I64), None);
    assert_eq!(common(TypeCode::String, TypeCode::U64), None);
    // A DECIMAL unions only with the same DECIMAL: the stored integers of two
    // scales, or of a scale and an integer column, never mean the same value.
    assert_eq!(
        set_op_common_type(ColType::decimal(2), ColType::decimal(2)),
        Some(ColType::decimal(2))
    );
    assert_eq!(set_op_common_type(ColType::decimal(2), ColType::decimal(3)), None);
    assert_eq!(set_op_common_type(ColType::decimal(0), TypeCode::I64.into()), None);
}
