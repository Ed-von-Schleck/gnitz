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

/// DATE counts days and TIMESTAMP microseconds, so neither an equality nor a
/// range key pairs them.
#[test]
fn join_key_pair_rejects_date_with_timestamp() {
    let (d, ts) = (col("d", TypeCode::Date), col("ts", TypeCode::Timestamp));
    for err in [validate_join_key_pair(&d, &ts), validate_range_join_key_pair(&ts, &d)] {
        let m = err.unwrap_err().to_string();
        assert!(m.contains("differ in unit (days vs microseconds)"), "{m}");
    }
    assert_eq!(
        validate_join_key_pair(&d, &col("i", TypeCode::I32)).unwrap(),
        TypeCode::I32
    );
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

/// Only an INNER step may be keyless (the cross join), and a pure-range step
/// preserves the left side only; every other (shape, kind) pair exists.
#[test]
fn join_shape_admission() {
    for shape in [JoinShape::Cross, JoinShape::Equi, JoinShape::Band, JoinShape::PureRange] {
        reject_join_shape(JoinType::Inner, shape).unwrap();
    }
    for kind in [
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::Semi,
        JoinType::Anti,
        JoinType::Mark(crate::hir::ColId::NONE),
    ] {
        assert!(reject_join_shape(kind, JoinShape::Cross).is_err(), "{kind:?}");
        reject_join_shape(kind, JoinShape::Band).unwrap();
        reject_join_shape(kind, JoinShape::Equi).unwrap();
        // A pure-range step derives its null-fill from a left-side threshold, so
        // only the orientations preserving the right side are refused.
        assert_eq!(
            reject_join_shape(kind, JoinShape::PureRange).is_err(),
            kind.preserves_right(),
            "{kind:?}"
        );
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
