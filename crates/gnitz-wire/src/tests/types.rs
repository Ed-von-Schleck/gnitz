use super::*;

/// Index-key promotion: unsigned ≤8-byte ints → U64, signed ≤8-byte ints →
/// I64 (order-preserving signed leading key); U128/UUID keep width; STRING/
/// BLOB/float (and unknown) are rejected. `wire_stride` of both U64 and I64 is
/// 8, so the index record stride is unchanged by the signed promotion.
#[test]
fn index_key_type_promotion() {
    use type_code as tc;
    for t in [tc::U8, tc::U16, tc::U32, tc::U64] {
        assert_eq!(index_key_type(t).unwrap(), tc::U64, "{t} must promote to U64");
    }
    for t in [tc::I8, tc::I16, tc::I32, tc::I64] {
        assert_eq!(index_key_type(t).unwrap(), tc::I64, "{t} must promote to I64");
        assert_eq!(
            wire_stride(index_key_type(t).unwrap()),
            wire_stride(tc::U64),
            "signed promotion must keep the 8-byte width"
        );
    }
    assert_eq!(index_key_type(tc::U128).unwrap(), tc::U128);
    assert_eq!(index_key_type(tc::UUID).unwrap(), tc::UUID);
    for t in [tc::F32, tc::F64, tc::STRING, tc::BLOB] {
        assert!(index_key_type(t).is_err(), "{t} must be rejected");
    }
}

/// Two structural facts the engine's index-span writer rests on, swept over
/// the whole `u8` domain rather than a hand-listed set:
///   (a) every index key type is one of the four promoted targets, and
///   (b) source width == index width EXACTLY when source type == index type.
///
/// (b) is what makes `promote_opk_column`'s identity arm length-safe *and*
/// makes `src_tc == target_tc` the correct selector for it. It is a
/// `gnitz-wire` property end to end — `index_key_type`, `is_pk_eligible` and
/// `wire_stride` all live here — so it is pinned here, where a change to any
/// of the three cannot pass `cargo test -p gnitz-wire`.
#[test]
fn index_key_type_set_is_closed_and_width_stable() {
    use type_code as tc;
    let mut reached = 0usize;
    for raw in 0u8..=u8::MAX {
        if !is_pk_eligible(raw) {
            assert!(
                index_key_type(raw).is_err(),
                "a PK-ineligible type {raw} must not be index-eligible"
            );
            continue;
        }
        let Ok(idx) = index_key_type(raw) else {
            // I128 is PK-eligible (produced only as a cross-sign equijoin
            // `_join_pk`) but has no index promotion.
            assert_eq!(raw, tc::I128, "only I128 may be PK-eligible yet index-ineligible");
            continue;
        };
        assert!(
            matches!(idx, tc::U64 | tc::I64 | tc::U128 | tc::UUID),
            "index_key_type({raw}) = {idx} lies outside the promoted target set",
        );
        assert_eq!(
            wire_stride(raw) == wire_stride(idx),
            raw == idx,
            "tc={raw}: width equality must coincide with type identity",
        );
        reached += 1;
    }
    // U64/I64/U128/UUID (identity) + U8/U16/U32/I8/I16/I32 (promoted).
    assert_eq!(reached, 10, "the reachable source→index pair set changed");
}

/// Membership, discriminant round-trip and duplicate codes are pinned by the
/// const sweep above; only the names need a runtime string compare.
#[test]
fn type_code_wire_names_are_distinct() {
    let mut names: Vec<&str> = TypeCode::ALL.iter().map(|&tc| tc.wire_name()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), TypeCode::ALL.len(), "duplicate wire_name in ALL");
}

/// The reindex / `_join_pk` width policy (the single source of truth the
/// engine compiler and SQL planner both derive from): a ≤8-byte integer key
/// keeps its native width; STRING/BLOB, U128/UUID, and floats collapse to
/// U128. Verified through both the `u8` free fn and the `TypeCode` method.
#[test]
fn reindex_output_type_policy() {
    let narrow = [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I64,
    ];
    for tc in narrow {
        assert_eq!(tc.reindex_output_type(), tc, "{tc:?} keeps native width");
        assert_eq!(reindex_output_type_code(tc as u8), tc as u8);
    }
    let wide = [
        TypeCode::U128,
        TypeCode::UUID,
        TypeCode::String,
        TypeCode::Blob,
        TypeCode::F32,
        TypeCode::F64,
    ];
    for tc in wide {
        assert_eq!(tc.reindex_output_type(), TypeCode::U128, "{tc:?} collapses to U128");
        assert_eq!(reindex_output_type_code(tc as u8), type_code::U128);
    }
    // I128 keeps width 16 WITH its sign (a signed-16 reindex slot), unlike the
    // unsigned 16-byte types that collapse to U128.
    assert_eq!(TypeCode::I128.reindex_output_type(), TypeCode::I128);
    assert_eq!(reindex_output_type_code(type_code::I128), type_code::I128);
}

/// Every accepted same-sign-class ladder pair promotes to the wider type
/// (U128 for any 16-byte unsigned operand), and the result is symmetric.
#[test]
fn join_key_common_type_accepts_ladders() {
    use type_code::*;
    let cases: &[(u8, u8, u8)] = &[
        // signed ladder → wider signed
        (I8, I16, I16),
        (I8, I32, I32),
        (I8, I64, I64),
        (I16, I32, I32),
        (I16, I64, I64),
        (I32, I64, I64),
        // signed ladder reaching the new 16-byte signed type
        (I8, I128, I128),
        (I64, I128, I128),
        (I128, I128, I128),
        // unsigned ladder → wider unsigned
        (U8, U16, U16),
        (U8, U32, U32),
        (U8, U64, U64),
        (U16, U32, U32),
        (U16, U64, U64),
        (U32, U64, U64),
        // unsigned with a 16-byte operand → U128
        (U32, U128, U128),
        (U64, U128, U128),
        (U32, UUID, U128),
        (U8, U128, U128),
        // same type → identity (fixed ints) / U128 (16-byte / string)
        (I32, I32, I32),
        (U64, U64, U64),
        (U128, U128, U128),
        (UUID, UUID, U128),
        (STRING, STRING, U128),
        (BLOB, BLOB, U128),
        // both german strings (different variants) → U128 content hash
        (STRING, BLOB, U128),
    ];
    for &(l, r, t) in cases {
        assert_eq!(join_key_common_type(l, r), Some(t), "common({l},{r}) should be {t}");
        assert_eq!(join_key_common_type(r, l), Some(t), "common is symmetric for ({l},{r})");
    }
}

/// Cross-sign integer pairs whose unsigned operand is ≤ 8 bytes (`U64`) promote
/// to the narrowest signed type that faithfully holds both ranges: strictly
/// wider than the unsigned operand AND at least as wide as the signed operand. A
/// `U64` unsigned side carries the pair to the new signed-128 type. Symmetric.
#[test]
fn join_key_common_type_accepts_cross_sign() {
    use type_code::*;
    let cases: &[(u8, u8, u8)] = &[
        (U8, I8, I16),
        (U8, I16, I16),
        (U8, I32, I32),
        (U8, I64, I64),
        (U16, I8, I32),
        (U16, I16, I32),
        (U16, I32, I32),
        (U16, I64, I64),
        (U32, I8, I64),
        (U32, I16, I64),
        (U32, I32, I64),
        (U32, I64, I64),
        // U64 unsigned side ⇒ the signed-128 common type.
        (U64, I8, I128),
        (U64, I16, I128),
        (U64, I32, I128),
        (U64, I64, I128),
    ];
    for &(u, s, t) in cases {
        assert_eq!(
            join_key_common_type(u, s),
            Some(t),
            "cross-sign common({u},{s}) should be {t}"
        );
        assert_eq!(
            join_key_common_type(s, u),
            Some(t),
            "cross-sign common is symmetric for ({u},{s})"
        );
    }
}

/// The surviving cross-sign reject contract: the unsigned operand is 128-bit
/// (`U128`/`UUID`), whose faithful common type is a signed-256 type that does
/// not exist. (A `U64` unsigned side now promotes to `I128` — see
/// `join_key_common_type_accepts_cross_sign`.) The one-sided string and float
/// cases are screened out earlier (in the planner) and are not this fn's job.
#[test]
fn join_key_common_type_rejects_wide_unsigned_cross_sign() {
    use type_code::*;
    let reject: &[(u8, u8)] = &[
        (U128, I8),
        (U128, I64),
        (UUID, I32),
        (UUID, I64),
        // a 128-bit unsigned side paired with the signed-128 type still has no
        // faithful (signed-256) common type.
        (U128, I128),
        (UUID, I128),
    ];
    for &(l, r) in reject {
        assert_eq!(join_key_common_type(l, r), None, "({l},{r}) must reject");
        assert_eq!(join_key_common_type(r, l), None, "({r},{l}) must reject");
    }
}

/// `resolve_reindex_type`: a non-zero carried `T` wins; `0` falls back to the
/// per-column default policy.
#[test]
fn resolve_reindex_type_carried_or_derive() {
    use type_code::*;
    // Carried T wins.
    assert_eq!(resolve_reindex_type(I32, I64), I64);
    assert_eq!(resolve_reindex_type(U8, U64), U64);
    // 0 = derive from source policy.
    assert_eq!(resolve_reindex_type(I32, 0), I32);
    assert_eq!(resolve_reindex_type(STRING, 0), U128);
    assert_eq!(resolve_reindex_type(F64, 0), U128);
}

/// `carried_reindex_tc` is the inverse of `resolve_reindex_type`: a self-
/// deriving slot collapses to `0` (byte-compat), a cross-width slot carries
/// `T`, and the two always round-trip.
#[test]
fn carried_reindex_tc_round_trips_with_resolve() {
    use type_code::*;
    // A slot that needs no promotion collapses to 0 and carries no target.
    assert_eq!(carried_reindex_tc(I32, I32), 0);
    assert_eq!(carried_reindex_tc(U64, U64), 0);
    assert_eq!(carried_reindex_tc(STRING, U128), 0);
    assert_eq!(carried_reindex_tc(U128, U128), 0);
    assert_eq!(carried_reindex_tc(UUID, U128), 0);
    // Cross-width slots carry the promoted target.
    assert_eq!(carried_reindex_tc(I32, I64), I64);
    assert_eq!(carried_reindex_tc(U8, U64), U64);
    assert_eq!(carried_reindex_tc(U32, U128), U128);
    // Round-trip: resolve(src, carried(src, T)) == T for any valid promotion,
    // including cross-sign promotions where the source promotes to a wider
    // signed type (e.g. the unsigned U8/U16/U32 sides and a same-sign signed
    // side both landing on a signed T).
    for &(src, t) in &[
        (I8, I64),
        (I32, I32),
        (I32, I64),
        (U8, U64),
        (U32, U64),
        (U32, U128),
        (U64, U64),
        (STRING, U128),
        (U128, U128),
        (UUID, U128),
        (U8, I16),
        (U16, I32),
        (U32, I64),
        (I32, I64),
        // cross-sign and same-sign promotions reaching the signed-128 slot:
        // the unsigned U64 side and every signed side land on I128.
        (U64, I128),
        (I8, I128),
        (I16, I128),
        (I32, I128),
        (I64, I128),
    ] {
        let carried = carried_reindex_tc(src, t);
        assert_eq!(
            resolve_reindex_type(src, carried),
            t,
            "round-trip failed for src={src} T={t}"
        );
        // Idempotency of promotion: a *carried* (non-zero) target re-derives to
        // itself through join_key_common_type. The engine compiler relies on
        // exactly this to validate a carried `_join_pk` slot against the planner
        // without re-implementing the sign/width ladder — so it must hold for
        // every promotion a source can carry. (Self-deriving slots carry 0 and
        // are validated by the per-column default policy, not this rule.)
        if carried != 0 {
            assert_eq!(
                join_key_common_type(src, t),
                Some(t),
                "promotion not idempotent for src={src} T={t}: \
                 compiler carried-slot guard would diverge from the planner"
            );
        }
    }
}

#[test]
fn fixed_int_predicate_matches_witness() {
    use TypeCode::*;
    for tc in [
        U8, I8, U16, I16, U32, I32, F32, U64, I64, F64, String, U128, UUID, Blob, I128,
    ] {
        assert_eq!(
            is_fixed_int(tc as u8),
            FixedInt::from_type_code(tc).is_some(),
            "mismatch for {tc:?}"
        );
    }
}

#[test]
fn signed_int_predicate_classifies_and_method_agrees() {
    use TypeCode::*;
    let signed = [I8, I16, I32, I64, I128];
    for tc in [
        U8, I8, U16, I16, U32, I32, F32, U64, I64, F64, String, U128, UUID, Blob, I128,
    ] {
        let want = signed.contains(&tc);
        assert_eq!(is_signed_int(tc as u8), want, "free fn mismatch for {tc:?}");
        assert_eq!(tc.is_signed_int(), want, "method mismatch for {tc:?}");
    }
}

#[test]
fn fixed_int_from_type_code_coverage() {
    assert!(FixedInt::from_type_code(TypeCode::U8).is_some());
    assert!(FixedInt::from_type_code(TypeCode::I8).is_some());
    assert!(FixedInt::from_type_code(TypeCode::U16).is_some());
    assert!(FixedInt::from_type_code(TypeCode::I16).is_some());
    assert!(FixedInt::from_type_code(TypeCode::U32).is_some());
    assert!(FixedInt::from_type_code(TypeCode::I32).is_some());
    assert!(FixedInt::from_type_code(TypeCode::U64).is_some());
    assert!(FixedInt::from_type_code(TypeCode::I64).is_some());
    for tc in [
        TypeCode::F32,
        TypeCode::F64,
        TypeCode::U128,
        TypeCode::UUID,
        TypeCode::String,
        TypeCode::Blob,
        TypeCode::I128,
    ] {
        assert!(FixedInt::from_type_code(tc).is_none(), "{tc:?} should be None");
    }
}

#[test]
fn cmp_typed_le_unsigned_and_signed_widths() {
    use core::cmp::Ordering;
    use type_code as tc;
    // Unsigned: plain magnitude order.
    assert_eq!(cmp_typed_le(&[1u8], &[2u8], tc::U8), Ordering::Less);
    assert_eq!(
        cmp_typed_le(&256u16.to_le_bytes(), &1u16.to_le_bytes(), tc::U16),
        Ordering::Greater,
        "multi-byte LE: 256 > 1 (guards a byte-swapped or first-byte-only compare)"
    );
    // Signed: negatives sort below non-negatives at every width.
    assert_eq!(
        cmp_typed_le(&(-1i8 as u8).to_le_bytes(), &0u8.to_le_bytes(), tc::I8),
        Ordering::Less
    );
    assert_eq!(
        cmp_typed_le(&(-5i32).to_le_bytes(), &5i32.to_le_bytes(), tc::I32),
        Ordering::Less
    );
    assert_eq!(
        cmp_typed_le(&i64::MIN.to_le_bytes(), &i64::MAX.to_le_bytes(), tc::I64),
        Ordering::Less
    );
}

#[test]
fn cmp_typed_le_u64_high_bit_sorts_last() {
    use core::cmp::Ordering;
    // The decode_le_i64 trap: `u64::MAX` must sort GREATER than 0, not first.
    assert_eq!(
        cmp_typed_le(&u64::MAX.to_le_bytes(), &0u64.to_le_bytes(), type_code::U64),
        Ordering::Greater
    );
    assert_eq!(
        cmp_typed_le(&u64::MAX.to_le_bytes(), &1u64.to_le_bytes(), type_code::U64),
        Ordering::Greater
    );
}

#[test]
fn cmp_typed_le_floats_total_cmp() {
    use core::cmp::Ordering;
    // total_cmp: -0.0 < +0.0, and NaN is ordered (does not collapse to Equal).
    assert_eq!(
        cmp_typed_le(&(-0.0f64).to_le_bytes(), &0.0f64.to_le_bytes(), type_code::F64),
        Ordering::Less
    );
    assert_eq!(
        cmp_typed_le(&f64::NAN.to_le_bytes(), &f64::INFINITY.to_le_bytes(), type_code::F64),
        Ordering::Greater
    );
    assert_eq!(
        cmp_typed_le(&1.5f32.to_le_bytes(), &2.5f32.to_le_bytes(), type_code::F32),
        Ordering::Less
    );
}

#[test]
fn cmp_typed_le_wide_ints() {
    use core::cmp::Ordering;
    assert_eq!(
        cmp_typed_le(&1u128.to_le_bytes(), &2u128.to_le_bytes(), type_code::U128),
        Ordering::Less
    );
    assert_eq!(
        cmp_typed_le(&u128::MAX.to_le_bytes(), &0u128.to_le_bytes(), type_code::UUID),
        Ordering::Greater
    );
    // I128 signed: a negative sorts below zero.
    assert_eq!(
        cmp_typed_le(&(-1i128).to_le_bytes(), &0i128.to_le_bytes(), type_code::I128),
        Ordering::Less
    );
}

#[test]
fn fixed_int_decode_le_i64_round_trips() {
    assert_eq!(FixedInt::U8.decode_le_i64(&[0xff]), 255i64);
    assert_eq!(FixedInt::I8.decode_le_i64(&[0xff]), -1i64);
    assert_eq!(FixedInt::U16.decode_le_i64(&[0xff, 0x00]), 255i64);
    assert_eq!(FixedInt::I16.decode_le_i64(&[0x00, 0x80]), i16::MIN as i64);
    assert_eq!(FixedInt::U32.decode_le_i64(&[0xff, 0xff, 0xff, 0xff]), u32::MAX as i64);
    assert_eq!(FixedInt::I32.decode_le_i64(&[0x00, 0x00, 0x00, 0x80]), i32::MIN as i64);
    assert_eq!(FixedInt::U64.decode_le_i64(&u64::MAX.to_le_bytes()), -1i64);
    assert_eq!(FixedInt::I64.decode_le_i64(&i64::MIN.to_le_bytes()), i64::MIN);
}
