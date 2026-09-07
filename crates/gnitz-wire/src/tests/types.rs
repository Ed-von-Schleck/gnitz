use super::*;

/// Every type-code predicate, as one partition of the type table. The row count
/// is guarded against `TypeCode::ALL`, so a new variant fails here rather than
/// falling silently outside a hand-listed set in each predicate's own test.
/// Free fn and method are checked together — they are two spellings of one rule.
#[test]
fn type_predicates_partition_the_type_table() {
    /// `(type, fixed_int, signed_int, int, float, german_string, wide_int,
    /// pk_eligible)` — one row per `TypeCode`, one column per predicate.
    type Row = (TypeCode, bool, bool, bool, bool, bool, bool, bool);
    let table: &[Row] = &[
        (TypeCode::U8, true, false, true, false, false, false, true),
        (TypeCode::I8, true, true, true, false, false, false, true),
        (TypeCode::U16, true, false, true, false, false, false, true),
        (TypeCode::I16, true, true, true, false, false, false, true),
        (TypeCode::U32, true, false, true, false, false, false, true),
        (TypeCode::I32, true, true, true, false, false, false, true),
        (TypeCode::F32, false, false, false, true, false, false, false),
        (TypeCode::U64, true, false, true, false, false, false, true),
        (TypeCode::I64, true, true, true, false, false, false, true),
        (TypeCode::F64, false, false, false, true, false, false, false),
        (TypeCode::String, false, false, false, false, true, false, false),
        (TypeCode::U128, false, false, true, false, false, true, true),
        (TypeCode::UUID, false, false, false, false, false, true, true),
        (TypeCode::Blob, false, false, false, false, true, false, false),
        (TypeCode::I128, false, true, true, false, false, true, true),
    ];
    assert_eq!(table.len(), TypeCode::ALL.len(), "a TypeCode variant is unclassified");

    for &(tc, fixed, signed, int, float, german, wide, pk) in table {
        let raw = tc as u8;
        assert!(is_valid_type_code(raw), "{tc:?} must decode");
        assert_eq!(is_fixed_int(raw), fixed, "is_fixed_int({tc:?})");
        assert_eq!(
            FixedInt::from_type_code(tc).is_some(),
            fixed,
            "FixedInt is the witness of is_fixed_int, at {tc:?}"
        );
        assert_eq!(is_signed_int(raw), signed, "is_signed_int({tc:?})");
        assert_eq!(tc.is_signed_int(), signed, "TypeCode::is_signed_int({tc:?})");
        assert_eq!(is_int(raw), int, "is_int({tc:?})");
        assert_eq!(is_float(raw), float, "is_float({tc:?})");
        assert_eq!(tc.is_float(), float, "TypeCode::is_float({tc:?})");
        assert_eq!(is_german_string(raw), german, "is_german_string({tc:?})");
        assert_eq!(tc.is_german_string(), german, "TypeCode::is_german_string({tc:?})");
        assert_eq!(is_wide_int(raw), wide, "is_wide_int({tc:?})");
        assert_eq!(tc.is_wide_int(), wide, "TypeCode::is_wide_int({tc:?})");
        assert_eq!(is_pk_eligible(raw), pk, "is_pk_eligible({tc:?})");
        assert_eq!(tc.is_pk_eligible(), pk, "TypeCode::is_pk_eligible({tc:?})");
    }

    // An unknown code is not inert: every predicate must exclude it, or a
    // forged schema column reaches a path that assumes a decoded type.
    let known: Vec<u8> = table.iter().map(|r| r.0 as u8).collect();
    for raw in (0u8..=u8::MAX).filter(|r| !known.contains(r)) {
        assert!(!is_valid_type_code(raw), "code {raw} must not decode");
        assert!(!is_fixed_int(raw) && !is_signed_int(raw) && !is_int(raw), "code {raw}");
        assert!(!is_float(raw) && !is_german_string(raw), "code {raw}");
        assert!(!is_wide_int(raw) && !is_pk_eligible(raw), "code {raw}");
    }
}

/// The whole source → index-key promotion map, over the whole `u8` domain:
/// unsigned ≤8-byte ints → U64, signed ≤8-byte ints → I64 (an order-preserving
/// signed leading key), U128/UUID keep their width, everything else is
/// index-ineligible.
///
/// Width equality coincides with type identity, which is what makes
/// `promote_opk_column`'s identity arm length-safe *and* makes
/// `src_tc == target_tc` the correct selector for it.
#[test]
fn index_key_type_pins_the_whole_promotion_map() {
    use type_code as tc;
    let map: &[(u8, u8)] = &[
        (tc::U8, tc::U64),
        (tc::U16, tc::U64),
        (tc::U32, tc::U64),
        (tc::U64, tc::U64),
        (tc::I8, tc::I64),
        (tc::I16, tc::I64),
        (tc::I32, tc::I64),
        (tc::I64, tc::I64),
        (tc::U128, tc::U128),
        (tc::UUID, tc::UUID),
    ];
    for raw in 0u8..=u8::MAX {
        match map.iter().find(|&&(src, _)| src == raw) {
            Some(&(_, want)) => {
                assert_eq!(index_key_type(raw), Ok(want), "tc={raw}");
                assert_eq!(
                    wire_stride(raw) == wire_stride(want),
                    raw == want,
                    "tc={raw}: width equality must coincide with type identity",
                );
            }
            None => assert!(index_key_type(raw).is_err(), "tc={raw} must be index-ineligible"),
        }
    }
    // I128 is PK-eligible (produced only as a cross-sign equijoin `_join_pk`)
    // yet has no index promotion — the one type on both sides of that line.
    assert!(is_pk_eligible(tc::I128) && index_key_type(tc::I128).is_err());
}

/// Membership, discriminant round-trip and duplicate codes are pinned by the
/// const sweep in the module above; only the names need a runtime compare.
#[test]
fn type_code_wire_names_are_distinct() {
    let mut names: Vec<&str> = TypeCode::ALL.iter().map(|&tc| tc.wire_name()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), TypeCode::ALL.len(), "duplicate wire_name in ALL");
}

/// The reindex / `_join_pk` width policy (the single source of truth the engine
/// compiler and SQL planner both derive from): a ≤8-byte integer key keeps its
/// native width, `I128` keeps width 16 **with** its sign, and everything else —
/// U128/UUID, the STRING/BLOB content hash, and PK-ineligible floats — collapses
/// to the unsigned 16-byte U128 key. Spelled as an expectation per type rather
/// than re-derived from the predicate the production body branches on, and
/// guarded against `TypeCode::ALL` so a new variant cannot escape it.
#[test]
fn reindex_output_type_policy() {
    let policy: &[(TypeCode, TypeCode)] = &[
        (TypeCode::U8, TypeCode::U8),
        (TypeCode::I8, TypeCode::I8),
        (TypeCode::U16, TypeCode::U16),
        (TypeCode::I16, TypeCode::I16),
        (TypeCode::U32, TypeCode::U32),
        (TypeCode::I32, TypeCode::I32),
        (TypeCode::U64, TypeCode::U64),
        (TypeCode::I64, TypeCode::I64),
        // Wider or non-integer: the unsigned 16-byte key.
        (TypeCode::F32, TypeCode::U128),
        (TypeCode::F64, TypeCode::U128),
        (TypeCode::String, TypeCode::U128),
        (TypeCode::Blob, TypeCode::U128),
        (TypeCode::U128, TypeCode::U128),
        (TypeCode::UUID, TypeCode::U128),
        // The one 16-byte type that keeps its sign instead.
        (TypeCode::I128, TypeCode::I128),
    ];
    assert_eq!(
        policy.len(),
        TypeCode::ALL.len(),
        "a TypeCode variant has no stated policy"
    );
    for &(src, want) in policy {
        assert_eq!(src.reindex_output_type(), want, "{src:?}");
        assert_eq!(reindex_output_type_code(src as u8), want as u8, "{src:?} (free fn)");
    }
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
        // signed ladder reaching the 16-byte signed type
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

/// The cross-sign rule, accepted and refused sides in one table. A pair whose
/// unsigned operand is ≤ 8 bytes promotes to the narrowest signed type that
/// faithfully holds both ranges — strictly wider than the unsigned operand and
/// at least as wide as the signed one. A 128-bit unsigned operand (`U128` /
/// `UUID`) would need a signed-256 type, which does not exist, so it is
/// refused. Symmetric either way. (One-sided string and float pairs are
/// screened out in the planner and are not this fn's job.)
#[test]
fn join_key_common_type_over_cross_sign_pairs() {
    use type_code::*;
    let cases: &[(u8, u8, Option<u8>)] = &[
        (U8, I8, Some(I16)),
        (U8, I16, Some(I16)),
        (U8, I32, Some(I32)),
        (U8, I64, Some(I64)),
        (U16, I8, Some(I32)),
        (U16, I16, Some(I32)),
        (U16, I32, Some(I32)),
        (U16, I64, Some(I64)),
        (U32, I8, Some(I64)),
        (U32, I16, Some(I64)),
        (U32, I32, Some(I64)),
        (U32, I64, Some(I64)),
        // A U64 unsigned side carries the pair to the signed-128 type.
        (U64, I8, Some(I128)),
        (U64, I16, Some(I128)),
        (U64, I32, Some(I128)),
        (U64, I64, Some(I128)),
        // A 128-bit unsigned side has no faithful common type at any width.
        (U128, I8, None),
        (U128, I64, None),
        (UUID, I32, None),
        (UUID, I64, None),
        (U128, I128, None),
        (UUID, I128, None),
    ];
    for &(u, s, want) in cases {
        assert_eq!(join_key_common_type(u, s), want, "cross-sign common({u},{s})");
        assert_eq!(
            join_key_common_type(s, u),
            want,
            "cross-sign is symmetric for ({u},{s})"
        );
    }
}

/// `TypeCode::carried_reindex_tc` and `resolve_reindex_type` are inverses: a
/// slot that needs no promotion carries `None` ("derive from source"), a
/// cross-width slot carries its target `T`, and resolving what was carried
/// returns `T` either way.
#[test]
fn carried_reindex_tc_round_trips_with_resolve() {
    use TypeCode::*;
    // (source, promoted target T, whether T has to be carried)
    let cases: &[(TypeCode, TypeCode, bool)] = &[
        // Self-deriving slots: the per-column default policy already lands on T.
        (I32, I32, false),
        (U64, U64, false),
        (String, U128, false),
        (F64, U128, false),
        (U128, U128, false),
        (UUID, U128, false),
        // Cross-width slots carry the promoted target.
        (I8, I64, true),
        (I32, I64, true),
        (U8, U64, true),
        (U32, U64, true),
        (U32, U128, true),
        // Cross-sign promotions: the unsigned side lands on a signed T.
        (U8, I16, true),
        (U16, I32, true),
        (U32, I64, true),
        // Every side reaching the signed-128 slot.
        (U64, I128, true),
        (I8, I128, true),
        (I16, I128, true),
        (I32, I128, true),
        (I64, I128, true),
    ];
    for &(src, t, want_carried) in cases {
        let carried = src.carried_reindex_tc(t);
        assert_eq!(carried, want_carried.then_some(t), "carried target for {src:?} → {t:?}");
        assert_eq!(
            resolve_reindex_type(src as u8, carried),
            t as u8,
            "round-trip for {src:?} → {t:?}"
        );
        // A carried target re-derives to itself, which is how the compiler's
        // `ReindexPacker::new` validates one without re-implementing the
        // ladder. (A self-deriving slot carries nothing and is not validated
        // this way.)
        if carried.is_some() {
            assert_eq!(
                join_key_common_type(src as u8, t as u8),
                Some(t as u8),
                "promotion not idempotent for {src:?} → {t:?}: \
                 compiler carried-slot guard would diverge from the planner"
            );
        }
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

/// `decode_le_i64` reads the column's own width into the i64 register; `pack`
/// is its encode-side inverse over `range()`, writing an in-range value into
/// exactly the low `width()` bytes — so `-1` on an `I8` column packs to `0xFF`,
/// not to a sign-extended `0xFFFF…`.
#[test]
fn fixed_int_packs_and_decodes_its_own_width() {
    assert_eq!(FixedInt::U8.decode_le_i64(&[0xff]), 255i64);
    assert_eq!(FixedInt::I8.decode_le_i64(&[0xff]), -1i64);
    assert_eq!(FixedInt::U16.decode_le_i64(&[0xff, 0x00]), 255i64);
    assert_eq!(FixedInt::I16.decode_le_i64(&[0x00, 0x80]), i16::MIN as i64);
    assert_eq!(FixedInt::U32.decode_le_i64(&[0xff, 0xff, 0xff, 0xff]), u32::MAX as i64);
    assert_eq!(FixedInt::I32.decode_le_i64(&[0x00, 0x00, 0x00, 0x80]), i32::MIN as i64);
    // The unsigned 64-bit edge: the register holds the bit pattern.
    assert_eq!(FixedInt::U64.decode_le_i64(&u64::MAX.to_le_bytes()), -1i64);
    assert_eq!(FixedInt::I64.decode_le_i64(&i64::MIN.to_le_bytes()), i64::MIN);

    assert_eq!(FixedInt::I8.pack(-1), 0xFF, "a signed value packs at its own width");
    for fi in [
        FixedInt::U8,
        FixedInt::I8,
        FixedInt::U16,
        FixedInt::I16,
        FixedInt::U32,
        FixedInt::I32,
        FixedInt::U64,
        FixedInt::I64,
    ] {
        let (lo, hi) = fi.range();
        assert!(lo <= 0 && hi > 0, "{fi:?}: range must bracket zero");
        for v in [lo, 0, hi] {
            let packed = fi.pack(v);
            assert_eq!(packed >> (8 * fi.width()), 0, "{fi:?}: pack spilled past its width");
            assert_eq!(fi.decode_le_i64(&packed.to_le_bytes()), v as i64, "{fi:?} v={v}");
        }
    }
}

/// `int_domain_fits` over the sign/width ladder: a rewrite is admitted only when
/// no value of `src` can fall outside `target`. Also pins that
/// `is_widening_promotion` narrows it to a ≤8-byte slot.
#[test]
fn int_domain_fits_admits_exactly_the_lossless_rewrites() {
    for (src, target) in [
        (TypeCode::I32, TypeCode::I64),  // signed widen
        (TypeCode::U32, TypeCode::I64),  // unsigned into strictly wider signed
        (TypeCode::U8, TypeCode::U16),   // unsigned widen
        (TypeCode::I32, TypeCode::I32),  // identity
        (TypeCode::U64, TypeCode::U64),  // identity
        (TypeCode::U64, TypeCode::I128), // u64 fits i128
    ] {
        assert!(int_domain_fits(src as u8, target as u8), "{src:?} -> {target:?} fits");
    }
    for (src, target) in [
        (TypeCode::U64, TypeCode::I64), // same width, unsigned into signed
        (TypeCode::U32, TypeCode::I32),
        (TypeCode::U8, TypeCode::I8),
        (TypeCode::U128, TypeCode::I128),
        (TypeCode::I64, TypeCode::U64), // signed into unsigned, at any width
        (TypeCode::I32, TypeCode::U32),
        (TypeCode::I64, TypeCode::I32), // narrowing
    ] {
        assert!(
            !int_domain_fits(src as u8, target as u8),
            "{src:?} -> {target:?} does not fit"
        );
    }
    // No non-integer pair is in the domain — UUID shares U128's width and a
    // STRING/BLOB FK is admitted by exact equality, never by this rule.
    for tc in [
        TypeCode::UUID,
        TypeCode::F64,
        TypeCode::F32,
        TypeCode::String,
        TypeCode::Blob,
    ] {
        assert!(!int_domain_fits(tc as u8, tc as u8), "{tc:?} is in no integer domain");
        assert!(!int_domain_fits(tc as u8, TypeCode::I128 as u8), "{tc:?} -> I128");
        assert!(!int_domain_fits(TypeCode::I64 as u8, tc as u8), "I64 -> {tc:?}");
    }
    // The column-slot gate is the caller's scope, not part of the rule.
    assert!(int_domain_fits(TypeCode::U64 as u8, TypeCode::I128 as u8));
    assert!(!is_widening_promotion(TypeCode::U64 as u8, TypeCode::I128 as u8));
    assert!(is_widening_promotion(TypeCode::U32 as u8, TypeCode::I64 as u8));
}
