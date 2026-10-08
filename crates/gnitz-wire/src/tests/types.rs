use super::*;

/// Every type-code predicate, as one partition of the type table. The row count
/// is guarded against `TypeCode::ALL`, so a new variant fails here rather than
/// falling silently outside a hand-listed set in each predicate's own test.
#[test]
fn type_predicates_partition_the_type_table() {
    /// `(type, fixed_int, signed_int, int, float, german_string, wide_int,
    /// pk_eligible, serial_eligible)` — one row per `TypeCode`, one column per
    /// predicate. A SERIAL key is exactly the plain integers of at most 8 bytes:
    /// not a DATE's days, a DECIMAL's scaled units, nor a 16-byte type. A
    /// BOOLEAN is stored as a fixed int and keys as one, and is no integer.
    type Row = (TypeCode, bool, bool, bool, bool, bool, bool, bool, bool);
    let table: &[Row] = &[
        (TypeCode::U8, true, false, true, false, false, false, true, true),
        (TypeCode::I8, true, true, true, false, false, false, true, true),
        (TypeCode::U16, true, false, true, false, false, false, true, true),
        (TypeCode::I16, true, true, true, false, false, false, true, true),
        (TypeCode::U32, true, false, true, false, false, false, true, true),
        (TypeCode::I32, true, true, true, false, false, false, true, true),
        (TypeCode::F32, false, false, false, true, false, false, false, false),
        (TypeCode::U64, true, false, true, false, false, false, true, true),
        (TypeCode::I64, true, true, true, false, false, false, true, true),
        (TypeCode::F64, false, false, false, true, false, false, false, false),
        (TypeCode::String, false, false, false, false, true, false, false, false),
        (TypeCode::U128, false, false, true, false, false, true, true, false),
        (TypeCode::UUID, false, false, false, false, false, true, true, false),
        (TypeCode::Blob, false, false, false, false, true, false, false, false),
        (TypeCode::I128, false, true, true, false, false, true, true, false),
        (TypeCode::Date, true, true, true, false, false, false, true, false),
        (TypeCode::Timestamp, true, true, true, false, false, false, true, false),
        (TypeCode::Decimal, true, true, true, false, false, false, true, false),
        (TypeCode::Bool, true, false, false, false, false, false, true, false),
    ];
    assert_eq!(table.len(), TypeCode::ALL.len(), "a TypeCode variant is unclassified");

    for &(tc, fixed, signed, int, float, german, wide, pk, serial) in table {
        assert_eq!(tc.is_fixed_int(), fixed, "is_fixed_int({tc:?})");
        assert_eq!(tc.is_signed_int(), signed, "is_signed_int({tc:?})");
        assert_eq!(tc.is_int(), int, "is_int({tc:?})");
        assert_eq!(tc.is_float(), float, "is_float({tc:?})");
        assert_eq!(tc.is_german_string(), german, "is_german_string({tc:?})");
        assert_eq!(tc.is_wide_int(), wide, "is_wide_int({tc:?})");
        assert_eq!(tc.is_pk_eligible(), pk, "is_pk_eligible({tc:?})");
        assert_eq!(FixedInt::exact(tc).is_some(), serial, "FixedInt::exact({tc:?})");
    }
}

/// Membership and the discriminant round-trip are `wire_enum!`'s; only the
/// names need a runtime compare.
#[test]
fn type_code_wire_names_are_distinct() {
    let mut names: Vec<&str> = TypeCode::ALL.iter().map(|&tc| tc.wire_name()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), TypeCode::ALL.len(), "duplicate wire_name in ALL");
}

/// `[min, max]` of an integer type's values, read through its storage type;
/// `None` for UUID, BOOLEAN and every non-integer.
fn int_bounds(tc: TypeCode) -> Option<(i128, u128)> {
    if tc == TypeCode::Bool {
        return None;
    }
    match tc.storage_type() {
        TypeCode::U128 => Some((0, u128::MAX)),
        TypeCode::I128 => Some((i128::MIN, i128::MAX as u128)),
        t => FixedInt::from_type_code(t).map(|fi| {
            let (lo, hi) = fi.range();
            (lo, hi as u128)
        }),
    }
}

/// The bounds a type's key image spans: a UUID keys as the U128 it is stored as.
fn key_bounds(tc: TypeCode) -> (i128, u128) {
    int_bounds(if tc == TypeCode::UUID { TypeCode::U128 } else { tc }).expect("an integer key type")
}

fn contains(outer: (i128, u128), inner: (i128, u128)) -> bool {
    outer.0 <= inner.0 && inner.1 <= outer.1
}

/// A rewrite is admitted exactly when no value of `src` can fall outside
/// `target`; `is_widening_promotion` narrows that to a ≤8-byte slot, the
/// column-copy caller's scope.
#[test]
fn int_domain_fits_is_bounds_containment() {
    for &s in TypeCode::ALL {
        for &t in TypeCode::ALL {
            let want = matches!((int_bounds(s), int_bounds(t)), (Some(a), Some(b)) if contains(b, a));
            assert_eq!(s.int_domain_fits(t), want, "{s} -> {t}");
            assert_eq!(s.is_widening_promotion(t), want && t.is_fixed_int(), "{s} -> {t}");
        }
    }
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
        (TypeCode::Date, TypeCode::Date),
        (TypeCode::Timestamp, TypeCode::Timestamp),
        (TypeCode::Decimal, TypeCode::Decimal),
        (TypeCode::Bool, TypeCode::Bool),
    ];
    assert_eq!(
        policy.len(),
        TypeCode::ALL.len(),
        "a TypeCode variant has no stated policy"
    );
    for &(src, want) in policy {
        assert_eq!(src.reindex_output_type(), want, "{src:?}");
    }
}

/// The key slot both sides of an equijoin key pack at: a refusal for a float, a
/// string against a native key, or two calendar units; the content hash for two
/// strings; and otherwise the narrowest slot holding both sides' values — a
/// 128-bit unsigned side against a signed one would need a signed-256 type,
/// which does not exist. Symmetric, and both sides pack at it.
#[test]
fn join_key_common_type_is_the_narrowest_faithful_slot() {
    use TypeCode::*;
    let want = |l: TypeCode, r: TypeCode| {
        if l.is_float() || r.is_float() {
            return Err(JoinKeyRule::Float);
        }
        if l.is_german_string() != r.is_german_string() {
            return Err(JoinKeyRule::StringWithNative);
        }
        if l == r {
            return Ok(l.reindex_output_type());
        }
        if l.is_german_string() {
            return Ok(U128);
        }
        if l == Bool || r == Bool {
            return Err(JoinKeyRule::BoolWithNumber);
        }
        if l.is_temporal() && r.is_temporal() {
            return Err(JoinKeyRule::UnitMismatch);
        }
        let (a, b) = (key_bounds(l), key_bounds(r));
        let both = (a.0.min(b.0), a.1.max(b.1));
        [U8, I8, U16, I16, U32, I32, U64, I64, U128, I128]
            .into_iter()
            .find(|&c| contains(int_bounds(c).unwrap(), both))
            .ok_or(JoinKeyRule::NoSigned256)
    };
    for &l in TypeCode::ALL {
        for &r in TypeCode::ALL {
            let got = l.join_key_common_type(r);
            assert_eq!(got, want(l, r), "({l},{r})");
            assert_eq!(got, r.join_key_common_type(l), "symmetric ({l},{r})");
            if let Ok(t) = got {
                assert!(l.packs_at(t) && r.packs_at(t), "({l},{r}) at {t}");
            }
        }
    }
}

/// The PK admission rule, one refusal per rule.
#[test]
fn validate_pk_tuple_names_each_rule() {
    use TypeCode::*;
    // (type, nullable) per column.
    let cols = [(U64, false), (I32, false), (String, false), (F64, false), (I64, true)];
    let check = |pk: &[u32], max| validate_pk_tuple(pk, cols.len(), max, |c| cols[c as usize]);
    assert_eq!(check(&[0, 1], 4), Ok(()));
    for (pk, max, want) in [
        (&[][..], 4, PkRule::Empty),
        (&[0, 1], 1, PkRule::TooManyColumns { count: 2, max: 1 }),
        (&[5], 4, PkRule::IndexOutOfRange { col: 5 }),
        (&[1, 1], 4, PkRule::Duplicate { col: 1 }),
        (&[2], 4, PkRule::NotEligible { col: 2, type_code: String }),
        (&[3], 4, PkRule::NotEligible { col: 3, type_code: F64 }),
        (&[4], 4, PkRule::Nullable { col: 4 }),
    ] {
        assert_eq!(check(pk, max), Err(want), "{pk:?}");
    }
}

/// A rule names a column by the name its caller holds for it, and by its index
/// where the caller holds none.
#[test]
fn a_pk_rule_names_the_columns_its_caller_can() {
    let names = ["id", "price"];
    let name = |c: u32| names.get(c as usize).copied();
    let role = PkListRole::PrimaryKey;
    for (rule, named, positional) in [
        (
            PkRule::Duplicate { col: 1 },
            "primary key names column 'price' twice",
            "primary key names column 1 twice",
        ),
        (
            PkRule::Nullable { col: 0 },
            "primary key column 'id' must not be nullable",
            "primary key column 0 must not be nullable",
        ),
        (
            PkRule::Nullable { col: 7 },
            "primary key column 7 must not be nullable",
            "primary key column 7 must not be nullable",
        ),
        (
            PkRule::IndexOutOfRange { col: 1 },
            "primary key index 1 out of bounds",
            "primary key index 1 out of bounds",
        ),
    ] {
        assert_eq!(rule.named(role, name), named, "{rule:?}");
        assert_eq!(rule.for_role(role), positional, "{rule:?}");
    }
    let float = PkRule::NotEligible { col: 1, type_code: TypeCode::F64 };
    assert!(float
        .named(role, name)
        .starts_with("primary key column 'price' has type_code F64;"));
    assert!(float
        .for_role(role)
        .starts_with("primary key column 1 has type_code F64;"));
}

/// Each fixed-width type's order, both directions: unsigned magnitude at every
/// width (the high bit last), signed with negatives first, a calendar or
/// decimal type as its storage integer, 128-bit values across the limb, and
/// floats by `total_cmp` (-0.0 < +0.0, NaN ordered and equal to itself).
#[test]
fn cmp_col_window_orders_every_fixed_width_type() {
    use core::cmp::Ordering::{self, *};
    use TypeCode::*;
    let le = |v: i128, n: usize| v.to_le_bytes()[..n].to_vec();
    let f = |v: f64| v.to_le_bytes().to_vec();
    let cases: &[(TypeCode, Vec<u8>, Vec<u8>, Ordering)] = &[
        (U8, le(0, 1), le(0xFF, 1), Less),
        (U16, le(1, 2), le(256, 2), Less),
        (U32, le(0, 4), le(u32::MAX as i128, 4), Less),
        (U64, le(1, 8), le(u64::MAX as i128, 8), Less),
        (U128, le(1, 16), le(1 << 64, 16), Less),
        (UUID, le(1, 16), le(1 << 64, 16), Less),
        (I8, le(-1, 1), le(0, 1), Less),
        (I16, le(-1, 2), le(0, 2), Less),
        (I32, le(-5, 4), le(5, 4), Less),
        (I64, le(i64::MIN as i128, 8), le(i64::MAX as i128, 8), Less),
        (I128, le(-1, 16), le(0, 16), Less),
        (Date, le(-1, 4), le(0, 4), Less),
        (Timestamp, le(-1, 8), le(0, 8), Less),
        (Decimal, le(-1, 8), le(0, 8), Less),
        (F64, f(-0.0), f(0.0), Less),
        (F64, f(f64::INFINITY), f(f64::NAN), Less),
        (F64, f(f64::NAN), f(f64::NAN), Equal),
        (F32, 1.5f32.to_le_bytes().to_vec(), 2.5f32.to_le_bytes().to_vec(), Less),
    ];
    for (tc, a, b, want) in cases {
        assert_eq!(cmp_col_window(a, &[], b, &[], *tc), *want, "{tc}");
        assert_eq!(cmp_col_window(b, &[], a, &[], *tc), want.reverse(), "{tc} reversed");
    }
}

/// `decode_le_i64` reads the column's own width into the i64 register; `pack`
/// is its encode-side inverse over `range()`, writing an in-range value into
/// exactly the low `width()` bytes — so `-1` on an `I8` column packs to `0xFF`,
/// not to a sign-extended `0xFFFF…`; and `unpack` answers what `decode_le_i64`
/// answers over any `u128`'s bytes, its unsigned arm masking the bits past the
/// width.
#[test]
fn fixed_int_packs_unpacks_and_decodes_its_own_width() {
    assert_eq!(FixedInt::U8.decode_le_i64(&[0xff]), 255i64);
    assert_eq!(FixedInt::I8.decode_le_i64(&[0xff]), -1i64);
    assert_eq!(FixedInt::U16.decode_le_i64(&[0xff, 0x00]), 255i64);
    assert_eq!(FixedInt::I16.decode_le_i64(&[0x00, 0x80]), i16::MIN as i64);
    assert_eq!(FixedInt::U32.decode_le_i64(&[0xff, 0xff, 0xff, 0xff]), u32::MAX as i64);
    assert_eq!(FixedInt::I32.decode_le_i64(&[0x00, 0x00, 0x00, 0x80]), i32::MIN as i64);
    // The unsigned 64-bit edge: the register holds the bit pattern.
    assert_eq!(FixedInt::U64.decode_le_i64(&u64::MAX.to_le_bytes()), -1i64);
    assert_eq!(FixedInt::I64.decode_le_i64(&i64::MIN.to_le_bytes()), i64::MIN);

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
        for v in [lo, 0, hi] {
            let packed = fi.pack(v);
            assert_eq!(packed >> (8 * fi.width()), 0, "{fi:?}: pack spilled past its width");
            assert_eq!(fi.unpack(packed), v as i64, "{fi:?} v={v}");
        }
        for v in [0u128, 1, 0x7F, 0x80, 0xFF, 0x100, u64::MAX as u128, u128::MAX] {
            assert_eq!(fi.unpack(v), fi.decode_le_i64(&v.to_le_bytes()), "{fi:?} v={v:#x}");
        }
    }
}

#[test]
fn narrow_f32_refuses_only_a_finite_overflow() {
    assert_eq!(narrow_f32(0.1), Some(0.1f64 as f32));
    assert_eq!(narrow_f32(1e-300), Some(0.0));
    assert_eq!(narrow_f32(1e39), None);
    assert_eq!(narrow_f32(-1e39), None);
    assert_eq!(narrow_f32(f64::INFINITY), Some(f32::INFINITY));
    assert!(narrow_f32(f64::NAN).is_some_and(f32::is_nan));
    // Just above `f32::MAX` rounds down onto it rather than overflowing.
    assert_eq!(narrow_f32(f32::MAX as f64 + 2.0f64.powi(102)), Some(f32::MAX));
}

/// An integer's order image is its OPK key in an 8-byte slot of its own sign, and
/// the inverse recovers the value.
#[test]
fn an_integers_order_image_is_its_opk_key_promoted() {
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
        let w = fi.width();
        let mask = u64::MAX >> (64 - 8 * w);
        // Exhaustive where the width allows, else the edges plus a sweep.
        let vals: Vec<u64> = match w {
            1 | 2 => (0..=mask).collect(),
            _ => [0, 1, mask, mask >> 1, (mask >> 1) + 1]
                .into_iter()
                .chain((1..=64u64).map(|i| 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(i) & mask))
                .collect(),
        };
        let kind = ScalarKind::Int(fi);
        for x in vals {
            let widened = fi.decode_le_i64(&x.to_le_bytes()[..w]);
            let mut key = [0u8; 8];
            crate::store_opk(&mut key, widened as u128, fi.is_signed());
            let image = kind.order_image(widened as u64);
            assert_eq!(image, u64::from_be_bytes(key), "{fi:?} value {x:#x}");
            assert_eq!(
                kind.order_inverse(image).to_le_bytes()[..w],
                x.to_le_bytes()[..w],
                "{fi:?} inverse, value {x:#x}"
            );
        }
    }
}

/// A float's order image orders as [`cmp_col_window`] and the inverse recovers its
/// bits, over the values only floats have: ±0.0, ±NaN and both infinities.
#[test]
fn a_floats_order_image_is_the_total_order() {
    const FLOATS: [f64; 9] = [
        f64::NEG_INFINITY,
        -1.5,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        1.5,
        f64::INFINITY,
        f64::NAN,
        -f64::NAN,
    ];
    for (kind, tc, w) in [(ScalarKind::F32, TypeCode::F32, 4), (ScalarKind::F64, TypeCode::F64, 8)] {
        let bits: Vec<u64> = FLOATS
            .iter()
            .map(|&f| {
                if w == 4 {
                    (f as f32).to_bits() as u64
                } else {
                    f.to_bits()
                }
            })
            .collect();
        for &a in &bits {
            let image = kind.order_image(a);
            assert_eq!(kind.order_inverse(image), a, "{kind:?} inverse of {a:#x}");
            for &b in &bits {
                assert_eq!(
                    image.cmp(&kind.order_image(b)),
                    cmp_col_window(&a.to_le_bytes()[..w], &[], &b.to_le_bytes()[..w], &[], tc),
                    "{kind:?}: {a:#x} vs {b:#x}"
                );
            }
        }
    }
}
