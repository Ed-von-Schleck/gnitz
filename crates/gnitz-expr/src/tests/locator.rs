//! Resolved-addressing reads, driven through a non-engine [`crate::BatchView`].

use std::cmp::Ordering;

use crate::test_support::{locator_fixture as fixture, TestView};
use crate::ColumnLocator;
use gnitz_wire::type_code as tc;
use gnitz_wire::{FixedInt, ScalarKind};

#[test]
fn pk_locator_reads_decode_the_opk_sign_flip() {
    let v = fixture();
    // Trailing signed column of a compound PK — non-zero `byte_off`.
    let signed = ColumnLocator::Pk {
        byte_off: 4,
        size: 8,
        type_code: tc::I64,
    };
    // Leading unsigned column.
    let unsigned = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    // `bytes` is the at-rest OPK image: big-endian with the sign bit flipped, so
    // `-1` lands just below `i64::MAX` and `i64::MIN` at all-zero. Spelled as
    // literal bytes rather than through the encoder the fixture wrote with,
    // which would only restate that offset 4 holds what was written there.
    assert_eq!(signed.bytes(&v, 0), &[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert_eq!(
        signed.bytes(&v, 2),
        &[0x00; 8],
        "i64::MIN is the bottom of the OPK order"
    );
    assert_eq!(unsigned.bytes(&v, 0), &7u32.to_be_bytes()[..]);

    // `native_le_bytes` undoes it.
    let mut scratch = [0u8; 16];
    assert_eq!(signed.native_le_bytes(&v, 0, &mut scratch), &(-1i64).to_le_bytes()[..]);
    assert_eq!(signed.native_le_bytes(&v, 2, &mut scratch), &i64::MIN.to_le_bytes()[..]);
    assert_eq!(
        unsigned.native_le_bytes(&v, 2, &mut scratch),
        &u32::MAX.to_le_bytes()[..]
    );
    // `native_key` is the zero-extended two's-complement value.
    assert_eq!(signed.native_key(&v, 0), (-1i64) as u64 as u128);
    assert_eq!(signed.native_key(&v, 2), i64::MIN as u64 as u128);
    assert_eq!(unsigned.native_key(&v, 0), 7u128);
    // `route_key` is the widened OPK image — sign-flipped, so -1 lands below 0.
    assert!(signed.route_key(&v, 2) < signed.route_key(&v, 0));
    assert!(signed.route_key(&v, 0) < signed.route_key(&v, 1));
    assert_eq!(unsigned.route_key(&v, 0), 7u128);
    // `decode_i64` fuses the OPK inverse with the widening, so it must land on
    // the same value the two-step read does at both signednesses and widths.
    assert_eq!(signed.decode_i64(&v, 0, gnitz_wire::FixedInt::I64), -1);
    assert_eq!(signed.decode_i64(&v, 2, gnitz_wire::FixedInt::I64), i64::MIN);
    assert_eq!(unsigned.decode_i64(&v, 2, gnitz_wire::FixedInt::U32), u32::MAX as i64);
}

/// `encode_opk_promoted` exists so that index-key projection and join-key
/// repartitioning emit byte-identical keys for one logical value. The two reach
/// it through *different* arms — a PK column re-encodes an OPK image, a payload
/// column encodes native LE — so the property only holds if both arms agree, and
/// nothing else in the suite drives them against each other.
#[test]
fn a_promoted_key_is_the_same_bytes_whichever_arm_encodes_it() {
    // One logical U32 value in a PK column and in a payload column of the same
    // row, plus a wider payload column to promote against.
    let mut v = TestView::new(3, 4);
    assert_eq!(v.push_col(4), 0);
    let vals = [0u32, 7, u32::MAX];
    for (row, &x) in vals.iter().enumerate() {
        v.set_pk_col(row, 0, &x.to_le_bytes(), tc::U32);
        v.set_payload(row, 0, &x.to_le_bytes());
    }
    let from_pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    let from_payload = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::U32,
    };

    let mut prev: Option<[u8; 8]> = None;
    for row in 0..3 {
        // Identity when the target already is the column's own type.
        let (mut a, mut b) = ([0u8; 4], [0u8; 4]);
        from_pk.encode_opk_promoted(&v, row, tc::U32, &mut a);
        from_payload.encode_opk_promoted(&v, row, tc::U32, &mut b);
        assert_eq!(a, b, "row {row}: the two arms disagree at the source width");
        assert_eq!(a, from_pk.bytes(&v, row), "identity promotion must copy the OPK image");

        // Promoted to a wider signed target, the arms must still agree...
        let (mut a, mut b) = ([0u8; 8], [0u8; 8]);
        from_pk.encode_opk_promoted(&v, row, tc::I64, &mut a);
        from_payload.encode_opk_promoted(&v, row, tc::I64, &mut b);
        assert_eq!(a, b, "row {row}: the two arms disagree after promotion");
        // ...and the encoding must stay order-preserving, which is what lets a
        // promoted key be compared with `memcmp` against an unpromoted one.
        if let Some(p) = prev {
            assert!(p < a, "row {row}: promotion is not order-preserving");
        }
        prev = Some(a);
    }
}

/// `cmp_non_null` is the ordering the reduce sort and the scan specs read
/// through, and it dispatches on the arm: a PK column compares its OPK bytes,
/// a payload column its native value.
#[test]
fn cmp_non_null_orders_both_arms_by_logical_value() {
    let v = fixture();
    let pk_signed = ColumnLocator::Pk {
        byte_off: 4,
        size: 8,
        type_code: tc::I64,
    };
    // Rows carry -1, 0, i64::MIN in the trailing PK column.
    assert_eq!(pk_signed.cmp_non_null(&v, 2, &v, 0), Ordering::Less, "i64::MIN < -1");
    assert_eq!(pk_signed.cmp_non_null(&v, 0, &v, 1), Ordering::Less, "-1 < 0");
    assert_eq!(pk_signed.cmp_non_null(&v, 1, &v, 1), Ordering::Equal);

    // Payload slot 2 holds the row index, so it is already ascending.
    let payload = ColumnLocator::Payload {
        slot: 2,
        size: 8,
        type_code: tc::U64,
    };
    assert_eq!(payload.cmp_non_null(&v, 0, &v, 2), Ordering::Less);
    assert_eq!(payload.cmp_non_null(&v, 2, &v, 0), Ordering::Greater);
}

#[test]
fn payload_locator_reads_are_verbatim_native_le() {
    let v = fixture();
    let narrow = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::I32,
    };
    let wide = ColumnLocator::Payload {
        slot: 1,
        size: 16,
        type_code: tc::U128,
    };
    assert_eq!(narrow.bytes(&v, 1), &(-3i32).to_le_bytes()[..]);
    let mut scratch = [0u8; 16];
    assert_eq!(narrow.native_le_bytes(&v, 1, &mut scratch), &(-3i32).to_le_bytes()[..]);
    assert_eq!(narrow.native_key(&v, 1), (-3i32) as u32 as u128);
    // A 16-byte cell reads all 16 bytes, not a zero-extended low 8.
    assert_eq!(wide.bytes(&v, 2), &(1u128 << 100).to_le_bytes()[..]);
    assert_eq!(wide.native_key(&v, 2), 1u128 << 100);
    assert_eq!(wide.route_key(&v, 2), 1u128 << 100);
}

#[test]
fn is_null_reads_the_addressed_slot_bit() {
    let mut v = fixture();
    v.set_null(1, 2);
    let slot0 = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::I32,
    };
    let slot2 = ColumnLocator::Payload {
        slot: 2,
        size: 8,
        type_code: tc::U64,
    };
    let pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    assert!(slot2.is_null(&v, 1));
    assert!(!slot2.is_null(&v, 0));
    assert!(!slot0.is_null(&v, 1), "a set bit at slot 2 must not read as slot 0");
    assert!(!pk.is_null(&v, 1), "PK columns are never null");

    // `native_key_opt` is that gate and `native_key` in one: no key for a NULL
    // payload cell, and never `None` on a PK column.
    assert_eq!(slot2.native_key_opt(&v, 1), None);
    assert_eq!(slot2.native_key_opt(&v, 0), Some(slot2.native_key(&v, 0)));
    assert_eq!(pk.native_key_opt(&v, 1), Some(pk.native_key(&v, 1)));
}

/// `order_bits`' integer half against an independent oracle: the OPK promotion
/// the rest of the engine keys on. `encode_pk_column_promoted` into the type's own
/// index key type is the same total order in big-endian bytes and has its own
/// tests, so this pins the AVI's stored byte format without restating it.
#[test]
fn order_bits_matches_the_opk_promotion_on_both_arms() {
    fn oracle(native_le: &[u8], type_code: u8) -> u64 {
        let mut key = [0u8; 8];
        let target = gnitz_wire::index_key_type(type_code).unwrap();
        gnitz_wire::encode_pk_column_promoted(native_le, type_code, target, &mut key);
        u64::from_be_bytes(key)
    }

    // (FixedInt, type code, the values to check as raw native-LE u64s.)
    let mut cases: Vec<(FixedInt, u8, Vec<u64>)> = vec![
        (FixedInt::U8, tc::U8, (0..=u8::MAX).map(u64::from).collect()),
        (FixedInt::I8, tc::I8, (0..=u8::MAX).map(u64::from).collect()),
        (FixedInt::U16, tc::U16, (0..=u16::MAX).map(u64::from).collect()),
        (FixedInt::I16, tc::I16, (0..=u16::MAX).map(u64::from).collect()),
    ];
    // Edges plus a deterministic sweep for the widths exhaustion cannot reach.
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for (fi, type_code) in [
        (FixedInt::U32, tc::U32),
        (FixedInt::I32, tc::I32),
        (FixedInt::U64, tc::U64),
        (FixedInt::I64, tc::I64),
    ] {
        let mask = u64::MAX >> (64 - 8 * fi.width());
        let mut vals = vec![0, 1, mask, mask >> 1, (mask >> 1) + 1];
        vals.extend((0..64).map(|_| next() & mask));
        cases.push((fi, type_code, vals));
    }

    for (fi, type_code, vals) in cases {
        let w = fi.width();
        let mut v = TestView::new(vals.len(), w);
        assert_eq!(v.push_col(w), 0);
        for (row, &x) in vals.iter().enumerate() {
            v.set_pk_col(row, 0, &x.to_le_bytes()[..w], type_code);
            v.set_payload(row, 0, &x.to_le_bytes()[..w]);
        }
        let from_pk = ColumnLocator::Pk {
            byte_off: 0,
            size: w as u8,
            type_code,
        };
        let from_payload = ColumnLocator::Payload {
            slot: 0,
            size: w as u8,
            type_code,
        };
        let kind = ScalarKind::Int(fi);
        for (row, &x) in vals.iter().enumerate() {
            let want = oracle(&x.to_le_bytes()[..w], type_code);
            assert_eq!(from_pk.order_bits(&v, row, kind), want, "{fi:?} pk arm, value {x:#x}");
            assert_eq!(
                from_payload.order_bits(&v, row, kind),
                want,
                "{fi:?} payload arm, value {x:#x}",
            );
        }
    }
}

/// `order_bits`' float half round-trips through `ScalarKind::order_inverse` and
/// orders by `total_cmp`, over the values only floats have: ±0.0, ±NaN and both
/// infinities. A float is never a PK column, so only the payload arm exists.
#[test]
fn order_bits_gives_floats_the_total_order() {
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
    for (kind, type_code, w) in [(ScalarKind::F32, tc::F32, 4), (ScalarKind::F64, tc::F64, 8)] {
        let cells: Vec<[u8; 8]> = FLOATS
            .iter()
            .map(|&f| {
                if w == 4 {
                    ((f as f32).to_bits() as u64).to_le_bytes()
                } else {
                    f.to_bits().to_le_bytes()
                }
            })
            .collect();
        let mut v = TestView::new(cells.len(), 8);
        assert_eq!(v.push_col(w), 0);
        for (row, cell) in cells.iter().enumerate() {
            v.set_payload(row, 0, &cell[..w]);
        }
        let loc = ColumnLocator::Payload {
            slot: 0,
            size: w as u8,
            type_code,
        };
        for (a, ca) in cells.iter().enumerate() {
            let enc = loc.order_bits(&v, a, kind);
            assert_eq!(
                &kind.order_inverse(enc).to_le_bytes()[..w],
                &ca[..w],
                "{kind:?}: order_inverse must recover the value's own bits",
            );
            for (b, cb) in cells.iter().enumerate() {
                assert_eq!(
                    enc.cmp(&loc.order_bits(&v, b, kind)),
                    gnitz_wire::cmp_typed_le(&ca[..w], &cb[..w], type_code),
                    "{kind:?}: order disagrees with cmp_typed_le for {ca:02x?} vs {cb:02x?}",
                );
            }
        }
    }
}
