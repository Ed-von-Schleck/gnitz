//! Order-image tests: the byte string whose plain lexicographic order is the
//! column's typed order, and its inverse.

use super::{
    append_bytes_image, int16_image, order_bits, order_inverse, wide_native_of_image, write_image_slot, WideKind,
};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaFacts;
use crate::schema::{SchemaColumn, TypeCode};
use crate::test_support::{pk_payload_schema, u64_pk_schema};
use gnitz_wire::{FixedInt, ScalarKind};

/// `vals` (native bit patterns) as one integer column of type `tc`, one row per
/// value: the PK column of a batch, or its sole payload column.
fn column_batch(tc: TypeCode, vals: &[u128], as_pk: bool) -> Batch {
    let schema = if as_pk {
        pk_payload_schema(&[tc])
    } else {
        u64_pk_schema(SchemaColumn::new(tc, false))
    };
    let mut b = BatchBuilder::new(schema);
    for (i, &v) in vals.iter().enumerate() {
        if as_pk {
            b.begin_row_opk(&[v], 1);
            b.put_int(0);
        } else {
            b.begin_row(i as u128, 1);
            b.put_int(v);
        }
        b.end_row();
    }
    b.finish()
}

/// The [`int16_image`] of every row of the column `vals` fill, asserting a PK
/// source and a payload source agree on each.
fn int16_images(tc: TypeCode, vals: &[i128], invert: bool) -> Vec<[u8; 16]> {
    let natives: Vec<u128> = vals.iter().map(|&v| v as u128).collect();
    let (pk, payload) = (column_batch(tc, &natives, true), column_batch(tc, &natives, false));
    let (pk_loc, payload_loc) = (pk.schema().locate(0), payload.schema().locate(1));
    let (pk, payload) = (pk.as_mem_batch(), payload.as_mem_batch());
    (0..vals.len())
        .map(|row| {
            let image = int16_image(&pk_loc, invert, &pk, row);
            assert_eq!(
                image,
                int16_image(&payload_loc, invert, &payload, row),
                "{tc:?} max={invert} {:?}: PK and payload images differ",
                vals[row]
            );
            image
        })
        .collect()
}

/// Each of `images` decodes back to its `natives` entry and orders as it does,
/// reversed when `invert`, whole and truncated to a key slot.
fn assert_images_order(kind: WideKind, invert: bool, natives: &[&[u8]], images: &[Vec<u8>]) {
    for (a, ia) in natives.iter().zip(images) {
        assert_eq!(&*wide_native_of_image(kind, invert, ia), *a, "{kind:?} {a:?}");
        for (b, ib) in natives.iter().zip(images) {
            let want = if invert {
                kind.cmp_native(b, a)
            } else {
                kind.cmp_native(a, b)
            };
            assert_eq!(ia.cmp(ib), want, "{kind:?} max={invert} {a:?} vs {b:?}");
            for width in [8usize, 16] {
                let (mut sa, mut sb) = ([0u8; 16], [0u8; 16]);
                write_image_slot(&mut sa[..width], ia);
                write_image_slot(&mut sb[..width], ib);
                let slot = sa.cmp(&sb);
                assert!(
                    slot == want || slot.is_eq(),
                    "{kind:?} max={invert} slot{width} {a:?} vs {b:?}"
                );
            }
        }
    }
}

/// A wide image orders and round-trips. Byte strings cover NULs, prefixes, and
/// values longer than the slot: the cases prefix-freeness exists for. 16-byte
/// integers are read from a PK column and a payload column alike.
#[test]
fn wide_image_orders_and_round_trips() {
    let strings: Vec<&[u8]> = vec![
        b"",
        b"\0",
        b"\0\0",
        b"a",
        b"a\0",
        b"a\0b",
        b"ab",
        b"abc",
        b"b",
        b"shared-prefix-1",
        b"shared-prefix-10",
        b"shared-prefix-2",
    ];
    let ints = [i128::MIN, -1, 0, 1, i128::MAX, u64::MAX as i128 + 1];
    let int_natives: Vec<[u8; 16]> = ints.iter().map(|v| v.to_le_bytes()).collect();
    let int_natives: Vec<&[u8]> = int_natives.iter().map(|b| &b[..]).collect();
    for invert in [false, true] {
        let images: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                let mut image = Vec::new();
                append_bytes_image(invert, s, &mut image);
                image
            })
            .collect();
        assert_images_order(WideKind::Bytes, invert, &strings, &images);
        for tc in [TypeCode::I128, TypeCode::U128] {
            let images: Vec<Vec<u8>> = int16_images(tc, &ints, invert).iter().map(|i| i.to_vec()).collect();
            assert_images_order(WideKind::Fixed(tc), invert, &int_natives, &images);
        }
    }
}

/// `order_bits`' integer half is the value's index key (`encode_pk_images` at
/// `index_key_type`), on both arms, and `order_inverse` recovers the value from
/// it.
#[test]
fn order_bits_matches_the_opk_promotion_on_both_arms() {
    fn oracle(native: u64, type_code: TypeCode) -> u64 {
        let target = gnitz_wire::index_key_type(type_code).unwrap();
        let image = gnitz_wire::key_image(type_code, native as u128);
        let key = gnitz_wire::encode_pk_images([(type_code, target, image)]);
        u64::from_be_bytes(key.pk_bytes().try_into().unwrap())
    }

    // (FixedInt, type code, the values to check as raw native-LE u64s.)
    let mut cases: Vec<(FixedInt, TypeCode, Vec<u64>)> = vec![
        (FixedInt::U8, TypeCode::U8, (0..=u8::MAX).map(u64::from).collect()),
        (FixedInt::I8, TypeCode::I8, (0..=u8::MAX).map(u64::from).collect()),
        (FixedInt::U16, TypeCode::U16, (0..=u16::MAX).map(u64::from).collect()),
        (FixedInt::I16, TypeCode::I16, (0..=u16::MAX).map(u64::from).collect()),
    ];
    // Edges plus a deterministic sweep for the widths exhaustion cannot reach.
    let mut rng = crate::test_support::Rng::new(0x2545_F491_4F6C_DD1D);
    for (fi, type_code) in [
        (FixedInt::U32, TypeCode::U32),
        (FixedInt::I32, TypeCode::I32),
        (FixedInt::U64, TypeCode::U64),
        (FixedInt::I64, TypeCode::I64),
    ] {
        let mask = u64::MAX >> (64 - 8 * fi.width());
        let mut vals = vec![0, 1, mask, mask >> 1, (mask >> 1) + 1];
        vals.extend((0..64).map(|_| rng.next_u64() & mask));
        cases.push((fi, type_code, vals));
    }

    for (fi, type_code, vals) in cases {
        let w = fi.width();
        let natives: Vec<u128> = vals.iter().map(|&v| v as u128).collect();
        let (pk, payload) = (
            column_batch(type_code, &natives, true),
            column_batch(type_code, &natives, false),
        );
        let (pk_loc, payload_loc) = (pk.schema().locate(0), payload.schema().locate(1));
        let (pk, payload) = (pk.as_mem_batch(), payload.as_mem_batch());
        let kind = ScalarKind::Int(fi);
        for (row, &x) in vals.iter().enumerate() {
            let want = oracle(x, type_code);
            assert_eq!(order_bits(&pk_loc, &pk, row, kind), want, "{fi:?} pk arm, value {x:#x}");
            assert_eq!(
                order_bits(&payload_loc, &payload, row, kind),
                want,
                "{fi:?} payload arm, value {x:#x}",
            );
            assert_eq!(
                order_inverse(kind, want).to_le_bytes()[..w],
                x.to_le_bytes()[..w],
                "{fi:?} order_inverse, value {x:#x}",
            );
        }
    }
}

/// `order_bits`' float half round-trips through `order_inverse` and orders by
/// `total_cmp`, over the values only floats have: ±0.0, ±NaN and both
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
    for (kind, type_code, w) in [(ScalarKind::F32, TypeCode::F32, 4), (ScalarKind::F64, TypeCode::F64, 8)] {
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
        let mut b = BatchBuilder::new(u64_pk_schema(SchemaColumn::new(type_code, false)));
        for (i, &f) in FLOATS.iter().enumerate() {
            b.begin_row(i as u128, 1);
            b.put_float(f);
            b.end_row();
        }
        let b = b.finish();
        let loc = b.schema().locate(1);
        let v = b.as_mem_batch();
        for (a, ca) in cells.iter().enumerate() {
            let enc = order_bits(&loc, &v, a, kind);
            assert_eq!(
                &order_inverse(kind, enc).to_le_bytes()[..w],
                &ca[..w],
                "{kind:?}: order_inverse must recover the value's own bits",
            );
            for (b, cb) in cells.iter().enumerate() {
                assert_eq!(
                    enc.cmp(&order_bits(&loc, &v, b, kind)),
                    gnitz_wire::cmp_col_window(&ca[..w], &[], &cb[..w], &[], type_code),
                    "{kind:?}: order disagrees with cmp_col_window for {ca:02x?} vs {cb:02x?}",
                );
            }
        }
    }
}
