//! Order-image tests: the byte string whose plain lexicographic order is the
//! column's typed order, and its inverse.

use super::{append_bytes_image, int16_image, wide_native_of_image, write_image_slot, WideKind};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaFacts;
use crate::schema::{SchemaColumn, TypeCode};
use crate::test_support::{pk_payload_schema, u64_pk_schema};

/// `vals` (native bit patterns) as one integer column of type `tc`, one row per
/// value: the PK column of a batch, or its sole payload column.
fn column_batch(tc: TypeCode, vals: &[u128], as_pk: bool) -> Batch {
    let schema = if as_pk {
        pk_payload_schema(&[tc])
    } else {
        u64_pk_schema(SchemaColumn::new(tc, false))
    };
    let mut b = BatchBuilder::new(&schema);
    for (i, &v) in vals.iter().enumerate() {
        if as_pk {
            b.begin_row_natives(&[v], 1);
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
