//! Order-image tests: the byte string whose plain lexicographic order is the
//! column's typed order, and its inverse.

use super::{ImageCol, order_bits, scalar_native_of_image, wide_native, wide_native_of_image};
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{STRS, keyed_schema, le_cell, u64_pk_schema};
use gnitz_foundation::rng::PATTERNS;
use gnitz_wire::{FixedInt, ScalarKind, TypeCode, ValueForm, image_mask};

/// Over a PK and a payload column of every image kind: a row's image decodes
/// back to its value, a wide one orders as the value does, and `invert`
/// reverses every order; a 16-byte integer images alike from the PK and the
/// payload. `write_slot` leaves a slot holding the image's leading bytes,
/// zero-padded whatever it held, and hands a byte string's whole image to
/// `whole`. The strings hold NULs, prefixes of one another and values longer
/// than a slot: the cases prefix-freeness exists for.
#[test]
fn an_image_orders_round_trips_and_slots_as_its_value() {
    use TypeCode::{F32, I64, I128, String, U64, U128};
    const INTS: [i128; 6] = [i128::MIN, -1, 0, 1, i128::MAX, u64::MAX as i128 + 1];
    let c = |tc| SchemaColumn::new(tc, false);
    let schema = keyed_schema(&[U64, I128], &[c(I128), c(U128), c(I64), c(F32), c(String)]);
    let rows = 2 * STRS.len();
    let mut b = BatchBuilder::new(&schema);
    for r in 0..rows {
        let int = INTS[r % INTS.len()] as u128;
        b.begin_row_natives(&[r as u128, int], 1);
        b.put_int(int);
        b.put_int(int);
        b.put_int((r as i64 - 7) as u128);
        b.put_int(u128::from(((r as f32 - 7.5) * 1.5).to_bits()));
        b.put_string(STRS[r % STRS.len()]);
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    let images = |ci: usize, invert: bool| -> Vec<Vec<u8>> {
        let col = ImageCol::new(schema.locate(ci), invert);
        (0..rows)
            .map(|r| {
                let mut out = vec![0x5A];
                col.append(&mb, r, &mut out);
                out.split_off(1)
            })
            .collect()
    };
    for invert in [false, true] {
        assert_eq!(images(1, invert), images(2, invert), "I128 from the PK and the payload");
    }
    for ci in 0..schema.num_columns() {
        let loc = schema.locate(ci);
        let (plain, inverted) = (images(ci, false), images(ci, true));
        for (a, b) in (0..rows).flat_map(|a| (0..rows).map(move |b| (a, b))) {
            assert_eq!(
                inverted[a].cmp(&inverted[b]),
                plain[a].cmp(&plain[b]).reverse(),
                "column {ci}"
            );
        }
        for (invert, images) in [(false, &plain), (true, &inverted)] {
            let col = ImageCol::new(loc, invert);
            let what = format!("column {ci} invert={invert}");
            let mut scratch = [[0u8; 16]; 2];
            for (r, image) in images.iter().enumerate() {
                match col.kind {
                    ValueForm::Scalar(kind) => {
                        let native = le_cell(loc.native_le_bytes(&mb, r, &mut scratch[0])) as u64;
                        let bits = u64::from_be_bytes(image[..].try_into().unwrap());
                        assert_eq!(scalar_native_of_image(kind, invert, bits), native, "{what} row {r}");
                    }
                    ValueForm::Wide(kind) => {
                        let [sa, sb] = &mut scratch;
                        let native = wide_native(&loc, kind, &mb, r, sa);
                        let mut v = image.clone();
                        let n = wide_native_of_image(kind, invert, &mut v);
                        assert_eq!(&v[..n], &*native, "{what} row {r}");
                        if !invert {
                            for (o, other) in images.iter().enumerate() {
                                let want = kind.cmp_native(&native, &wide_native(&loc, kind, &mb, o, sb));
                                assert_eq!(image.cmp(other), want, "{what} rows {r} and {o}");
                            }
                        }
                    }
                }
                for &width in if col.is_wide() { &[16][..] } else { &[8, 16] } {
                    let (mut slot, mut whole) = ([0xAAu8; 16], vec![0x5A; 3]);
                    col.write_slot(&mb, r, &mut slot[..width], &mut whole);
                    let take = image.len().min(width);
                    assert_eq!(&slot[..take], &image[..take], "{what} row {r} slot {width}");
                    assert!(slot[take..width].iter().all(|&b| b == 0), "{what} row {r} slot {width}");
                    let held: &[u8] = if col.fits_slot() { &[] } else { image };
                    assert_eq!((&whole[..3], &whole[3..]), (&[0x5A; 3][..], held), "{what} row {r}");
                }
            }
        }
    }
}

/// `write_slots` writes, into each key's slot window, the bytes `write_slot`
/// writes for the same row, over a run list with gaps, for a scalar PK column
/// between two others, scalar payload columns of every kind and both slot
/// widths, and leaves every other key byte as it found it. A wide kind writes
/// nothing.
#[test]
fn write_slots_matches_write_slot_per_row() {
    use TypeCode::{F32, F64, I16, I32, I64, String, U8, U64, U128};
    let c = |tc| SchemaColumn::new(tc, false);
    let schema = SchemaDescriptor::new(
        &[
            c(U64),
            c(I32),
            c(I16),
            c(U8),
            c(I64),
            c(F64),
            c(F32),
            c(U128),
            c(String),
            c(U8),
        ],
        &[0, 1, 9],
    );
    let rows = 40;
    let mut b = BatchBuilder::new(&schema);
    for r in 0..rows {
        let v = (r as i64 - 20) * 977;
        b.begin_row_natives(&[r as u128, v as u128, r as u128], 1);
        b.put_int(v as u128);
        b.put_int((r as u128 * 7) % 256);
        b.put_int((v * 1_000_003) as u128);
        b.put_int(u128::from((v as f64 * -0.25).to_bits()));
        b.put_int(u128::from((v as f32 * 1.5).to_bits()));
        b.put_int(v as u128);
        b.put_string(STRS[r % STRS.len()]);
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    let runs = [(0, 3), (5, 6), (9, 30), (33, 40)];
    let live: Vec<usize> = runs.iter().flat_map(|&(s, e)| s..e).collect();
    for ci in 1..schema.num_columns() {
        for invert in [false, true] {
            let col = ImageCol::new(schema.locate(ci), invert);
            for slot in [8, 16] {
                let (at, stride) = (3, 3 + slot + 2);
                let mut keys = vec![0xAAu8; live.len() * stride];
                let wrote = col.write_slots(&mb, &runs, &mut keys, stride, at, slot);
                let what = format!("column {ci} invert={invert} slot {slot}");
                assert_eq!(wrote, !col.is_wide(), "{what}");
                if !wrote {
                    assert!(keys.iter().all(|&b| b == 0xAA), "{what}");
                    continue;
                }
                for (key, &r) in keys.chunks_exact(stride).zip(&live) {
                    let mut want = vec![0xAAu8; stride];
                    col.write_slot(&mb, r, &mut want[at..at + slot], &mut Vec::new());
                    assert_eq!(key, &want[..], "{what} row {r}");
                }
            }
        }
    }
}

/// `order_bits` is the order image of the value a scalar column holds: a float
/// payload slot's bits, and an integer read from a key column and from a payload
/// slot holding the same values.
#[test]
fn order_bits_reads_a_scalar_as_its_image() {
    const FLOATS: [f64; 3] = [-1.5, 0.0, f64::NAN];
    for (kind, tc) in [(ScalarKind::F32, TypeCode::F32), (ScalarKind::F64, TypeCode::F64)] {
        let schema = u64_pk_schema(&[SchemaColumn::new(tc, false)]);
        let bits = |f: f64| match kind {
            ScalarKind::F32 => (f as f32).to_bits() as u64,
            _ => f.to_bits(),
        };
        let mut b = BatchBuilder::new(&schema);
        for (row, &f) in FLOATS.iter().enumerate() {
            b.begin_row(row as u128 + 1, 1).put_float(f);
        }
        let b = b.finish();
        for (row, &f) in FLOATS.iter().enumerate() {
            assert_eq!(
                order_bits(&schema.locate(1), &b, row, kind),
                kind.order_image(bits(f)),
                "{tc} {f}"
            );
        }
    }
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let Some(fi) = FixedInt::from_type_code(tc) else {
            continue;
        };
        // Key column 1 sits at byte 2, behind a U16 key column.
        let schema = keyed_schema(&[TypeCode::U16, tc], &[SchemaColumn::new(tc, true)]);
        let native = |p: u128| p & image_mask(tc.wire_stride());
        let mut b = BatchBuilder::new(&schema);
        for (row, &p) in PATTERNS.iter().enumerate() {
            b.begin_row_natives(&[row as u128 + 1, native(p)], 1).put_int(native(p));
        }
        let b = b.finish();
        let kind = ScalarKind::Int(fi);
        for loc in [schema.locate(1), schema.locate(2)] {
            for (row, &p) in PATTERNS.iter().enumerate() {
                let le = native(p).to_le_bytes();
                assert_eq!(
                    order_bits(&loc, &b, row, kind),
                    kind.order_image(fi.decode_le_i64(&le[..tc.wire_stride()]) as u64),
                    "{tc} {loc:?} row {row}"
                );
            }
        }
    }
}
