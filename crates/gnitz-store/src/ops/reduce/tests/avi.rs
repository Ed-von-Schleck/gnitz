//! AVI tests: the order-preserving value image, and the equality of a PK-source
//! and a payload-source aggregate's encoded image.

use super::*;
use crate::schema::{ColumnLocator, TypeCode};
use crate::storage::MemBatch;
use gnitz_wire::ScalarKind;

/// One-row batch holding `le` (the value's native little-endian bytes) in a
/// nullable payload column of type `tc`, plus the locator addressing it. The
/// value image is read through the same accessor the AVI's write side uses.
fn payload_row(tc: TypeCode, le: &[u8]) -> (Batch, ColumnLocator) {
    let schema = SchemaDescriptor::new(
        &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc as u8, 0)],
        &[0],
    );
    let mut b = Batch::with_capacity(&schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &le[..schema.columns[1].size() as usize]);
    b.count += 1;
    (b, schema.locate(1))
}

/// `order_bits` is the *encoding* form of the scalar total order whose
/// *comparison* form is `gnitz_wire::cmp_typed_le`; the AVI's correctness is
/// their agreement, since an ascending cursor walk over encoded keys must yield
/// the extremum `cmp_typed_le` would pick. Checked for every pair of every
/// scalar type in both directions, together with the `order_inverse` round-trip.
fn assert_codec(tc: TypeCode, vals: &[[u8; 8]]) {
    let kind = ScalarKind::from_type_code(tc).unwrap();
    let w = SchemaColumn::new(tc as u8, 0).size() as usize;
    let bits = |v: &[u8; 8]| {
        let (b, loc) = payload_row(tc, v);
        loc.order_bits(&b.as_mem_batch(), 0, kind)
    };
    for a in vals {
        let enc = bits(a);
        assert_eq!(
            kind.order_inverse(enc).to_le_bytes()[..w],
            a[..w],
            "{tc:?}: order_inverse must recover the value's own bytes",
        );
        for b in vals {
            assert_eq!(
                enc.cmp(&bits(b)),
                gnitz_wire::cmp_typed_le(&a[..w], &b[..w], tc as u8),
                "{tc:?}: encode order disagrees with cmp_typed_le for {a:02x?} vs {b:02x?}",
            );
        }
    }
}

#[test]
fn order_codec_matches_cmp_typed_le_and_round_trips() {
    let pad = |v: u64| v.to_le_bytes();
    for (tc, vals) in [
        (TypeCode::U8, vec![0u64, 1, 0x7f, 0x80, 0xff]),
        (TypeCode::U16, vec![0, 1, 256, 0x7fff, 0x8000, 0xffff]),
        (TypeCode::U32, vec![0, 1, 0x7fff_ffff, 0x8000_0000, u32::MAX as u64]),
        (TypeCode::U64, vec![0, 1, i64::MAX as u64, 1u64 << 63, u64::MAX]),
        (TypeCode::I8, vec![-128i64 as u64, -1i64 as u64, 0, 1, 127]),
        (TypeCode::I16, vec![-32768i64 as u64, -1i64 as u64, 0, 1, 32767]),
        (
            TypeCode::I32,
            vec![i32::MIN as i64 as u64, -1i64 as u64, 0, 1, i32::MAX as u64],
        ),
        (
            TypeCode::I64,
            vec![i64::MIN as u64, -1i64 as u64, 0, 1, i64::MAX as u64],
        ),
    ] {
        assert_codec(tc, &vals.into_iter().map(pad).collect::<Vec<_>>());
    }

    // Floats carry the cases the integer types cannot: NaN (which has a defined
    // position under `total_cmp` and none under `<`), and −0.0 vs +0.0
    // (byte-distinct, numerically equal, ordered by `total_cmp`).
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
    assert_codec(TypeCode::F64, &FLOATS.map(|v| v.to_bits().to_le_bytes()));
    assert_codec(
        TypeCode::F32,
        &FLOATS.map(|v| ((v as f32).to_bits() as u64).to_le_bytes()),
    );
}

/// A PK aggregate column's at-rest bytes are its OPK window (big-endian,
/// sign-flipped), so `order_bits` inverts that window rather than reading it
/// raw. That inversion is what makes a PK-source aggregate encode identically to
/// the same value in a payload column; without it the image is byte-swapped, and
/// a byte-swap sensitive pair like 1/256 inverts.
///
/// Schema `[U64 a (pk), <tc> b (pk), <tc> c (payload)]`, with the same native
/// value in `b` and `c`, and `a` the row index so every PK stays distinct.
#[test]
fn pk_source_and_payload_source_encode_identically() {
    for (tc, vals) in [
        (TypeCode::U16, vec![1i128, 256, 0x0102, 0xFFFF]),
        (TypeCode::U64, vec![1, 256, 0x0100_0000, (u32::MAX as i128) + 1]),
        (TypeCode::I32, vec![i32::MIN as i128, -100, -1, 0, 1_000_000]),
        (TypeCode::I64, vec![i64::MIN as i128, -1, 0, 1, i64::MAX as i128]),
    ] {
        let kind = ScalarKind::from_type_code(tc).unwrap();
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(tc as u8, 0),
                SchemaColumn::new(tc as u8, 0),
            ],
            &[0, 1],
        );
        let width = schema.columns[2].size() as usize;
        let payload_idx = schema.try_payload_idx(2).unwrap();

        let mut batch = Batch::with_capacity(&schema, vals.len());
        for (i, &v) in vals.iter().enumerate() {
            // `extend_pk_opk` truncates each u128 to the column width, so a
            // negative `v as u128` still packs the right two's-complement bytes.
            batch.extend_pk_opk(&[i as u128, v as u128]);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(payload_idx, &(v as u128).to_le_bytes()[..width]);
            batch.count += 1;
        }
        let mb = batch.as_mem_batch();

        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(
                schema.locate(1).order_bits(&mb, i, kind),
                schema.locate(2).order_bits(&mb, i, kind),
                "{tc:?} v={v}",
            );
        }
    }
}

/// The AVI's own convention on top of the shared value image: a MAX ordinal
/// stores the bitwise complement, so the index's ascending walk yields that
/// ordinal's extreme first. Checked against the MIN image of the same row.
#[test]
fn for_max_inverts_the_value_image() {
    let (b, loc) = payload_row(TypeCode::I64, &(-7i64).to_le_bytes());
    let mb: MemBatch = b.as_mem_batch();
    let kind = ScalarKind::Int(gnitz_wire::FixedInt::I64);
    assert_eq!(
        scalar_image(&loc, kind, true, &mb, 0),
        !scalar_image(&loc, kind, false, &mb, 0)
    );
}
