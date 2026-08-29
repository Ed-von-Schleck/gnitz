//! AVI tests: the order-preserving value codec, and the equality of a PK-source
//! and a payload-source aggregate's encoded image.

use super::*;

/// The value's native bits as `decode_ordered` recovers them: sign-extended for
/// signed ints, zero-extended for unsigned, IEEE bits for F64, and the F64
/// promotion for F32 (the accumulator's slot type).
fn native_bits(le: &[u8], tc: TypeCode) -> u64 {
    match tc {
        TypeCode::F64 => u64::from_le_bytes(le.try_into().unwrap()),
        TypeCode::F32 => f64::to_bits(f32::from_bits(u32::from_le_bytes(le[..4].try_into().unwrap())) as f64),
        _ if gnitz_wire::is_signed_int(tc as u8) => gnitz_wire::read_signed_exact(le) as u64,
        _ => gnitz_wire::read_unsigned_exact(le),
    }
}

/// `encode_ordered` is the *encoding* form of the scalar total order whose
/// *comparison* form is `gnitz_wire::cmp_typed_le`; the AVI's correctness is
/// their agreement, since an ascending cursor walk over encoded keys must yield
/// the extremum `cmp_typed_le` would pick. Checked for every pair of every
/// order-encodable type in both directions, together with the `for_max`
/// inversion (which is what makes the ascending walk yield MAX first) and the
/// `decode_ordered` round-trip both ways.
fn assert_codec(tc: TypeCode, vals: &[[u8; 8]]) {
    let w = SchemaColumn::new(tc as u8, 0).size() as usize;
    for a in vals {
        let (min, max) = (encode_ordered(&a[..w], tc, false), encode_ordered(&a[..w], tc, true));
        assert_eq!(max, !min, "{tc:?}: for_max must invert the order");
        assert_eq!(decode_ordered(min, tc), native_bits(&a[..w], tc), "{tc:?}: round-trip");
        assert_eq!(
            decode_ordered(!max, tc),
            native_bits(&a[..w], tc),
            "{tc:?}: MAX round-trip"
        );
        for b in vals {
            assert_eq!(
                min.cmp(&encode_ordered(&b[..w], tc, false)),
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
/// sign-flipped), so the AVI population reads them through
/// `ColumnLocator::native_le_bytes` before order-encoding. That read is what
/// makes a PK-source aggregate encode identically to the same value in a
/// payload column; without it the image is byte-swapped, and a byte-swap
/// sensitive pair like 1/256 inverts.
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

        let mut batch = Batch::with_capacity(schema, vals.len());
        for (i, &v) in vals.iter().enumerate() {
            // `extend_pk_opk` truncates each u128 to the column width, so a
            // negative `v as u128` still packs the right two's-complement bytes.
            batch.extend_pk_opk(&schema, &[i as u128, v as u128]);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(payload_idx, &(v as u128).to_le_bytes()[..width]);
            batch.count += 1;
        }
        let mb = batch.as_mem_batch();

        // The AVI value image exactly as `avi_batch` builds it.
        let encode_at = |col: usize, row: usize, for_max: bool| {
            let mut scratch = [0u8; 16];
            encode_ordered(schema.locate(col).native_le_bytes(&mb, row, &mut scratch), tc, for_max)
        };
        for for_max in [false, true] {
            for (i, &v) in vals.iter().enumerate() {
                assert_eq!(
                    encode_at(1, i, for_max),
                    encode_at(2, i, for_max),
                    "{tc:?} v={v} for_max={for_max}",
                );
            }
        }
    }
}
