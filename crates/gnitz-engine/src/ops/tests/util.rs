use super::*;
use crate::schema::{type_code, SchemaColumn};
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// Group key
// ---------------------------------------------------------------------------

/// The canonical single-column group key must be the value's OPK image (native
/// for unsigned, sign-flipped for signed) and must be the *same* image whether
/// the column is a PK column or a payload column. A distributed join routes one
/// side by its PK and the other by a payload FK; were the two to disagree, equal
/// keys would land on different workers and the join would drop rows.
#[test]
fn single_col_group_key_is_the_opk_image_from_either_side() {
    // The group key of the value `le` held in column 1 of `[U64 pk, <tc>]`,
    // with column 1 either a second PK column or the sole payload column.
    let key = |tc: u8, le: &[u8], col1_is_pk: bool| -> u128 {
        let cols = [SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)];
        let schema = SchemaDescriptor::new(&cols, if col1_is_pk { &[0, 1] } else { &[0] });
        let mut b = Batch::with_capacity(schema, 1);
        if col1_is_pk {
            let mut native = [0u8; 16];
            native[..le.len()].copy_from_slice(le);
            b.extend_pk_opk(&schema, &[0, u128::from_le_bytes(native)]);
        } else {
            b.extend_pk(0u128);
        }
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        if !col1_is_pk {
            b.extend_col(schema.try_payload_idx(1).unwrap(), le);
        }
        b.count += 1;
        GroupKeyCols::new(&schema, &[1]).key_row(&b.as_mem_batch(), 0)
    };

    for (tc, vals) in [
        (type_code::I32, vec![1i128, -1, 100, i32::MIN as i128, i32::MAX as i128]),
        (type_code::I64, vec![0, -1, i64::MIN as i128, i64::MAX as i128]),
        (type_code::U16, vec![0, 1, 0xBEEF, u16::MAX as i128]),
        (type_code::U64, vec![0, 1, u64::MAX as i128]),
    ] {
        let width = SchemaColumn::new(tc, 0).size() as usize;
        for v in vals {
            let le = &(v as u128).to_le_bytes()[..width];
            // Signed columns are sign-flipped into the OPK image; unsigned ones
            // pass through, so the OPK image is the native value.
            let want = if gnitz_wire::is_signed_int(tc) {
                (v + (1i128 << (8 * width - 1))) as u128
            } else {
                v as u128
            };
            assert_eq!(key(tc, le, true), want, "PK-column key for {tc} v={v}");
            assert_eq!(key(tc, le, false), want, "payload-column key for {tc} v={v}");
        }
    }
}

// ---------------------------------------------------------------------------
// Order-preserving aggregate-value codec (AVI keys)
// ---------------------------------------------------------------------------

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

        // The AVI value image exactly as `op_integrate_with_indexes` builds it.
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
