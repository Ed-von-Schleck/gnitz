use super::super::util::{decode_ordered, encode_ordered};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::Batch;

fn decode_i32(enc: u64, for_max: bool) -> i32 {
    decode_ordered(if for_max { !enc } else { enc }, TypeCode::I32) as i32
}

// A signed aggregate (PK or payload) must encode order-preservingly so the
// ascending AVI cursor walk yields MIN first, and decode back to the value.
#[test]
fn i32_encoding_is_order_preserving_and_round_trips() {
    let vals: [i32; 5] = [i32::MIN, -100, -1, 0, 1_000_000];
    let enc: Vec<u64> = vals
        .iter()
        .map(|v| encode_ordered(&v.to_le_bytes(), TypeCode::I32, false))
        .collect();
    for w in enc.windows(2) {
        assert!(w[0] < w[1], "ascending i32 must encode to ascending u64");
    }
    for (v, &e) in vals.iter().zip(enc.iter()) {
        assert_eq!(decode_i32(e, false), *v, "round-trip i32");
    }
}

// for_max inverts the order so the ascending walk yields MAX first.
#[test]
fn for_max_inverts_order() {
    let lo = encode_ordered(&(-5i32).to_le_bytes(), TypeCode::I32, true);
    let hi = encode_ordered(&100i32.to_le_bytes(), TypeCode::I32, true);
    assert!(lo > hi, "for_max: larger value must encode smaller");
    assert_eq!(decode_i32(hi, true), 100);
}

#[test]
fn f32_encoding_is_order_preserving_and_round_trips() {
    let vals: [f32; 5] = [-2.5, -0.5, 0.0, 0.5, 2.5];
    let enc: Vec<u64> = vals
        .iter()
        .map(|v| encode_ordered(&v.to_bits().to_le_bytes(), TypeCode::F32, false))
        .collect();
    for w in enc.windows(2) {
        assert!(w[0] < w[1], "ascending f32 must encode to ascending u64");
    }
    // The decode widens to F64 bits (the accumulator's slot type), so compare
    // against the promoted value rather than the F32 pattern.
    for (v, &e) in vals.iter().zip(enc.iter()) {
        assert_eq!(
            decode_ordered(e, TypeCode::F32),
            f64::to_bits(*v as f64),
            "round-trip {v}"
        );
    }
}

#[test]
fn unsigned_encoding_is_raw_value() {
    let v: u32 = 0xABCD_1234;
    let enc = encode_ordered(&v.to_le_bytes(), TypeCode::U32, false);
    assert_eq!(enc, v as u64);
}

// PK-source aggregate columns: the AVI population reads a row's aggregate
// value through `ColumnLocator::native_le_bytes`, which must OPK-decode a PK
// column's at-rest bytes to native LE before order-encoding. Without the
// decode the image is byte-swapped: order breaks (a byte-swap-sensitive pair
// like 1/256 inverts) and the value no longer round-trips.
//
// Schema `[U64 a (pk), <tc> b (pk), <tc> c (payload)]`. Each row stores the
// same native value in the PK-source column `b` and the payload column `c`,
// so the read+encode must produce an identical image from either — a PK
// aggregate encodes exactly like the same value in a payload column. `a` is
// the row index, keeping every PK distinct. `vals` are native values (signed
// ones as their `i128` value) in ascending order.
fn check_pk_source(tc: TypeCode, vals: &[i128]) {
    let signed = gnitz_wire::is_signed_int(tc as u8);
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(tc as u8, 0),
            SchemaColumn::new(tc as u8, 0),
        ],
        &[0, 1],
    );
    let cs = schema.columns[2].size() as usize;
    let payload_idx = schema.try_payload_idx(2).unwrap();

    let mut batch = Batch::with_capacity(schema, vals.len().max(1));
    for (i, &v) in vals.iter().enumerate() {
        // OPK-encode the PK region (sign-flip for a signed `b`); `opk_pk`
        // truncates each u128 to the column width, so a negative `v as u128`
        // still packs the correct two's-complement LE bytes.
        batch.extend_pk_opk(&schema, &[i as u128, v as u128]);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(payload_idx, &(v as u128).to_le_bytes()[..cs]);
        batch.count += 1;
    }
    let mb = batch.as_mem_batch();

    // The AVI value image exactly as `op_integrate_with_indexes` builds it.
    let encode_at = |loc: &crate::schema::ColumnLocator, row: usize, for_max: bool| {
        let mut scratch = [0u8; 16];
        encode_ordered(loc.native_le_bytes(&mb, row, &mut scratch), tc, for_max)
    };
    let loc_pk = schema.locate(1);
    let loc_pl = schema.locate(2);

    for for_max in [false, true] {
        let mut prev: Option<u64> = None;
        for (i, &v) in vals.iter().enumerate() {
            let enc_pk = encode_at(&loc_pk, i, for_max);
            let enc_pl = encode_at(&loc_pl, i, for_max);
            assert_eq!(
                enc_pk, enc_pl,
                "PK-source and payload-source must encode identically (v={v}, for_max={for_max})",
            );
            // Order-preserving: for MIN a larger value encodes larger; for
            // MAX the order inverts so the ascending cursor walk yields the
            // max first. With the byte-swap bug the 1/256 pair would break
            // this monotonicity.
            if let Some(p) = prev {
                if for_max {
                    assert!(enc_pk < p, "MAX: larger value must encode smaller (v={v})");
                } else {
                    assert!(enc_pk > p, "MIN: larger value must encode larger (v={v})");
                }
            }
            prev = Some(enc_pk);
            // Round-trips through `decode_ordered` to the value's native bits
            // (sign-extended for signed, zero-extended for unsigned).
            let expected = if signed { (v as i64) as u64 } else { v as u64 };
            assert_eq!(
                decode_ordered(if for_max { !enc_pk } else { enc_pk }, tc),
                expected,
                "round-trip through decode_ordered (v={v}, for_max={for_max})",
            );
        }
    }
}

// A signed PK aggregate column with negative and positive values.
#[test]
fn signed_pk_source_orders_and_round_trips() {
    check_pk_source(TypeCode::I32, &[i32::MIN as i128, -100, -1, 0, 1_000_000]);
    check_pk_source(
        TypeCode::I64,
        &[i64::MIN as i128, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX as i128],
    );
}

// An unsigned PK aggregate column crossing the high-byte boundary: 1
// (0x0001) and 256 (0x0100) byte-swap into each other, so the buggy
// encode-without-decode would report 256 < 1.
#[test]
fn unsigned_pk_source_high_byte_boundary_orders_and_round_trips() {
    check_pk_source(TypeCode::U16, &[1, 256, 0x0102, 0xFFFF]);
    check_pk_source(
        TypeCode::U64,
        &[1, 256, 0x0100_0000, (u32::MAX as i128) + 1, i64::MAX as i128],
    );
}

/// `encode_ordered` is the *encoding* form of the scalar total order whose
/// *comparison* form is `gnitz_wire::cmp_typed_le`. The AVI's correctness is
/// exactly their agreement: an ascending cursor walk over encoded keys yields
/// the extremum `cmp_typed_le` would pick. Every pair of every order-encodable
/// type, both directions.
fn assert_encode_matches_cmp(tc: TypeCode, vals: &[[u8; 8]]) {
    let w = SchemaColumn::new(tc as u8, 0).size() as usize;
    for a in vals {
        for b in vals {
            let (ea, eb) = (encode_ordered(&a[..w], tc, false), encode_ordered(&b[..w], tc, false));
            assert_eq!(
                ea.cmp(&eb),
                gnitz_wire::cmp_typed_le(&a[..w], &b[..w], tc as u8),
                "{tc:?}: encode order disagrees with cmp_typed_le for {a:02x?} vs {b:02x?}",
            );
        }
    }
}

#[test]
fn encode_ordered_agrees_with_cmp_typed_le() {
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
        assert_encode_matches_cmp(tc, &vals.into_iter().map(pad).collect::<Vec<_>>());
    }

    // Floats carry the cases the integer types cannot: NaN (which has a
    // defined position under `total_cmp` and none under `<`), and −0.0 vs
    // +0.0 (byte-distinct, numerically equal, and ordered by `total_cmp`).
    let f64s: Vec<[u8; 8]> = [
        f64::NEG_INFINITY,
        -1.5,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        1.5,
        f64::INFINITY,
        f64::NAN,
        -f64::NAN,
    ]
    .iter()
    .map(|v| v.to_bits().to_le_bytes())
    .collect();
    assert_encode_matches_cmp(TypeCode::F64, &f64s);

    let f32s: Vec<[u8; 8]> = [
        f32::NEG_INFINITY,
        -1.5,
        -0.0,
        0.0,
        f32::MIN_POSITIVE,
        1.5,
        f32::INFINITY,
        f32::NAN,
        -f32::NAN,
    ]
    .iter()
    .map(|v| (v.to_bits() as u64).to_le_bytes())
    .collect();
    assert_encode_matches_cmp(TypeCode::F32, &f32s);
}
