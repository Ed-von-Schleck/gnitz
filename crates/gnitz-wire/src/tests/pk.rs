use super::*;
use crate::{cmp_col_window, FixedInt, TypeCode};

/// Bit patterns every width-parameterized sweep below truncates to its type's
/// width: both sign boundaries, the all-ones edges, and the 2^63 / 2^64 / 2^127
/// steps that separate a narrow image from a wide one.
const PATTERNS: &[u128] = &[
    0,
    1,
    2,
    0x7F,
    0x80,
    0xFF,
    0x100,
    i64::MAX as u128,
    1 << 63,
    u64::MAX as u128,
    1 << 64,
    i128::MAX as u128,
    1 << 127,
    u128::MAX,
];

/// The OPK codec at every PK-eligible width. Unsigned byte comparison over an
/// encoded key IS the typed order of the value it encodes (`cmp_col_window` is
/// the crate's own definition of that order over native LE bytes, tested
/// independently), so a wrong sign flip fails here rather than as silently
/// mis-summed weights downstream. `decode_pk_column` inverts the encode — the
/// bijection byte-equal ⟺ key-equal rests on — `key_image` is the key read as
/// a big-endian integer, and `decode_opk_i64` fuses the decode with
/// `FixedInt`'s widening.
#[test]
fn opk_codec_over_every_pk_type() {
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let sz = tc.wire_stride();
        let les: Vec<[u8; 16]> = PATTERNS.iter().map(|p| p.to_le_bytes()).collect();
        let keys: Vec<Vec<u8>> = les
            .iter()
            .map(|le| {
                let mut o = vec![0u8; sz];
                encode_pk_column(&le[..sz], tc, &mut o);
                o
            })
            .collect();
        for (i, (le, opk)) in les.iter().zip(&keys).enumerate() {
            let mut back = [0u8; 16];
            decode_pk_column(opk, tc, &mut back[..sz]);
            assert_eq!(back[..sz], le[..sz], "tc={tc} p={i}: decode(encode(v)) != v");
            assert_eq!(key_image(tc, PATTERNS[i]), widen_pk_be(opk), "tc={tc} p={i}: key_image");
            if let Some(fi) = FixedInt::from_type_code(tc) {
                assert_eq!(decode_opk_i64(opk, fi), fi.decode_le_i64(&le[..sz]), "tc={tc} p={i}");
            }
            for (j, b) in les.iter().enumerate() {
                assert_eq!(
                    opk.cmp(&keys[j]),
                    cmp_col_window(&le[..sz], &[], &b[..sz], &[], tc),
                    "tc={tc}: pattern {i} vs {j}"
                );
            }
        }
    }
}

/// `store_opk_image` writes what decode → widen → encode writes, for every
/// promotion the engine performs: an FK's domain widening and a join key's
/// packing at its common slot.
#[test]
fn store_opk_image_matches_decode_widen_encode() {
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    let mut pairs: Vec<(TypeCode, TypeCode)> = Vec::new();
    for &src in &pk_types {
        for &target in &pk_types {
            if src == target || src.int_domain_fits(target) || src.packs_at(target) {
                pairs.push((src, target));
            }
        }
    }
    for (src, target) in pairs {
        let (sw, tw) = (src.wire_stride(), target.wire_stride());
        for p in PATTERNS {
            let le = p.to_le_bytes();
            let native = &le[..sw];
            let mut opk_src = [0u8; 16];
            encode_pk_column(native, src, &mut opk_src[..sw]);

            let mut got = [0u8; 16];
            store_opk_image(widen_pk_be(&opk_src[..sw]), src, sw, target, &mut got[..tw]);

            let mut wide = [0u8; 16];
            widen_native_le(native, src, &mut wide[..tw]);
            let mut want = [0u8; 16];
            encode_pk_column(&wide[..tw], target, &mut want[..tw]);
            assert_eq!(got[..tw], want[..tw], "src={src} target={target} p={p:#x}");
        }
    }
}

/// The width-specialized arms must agree with the general right-align form at
/// every stride a PK region can have — the 9..=15 overlapping-load band and
/// the 3/5/6/7 widths only the buffer arm serves — including the all-zero and
/// all-ones edges.
#[test]
fn widen_pk_be_matches_the_general_form() {
    let mixed: [u8; 16] = core::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(1));
    for bytes in [mixed, [0u8; 16], [0xFFu8; 16]] {
        for stride in 1..=16usize {
            let mut buf = [0u8; 16];
            buf[16 - stride..].copy_from_slice(&bytes[..stride]);
            assert_eq!(
                widen_pk_be(&bytes[..stride]),
                u128::from_be_bytes(buf),
                "stride {stride} diverges from the general form"
            );
        }
    }
}

/// A `MAX_PK_BYTES` key fits, and narrowing a buffer re-zeroes the tail it
/// gives up, so widening it again reads zeros there rather than stale bytes.
#[test]
fn a_pk_buf_holds_max_pk_bytes_and_narrows_clean() {
    let mut t = PkBuf::from_bytes(&[0xab; crate::MAX_PK_BYTES]);
    assert_eq!(t.width(), crate::MAX_PK_BYTES);
    t.write(4, |b| b.fill(1));
    assert_eq!(t.widened(8).pk_bytes(), [1, 1, 1, 1, 0, 0, 0, 0]);
}

#[test]
#[should_panic(expected = "PkBuf::from_bytes: length")]
fn a_key_past_max_pk_bytes_panics() {
    PkBuf::from_bytes(&[0; crate::MAX_PK_BYTES + 1]);
}
