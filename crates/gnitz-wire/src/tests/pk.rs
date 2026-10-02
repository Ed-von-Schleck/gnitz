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
/// mis-summed weights downstream. `decode_pk_cell` inverts the encode — the
/// bijection byte-equal ⟺ key-equal rests on — `key_image` is the key read as
/// a big-endian integer and its own inverse, `push_opk` appends what `store_opk`
/// stores, and `decode_opk_i64` fuses the decode with `FixedInt`'s widening.
#[test]
fn opk_codec_over_every_pk_type() {
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let sz = tc.wire_stride();
        let les: Vec<[u8; 16]> = PATTERNS.iter().map(|p| p.to_le_bytes()).collect();
        let keys: Vec<Vec<u8>> = PATTERNS
            .iter()
            .map(|&p| {
                let mut o = vec![0u8; sz];
                store_opk(&mut o, p, tc.is_signed_int());
                let mut pushed = vec![0xEE];
                push_opk(&mut pushed, sz, p, tc.is_signed_int());
                assert_eq!(pushed[1..], o[..], "tc={tc} p={p:#x}: push_opk");
                o
            })
            .collect();
        for (i, (le, opk)) in les.iter().zip(&keys).enumerate() {
            let mut back = [0u8; 16];
            decode_pk_cell(opk, tc.is_signed_int(), &mut back[..sz]);
            assert_eq!(back[..sz], le[..sz], "tc={tc} p={i}: decode(encode(v)) != v");
            let image = key_image(tc, PATTERNS[i]);
            assert_eq!(image, widen_pk_be(opk), "tc={tc} p={i}: key_image");
            assert_eq!(
                key_image(tc, image),
                PATTERNS[i] & image_mask(sz),
                "tc={tc} p={i}: inverse"
            );
            let mut stored = vec![0u8; sz];
            store_opk(&mut stored, image, false);
            assert_eq!(&stored, opk, "tc={tc} p={i}: an image stores as it is");
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

/// The column decoder writes what `decode_pk_cell` writes per row, at every width,
/// for a column that is the whole row and for one inside a wider key.
#[test]
fn decode_pk_cells_matches_decode_pk_cell() {
    let rows = 37usize;
    for width in [1usize, 2, 4, 8, 16] {
        for (stride, off) in [(width, 0), (width + 11, 3)] {
            let pk: Vec<u8> = (0..rows * stride)
                .map(|i| (i as u8).wrapping_mul(151).wrapping_add(7))
                .collect();
            for signed in [false, true] {
                let mut got = vec![0u8; rows * width];
                decode_pk_cells(&pk, stride, off, width, signed, &mut got);
                let mut want = vec![0u8; rows * width];
                for r in 0..rows {
                    let at = r * stride + off;
                    decode_pk_cell(&pk[at..at + width], signed, &mut want[r * width..(r + 1) * width]);
                }
                assert_eq!(got, want, "width {width} stride {stride} signed {signed}");
            }
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

/// A `MAX_PK_BYTES` key fits; a key built column by column widens with zeros and
/// equals, hashes and looks up as the same bytes do.
#[test]
fn a_pk_buf_holds_max_pk_bytes_and_widens() {
    let full = PkBuf::from_bytes(&[0xab; crate::MAX_PK_BYTES]);
    assert_eq!(full.pk_bytes().len(), crate::MAX_PK_BYTES);
    let mut t = PkBuf::zeroed(0);
    t.push(2, 0x0101, false);
    t.push(2, 0x0101, false);
    assert_eq!(t.widened(8).pk_bytes(), [1, 1, 1, 1, 0, 0, 0, 0]);
    let fresh = PkBuf::from_bytes(&[1; 4]);
    assert_eq!(t, fresh);
    let set = std::collections::HashSet::from([fresh]);
    assert!(set.contains(&t) && set.contains(&[1u8; 4][..]));
}

#[test]
#[should_panic(expected = "PkBuf::from_bytes: length")]
fn a_key_past_max_pk_bytes_panics() {
    PkBuf::from_bytes(&[0; crate::MAX_PK_BYTES + 1]);
}
