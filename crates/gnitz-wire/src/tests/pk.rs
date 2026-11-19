use super::*;
use crate::{FixedInt, TypeCode, cmp_col_window};

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

/// The OPK codec round-trips and orders like the typed value (`cmp_col_window`)
/// at every PK-eligible width.
#[test]
fn opk_codec_over_every_pk_type() {
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let sz = tc.wire_stride();
        let les: Vec<[u8; 16]> = PATTERNS.iter().map(|p| p.to_le_bytes()).collect();
        let keys: Vec<Vec<u8>> = PATTERNS
            .iter()
            .map(|&p| {
                let mut o = vec![0u8; sz];
                store_opk(&mut o, p ^ opk_bias(tc));
                let mut pushed = vec![0xEE];
                push_opk(&mut pushed, sz, p ^ opk_bias(tc));
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
                decode_opk_u128(opk, tc),
                PATTERNS[i] & image_mask(sz),
                "tc={tc} p={i}: decode_opk_u128"
            );
            assert_eq!(
                key_image(tc, image),
                PATTERNS[i] & image_mask(sz),
                "tc={tc} p={i}: inverse"
            );
            if let Some(fi) = FixedInt::from_type_code(tc) {
                assert_eq!(decode_opk_i64(opk, fi), fi.decode_le_i64(&le[..sz]), "tc={tc} p={i}");
            }
            for (j, b) in les.iter().enumerate() {
                assert_eq!(
                    opk.cmp(&keys[j]),
                    cmp_col_window(&le[..sz], &b[..sz], tc),
                    "tc={tc}: pattern {i} vs {j}"
                );
            }
        }
    }
}

/// The column codec at every width, for a column that is the whole row and for
/// one inside a wider key: the encoder writes cells `decode_pk_cell` reads back
/// row by row, the column decoder reads them back unchanged, and the row's other
/// bytes stay as they were.
#[test]
fn the_column_codec_is_the_cell_codec_per_row() {
    let rows = 37usize;
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let (width, signed) = (tc.wire_stride(), tc.is_signed_int());
        for (stride, off) in [(width, 0), (width + 11, 3)] {
            let src: Vec<u8> = (0..rows * width)
                .map(|i| (i as u8).wrapping_mul(151).wrapping_add(7))
                .collect();
            let at = format!("{tc} stride {stride}");
            let before = vec![0xA5u8; rows * stride];
            let mut pk = before.clone();
            encode_pk_cells(&src, tc, &mut pk, stride, off);
            let mut back = vec![0u8; rows * width];
            decode_pk_cells(&pk, stride, off, tc, &mut back);
            assert_eq!(back, src, "{at}");
            for (r, (row, was)) in pk.chunks_exact(stride).zip(before.chunks_exact(stride)).enumerate() {
                let mut cell = vec![0u8; width];
                decode_pk_cell(&row[off..off + width], signed, &mut cell);
                assert_eq!(cell, src[r * width..(r + 1) * width], "{at}: row {r}");
                assert_eq!(row[..off], was[..off], "{at}: row {r} before the cell");
                assert_eq!(row[off + width..], was[off + width..], "{at}: row {r} after the cell");
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
        for stride in 0..=16usize {
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
    t.push(2, 0x0101);
    t.push(2, 0x0101);
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

/// A key reads back as written at every width up to `MAX_PK_BYTES`, the empty
/// one included; a section one byte wider is refused, not truncated.
#[test]
fn a_pk_buf_reads_back_as_written_or_is_refused() {
    use crate::codec::{Writer, decode_all};
    for len in [0, crate::MAX_PK_BYTES] {
        let key = PkBuf::from_bytes(&vec![0xcd; len]);
        let mut w = Writer::new();
        w.put(&key);
        assert_eq!(decode_all(&w.into_vec(), "k", |r| r.get::<PkBuf>()), Ok(key));
    }
    let mut w = Writer::new();
    w.bytes32(&[0; crate::MAX_PK_BYTES + 1]);
    let err = decode_all(&w.into_vec(), "k", |r| r.get::<PkBuf>()).unwrap_err();
    assert!(err.contains("-byte key exceeds"), "{err}");
}

fn u64_keys(vals: impl IntoIterator<Item = u64>) -> Vec<u8> {
    vals.into_iter().flat_map(u64::to_be_bytes).collect()
}

/// A list off the wire round-trips through `encode_probe` whether it decodes
/// inline (up to `MAX_PK_BYTES`) or onto the heap, and a decoded list equals the
/// same keys built by `from_sorted`.
#[test]
fn pk_keys_decode_inline_and_heap() {
    for n in [1u64, 2, 10, 11, 64] {
        let bytes = u64_keys(0..n);
        let k = PkKeys::checked(8, &bytes).unwrap();
        assert_eq!(!k.bytes.spilled(), bytes.len() <= MAX_PK_BYTES, "n {n}");
        assert_eq!(k.as_bytes(), bytes, "n {n}");
        assert_eq!(k.len(), n as usize, "n {n}");
        assert_eq!(k, PkKeys::from_sorted(8, bytes.clone()), "n {n}");
        assert_eq!(PkKeys::decode_probe(&k.encode_probe()), Ok(k), "n {n}");
    }
    assert!(PkKeys::checked(8, &u64_keys([2, 1])).is_err());
    assert!(PkKeys::checked(8, &u64_keys([1, 1])).is_err());
}

/// An inline list shrinks in place, down to empty.
#[test]
fn pk_keys_retain_inline_to_empty() {
    let mut k = PkKeys::checked(8, &u64_keys([1, 2, 3, 4])).unwrap();
    k.retain(|key| key[7] % 2 == 0);
    assert_eq!(k, PkKeys::from_sorted(8, u64_keys([2, 4])));
    assert_eq!(k.bounds(), Some((&u64_keys([2])[..], &u64_keys([4])[..])));
    k.retain(|_| false);
    assert!(k.is_empty());
    assert_eq!(k.len(), 0);
    assert_eq!(k.bounds(), None);
    assert_eq!(k, PkKeys::from_sorted(8, Vec::new()));
}

/// `from_ascending` drops a repeated key and keeps the rest as they come, past
/// the inline capacity too.
#[test]
fn pk_keys_from_ascending_drops_repeats() {
    let long = u64_keys(0..11);
    assert!(long.len() > MAX_PK_BYTES);
    assert_eq!(PkKeys::from_ascending(8, &long).as_bytes(), long);

    let k = PkKeys::from_ascending(8, &u64_keys((0..40).map(|k| k / 2)));
    assert_eq!(k.as_bytes(), u64_keys(0..20));

    assert_eq!(
        PkKeys::from_ascending(8, &u64_keys([7, 7])),
        PkKeys::from_sorted(8, u64_keys([7]))
    );
    assert!(PkKeys::from_ascending(8, &[]).is_empty());
}
