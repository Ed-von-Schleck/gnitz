use super::*;
use gnitz_foundation::rng::Rng;
use gnitz_wire::widen_pk_be;

/// `pack_pk_be` is the leading 16 bytes read big-endian; `compare_pk_ordering`
/// and every [`PkSortKey`] order keys exactly as `memcmp` orders their bytes, at
/// every PK width and over key pairs sharing a prefix of every length; and a
/// sort key puts back the bytes it was built from, then zeros.
#[test]
fn sort_keys_agree_with_memcmp_at_every_width() {
    let mut rng = Rng::new(0xB17E5);
    for w in 0..=MAX_PK_BYTES {
        for _ in 0..16 {
            let a: Vec<u8> = (0..w).map(|_| rng.next_u64() as u8).collect();
            let mut lead = [0u8; 16];
            lead[..w.min(16)].copy_from_slice(&a[..w.min(16)]);
            assert_eq!(pack_pk_be(&a), u128::from_be_bytes(lead), "w={w}");
            pk_width_dispatch!(w, |K| {
                let mut out = vec![0xAA; K::put_width(w)];
                K::from_opk(&a).put(&mut out);
                assert_eq!(out[..w], a[..], "w={w}");
                assert!(out[w..].iter().all(|&b| b == 0), "w={w}");
            });
            for pos in 0..=w {
                let mut b = a.clone();
                if pos < w {
                    b[pos] = rng.next_u64() as u8;
                }
                let want = a.cmp(&b);
                assert_eq!(compare_pk_ordering(&a, &b), want, "w={w} pos={pos}");
                pk_width_dispatch!(w, |K| assert_eq!(
                    Ord::cmp(&K::from_opk(&a), &K::from_opk(&b)),
                    want,
                    "w={w} pos={pos}"
                ));
            }
        }
    }
}

/// [`pk_keys`] is the byte sort with duplicates collapsed, and at a narrow
/// stride sorting the keys' images builds the same list. Every stride takes
/// lists of up to a few dozen keys, the inline and empty ones included; the
/// strides of an image or a pair of them, and two wider ones, take lists long
/// enough for each arm of the radix sort. Keys over a two-letter alphabet tie
/// and share prefixes, so every byte of a wide key decides somewhere; keys
/// varying in only their first and last byte take few radix passes.
#[test]
fn pk_keys_is_the_byte_sort() {
    let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15);
    let binary = |rng: &mut Rng, stride| (0..stride).map(|_| rng.gen_range(2) as u8).collect();
    let sparse = |rng: &mut Rng, stride| {
        let r = rng.next_u64();
        let mut key = vec![0u8; stride];
        key[0] |= (r >> 8) as u8 & 3;
        key[stride - 1] = r as u8;
        key
    };
    let random = |rng: &mut Rng, stride| (0..stride).map(|_| rng.next_u64() as u8).collect();
    let gens: [fn(&mut Rng, usize) -> Vec<u8>; 3] = [binary, sparse, random];
    for stride in 1..=MAX_PK_BYTES {
        let long = stride <= 2 * NARROW_PK_MAX_BYTES || [40, 64].contains(&stride);
        let ns = [0usize, 1, 2, 3, 64]
            .into_iter()
            .chain(long.then_some([255, 256, 5000]).into_iter().flatten());
        for n in ns {
            for keygen in gens {
                let mut raw: Vec<Vec<u8>> = (0..n).map(|_| keygen(&mut rng, stride)).collect();
                if n >= 2 {
                    raw.push(raw[0].clone());
                }
                let mut want = raw.clone();
                want.sort_unstable();
                want.dedup();
                let got = pk_keys(stride, raw.iter().map(Vec::as_slice));
                assert_eq!(got.as_bytes(), want.concat(), "stride {stride} n {n}");
                if stride <= NARROW_PK_MAX_BYTES {
                    let images = raw.iter().map(|k| widen_pk_be(k));
                    assert_eq!(pk_keys_of_images(stride, images), got, "stride {stride} n {n}");
                }
            }
        }
    }
}

#[test]
#[should_panic(expected = "key width")]
fn pk_keys_refuses_a_key_of_another_width() {
    pk_keys(8, [&[0u8; 4][..]]);
}
