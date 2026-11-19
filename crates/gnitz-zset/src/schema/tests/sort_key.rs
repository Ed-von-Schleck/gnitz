use super::*;
use gnitz_foundation::rng::{Rng, splitmix};
use gnitz_wire::widen_pk_be;

/// `pack_pk_be` and every [`PkSortKey`] order keys exactly as `memcmp` orders
/// their bytes, at every PK width and over key pairs sharing a prefix of every
/// length.
#[test]
fn sort_keys_agree_with_memcmp_at_every_width() {
    let mut rng = Rng::new(0xB17E5);
    for w in 0..=MAX_PK_BYTES {
        for _ in 0..16 {
            let a: Vec<u8> = (0..w).map(|_| rng.next_u64() as u8).collect();
            // The leading 16 bytes, zero-padded, read big-endian.
            let mut lead = [0u8; 16];
            lead[..w.min(16)].copy_from_slice(&a[..w.min(16)]);
            assert_eq!(pack_pk_be(&a), u128::from_be_bytes(lead), "w={w}");
            for pos in 0..=w {
                let mut b = a.clone();
                if pos < w {
                    b[pos] = rng.next_u64() as u8;
                }
                pk_width_dispatch!(w, |K| assert_eq!(
                    Ord::cmp(&K::from_opk(&a[..]), &K::from_opk(&b[..])),
                    a.cmp(&b),
                    "w={w} pos={pos}"
                ));
            }
        }
    }
}

/// [`pk_keys`] is the one sort: at every stride duplicates collapse and the
/// list comes out ascending whatever order it went in. At every narrow stride, sorting images builds the same list, both
/// arms of the integer width included. Keys over a two-letter alphabet tie and
/// share prefixes, so every byte of a wide key decides somewhere.
#[test]
fn pk_keys_is_the_byte_sort() {
    let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15);
    for stride in 1..=MAX_PK_BYTES {
        let mut raw: Vec<Vec<u8>> = (0..64)
            .map(|_| (0..stride).map(|_| rng.gen_range(2) as u8).collect())
            .collect();
        raw.push(raw[3].clone());
        let mut want = raw.clone();
        want.sort_unstable();
        want.dedup();
        let k = pk_keys(stride, raw.iter().map(|k| &k[..]));
        assert_eq!(k.as_bytes(), want.concat(), "stride {stride}");
        if stride <= NARROW_PK_MAX_BYTES {
            let images = raw.iter().map(|k| widen_pk_be(k));
            assert_eq!(pk_keys_of_images(stride, images), k, "stride {stride}");
        }
    }
}

/// A single key, by bytes or by image, builds the same list as the multi-key
/// sort would; so do two and three keys around it.
#[test]
fn pk_keys_single_key_matches_the_sort() {
    for stride in [1usize, 3, 8, 9, 16, 24, 40] {
        let key: Vec<u8> = (0..stride as u8).map(|b| b.wrapping_mul(37) | 1).collect();
        let one = pk_keys(stride, [&key[..]]);
        assert_eq!(one.as_bytes(), key, "stride {stride}");
        assert_eq!(one.len(), 1, "stride {stride}");
        assert_eq!(pk_keys(stride, [&key[..], &key[..]]), one, "stride {stride}");
        let zero = vec![0u8; stride];
        let two = pk_keys(stride, [&key[..], &zero[..], &key[..]]);
        assert_eq!(two.as_bytes(), [&zero[..], &key[..]].concat(), "stride {stride}");
        if stride <= NARROW_PK_MAX_BYTES {
            let image = widen_pk_be(&key);
            assert_eq!(pk_keys_of_images(stride, [image]), one, "stride {stride}");
            assert_eq!(pk_keys_of_images(stride, [image, 0, image]), two, "stride {stride}");
        }
    }
    assert!(pk_keys(8, std::iter::empty()).is_empty());
    assert!(pk_keys_of_images(8, std::iter::empty()).is_empty());
}

#[test]
#[should_panic(expected = "key width")]
fn pk_keys_single_key_checks_its_width() {
    pk_keys(8, [&[0u8; 4][..]]);
}

/// Lists long enough for the radix sort, at every stride of an image or a pair
/// of them: over keys whose varying bytes are few and scattered (the radix arm)
/// and over random keys (the comparison arm), with duplicates. Each comes out as
/// the byte sort of its keys, and at a narrow stride sorting their images agrees.
#[test]
fn pk_keys_radix_is_the_byte_sort() {
    let mut rng = Rng::new(0x2545_F491_4F6C_DD1D);
    for stride in 1..=2 * NARROW_PK_MAX_BYTES {
        for n in [255usize, 256, 300, 5000] {
            for sparse in [true, false] {
                let raw: Vec<Vec<u8>> = (0..n)
                    .map(|_| match sparse {
                        // Only the lowest byte and the highest vary.
                        true => {
                            let r = rng.next_u64();
                            let mut key = vec![0u8; stride];
                            key[0] |= (r >> 8) as u8 & 3;
                            key[stride - 1] = r as u8;
                            key
                        }
                        false => (0..stride).map(|_| rng.next_u64() as u8).collect(),
                    })
                    .collect();
                let mut want = raw.clone();
                want.sort_unstable();
                want.dedup();
                let got = pk_keys(stride, raw.iter().map(|k| &k[..]));
                assert_eq!(got.as_bytes(), want.concat(), "stride {stride} n {n}");
                if stride <= NARROW_PK_MAX_BYTES {
                    let images = raw.iter().map(|k| widen_pk_be(k));
                    assert_eq!(pk_keys_of_images(stride, images), got, "stride {stride} n {n}");
                }
            }
        }
    }
}

/// `v` sorted by `key` is a permutation of `v` whose keys ascend — and the
/// comparison sort's keys, in every shape of input the radix sort branches on.
fn check<K: RadixKey + std::fmt::Debug>(v: Vec<(K, u32)>) {
    let mut sorted = v.clone();
    radix_sort_by_key(&mut sorted, |e| e.0);
    let mut want = v.clone();
    want.sort_unstable();
    assert!(sorted.is_sorted_by_key(|e| e.0), "keys out of order");
    assert_eq!(
        sorted.iter().map(|e| e.0).collect::<Vec<_>>(),
        want.iter().map(|e| e.0).collect::<Vec<_>>()
    );
    sorted.sort_unstable();
    assert_eq!(sorted, want, "not a permutation");
}

/// Rows `0..n` whose keys are `key(i)`, each tagged with a scrambled index so
/// rows of one key stand out of index order.
fn rows<K>(n: u64, key: impl Fn(u64) -> K) -> Vec<(K, u32)> {
    (0..n).map(|i| (key(i), splitmix(i) as u32)).collect()
}

/// Each shape at a key whose varying bytes `spread` places into it.
fn shapes<K: RadixKey + std::fmt::Debug>(spread: impl Fn(u64) -> K) {
    for n in [0, 1, 255, 256, 4096, 65536] {
        check(rows(n, |i| spread(splitmix(i))));
        check(rows(n, &spread));
        check(rows(n, |i| spread(n - i)));
        check(rows(n, |i| spread(if i == n - 1 { 0 } else { i + 1 })));
        // Every key one: neither ascending nor descending by the tag.
        check(rows(n, |_| spread(1)));
        // One varying byte, then two, then three: odd and even pass counts.
        for bits in [8, 16, 24] {
            check(rows(n, |i| spread(splitmix(i) & ((1 << bits) - 1))));
        }
        // Too many varying bytes, found only past the first chunk.
        check(rows(n, |i| spread(if i < 1000 { i & 0xFF } else { splitmix(i) })));
    }
}

#[test]
fn radix_sort_by_key_sorts_every_shape() {
    shapes(|x| x);
    shapes(|x| (x as u128) << 40 | x as u128);
    shapes(|x| [(x as u128) << 70, x as u128]);
}
