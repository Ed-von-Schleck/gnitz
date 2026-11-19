use super::*;
use gnitz_foundation::rng::splitmix;

/// `v` sorted by `key` is a permutation of `v` whose keys ascend — and the
/// comparison sort's keys, in every shape of input the radix sort branches on.
fn check<K: RadixKey + Default + std::fmt::Debug>(v: Vec<(K, u32)>) {
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
fn shapes<K: RadixKey + Default + std::fmt::Debug>(spread: impl Fn(u64) -> K) {
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
        // Many varying bytes, found only past the first chunk: the MSD arm.
        check(rows(n, |i| spread(if i < 1000 { i & 0xFF } else { splitmix(i) })));
        // Many varying bytes in runs of one key longer than an MSD leaf, which
        // run out of digits that split them.
        check(rows(n, |i| spread(splitmix(i / 300))));
    }
}

#[test]
fn radix_sort_by_key_sorts_every_shape() {
    shapes(|x| x);
    shapes(|x| (x as u128) << 40 | x as u128);
    shapes(|x| [(x as u128) << 70, x as u128]);
}

/// [`stable_radix_sort_by_key`] keeps the order of elements of one key, in every
/// shape [`shapes`] takes, which lead it down each of its arms: rows sorted by key
/// alone come out in the order of the rows sorted by key, then by place.
#[test]
fn stable_radix_sort_keeps_ties_in_order() {
    for n in [255u64, 256, 4096, 65536] {
        for key in [
            |i: u64| splitmix(i) % 7,
            |i: u64| splitmix(i / 300),
            |i: u64| splitmix(i) & 0xFF00,
            |i: u64| splitmix(i),
            |i: u64| u64::MAX - i / 3,
        ] {
            let v: Vec<(u64, u32)> = (0..n).map(|i| (key(i), i as u32)).collect();
            let mut got = v.clone();
            stable_radix_sort_by_key(&mut got, |e| e.0);
            let mut want = v;
            want.sort();
            assert_eq!(got, want, "n {n}");
        }
    }
}

/// A byte-string key orders as its bytes do, at a width past any integer key:
/// keys with few varying bytes, with many, and in runs of one key.
#[test]
fn radix_sort_by_byte_string_is_the_byte_sort() {
    const STRIDE: usize = 40;
    for n in [255u64, 256, 4096, 65536] {
        for spread in [
            |i: u64| splitmix(i) & 0x0300_00FF,
            |i: u64| splitmix(i),
            |i: u64| splitmix(i / 300),
        ] {
            let keys: Vec<[u8; STRIDE]> = (0..n)
                .map(|i| {
                    let mut k = [7u8; STRIDE];
                    let x = spread(i).to_be_bytes();
                    k[..4].copy_from_slice(&x[..4]);
                    k[STRIDE - 4..].copy_from_slice(&x[4..]);
                    k
                })
                .collect();
            let mut got: Vec<u32> = (0..n as u32).collect();
            radix_sort_by_key(&mut got, |&i| &keys[i as usize][..]);
            assert!(got.is_sorted_by_key(|&i| keys[i as usize]), "n {n}");
            got.sort();
            assert!(got.iter().copied().eq(0..n as u32), "not a permutation, n {n}");
        }
    }
}
