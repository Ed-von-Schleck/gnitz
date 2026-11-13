use super::*;

/// A deterministic scramble of `i`.
fn mix(i: u64) -> u64 {
    let x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
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
    (0..n).map(|i| (key(i), mix(i) as u32)).collect()
}

/// Each shape at a key whose varying bytes `spread` places into it.
fn shapes<K: RadixKey + std::fmt::Debug>(spread: impl Fn(u64) -> K) {
    for n in [0, 1, 255, 256, 4096, 65536] {
        check(rows(n, |i| spread(mix(i))));
        check(rows(n, &spread));
        check(rows(n, |i| spread(n - i)));
        check(rows(n, |i| spread(if i == n - 1 { 0 } else { i + 1 })));
        // Every key one: neither ascending nor descending by the tag.
        check(rows(n, |_| spread(1)));
        // One varying byte, then two, then three: odd and even pass counts.
        for bits in [8, 16, 24] {
            check(rows(n, |i| spread(mix(i) & ((1 << bits) - 1))));
        }
        // Too many varying bytes, found only past the first chunk.
        check(rows(n, |i| spread(if i < 1000 { i & 0xFF } else { mix(i) })));
    }
}

#[test]
fn radix_sort_by_key_sorts_every_shape() {
    shapes(|x| x);
    shapes(|x| (x as u128) << 40 | x as u128);
    shapes(|x| [(x as u128) << 70, x as u128]);
}
