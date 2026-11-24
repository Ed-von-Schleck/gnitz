use super::*;
use gnitz_foundation::rng::splitmix;

/// Over indices into `keys`: [`stable_radix_sort_by_key`] is the stable
/// comparison sort, and [`radix_sort_by_key`] a permutation whose keys are that
/// sort's keys.
fn check<K: RadixKey + std::fmt::Debug>(keys: &[K]) {
    let key = |&i: &u32| keys[i as usize];
    let ids = || (0..keys.len() as u32).collect::<Vec<u32>>();
    let mut want = ids();
    want.sort_by_key(key);
    let mut stable = ids();
    stable_radix_sort_by_key(&mut stable, key);
    assert_eq!(stable, want, "stable sort, n {}", keys.len());
    let mut unstable = ids();
    radix_sort_by_key(&mut unstable, key);
    assert!(
        unstable.iter().map(key).eq(want.iter().map(key)),
        "keys out of order, n {}",
        keys.len()
    );
    unstable.sort_unstable();
    assert_eq!(unstable, ids(), "not a permutation");
}

/// Each shape of `n` keys the sorts branch on, as the varying value of a key.
fn shapes(n: u64) -> Vec<Vec<u64>> {
    let shape = |f: &dyn Fn(u64) -> u64| (0..n).map(f).collect();
    vec![
        shape(&splitmix),
        shape(&|i| i),
        shape(&|i| n - i),
        shape(&|i| if i == n - 1 { 0 } else { i + 1 }),
        // Every key one.
        shape(&|_| 1),
        // One varying byte, then two, then three: odd and even pass counts.
        shape(&|i| splitmix(i) & 0xFF),
        shape(&|i| splitmix(i) & 0xFFFF),
        shape(&|i| splitmix(i) & 0xFF_FFFF),
        // Two varying bytes far apart.
        shape(&|i| splitmix(i) & 0x0300_00FF),
        // Many varying bytes, found only past the first chunk: the MSD arm.
        shape(&|i| if i < 1000 { i & 0xFF } else { splitmix(i) }),
        // Many varying bytes in runs of one key longer than an MSD leaf, which
        // run out of digits that split them.
        shape(&|i| splitmix(i / 300)),
        // Few keys, each tied many times, in no order and descending.
        shape(&|i| splitmix(i) % 7),
        shape(&|i| u64::MAX - i / 3),
    ]
}

/// Every shape at every key type: a word, a pair of words, a pair of `u128`s,
/// and a byte string wider than any of them whose varying bytes sit at both ends.
#[test]
fn radix_sorts_order_every_shape_at_every_key_type() {
    for n in [0, 1, 255, 256, 4096, 65536] {
        for raw in shapes(n) {
            check(&raw);
            check(&raw.iter().map(|&x| (x as u128) << 40 | x as u128).collect::<Vec<_>>());
            check(&raw.iter().map(|&x| [(x as u128) << 70, x as u128]).collect::<Vec<_>>());
            let wide: Vec<[u8; 40]> = (raw.iter())
                .map(|&x| {
                    let (mut k, b) = ([7u8; 40], x.to_be_bytes());
                    k[..4].copy_from_slice(&b[..4]);
                    k[36..].copy_from_slice(&b[4..]);
                    k
                })
                .collect();
            check(&wide.iter().map(|k| &k[..]).collect::<Vec<_>>());
        }
    }
}
