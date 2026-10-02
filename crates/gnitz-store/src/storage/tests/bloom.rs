use super::*;
use gnitz_zset::schema::key::probe_key;

/// Keys are derived exactly as production derives them — from a PK's OPK
/// bytes — so these exercise the real fingerprint, not raw counters.
fn key(i: u64) -> u64 {
    probe_key(&i.to_be_bytes())
}

/// A wide (24-byte) OPK whose leading 16 bytes are shared, so only the tail
/// separates one key from the next.
fn wide_key(tail: u64) -> u64 {
    let mut k = [0u8; 24];
    k[..16].copy_from_slice(&0xABCD_1234_5678_9ABCu64.to_be_bytes().repeat(2));
    k[16..].copy_from_slice(&tail.to_be_bytes());
    probe_key(&k)
}

#[test]
fn no_false_negatives() {
    for derive in [key as fn(u64) -> u64, wide_key] {
        let mut bf = BloomFilter::new(100);
        for i in 0u64..100 {
            bf.add(derive(i));
        }
        for i in 0u64..100 {
            assert!(bf.may_contain(derive(i)), "false negative for key {i}");
        }
    }
}

#[test]
fn false_positive_rate() {
    let mut bf = BloomFilter::new(1000);
    let fp = |bf: &BloomFilter| (10_000u64..11_000).filter(|&i| bf.may_contain(key(i))).count();
    assert_eq!(fp(&bf), 0, "an empty filter matches nothing");

    for i in 0u64..1000 {
        bf.add(key(i));
    }
    // BITS_PER_KEY bits per key over NUM_PROBES probes puts the theoretical
    // rate near 1%; the bound is loose enough to survive a hash change.
    assert!(fp(&bf) < 50, "FPR too high: {}/1000", fp(&bf));
}

/// A filter is stale once it has taken more keys than its bits are sized for
/// and at most half of them are live.
#[test]
fn a_filter_is_stale_once_overfull_and_half_gone() {
    let mut bf = BloomFilter::new(100);
    let sized_for = bf.num_bits as usize / BITS_PER_KEY;
    for i in 0..sized_for as u64 {
        bf.add(key(i));
    }
    assert!(!bf.stale(0), "at its size");
    bf.add(key(u64::MAX));
    let added = sized_for + 1;
    assert!(bf.stale(added / 2));
    assert!(!bf.stale(added / 2 + 1), "more than half live");
}
