use super::*;
use gnitz_zset::schema::key::probe_key;

/// Keys are derived exactly as production derives them — from a PK's OPK
/// bytes — so these exercise the real fingerprint, not raw counters.
fn key(i: u64) -> u64 {
    probe_key(&i.to_be_bytes())
}

#[test]
fn a_filter_holds_every_added_key_and_few_others() {
    let mut bf = BloomFilter::new(1000);
    let fp = |bf: &BloomFilter| (10_000u64..11_000).filter(|&i| bf.may_contain(key(i))).count();
    assert_eq!(fp(&bf), 0, "an empty filter matches nothing");

    for i in 0u64..1000 {
        bf.add(key(i));
    }
    assert!((0u64..1000).all(|i| bf.may_contain(key(i))), "a false negative");
    // BITS_PER_KEY bits per key in split blocks puts the rate near 1%; the
    // bound is loose enough to survive a hash change.
    assert!(fp(&bf) < 50, "FPR too high: {}/1000", fp(&bf));
}
