use super::*;
use tls_pool::{MAX_POOLED, MAX_POOLED_BYTES};

#[test]
fn recycle_admits_only_nonempty_items_within_the_byte_cap() {
    for (offered, pooled) in [
        (0usize, false),
        (MAX_POOLED_BYTES + 1, false),
        (MAX_POOLED_BYTES, true),
        (4096, true),
    ] {
        drain_pool();
        let mut v: Vec<u8> = Vec::with_capacity(offered);
        v.resize(offered.min(64), 1);
        let cap = v.capacity();
        drop(PooledBuf(v));

        let got = drain_pool();
        if pooled {
            assert_eq!(got.len(), 1, "capacity {offered} must be pooled");
            assert_eq!(got[0].capacity(), cap);
            assert!(got[0].is_empty(), "a pooled buffer comes back cleared");
        } else {
            assert!(got.is_empty(), "capacity {offered} must not be pooled");
        }
    }
}

#[test]
fn full_pool_evicts_its_oldest() {
    drain_pool();
    for cap in 1..=MAX_POOLED + 4 {
        drop(PooledBuf(Vec::with_capacity(cap)));
    }
    let caps: Vec<usize> = drain_pool().iter().map(Vec::capacity).collect();
    assert_eq!(caps, (5..=MAX_POOLED + 4).collect::<Vec<_>>());
}

#[test]
fn acquire_takes_only_a_buffer_within_twice_the_request() {
    drain_pool();
    drop(PooledBuf(Vec::with_capacity(4096)));

    let small = PooledBuf::with_capacity(1000);
    assert!(small.capacity() < 4096, "4096 is more than twice 1000");
    let fits = PooledBuf::with_capacity(3000);
    assert_eq!(fits.capacity(), 4096, "4096 is within twice 3000");
    drop(fits);

    let none = PooledBuf::with_capacity(0);
    assert_eq!(none.capacity(), 0);
    assert_eq!(drain_pool().len(), 1, "a 0-byte request takes nothing");
    drop(small);
}
