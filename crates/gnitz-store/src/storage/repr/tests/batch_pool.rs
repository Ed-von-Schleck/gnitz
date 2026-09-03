use super::*;

/// Empty the thread-local pool. libtest may run several tests on one thread
/// (`--test-threads=1`), where a buffer left by an earlier test is visible here.
fn drain_pool() {
    while acquire_buf().capacity() > 0 {}
}

#[test]
fn only_buffers_within_the_cap_are_pooled() {
    // A zero-capacity buffer is the moved-from state and an oversized one would
    // trap memory; both are dropped rather than retained.
    for (offered, pooled) in [
        (0usize, false),
        (POOL_BYPASS_BYTES + 1, false),
        (POOL_BYPASS_BYTES, true),
        (4096, true),
    ] {
        drain_pool();
        let mut v: Vec<u8> = Vec::with_capacity(offered);
        v.resize(offered.min(64), 1);
        recycle_buf(v);

        let got = acquire_buf();
        if pooled {
            assert!(got.capacity() >= offered, "capacity {offered} must be pooled");
        } else {
            assert_eq!(got.capacity(), 0, "capacity {offered} must not be pooled");
        }
        assert_eq!(got.len(), 0, "a pooled buffer comes back cleared");
    }
}

#[test]
fn pool_capped_at_max_pooled() {
    drain_pool();
    for _ in 0..tls_pool::MAX_POOLED + 4 {
        recycle_buf(vec![0u8; 128]);
    }
    let mut count = 0usize;
    while acquire_buf().capacity() > 0 {
        count += 1;
    }
    assert_eq!(count, tls_pool::MAX_POOLED);
}
