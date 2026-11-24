use super::*;

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

/// A pooled buffer goes to a request it fits — within twice the request for
/// `with_capacity`, of exactly the request for `exact` — newest first; a 0-byte
/// request takes nothing.
#[test]
fn a_request_takes_the_newest_pooled_buffer_it_fits() {
    let pooled = |cap: usize| {
        let v = Vec::with_capacity(cap);
        let at = v.as_ptr();
        drop(PooledBuf(v));
        at
    };
    let with_capacity: fn(usize) -> PooledBuf = PooledBuf::with_capacity;
    let exact: fn(usize) -> PooledBuf = PooledBuf::exact;
    for (what, get, size, taken) in [
        ("with_capacity", with_capacity, 1000, false),
        ("with_capacity", with_capacity, 3000, true),
        ("with_capacity", with_capacity, 0, false),
        ("exact", exact, 4095, false),
        ("exact", exact, 4096, true),
    ] {
        drain_pool();
        pooled(4096);
        let newest = pooled(4096);
        assert_eq!(get(size).as_ptr() == newest, taken, "{what}({size})");
    }
}

/// A buffer that outgrows its capacity moves into a pooled one of at least
/// twice it, holding exactly its old bytes, and the outgrown buffer goes back to
/// the pool; a reserve the capacity covers moves nothing, and a growth past
/// `MAX_POOLED_BYTES` neither takes from the pool nor returns to it.
#[test]
fn reserve_pooled_grows_into_a_pooled_buffer() {
    drain_pool();
    let mut b = PooledBuf(Vec::with_capacity(100));
    b.extend_from_slice(&[7; 100]);
    // A pooled buffer that held other bytes.
    let pooled = vec![0xEE; 300];
    let at = pooled.as_ptr();
    drop(PooledBuf(pooled));

    b.reserve_pooled(1);
    assert_eq!(b.as_ptr(), at, "the growth takes the pooled buffer");
    assert_eq!(b.as_slice(), &[7; 100]);
    b.reserve_pooled(200);
    assert_eq!(b.as_ptr(), at, "a covered reserve moves nothing");
    let caps: Vec<usize> = drain_pool().iter().map(Vec::capacity).collect();
    assert_eq!(caps, [100], "the outgrown buffer is pooled");

    b.reserve_pooled(MAX_POOLED_BYTES);
    assert!(b.capacity() >= 100 + MAX_POOLED_BYTES);
    assert_eq!(b.as_slice(), &[7; 100]);
    assert!(drain_pool().is_empty(), "the outgrown buffer is freed");
}

thread_local! {
    static SCRATCH: Cell<Vec<u64>> = const { Cell::new(Vec::new()) };
}

/// The scratch list comes back emptied to the next call, a nested call gets a
/// fresh one, and a list grown past `MAX_POOLED_BYTES` is not kept.
#[test]
fn with_scratch_lends_one_list_within_the_byte_cap() {
    let kept = with_scratch(&SCRATCH, |v| {
        v.extend([1, 2, 3]);
        with_scratch(&SCRATCH, |nested| {
            assert_eq!(nested.capacity(), 0, "a nested call's list")
        });
        v.as_ptr()
    });
    with_scratch(&SCRATCH, |v| {
        assert_eq!((v.as_ptr(), v.len()), (kept, 0), "the list comes back emptied");
        v.reserve(MAX_POOLED_BYTES / 8 + 1);
    });
    with_scratch(&SCRATCH, |v| {
        assert_eq!(v.capacity(), 0, "an oversized list is dropped")
    });
}
