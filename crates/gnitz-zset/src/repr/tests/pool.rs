use super::*;

thread_local! {
    static POOL: Cell<VecDeque<Vec<u8>>> = const { Cell::new(VecDeque::new()) };
}

fn drain_pool() -> VecDeque<Vec<u8>> {
    POOL.with(Cell::take)
}

/// Return `v` to [`POOL`], charged its capacity.
fn give(v: Vec<u8>) {
    let cap = v.capacity();
    recycle(&POOL, v, cap);
}

#[test]
fn recycle_admits_only_nonempty_items_within_the_byte_cap() {
    for (offered, pooled) in [
        (0usize, false),
        (MAX_POOLED_BYTES + 1, false),
        (MAX_POOLED_BYTES, true),
        (4096, true),
    ] {
        drain_pool();
        let v: Vec<u8> = Vec::with_capacity(offered);
        let cap = v.capacity();
        give(v);

        let got = drain_pool();
        if pooled {
            assert_eq!(got.len(), 1, "capacity {offered} must be pooled");
            assert_eq!(got[0].capacity(), cap);
        } else {
            assert!(got.is_empty(), "capacity {offered} must not be pooled");
        }
    }
}

#[test]
fn full_pool_evicts_its_oldest() {
    drain_pool();
    for cap in 1..=MAX_POOLED + 4 {
        give(Vec::with_capacity(cap));
    }
    let caps: Vec<usize> = drain_pool().iter().map(Vec::capacity).collect();
    assert_eq!(caps, (5..=MAX_POOLED + 4).collect::<Vec<_>>());
}

/// `take` answers the newest item `fits` accepts, and none when it accepts none.
#[test]
fn take_answers_the_newest_item_that_fits() {
    let pooled = |cap: usize| {
        let v = Vec::with_capacity(cap);
        let at = v.as_ptr();
        give(v);
        at
    };
    drain_pool();
    pooled(4096);
    let newest = pooled(4096);
    pooled(100);
    let got = take(&POOL, |b| b.capacity() >= 1000).expect("a fitting item");
    assert_eq!(got.as_ptr(), newest);
    assert!(take(&POOL, |b| b.capacity() >= 5000).is_none());
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
