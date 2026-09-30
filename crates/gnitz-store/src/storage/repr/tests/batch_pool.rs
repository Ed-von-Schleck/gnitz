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
        recycle_buf(v);

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
        recycle_buf(Vec::with_capacity(cap));
    }
    let caps: Vec<usize> = drain_pool().iter().map(Vec::capacity).collect();
    assert_eq!(caps, (5..=MAX_POOLED + 4).collect::<Vec<_>>());
}

#[test]
fn acquire_takes_only_a_buffer_within_twice_the_request() {
    drain_pool();
    recycle_buf(Vec::with_capacity(4096));

    let small = acquire_arena(1000);
    assert!(small.capacity() < 4096, "4096 is more than twice 1000");
    let fits = acquire_arena(3000);
    assert_eq!(fits.capacity(), 4096, "4096 is within twice 3000");
    recycle_buf(fits);

    let none = acquire_arena(0);
    assert_eq!(none.capacity(), 0);
    assert_eq!(drain_pool().len(), 1, "a 0-byte request takes nothing");
    drop(small);
}

/// Pool scan cost: an empty request, a miss over a full pool, and a hit on its
/// oldest entry.
///
/// `cd crates && cargo test -p gnitz-store --release pool_scan_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn pool_scan_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const ITERS: u32 = 1_000_000;
    const OLDEST: usize = 1 << 20;
    const REST: usize = 64 << 10;
    const MISS: usize = 16;
    let fill = || {
        drain_pool();
        recycle_buf(Vec::with_capacity(OLDEST));
        for _ in 1..MAX_POOLED {
            recycle_buf(Vec::with_capacity(REST));
        }
    };
    let per_iter = |t: Instant| t.elapsed().as_nanos() as f64 / ITERS as f64;

    fill();
    let t = Instant::now();
    for _ in 0..ITERS {
        black_box(acquire_arena(black_box(0)));
    }
    println!("pool_scan empty request: {:.2} ns", per_iter(t));

    fill();
    let t = Instant::now();
    for _ in 0..ITERS {
        black_box(acquire_arena(black_box(MISS)));
    }
    println!("pool_scan full-scan miss: {:.2} ns", per_iter(t));
    assert_eq!(drain_pool().len(), MAX_POOLED, "every request missed");

    fill();
    assert_eq!(acquire_arena(OLDEST).capacity(), OLDEST, "the oldest entry fits");
    fill();
    let t = Instant::now();
    for _ in 0..ITERS {
        let v = acquire_arena(black_box(OLDEST));
        black_box(v.as_ptr());
        // Back to the oldest end, for the next take to scan past every entry.
        BUF_POOL.with(|p| {
            let mut items = p.take();
            items.push_front(v);
            p.set(items);
        });
    }
    println!("pool_scan hit on oldest (+ reinsert): {:.2} ns", per_iter(t));
    drain_pool();
}
