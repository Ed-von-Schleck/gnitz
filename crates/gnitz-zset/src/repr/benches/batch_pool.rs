use super::tls_pool::{recycle, take, MAX_POOLED};
use gnitz_foundation::perf::Counter;
use std::cell::Cell;
use std::collections::VecDeque;
use std::hint::black_box;

thread_local! {
    static POOL: Cell<VecDeque<usize>> = const { Cell::new(VecDeque::new()) };
}

/// Instructions per `take` over a full pool: one that scans every entry and
/// accepts none, and one that accepts only the oldest, each followed by the
/// `recycle` that keeps the pool full.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pool_scan_bench() {
    const ITERS: usize = 1_000_000;
    let counter = Counter::instructions();
    (0..MAX_POOLED).for_each(|id| recycle(&POOL, id, 1));

    let ((), miss) = counter.measure(|| {
        for _ in 0..ITERS {
            let absent = black_box(MAX_POOLED);
            assert!(take(&POOL, |&id| id == absent).is_none());
        }
    });
    // Each id taken goes back as the newest, which leaves the next one oldest.
    let ((), hit) = counter.measure(|| {
        for i in 0..ITERS {
            let oldest = black_box(i % MAX_POOLED);
            let id = take(&POOL, |&id| id == oldest).expect("the pool holds every id");
            recycle(&POOL, id, 1);
        }
    });
    assert_eq!(POOL.take().len(), MAX_POOLED);
    println!(
        "pool_scan_bench: miss {:.1} instr, hit on the oldest + recycle {:.1} instr",
        miss as f64 / ITERS as f64,
        hit as f64 / ITERS as f64
    );
}
