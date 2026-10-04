use super::*;
use tls_pool::MAX_POOLED;

/// Pool scan cost: an empty request, a miss over a full pool, and a hit on its
/// oldest entry.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pool_scan_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const ITERS: u32 = 1_000_000;
    const OLDEST: usize = 1 << 20;
    const REST: usize = 64 << 10;
    const MISS: usize = 16;
    let fill = || {
        drain_pool();
        drop(PooledBuf(Vec::with_capacity(OLDEST)));
        for _ in 1..MAX_POOLED {
            drop(PooledBuf(Vec::with_capacity(REST)));
        }
    };
    let per_iter = |t: Instant| t.elapsed().as_nanos() as f64 / ITERS as f64;

    fill();
    let t = Instant::now();
    for _ in 0..ITERS {
        black_box(PooledBuf::with_capacity(black_box(0)));
    }
    println!("pool_scan empty request: {:.2} ns", per_iter(t));

    fill();
    let t = Instant::now();
    for _ in 0..ITERS {
        // Taken out, so the fresh buffer is freed rather than pooled.
        let mut missed = PooledBuf::with_capacity(black_box(MISS));
        black_box(std::mem::take(&mut missed.0));
    }
    println!("pool_scan full-scan miss: {:.2} ns", per_iter(t));
    assert_eq!(drain_pool().len(), MAX_POOLED, "every request missed");

    fill();
    assert_eq!(
        PooledBuf::with_capacity(OLDEST).capacity(),
        OLDEST,
        "the oldest entry fits"
    );
    fill();
    let t = Instant::now();
    for _ in 0..ITERS {
        let v = std::mem::take(&mut PooledBuf::with_capacity(black_box(OLDEST)).0);
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
