use super::super::test_support::*;
use super::*;

/// Instructions retired per op round trip: submit, pending poll, CQE, ready poll.
/// Run under `perf stat -e instructions:u`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_park_roundtrip_bench() {
    const N: u64 = 1_000_000;
    let r = make_reactor();
    let mut cx = Context::from_waker(Waker::noop());
    let start = Instant::now();
    for _ in 0..N {
        let (u, mut rx) = r.install_op(None);
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        r.dispatch_cqe(u, 0);
        assert!(std::hint::black_box(Pin::new(&mut rx).poll(&mut cx)).is_ready());
    }
    let ns = start.elapsed().as_nanos() as f64 / N as f64;
    println!("op_park_roundtrip_bench: {N} ops, {ns:.1} ns/op");
}
