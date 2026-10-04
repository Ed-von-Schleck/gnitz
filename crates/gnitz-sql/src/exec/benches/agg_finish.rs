use super::tests::plan;
use super::tests::t;
use super::*;
use crate::test_support::Cell::Int;
use gnitz_core::BatchAppender;

/// Instructions per partial row of `combine`, 1M rows under an 8-byte key.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn agg_combine_bench() {
    const N: u64 = 1_000_000;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let two = "SELECT g, COUNT(*), SUM(x) FROM t GROUP BY g";
    let four = "SELECT g, COUNT(*), SUM(x), MIN(x), MAX(x) FROM t GROUP BY g";
    let worker_order = |i: u64| i % (N / 4);
    let interleaved = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) % (N / 4);
    let distinct = |i: u64| i;
    /// `(label, query, row → key, group count)`.
    type Shape<'a> = (&'a str, &'a str, &'a dyn Fn(u64) -> u64, u64);
    let shapes: [Shape; 4] = [
        ("4 partials a group, worker order", two, &worker_order, N / 4),
        ("4 partials a group, interleaved", two, &interleaved, N / 4),
        ("all distinct", two, &distinct, N),
        ("MIN, MAX added, worker order", four, &worker_order, N / 4),
    ];
    for (label, sql, key, groups) in shapes {
        let f = plan(sql, &t());
        let cells = f.partial_schema.num_payload_cols();
        let mut b = ZSetBatch::new(f.partial_schema.as_ref());
        let mut app = BatchAppender::new(&mut b);
        for i in 0..N {
            let k = key(i);
            app.add_row_natives(&[k as u128], 1);
            (0..cells).for_each(|_| Int(k as i128 % 1000).push(&mut app));
        }
        std::hint::black_box(f.combine(b.clone()));
        let (out, instructions) = counter.measure(|| f.combine(b));
        // An interleaved key may repeat past four times; the group count is at most `groups`.
        assert!(out.len() as u64 <= groups && out.len() as u64 > groups / 2, "{label}");
        std::hint::black_box(out);
        println!(
            "agg_combine_bench {label:<34} {:6.1} instr/row",
            instructions as f64 / N as f64
        );
    }
}
