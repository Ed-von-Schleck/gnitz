use super::*;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions per cell of [`push_zero_cell`] filling a column of each
/// fixed width, the way a writer fills a NULL.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn push_zero_cell_bench() {
    const N: usize = 1 << 20;
    let counter = Counter::instructions();
    for tc in [TypeCode::U8, TypeCode::I64, TypeCode::U128, TypeCode::String] {
        let mut col = Vec::with_capacity(N * tc.wire_stride());
        let (_, count) = counter.measure(|| {
            for _ in 0..N {
                push_zero_cell(black_box(&mut col), black_box(tc));
            }
        });
        black_box(&col);
        println!("push_zero_cell_bench {tc:?} {:.2} instr/cell", count as f64 / N as f64);
    }
}
