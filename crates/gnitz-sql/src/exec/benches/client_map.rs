use super::tests::map_of;
use super::*;
use crate::test_support::{col, ncol, schema};
use gnitz_core::BatchAppender;
use gnitz_expr::payload_u64;
use gnitz_wire::TypeCode;

/// Instructions per reply row of a map keeping one I64 PK column beside a payload move.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn client_map_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let src = schema(vec![col("k", TypeCode::I64), ncol("v", TypeCode::I64)], &[0]);
    for n in [1_000_000usize, 256] {
        let mut b = ZSetBatch::new(&src);
        let mut app = BatchAppender::new(&mut b);
        for i in 0..n as i64 {
            app.add_row((i - n as i64 / 2) as u128, 1).i64_val(i);
        }
        let mut map = map_of(&src, &["k", "v"]);
        std::hint::black_box(map.apply(b.clone()));
        let (out, instructions) = counter.measure(|| map.apply(b));
        assert_eq!(payload_u64(&out, n - 1, 0) as i64, n as i64 - 1 - n as i64 / 2);
        std::hint::black_box(out);
        println!(
            "client_map_bench {n:>8} rows {:6.1} instr/row",
            instructions as f64 / n as f64
        );
    }
}
