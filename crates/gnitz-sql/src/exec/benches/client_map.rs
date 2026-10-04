use super::tests::map_of;
use super::*;
use crate::test_support::{col, ncol, schema};
use gnitz_core::BatchAppender;
use gnitz_expr::payload_u64;
use gnitz_wire::TypeCode;

/// Instructions of a client map's `apply` over a large reply and over one row, which is all
/// call overhead: an I64 PK column decoded beside a payload column moved, then computed.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn client_map_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let src = schema(vec![col("k", TypeCode::I64), ncol("v", TypeCode::I64)], &[0]);
    for (items, moves) in [(["k", "v"], true), (["k", "v + 1"], false)] {
        for n in [262_144usize, 1] {
            let mut b = ZSetBatch::new(&src);
            let mut app = BatchAppender::new(&mut b);
            for i in 0..n as i64 {
                app.add_row((i - n as i64 / 2) as u128, 1).i64_val(i);
            }
            let mut map = map_of(&src, &items);
            std::hint::black_box(map.apply(b.clone()));
            let v = b.payload[0].bytes.as_ptr();
            let (out, instructions) = counter.measure(|| map.apply(b));
            assert_eq!(payload_u64(&out, 0, 0) as i64, -(n as i64 / 2), "{items:?}");
            assert_eq!(out.payload[1].bytes.as_ptr() == v, moves, "{items:?}");
            std::hint::black_box(out);
            println!(
                "client_map_bench {:<6} {n:>7} rows {instructions:>8} instr {:6.1} instr/row",
                items[1],
                instructions as f64 / n as f64
            );
        }
    }
}
