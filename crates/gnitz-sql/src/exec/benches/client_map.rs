use super::tests::map_of;
use super::*;
use crate::test_support::{col, ncol, schema};
use gnitz_wire::TypeCode;
use gnitz_wire::payload_u64;

/// Instructions of a client map's `apply` over a large reply and over one row, which is all
/// call overhead: an I64 PK column decoded beside a payload column copied, computed, or a
/// computed string, which copies the source heap.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn client_map_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let src = std::sync::Arc::new(schema(
        vec![
            col("k", TypeCode::I64),
            ncol("v", TypeCode::I64),
            ncol("s", TypeCode::String),
        ],
        &[0],
    ));
    for items in [["k", "v"], ["k", "v + 1"], ["k", "COALESCE(s, 'x')"]] {
        for n in [262_144usize, 1] {
            let mut b = ZSetBatch::new(&src);
            let mut app = b.builder();
            for i in 0..n as i64 {
                app.begin_row((i - n as i64 / 2) as u128, 1)
                    .put_int(i as u128)
                    .put_string(&format!("a string past the inline prefix {i}"));
            }
            let mut map = map_of(&src, &items);
            let (out, instructions) = counter.measure(|| map.apply(b));
            assert_eq!(payload_u64(&out.view(), 0, 0) as i64, -(n as i64 / 2), "{items:?}");
            std::hint::black_box(out);
            println!(
                "client_map_bench {:<16} {n:>7} rows {instructions:>8} instr {:6.1} instr/row",
                items[1],
                instructions as f64 / n as f64
            );
        }
    }
}
