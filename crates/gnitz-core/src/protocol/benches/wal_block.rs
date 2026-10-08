use super::*;
use crate::BatchAppender;
use gnitz_wire::{ColumnDef, TypeCode};
use std::hint::black_box;

/// Instructions per row to append a region list of two payload columns to an
/// empty batch, as a foreign list and as the process's own: what the German
/// cell check costs, over all-short cells, all-long cells and integer columns,
/// which carry no cell to check.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn append_regions_bench() {
    const ROWS: usize = 10_000;
    const ITERS: usize = 200;
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (shape, cell) in [
        ("short strings", Some("ab")),
        ("long strings", Some("a value well past the inline cell")),
        ("integers", None),
    ] {
        let tc = if cell.is_some() {
            TypeCode::String
        } else {
            TypeCode::I64
        };
        let schema = Schema::from_parts(
            vec![
                ColumnDef::new("k", TypeCode::U64, false),
                ColumnDef::new("a", tc, false),
                ColumnDef::new("b", tc, false),
            ],
            &[0],
        )
        .unwrap();
        let mut batch = ZSetBatch::new(&schema);
        let mut rows = BatchAppender::new(&mut batch);
        for i in 0..ROWS {
            rows.add_row(i as u128, 1);
            match cell {
                Some(s) => rows.str_val(s).str_val(s),
                None => rows.i64_val(i as i64).i64_val(7),
            };
        }
        let regions = batch.wire_regions();
        for foreign in [true, false] {
            let ((), instr) = counter.measure(|| {
                for _ in 0..ITERS {
                    let mut sink = ZSetBatch::with_capacity(&schema, ROWS);
                    append_regions(&mut sink, black_box(&regions), &schema, black_box(foreign)).unwrap();
                    black_box(&sink);
                }
            });
            println!(
                "append_regions_bench {shape}, foreign={foreign}: {:.1} instr/row",
                instr as f64 / (ITERS * ROWS) as f64
            );
        }
    }
}
