//! Instruction benchmarks for a copy-only map's null words.

use crate::LogicalProgram;
use crate::test_support::{Meter, TestView, bench_rows, fill, schema_pk_ints};

/// The null words of a copy-only map over eight nullable columns: a
/// column prefix, every column moved one slot down, and the columns reversed.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn copy_nulls_bench() {
    let meter = Meter::new();
    let n = bench_rows();
    let in_schema = schema_pk_ints(8, true);
    let view = fill(
        &in_schema,
        n,
        |row, col| (row + col) as i64,
        |row, col| (row * 7 + col * 13) % 5 == 0,
    );
    let shapes: [(&str, Vec<u32>); 3] = [
        ("prefix", (1..=7).collect()),
        ("shift", (2..=8).collect()),
        ("reversed", (1..=8).rev().collect()),
    ];
    for (name, cols) in shapes {
        let out_schema = schema_pk_ints(cols.len(), true);
        let mut map = LogicalProgram::copy_cols(&cols)
            .resolve_map(&in_schema, &out_schema)
            .expect("a copy-only map");
        let mut out = TestView::new(&out_schema, n);
        meter.pass("copy_nulls_bench", name, n, || {
            map.write_computed(&view, 0, n, &mut out, 0)
        });
    }
}
