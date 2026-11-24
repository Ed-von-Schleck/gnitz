//! Instruction benchmarks for a column copy's null words.

use super::ColMoves;
use crate::schema::SchemaColumn;
use crate::test_support::u64_pk_schema;
use gnitz_expr::ColCopy;
use gnitz_wire::TypeCode;
use std::hint::black_box;

/// The null words of eight nullable columns copied: a column prefix, every
/// column moved one slot down, and the columns reversed.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn copy_nulls_bench() {
    let n = gnitz_foundation::env::env_num("GNITZ_BENCH_ROWS", 200_000);
    let counter = gnitz_foundation::perf::Counter::instructions();
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true); 8]);
    // Payload slot `pi` of row `row` is NULL on every fifth (row, pi) step.
    let src: Vec<u8> = (0..n)
        .flat_map(|row| {
            (0..8)
                .filter(|pi| (row * 7 + pi * 13) % 5 == 0)
                .fold(0u64, |w, pi| w | 1 << pi)
                .to_le_bytes()
        })
        .collect();
    let mut dst = vec![0u8; n * 8];
    let shapes: [(&str, Vec<usize>); 3] = [
        ("prefix", (0..7).collect()),
        ("shift", (1..8).collect()),
        ("reversed", (0..8).rev().collect()),
    ];
    for (name, srcs) in shapes {
        let copies = srcs
            .iter()
            .enumerate()
            .map(|(slot, &pi)| ColCopy {
                src: schema.payload_locator(pi),
                slot,
                width: 8,
            })
            .collect();
        let moves = ColMoves::new(copies, &schema);
        moves.write_null_rows(&src, 0, &mut dst, 0, n);
        let (_, instructions) = counter.measure(|| moves.write_null_rows(black_box(&src), 0, &mut dst, 0, n));
        black_box(&dst);
        println!(
            "copy_nulls_bench {name:<10} {:8.2} instr/row",
            instructions as f64 / n as f64
        );
    }
}
