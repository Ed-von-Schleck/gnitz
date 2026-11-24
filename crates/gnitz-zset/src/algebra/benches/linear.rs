//! Instruction benchmarks for the linear operators' row copies.

use super::*;
use crate::repr::BatchBuilder;
use crate::test_support::{certified, prose, u64_pk_schema};
use std::hint::black_box;

/// A null-extend of a consolidated batch of a nullable integer, a nullable
/// string (inline and heap-held) and an integer column by two fill columns, on
/// either side.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn null_extend_bench() {
    let n = gnitz_foundation::env::env_num("GNITZ_BENCH_ROWS", 65_536);
    let counter = gnitz_foundation::perf::Counter::instructions();
    let col = SchemaColumn::new;
    let own = [
        col(TypeCode::I64, true),
        col(TypeCode::String, true),
        col(TypeCode::I32, false),
    ];
    let in_schema = u64_pk_schema(&own);
    let mut b = BatchBuilder::new(&in_schema);
    for i in 0..n {
        b.begin_row(i as u128, 1);
        b.put_opt_int((i % 5 != 0).then_some(i as u128));
        match i % 3 {
            0 => b.put_null(),
            1 => b.put_string("short"),
            _ => b.put_string(&prose(i)),
        };
        b.put_int(i as u128 & 0xffff);
        b.end_row();
    }
    let input = certified(b.finish());
    for nulls_first in [false, true] {
        let out_schema =
            null_extend_output_schema(&in_schema, &[TypeCode::I64, TypeCode::String], nulls_first).unwrap();
        black_box(null_extend(&input, &out_schema, nulls_first));
        let (out, instructions) = counter.measure(|| null_extend(black_box(&input), &out_schema, nulls_first));
        assert_eq!(out.len(), n);
        println!(
            "null_extend_bench nulls_first={nulls_first:<5} {:8.2} instr/row",
            instructions as f64 / n as f64
        );
    }
}
