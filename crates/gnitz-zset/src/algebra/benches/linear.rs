//! Instruction benchmarks for the linear operators' row copies.

use super::*;
use crate::repr::BatchBuilder;
use crate::test_support::{certified, prose};
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
    let pk = col(TypeCode::U64, false);
    let own = [
        col(TypeCode::I64, true),
        col(TypeCode::String, true),
        col(TypeCode::I32, false),
    ];
    let fill = [col(TypeCode::I64, true), col(TypeCode::String, true)];
    let in_schema = SchemaDescriptor::new(&[&[pk][..], &own].concat(), &[0]);
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
        let cols = match nulls_first {
            true => [&[pk][..], &fill, &own].concat(),
            false => [&[pk][..], &own, &fill].concat(),
        };
        let out_schema = SchemaDescriptor::new(&cols, &[0]);
        black_box(null_extend(&input, &out_schema, nulls_first));
        let (out, instructions) = counter.measure(|| null_extend(black_box(&input), &out_schema, nulls_first));
        assert_eq!(out.len(), n);
        println!(
            "null_extend_bench nulls_first={nulls_first:<5} {:8.2} instr/row",
            instructions as f64 / n as f64
        );
    }
}
