//! Microbenchmark of the ad-hoc fold. Ignored by default; run with:
//!
//! ```text
//! cargo test -p gnitz-zset --release adhoc_fold_bench -- --ignored --nocapture --test-threads=1
//! ```

use super::adhoc_fold::AdhocFold;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{AggDescriptor, AggFunc, AggReadSpec};

const N: u64 = 1 << 20;
/// A store's scan chunk.
const CHUNK: u64 = 65_536;

/// Instructions per row of a whole fold — every chunk and the finish — over
/// `[U64 pk | I64 grp | I64 a | I32 b NULL | F64 c]` grouped by `grp`, across
/// group counts and aggregate sets.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn adhoc_fold_bench() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I32, true),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    let mix = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let agg = |col_idx: u32, agg_op: AggFunc| AggDescriptor { col_idx, agg_op };
    let agg_sets: [(&str, Vec<AggDescriptor>); 3] = [
        ("count+sum", vec![AggDescriptor::COUNT_STAR, agg(2, AggFunc::Sum)]),
        (
            "count+3sum+cnn",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(2, AggFunc::Sum),
                agg(3, AggFunc::Sum),
                agg(4, AggFunc::Sum),
                agg(3, AggFunc::CountNonNull),
            ],
        ),
        (
            "count+sum+min+max",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(2, AggFunc::Sum),
                agg(2, AggFunc::Min),
                agg(3, AggFunc::Max),
            ],
        ),
    ];
    let counter = Counter::instructions().expect("instructions counter");
    for groups in [16u64, 4096, 60_000] {
        let chunks: Vec<Batch> = (0..N / CHUNK)
            .map(|c| {
                let mut b = BatchBuilder::new(&schema);
                for i in c * CHUNK..(c + 1) * CHUNK {
                    b.begin_row(i as u128, 1);
                    b.put_int((mix(i) >> 20) as u128 % groups as u128);
                    b.put_int(mix(i ^ 7) as i64 as u128);
                    match i % 5 {
                        0 => b.put_null(),
                        _ => b.put_int(mix(i ^ 9) as u32 as u128),
                    }
                    b.put_float((mix(i) >> 11) as f64 / 3.0);
                    b.end_row();
                }
                let mut b = b.finish();
                b.certify_consolidated();
                b
            })
            .collect();
        for (name, aggs) in &agg_sets {
            let spec = AggReadSpec { group_cols: vec![1], aggs: aggs.clone() };
            let fold = || {
                let mut fold = AdhocFold::new(&schema, &spec, 65_536).unwrap();
                for c in &chunks {
                    fold.fold_ranges(c, &[(0, c.len())]).unwrap();
                }
                fold.finish()
            };
            std::hint::black_box(fold());
            let (out, instructions) = counter.measure(fold);
            std::hint::black_box(out);
            println!(
                "adhoc fold groups={groups:<6} {name:<18} {:>6.1} instr/row",
                instructions as f64 / N as f64
            );
        }
    }
}
