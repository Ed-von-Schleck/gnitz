use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::u64_pk_schema;
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;
use std::hint::black_box;

const N: usize = 500_000;

/// Instructions and cycles per row of [`FoldCols::key_row`] by column count,
/// over `[U64 PK, I64, U32 NULL, I64, I64, I64]` — the cycles the least of
/// several passes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn fold_key_bench() {
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    let schema = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::U32, true),
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::I64, false),
    ]);
    let mut b = BatchBuilder::new(&schema);
    for i in 0..N as u64 {
        b.begin_row(i as u128, 1);
        b.put_int((i as i64).wrapping_mul(-7) as u128);
        b.put_opt_int((i % 8 != 0).then_some(i as u32 as u128));
        b.put_int(i.wrapping_mul(2_654_435_761) as u128);
        b.put_int((!i) as u128);
        b.put_int(i as u128);
        b.end_row();
    }
    let b = b.finish();
    let mb = &b.as_mem_batch();
    for cols in 1..=5usize {
        let fold = FoldCols::new((1..=cols).map(|c| schema.locate(c)).collect());
        let run = || {
            let mut acc = 0u128;
            for row in 0..N {
                acc ^= fold.key_row(mb, row);
            }
            black_box(acc)
        };
        run();
        let (_, instr) = instructions.measure(run);
        let cyc = (0..9).map(|_| cycles.measure(run).1).min().unwrap();
        println!(
            "fold_key_bench {cols} columns {:6.1} instr/row {:6.1} cycles/row",
            instr as f64 / N as f64,
            cyc as f64 / N as f64
        );
    }
}
