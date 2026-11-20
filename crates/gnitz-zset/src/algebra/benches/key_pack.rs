use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::u64_pk_schema;
use gnitz_foundation::perf::{Counters, Event};
use gnitz_wire::TypeCode;
use std::hint::black_box;

const N: usize = 500_000;

/// Instructions and cycles per row of [`FoldCols::key_row`] and of
/// [`FoldCols::identities`] by column count, over `[U64 PK, I64, U32 NULL, I64,
/// I64, I64]` — each count the least of several passes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn fold_key_bench() {
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    let least = |f: &dyn Fn() -> u128| {
        (0..9)
            .map(|_| counters.measure(f).1)
            .reduce(|[i, c], [ni, nc]| [i.min(ni), c.min(nc)])
            .unwrap()
    };
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
        let row = || {
            let mut acc = 0u128;
            for row in 0..N {
                acc ^= fold.key_row(mb, row);
            }
            black_box(acc)
        };
        let chunked = || {
            let mut acc = 0u128;
            let mut keys = [0u128; FOLD_CHUNK];
            for start in (0..N).step_by(FOLD_CHUNK) {
                let keys = &mut keys[..(N - start).min(FOLD_CHUNK)];
                fold.identities(mb, start, keys);
                acc = keys.iter().fold(acc, |acc, &k| acc ^ k);
            }
            black_box(acc)
        };
        assert_eq!(row(), chunked(), "identities digests as key_row does");
        let [instr, cyc] = least(&row);
        let [chunk_instr, chunk_cyc] = least(&chunked);
        println!(
            "fold_key_bench {cols} columns key_row {:6.1} instr/row {:6.1} cycles/row   chunked {:6.1} instr/row {:6.1} cycles/row",
            instr as f64 / N as f64,
            cyc as f64 / N as f64,
            chunk_instr as f64 / N as f64,
            chunk_cyc as f64 / N as f64
        );
    }
}
