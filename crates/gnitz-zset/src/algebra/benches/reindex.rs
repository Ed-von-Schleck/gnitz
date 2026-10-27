use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::{pk_payload_schema, u64_pk_schema};
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

/// Instructions per input row of a secondary index's projection — the span
/// alone ([`spans`]) and the whole entry ([`index_entries`]) — by span
/// shape, NULL density of the indexed columns, and width of the source PK the
/// entry ends in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn index_entries_bench() {
    let counter = Counter::instructions();
    // `live`: the rows NULL in no indexed column, each an entry.
    let measure = |label: &str, batch: &Batch, cols: &[u32], live: usize| {
        let (spec, idx_schema) = crate::schema::key::KeySpec::new(cols, batch.schema())
            .map(|spec| (spec, spec.index_schema(batch.schema())))
            .unwrap();
        let mb = batch.as_mem_batch();
        let key_size = spec.key_size();
        black_box(spans(&mb, &spec));
        let (spans, span) = counter.measure(|| spans(&mb, &spec));
        black_box(index_entries(batch, &spec, &idx_schema));
        let (entries, entry) = counter.measure(|| index_entries(batch, &spec, &idx_schema));
        assert_eq!((entries.len(), spans.len()), (live, live * key_size), "{label}");
        println!(
            "index_entries_bench {label:<40} spans {:5.1}, entries {:5.1} instr/row ({live} entries)",
            span as f64 / N as f64,
            entry as f64 / N as f64,
        );
    };
    let spread = |row: usize| (row as u64).wrapping_mul(2_654_435_761);

    let schema = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::U32, true),
    ]);
    for null_every in [0usize, 8, 2] {
        let mut b = BatchBuilder::new(&schema);
        for row in 0..N {
            let live = null_every == 0 || row % null_every != 0;
            b.begin_row(row as u128, 1);
            b.put_opt_int(live.then(|| spread(row) as i64 as u128));
            b.put_opt_int(live.then(|| spread(row) as u32 as u128));
            b.end_row();
        }
        let batch = b.finish();
        let live = match null_every {
            0 => N,
            k => N - N.div_ceil(k),
        };
        for (shape, cols) in [
            ("I64 payload", &[1u32][..]),
            ("U32 payload, 8-byte slot", &[2]),
            ("compound (PK, I64)", &[0, 1]),
            // No indexed column is nullable, so one density is every density.
            ("U64 PK", &[0]),
        ] {
            if cols == [0] && null_every != 0 {
                continue;
            }
            measure(&format!("{shape}, NULL every {null_every}"), &batch, cols, live);
        }
    }

    let wide = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let mut b = BatchBuilder::new(&wide);
    for row in 0..N {
        b.begin_row_natives(&[(row >> 8) as u128, row as u128], 1);
        b.put_int(spread(row) as i64 as u128);
        b.end_row();
    }
    measure("I64 payload, 16-byte source PK", &b.finish(), &[2], N);
}
