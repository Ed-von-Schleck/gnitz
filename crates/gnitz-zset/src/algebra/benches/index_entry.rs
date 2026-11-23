use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::{pk_payload_schema, u64_pk_schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;
use std::hint::black_box;

const N: usize = 500_000;

/// Instructions per input row of a secondary index's projection — the span
/// alone ([`extend_spans`]) and the whole entry ([`index_entries`]) — by span
/// shape, NULL density of the indexed columns, and width of the source PK the
/// entry ends in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn index_entries_bench() {
    let counter = Counter::instructions();
    // `live`: the rows NULL in no indexed column, each an entry.
    let measure = |label: &str, batch: &Batch, cols: &[u32], live: usize| {
        let (idx_schema, spec) = KeySpec::new(cols, batch.schema())
            .map(|spec| (spec.index_schema(batch.schema()), spec))
            .unwrap();
        let mb = batch.as_mem_batch();
        let key_size = spec.key_size();
        let spans = || {
            let mut out = Vec::new();
            extend_spans(&mut out, &mb, &spec);
            out
        };
        black_box(spans());
        let (spans, span) = counter.measure(spans);
        assert_eq!(spans.len(), live * key_size, "{label}");
        // A span that leads the PK has no entries of its own: its entry is the source PK.
        let entry = match spec.leads_pk() {
            true => String::from("  n/a"),
            false => {
                black_box(index_entries(batch, &spec, &idx_schema));
                let (entries, entry) = counter.measure(|| index_entries(batch, &spec, &idx_schema));
                assert_eq!(entries.len(), live, "{label}");
                format!("{:5.1}", entry as f64 / N as f64)
            }
        };
        println!(
            "index_entries_bench {label:<40} spans {:5.1}, entries {entry} instr/row ({live} entries)",
            span as f64 / N as f64,
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
            ("U32 payload", &[2]),
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
