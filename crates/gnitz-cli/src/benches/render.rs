use super::*;
use crate::value::Rows;
use gnitz_core::{ScanReply, Schema, ZSetBatch};
use gnitz_wire::ColumnDef;
use std::hint::black_box;

/// Instructions to decode a reply and render it in each format: an integer
/// key, a nullable integer, a float and a string per row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn decode_render_bench() {
    const ROWS: u64 = 200_000;
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("n", TypeCode::I64, true),
            ColumnDef::new("f", TypeCode::F64, false),
            ColumnDef::new("s", TypeCode::String, false),
        ],
        &[0],
    )
    .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = batch.builder();
        for i in 0..ROWS {
            let r = a.begin_row(u128::from(i), 1);
            match i % 7 {
                0 => r.put_null(),
                _ => r.put_int((i as i64 * 31 - 5000) as u128),
            };
            r.put_float(i as f64 / 3.0).put_string(if i % 2 == 0 {
                "short"
            } else {
                "a somewhat longer string value"
            });
        }
    }
    let reply = ScanReply {
        schema: std::sync::Arc::new(schema),
        batch,
        lsn: None,
    };
    let counter = gnitz_foundation::perf::Counter::instructions();
    for format in [Format::Csv, Format::Json, Format::Unaligned, Format::Aligned] {
        let settings = Settings { format, ..Settings::default() };
        let target = Target {
            width: None,
            wrap: false,
            color: false,
            header: true,
        };
        let (rows, decode) = counter.measure(|| Rows::decode(black_box(&reply)));
        let (len, n) = counter.measure(|| render(black_box(&rows), Weights::Repeat, &settings, target).len());
        println!(
            "{format:?}: decode {} + render {} instr/row, {len} bytes",
            decode / ROWS,
            n / ROWS
        );
    }
}
