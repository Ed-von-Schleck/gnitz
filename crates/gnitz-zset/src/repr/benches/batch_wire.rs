use super::tests::string_batch;
use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{encode_to_wire_vec, make_schema_pk_u64_payload_string, pk_u64_two_i64_schema};

/// Encode then decode 1-row and 100-row blocks of a (u64 PK, two i64 payload)
/// schema: the per-block constant of the framer and the decoder.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn wal_block_bench() {
    use std::hint::black_box;
    const ITERS: usize = 1_000_000;
    let schema = pk_u64_two_i64_schema();
    for rows in [1usize, 100] {
        let mut b = BatchBuilder::new(&schema);
        for i in 0..rows as u64 {
            b.begin_row(i as u128, 1);
            b.put_int(i as u128);
            b.put_int((i * 3) as u128);
            b.end_row();
        }
        let batch = b.finish();
        let mut buf = vec![0u8; batch.wire_whole().unwrap().byte_size()];

        let t = crate::test_support::bench_time(ITERS, || {
            let n = black_box(&batch).wire_whole().unwrap().encode(black_box(&mut buf));
            let block = MemBatch::of_wal_block(black_box(&buf[..n]), &schema).unwrap();
            black_box(block.count);
        });
        println!(
            "wal block {rows} rows: encode + decode {:.1} ns",
            t.as_nanos() as f64 / ITERS as f64
        );
    }
}

/// Retired instructions for `decode_foreign_wal_block` over 1000 rows of one
/// STRING column. `GNITZ_BENCH_SHAPE` picks the value; difference two
/// `GNITZ_BENCH_PASSES` counts under `perf stat -e instructions:u`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn foreign_decode_string_bench() {
    let shape = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "s12".to_string());
    let passes = gnitz_foundation::perf::bench_passes();
    let unit = match shape.as_str() {
        "s12" => "abcdefghijkl".to_string(),
        "u9" => "aé€bxy".to_string(),
        "s64" => "abcdefgh".repeat(8),
        "m64" => format!("{}é", "abcdefgh".repeat(7)),
        "u64" => "aéb€".repeat(8),
        "s512" => "abcdefgh".repeat(64),
        "u512" => "aéb€".repeat(73),
        other => panic!("GNITZ_BENCH_SHAPE must be s12/u9/s64/m64/u64/s512/u512, got {other:?}"),
    };
    let schema = make_schema_pk_u64_payload_string();
    let mut b = BatchBuilder::new(&schema);
    for i in 0..1000u64 {
        b.begin_row(i as u128, 1);
        let mut v = unit.clone().into_bytes();
        v[0] = b'0' + (i % 10) as u8;
        b.put_string(std::str::from_utf8(&v).unwrap());
        b.end_row();
    }
    let block = encode_to_wire_vec(&b.finish());
    let mut acc = 0usize;
    for _ in 0..passes {
        acc += Batch::decode_foreign_wal_block(std::hint::black_box(&block), &schema)
            .unwrap()
            .len();
    }
    println!(
        "foreign_decode_string_bench shape={shape} passes={passes} acc={}",
        std::hint::black_box(acc)
    );
}

/// Retired instructions to drain a 10⁵-row batch frame by frame at a 64 KiB
/// budget, each frame sized by `wire_rows_within` and then encoded. Two shapes:
/// every row a long 40-byte string, and a wide fixed row whose string is short
/// on all but one row in 64.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reply_chunk_strings_bench() {
    const N: usize = 100_000;
    const BUDGET: usize = 64 << 10;

    let long = string_batch(&(0..N as u64).map(|i| (i, format!("{i:040}"))).collect::<Vec<_>>());

    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend([SchemaColumn::new(TypeCode::I64, false); 6]);
    cols.push(SchemaColumn::new(TypeCode::String, false));
    let wide_schema = SchemaDescriptor::new(&cols, &[0]);
    let mut b = BatchBuilder::new(&wide_schema);
    for i in 0..N as u64 {
        b.begin_row(i as u128, 1);
        for c in 0..6u64 {
            b.put_int(i.wrapping_mul(2_654_435_761 + c) as u128);
        }
        match i % 64 {
            0 => b.put_string(&format!("{i:040}")),
            _ => b.put_string(&format!("s{}", i % 1000)),
        }
        b.end_row();
    }
    let wide = b.finish();

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let mut buf = vec![0u8; 2 * BUDGET];
    for (name, batch) in [("long", &long), ("wide_short", &wide)] {
        let ((frames, bytes), instructions) = counter.measure(|| {
            let (mut start, mut frames, mut bytes) = (0, 0, 0);
            while let Some(frame) = batch.wire_rows_within(start, BUDGET) {
                bytes += frame.encode(&mut buf);
                start += frame.rows();
                frames += 1;
            }
            (frames, bytes)
        });
        std::hint::black_box(&buf);
        println!("reply_chunk_strings_bench {name}: {instructions} instr, {frames} frames, {bytes} bytes");
    }
}
