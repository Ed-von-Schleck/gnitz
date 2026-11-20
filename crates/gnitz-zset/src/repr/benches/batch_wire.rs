use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::{
    encode_to_wire_vec, make_batch_bytes, make_schema_pk_u64_payload_string, pk_u64_two_i64_schema, u64_pk_schema,
};
use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;
use std::hint::black_box;

/// Instructions to frame, and to decode in place, 1-row and 100-row blocks of a
/// (u64 PK, two i64 payload) schema: the per-block constant of each.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn wal_block_bench() {
    const ITERS: usize = 100_000;
    let counter = Counter::instructions();
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
        let ((), encode) = counter.measure(|| {
            for _ in 0..ITERS {
                black_box(black_box(&batch).wire_whole().unwrap().encode(black_box(&mut buf)));
            }
        });
        let (decoded, decode) = counter.measure(|| {
            (0..ITERS)
                .map(|_| MemBatch::of_wal_block(black_box(&buf), &schema).unwrap().count)
                .sum::<usize>()
        });
        assert_eq!(decoded, rows * ITERS);
        println!(
            "wal_block_bench {rows} rows: encode {} instr/block, decode {}",
            encode / ITERS as u64,
            decode / ITERS as u64
        );
    }
}

/// Instructions per row of `decode_foreign_wal_block`, the validating decode,
/// over 1000 rows: a fixed-width nullable schema, then one NOT NULL STRING
/// column at each value shape its validation tells apart — inline or in the
/// heap, ASCII or not, and where in a long value the first non-ASCII byte sits.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn foreign_decode_bench() {
    const ROWS: usize = 1000;
    const PASSES: usize = 100;
    let counter = Counter::instructions();
    let measure = |label: &str, batch: Batch| {
        let block = encode_to_wire_vec(&batch);
        let (rows, instructions) = counter.measure(|| {
            (0..PASSES)
                .map(|_| {
                    MemBatch::of_foreign_wal_block(black_box(&block), batch.schema())
                        .unwrap()
                        .to_batch()
                        .len()
                })
                .sum::<usize>()
        });
        assert_eq!(rows, ROWS * PASSES);
        println!(
            "foreign_decode_bench {label:<28} {:7.1} instr/row",
            instructions as f64 / rows as f64
        );
    };

    let mut b = BatchBuilder::new(&u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true)]));
    for i in 0..ROWS {
        b.begin_row(i as u128, 1);
        b.put_opt_int((i % 5 != 0).then_some(i as u128));
        b.end_row();
    }
    measure("nullable i64", b.finish());

    for (label, unit) in [
        ("12 bytes, ASCII", "abcdefghijkl".to_string()),
        ("9 bytes, non-ASCII", "aé€bxy".to_string()),
        ("64 bytes, ASCII", "abcdefgh".repeat(8)),
        ("64 bytes, non-ASCII at the end", format!("{}é", "abcdefgh".repeat(7))),
        ("64 bytes, non-ASCII", "aéb€".repeat(8)),
        ("512 bytes, ASCII", "abcdefgh".repeat(64)),
        ("512 bytes, non-ASCII", "aéb€".repeat(73)),
    ] {
        let rows: Vec<_> = (0..ROWS as u64)
            .map(|i| (i, 1, format!("{}{}", i % 10, &unit[1..])))
            .collect();
        measure(label, make_batch_bytes(&make_schema_pk_u64_payload_string(), &rows));
    }
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

    let long = make_batch_bytes(
        &make_schema_pk_u64_payload_string(),
        &(0..N as u64).map(|i| (i, 1, format!("{i:040}"))).collect::<Vec<_>>(),
    );

    let mut cols = vec![SchemaColumn::new(TypeCode::I64, false); 6];
    cols.push(SchemaColumn::new(TypeCode::String, false));
    let wide_schema = u64_pk_schema(&cols);
    let mut b = BatchBuilder::new(&wide_schema);
    for i in 0..N as u64 {
        b.begin_row(i as u128, 1);
        for c in 0..6u64 {
            b.put_int(i.wrapping_mul(2_654_435_761 + c) as u128);
        }
        match i % 64 {
            0 => {
                b.put_string(&format!("{i:040}"));
            }
            _ => {
                b.put_string(&format!("s{}", i % 1000));
            }
        }
        b.end_row();
    }
    let wide = b.finish();

    let counter = Counter::instructions();
    let mut buf = vec![0u8; 2 * BUDGET];
    for (name, batch) in [("long", &long), ("wide_short", &wide)] {
        let ((frames, bytes), instructions) = counter.measure(|| {
            let (mut start, mut frames, mut bytes) = (0, 0, 0);
            while let Some(frame) = batch.wire_rows_within(start, BUDGET) {
                bytes += frame.encode(&mut buf);
                start += frame.rows();
                frames += 1;
            }
            assert_eq!(start, N, "every row went out");
            (frames, bytes)
        });
        black_box(&buf);
        println!(
            "reply_chunk_strings_bench {name}: {:.1} instr/row, {frames} frames, {bytes} bytes",
            instructions as f64 / N as f64
        );
    }
}

/// Instructions per row of `wire_listed(..).encode` and of
/// `encode_scattered_prefix` over every other row of a 1000-row batch, ascending:
/// a fixed-width schema, a string schema whose values are all inline (an empty
/// heap), and one whose values all live in the heap.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn wire_listed_encode_bench() {
    const ROWS: usize = 1000;
    const PASSES: usize = 1000;
    let mut b = BatchBuilder::new(&pk_u64_two_i64_schema());
    for i in 0..ROWS as u64 {
        b.begin_row(i as u128, 1);
        b.put_int(i as u128);
        b.put_int((i * 3) as u128);
        b.end_row();
    }
    let fixed = b.finish();
    let strings = |value: &dyn Fn(u64) -> String| {
        make_batch_bytes(
            &make_schema_pk_u64_payload_string(),
            &(0..ROWS as u64).map(|i| (i, 1, value(i))).collect::<Vec<_>>(),
        )
    };
    let inline = strings(&|i| format!("s{i}"));
    let heap = strings(&|i| format!("{i:040}"));
    let list: Vec<u32> = (0..ROWS as u32).step_by(2).collect();
    let counter = Counter::instructions();
    for (name, batch) in [("fixed", &fixed), ("inline strings", &inline), ("heap strings", &heap)] {
        let mb = batch.as_mem_batch();
        let mut buf = vec![0u8; 2 * batch.wire_whole().unwrap().byte_size()];
        let (listed, listed_instr) = counter.measure(|| {
            (0..PASSES)
                .map(|_| {
                    black_box(mb)
                        .wire_listed(black_box(&list))
                        .unwrap()
                        .encode(black_box(&mut buf))
                })
                .sum::<usize>()
        });
        let (prefix, prefix_instr) = counter.measure(|| {
            (0..PASSES)
                .map(|_| {
                    black_box(mb)
                        .encode_scattered_prefix(black_box(&list), black_box(&mut buf))
                        .unwrap()
                        .0
                })
                .sum::<usize>()
        });
        assert!(listed > 0);
        assert_eq!(prefix, list.len() * PASSES);
        let per_row = |instr: u64| instr as f64 / (list.len() * PASSES) as f64;
        println!(
            "wire_listed_encode_bench {name:<15} listed {:6.1} instr/row, scattered prefix {:6.1}",
            per_row(listed_instr),
            per_row(prefix_instr)
        );
    }
}
