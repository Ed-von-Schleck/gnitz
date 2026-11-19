use super::*;
use crate::test_support::{encode_wal_block, wide_rows, wide_schema};
use gnitz_foundation::rng::Rng;
use gnitz_wire::{ColumnDef, TypeCode};
use std::hint::black_box;
use std::sync::Arc;

/// Instructions per key of a key set built from native values: one key column and three,
/// whose encode cost is per column.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_keys_bench() {
    const ROWS: usize = 1_000_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let key_schema = |types: &[TypeCode]| {
        Arc::new(
            Schema::from_parts(
                types.iter().map(|&tc| ColumnDef::new("k", tc, false)).collect(),
                &(0..types.len() as u32).collect::<Vec<u32>>(),
            )
            .unwrap(),
        )
    };
    for types in [&[TypeCode::I64][..], &[TypeCode::I32, TypeCode::U128, TypeCode::I16]] {
        let schema = black_box(key_schema(types));
        let natives: Vec<[u128; 3]> = (0..ROWS as u128).map(|i| [i, i ^ 0x55, i & 0x7fff]).collect();
        let (keys, instr) = counter.measure(|| schema.pk_keys(natives.iter().map(|n| &n[..types.len()])));
        black_box(&keys);
        println!("pk_keys {types:?}: {:.1} instr/key", instr as f64 / ROWS as f64);
    }
}

/// Instructions per gathered row: a full permutation and half of one, over 1, 4 and 16 I64
/// payload columns and over one with a STRING column, whose cells a partial gather relocates.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn gather_bench() {
    const ROWS: usize = 262_144;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    let mut perm: Vec<u32> = (0..ROWS as u32).collect();
    rng.shuffle(&mut perm);
    for (name, ints, string) in [
        ("1 col", 1usize, false),
        ("4 cols", 4, false),
        ("16 cols", 16, false),
        ("1 col + string", 1, true),
    ] {
        let mut columns = vec![ColumnDef::new("id", TypeCode::U64, false)];
        columns.extend((0..ints).map(|_| ColumnDef::new("v", TypeCode::I64, false)));
        columns.extend(string.then(|| ColumnDef::new("s", TypeCode::String, false)));
        let schema = Arc::new(Schema::from_parts(columns, &[0]).unwrap());
        let mut b = ZSetBatch::new(&schema);
        let mut app = b.builder();
        for i in 0..ROWS {
            let r = rng.next_u64();
            app.begin_row(i as u128, 1);
            for _ in 0..ints {
                app.put_int((r as i64) as u128);
            }
            if string {
                app.put_string(if r & 1 == 0 {
                    "short"
                } else {
                    "a string long enough to spill out of line"
                });
            }
        }
        for (label, len) in [("full", ROWS), ("half", ROWS / 2)] {
            std::hint::black_box(b.clone().gather(&perm[..len]));
            // `gather` consumes its batch, so the clone it is handed is measured and subtracted.
            let (copy, clone) = counter.measure(|| b.clone());
            let (out, gather) = counter.measure(|| b.clone().gather(&perm[..len]));
            std::hint::black_box((copy, out));
            println!(
                "gather_bench {name:<16} {label:<5} {:7.1} instr/row",
                (gather - clone) as f64 / len as f64
            );
        }
    }
    // A copy of two rows in three, ascending, out of a batch that stays, row by row and
    // borrowing gather: over 2, 8 and 32 I64 payload columns, and over 8 with a STRING column.
    let picks: Vec<u32> = (0..ROWS as u32).filter(|r| r % 3 != 0).collect();
    for (name, ints, string) in [
        ("2 cols", 2usize, false),
        ("8 cols", 8, false),
        ("32 cols", 32, false),
        ("8 cols + string", 8, true),
    ] {
        let mut columns = vec![ColumnDef::new("id", TypeCode::U64, false)];
        columns.extend((0..ints).map(|_| ColumnDef::new("v", TypeCode::I64, false)));
        columns.extend(string.then(|| ColumnDef::new("s", TypeCode::String, false)));
        let schema = Arc::new(Schema::from_parts(columns, &[0]).unwrap());
        let mut b = ZSetBatch::new(&schema);
        let mut app = b.builder();
        for i in 0..ROWS {
            let r = rng.next_u64();
            app.begin_row(i as u128, 1);
            for _ in 0..ints {
                app.put_int((r as i64) as u128);
            }
            if string {
                app.put_string(if r & 1 == 0 {
                    "short"
                } else {
                    "a string long enough to spill out of line"
                });
            }
        }
        let (rows, copy_row_at) = counter.measure(|| {
            let mut out = ZSetBatch::new(&schema);
            for &r in &picks {
                out.copy_row_at(&b, r as usize, 1);
            }
            out
        });
        black_box(rows);
        let (rows, gathered) = counter.measure(|| b.gathered(&picks));
        black_box(rows);
        println!(
            "gather_bench {name:<16} copy_row_at {:7.1}  gathered {:7.1} instr/row",
            copy_row_at as f64 / picks.len() as f64,
            gathered as f64 / picks.len() as f64
        );
    }
}

/// Instructions per row to decode two payload columns' rows into a batch, as a WAL block
/// copied out of a borrowed frame: a block's parse and validation, over all-short cells,
/// all-long cells and integer columns, which carry no cell to check.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn region_append_bench() {
    const ROWS: usize = 10_000;
    const ITERS: usize = 200;
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (shape, cell) in [
        ("short strings", Some("ab")),
        ("long strings", Some("a value well past the inline cell")),
        ("integers", None),
    ] {
        let tc = cell.map_or(TypeCode::I64, |_| TypeCode::String);
        let schema = Arc::new(
            Schema::from_parts(
                vec![
                    ColumnDef::new("k", TypeCode::U64, false),
                    ColumnDef::new("a", tc, false),
                    ColumnDef::new("b", tc, false),
                ],
                &[0],
            )
            .unwrap(),
        );
        let mut batch = ZSetBatch::new(&schema);
        let mut rows = batch.builder();
        for i in 0..ROWS {
            rows.begin_row(i as u128, 1);
            match cell {
                Some(s) => rows.put_string(s).put_string(s),
                None => rows.put_int((i as i64) as u128).put_int(7),
            };
        }
        let block = encode_wal_block(&batch);
        let ((), instr) = counter.measure(|| {
            for _ in 0..ITERS {
                black_box(decode_borrowed(black_box(&block), &schema));
            }
        });
        println!(
            "region_append_bench {shape}: {:.1} instr/row",
            instr as f64 / (ITERS * ROWS) as f64
        );
    }
}

/// Instructions to decode a reply plus the batch's and the frame's drop: a block copied out of
/// a frame the transport lent (a small reply, read whole in one socket read), and a block
/// adopted with the frame the transport handed over (a reply larger than one read); against
/// the batch's allocation alone ([`ZSetBatch::with_capacity`] plus drop), by column and row
/// count.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reply_alloc_cost_bench() {
    const ROUNDS: u64 = 2000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (ncols, rows) in [(4usize, 1usize), (16, 1), (64, 1), (4, 1000), (16, 1000), (64, 1000)] {
        let schema = wide_schema(ncols, false);
        let block = encode_wal_block(&wide_rows(&schema, rows, 0));
        let (mut copied, mut alloc, mut adopted) = (0, 0, 0);
        for _ in 0..ROUNDS {
            copied += counter
                .measure(|| drop(black_box(decode_borrowed(black_box(&block[..]), &schema))))
                .1;
            alloc += counter
                .measure(|| drop(black_box(ZSetBatch::with_capacity(black_box(&schema), black_box(rows)))))
                .1;
            let frame = FrameBlock {
                frame: std::borrow::Cow::Owned(block.clone()),
                block: 0..block.len(),
            };
            adopted += counter
                .measure(|| {
                    drop(black_box(
                        decode_wal_blocks(Vec::new(), Some(black_box(frame)), &schema).unwrap(),
                    ))
                })
                .1;
        }
        println!(
            "reply_alloc_cost_bench {ncols:>2} cols {rows:>4} rows: copied + drop {:>6} instr, alloc + drop {:>6}, adopted + drop {:>6}",
            copied / ROUNDS,
            alloc / ROUNDS,
            adopted / ROUNDS
        );
    }
}

/// `block` decoded out of a frame the transport lent, as a small reply arrives.
fn decode_borrowed(block: &[u8], schema: &Arc<Schema>) -> ZSetBatch {
    let frame = FrameBlock {
        frame: std::borrow::Cow::Borrowed(block),
        block: 0..block.len(),
    };
    decode_wal_blocks(Vec::new(), Some(frame), schema).unwrap()
}
