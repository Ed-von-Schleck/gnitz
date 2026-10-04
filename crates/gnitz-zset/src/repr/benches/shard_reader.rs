use super::super::batch::Batch;
use super::super::scatter::UnifiedSet;
use super::tests::build;
use super::tests::wide_string;
use super::tests::write;
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64, u64_pk_schema};
use gnitz_wire::num_regions;

/// The measurement `RELOCATE_CELL_COST_BYTES` is set from: whole-region memcpy
/// against per-cell relocation on the *same* slice, swept over slice fraction ×
/// string width.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn slice_blob_relocate_bench() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    const N: usize = 20_000;
    const ITERS: usize = 50;

    let time_arm = |shard: &MappedShard, rc: usize, relocate: bool| -> f64 {
        let carried = (!relocate).then(|| shard.blob().len());
        // The untimed warmup faults in the cold mmap pages.
        let t = crate::test_support::bench_time(ITERS, || {
            std::hint::black_box(shard.slice_to_owned_batch_with(0, rc, carried));
        });
        t.as_secs_f64() * 1e9 / ITERS as f64
    };

    for &w in &[16usize, 40, 256, 1024] {
        let batch = build(schema, N, |b, i| {
            b.begin_row(i as u128 + 1, 1);
            b.put_string(&wide_string(i, w));
        });
        let shard = MappedShard::open(&write(dir.path(), &format!("bench_{w}.db"), &batch), &schema).unwrap();
        for &pct in &[
            1usize, 2, 3, 4, 6, 8, 12, 16, 20, 25, 33, 40, 50, 60, 68, 75, 85, 90, 99,
        ] {
            let rc = (N * pct / 100).max(1);
            let reloc = time_arm(&shard, rc, true);
            let copy = time_arm(&shard, rc, false);
            let picks = if super::super::string_heap::should_relocate_blob(shard.blob().len(), shard.row_count(), rc) {
                "relocate"
            } else {
                "memcpy  "
            };
            println!(
                "width={w:>5} slice={pct:>3}% picks {picks}: \
                 relocate {reloc:9.0} ns  memcpy {copy:9.0} ns  speedup {:5.2}x",
                copy / reloc,
            );
        }
    }
}

/// Per-pass cost of slicing a packed shard window by window: a framed column,
/// and a column holding a value in one row of ten as a frame over its zeroes
/// and as its non-NULL cells alone.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_slice_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    const WINDOW: usize = 1024;
    const HANDLES: usize = 20;
    let nullable = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    fn value(i: usize) -> Option<u128> {
        Some(3_000_000_000 + i as u128)
    }
    fn tenth(i: usize) -> Option<u128> {
        value(i).filter(|_| i.is_multiple_of(10))
    }
    type Value = fn(usize) -> Option<u128>;
    let shapes: [(&str, SchemaDescriptor, Value); 3] = [
        ("framed", make_schema_u64_i64(), value),
        ("a value in one row of ten, framed", make_schema_u64_i64(), |i| {
            tenth(i).or(Some(0))
        }),
        ("a value in one row of ten, the rest NULL", nullable, tenth),
    ];
    let dir = tempfile::tempdir().unwrap();
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    for (label, schema, value) in shapes {
        let batch = build(schema, N, |b, i| {
            b.begin_row(i as u128, 1);
            b.put_opt_int(value(i));
        });
        let path = write(dir.path(), "for_slice.db", &batch);
        let handles: Vec<MappedShard> = (0..HANDLES)
            .map(|_| MappedShard::open(&path, &schema).unwrap())
            .collect();
        assert!(matches!(handles[0].col_regions[0], PayloadRegion::Packed(_)));
        let slice_all = |shard: &MappedShard| {
            for start in (0..N).step_by(WINDOW) {
                black_box(shard.slice_to_owned_batch(start, WINDOW.min(N - start)));
            }
        };
        println!("{label}: {} bytes", handles[0].file_len());
        for pass in ["first pass", "second pass"] {
            let (((), i), c) = cycles.measure(|| instructions.measure(|| handles.iter().for_each(slice_all)));
            println!(
                "  {pass}: {} cycles, {} instructions per pass",
                c / HANDLES as u64,
                i / HANDLES as u64,
            );
        }
        std::fs::remove_file(&path).unwrap();
    }
}

/// Instructions per row to read a weight and a null word through the per-row
/// accessors, and to decode both regions in one whole slice, by the encoding
/// the two regions take.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn word_region_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    type Row = fn(usize) -> (i64, Option<u128>);
    let shapes: [(&str, Row); 3] = [
        ("constant", |i| (1, Some(i as u128))),
        ("two-value", |i| {
            (if i % 3 == 0 { -1 } else { 1 }, (i % 5 != 0).then_some(i as u128))
        }),
        ("for", |i| ((i % 7) as i64 + 1, (i % 5 != 0).then_some(i as u128))),
    ];
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions().unwrap();
    for (label, row) in shapes {
        let batch = build(schema, N, |b, i| {
            let (weight, value) = row(i);
            b.begin_row(i as u128, weight);
            b.put_opt_int(value);
        });
        let path = write(dir.path(), "words.db", &batch);
        let shard = MappedShard::open(&path, &schema).unwrap();
        let (sum, per_row) = instructions.measure(|| {
            let mut sum = 0u64;
            for r in 0..N {
                sum = sum.wrapping_add(shard.get_weight(black_box(r)) as u64 ^ shard.get_null_word(r));
            }
            sum
        });
        black_box(sum);
        let (_, slice) = instructions.measure(|| black_box(shard.slice_to_owned_batch(0, N)));
        println!(
            "{label} weight: {:.2} instructions per row for both per-row reads, {:.2} for a whole slice",
            per_row as f64 / N as f64,
            slice as f64 / N as f64,
        );
        std::fs::remove_file(&path).unwrap();
    }
}

/// First-touch cost of a per-row read on a packed column: a fresh handle, one
/// `get_col_ptr` per FoR column at a mid row, then the same read again.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_point_touch_bench() {
    use crate::test_support::{pk_u64_two_i64_schema, settled_rss};
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    let schema = pk_u64_two_i64_schema();
    let dir = tempfile::tempdir().unwrap();
    let batch = build(schema, N, |b, i| {
        b.begin_row(i as u128, 1);
        b.put_int((i % 1000) as u128);
        b.put_int(3 * i as u128);
    });
    let path = write(dir.path(), "point.db", &batch);
    let shard = MappedShard::open(&path, &schema).unwrap();
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    let read = || {
        for pi in 0..2 {
            black_box(shard.get_col_ptr(black_box(N / 2), pi, 8));
        }
    };
    let rss0 = settled_rss();
    for label in ["cold", "warm"] {
        let (((), i), c) = cycles.measure(|| instructions.measure(read));
        let retained = settled_rss().saturating_sub(rss0);
        println!("{label}: {i} instructions, {c} cycles; {retained} bytes retained");
    }
    // Every block: the second pass is the read of a block already held.
    for pass in ["every row, first pass", "every row, second pass"] {
        let ((), i) = instructions.measure(|| {
            for row in 0..N {
                for pi in 0..2 {
                    black_box(shard.get_col_ptr(black_box(row), pi, 8));
                }
            }
        });
        println!("{pass}: {} instructions per read", i / (2 * N as u64));
    }
}

/// What one shard of each string shape costs on disk, to write and to read
/// back. Per row: the file's bytes, then instructions to write it, to slice it
/// whole and in 1024-row windows, to read every string cell through the per-row
/// accessor, and to materialize it through a `UnifiedSet` as a compaction does.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_string_footprint_bench() {
    use crate::test_support::Rng;
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 200_000;
    let str_col = SchemaColumn::new(TypeCode::String, false);
    let events = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            str_col,
            str_col,
            str_col,
            str_col,
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let one_string = make_schema_pk_u64_payload_string();
    let nullable_string = u64_pk_schema(SchemaColumn::new(TypeCode::String, true));

    let tenants: Vec<String> = (0..50).map(|i| format!("tenant-{i:03}")).collect();
    let urls: Vec<String> = (0..2_000)
        .map(|i| format!("https://app.example.com/api/v2/resources/{:09}/items/{i:05}", i * 7919))
        .collect();
    let agents: Vec<String> = (0..30)
        .map(|i| format!("Mozilla/5.0 (X11; Linux x86_64; rv:{i}.0) Gecko/20100101 Firefox/{i}.0 build-{i:04}"))
        .collect();
    let statuses = ["ok", "ok", "ok", "client_error", "server_error_upstream_timeout"];

    let mut rng = Rng::new(7);
    let mut pick = |n: usize| rng.gen_range(n as u64) as usize;
    let shapes: Vec<(&str, Batch)> = vec![
        (
            "four repeating columns (50 inline, 2000 long, 30 long, 3 mixed) and an i64",
            build(events, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&tenants[pick(50)]);
                b.put_string(&urls[pick(2_000)]);
                b.put_string(&agents[pick(30)]);
                b.put_string(statuses[pick(5)]);
                b.put_int(pick(1000) as u128);
            }),
        ),
        (
            "one column, distinct 60-byte values",
            build(one_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&wide_string(i, 60));
            }),
        ),
        (
            "one column, distinct inline values",
            build(one_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&format!("r{i:09}"));
            }),
        ),
        (
            "one nullable column, distinct inline values, one row in twenty NULL",
            build(nullable_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                match i % 20 {
                    7 => b.put_null(),
                    _ => b.put_string(&format!("r{i:09}")),
                }
            }),
        ),
        (
            "one nullable column, distinct 60-byte values, one row in twenty NULL",
            build(nullable_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                match i % 20 {
                    7 => b.put_null(),
                    _ => b.put_string(&wide_string(i, 60)),
                }
            }),
        ),
        (
            "one column, 60-byte values drawn from N/2",
            build(one_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&wide_string(pick(N / 2), 60));
            }),
        ),
    ];

    let instructions = Counter::instructions().unwrap();
    let dir = tempfile::tempdir().unwrap();
    for (si, (label, batch)) in shapes.iter().enumerate() {
        let schema = *batch.schema();
        let name = format!("{si}.db");
        let (path, write_i) = instructions.measure(|| write(dir.path(), &name, batch));
        let image = std::fs::read(&path).unwrap();
        let regions: Vec<String> = (0..=num_regions(schema.num_payload_cols()))
            .map(|r| {
                let Span { size, encoding, .. } = spans_of(&image)[r];
                format!("{size}/{encoding:?}")
            })
            .collect();
        let shard = MappedShard::open(&path, &schema).unwrap();
        let (_, slice_i) = instructions.measure(|| black_box(shard.slice_to_owned_batch(0, N)));
        let (_, window_i) = instructions.measure(|| {
            for start in (0..N).step_by(1024) {
                black_box(shard.slice_to_owned_batch(start, 1024.min(N - start)));
            }
        });
        let (content, cell_i) = instructions.measure(|| {
            let mut content = 0usize;
            for row in 0..N {
                for (pi, col) in schema.payload_columns() {
                    if col.type_code.is_german_string() {
                        content += gnitz_expr::payload_bytes(&shard, row, pi).len();
                    }
                }
            }
            content
        });
        let rows: Vec<(u32, u32, i64)> = (0..N as u32).map(|r| (0, r, 1)).collect();
        let (_, merge_i) = instructions.measure(|| {
            let set = UnifiedSet::whole(std::slice::from_ref(&shard), &schema);
            black_box(set.materialize(&rows, N))
        });
        let per_row = |i: u64| i / N as u64;
        println!(
            "{label}: {N} rows of {content} string bytes\n  {} bytes, {:.1} per row; instructions per row: \
             write {}, slice {}, windows {}, cells {}, materialize {}\n  regions {}",
            image.len(),
            image.len() as f64 / N as f64,
            per_row(write_i),
            per_row(slice_i),
            per_row(window_i),
            per_row(cell_i),
            per_row(merge_i),
            regions.join(" "),
        );
    }
}
