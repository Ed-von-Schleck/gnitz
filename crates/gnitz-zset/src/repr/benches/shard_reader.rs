use super::super::batch::Batch;
use super::super::scatter::UnifiedSet;
use super::super::shard_file::ShardWriteOpts;
use super::super::string_heap::should_relocate_blob;
use super::tests::{build, wide_string, write};
use super::*;
use crate::schema::key::probe_key;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_u64_two_i64_schema, u64_pk_schema, Rng,
};
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// The measurement `RELOCATE_CELL_COST_BYTES` is set from: cycles of a
/// whole-heap memcpy against per-cell relocation on the *same* slice, swept over
/// slice fraction × string width, beside the arm the constant picks.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn slice_blob_relocate_bench() {
    const N: usize = 20_000;
    const ITERS: usize = 50;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    let cycles = Counter::cycles();
    let arm = |shard: &MappedShard, rc: usize, relocate: bool| {
        let carried = (!relocate).then(|| shard.blob().len());
        let slice = || drop(black_box(shard.slice_to_owned_batch_with(0, rc, carried)));
        // The first slice faults in the cold mmap pages.
        slice();
        cycles.measure(|| (0..ITERS).for_each(|_| slice())).1 as f64 / ITERS as f64
    };

    for w in [16usize, 40, 256, 1024] {
        let batch = build(schema, N, |b, i| {
            b.begin_row(i as u128 + 1, 1);
            b.put_string(&wide_string(i, w));
        });
        let shard = MappedShard::open(&write(dir.path(), &format!("bench_{w}.db"), &batch), &schema).unwrap();
        for pct in [
            1usize, 2, 3, 4, 6, 8, 12, 16, 20, 25, 33, 40, 50, 60, 68, 75, 85, 90, 99,
        ] {
            let rc = (N * pct / 100).max(1);
            let (reloc, copy) = (arm(&shard, rc, true), arm(&shard, rc, false));
            let picks = match should_relocate_blob(shard.blob().len(), shard.row_count(), rc) {
                true => "relocate",
                false => "memcpy  ",
            };
            println!(
                "width={w:>5} slice={pct:>3}% picks {picks}: \
                 relocate {reloc:9.0} cycles  memcpy {copy:9.0} cycles  speedup {:5.2}x",
                copy / reloc,
            );
        }
    }
}

/// What one shard of each column shape costs on disk, to write and to read
/// back. Per row: the file's bytes, then instructions to write it, to slice it
/// whole and in 1024-row windows, to read every payload cell through the per-row
/// accessor on a fresh handle and again once its blocks are decoded, to read
/// every weight and null word, and to materialize it through a `UnifiedSet` as a
/// compaction does. The regions' stored sizes and encodings name what each
/// shape packed to.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_read_bench() {
    const N: usize = 200_000;
    const WINDOW: usize = 1024;
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
    let i64_col = make_schema_u64_i64();
    let nullable_i64 = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let one_in_ten = |i: usize| i.is_multiple_of(10).then_some(3_000_000_000 + i as u128);

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
        (
            "an i64, ascending",
            build(i64_col, N, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int(3_000_000_000 + i as u128);
            }),
        ),
        (
            "an i64, a value in one row of ten and zero in the rest",
            build(i64_col, N, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int(one_in_ten(i).unwrap_or(0));
            }),
        ),
        (
            "a nullable i64, a value in one row of ten and NULL in the rest",
            build(nullable_i64, N, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_opt_int(one_in_ten(i));
            }),
        ),
        (
            "an i64 of 16 values far apart",
            build(i64_col, N, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int((pick(16) as u128) << 40);
            }),
        ),
        (
            "two i64, one of 1000 values and one ascending",
            build(pk_u64_two_i64_schema(), N, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int((i % 1000) as u128);
                b.put_int(3 * i as u128);
            }),
        ),
        (
            "a nullable i64, one row in five NULL, weights of two values",
            build(nullable_i64, N, |b, i| {
                b.begin_row(i as u128, if i % 3 == 0 { -1 } else { 1 });
                b.put_opt_int((i % 5 != 0).then_some(i as u128));
            }),
        ),
        (
            "a nullable i64, one row in five NULL, weights of seven values",
            build(nullable_i64, N, |b, i| {
                b.begin_row(i as u128, (i % 7) as i64 + 1);
                b.put_opt_int((i % 5 != 0).then_some(i as u128));
            }),
        ),
    ];

    let instructions = Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    for (si, (label, batch)) in shapes.iter().enumerate() {
        let schema = *batch.schema();
        let name = format!("{si}.db");
        let (path, write_i) = instructions.measure(|| write(dir.path(), &name, batch));
        let regions: Vec<String> = ShardDirectory::read(&path)
            .unwrap()
            .regions
            .iter()
            .map(|(role, encoding, size)| format!("{role}:{size}/{encoding}"))
            .collect();
        let shard = MappedShard::open(&path, &schema).unwrap();
        let (_, slice_i) = instructions.measure(|| black_box(shard.slice_to_owned_batch(0, N)));
        let (_, window_i) = instructions.measure(|| {
            for start in (0..N).step_by(WINDOW) {
                black_box(shard.slice_to_owned_batch(start, WINDOW.min(N - start)));
            }
        });
        let cells = || {
            let mut sink = 0usize;
            for row in 0..N {
                for (pi, col) in schema.payload_columns() {
                    sink += match col.type_code.is_german_string() {
                        true => gnitz_wire::payload_bytes(&shard, black_box(row), pi).len(),
                        false => shard.get_col_ptr(black_box(row), pi, col.size() as usize)[0] as usize,
                    };
                }
            }
            black_box(sink)
        };
        let [fresh_i, decoded_i] = [(); 2].map(|()| instructions.measure(cells).1);
        let (_, word_i) = instructions.measure(|| {
            let mut sink = 0u64;
            for row in 0..N {
                sink = sink.wrapping_add(shard.get_weight(black_box(row)) as u64 ^ shard.get_null_word(row));
            }
            black_box(sink)
        });
        let rows: Vec<(u32, u32, i64)> = (0..N as u32).map(|r| (0, r, 1)).collect();
        let (_, merge_i) = instructions.measure(|| {
            let set = UnifiedSet::whole(std::slice::from_ref(&shard), &schema);
            black_box(set.materialize(&rows, N))
        });
        let per_row = |i: u64| i as f64 / N as f64;
        println!(
            "{label}\n  {:.1} bytes per row; instructions per row: write {:.1}, slice {:.1}, windows {:.1}, \
             cells {:.1} then {:.1}, weight and null word {:.1}, materialize {:.1}\n  regions {}",
            per_row(shard.file_len()),
            per_row(write_i),
            per_row(slice_i),
            per_row(window_i),
            per_row(fresh_i),
            per_row(decoded_i),
            per_row(word_i),
            per_row(merge_i),
            regions.join(" "),
        );
    }
}

/// What the PK filter adds to a shard write and costs a probe, at key counts
/// inside and outside the cache. Cycles beside instructions because the
/// filter's construction trades one for the other.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_filter_bench() {
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    for rows in [1usize << 12, 1 << 16, 1 << 20] {
        let batch = build(schema, rows, |b, i| {
            b.begin_row(i as u128, 1);
            b.put_int(i as u128);
        });
        let [with, without] = [false, true].map(|skip_pk_filter| {
            let opts = ShardWriteOpts { skip_pk_filter, ..Default::default() };
            let write = |pass: &str| {
                let path = dir.path().join(format!("{rows}_{skip_pk_filter}_{pass}.db"));
                batch.write_as_shard(path.to_str().unwrap(), opts).unwrap();
                MappedShard::open(path.to_str().unwrap(), &schema).unwrap()
            };
            let (shard, i) = instructions.measure(|| write("instructions"));
            assert_eq!(shard.has_shard_filter(), !skip_pk_filter);
            let per_row = [i, cycles.measure(|| write("cycles")).1].map(|n| n as f64 / rows as f64);
            (shard, per_row)
        });
        let probes: Vec<u64> = (0..rows).map(|row| probe_key(with.0.get_pk_bytes(row))).collect();
        let (admitted, probe) =
            instructions.measure(|| probes.iter().filter(|&&p| with.0.shard_filter_may_contain(p)).count());
        assert_eq!(admitted, rows);
        println!(
            "pk_filter_bench {rows:>7} rows: {:.1} instr/row and {:.1} cycles/row with the filter, \
             {:.1} and {:.1} without, {:.1} instr/probe",
            with.1[0],
            with.1[1],
            without.1[0],
            without.1[1],
            probe as f64 / rows as f64
        );
    }
}
