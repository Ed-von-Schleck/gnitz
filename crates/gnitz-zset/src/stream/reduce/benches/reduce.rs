//! Microbenchmarks for the reduce: `op_reduce` itself, and the population of
//! the value index (AVI) its MIN/MAX aggregates read. Ignored by default; run
//! one with:
//!
//! ```text
//! cargo test -p gnitz-zset --release <name>_bench -- --ignored --nocapture --test-threads=1
//! ```
//!
//! The population benches time every layer directly and print their sum beside
//! the independently timed full path, never substituted for it. Keeping the
//! folded batch as a trace run is not a layer: it does no per-row work at all.
//!
//! Their numbers describe one shape — a single 500k-row batch, one MIN over an
//! I64 payload grouped by a U32 payload (AVI stride 13, inside the `u128`
//! sort-key arm), into a fresh trace. Production
//! `GROUP BY <BIGINT>` is stride 17, one byte past that arm, and a view backfill
//! chunks at ~16k rows per worker, where the sort's share is lower. A second
//! shape runs the same decomposition over a wide MAX, whose key slot and BLOB
//! image payload the scalar one does not pay.

use std::time::{Duration, Instant};

use super::avi::{avi_batch, AviBake};
use super::plan::ReducePlan;
use super::tests::Harness;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{bench_time, bench_time_each, pk_payload_schema, pk_u64_two_i64_schema, TestTrace};
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

const N_ROWS: usize = 500_000;
const N_GROUPS: u64 = 10_000;
const ITERS: usize = 40;

/// Source schema: U64 pk (col 0) | U32 grp (col 1) | I64 val (col 2).
/// AVI groups by col 1 and aggregates MIN over col 2.
fn src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

fn build_input(schema: &SchemaDescriptor) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for row in 0..N_ROWS as u64 {
        b.begin_row(row as u128, 1i64);
        b.put_int(((row % N_GROUPS) as u32) as u128);
        b.put_int(((row.wrapping_mul(2654435761)) as i64) as u128);
        b.end_row();
    }
    b.finish()
}

/// Source schema for the wide shape: U64 pk | U32 grp | U128 val.
fn wide_src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[0],
    )
}

/// [`build_input`] over [`wide_src_schema`], the value scrambled across the
/// whole 128-bit range.
fn build_wide_input(schema: &SchemaDescriptor) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for row in 0..N_ROWS as u64 {
        let v = (row.wrapping_mul(0x9E37_79B9_7F4A_7C15) as u128) << 64 | row as u128;
        b.begin_row(row as u128, 1);
        b.put_int((row % N_GROUPS) as u128);
        b.put_int(v);
        b.end_row();
    }
    b.finish()
}

/// The production bake for one extreme over `col` grouped by `group`, reached
/// the way the compiler reaches it — through the plan that owns the
/// accumulators the bake reads.
fn extreme_bake(schema: &SchemaDescriptor, group: &[u32], col: u32, agg_op: AggFunc) -> AviBake {
    let aggs = [AggDescriptor { col_idx: col, agg_op }, AggDescriptor::COUNT_STAR];
    ReducePlan::from_wire(schema, group, &aggs, false)
        .unwrap()
        .avi
        .expect("a MIN/MAX reduce is value-indexed")
}

fn ns_per_row(elapsed: Duration) -> f64 {
    elapsed.as_nanos() as f64 / (N_ROWS * ITERS) as f64
}

fn report(index: &str, population: Duration, sort: Duration, full: Duration) {
    let (p, s, f) = (ns_per_row(population), ns_per_row(sort), ns_per_row(full));
    println!("\n{index} population — per-row cost decomposition ({ITERS}x{N_ROWS} rows):");
    println!("  population (avi_batch)   {p:7.2} ns/row   {:5.1}%", 100.0 * p / f);
    println!("  sort (into_consolidated) {s:7.2} ns/row   {:5.1}%", 100.0 * s / f);
    println!("  -----");
    // Both layers are timed independently of `full`, so their sum is a check on
    // the decomposition rather than a restatement of it.
    println!("  layer sum                {:7.2} ns/row", p + s);
    println!(
        "  avi_batch + ingest (full)   {f:7.2} ns/row   ({:.2} Mrows/s)",
        1000.0 / f
    );
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn secondary_index_avi_decomposition_bench() {
    let schema = src_schema();
    decompose(
        "AVI (U32 grp, I64 val)",
        extreme_bake(&schema, &[1], 2, AggFunc::Min),
        build_input(&schema),
    );
}

/// The wide shape, under the scalar one's group key.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn secondary_index_avi_wide_decomposition_bench() {
    let schema = wide_src_schema();
    decompose(
        "AVI (U32 grp, U128 val)",
        extreme_bake(&schema, &[1], 2, AggFunc::Max),
        build_wide_input(&schema),
    );
}

/// One MIN over an I64 across packed group-key shapes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn secondary_index_avi_group_shape_bench() {
    for (label, group) in [
        ("I32 NOT NULL", &[(TypeCode::I32, false)][..]),
        ("I32 nullable", &[(TypeCode::I32, true)][..]),
        ("I64 nullable", &[(TypeCode::I64, true)][..]),
        ("2xI32 NOT NULL", &[(TypeCode::I32, false); 2][..]),
        ("2xI64 nullable", &[(TypeCode::I64, true); 2][..]),
    ] {
        let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
        cols.extend(group.iter().map(|&(tc, nullable)| SchemaColumn::new(tc, nullable)));
        cols.push(SchemaColumn::new(TypeCode::I64, false));
        let schema = SchemaDescriptor::new(&cols, &[0]);
        let mut b = BatchBuilder::new(&schema);
        for row in 0..N_ROWS as u64 {
            b.begin_row(row as u128, 1);
            for (i, &(tc, nullable)) in group.iter().enumerate() {
                let g = (row + i as u64 * 7) % N_GROUPS;
                match nullable && row % 16 == i as u64 {
                    true => b.put_null(),
                    false if tc == TypeCode::I32 => b.put_int((g as i32 - 5_000) as u32 as u128),
                    false => b.put_int((g as i64 - 5_000) as u64 as u128),
                }
            }
            b.put_int(((row.wrapping_mul(2654435761)) as i64) as u128);
            b.end_row();
        }
        let group_cols: Vec<u32> = (1..=group.len() as u32).collect();
        let val = group.len() as u32 + 1;
        decompose(
            &format!("AVI ({label} grp, I64 val)"),
            extreme_bake(&schema, &group_cols, val, AggFunc::Min),
            b.finish(),
        );
    }
}

/// The per-layer decomposition of one bake's population, timed and reported.
fn decompose(label: &str, bake: AviBake, input: Batch) {
    let avi_schema = bake.schema;
    let bake = &bake;
    let input = &input;

    let population = bench_time(ITERS, || {
        std::hint::black_box(avi_batch(input, bake));
    });
    let sort = bench_time_each(
        ITERS,
        || avi_batch(input, bake),
        |b| {
            std::hint::black_box(b.into_consolidated());
        },
    );
    // A fresh trace per iteration, built outside the clock.
    let full = bench_time_each(
        ITERS,
        || TestTrace::new(avi_schema),
        |mut t| {
            t.ingest(avi_batch(input, bake));
            std::hint::black_box(&t);
        },
    );

    report(label, population, sort, full);
}

/// Time the `into_consolidated` sort layer for a single-column PK schema
/// (`[pk, I64 val]`). `pk_bytes_for(row)` is the stored 8-byte key; the payload
/// is a scrambled I64 so the payload tiebreak is exercised. Hashed PKs keep the
/// input unsorted (real sort work) with occasional folds.
fn bench_single_pk_sort(label: &str, pk_schema: SchemaDescriptor, pk_bytes_for: impl Fn(usize) -> [u8; 8]) {
    let build = || {
        let mut out = Batch::with_capacity(&pk_schema, N_ROWS);
        for row in 0..N_ROWS {
            out.begin_row(&pk_bytes_for(row), 1);
            out.extend_col(0, &((row as i64).wrapping_mul(2654435761)).to_le_bytes());
            out.commit_row();
        }
        out
    };
    let sort = bench_time(ITERS, || {
        std::hint::black_box(build().into_consolidated());
    });
    let s = ns_per_row(sort);
    println!(
        "\n{label} — sort (into_consolidated): {s:7.2} ns/row   ({:.2} Mrows/s)",
        1000.0 / s,
    );
}

/// The sort of a batch keyed on one 8-byte column.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn secondary_index_single_u64_pk_sort_bench() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    bench_single_pk_sort("single 8-byte PK", schema, |row| {
        (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes()
    });
}

/// Per-row cost of projecting a secondary index's entries (`index_entries` =
/// leading-key span ‖ source-PK suffix, per live row).
///
/// Four span shapes, chosen to separate the encode paths: a **U64 PK** (the span
/// is the OPK bytes already in the PK region), an **I64 payload** (native LE
/// into the encoder), a **U32 payload** (a 4-byte span) and a **compound (U64
/// PK, I64 payload)** two-column span — then the suffix copy: a 16-byte source
/// PK, and an index on a nullable column at three NULL densities, which cut the
/// rows into runs.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn index_entries_bench() {
    use crate::algebra::index_entries;
    use crate::schema::index_spec_and_schema;

    let counter = gnitz_foundation::perf::Counter::instructions();
    let measure = |label: &str, input: &Batch, cols: &[u32]| {
        let (spec, idx_schema) = index_spec_and_schema(cols, input.schema()).unwrap();
        std::hint::black_box(index_entries(input, &spec, &idx_schema));
        let (out, instructions) = counter.measure(|| index_entries(input, &spec, &idx_schema));
        std::hint::black_box(out);
        println!("  {label:<32} {:7.1} instr/row", instructions as f64 / N_ROWS as f64);
    };

    // `src_schema()` / `build_input` from the top of this file: U64 pk (col 0) |
    // U32 (col 1) | I64 (col 2), 500k rows. The four shapes map onto it directly.
    let src = src_schema();
    let input = build_input(&src);
    println!("\nindex_entries — per-row cost ({N_ROWS} rows):");
    for (label, cols) in [
        ("U64 PK        (identity)", &[0u32][..]),
        ("I64 payload   (direct)", &[2][..]),
        ("U32 payload   (4-byte span)", &[1][..]),
        ("compound (PK, I64)", &[0, 2][..]),
    ] {
        measure(label, &input, cols);
    }

    let wide = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let mut b = BatchBuilder::new(&wide);
    for row in 0..N_ROWS as u64 {
        b.begin_row_natives(&[(row >> 8) as u128, row as u128], 1);
        b.put_int(row.wrapping_mul(2654435761) as i64 as u128);
        b.end_row();
    }
    measure("16-byte PK, I64 payload", &b.finish(), &[2]);

    let nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    for null_every in [0u64, 8, 2] {
        let mut b = BatchBuilder::new(&nullable);
        for row in 0..N_ROWS as u64 {
            b.begin_row(row as u128, 1);
            match null_every != 0 && row % null_every == 0 {
                true => b.put_null(),
                false => b.put_int(row.wrapping_mul(2654435761) as i64 as u128),
            }
            b.end_row();
        }
        measure(&format!("nullable I64, NULL every {null_every}"), &b.finish(), &[1]);
    }
}

/// `op_reduce` over `delta` against `h`'s state, alone on the clock: the index
/// population the VM runs ahead of it is not.
fn time_op_reduce(h: &mut Harness, delta: &Batch) -> (Batch, Duration) {
    h.index(delta);
    let start = Instant::now();
    let out = h.reduce(delta);
    (out, start.elapsed())
}

/// `op_reduce` over `d2`, against the state `d1` left behind: its wall time and
/// the instructions it retired.
fn time_second_epoch(plan: ReducePlan, d1: &Batch, d2: &Batch) -> (Duration, u64) {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let mut h = Harness::new(plan);
    let (out, _) = time_op_reduce(&mut h, d1);
    h.trace_out.ingest(out);
    h.index(d2);
    let start = Instant::now();
    let (out, instructions) = counter.measure(|| h.reduce(d2));
    let warm = start.elapsed();
    std::hint::black_box(out);
    (warm, instructions)
}

/// `delta` as the VM hands it to `plan`'s reduce: folded unless every aggregate
/// is exact linear.
fn as_read(plan: &ReducePlan, delta: Batch) -> Batch {
    match plan.is_exact_linear() {
        true => delta,
        false => delta.into_consolidated(),
    }
}

/// Times `op_reduce` over a 1M-row delta per shape, against an empty and a
/// populated trace. `OP_REDUCE_BENCH_SHAPE=<label>` runs one shape alone, for
/// `perf stat`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_bench() {
    const N: u64 = 1 << 20;
    let only = std::env::var("OP_REDUCE_BENCH_SHAPE").ok();
    let mix = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let grp_val = pk_u64_two_i64_schema();
    let single_pk = pk_payload_schema(&[TypeCode::U64]);
    let compound_pk = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let agg = |col_idx: u32, agg_op: AggFunc| [AggDescriptor { col_idx, agg_op }, AggDescriptor::COUNT_STAR];

    // `[U64 pk, I64 grp, I64 val]` rows, raw.
    let grp_rows = |salt: u64, grp: &dyn Fn(u64) -> u64| {
        let mut bb = BatchBuilder::new(&grp_val);
        for i in 0..N {
            bb.begin_row(mix(i + salt * N) as u128, 1);
            bb.put_int(grp(i) as u128);
            bb.put_int(mix(i ^ salt) as i64 as u128);
            bb.end_row();
        }
        bb.finish()
    };
    // PK-sorted rows over `schema` (one or two U64 PK columns, one I64 value),
    // certified consolidated.
    let sorted_rows = |schema: SchemaDescriptor, salt: u64| {
        let mut bb = BatchBuilder::new(&schema);
        for i in 0..N {
            match schema.pk_cols().len() {
                1 => bb.begin_row(i as u128, 1),
                _ => bb.begin_row_natives(&[(i / 16) as u128, (i % 16) as u128], 1),
            }
            bb.put_int((i + salt) as u128);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_consolidated();
        b
    };

    type Shape<'a> = (
        &'a str,
        SchemaDescriptor,
        Vec<u32>,
        [AggDescriptor; 2],
        Box<dyn Fn(u64) -> Batch + 'a>,
    );
    let shapes: Vec<Shape> = vec![
        (
            "keyed_u64_sum",
            grp_val,
            vec![1],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|i| i % 65_536)),
        ),
        // Unsorted groups at the row counts per group the hashed and the sorted
        // group numbering trade places at.
        (
            "keyed_distinct_sum",
            grp_val,
            vec![1],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|i| mix(i ^ 0x55))),
        ),
        (
            "keyed_2_per_group_sum",
            grp_val,
            vec![1],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|i| mix((i / 2) ^ 0x55))),
        ),
        (
            "keyed_4_scattered_sum",
            grp_val,
            vec![1],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|i| mix(i % (N / 4)))),
        ),
        (
            "keyed_256_sum",
            grp_val,
            vec![1],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|i| mix(i) % 256)),
        ),
        (
            "source_pk",
            single_pk,
            vec![0],
            agg(1, AggFunc::Sum),
            Box::new(|s| sorted_rows(single_pk, s)),
        ),
        (
            "leading_pk_col",
            compound_pk,
            vec![0],
            agg(2, AggFunc::Sum),
            Box::new(|s| sorted_rows(compound_pk, s)),
        ),
        (
            "leading_pk_min",
            compound_pk,
            vec![0],
            agg(2, AggFunc::Min),
            Box::new(|s| sorted_rows(compound_pk, s)),
        ),
        (
            "ungrouped_sum",
            grp_val,
            vec![],
            agg(2, AggFunc::Sum),
            Box::new(|s| grp_rows(s, &|_| 0)),
        ),
        (
            "ungrouped_min",
            grp_val,
            vec![],
            agg(2, AggFunc::Min),
            Box::new(|s| grp_rows(s, &|_| 0)),
        ),
        (
            "keyed_min_8",
            grp_val,
            vec![1],
            agg(2, AggFunc::Min),
            Box::new(|s| grp_rows(s, &|i| i / 8)),
        ),
    ];
    let shapes: Vec<Shape> = shapes
        .into_iter()
        .filter(|s| only.as_deref().is_none_or(|o| o == s.0))
        .collect();
    assert!(!shapes.is_empty(), "OP_REDUCE_BENCH_SHAPE names no shape");
    for (label, schema, group, aggs, make) in &shapes {
        let plan = || ReducePlan::from_wire(schema, group, aggs, false).unwrap();
        let (d1, d2) = (as_read(&plan(), make(1)), as_read(&plan(), make(2)));
        let (out, cold) = time_op_reduce(&mut Harness::new(plan()), &d2);
        std::hint::black_box(out);
        let (warm, instructions) = time_second_epoch(plan(), &d1, &d2);
        let per_row = instructions as f64 / d2.count as f64;
        println!("op_reduce {label}: empty trace {cold:?}, populated trace {warm:?}, {per_row:.1} instr/row");
    }
}

/// Times `op_reduce` over a 1M-row delta against a populated trace, for SUM and
/// MIN across packed group-key shapes. `REDUCE_SWEEP=<label>` runs one alone,
/// for `perf stat`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_group_sweep_bench() {
    const N: u64 = 1 << 20;
    let only = std::env::var("REDUCE_SWEEP").ok();
    let mix = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let shapes: [(&str, &[(TypeCode, bool)]); 6] = [
        ("i32_notnull", &[(TypeCode::I32, false)]),
        ("i32_nullable", &[(TypeCode::I32, true)]),
        ("i64_nullable", &[(TypeCode::I64, true)]),
        ("2xi32_notnull", &[(TypeCode::I32, false); 2]),
        ("2xi64_nullable", &[(TypeCode::I64, true); 2]),
        ("3xi64_nullable", &[(TypeCode::I64, true); 3]),
    ];
    let mut ran = 0;
    for (shape, group) in shapes {
        let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
        cols.extend(group.iter().map(|&(tc, nullable)| SchemaColumn::new(tc, nullable)));
        cols.push(SchemaColumn::new(TypeCode::I64, false));
        let schema = SchemaDescriptor::new(&cols, &[0]);
        let group_cols: Vec<u32> = (1..=group.len() as u32).collect();
        let val = group.len() as u32 + 1;
        let rows = |salt: u64| {
            let mut bb = BatchBuilder::new(&schema);
            for i in 0..N {
                bb.begin_row(mix(i + salt * N) as u128, 1);
                for (c, &(tc, nullable)) in group.iter().enumerate() {
                    let g = (i >> (4 * c)) % 256;
                    match nullable && i % 16 == c as u64 {
                        true => bb.put_null(),
                        false if tc == TypeCode::I32 => bb.put_int((g as i32 - 128) as u32 as u128),
                        false => bb.put_int((g as i64 - 128) as u64 as u128),
                    }
                }
                bb.put_int(mix(i ^ salt) as i64 as u128);
                bb.end_row();
            }
            bb.finish()
        };
        for agg_op in [AggFunc::Sum, AggFunc::Min] {
            let label = format!("{shape}_{agg_op:?}").to_lowercase();
            if only.as_deref().is_some_and(|o| o != label) {
                continue;
            }
            ran += 1;
            let aggs = [AggDescriptor { col_idx: val, agg_op }, AggDescriptor::COUNT_STAR];
            let plan = ReducePlan::from_wire(&schema, &group_cols, &aggs, false).unwrap();
            let (d1, d2) = (as_read(&plan, rows(1)), as_read(&plan, rows(2)));
            let (warm, instructions) = time_second_epoch(plan, &d1, &d2);
            let per_row = instructions as f64 / d2.count as f64;
            println!("op_reduce_group_sweep {label}: populated trace {warm:?}, {per_row:.1} instr/row");
        }
    }
    assert!(ran > 0, "REDUCE_SWEEP names no shape");
}

/// `op_reduce` over a three-run trace_out whose two older runs lie wholly below the
/// delta's first output key: the delta touches every group of the newest run and
/// adds a new group between each pair.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_multi_run_bench() {
    const G: u64 = 1 << 14;
    const RUNS: usize = 200;
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum },
        AggDescriptor::COUNT_STAR,
    ];
    let rows = |keys: &[u64]| {
        let mut bb = BatchBuilder::new(&schema);
        for &k in keys {
            bb.begin_row(k as u128, 1);
            bb.put_int(k as u128);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_consolidated();
        b
    };
    let mut h = Harness::new(ReducePlan::from_wire(&schema, &[0], &aggs, false).unwrap());
    let runs: [Vec<u64>; 3] = [
        (0..2 * G).step_by(2).collect(),
        (1..2 * G).step_by(2).collect(),
        (2 * G..4 * G).step_by(2).collect(),
    ];
    for keys in &runs {
        let (out, _) = time_op_reduce(&mut h, &rows(keys));
        h.trace_out.ingest(out);
    }
    let delta = rows(&(2 * G..4 * G).collect::<Vec<_>>());
    let counter = gnitz_foundation::perf::Counter::instructions();
    let mut instructions = 0;
    for _ in 0..RUNS {
        let (out, n) = counter.measure(|| h.reduce(&delta));
        std::hint::black_box(out);
        instructions += n;
    }
    println!("op_reduce multi-run: {} instr/iter", instructions / RUNS as u64);
}
