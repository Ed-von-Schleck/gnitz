//! Microbenchmarks for the reduce-time AVI population hot path. Ignored by
//! default; run with:
//!
//! ```text
//! cargo test -p gnitz-store --release secondary_index -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Lives inside `reduce` so the decomposition calls the production `avi_batch`
//! rather than a hand-rolled twin. Every layer is timed directly and their sum is
//! printed beside the independently timed full path, never substituted for it.
//! The memtable upsert is not a layer: on a pre-consolidated batch
//! `ingest_owned_batch` does no per-row work at all.
//!
//! The numbers describe one shape — a single 500k-row batch, one MIN over an I64
//! payload grouped by a U32 payload (AVI stride 13, inside the `u128` sort-key
//! arm), into a fresh table at the default budgets. Production
//! `GROUP BY <BIGINT>` is stride 17, one byte past that arm, and a view backfill
//! chunks at ~16k rows per worker, where the sort's share is lower. A second
//! shape runs the same decomposition over a wide MAX, whose key slot and BLOB
//! image payload the scalar one does not pay.

use std::time::Duration;

use super::avi::{avi_batch, AviBake};
use super::plan::ReducePlan;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES};
use crate::storage::{Batch, BatchBuilder};
use crate::test_support::{bench_time, bench_time_each, scratch_table};
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
    let mut b = BatchBuilder::new(*schema);
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
    let mut b = BatchBuilder::new(*schema);
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
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
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
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
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
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
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
        let mut b = BatchBuilder::new(schema);
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
    let tmp = tempfile::tempdir().unwrap();

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
    // A fresh table per iteration, opened outside the clock: an ingest into a
    // table already holding 500k entries would be measuring the memtable's
    // growth, not the population.
    let full = bench_time_each(
        ITERS,
        || scratch_table(tmp.path().to_str().unwrap(), avi_schema),
        |mut t| {
            t.ingest_owned_batch(avi_batch(input, bake)).unwrap();
            std::hint::black_box(&t);
        },
    );

    report(label, population, sort, full);
}

/// Time the `into_consolidated` sort layer for a single-column PK schema
/// (`[pk, I64 val]`). `pk_bytes_for(row)` yields the 8-byte LE PK; payload is a
/// scrambled I64 so the payload tiebreak is exercised. Hashed PKs keep the input
/// unsorted (real sort work) with occasional folds.
fn bench_single_pk_sort(label: &str, pk_schema: SchemaDescriptor, pk_bytes_for: impl Fn(usize) -> [u8; 8]) {
    let build = || {
        let mut out = Batch::with_capacity(&pk_schema, N_ROWS);
        for row in 0..N_ROWS {
            out.begin_row(&pk_bytes_for(row), 1);
            out.extend_col(0, &((row as i64).wrapping_mul(2654435761)).to_le_bytes());
            out.commit_row(0);
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

/// Single-U64 PK: the `pk_is_fast` path that must NOT regress. Guards against
/// accidental routing of the fast path through the OPK encoder.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_single_u64_pk_sort_bench() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    bench_single_pk_sort("single-U64 PK (fast path)", schema, |row| {
        (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes()
    });
}

/// Per-row cost of composing one secondary-index entry key
/// (`KeySpec::write_entry` = leading-key span ‖ source-PK suffix).
///
/// Four shapes, chosen to separate the encode paths: a **U64 PK** source (no
/// promotion — the span is a verbatim copy of the OPK bytes already in the PK
/// region), an **I64 payload** source (no promotion, native LE straight into the
/// encoder), a **U32 payload → U64** source (real width promotion), and a
/// **compound (U64 PK, I64 payload)** two-column span.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn index_write_span_bench() {
    use crate::schema::KeySpec;

    // `src_schema()` / `build_input` from the top of this file: U64 pk (col 0) |
    // U32 (col 1) | I64 (col 2), 500k rows. The four shapes map onto it directly.
    let src = src_schema();
    let input = build_input(&src);
    let mb = input.as_mem_batch();

    println!("\nKeySpec::write_entry — per-row cost ({ITERS}x{N_ROWS} rows):");
    for (label, cols) in [
        ("U64 PK        (identity)", &[0u32][..]),
        ("I64 payload   (direct)  ", &[2][..]),
        ("U32 payload   (promoted)", &[1][..]),
        ("compound (PK, I64)      ", &[0, 2][..]),
    ] {
        let spec = KeySpec::new(cols, &src).unwrap();
        let elapsed = bench_time(ITERS, || {
            let mut key = [0u8; MAX_PK_BYTES];
            for row in 0..N_ROWS {
                std::hint::black_box(spec.write_entry(&mb, row, &mut key));
                std::hint::black_box(&key);
            }
        });
        let ns = ns_per_row(elapsed);
        println!("  {label}  {ns:7.2} ns/row   ({:.2} Mrows/s)", 1000.0 / ns);
    }
}

/// Single-I64 PK: the signed arm, where the sign flip makes the OPK encode
/// non-verbatim.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_single_i64_pk_sort_bench() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    bench_single_pk_sort("single-I64 PK (OPK path)", schema, |row| {
        // Cast to i64 spreads the hash across negatives and positives.
        ((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64).to_le_bytes()
    });
}
