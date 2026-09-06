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
//! arm), into a fresh table whose memtable never flushes mid-run. Production
//! `GROUP BY <BIGINT>` is stride 17, one byte past that arm, and a view backfill
//! chunks at ~16k rows per worker, where the sort's share is lower.

use std::time::Duration;

use super::avi::{avi_batch, op_populate_avi, AviBake};
use super::plan::ReducePlan;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_PK_BYTES};
use crate::storage::{Batch, RamBudgets, RecoverySource, Table};
use crate::test_support::{bench_time, bench_time_each};
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

const N_ROWS: usize = 500_000;
const N_GROUPS: u64 = 10_000;
const ITERS: usize = 40;

/// Memtable byte budget. Default 1 GiB isolates the in-memory cost (no flush
/// during the timed region). Override with `GNITZ_BENCH_MEMTABLE_KB` to measure
/// the production-representative path, where the real budget (a few hundred KiB)
/// flushes shards to disk mid-population.
fn memtable_budget() -> usize {
    crate::foundation::env::env_num("GNITZ_BENCH_MEMTABLE_KB", 1 << 20) * 1024
}

/// Source schema: U64 pk (col 0) | U32 grp (col 1) | I64 val (col 2).
/// AVI groups by col 1 and aggregates MIN over col 2.
fn src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn build_input(schema: &SchemaDescriptor) -> Batch {
    let mut b = Batch::with_capacity(schema, N_ROWS);
    for row in 0..N_ROWS as u64 {
        b.extend_pk(row as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_col(0, &((row % N_GROUPS) as u32).to_le_bytes());
        b.extend_col(1, &((row.wrapping_mul(2654435761)) as i64).to_le_bytes());
        b.commit_row(0);
    }
    b
}

/// The production bake for `MIN(val) GROUP BY grp`, reached the way the compiler
/// reaches it — through the plan that owns the accumulators the bake reads.
fn min_bake(schema: &SchemaDescriptor) -> AviBake {
    let group = [1u32];
    let aggs = [AggDescriptor { col_idx: 2, agg_op: AggFunc::Min }];
    ReducePlan::new(schema, &group, &aggs, schema.reduce_out_key(&group), false, false)
        .unwrap()
        .avi
        .expect("a MIN reduce is value-indexed")
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
        "  op_populate_avi (full)   {f:7.2} ns/row   ({:.2} Mrows/s)",
        1000.0 / f
    );
}

#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_avi_decomposition_bench() {
    let schema = src_schema();
    let bake = min_bake(&schema);
    let avi_schema = bake.schema;
    let input = build_input(&schema);
    let tmp = tempfile::tempdir().unwrap();

    let population = bench_time(ITERS, || {
        std::hint::black_box(avi_batch(&input, &bake));
    });
    let sort = bench_time_each(
        ITERS,
        || avi_batch(&input, &bake),
        |b| {
            std::hint::black_box(b.into_consolidated(&avi_schema));
        },
    );
    // A fresh table per iteration, opened outside the clock: an ingest into a
    // table already holding 500k entries would be measuring the memtable's
    // growth, not the population.
    let mut id = 2000u32;
    let full = bench_time_each(
        ITERS,
        || {
            id += 1;
            Table::with_budgets(
                tmp.path().to_str().unwrap(),
                avi_schema,
                id,
                RecoverySource::Rederive { resume_at: None },
                RamBudgets {
                    memtable_bytes: memtable_budget(),
                    ..Default::default()
                },
            )
            .unwrap()
        },
        |mut t| {
            op_populate_avi(&input, &mut t, &bake).unwrap();
            std::hint::black_box(&t);
        },
    );

    report("AVI (U32 grp)", population, sort, full);
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
        std::hint::black_box(build().into_consolidated(&pk_schema));
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    bench_single_pk_sort("single-U64 PK (fast path)", schema, |row| {
        (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes()
    });
}

/// Per-row cost of composing one secondary-index entry key
/// (`IndexKeySpec::write_entry` = leading-key span ‖ source-PK suffix) — the
/// only bench that reaches the resolved-addressing free functions
/// (`pk_native_key` / `payload_native_key`) and the `ColumnLocator` accessors
/// they sit behind.
///
/// Four shapes, chosen to separate the encode paths: a **U64 PK** source (no
/// promotion — the span is a verbatim copy of the OPK bytes already in the PK
/// region), an **I64 payload** source (no promotion, native LE straight into the
/// encoder), a **U32 payload → U64** source (real width promotion), and a
/// **compound (U64 PK, I64 payload)** two-column span.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn index_write_span_bench() {
    use crate::schema::IndexKeySpec;

    // `src_schema()` / `build_input` from the top of this file: U64 pk (col 0) |
    // U32 (col 1) | I64 (col 2), 500k rows. The four shapes map onto it directly.
    let src = src_schema();
    let input = build_input(&src);
    let mb = input.as_mem_batch();

    println!("\nIndexKeySpec::write_entry — per-row cost ({ITERS}x{N_ROWS} rows):");
    for (label, cols) in [
        ("U64 PK        (identity)", &[0u32][..]),
        ("I64 payload   (direct)  ", &[2][..]),
        ("U32 payload   (promoted)", &[1][..]),
        ("compound (PK, I64)      ", &[0, 2][..]),
    ] {
        let spec = IndexKeySpec::new(cols, &src).unwrap();
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

    // What the identity arm buys, priced directly: the same unpromoted U64 PK
    // column through `promote_opk_column`'s general decode∘encode path. Without
    // this row the four numbers above have nothing to be compared against.
    let stride = src.pk_stride();
    for (label, identity) in [("identity (copy)   ", true), ("decode∘encode     ", false)] {
        let elapsed = bench_time(ITERS, || {
            let mut key = [0u8; MAX_PK_BYTES];
            for row in 0..N_ROWS {
                let s = &mb.get_pk_bytes(row)[..stride];
                if identity {
                    gnitz_wire::promote_opk_column(s, type_code::U64, type_code::U64, &mut key[..stride]);
                } else {
                    let native = gnitz_wire::decode_pk_column_owned(s, type_code::U64);
                    gnitz_wire::encode_pk_column_promoted(
                        &native[..stride],
                        type_code::U64,
                        type_code::U64,
                        &mut key[..stride],
                    );
                }
                std::hint::black_box(&key);
            }
        });
        let ns = ns_per_row(elapsed);
        println!("  unpromoted PK OPK→OPK, {label}  {ns:7.2} ns/row");
    }
}

/// Single-I64 PK: the signed arm, where the sign flip makes the OPK encode
/// non-verbatim.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_single_i64_pk_sort_bench() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    bench_single_pk_sort("single-I64 PK (OPK path)", schema, |row| {
        // Cast to i64 spreads the hash across negatives and positives.
        ((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64).to_le_bytes()
    });
}
