//! Microbenchmarks for the two delta-trace join probes. Ignored by default;
//! wall-clock on a contended box is not decisive, so take every A/B from
//! instructions retired:
//!
//! ```text
//! perf stat -e instructions:u -- cargo test -p gnitz-engine --release join_ \
//!     -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three fixture axes are swept because without them the benches cannot see what
//! they exist to measure: **source count** (a single-source cursor never reaches
//! the `with_payload_cmp!` dispatch that only `Multi` re-runs per row), **a
//! German-string payload** (no blob cache exists without one), and **a
//! `PayloadCmpKind::Generic` schema**, so both comparator monomorphizations are
//! compiled and timed.
//!
//! The delta is certified `Consolidated`, so the timed region is the probe and
//! the emit, not a sort.

use std::rc::Rc;
use std::time::Duration;

use super::join::merge_schemas_for_join;
use super::{op_join_delta_trace, JoinProbe, RangeProbe};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout, ReadCursor};
use crate::test_support::bench_time;
use gnitz_wire::RangeRel;

/// Rows on the larger side of every fixture. Big enough that the per-call
/// preamble (one `Batch::with_capacity`, one blob-cache acquire) is noise
/// against the per-row walk, small enough that a full sweep stays interactive.
const N: usize = 4096;

const ITERS: usize = 20;

/// Source counts swept per shape: a `Single`-mode cursor and a `Multi`-mode one.
const SOURCE_COUNTS: [usize; 2] = [1, 4];

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The payload shape a fixture carries. Each selects a different kernel path:
/// the comparator monomorphization, and whether a blob cache exists at all.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Payload {
    /// One non-nullable I64 — `PayloadCmpKind::FixedIntNonnull`, no blob heap.
    Int,
    /// I64 + a nullable I64 — `Generic`, still no blob heap.
    Nullable,
    /// I64 + a STRING — `Generic`, and the only shape whose emit relocates
    /// German strings through the dedup cache.
    Str,
}

impl Payload {
    fn tag(self) -> &'static str {
        match self {
            Payload::Int => "int",
            Payload::Nullable => "nullable",
            Payload::Str => "string",
        }
    }
}

/// `pk_types` PK columns followed by `p`'s payload columns. Column 0 of the
/// payload is always the non-nullable I64 the fixtures order rows by, so
/// `(PK, payload)` order is decided by it alone in every payload shape.
fn schema_for(pk_types: &[u8], p: Payload) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = pk_types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
    cols.push(SchemaColumn::new(type_code::I64, 0));
    match p {
        Payload::Int => {}
        Payload::Nullable => cols.push(SchemaColumn::new(type_code::I64, 1)),
        Payload::Str => cols.push(SchemaColumn::new(type_code::STRING, 0)),
    }
    let pk: Vec<u32> = (0..pk_types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// One fixture row: its PK column values (in pk-list order) and the I64 that
/// both orders it within its PK group and seeds the extra payload column.
type Row = (Vec<u128>, i64);

/// Build a batch over `schema_for(_, p)` and certify it `Consolidated`. `rows`
/// must arrive sorted by `(PK, ord)`; every weight is `+1`.
///
/// The long strings are 24 bytes — past `SHORT_STRING_THRESHOLD`, so they land
/// in the blob heap and the emit path must relocate rather than copy them
/// inline. Every fourth nullable cell is NULL.
fn build(schema: &SchemaDescriptor, p: Payload, rows: &[Row]) -> Batch {
    let mut b = Batch::with_capacity(*schema, rows.len().max(1));
    for (i, (pk, ord)) in rows.iter().enumerate() {
        b.extend_pk_opk(schema, pk);
        b.extend_weight(&1i64.to_le_bytes());
        let null_word = match p {
            Payload::Nullable if i % 4 == 0 => 1u64 << 1,
            _ => 0,
        };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(0, &ord.to_le_bytes());
        match p {
            Payload::Int => {}
            Payload::Nullable => b.extend_col(1, &(ord * 7).to_le_bytes()),
            Payload::Str => {
                // Only a handful of distinct long strings, so the dedup cache
                // has repeated spans to collapse — the shape a fan-out join
                // actually presents it.
                let s = format!("join-bench-payload-{:05}", ord % 8);
                let cell = gnitz_wire::encode_german_string(s.as_bytes(), &mut b.blob);
                b.extend_col(1, &cell);
            }
        }
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// Split `rows` round-robin into `n` batches, each of which stays sorted, and
/// open one cursor over all of them. `n == 1` yields a `Single`-mode cursor;
/// `n > 1` a `Multi`-mode one, where a bare `advance()` per row would re-run the
/// payload-comparator dispatch that both group walks hoist out.
fn cursor_over(schema: &SchemaDescriptor, p: Payload, rows: &[Row], n: usize) -> ReadCursor {
    let batches: Vec<Rc<Batch>> = (0..n)
        .map(|s| {
            let part: Vec<Row> = rows.iter().skip(s).step_by(n).cloned().collect();
            Rc::new(build(schema, p, &part))
        })
        .collect();
    ReadCursor::over_batches(&batches, *schema)
}

// ---------------------------------------------------------------------------
// Timing
// ---------------------------------------------------------------------------

/// Print one row of the result table. `rows` is the output row count of a
/// single call — the emit-side work the timing has to be read against, since a
/// fan-out shape emits far more rows than either input holds.
fn report(name: &str, elapsed: Duration, delta_rows: usize, out_rows: usize) {
    let per_out = match out_rows {
        0 => f64::NAN,
        n => elapsed.as_nanos() as f64 / (n * ITERS) as f64,
    };
    let per_delta = elapsed.as_nanos() as f64 / (delta_rows * ITERS) as f64;
    println!(
        "{name:<46} {:>9.3} ms/iter  {per_delta:>8.2} ns/delta-row  {per_out:>8.2} ns/out-row  (out {out_rows})",
        elapsed.as_secs_f64() * 1000.0 / ITERS as f64,
    );
}

// ---------------------------------------------------------------------------
// Equi delta-trace join
// ---------------------------------------------------------------------------

/// Delta/trace fan-out shape: `d` delta rows and `t` trace rows per key, over a
/// key space where only every `1 / hit` th delta key is present in the trace.
struct EquiShape {
    name: &'static str,
    d: usize,
    t: usize,
    /// Keep every `miss_stride`-th delta key in the trace; `1` = every key hits.
    miss_stride: usize,
}

const EQUI_SHAPES: [EquiShape; 5] = [
    EquiShape {
        name: "1:1",
        d: 1,
        t: 1,
        miss_stride: 1,
    },
    EquiShape {
        name: "trace-fanout(1:64)",
        d: 1,
        t: 64,
        miss_stride: 1,
    },
    EquiShape {
        name: "delta-fanout(64:1)",
        d: 64,
        t: 1,
        miss_stride: 1,
    },
    EquiShape {
        name: "many-to-many(8:8)",
        d: 8,
        t: 8,
        miss_stride: 1,
    },
    EquiShape {
        name: "selective(1:1, 1/32 hit)",
        d: 1,
        t: 1,
        miss_stride: 32,
    },
];

/// `(delta rows, trace rows)` for one equi shape. Both sides key on a single
/// U64; the larger side carries ~`N` rows.
fn equi_rows(shape: &EquiShape) -> (Vec<Row>, Vec<Row>) {
    let keys = N / shape.d.max(shape.t);
    let mut delta = Vec::with_capacity(keys * shape.d);
    let mut trace = Vec::with_capacity(keys * shape.t);
    for k in 0..keys as u128 {
        for i in 0..shape.d {
            delta.push((vec![k], i as i64));
        }
        if (k as usize).is_multiple_of(shape.miss_stride) {
            for i in 0..shape.t {
                trace.push((vec![k], i as i64));
            }
        }
    }
    (delta, trace)
}

#[test]
#[ignore]
fn join_equi_dt_bench() {
    println!("\n=== equi delta-trace join ({ITERS} iters) ===");
    for p in [Payload::Int, Payload::Nullable, Payload::Str] {
        let schema = schema_for(&[type_code::U64], p);
        let out_schema = merge_schemas_for_join(&schema, &schema).unwrap();
        for shape in &EQUI_SHAPES {
            let (delta_rows, trace_rows) = equi_rows(shape);
            let delta = build(&schema, p, &delta_rows);
            for srcs in SOURCE_COUNTS {
                let mut cursor = cursor_over(&schema, p, &trace_rows, srcs);
                let mut out_rows = 0;
                let elapsed = bench_time(ITERS, || {
                    let out = op_join_delta_trace(&delta, &mut cursor, &schema, &schema, &out_schema, JoinProbe::Equi);
                    out_rows = out.count;
                    std::hint::black_box(&out);
                });
                report(
                    &format!("equi {:<24} {:<8} src={srcs}", shape.name, p.tag()),
                    elapsed,
                    delta.count,
                    out_rows,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Range (non-equi / band) delta-trace join
// ---------------------------------------------------------------------------

/// A range-join fixture: the reindexed key shape (`n_eq` equality slots plus
/// one range slot) and how wide a slice of each trace eq-group the delta covers.
struct RangeShape {
    name: &'static str,
    /// PK column type codes; the last is the range slot, the rest the eq prefix.
    pk_types: &'static [u8],
    /// Delta slot values are drawn from the low `span_num / span_den` of the
    /// slot space for the upward rels — so `Gt`/`Ge` cover a wide span and
    /// `Lt`/`Le` a narrow one, and the pair of entries below covers both.
    span_num: u64,
    span_den: u64,
    /// Keep every `miss_stride`-th delta eq-group in the trace; `1` = every one
    /// matches. `> 1` is what the walk's trailing group skip targets.
    miss_stride: usize,
}

const RANGE_SHAPES: [RangeShape; 5] = [
    RangeShape {
        name: "n_eq=0 stride=8 wide-span",
        pk_types: &[type_code::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
    },
    RangeShape {
        name: "n_eq=0 stride=8 narrow-span",
        pk_types: &[type_code::U64],
        span_num: 15,
        span_den: 16,
        miss_stride: 1,
    },
    RangeShape {
        name: "n_eq=1 stride=8 wide-span",
        pk_types: &[type_code::U32, type_code::U32],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
    },
    RangeShape {
        name: "n_eq=1 stride=12 wide-span",
        pk_types: &[type_code::U32, type_code::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
    },
    RangeShape {
        name: "n_eq=1 stride=12 mostly-unmatched",
        pk_types: &[type_code::U32, type_code::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 16,
    },
];

/// Slot values per eq group in a band shape. `n_eq = 0` is one group instead,
/// holding the whole `N`-row trace.
const SLOTS_PER_GROUP: u64 = 64;

/// Delta probes per group at `n_eq = 0`, spread across the shape's span. A band
/// shape probes once per group instead, so its delta row count is its group
/// count.
const N_EQ0_PROBES: u64 = 8;

/// `(delta rows, trace rows)` for one range shape. The trace holds every slot
/// of every group it has; the delta probes at slots the shape's span picks.
fn range_rows(shape: &RangeShape) -> (Vec<Row>, Vec<Row>) {
    let n_eq = shape.pk_types.len() - 1;
    let (groups, slots) = match n_eq {
        0 => (1usize, N as u64),
        _ => ((N / SLOTS_PER_GROUP as usize).max(1), SLOTS_PER_GROUP),
    };
    let base = shape.span_num * slots / shape.span_den;
    let mut delta = Vec::new();
    let mut trace = Vec::new();
    for g in 0..groups as u128 {
        let eq: Vec<u128> = (0..n_eq).map(|_| g).collect();
        let probes = if n_eq == 0 { N_EQ0_PROBES } else { 1 };
        let step = ((slots - base) / probes).max(1);
        for i in 0..probes {
            let mut pk = eq.clone();
            pk.push((base + i * step) as u128);
            delta.push((pk, i as i64));
        }
        if !(g as usize).is_multiple_of(shape.miss_stride) {
            continue;
        }
        for s in 0..slots {
            let mut pk = eq.clone();
            pk.push(s as u128);
            trace.push((pk, 0));
        }
    }
    (delta, trace)
}

#[test]
#[ignore]
fn join_range_dt_bench() {
    println!("\n=== range delta-trace join ({ITERS} iters) ===");
    const RELS: [RangeRel; 4] = [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge];
    for p in [Payload::Int, Payload::Nullable, Payload::Str] {
        for shape in &RANGE_SHAPES {
            let schema = schema_for(shape.pk_types, p);
            let out_schema = merge_schemas_for_join(&schema, &schema).unwrap();
            let n_eq = shape.pk_types.len() - 1;
            let (delta_rows, trace_rows) = range_rows(shape);
            let delta = build(&schema, p, &delta_rows);
            for rel in RELS {
                for srcs in SOURCE_COUNTS {
                    let mut cursor = cursor_over(&schema, p, &trace_rows, srcs);
                    let mut out_rows = 0;
                    let elapsed = bench_time(ITERS, || {
                        let out = op_join_delta_trace(
                            &delta,
                            &mut cursor,
                            &schema,
                            &schema,
                            &out_schema,
                            JoinProbe::Range(
                                RangeProbe::new(&schema, &schema, n_eq as u8, rel).expect("bench probe is well-formed"),
                            ),
                        );
                        out_rows = out.count;
                        std::hint::black_box(&out);
                    });
                    report(
                        &format!("range {:<32} {:<8} {rel:?} src={srcs}", shape.name, p.tag()),
                        elapsed,
                        delta.count,
                        out_rows,
                    );
                }
            }
        }
    }
}
