//! Microbenchmarks for the three delta-trace join kinds. Ignored by default;
//! wall-clock on a contended box is not decisive, so take every A/B from
//! instructions retired:
//!
//! ```text
//! perf stat -e instructions:u -- cargo test -p gnitz-zset --release join_ \
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
//! The delta is certified consolidated, so the timed region is the probe and
//! the emit, not a sort.

use std::rc::Rc;
use std::time::Duration;

use super::{op_join_delta_trace, JoinPlan};
use crate::repr::{Batch, BatchBuilder, ReadCursor};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::bench_time_each;
use gnitz_wire::{JoinKind, RangeRel};

/// Rows on the larger side of every fixture. Big enough that the per-call
/// preamble (one `Batch::with_capacity`, one blob-cache acquire) is noise
/// against the per-row walk, small enough that a full sweep stays interactive.
const N: usize = 4096;

const ITERS: usize = 20;

/// Source counts swept per shape: a `Single`-mode cursor and a `Multi`-mode one.
const SOURCE_COUNTS: [usize; 2] = [1, 4];

/// Both delta sides: the emit loop writes the two payload halves at whichever
/// output slot range the side flag puts them.
const SIDES: [bool; 2] = [false, true];

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
fn schema_for(pk_types: &[TypeCode], p: Payload) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = pk_types.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
    cols.push(SchemaColumn::new(TypeCode::I64, false));
    match p {
        Payload::Int => {}
        Payload::Nullable => cols.push(SchemaColumn::new(TypeCode::I64, true)),
        Payload::Str => cols.push(SchemaColumn::new(TypeCode::String, false)),
    }
    let pk: Vec<u32> = (0..pk_types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// One fixture row: its PK column values (in pk-list order) and the I64 that
/// both orders it within its PK group and seeds the extra payload column.
type Row = (Vec<u128>, i64);

/// Build a batch over `schema_for(_, p)` and certify it consolidated. `rows`
/// must arrive sorted by `(PK, ord)`; every weight is `+1`.
///
/// The long strings are 24 bytes — past `SHORT_STRING_THRESHOLD`, so they land
/// in the blob heap and the emit path must relocate rather than copy them
/// inline. Every fourth nullable cell is NULL.
fn build(schema: &SchemaDescriptor, p: Payload, rows: &[Row]) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for (i, (pk, ord)) in rows.iter().enumerate() {
        b.begin_row_natives(pk, 1);
        b.put_int(*ord as u128);
        match p {
            Payload::Int => {}
            Payload::Nullable if i % 4 == 0 => b.put_null(),
            Payload::Nullable => b.put_int((ord * 7) as u128),
            // Only a handful of distinct long strings, so the dedup cache has
            // repeated spans to collapse — the shape a fan-out join actually
            // presents it.
            Payload::Str => b.put_string(&format!("join-bench-payload-{:05}", ord % 8)),
        }
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_consolidated();
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
    crate::test_support::create_read_cursor(&batches, &[], *schema)
}

// ---------------------------------------------------------------------------
// Timing
// ---------------------------------------------------------------------------

/// Time `ITERS` calls of one join kind and print the row. The plan and each
/// call's fresh `cursor()` are built outside the timed region, as an epoch gets them.
fn time_join(
    name: String,
    kind: JoinKind,
    delta_is_right: bool,
    schema: &SchemaDescriptor,
    delta: &Batch,
    cursor: impl FnMut() -> ReadCursor,
) {
    let plan = JoinPlan::from_wire(kind, delta_is_right, schema, schema).expect("bench join plan is well-formed");
    let mut out_rows = 0;
    let elapsed = bench_time_each(ITERS, cursor, |mut cursor| {
        let out = op_join_delta_trace(delta, &mut cursor, &plan.out_schema, &plan.probe);
        out_rows = out.count;
        std::hint::black_box(&out);
    });
    report(&name, elapsed, delta.count, out_rows);
}

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
    EquiShape { name: "1:1", d: 1, t: 1, miss_stride: 1 },
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
        let schema = schema_for(&[TypeCode::U64], p);
        for shape in &EQUI_SHAPES {
            let (delta_rows, trace_rows) = equi_rows(shape);
            let delta = build(&schema, p, &delta_rows);
            for srcs in SOURCE_COUNTS {
                for right in SIDES {
                    time_join(
                        format!("equi {:<24} {:<8} src={srcs} right={right}", shape.name, p.tag()),
                        JoinKind::Equi,
                        right,
                        &schema,
                        &delta,
                        || cursor_over(&schema, p, &trace_rows, srcs),
                    );
                }
            }
        }
    }
}

/// What the probe costs over a trace's source instead of over the stored trace,
/// in instructions per output row, over one run and over several: once with the
/// source keyed exactly as the trace, where the two walk the same rows, and once
/// keyed one column wider, where the walk matches a prefix and the kept key
/// column is read out of the PK.
#[test]
#[ignore]
fn join_over_source_bench() {
    use crate::test_support::rekey_plan;
    use gnitz_foundation::perf::Counter;
    let instructions = Counter::instructions().unwrap();
    // `runs` round-robin slices of `rows`, each mapped by `run`, under one cursor.
    let cursor_of = |rows: &Batch, runs: usize, run: &mut dyn FnMut(Batch) -> Batch| {
        let parts: Vec<Rc<Batch>> = (0..runs)
            .map(|s| {
                let picks: Vec<u32> = (s..rows.count).step_by(runs).map(|i| i as u32).collect();
                Rc::new(run(rows.ascending_subset(&picks)))
            })
            .collect();
        let schema = *parts[0].schema();
        crate::test_support::create_read_cursor(&parts, &[], schema)
    };
    let per_out = |plan: &JoinPlan, delta: &Batch, mut cursor: ReadCursor| {
        let (out, n) = instructions.measure(|| op_join_delta_trace(delta, &mut cursor, &plan.out_schema, &plan.probe));
        (n / out.count.max(1) as u64, out.count)
    };
    println!("\n=== equi join over the trace's source, instructions per output row ===");
    for p in [Payload::Int, Payload::Nullable, Payload::Str] {
        let narrow = schema_for(&[TypeCode::U64], p);
        let wide = schema_for(&[TypeCode::U64, TypeCode::U64], p);
        let keep_narrow: Vec<u32> = (1..narrow.num_columns() as u32).collect();
        let keep_wide: Vec<u32> = (1..wide.num_columns() as u32).collect();
        for shape in &EQUI_SHAPES {
            let (delta_rows, trace_rows) = equi_rows(shape);
            let delta = build(&narrow, p, &delta_rows);
            let trace = build(&narrow, p, &trace_rows);
            // The source keyed `(k, i)`; its stored trace is that re-keyed on `k`.
            let wide_rows: Vec<Row> = trace_rows.iter().map(|(k, i)| (vec![k[0], *i as u128], *i)).collect();
            let source = build(&wide, p, &wide_rows);
            let same = rekey_plan(&narrow, &[0], &keep_narrow);
            let mut map = rekey_plan(&wide, &[0], &keep_wide);
            let trace_schema = *map.out_schema();
            for runs in SOURCE_COUNTS {
                let stored = JoinPlan::from_wire(JoinKind::Equi, false, &narrow, &narrow).unwrap();
                let over = JoinPlan::over_source(false, &narrow, &narrow, &same).unwrap();
                let (same_stored, out) = per_out(&stored, &delta, cursor_of(&trace, runs, &mut |b| b));
                let (same_over, _) = per_out(&over, &delta, cursor_of(&trace, runs, &mut |b| b));

                let stored = JoinPlan::from_wire(JoinKind::Equi, false, &narrow, &trace_schema).unwrap();
                let over = JoinPlan::over_source(false, &narrow, &wide, &map).unwrap();
                let rekeyed = cursor_of(&source, runs, &mut |b| map.evaluate_map_batch(&b).into_consolidated());
                let (prefix_stored, _) = per_out(&stored, &delta, rekeyed);
                let (prefix_over, _) = per_out(&over, &delta, cursor_of(&source, runs, &mut |b| b));
                println!(
                    "{:<24} {:<8} src={runs} out {out:>5}: same key {same_stored:>4} stored, {same_over:>4} over \
                     source; key prefix {prefix_stored:>4} stored, {prefix_over:>4} over source",
                    shape.name,
                    p.tag(),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Cross (keyless) delta-trace join
// ---------------------------------------------------------------------------

/// `(delta rows, trace rows)` per keyless shape. The product is what the walk
/// emits, so both sides stay small next to `N`.
const CROSS_SHAPES: [(&str, usize, usize); 3] = [("64x64", 64, 64), ("512x8", 512, 8), ("8x512", 8, 512)];

#[test]
#[ignore]
fn join_cross_dt_bench() {
    println!("\n=== cross delta-trace join ({ITERS} iters) ===");
    let rows = |n: usize| -> Vec<Row> { (0..n as u128).map(|k| (vec![k], k as i64)).collect() };
    for p in [Payload::Int, Payload::Nullable, Payload::Str] {
        let schema = schema_for(&[TypeCode::U64], p);
        for (name, d, t) in CROSS_SHAPES {
            let delta = build(&schema, p, &rows(d));
            for srcs in SOURCE_COUNTS {
                for right in SIDES {
                    time_join(
                        format!("cross {name:<23} {:<8} src={srcs} right={right}", p.tag()),
                        JoinKind::Cross,
                        right,
                        &schema,
                        &delta,
                        || cursor_over(&schema, p, &rows(t), srcs),
                    );
                }
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
    pk_types: &'static [TypeCode],
    /// Delta slot values are drawn from the low `span_num / span_den` of the
    /// slot space for the upward rels — so `Gt`/`Ge` cover a wide span and
    /// `Lt`/`Le` a narrow one, and the pair of entries below covers both.
    span_num: u64,
    span_den: u64,
    /// Keep every `miss_stride`-th delta eq-group in the trace; `1` = every one
    /// matches. `> 1` is what the walk's trailing group skip targets.
    miss_stride: usize,
    /// Trace slots per eq group, which with `N` fixes the group count. Ignored
    /// at `n_eq = 0`, one group holding all of `N`.
    slots_per_group: u64,
}

const RANGE_SHAPES: [RangeShape; 6] = [
    RangeShape {
        name: "n_eq=0 stride=8 wide-span",
        pk_types: &[TypeCode::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=0 stride=8 narrow-span",
        pk_types: &[TypeCode::U64],
        span_num: 15,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=1 stride=8 wide-span",
        pk_types: &[TypeCode::U32, TypeCode::U32],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=1 stride=12 wide-span",
        pk_types: &[TypeCode::U32, TypeCode::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=1 stride=12 mostly-unmatched",
        pk_types: &[TypeCode::U32, TypeCode::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 16,
        slots_per_group: 64,
    },
    // 512 delta groups against a trace holding four — a ~128-group skip between
    // sweeps, where the shapes above skip at most 15 rows.
    RangeShape {
        name: "n_eq=1 stride=12 sparse-groups",
        pk_types: &[TypeCode::U32, TypeCode::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 128,
        slots_per_group: 8,
    },
];

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
        _ => ((N / shape.slots_per_group as usize).max(1), shape.slots_per_group),
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
    for p in [Payload::Int, Payload::Nullable, Payload::Str] {
        for shape in &RANGE_SHAPES {
            let schema = schema_for(shape.pk_types, p);
            let (delta_rows, trace_rows) = range_rows(shape);
            let delta = build(&schema, p, &delta_rows);
            for &rel in RangeRel::ALL {
                for srcs in SOURCE_COUNTS {
                    for right in SIDES {
                        time_join(
                            format!(
                                "range {:<32} {:<8} {rel:?} src={srcs} right={right}",
                                shape.name,
                                p.tag()
                            ),
                            JoinKind::Range { rel },
                            right,
                            &schema,
                            &delta,
                            || cursor_over(&schema, p, &trace_rows, srcs),
                        );
                    }
                }
            }
        }
    }
}
