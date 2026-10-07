use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, TypeCode};
use crate::test_support::{opens, rekey_plan, TestTrace};
use gnitz_foundation::perf::Counter;
use gnitz_wire::RangeRel;

/// Rows on the larger side of an equi or a range fixture.
const N: usize = 4096;

/// Runs the trace is dealt into: one, where the cursor reads its only source
/// directly, and several, where it merges them and breaks a PK tie on the payload.
const RUNS: [usize; 2] = [1, 4];

/// The payload columns a fixture carries behind the I64 its rows are ordered by.
#[derive(Clone, Copy, Debug)]
enum Payload {
    /// None: the payload compares as one non-nullable integer, and no blob heap.
    Int,
    /// A nullable I64: the generic payload comparator, still no blob heap.
    Nullable,
    /// A STRING: the generic comparator, and an emit that relocates strings.
    Str,
}

const PAYLOADS: [Payload; 3] = [Payload::Int, Payload::Nullable, Payload::Str];

/// `pk_types` PK columns, a non-nullable I64, then `p`'s column.
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

/// One fixture row: its PK column values and the I64 that orders it within its
/// PK.
type Row = (Vec<u128>, i64);

/// `rows`, sorted by `(PK, ord)`, each at weight 1 over `schema_for(_, p)`.
/// Every fourth nullable cell is NULL; the strings are heap-backed and take
/// eight distinct values.
fn build(schema: &SchemaDescriptor, p: Payload, rows: &[Row]) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for (i, (pk, ord)) in rows.iter().enumerate() {
        b.begin_row_natives(pk, 1);
        b.put_int(*ord as u128);
        match p {
            Payload::Int => {}
            Payload::Nullable if i % 4 == 0 => b.put_null(),
            Payload::Nullable => b.put_int((ord * 7) as u128),
            Payload::Str => b.put_string(&format!("join-bench-payload-{:05}", ord % 8)),
        }
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_consolidated();
    b
}

/// One probe of `delta` against `trace` dealt into `runs` runs: its instructions
/// per delta row and its output rows.
fn probe(counter: &Counter, plan: &JoinPlan, delta: &Batch, trace: &Batch, runs: usize) -> (f64, usize) {
    let trace = TestTrace::dealt(trace, runs);
    // The first pass takes the pool's first allocations.
    let [_, (out, instructions)] = [(); 2].map(|()| {
        let mut open = opens(trace.cursor());
        counter.measure(|| op_join_delta_trace(delta, &mut open, &plan.out_schema, &plan.probe))
    });
    (instructions as f64 / delta.count as f64, out.count)
}

/// `d` delta rows and `t` trace rows per key, the trace holding every
/// `miss_stride`-th of the delta's keys.
struct EquiShape {
    name: &'static str,
    d: usize,
    t: usize,
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

/// The equi join, in instructions per delta row: over a stored trace keyed as
/// the delta, then over a trace that is its source re-keyed onto the leading
/// column of a two-column PK — stored, and read out of the source, where the
/// walk matches a key prefix and the kept key column comes out of the PK.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn join_equi_bench() {
    let counter = Counter::instructions();
    for p in PAYLOADS {
        let narrow = schema_for(&[TypeCode::U64], p);
        let wide = schema_for(&[TypeCode::U64, TypeCode::U64], p);
        let keep: Vec<u32> = (1..wide.num_columns() as u32).collect();
        let mut map = rekey_plan(&wide, &[0], &keep);
        let rekeyed = *map.out_schema();
        let plans = [
            JoinPlan::from_wire(JoinKind::Equi, false, &narrow, &narrow).unwrap(),
            JoinPlan::from_wire(JoinKind::Equi, false, &narrow, &rekeyed).unwrap(),
            JoinPlan::over_source(false, &narrow, &wide, &map).unwrap(),
        ];
        for shape in &EQUI_SHAPES {
            let keys = N / shape.d.max(shape.t);
            let rows = |per_key: usize, stride: usize| -> Vec<Row> {
                (0..keys as u128)
                    .step_by(stride)
                    .flat_map(|k| (0..per_key).map(move |i| (vec![k], i as i64)))
                    .collect()
            };
            let delta = build(&narrow, p, &rows(shape.d, 1));
            let trace_rows = rows(shape.t, shape.miss_stride);
            // The source keyed `(k, i)`; its stored trace is that re-keyed on `k`.
            let source_rows: Vec<Row> = trace_rows.iter().map(|(k, i)| (vec![k[0], *i as u128], *i)).collect();
            let source = build(&wide, p, &source_rows);
            let traces = [
                build(&narrow, p, &trace_rows),
                map.evaluate_map_batch(&source).into_consolidated(),
                source,
            ];
            for runs in RUNS {
                let [(stored, out), (prefix_stored, _), (prefix_source, _)] =
                    [0, 1, 2].map(|i| probe(&counter, &plans[i], &delta, &traces[i], runs));
                println!(
                    "join_equi_bench {:<24} {:<8} runs={runs} out {out:>5}: {stored:>8.1} instr/delta-row; \
                     key prefix {prefix_stored:>8.1} stored, {prefix_source:>8.1} over source",
                    shape.name,
                    format!("{p:?}"),
                );
            }
        }
    }
}

/// The keyless join, in instructions per delta row, with the product's long side
/// in the delta and in the trace.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn join_cross_bench() {
    let counter = Counter::instructions();
    let schema = schema_for(&[TypeCode::U64], Payload::Int);
    let plan = JoinPlan::from_wire(JoinKind::Cross, false, &schema, &schema).unwrap();
    let side = |n: usize| {
        let rows: Vec<Row> = (0..n as u128).map(|k| (vec![k], k as i64)).collect();
        build(&schema, Payload::Int, &rows)
    };
    for (d, t) in [(512, 8), (8, 512)] {
        let (per_row, out) = probe(&counter, &plan, &side(d), &side(t), 1);
        println!("join_cross_bench {d:>3}x{t:<3} out {out}: {per_row:>9.1} instr/delta-row");
    }
}

/// A range-join fixture: a key of equality slots and one range slot.
struct RangeShape {
    name: &'static str,
    /// The PK column types: the last is the range slot, the rest the equality
    /// prefix.
    pk_types: &'static [TypeCode],
    /// The delta's first probe sits `span_num / span_den` of the way up a
    /// group's slots.
    span_num: u64,
    span_den: u64,
    /// The trace holds every `miss_stride`-th of the delta's groups.
    miss_stride: usize,
    /// Trace slots per group; without an equality prefix, one group holds `N`.
    slots_per_group: u64,
}

const RANGE_SHAPES: [RangeShape; 6] = [
    RangeShape {
        name: "n_eq=0 stride=8 low probes",
        pk_types: &[TypeCode::U64],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=0 stride=8 high probes",
        pk_types: &[TypeCode::U64],
        span_num: 15,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=1 stride=8",
        pk_types: &[TypeCode::U32, TypeCode::U32],
        span_num: 1,
        span_den: 16,
        miss_stride: 1,
        slots_per_group: 64,
    },
    RangeShape {
        name: "n_eq=1 stride=12",
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
    // 512 delta groups against a trace holding four of them.
    RangeShape {
        name: "n_eq=1 stride=12 sparse-groups",
        pk_types: &[TypeCode::U32, TypeCode::U64],
        span_num: 1,
        span_den: 2,
        miss_stride: 128,
        slots_per_group: 8,
    },
];

/// The range join, in instructions per delta row, per relation: the walk reads
/// no payload, so the fixtures carry the one integer.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn join_range_bench() {
    /// Delta probes of the one group a key without an equality prefix has; a
    /// keyed shape probes each of its groups once.
    const N_EQ0_PROBES: u64 = 8;
    let counter = Counter::instructions();
    for shape in &RANGE_SHAPES {
        let schema = schema_for(shape.pk_types, Payload::Int);
        let n_eq = shape.pk_types.len() - 1;
        let (groups, slots, probes) = match n_eq {
            0 => (1, N as u64, N_EQ0_PROBES),
            _ => (N / shape.slots_per_group as usize, shape.slots_per_group, 1),
        };
        let base = shape.span_num * slots / shape.span_den;
        let step = ((slots - base) / probes).max(1);
        let key = |g: usize, slot: u64| -> Vec<u128> { (0..n_eq).map(|_| g as u128).chain([slot as u128]).collect() };
        let delta_rows: Vec<Row> = (0..groups)
            .flat_map(|g| (0..probes).map(move |i| (g, i)))
            .map(|(g, i)| (key(g, base + i * step), i as i64))
            .collect();
        let trace_rows: Vec<Row> = (0..groups)
            .step_by(shape.miss_stride)
            .flat_map(|g| (0..slots).map(move |s| (g, s)))
            .map(|(g, s)| (key(g, s), 0))
            .collect();
        let delta = build(&schema, Payload::Int, &delta_rows);
        let trace = build(&schema, Payload::Int, &trace_rows);
        for &rel in RangeRel::ALL {
            let plan = JoinPlan::from_wire(JoinKind::Range { rel }, true, &schema, &schema).unwrap();
            for runs in RUNS {
                let (per_row, out) = probe(&counter, &plan, &delta, &trace, runs);
                println!(
                    "join_range_bench {:<34} {rel:?} runs={runs} out {out:>5}: {per_row:>9.1} instr/delta-row",
                    shape.name
                );
            }
        }
    }
}
