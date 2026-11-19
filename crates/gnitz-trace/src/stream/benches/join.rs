use super::*;
use crate::test_support::{TestTrace, rekey_plan};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{NullKeys, RangeRel, TypeCode};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::SchemaColumn;

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
            Payload::Nullable if i % 4 == 0 => {
                b.put_null();
            }
            Payload::Nullable => {
                b.put_int((ord * 7) as u128);
            }
            Payload::Str => {
                b.put_string(&format!("join-bench-payload-{:05}", ord % 8));
            }
        }
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_consolidated();
    b
}

/// One probe of `delta` against `trace` dealt into `runs` runs, opening its
/// cursor inside the counted region: its instructions and its output rows.
fn probe(counter: &Counter, plan: &JoinPlan, delta: &Batch, trace: &Batch, runs: usize) -> (u64, usize) {
    let trace = TestTrace::dealt(trace, runs);
    // The first pass takes the pool's first allocations.
    let [_, (out, instructions)] =
        [(); 2].map(|()| counter.measure(|| op_join_delta_trace(delta, &mut |f, l| trace.cursor_within(f, l), plan)));
    (instructions, out.len())
}

/// The equi join, in instructions per delta row, over a trace that is its
/// source re-keyed onto the leading column of a two-column PK — stored, and
/// read out of the source, where the walk matches a key prefix and the kept key
/// column comes out of the PK.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn join_equi_bench() {
    // `d` delta rows and `t` trace rows per key, the trace holding every
    // `miss_stride`-th of the delta's keys.
    const SHAPES: [(&str, usize, usize, usize); 5] = [
        ("1:1", 1, 1, 1),
        ("trace-fanout(1:64)", 1, 64, 1),
        ("delta-fanout(64:1)", 64, 1, 1),
        ("many-to-many(8:8)", 8, 8, 1),
        ("selective(1:1, 1/32 hit)", 1, 1, 32),
    ];
    let counter = Counter::instructions();
    for p in PAYLOADS {
        let narrow = schema_for(&[TypeCode::U64], p);
        let wide = schema_for(&[TypeCode::U64, TypeCode::U64], p);
        let keep: Vec<u32> = (1..wide.num_columns() as u32).collect();
        let mut map = rekey_plan(&wide, &[0], &keep, NullKeys::Drop);
        let stored = JoinPlan::from_wire(JoinKind::Equi, false, &narrow, map.output_schema()).unwrap();
        let over = JoinPlan::over_source(false, &narrow, &wide, &map).unwrap();
        for (name, d, t, miss_stride) in SHAPES {
            let keys = N / d.max(t);
            let delta_rows: Vec<Row> = (0..keys as u128)
                .flat_map(|k| (0..d).map(move |i| (vec![k], i as i64)))
                .collect();
            let delta = build(&narrow, p, &delta_rows);
            // The source keyed `(k, i)`; its stored trace is that re-keyed on `k`.
            let source_rows: Vec<Row> = (0..keys as u128)
                .step_by(miss_stride)
                .flat_map(|k| (0..t as u128).map(move |i| (vec![k, i], i as i64)))
                .collect();
            let source = build(&wide, p, &source_rows);
            let trace = map.evaluate_map_batch(&source.as_mem_batch()).into_consolidated();
            for runs in RUNS {
                let (stored_instr, out) = probe(&counter, &stored, &delta, &trace, runs);
                let (over_instr, over_out) = probe(&counter, &over, &delta, &source, runs);
                assert_eq!((out, over_out), (keys.div_ceil(miss_stride) * d * t, out), "{name}");
                println!(
                    "join_equi_bench {name:<24} {:<8} runs={runs} out {out:>5}: {:>8.1} stored, {:>8.1} over source \
                     instr/delta-row",
                    format!("{p:?}"),
                    stored_instr as f64 / delta.len() as f64,
                    over_instr as f64 / delta.len() as f64,
                );
            }
        }
    }
}

/// The keyless join, in instructions per output row, with the product's long
/// side in the delta and in the trace.
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
        let (instructions, out) = probe(&counter, &plan, &side(d), &side(t), 1);
        assert_eq!(out, d * t);
        println!(
            "join_cross_bench {d:>3}x{t:<3}: {:>6.1} instr/out-row",
            instructions as f64 / out as f64
        );
    }
}

/// The range join, in instructions per delta and per output row, for a rel
/// cutting each way: the walk reads no payload, so the fixtures carry the one
/// integer.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn join_range_bench() {
    use TypeCode::*;
    // The PK column types — the last the range slot, the rest the equality
    // prefix — the delta's first probe `num/den` of the way up a group's slots,
    // the trace holding every `miss_stride`-th of the delta's groups, and the
    // trace slots per group; without an equality prefix one group holds `N`,
    // probed `N_EQ0_PROBES` times, and a keyed shape probes each group once.
    const N_EQ0_PROBES: u64 = 8;
    type Shape = (&'static str, &'static [TypeCode], u64, u64, usize, u64);
    const SHAPES: [Shape; 6] = [
        ("n_eq=0 stride=8 low probes", &[U64], 1, 16, 1, N as u64),
        ("n_eq=0 stride=8 high probes", &[U64], 15, 16, 1, N as u64),
        ("n_eq=1 stride=8", &[U32, U32], 1, 16, 1, 64),
        ("n_eq=1 stride=12", &[U32, U64], 1, 16, 1, 64),
        ("n_eq=1 stride=12 mostly-unmatched", &[U32, U64], 1, 16, 16, 64),
        // 512 delta groups against a trace holding four of them.
        ("n_eq=1 stride=12 sparse-groups", &[U32, U64], 1, 2, 128, 8),
    ];
    let counter = Counter::instructions();
    for (name, pk_types, num, den, miss_stride, slots) in SHAPES {
        let schema = schema_for(pk_types, Payload::Int);
        let n_eq = pk_types.len() - 1;
        let (groups, probes) = match n_eq {
            0 => (1, N_EQ0_PROBES),
            _ => (N / slots as usize, 1),
        };
        let base = num * slots / den;
        let step = ((slots - base) / probes).max(1);
        let key = |g: usize, slot: u64| -> Vec<u128> { (0..n_eq).map(|_| g as u128).chain([slot as u128]).collect() };
        let delta_rows: Vec<Row> = (0..groups)
            .flat_map(|g| (0..probes).map(move |i| (g, i)))
            .map(|(g, i)| (key(g, base + i * step), i as i64))
            .collect();
        let trace_rows: Vec<Row> = (0..groups)
            .step_by(miss_stride)
            .flat_map(|g| (0..slots).map(move |s| (g, s)))
            .map(|(g, s)| (key(g, s), 0))
            .collect();
        let delta = build(&schema, Payload::Int, &delta_rows);
        let trace = build(&schema, Payload::Int, &trace_rows);
        for rel in [RangeRel::Lt, RangeRel::Gt] {
            let plan = JoinPlan::from_wire(JoinKind::Range { rel }, true, &schema, &schema).unwrap();
            let [(i1, out), (i4, out4)] = RUNS.map(|runs| probe(&counter, &plan, &delta, &trace, runs));
            assert!(out > 0 && out == out4, "{name} {rel:?}");
            for (runs, instructions) in RUNS.into_iter().zip([i1, i4]) {
                println!(
                    "join_range_bench {name:<34} {rel:?} runs={runs} out {out:>7}: {:>9.1} instr/delta-row, \
                     {:>6.1} instr/out-row",
                    instructions as f64 / delta.len() as f64,
                    instructions as f64 / out as f64
                );
            }
        }
    }
}
