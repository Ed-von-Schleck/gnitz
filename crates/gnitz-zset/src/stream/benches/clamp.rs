use super::tests::ClampRow;
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use gnitz_wire::ClampKind::Distinct;

/// `pk` holding payloads `1..=g`, each at weight 1.
fn hot_group(pk: u64, g: i64) -> Vec<ClampRow> {
    (1..=g).map(|v| (pk, 1, v)).collect()
}

/// `rows` dealt round-robin over `n` runs.
fn deal(rows: impl IntoIterator<Item = ClampRow>, n: usize) -> Vec<Vec<ClampRow>> {
    let mut runs = vec![Vec::new(); n];
    for (k, row) in rows.into_iter().enumerate() {
        runs[k % n].push(row);
    }
    runs
}

/// A U64 PK over `n` I64 payload columns, or `n` STRING ones under `strings`.
fn bench_schema(n: usize, strings: bool) -> SchemaDescriptor {
    let tc = if strings { TypeCode::String } else { TypeCode::I64 };
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..n).map(|_| SchemaColumn::new(tc, false)));
    SchemaDescriptor::new(&cols, &[0])
}

fn bench_batch(schema: &SchemaDescriptor, rows: &[ClampRow]) -> Batch {
    let mut b = crate::repr::BatchBuilder::new(schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk as u128, w);
        for (_, col) in schema.payload_columns() {
            match col.type_code {
                TypeCode::String => b.put_string(&format!("{pk:020}-{val:019}")),
                _ => b.put_int(val as u128),
            }
        }
        b.end_row();
    }
    b.finish().into_consolidated()
}

/// One bench shape: the trace's runs and the delta, each as `(pk, weight,
/// payload)` rows, every payload column holding the row's payload — as a
/// 40-byte heap-backed string of the row under `strings`.
struct BenchShape {
    name: String,
    payload_cols: usize,
    strings: bool,
    runs: Vec<Vec<ClampRow>>,
    delta: Vec<ClampRow>,
}

impl BenchShape {
    /// A shape over one I64 payload column.
    fn ints(name: impl Into<String>, runs: Vec<Vec<ClampRow>>, delta: Vec<ClampRow>) -> Self {
        BenchShape {
            name: name.into(),
            payload_cols: 1,
            strings: false,
            runs,
            delta,
        }
    }
}

/// `op_weight_clamp` per shape, opening its ranged cursor inside the timed region
/// as an epoch does.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn weight_clamp_bench() {
    const ITERS: usize = 200;
    const N: u64 = 4096;
    let dense = |w: i64| (0..N).map(move |k| (k, w, 0));
    let hot_probes = |g: i64| (0..256).map(move |p| (1, -1, 1 + p * g / 256)).collect::<Vec<_>>();
    let mut shapes = vec![
        BenchShape::ints(
            "sparse, 4 runs",
            (0..4u64)
                .map(|r| (0..16_384u64).map(|k| (k * 8 + 2 * r, 1, 0)).collect())
                .collect(),
            (0..N).map(|k| (16_384 + 3 * k, 1, 0)).collect(),
        ),
        BenchShape::ints("dense retract, 1 run", deal(dense(1), 1), dense(-1).collect()),
        BenchShape::ints("dense re-add, 1 run", deal(dense(1), 1), dense(1).collect()),
        BenchShape::ints("dense retract, 4 runs", deal(dense(1), 4), dense(-1).collect()),
        BenchShape {
            name: "dense retract, 4 payload cols".into(),
            payload_cols: 4,
            strings: false,
            runs: deal(dense(1), 1),
            delta: dense(-1).collect(),
        },
        BenchShape::ints(
            "spread, half hits",
            deal((0..N).map(|k| (2 * k, 1, 0)), 1),
            dense(-1).collect(),
        ),
        BenchShape::ints(
            "insert-only, all emit",
            deal((0..N).map(|k| (2 * k + 1, 1, 0)), 1),
            (0..N).map(|k| (2 * k, 1, 0)).collect(),
        ),
    ];
    // Every row emits a weight other than its own, so the output is copied rather
    // than handed back, with its strings.
    shapes.push(BenchShape {
        name: "insert-only at w=2, strings".into(),
        payload_cols: 1,
        strings: true,
        runs: deal((0..N).map(|k| (2 * k + 1, 1, 0)), 1),
        delta: (0..N).map(|k| (2 * k, 2, 0)).collect(),
    });
    for g in [1_000, 100_000] {
        shapes.push(BenchShape::ints(
            format!("hot, 1 probe, G={g}, 4 runs"),
            deal(hot_group(1, g), 4),
            vec![(1, 1, g + 10)],
        ));
    }
    for (g, n_runs) in [(1_000, 1), (1_000, 4), (100_000, 4)] {
        shapes.push(BenchShape::ints(
            format!("hot, 256 probes, G={g}, {n_runs} runs"),
            deal(hot_group(1, g), n_runs),
            hot_probes(g),
        ));
    }

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for shape in shapes {
        let schema = bench_schema(shape.payload_cols, shape.strings);
        let mut trace = crate::test_support::TestTrace::new(schema);
        for run in &shape.runs {
            trace.ingest(bench_batch(&schema, run));
        }
        let delta = bench_batch(&schema, &shape.delta);
        let first = delta.get_pk_bytes(0).to_vec();
        let mut instructions = 0;
        for _ in 0..ITERS {
            let (out, n) = counter.measure(|| {
                let mut cursor = trace.cursor_from(&first);
                op_weight_clamp(&delta, &mut cursor, Distinct)
            });
            std::hint::black_box(out);
            instructions += n;
        }
        println!(
            "op_weight_clamp {}: {} instr/iter",
            shape.name,
            instructions / ITERS as u64
        );
    }
}
