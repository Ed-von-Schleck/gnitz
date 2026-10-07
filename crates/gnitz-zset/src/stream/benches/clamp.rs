use super::tests::ClampRow;
use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64, make_string_batch, TestTrace};
use gnitz_wire::ClampKind::Distinct;

/// Instructions per delta row of `op_weight_clamp` per shape, opening its cursor
/// at the delta's first key inside the counted region.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn weight_clamp_bench() {
    const N: u64 = 4096;
    let ints = make_schema_u64_i64();
    let batch = |rows: &mut dyn Iterator<Item = ClampRow>| make_batch(&ints, &rows.collect::<Vec<_>>());
    let dense = |w: i64| batch(&mut (0..N).map(|k| (k, w, 0)));
    let evens = |w: i64| batch(&mut (0..N).map(|k| (2 * k, w, 0)));
    // `pk` 1 holding the payloads `2, 4, ..= 2g`.
    let hot = |g: i64| batch(&mut (1..=g).map(|v| (1, 1, 2 * v)));
    // 256 probes spread over that group, each `off` past a payload it holds.
    let probes = |g: i64, off: i64| batch(&mut (0..256).map(|p| (1, -1, 2 + 2 * (p * g / 256) + off)));
    let strings = |keys: &mut dyn Iterator<Item = (u64, i64)>| {
        let rows: Vec<(u64, i64, Vec<u8>)> = keys.map(|(k, w)| (k, w, format!("{k:040}").into_bytes())).collect();
        let rows: Vec<(u64, i64, &[u8])> = rows.iter().map(|(k, w, s)| (*k, *w, &s[..])).collect();
        make_string_batch(&rows)
    };

    // The trace, the runs it is dealt into, and the delta.
    let shapes: [(&str, Batch, usize, Batch); 13] = [
        (
            "sparse, 4 runs",
            batch(&mut (0..65_536).map(|k| (2 * k, 1, 0))),
            4,
            batch(&mut (0..N).map(|k| (16_384 + 3 * k, 1, 0))),
        ),
        ("dense retract, 1 run", dense(1), 1, dense(-1)),
        ("dense re-add, 1 run", dense(1), 1, dense(1)),
        ("dense retract, 4 runs", dense(1), 4, dense(-1)),
        ("spread, half hits", evens(1), 1, dense(-1)),
        (
            "spread, half hits, strings",
            strings(&mut (0..N).map(|k| (2 * k, 1))),
            1,
            strings(&mut (0..N).map(|k| (k, -1))),
        ),
        (
            "insert-only, all emit",
            batch(&mut (0..N).map(|k| (2 * k + 1, 1, 0))),
            1,
            evens(1),
        ),
        (
            "hot, 1 probe past the group, G=100000, 4 runs",
            hot(100_000),
            4,
            batch(&mut std::iter::once((1, 1, 200_010))),
        ),
        ("hot, 256 probes, G=1000, 1 run", hot(1_000), 1, probes(1_000, 0)),
        ("hot, 256 probes, G=1000, 4 runs", hot(1_000), 4, probes(1_000, 0)),
        (
            "hot, 256 absent probes, G=1000, 4 runs",
            hot(1_000),
            4,
            probes(1_000, 1),
        ),
        ("hot, 256 probes, G=100000, 4 runs", hot(100_000), 4, probes(100_000, 0)),
        (
            "hot, 256 absent probes, G=100000, 4 runs",
            hot(100_000),
            4,
            probes(100_000, 1),
        ),
    ];

    let counter = gnitz_foundation::perf::Counter::instructions();
    for (name, trace, runs, delta) in shapes {
        let trace = TestTrace::dealt(&trace, runs);
        // The first pass takes the pool's first allocations.
        let [_, (out, instructions)] = [(); 2]
            .map(|()| counter.measure(|| op_weight_clamp(&delta, &mut |first, _| trace.cursor_from(first), Distinct)));
        println!(
            "weight_clamp_bench {name:<42} {:>8.1} instr/row (out {})",
            instructions as f64 / delta.count as f64,
            out.count
        );
    }
}
