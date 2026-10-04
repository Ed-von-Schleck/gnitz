use super::tests::by_pk;
use super::tests::wide_partition;
use super::*;
use crate::test_support::make_schema_u64_i64;

/// One exchange round's cost on the mesh, in instructions: every worker opens the
/// round and every worker steps it to its close. The outboxes hold a whole
/// partition, so every round is one part.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn mesh_round_bench() {
    use std::hint::black_box;

    const ROUNDS: u64 = 200;

    let counter = gnitz_foundation::perf::Counter::instructions();
    let pk = by_pk(&make_schema_u64_i64());
    for nw in [2, 4] {
        for rows in [1, 1_000, 100_000] {
            let mut mesh = super::fixtures::meshes(nw, 64 << 20);
            let parts: Vec<Batch> = (0..nw).map(|w| wide_partition(w, rows)).collect();
            let mut rounds: Vec<Round> = Vec::with_capacity(nw);
            let ((), instructions) = counter.measure(|| {
                for _ in 0..ROUNDS {
                    rounds.clear();
                    let opened = mesh.iter_mut().zip(&parts);
                    rounds.extend(opened.map(|(m, batch)| m.open(9, Cow::Borrowed(batch), &pk, true, true)));
                    for (m, round) in mesh.iter_mut().zip(&mut rounds) {
                        black_box(m.step(round).expect("a round that fits one part"));
                    }
                }
            });
            println!(
                "mesh_round_bench nw={nw} rows={rows:<6} {:>12.1} instr/round",
                instructions as f64 / ROUNDS as f64
            );
        }
    }
}
