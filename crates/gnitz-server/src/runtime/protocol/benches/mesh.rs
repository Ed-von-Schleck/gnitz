use super::tests::by_pk;
use super::tests::wide_partition;
use super::*;
use crate::test_support::make_schema_u64_i64;
use gnitz_foundation::perf::Counter;

/// One worker's cost of an exchange round on the mesh, in instructions: it opens
/// the round and steps it to its close. The outboxes hold a whole partition, so
/// every round is one part.
///
/// `scatter` routes each row to its owner and `broadcast` every row to every
/// worker; `fold` gathers by merging the senders' rows, `concat` by appending
/// them. One row is the round's fixed cost.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn mesh_round_bench() {
    const ROUNDS: u64 = 200;

    let counter = Counter::instructions();
    let pk = by_pk(&make_schema_u64_i64());
    let broadcast = ScatterPlan::broadcast();
    for nw in [2, 4] {
        let mut mesh = super::fixtures::meshes(nw, 64 << 20);
        for rows in [1, 1_000] {
            let parts: Vec<Batch> = (0..nw).map(|w| wide_partition(w, rows)).collect();
            // The rows all workers gather in a round: every sender holds the same
            // `rows` elements, which a fold sums into one row each.
            for (case, plan, fold, gathered) in [
                ("scatter fold", &pk, true, rows),
                ("scatter concat", &pk, false, rows * nw as u64),
                ("broadcast fold", &broadcast, true, rows * nw as u64),
            ] {
                let mut rounds: Vec<Round> = Vec::with_capacity(nw);
                let (got, instructions) = counter.measure(|| {
                    let mut got = 0;
                    for _ in 0..ROUNDS {
                        rounds.clear();
                        let opened = mesh.iter_mut().zip(&parts);
                        rounds.extend(opened.map(|(m, batch)| m.open(9, Cow::Borrowed(batch), plan, fold, true)));
                        for (m, round) in mesh.iter_mut().zip(&mut rounds) {
                            let (rows, _) = m.step(round).expect("a round that fits one part");
                            got += rows.len() as u64;
                        }
                    }
                    got
                });
                assert_eq!(got, gathered * ROUNDS, "{case} at nw={nw}, rows={rows}");
                println!(
                    "mesh_round_bench nw={nw} rows={rows:<4} {case:<14} {:>10.1} instr/worker-round",
                    instructions as f64 / (ROUNDS * nw as u64) as f64
                );
            }
        }
    }
}
