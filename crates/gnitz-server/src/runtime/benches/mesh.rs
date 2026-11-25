use super::fixtures::{by_pk, meshes, partition};
use super::*;
use crate::query::Lane;
use crate::test_support::make_schema_u64_i64;
use gnitz_foundation::perf::Counter;

/// One worker's cost of an exchange round on the mesh, in instructions: it opens
/// the round and steps it to its close. The outboxes hold a whole partition, so
/// every round is one part.
///
/// `scatter` routes each row to its owner and `broadcast` every row to every
/// worker; `fold` gathers by merging the senders' rows, `concat` by appending
/// them. One row is the round's fixed cost. `lanes` opens one round of that many
/// one-row scatter-fold lanes, the views a worker's drain exchanges together:
/// per lane, against one lane per round, it is what sharing a round saves; with
/// no rows, it is what a lane costs a worker that sends and takes in nothing.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn mesh_round_bench() {
    const ROUNDS: u64 = 200;

    let counter = Counter::instructions();
    let pk = by_pk(&make_schema_u64_i64());
    let broadcast = ScatterPlan::broadcast();
    for nw in [2, 4] {
        let mut mesh = meshes(nw, 64 << 20);
        for rows in [1, 1_000] {
            let parts: Vec<Batch> = (0..nw).map(|w| partition(w, 0, rows)).collect();
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
                        rounds.extend(opened.map(|(m, batch)| m.open_one(9, Cow::Borrowed(batch), plan, fold, true)));
                        for (m, round) in mesh.iter_mut().zip(&mut rounds) {
                            let (rows, _) = m.step_one(round).expect("a round that fits one part");
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
        for (rows, lanes) in [(1, 1), (1, 8), (0, 1), (0, 8)] {
            let parts: Vec<Batch> = (0..nw).map(|w| partition(w, 0, rows)).collect();
            let mut rounds: Vec<Round> = Vec::with_capacity(nw);
            let mut gathered = Vec::with_capacity(lanes);
            let (got, instructions) = counter.measure(|| {
                let mut got = 0;
                for _ in 0..ROUNDS {
                    rounds.clear();
                    rounds.extend(mesh.iter_mut().zip(&parts).map(|(m, batch)| {
                        let lane = |view: usize| Lane {
                            view: view as u64,
                            batch: Cow::Borrowed(batch),
                            plan: &pk,
                            fold: true,
                        };
                        m.open((0..lanes).map(lane), true)
                    }));
                    for (m, round) in mesh.iter_mut().zip(&mut rounds) {
                        gathered.clear();
                        m.step(round, &mut gathered).expect("a round that fits one part");
                        got += gathered.iter().map(|b| b.len() as u64).sum::<u64>();
                    }
                }
                got
            });
            assert_eq!(got, rows * (lanes as u64) * ROUNDS, "{lanes} lanes at nw={nw}");
            println!(
                "mesh_round_bench nw={nw} rows={rows} lanes={lanes:<2} {:>10.1} instr/worker-lane",
                instructions as f64 / (ROUNDS * nw as u64 * lanes as u64) as f64
            );
        }
    }
}
