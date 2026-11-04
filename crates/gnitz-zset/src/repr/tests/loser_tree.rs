use super::*;
use crate::test_support::Rng;

/// A value's player key: coarser than the value, so adjacent values tie on it,
/// and `u128::MAX` for the highest, which is the sentinel's.
fn key(value: u128) -> u128 {
    value | 1
}

/// The order of two players whose keys tie: the value at `(source_idx, row)`,
/// then `source_idx` for a stable tiebreak.
fn run_less(runs: &[Vec<u128>]) -> impl Fn(&Player, &Player) -> bool + '_ {
    move |a, b| {
        let ka = (runs[a.source_idx as usize][a.row as usize], a.source_idx);
        let kb = (runs[b.source_idx as usize][b.row as usize], b.source_idx);
        ka.cmp(&kb).is_lt()
    }
}

/// The canonical k-way merge, for at most `limit` rows: emit each champion as
/// `(key, source)`, then step that source to its next row, or drop it at the
/// run's end. `pos` tracks each source's next unread row.
fn drain(runs: &[Vec<u128>], t: &mut LoserTree, pos: &mut [usize], limit: usize) -> Vec<(u128, u32)> {
    let less = run_less(runs);
    let mut out = Vec::new();
    while out.len() < limit {
        let Some(n) = t.peek() else { break };
        let (src, row) = (n.source_idx as usize, n.row as usize);
        out.push((runs[src][row], n.source_idx));
        pos[src] = row + 1;
        t.step_top(runs[src].get(row + 1).map(|&next| ((row + 1) as u32, key(next))), &less);
    }
    out
}

/// Random k-way merges drain in `(key, source)` order, also across a `rebuild`
/// partway through.
#[test]
fn random_kway_merges_drain_in_key_then_source_order() {
    for &k in &[0usize, 1, 2, 3, 5, 8, 16, 32, STACK_LEAVES + 1, 100] {
        for seed in 0..16u64 {
            let mut rng = Rng::new(seed.wrapping_mul(1_000_003) + k as u64 * 7);

            // `u128::MAX` is a real key here — only `source_idx` discriminates
            // the sentinel. Small values force ties, on the value and on its key;
            // a band of its own per source gives a champion stretches it wins
            // outright.
            let runs: Vec<Vec<u128>> = (0..k)
                .map(|src| {
                    // Empty runs leave adjacent sentinel losers for a drop to walk past.
                    let len = match rng.gen_range(3) {
                        0 => 0,
                        _ => rng.gen_range(30) as usize,
                    };
                    let mut keys: Vec<u128> = (0..len)
                        .map(|_| match rng.gen_range(8) {
                            0 => u128::MAX,
                            1 => rng.gen_range(5) as u128,
                            2 => rng.gen_range(20) as u128,
                            3 | 4 => (src as u128 + 1) << 64 | rng.gen_range(4) as u128,
                            // One leading word every source shares, so only the
                            // second word orders them.
                            5 => 1 << 64 | rng.gen_range(64) as u128,
                            _ => rng.gen_u128(),
                        })
                        .collect();
                    keys.sort();
                    keys
                })
                .collect();

            let mut expected: Vec<(u128, u32)> = runs
                .iter()
                .enumerate()
                .flat_map(|(src, run)| run.iter().map(move |&key| (key, src as u32)))
                .collect();
            expected.sort();

            let mut pos = vec![0usize; k];
            let mut t = LoserTree::empty(k);
            t.rebuild(|i| runs[i].first().map(|&v| (0, key(v))), run_less(&runs));
            let cut = rng.gen_range(expected.len() as u64 + 1) as usize;
            let mut got = drain(&runs, &mut t, &mut pos, cut);
            t.rebuild(
                |i| runs[i].get(pos[i]).map(|&v| (pos[i] as u32, key(v))),
                run_less(&runs),
            );
            got.extend(drain(&runs, &mut t, &mut pos, usize::MAX));
            assert_eq!(got, expected, "k={k}, seed={seed}, cut={cut}, runs={runs:?}");
        }
    }
}

/// `k` sorted runs of random values in `0..40`, so values tie across runs.
fn random_runs(rng: &mut Rng, k: usize) -> Vec<Vec<u128>> {
    (0..k)
        .map(|_| {
            let mut run: Vec<u128> = (0..rng.gen_range(12)).map(|_| rng.gen_range(40) as u128).collect();
            run.sort();
            run
        })
        .collect()
}

/// A tree built in a buffer a larger tournament played in drains as a fresh one.
#[test]
fn a_reused_buffer_drains_like_a_fresh_one() {
    let mut rng = Rng::new(7);
    let big = random_runs(&mut rng, 13);
    let mut t = LoserTree::empty(big.len());
    t.rebuild(|i| big[i].first().map(|&v| (0, key(v))), run_less(&big));
    drain(&big, &mut t, &mut vec![0; big.len()], 5);
    let mut buf = t.take_buffer();
    for k in [0usize, 1, 2, 3, 5, 0, 16, 1] {
        let runs = random_runs(&mut rng, k);
        let mut expected: Vec<(u128, u32)> = runs
            .iter()
            .enumerate()
            .flat_map(|(src, run)| run.iter().map(move |&v| (v, src as u32)))
            .collect();
        expected.sort();
        let mut t = LoserTree::with_buffer(k, buf);
        t.rebuild(|i| runs[i].first().map(|&v| (0, key(v))), run_less(&runs));
        let got = drain(&runs, &mut t, &mut vec![0; k], usize::MAX);
        assert_eq!(got, expected, "k={k}, runs={runs:?}");
        buf = t.take_buffer();
    }
}
