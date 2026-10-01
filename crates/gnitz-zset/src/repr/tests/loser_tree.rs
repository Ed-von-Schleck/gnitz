use super::*;
use crate::test_support::Rng;

/// The tree is keyless, so the key lives out here: the value at
/// `(source_idx, row)`, then `source_idx` for a stable tiebreak.
fn run_less(runs: &[Vec<u128>]) -> impl Fn(&HeapNode, &HeapNode) -> bool + '_ {
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
        t.step_top((row + 1 < runs[src].len()).then(|| (row + 1) as u32), &less);
    }
    out
}

/// Random k-way merges drain in `(key, source)` order, also across a `rebuild`
/// partway through.
#[test]
fn random_kway_merges_drain_in_key_then_source_order() {
    for &k in &[0usize, 1, 2, 3, 5, 8, 16, 32] {
        for seed in 0..16u64 {
            let mut rng = Rng::new(seed.wrapping_mul(1_000_003) + k as u64 * 7);

            // `u128::MAX` is a real key here — only `source_idx` discriminates
            // the sentinel. Small values force ties.
            let runs: Vec<Vec<u128>> = (0..k)
                .map(|_| {
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
            let mut t = LoserTree::build(k, |i| (!runs[i].is_empty()).then_some(0), run_less(&runs));
            let cut = rng.gen_range(expected.len() as u64 + 1) as usize;
            let mut got = drain(&runs, &mut t, &mut pos, cut);
            t.rebuild(|i| (pos[i] < runs[i].len()).then(|| pos[i] as u32), run_less(&runs));
            got.extend(drain(&runs, &mut t, &mut pos, usize::MAX));
            assert_eq!(got, expected, "k={k}, seed={seed}, cut={cut}, runs={runs:?}");
        }
    }
}
