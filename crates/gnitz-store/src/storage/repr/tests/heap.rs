use super::*;

/// The tree is keyless, so the key lives out here: the value at
/// `(source_idx, row)`, then `source_idx` for a stable tiebreak.
fn run_less(runs: &[Vec<u128>]) -> impl Fn(&HeapNode, &HeapNode) -> bool + '_ {
    move |a, b| {
        let ka = (runs[a.source_idx as usize][a.row as usize], a.source_idx);
        let kb = (runs[b.source_idx as usize][b.row as usize], b.source_idx);
        ka.cmp(&kb).is_lt()
    }
}

/// The keyed value at a node, via its `(source_idx, row)`.
fn key_at(runs: &[Vec<u128>], n: &HeapNode) -> u128 {
    runs[n.source_idx as usize][n.row as usize]
}

/// Build a tree over `runs`, each non-empty source starting at row 0.
fn build_runs(runs: &[Vec<u128>]) -> LoserTree {
    LoserTree::build(runs.len(), |i| (!runs[i].is_empty()).then_some(0u32), run_less(runs))
}

/// The canonical k-way merge over `runs`: emit each champion's value, then step
/// that source to its next row, or drop it at the run's end.
fn drain_keys(runs: &[Vec<u128>], mut t: LoserTree) -> Vec<u128> {
    let less = run_less(runs);
    let mut out = Vec::new();
    while let Some(n) = t.peek() {
        let (src, row) = (n.source_idx as usize, n.row as usize);
        out.push(runs[src][row]);
        t.step_top((row + 1 < runs[src].len()).then(|| (row + 1) as u32), &less);
    }
    out
}

/// `step_top` swaps whole nodes up the tree on every merge, so the node must
/// stay register-sized: a re-introduced cached key or a `usize` field fails here.
#[test]
fn heap_node_is_8_bytes() {
    assert_eq!(std::mem::size_of::<HeapNode>(), 8);
}

/// A padded tournament whose minimum sits in the right subtree, behind a
/// sentinel leaf: the build must still seat src2 at the root.
#[test]
fn build_min_in_right_subtree_padded() {
    let runs = vec![vec![30], vec![40], vec![10]];
    let t = build_runs(&runs);
    assert_eq!(key_at(&runs, &t.peek().unwrap()), 10);
    assert_eq!(t.peek().unwrap().source_idx, 2);
    assert_eq!(drain_keys(&runs, t), vec![10, 30, 40]);
}

/// Dropping src0 with only sources 0 and 5 live walks past two adjacent
/// sentinel losers before reaching a real one.
#[test]
fn step_top_drop_walks_past_sentinel_losers() {
    let runs = vec![vec![10], vec![], vec![], vec![], vec![], vec![99], vec![], vec![]];
    let mut t = build_runs(&runs);
    assert_eq!(t.peek().unwrap().source_idx, 0);
    t.step_top(None, &run_less(&runs));
    assert_eq!(key_at(&runs, &t.peek().unwrap()), 99);
    assert_eq!(t.peek().unwrap().source_idx, 5);
    t.step_top(None, &run_less(&runs));
    assert!(t.peek().is_none());
}

// --- property test: random k-way merges vs sorted reference ---
use crate::test_rng::Rng;

/// Random k-way merges against the sorted reference.
#[test]
fn property_kway_merge_random() {
    for &k in &[0usize, 1, 2, 3, 5, 8, 16, 32] {
        for seed in 0..16u64 {
            let mut rng = Rng::new(seed.wrapping_mul(1_000_003) + k as u64 * 7);

            // `u128::MAX` is a real key here — only `source_idx` discriminates
            // the sentinel. Small values force ties; runs are randomly empty.
            let runs: Vec<Vec<u128>> = (0..k)
                .map(|_| {
                    let len = rng.gen_range(30) as usize;
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

            let mut expected: Vec<u128> = runs.iter().flatten().copied().collect();
            expected.sort();
            assert_eq!(
                drain_keys(&runs, build_runs(&runs)),
                expected,
                "k={k}, seed={seed}, runs={runs:?}"
            );
        }
    }
}
