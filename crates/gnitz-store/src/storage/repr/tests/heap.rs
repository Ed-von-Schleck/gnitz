use super::*;

/// `less` over per-source sorted runs: order by the keyed value at
/// `(source_idx, row)`, then `source_idx` for a stable tiebreak. The heap is
/// keyless, so the key lives here — indexed exactly as the production
/// comparators index their sources from `(source_idx, row)`.
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

/// Drain the whole tree in merge order: emit the keyed value at each popped
/// root, then advance within that source's run (`replace_top` to the next
/// row, else `pop_top`). The canonical k-way merge over `runs`.
fn drain_keys(runs: &[Vec<u128>], mut t: LoserTree) -> Vec<u128> {
    let less = run_less(runs);
    let mut out = Vec::new();
    while !t.is_empty() {
        let (src, row) = {
            let n = t.peek();
            (n.source_idx as usize, n.row as usize)
        };
        out.push(runs[src][row]);
        if row + 1 < runs[src].len() {
            t.replace_top((row + 1) as u32, &less);
        } else {
            t.pop_top(&less);
        }
    }
    out
}

/// `LoserTree::walk_up` swaps whole `HeapNode`s on the hot merge path of every
/// operator. The keyless node is `(source_idx: u32, row: u32)` = 8 bytes, a
/// single register swap. Fail loudly if it grows (a re-introduced cached key,
/// or `usize` fields).
#[test]
fn heap_node_is_8_bytes() {
    assert_eq!(std::mem::size_of::<HeapNode>(), 8);
}

/// Regression: a sequential walk-up build mishandles this layout — src2 (the
/// actual minimum) sits in the right subtree as a placeholder while a stale
/// left-subtree placeholder is promoted to the root. Bottom-up build sets it
/// correctly.
#[test]
fn build_min_in_right_subtree_padded() {
    let runs = vec![vec![30], vec![40], vec![10]];
    let t = build_runs(&runs);
    assert_eq!(key_at(&runs, t.peek()), 10);
    assert_eq!(t.peek().source_idx, 2);
    assert_eq!(drain_keys(&runs, t), vec![10, 30, 40]);
}

/// `pop_top`'s first phase must walk past several sentinel losers before it
/// finds a real one. With k=8 and only sources 0 and 5 live, popping src0
/// traverses two adjacent sentinel internal nodes before reaching the real
/// loser at the next subtree boundary — a shape the random property test
/// below reaches only vanishingly rarely.
#[test]
fn pop_top_walks_past_sentinel_losers() {
    let runs = vec![vec![10], vec![], vec![], vec![], vec![], vec![99], vec![], vec![]];
    let mut t = build_runs(&runs);
    assert_eq!(t.peek().source_idx, 0);
    t.pop_top(&run_less(&runs));
    assert_eq!(key_at(&runs, t.peek()), 99);
    assert_eq!(t.peek().source_idx, 5);
    t.pop_top(&run_less(&runs));
    assert!(t.is_empty());
}

// --- property test: random k-way merges vs sorted reference ---
use crate::test_rng::Rng;

/// `drain_keys` is the whole tournament — build, `replace_top`, `pop_top` and
/// the sentinel walk — so any misplaced champion, stale `row` or dropped
/// player shows up as a drain that is not the sorted input.
#[test]
fn property_kway_merge_random() {
    for &k in &[0usize, 1, 2, 3, 5, 8, 16, 32] {
        for seed in 0..16u64 {
            let mut rng = Rng::new(seed.wrapping_mul(1_000_003) + k as u64 * 7);

            // Per-source sorted run; mix in u128::MAX, ties on small values, and
            // full-range randoms to exercise the tie-break and the
            // value/sentinel separation (a u128::MAX key is a real value; only
            // source_idx == u32::MAX is the sentinel). Runs are randomly empty.
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
