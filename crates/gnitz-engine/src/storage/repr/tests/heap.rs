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

// --- layout guard ---

/// `LoserTree::walk_up` swaps whole `HeapNode`s on the hot merge path of
/// every operator. The keyless node is `(source_idx: u32, row: u32)` = 8
/// bytes — a single register swap. Fail loudly here if it grows (a
/// re-introduced cached key, or `usize` fields).
#[test]
fn heap_node_is_8_bytes() {
    assert_eq!(std::mem::size_of::<HeapNode>(), 8);
}

// --- build ---

#[test]
fn build_empty() {
    let t = build_runs(&[]);
    assert!(t.is_empty());
}

#[test]
fn build_single() {
    let runs = vec![vec![42]];
    let t = build_runs(&runs);
    assert!(!t.is_empty());
    assert_eq!(key_at(&runs, t.peek()), 42);
    assert_eq!(t.peek().source_idx, 0);
}

#[test]
fn build_all_exhausted() {
    let t = build_runs(&[vec![], vec![], vec![]]);
    assert!(t.is_empty());
}

#[test]
fn build_some_exhausted() {
    // Entries: 0→30, 1→empty, 2→10. Only 0 and 2 are valid.
    let runs = vec![vec![30], vec![], vec![10]];
    let t = build_runs(&runs);
    assert_eq!(key_at(&runs, t.peek()), 10);
    assert_eq!(t.peek().source_idx, 2);
    assert_eq!(drain_keys(&runs, t), vec![10, 30]);
}

#[test]
fn build_min_at_root() {
    let runs = vec![vec![50], vec![40], vec![30], vec![20], vec![10]];
    let t = build_runs(&runs);
    assert_eq!(key_at(&runs, t.peek()), 10);
    assert_eq!(t.peek().source_idx, 4);
}

/// Regression: a sequential walk-up build mishandles this layout —
/// src2 (the actual minimum) sits in the right subtree as a
/// placeholder while a stale left-subtree placeholder gets promoted
/// to the root. Bottom-up build sets it correctly.
#[test]
fn build_min_in_right_subtree_padded() {
    let runs = vec![vec![30], vec![40], vec![10]];
    let t = build_runs(&runs);
    assert_eq!(key_at(&runs, t.peek()), 10);
    assert_eq!(t.peek().source_idx, 2);
    assert_eq!(drain_keys(&runs, t), vec![10, 30, 40]);
}

// --- replace_top ---

#[test]
fn replace_top_sinks_below_sibling() {
    // src0 advances 10 → 25, sinking below src1's 20.
    let runs = vec![vec![10, 25], vec![20], vec![30]];
    let mut t = build_runs(&runs);
    assert_eq!(t.peek().source_idx, 0);

    t.replace_top(1, &run_less(&runs));

    assert_eq!(key_at(&runs, t.peek()), 20);
    assert_eq!(t.peek().source_idx, 1);
}

#[test]
fn replace_top_to_max_sinks_to_leaf() {
    let runs = vec![vec![1, 1000], vec![2], vec![3]];
    let mut t = build_runs(&runs);
    t.replace_top(1, &run_less(&runs)); // src0 1 → 1000
    assert_eq!(key_at(&runs, t.peek()), 2);
    assert_eq!(t.peek().source_idx, 1);
    assert_eq!(drain_keys(&runs, t), vec![2, 3, 1000]);
}

#[test]
fn replace_top_already_min_stays_root() {
    let runs = vec![vec![1, 3], vec![5], vec![10]];
    let mut t = build_runs(&runs);
    t.replace_top(1, &run_less(&runs)); // src0 1 → 3, still the min
    assert_eq!(key_at(&runs, t.peek()), 3);
    assert_eq!(t.peek().source_idx, 0);
}

/// `row` is load-bearing for the production `drive_merge` path: callers read
/// it back from `heap.peek().row` to index their source data (and the keyless
/// comparator reads the key through it). Verify it round-trips through both
/// root-mutating ops.
#[test]
fn row_field_round_trips_through_replace_and_pop() {
    // src0 run = [10, 15, 99]; src1 = [20]; src2 = [30].
    let runs = vec![vec![10, 15, 99], vec![20], vec![30]];
    let less = run_less(&runs);
    let mut t = build_runs(&runs);

    // src0 advances to row 1 (key 15), still the new min.
    t.replace_top(1, &less);
    assert_eq!(t.peek().source_idx, 0);
    assert_eq!(t.peek().row, 1);
    assert_eq!(key_at(&runs, t.peek()), 15);

    // src0 advances to row 2 (key 99), sinking below src1 — the promoted
    // node must carry src1's (source_idx, row), not src0's stale row.
    t.replace_top(2, &less);
    assert_eq!(t.peek().source_idx, 1);
    assert_eq!(t.peek().row, 0);
    assert_eq!(key_at(&runs, t.peek()), 20);

    // Pop src1; src2 is promoted with its original row 0.
    t.pop_top(&less);
    assert_eq!(t.peek().source_idx, 2);
    assert_eq!(t.peek().row, 0);
}

// --- pop_top ---

#[test]
fn pop_top_removes_root() {
    let runs = vec![vec![10], vec![20], vec![30]];
    let mut t = build_runs(&runs);
    t.pop_top(&run_less(&runs));
    assert_eq!(key_at(&runs, t.peek()), 20);
    assert_eq!(drain_keys(&runs, t), vec![20, 30]);
}

#[test]
fn pop_top_single_entry_empties() {
    let runs = vec![vec![42]];
    let mut t = build_runs(&runs);
    t.pop_top(&run_less(&runs));
    assert!(t.is_empty());
}

#[test]
fn pop_top_drains_in_sorted_order() {
    let runs = vec![vec![40], vec![10], vec![30], vec![20]];
    let t = build_runs(&runs);
    assert_eq!(drain_keys(&runs, t), vec![10, 20, 30, 40]);
}

/// pop_top phase-1 must walk past multiple sentinel losers up the
/// tree before finding a real one. With k=8 and only sources 0 and 5
/// real, popping src 0 forces phase 1 to traverse two adjacent
/// sentinel internal nodes before hitting the real loser at the
/// next subtree boundary.
#[test]
fn pop_top_walks_past_sentinel_losers() {
    let runs = vec![vec![10], vec![], vec![], vec![], vec![], vec![99], vec![], vec![]];
    let mut t = build_runs(&runs);
    assert_eq!(key_at(&runs, t.peek()), 10);
    assert_eq!(t.peek().source_idx, 0);
    t.pop_top(&run_less(&runs));
    assert_eq!(key_at(&runs, t.peek()), 99);
    assert_eq!(t.peek().source_idx, 5);
    t.pop_top(&run_less(&runs));
    assert!(t.is_empty());
}

// --- mixed ops ---

#[test]
fn mixed_ops_drain_sorted() {
    // src0 = [5, 50]; src1 = [15, 16]; the rest single-key.
    let runs = vec![vec![5, 50], vec![15, 16], vec![25], vec![35], vec![45]];
    let less = run_less(&runs);
    let mut t = build_runs(&runs);
    // src0 advances: 5 → 50.
    t.replace_top(1, &less);
    assert_eq!(key_at(&runs, t.peek()), 15);
    // src1 advances: 15 → 16.
    t.replace_top(1, &less);
    assert_eq!(key_at(&runs, t.peek()), 16);
    // src1 exhausts.
    t.pop_top(&less);
    assert_eq!(key_at(&runs, t.peek()), 25);
    // Drain the rest.
    assert_eq!(drain_keys(&runs, t), vec![25, 35, 45, 50]);
}

/// Build then drain: emitted sequence is the sorted input (one key per source).
#[test]
fn drain_emits_sorted() {
    let keys = [97u128, 12, 53, 88, 1, 44, 73, 25, 60, 32, 18, 91];
    let runs: Vec<Vec<u128>> = keys.iter().map(|&k| vec![k]).collect();

    let mut sorted = keys.to_vec();
    sorted.sort();
    assert_eq!(drain_keys(&runs, build_runs(&runs)), sorted);
}

/// The read-cursor pattern: many sources, each a sorted run; the merge emits
/// the merged-and-sorted concatenation. `drain_keys` *is* that merge (the
/// keyless comparator reads each source's key through `(source_idx, row)`).
#[test]
fn k_way_merge_against_reference() {
    let runs: Vec<Vec<u128>> = vec![
        vec![1, 4, 7, 10, 13],
        vec![2, 5, 8, 11, 14],
        vec![3, 6, 9, 12, 15],
        vec![], // exhausted source
        vec![0, 100, 200],
    ];
    let mut expected: Vec<u128> = runs.iter().flatten().copied().collect();
    expected.sort();
    assert_eq!(drain_keys(&runs, build_runs(&runs)), expected);
}

// --- property test: random k-way merges vs sorted reference ---
use crate::test_rng::Rng;

#[test]
fn property_kway_merge_random() {
    for &k in &[2usize, 3, 5, 8, 16, 32] {
        for seed in 0..16u64 {
            let mut rng = Rng::new(seed.wrapping_mul(1_000_003) + k as u64 * 7);

            // Per-source sorted run; mix in u128::MAX, ties on small
            // values, and full-range randoms to exercise the tie-break
            // and the value/sentinel separation (a u128::MAX key is a real
            // value; only source_idx == u32::MAX is the sentinel).
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

/// Keyless ordering by full PK bytes: two players whose OPK bytes agree on
/// the low 16 but disagree past byte 16 must emit in full-byte order. A cached
/// `u128` prefix key would risk folding them; the keyless tree always reads the
/// full bytes through `less`, so this pins that the tournament orders purely by
/// the comparator.
#[test]
fn loser_tree_orders_by_full_bytes() {
    // Two 24-byte keys sharing their low-16 (1,1) prefix, differing past
    // byte 16. The node `row` indexes this table (one row per source).
    let full: [[u8; 24]; 2] = [
        {
            let mut k = [0u8; 24];
            k[0] = 1;
            k[8] = 1;
            k[16] = 100;
            k
        },
        {
            let mut k = [0u8; 24];
            k[0] = 1;
            k[8] = 1;
            k[16] = 200;
            k
        },
    ];
    let byte_less = |a: &HeapNode, b: &HeapNode| {
        full[a.row as usize][..].cmp(&full[b.row as usize][..]) == std::cmp::Ordering::Less
    };
    // Source i carries full[i]: src 0 the smaller (100), src 1 the larger (200).
    let mut t = LoserTree::build(2, |i| Some(i as u32), byte_less);
    let mut order = Vec::new();
    while !t.is_empty() {
        order.push(t.peek().row);
        t.pop_top(&byte_less);
    }
    assert_eq!(
        order,
        vec![0, 1],
        "prefix-colliding wide PKs must order by full bytes via `less`"
    );
}

/// A real `u128::MAX` key is a value, not the exhausted-source sentinel —
/// only `source_idx == u32::MAX` is. Round-trip it to confirm separation.
#[test]
fn u128_max_key_is_a_real_value() {
    let runs = vec![vec![u128::MAX], vec![0], vec![u128::MAX], vec![50]];
    assert_eq!(drain_keys(&runs, build_runs(&runs)), vec![0, 50, u128::MAX, u128::MAX]);
}
