//! N-way min-merge tournament: a loser tree.
//!
//! Two operations on the root drive every caller:
//! - `replace_top(new_row, &less)` — overwrite the champion's row and walk
//!   up. The fast path used by every emit on a still-valid source.
//! - `pop_top(&less)` — remove the champion when its source exhausts.
//!
//! Each internal node holds the LOSER of the most recent match between
//! its two subtrees' current champions; `tree[0]` holds the overall
//! champion. A walk-up after a leaf change pays ONE compare per level
//! (vs two for a binary heap's sift-down: pick smaller child, then test
//! against parent). For typical k=2..16 this halves the comparator cost
//! on the hot merge path; the inlined payload tie-break inside `less`
//! sees the same halving.
//!
//! The node is **keyless** — just `(source_idx, row)`, 8 bytes. `less`
//! reads each player's OPK bytes straight from the caller's sources via
//! `(source_idx, row)` (and tie-breaks on payload from the same pair), so the
//! comparator captures only immutable references and never collides with a
//! `&mut cursors` borrow held elsewhere in the merge driver. There is no cached
//! sort key: OPK byte order *is* the order at every PK width, and the comparator
//! loads those bytes register-cheap via the caller's stride dispatch.
//!
//! No `pos_map`. No caller advances a non-root entry: ReadCursor folds
//! tied rows by repeatedly popping/replacing the root; merge_batches
//! and compact peel the root one at a time.

#[derive(Clone, Copy)]
pub(crate) struct HeapNode {
    /// `u32` (not `usize`): sources are `u32`-bounded and rows-per-source
    /// `< 2^32` (asserted at tree build), so the node is 8 bytes — a single
    /// register swap in `walk_up`. Indexes into the caller's `usize`-typed
    /// source/state arrays via `as usize` at each use site.
    pub source_idx: u32,
    pub row: u32,
}

/// `u32::MAX` is unambiguously distinct from any real `source_idx` (bounded by
/// the caller's source-array length, asserted `< u32::MAX` at build). The `row`
/// field is unused for sentinels — discrimination is on `source_idx` alone.
const SENTINEL: u32 = u32::MAX;

const SENTINEL_NODE: HeapNode = HeapNode { source_idx: SENTINEL, row: 0 };

pub(crate) struct LoserTree {
    /// `tree[0]` is the overall champion. `tree[1..tree.len()]` hold the
    /// loser of each internal node's most recent match. A loser of
    /// `SENTINEL` means the subtree has no real player on one side
    /// (padding leaf or exhausted source). `tree.len()` is always
    /// `n.next_power_of_two().max(1)` and never resizes after build, so
    /// the walk-up arithmetic uses it directly as `n_pad`.
    tree: Vec<HeapNode>,
}

impl LoserTree {
    /// Bottom-up tournament build. O(n) compares, with a transient
    /// `winners` array of `2 * n_pad` HeapNodes that is dropped before
    /// return. Build cost is one-time and ≪1µs at typical k.
    ///
    /// A pure sequential walk-up build (no auxiliary array) is tempting
    /// for code-sharing with the hot path, but it cannot distinguish
    /// "sentinel cur travelling up because the padding leaf produced
    /// nothing" from "sentinel cur travelling up because a real source
    /// just became a placeholder lower in the tree": both look identical
    /// to the walk yet require opposite handling at higher internal
    /// nodes. The bottom-up scheme makes the subtree-winner explicit
    /// and avoids the ambiguity.
    pub fn build(
        n: usize,
        init_fn: impl Fn(usize) -> Option<u32>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> Self {
        // `source_idx`/`row` are `u32`. Sources `< u32::MAX` (the sentinel) and
        // rows-per-source `< 2^32` — fail loudly if a caller ever violates it.
        debug_assert!(n < u32::MAX as usize, "loser tree: source count must be < u32::MAX");
        let n_pad = n.next_power_of_two().max(1);
        let mut tree = vec![SENTINEL_NODE; n_pad];

        // winners[idx] holds the current champion of the subtree rooted
        // at `idx`. Leaves live at indices `n_pad..2*n_pad`; for `i < n`
        // with a live row, leaf `n_pad + i` carries the source's `(i, row)`.
        let mut winners = vec![SENTINEL_NODE; 2 * n_pad];
        for i in 0..n {
            if let Some(row) = init_fn(i) {
                winners[n_pad + i] = HeapNode { source_idx: i as u32, row };
            }
        }

        // Bottom-up: each internal node plays the match between its two
        // subtree winners. Loser → tree[idx]; winner → winners[idx],
        // propagated up to the next match.
        for idx in (1..n_pad).rev() {
            let a = winners[2 * idx];
            let b = winners[2 * idx + 1];
            let (winner, loser) = match (a.source_idx == SENTINEL, b.source_idx == SENTINEL) {
                (true, _) => (b, a),
                (_, true) => (a, b),
                (false, false) => {
                    if less(&a, &b) { (a, b) } else { (b, a) }
                }
            };
            winners[idx] = winner;
            tree[idx] = loser;
        }

        // For n_pad == 1 (n ∈ {0, 1}), the loop above is empty and
        // winners[1] is the leaf value (or sentinel for n=0).
        tree[0] = winners[1];
        Self { tree }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tree[0].source_idx == SENTINEL
    }

    #[inline]
    pub fn peek(&self) -> &HeapNode {
        &self.tree[0]
    }

    /// Steady-state walk-up. PRECONDITION: `cur.source_idx != SENTINEL`
    /// OR `idx == 0` (in which case the loop is a no-op and `cur` is
    /// returned unchanged). The pop_top phase-1/phase-2 split relies on
    /// the latter case to handle a fully-drained tree: phase 1 leaves
    /// `idx == 0` with a sentinel cur, and walk_up safely returns it.
    /// Any other sentinel-cur entry would invoke `less(loser, cur)`,
    /// which (in two of three callsites) indexes `cursors[cur.source_idx]`
    /// and panics on `usize::MAX`.
    ///
    /// `inline(always)` so the closure flattens into the public-API
    /// callers; a hot path with one cmp per level cannot afford a call.
    #[inline(always)]
    fn walk_up(
        &mut self,
        mut cur: HeapNode,
        mut idx: usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> HeapNode {
        while idx > 0 {
            // SAFETY: idx < tree.len() on entry from every caller
            // (`(tree.len() + s) >> 1` with `s < n ≤ tree.len()`,
            // max = tree.len() - 1) and `idx >>= 1` only decreases it.
            let loser = unsafe { self.tree.get_unchecked_mut(idx) };
            if loser.source_idx != SENTINEL && less(loser, &cur) {
                std::mem::swap(&mut cur, loser);
            }
            idx >>= 1;
        }
        cur
    }

    /// Overwrite the champion's row and walk up. The new row's PK may sort
    /// larger or smaller than the prior one; the loser tree handles either
    /// correctly (no monotonicity precondition) since `less` re-reads the bytes.
    #[inline]
    pub fn replace_top(
        &mut self,
        new_row: u32,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        debug_assert!(!self.is_empty(), "replace_top on empty tree");
        let source_idx = self.tree[0].source_idx;
        let cur = HeapNode { source_idx, row: new_row };
        let idx = (self.tree.len() + source_idx as usize) >> 1;
        self.tree[0] = self.walk_up(cur, idx, less);
    }

    /// Remove the champion (its source is exhausted). After return, the
    /// new champion (if any) is at `tree[0]`; otherwise `is_empty()`
    /// returns true.
    #[inline]
    pub fn pop_top(
        &mut self,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        debug_assert!(!self.is_empty(), "pop_top on empty tree");
        let source_idx = self.tree[0].source_idx;
        let mut cur = SENTINEL_NODE;
        let mut idx = (self.tree.len() + source_idx as usize) >> 1;

        // Phase 1: fast-forward sentinel cur up the tree, skipping any
        // sentinel losers (sentinel-vs-sentinel is a no-op compare). At
        // the first real loser, swap (cur becomes real, slot becomes
        // sentinel) and exit so phase 2 runs walk_up with a real cur.
        //
        // Mandatory, not an optimisation: walk_up requires a real cur.
        while idx > 0 {
            // SAFETY: same bound as walk_up — idx is < n_pad on entry
            // and only decreases via `idx >>= 1`.
            let loser = unsafe { self.tree.get_unchecked_mut(idx) };
            if loser.source_idx != SENTINEL {
                std::mem::swap(&mut cur, loser);
                idx >>= 1;
                break;
            }
            idx >>= 1;
        }

        // Phase 2: shared walk-up. If phase 1 found no real loser, idx
        // is 0 and walk_up is a no-op returning the sentinel; tree[0]
        // becomes sentinel and is_empty returns true.
        self.tree[0] = self.walk_up(cur, idx, less);
    }
}

/// Drive an N-way merge to completion.
///
/// `less` — compare two `HeapNode`s; reads each player's OPK bytes (and payload)
///   from the caller's sources via `(source_idx, row)`, NEVER reads live cursor
///   state directly. This is what frees `advance` below to hold the only
///   `&mut cursors` borrow.
/// `advance(src) -> Option<row>` — advance source `src`; returns the cursor's new
///   `row` (a `u32` index) or `None` when exhausted.
/// `same_pk(a_src, a_row, b_src, b_row) -> bool` — true when both positions carry
///   the same PK (OPK byte equality, width-agnostic). The PK term of the group
///   boundary, replacing the old cached-key comparison.
/// `eq_payload(a_src, a_row, b_src, b_row) -> bool` — true when both positions
///   carry the same payload. The payload term of the group boundary; never inside
///   the heap.
/// `weight(src, row) -> i64` — weight at `(src, row)`.
/// `emit(group_src, group_row, net_weight) -> ControlFlow<()>` — called for each
///   non-ghost group; `Break` returns immediately. The output PK is re-derived
///   from `(group_src, group_row)` by the caller (no cached key to pass).
///
/// `#[inline(always)]`: the compact/merge `emit` closures return a
/// constant `ControlFlow::Continue(())` and read_cursor's returns a
/// constant `Break(())`. Forced inlining lets LLVM evaluate the branch
/// at compile time and DCE the unused arm in each monomorphisation.
#[inline(always)]
pub(crate) fn drive_merge<ADV, SP, EQ, W, EM>(
    heap: &mut LoserTree,
    less: impl Fn(&HeapNode, &HeapNode) -> bool,
    mut advance: ADV,
    mut same_pk: SP,
    mut eq_payload: EQ,
    mut weight: W,
    mut emit: EM,
)
where
    ADV: FnMut(usize) -> Option<u32>,
    SP: FnMut(usize, usize, usize, usize) -> bool,
    EQ: FnMut(usize, usize, usize, usize) -> bool,
    W: FnMut(usize, usize) -> i64,
    EM: FnMut(usize, usize, i64) -> std::ops::ControlFlow<()>,
{
    // Step the heap root past `(src, row)` into its successor (or pop if
    // exhausted). Inlined since both call sites share it byte-for-byte.
    #[inline(always)]
    fn step(
        heap: &mut LoserTree,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
        src: usize,
        advance: &mut impl FnMut(usize) -> Option<u32>,
    ) {
        if let Some(new_row) = advance(src) {
            heap.replace_top(new_row, less);
        } else {
            heap.pop_top(less);
        }
    }

    loop {
        if heap.is_empty() { return; }

        let (group_src, group_row) = {
            let top = heap.peek();
            (top.source_idx as usize, top.row as usize)
        };

        // Open the group: account for the root's weight and step past it.
        // No `same_pk`/`eq_payload` test on the first row — by construction it is
        // the group exemplar, so the tests would be tautologically true and
        // `eq_payload` walks every payload column (expensive on wide rows).
        let mut net_weight: i64 = weight(group_src, group_row);
        step(heap, &less, group_src, &mut advance);

        // Fold tied rows: each iteration peeks the new root, breaks on a PK or
        // payload mismatch, otherwise accumulates weight and steps again. The PK
        // term (`same_pk`) replaces the old cached-key compare; the byte equality
        // is exact at every width, so a low-16-prefix collision can no longer
        // false-merge two distinct wide PKs.
        while !heap.is_empty() {
            let (cur_src, cur_row) = {
                let top = heap.peek();
                (top.source_idx as usize, top.row as usize)
            };
            if !same_pk(group_src, group_row, cur_src, cur_row)
                || !eq_payload(group_src, group_row, cur_src, cur_row)
            {
                break;
            }
            net_weight += weight(cur_src, cur_row);
            step(heap, &less, cur_src, &mut advance);
        }

        if net_weight != 0
            && emit(group_src, group_row, net_weight).is_break()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
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
            let (src, row) = { let n = t.peek(); (n.source_idx as usize, n.row as usize) };
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
            vec![],                // exhausted source
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
                    drain_keys(&runs, build_runs(&runs)), expected,
                    "k={k}, seed={seed}, runs={runs:?}"
                );
            }
        }
    }

    /// Keyless ordering by full PK bytes: two players whose OPK bytes agree on
    /// the low 16 but disagree past byte 16 must emit in full-byte order. With a
    /// cached `u128` prefix key (pre-Phase B) the prefix collision risked folding
    /// them; the keyless tree always reads the full bytes through `less`, so this
    /// pins that the tournament orders purely by the comparator.
    #[test]
    fn loser_tree_orders_by_full_bytes() {
        // Two 24-byte keys sharing their low-16 (1,1) prefix, differing past
        // byte 16. The node `row` indexes this table (one row per source).
        let full: [[u8; 24]; 2] = [
            { let mut k = [0u8; 24]; k[0] = 1; k[8] = 1; k[16] = 100; k },
            { let mut k = [0u8; 24]; k[0] = 1; k[8] = 1; k[16] = 200; k },
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
        assert_eq!(order, vec![0, 1],
            "prefix-colliding wide PKs must order by full bytes via `less`");
    }

    /// A real `u128::MAX` key is a value, not the exhausted-source sentinel —
    /// only `source_idx == u32::MAX` is. Round-trip it to confirm separation.
    #[test]
    fn u128_max_key_is_a_real_value() {
        let runs = vec![vec![u128::MAX], vec![0], vec![u128::MAX], vec![50]];
        assert_eq!(drain_keys(&runs, build_runs(&runs)), vec![0, 50, u128::MAX, u128::MAX]);
    }
}
