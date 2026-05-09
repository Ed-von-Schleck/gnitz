//! N-way min-merge tournament: a loser tree.
//!
//! Two operations on the root drive every caller:
//! - `replace_top(new_key, new_row, &less)` — overwrite the champion's
//!   key + row and walk up. The fast path used by every emit on a
//!   still-valid source.
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
//! Each node carries `(key, source_idx, row)`. Embedding `row` in the
//! node lets the comparator do payload tie-breaks from the node alone —
//! no read of caller-side cursor state — so `less` captures only
//! immutable references and never collides with a `&mut cursors` borrow
//! held elsewhere in the merge driver.
//!
//! No `pos_map`. No caller advances a non-root entry: ReadCursor folds
//! tied rows by repeatedly popping/replacing the root; merge_batches
//! and compact peel the root one at a time.

#[derive(Clone, Copy)]
pub struct HeapNode {
    pub key: u128,
    pub source_idx: usize,
    pub row: usize,
}

/// `usize::MAX` is unambiguously distinct from any real `source_idx`
/// (bounded by the caller's source-array length). The `key` and `row`
/// fields are unused for sentinels — collisions with a real `u128::MAX`
/// PK are avoided by discriminating on `source_idx` alone.
const SENTINEL: usize = usize::MAX;

const SENTINEL_NODE: HeapNode = HeapNode { key: 0, source_idx: SENTINEL, row: 0 };

pub struct LoserTree {
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
        init_fn: impl Fn(usize) -> Option<(u128, usize)>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> Self {
        let n_pad = n.next_power_of_two().max(1);
        let mut tree = vec![SENTINEL_NODE; n_pad];

        // winners[idx] holds the current champion of the subtree rooted
        // at `idx`. Leaves live at indices `n_pad..2*n_pad`; for `i < n`
        // with a key, leaf `n_pad + i` carries the source's value.
        let mut winners = vec![SENTINEL_NODE; 2 * n_pad];
        for i in 0..n {
            if let Some((key, row)) = init_fn(i) {
                winners[n_pad + i] = HeapNode { key, source_idx: i, row };
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

    /// Overwrite the champion's key + row and walk up. The new key may
    /// be larger or smaller than the prior one; the loser tree handles
    /// either correctly (no monotonicity precondition).
    #[inline]
    pub fn replace_top(
        &mut self,
        new_key: u128,
        new_row: usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        debug_assert!(!self.is_empty(), "replace_top on empty tree");
        let source_idx = self.tree[0].source_idx;
        let cur = HeapNode { key: new_key, source_idx, row: new_row };
        let idx = (self.tree.len() + source_idx) >> 1;
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
        let mut idx = (self.tree.len() + source_idx) >> 1;

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
/// `less` — compare two `HeapNode`s; uses `a.row` / `b.row` for payload
///   tie-breaks, NEVER reads cursor state directly. This is what frees
///   `advance` below to hold the only `&mut cursors` borrow.
/// `advance(src) -> Option<(key, row)>` — advance source `src`; returns
///   the cursor's new (key, row) or `None` when exhausted.
/// `eq_payload(a_src, a_row, b_src, b_row) -> bool` — true when both
///   positions carry the same payload. Used only for the inner
///   group-tie test, never inside the heap.
/// `weight(src, row) -> i64` — weight at `(src, row)`.
/// `emit(group_src, group_row, group_key, net_weight) -> ControlFlow<()>` —
///   called for each non-ghost group; `Break` returns immediately.
///
/// `#[inline(always)]`: the compact/merge `emit` closures return a
/// constant `ControlFlow::Continue(())` and read_cursor's returns a
/// constant `Break(())`. Forced inlining lets LLVM evaluate the branch
/// at compile time and DCE the unused arm in each monomorphisation.
#[inline(always)]
pub fn drive_merge<ADV, EQ, W, EM>(
    heap: &mut LoserTree,
    less: impl Fn(&HeapNode, &HeapNode) -> bool,
    mut advance: ADV,
    mut eq_payload: EQ,
    mut weight: W,
    mut emit: EM,
)
where
    ADV: FnMut(usize) -> Option<(u128, usize)>,
    EQ: FnMut(usize, usize, usize, usize) -> bool,
    W: FnMut(usize, usize) -> i64,
    EM: FnMut(usize, usize, u128, i64) -> std::ops::ControlFlow<()>,
{
    // Step the heap root past `(src, row)` into its successor (or pop if
    // exhausted). Inlined since both call sites share it byte-for-byte.
    #[inline(always)]
    fn step(
        heap: &mut LoserTree,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
        src: usize,
        advance: &mut impl FnMut(usize) -> Option<(u128, usize)>,
    ) {
        if let Some((new_key, new_row)) = advance(src) {
            heap.replace_top(new_key, new_row, less);
        } else {
            heap.pop_top(less);
        }
    }

    loop {
        if heap.is_empty() { return; }

        let (group_src, group_row, group_key) = {
            let top = heap.peek();
            (top.source_idx, top.row, top.key)
        };

        // Open the group: account for the root's weight and step past it.
        // No `eq_payload` test on the first row — by construction it is
        // the group exemplar, so the test would be tautologically true and
        // `eq_payload` walks every payload column (expensive on wide rows).
        let mut net_weight: i64 = weight(group_src, group_row);
        step(heap, &less, group_src, &mut advance);

        // Fold tied rows: each iteration peeks the new root, breaks on a
        // mismatch, otherwise accumulates weight and steps again.
        while !heap.is_empty() {
            let (cur_src, cur_row, cur_key) = {
                let top = heap.peek();
                (top.source_idx, top.row, top.key)
            };
            if cur_key != group_key
                || !eq_payload(group_src, group_row, cur_src, cur_row)
            {
                break;
            }
            net_weight += weight(cur_src, cur_row);
            step(heap, &less, cur_src, &mut advance);
        }

        if net_weight != 0
            && emit(group_src, group_row, group_key, net_weight).is_break()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn less(a: &HeapNode, b: &HeapNode) -> bool {
        if a.key != b.key {
            return a.key < b.key;
        }
        a.source_idx < b.source_idx
    }

    fn build(keys: &[Option<u128>]) -> LoserTree {
        LoserTree::build(keys.len(), |i| keys[i].map(|k| (k, 0)), less)
    }

    fn drain_keys(mut t: LoserTree) -> Vec<u128> {
        let mut out = Vec::new();
        while !t.is_empty() {
            out.push(t.peek().key);
            t.pop_top(&less);
        }
        out
    }

    // --- build ---

    #[test]
    fn build_empty() {
        let t = build(&[]);
        assert!(t.is_empty());
    }

    #[test]
    fn build_single() {
        let t = build(&[Some(42)]);
        assert!(!t.is_empty());
        assert_eq!(t.peek().key, 42);
        assert_eq!(t.peek().source_idx, 0);
    }

    #[test]
    fn build_all_exhausted() {
        let t = build(&[None, None, None]);
        assert!(t.is_empty());
    }

    #[test]
    fn build_some_exhausted() {
        // Entries: 0→30, 1→None, 2→10. Only 0 and 2 are valid.
        let t = build(&[Some(30), None, Some(10)]);
        assert_eq!(t.peek().key, 10);
        assert_eq!(t.peek().source_idx, 2);
        assert_eq!(drain_keys(t), vec![10, 30]);
    }

    #[test]
    fn build_min_at_root() {
        let t = build(&[Some(50), Some(40), Some(30), Some(20), Some(10)]);
        assert_eq!(t.peek().key, 10);
        assert_eq!(t.peek().source_idx, 4);
    }

    /// Regression: a sequential walk-up build mishandles this layout —
    /// src2 (the actual minimum) sits in the right subtree as a
    /// placeholder while a stale left-subtree placeholder gets promoted
    /// to the root. Bottom-up build sets it correctly.
    #[test]
    fn build_min_in_right_subtree_padded() {
        let t = build(&[Some(30), Some(40), Some(10)]);
        assert_eq!(t.peek().key, 10);
        assert_eq!(t.peek().source_idx, 2);
        assert_eq!(drain_keys(t), vec![10, 30, 40]);
    }

    // --- replace_top_key ---

    #[test]
    fn replace_top_sinks_below_sibling() {
        let mut t = build(&[Some(10), Some(20), Some(30)]);
        assert_eq!(t.peek().source_idx, 0);

        t.replace_top(25, 0, &less);

        assert_eq!(t.peek().key, 20);
        assert_eq!(t.peek().source_idx, 1);
    }

    #[test]
    fn replace_top_to_max_sinks_to_leaf() {
        let mut t = build(&[Some(1), Some(2), Some(3)]);
        t.replace_top(1000, 0, &less);
        assert_eq!(t.peek().key, 2);
        assert_eq!(t.peek().source_idx, 1);
        assert_eq!(drain_keys(t), vec![2, 3, 1000]);
    }

    #[test]
    fn replace_top_already_min_stays_root() {
        let mut t = build(&[Some(1), Some(5), Some(10)]);
        t.replace_top(3, 0, &less);
        assert_eq!(t.peek().key, 3);
        assert_eq!(t.peek().source_idx, 0);
    }

    /// `row` is load-bearing for the production drive_merge path: callers
    /// read it back from `heap.peek().row` to index their source data.
    /// Verify the field round-trips through both root-mutating ops.
    #[test]
    fn row_field_round_trips_through_replace_and_pop() {
        let mut t = build(&[Some(10), Some(20), Some(30)]);
        // Replace src 0's leaf with (key=15, row=42), still the new min.
        t.replace_top(15, 42, &less);
        assert_eq!(t.peek().key, 15);
        assert_eq!(t.peek().source_idx, 0);
        assert_eq!(t.peek().row, 42);

        // Replace again, making src 0 lose to src 1 — promoted node must
        // carry src 1's row, not the stale 42.
        t.replace_top(99, 7, &less);
        assert_eq!(t.peek().key, 20);
        assert_eq!(t.peek().source_idx, 1);
        assert_eq!(t.peek().row, 0);

        // Pop src 1; src 2 is promoted with its original row=0.
        t.pop_top(&less);
        assert_eq!(t.peek().source_idx, 2);
        assert_eq!(t.peek().row, 0);
    }

    // --- pop_top ---

    #[test]
    fn pop_top_removes_root() {
        let mut t = build(&[Some(10), Some(20), Some(30)]);
        t.pop_top(&less);
        assert_eq!(t.peek().key, 20);
        assert_eq!(drain_keys(t), vec![20, 30]);
    }

    #[test]
    fn pop_top_single_entry_empties() {
        let mut t = build(&[Some(42)]);
        t.pop_top(&less);
        assert!(t.is_empty());
    }

    #[test]
    fn pop_top_drains_in_sorted_order() {
        let t = build(&[Some(40), Some(10), Some(30), Some(20)]);
        assert_eq!(drain_keys(t), vec![10, 20, 30, 40]);
    }

    /// pop_top phase-1 must walk past multiple sentinel losers up the
    /// tree before finding a real one. With k=8 and only sources 0 and 5
    /// real, popping src 0 forces phase 1 to traverse two adjacent
    /// sentinel internal nodes before hitting the real loser at the
    /// next subtree boundary.
    #[test]
    fn pop_top_walks_past_sentinel_losers() {
        let mut t = build(&[Some(10), None, None, None, None, Some(99), None, None]);
        assert_eq!(t.peek().key, 10);
        assert_eq!(t.peek().source_idx, 0);
        t.pop_top(&less);
        assert_eq!(t.peek().key, 99);
        assert_eq!(t.peek().source_idx, 5);
        t.pop_top(&less);
        assert!(t.is_empty());
    }

    // --- mixed ops ---

    #[test]
    fn mixed_ops_drain_sorted() {
        let mut t = build(&[Some(5), Some(15), Some(25), Some(35), Some(45)]);
        // src 0 advances: 5 → 50.
        t.replace_top(50, 0, &less);
        assert_eq!(t.peek().key, 15);
        // src 1 advances: 15 → 16.
        t.replace_top(16, 0, &less);
        assert_eq!(t.peek().key, 16);
        // src 1 exhausts.
        t.pop_top(&less);
        assert_eq!(t.peek().key, 25);
        // Drain.
        assert_eq!(drain_keys(t), vec![25, 35, 45, 50]);
    }

    /// Build then drain: emitted sequence is the sorted input.
    #[test]
    fn drain_emits_sorted() {
        let keys = [97u128, 12, 53, 88, 1, 44, 73, 25, 60, 32, 18, 91];
        let t = LoserTree::build(keys.len(), |i| Some((keys[i], 0)), less);

        let mut sorted = keys.to_vec();
        sorted.sort();
        assert_eq!(drain_keys(t), sorted);
    }

    /// Simulate the read-cursor pattern: many sources, each contributing
    /// a sorted run; emit by repeatedly replacing root with the next key
    /// from the winning source and popping when a source exhausts. Output
    /// must equal the merged-and-sorted concatenation.
    #[test]
    fn k_way_merge_against_reference() {
        let runs: Vec<Vec<u128>> = vec![
            vec![1, 4, 7, 10, 13],
            vec![2, 5, 8, 11, 14],
            vec![3, 6, 9, 12, 15],
            vec![],                // exhausted source
            vec![0, 100, 200],
        ];
        let mut positions = vec![0usize; runs.len()];
        let mut t = LoserTree::build(
            runs.len(),
            |i| runs[i].first().copied().map(|k| (k, 0)),
            less,
        );

        let mut emitted = Vec::new();
        while !t.is_empty() {
            let src = t.peek().source_idx;
            emitted.push(t.peek().key);
            positions[src] += 1;
            if positions[src] < runs[src].len() {
                t.replace_top(runs[src][positions[src]], 0, &less);
            } else {
                t.pop_top(&less);
            }
        }

        let mut expected: Vec<u128> = runs.iter().flatten().copied().collect();
        expected.sort();
        assert_eq!(emitted, expected);
    }

    // --- property test: random k-way merges vs sorted reference ---
    //
    // Tiny xorshift64* — no rand crate dep, deterministic across hosts.
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self { Self(seed | 1) }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn gen_u128(&mut self) -> u128 {
            ((self.next_u64() as u128) << 64) | (self.next_u64() as u128)
        }
        fn gen_range(&mut self, max: u64) -> u64 {
            self.next_u64() % max
        }
    }

    #[test]
    fn property_kway_merge_random() {
        for &k in &[2usize, 3, 5, 8, 16, 32] {
            for seed in 0..16u64 {
                let mut rng = Rng::new(seed.wrapping_mul(1_000_003) + k as u64 * 7);

                // Per-source sorted run; mix in u128::MAX, ties on small
                // values, and full-range randoms to exercise the
                // tie-break and key/sentinel separation.
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

                let mut positions = vec![0usize; k];
                let mut t = LoserTree::build(
                    k,
                    |i| runs[i].first().copied().map(|k| (k, 0)),
                    less,
                );

                let mut emitted = Vec::with_capacity(runs.iter().map(|r| r.len()).sum());
                while !t.is_empty() {
                    let src = t.peek().source_idx;
                    emitted.push(t.peek().key);
                    positions[src] += 1;
                    if positions[src] < runs[src].len() {
                        t.replace_top(runs[src][positions[src]], 0, &less);
                    } else {
                        t.pop_top(&less);
                    }
                }

                let mut expected: Vec<u128> = runs.iter().flatten().copied().collect();
                expected.sort();
                assert_eq!(
                    emitted, expected,
                    "k={k}, seed={seed}, runs={runs:?}"
                );
            }
        }
    }

    /// Round-trip a real `u128::MAX` PK to confirm sentinel-vs-value
    /// separation: the tree must never treat a `u128::MAX` key as
    /// "exhausted source" — only `source_idx == usize::MAX` does.
    #[test]
    fn u128_max_key_is_a_real_value() {
        let t = build(&[Some(u128::MAX), Some(0), Some(u128::MAX), Some(50)]);
        assert_eq!(drain_keys(t), vec![0, 50, u128::MAX, u128::MAX]);
    }
}
