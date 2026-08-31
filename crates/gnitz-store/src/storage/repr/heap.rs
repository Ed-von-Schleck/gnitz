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
//! sort key: OPK byte order *is* the order at every PK width, so the comparator
//! (`compare_pk_ordering`) reads each player's bytes straight from its source.
//!
//! No `pos_map`. No caller advances a non-root entry: ReadCursor folds
//! tied rows by repeatedly popping/replacing the root; the flush merge
//! and compaction peel the root one at a time.

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

const SENTINEL_NODE: HeapNode = HeapNode {
    source_idx: SENTINEL,
    row: 0,
};

pub(crate) struct LoserTree {
    /// `tree[0]` is the overall champion. `tree[1..tree.len()]` hold the
    /// loser of each internal node's most recent match. A loser of
    /// `SENTINEL` means the subtree has no real player on one side
    /// (padding leaf or exhausted source). `tree.len()` is always
    /// `n.next_power_of_two().max(1)` and never resizes after build, so
    /// the walk-up arithmetic uses it directly as `n_pad`.
    tree: Vec<HeapNode>,
    /// Subtree champions, scratch for [`rebuild`](Self::rebuild). Retained
    /// rather than allocated per rebuild: a read cursor re-runs the tournament
    /// on every absolute reposition, always at the same `n`.
    winners: Vec<HeapNode>,
    /// Source count the tournament was built for; `tree`/`winners` are sized
    /// from it and neither resizes.
    n: usize,
}

impl LoserTree {
    /// Buffers for a tournament over `n` sources, with every leaf a sentinel —
    /// an empty tree until [`rebuild`](Self::rebuild) plays it.
    pub(crate) fn empty(n: usize) -> Self {
        // `source_idx`/`row` are `u32`. Sources `< u32::MAX` (the sentinel) and
        // rows-per-source `< 2^32` — fail loudly if a caller ever violates it.
        debug_assert!(n < u32::MAX as usize, "loser tree: source count must be < u32::MAX");
        let n_pad = n.next_power_of_two().max(1);
        Self {
            tree: vec![SENTINEL_NODE; n_pad],
            winners: vec![SENTINEL_NODE; 2 * n_pad],
            n,
        }
    }

    /// Allocate a tournament over `n` sources and play it. Callers that
    /// re-tournament the same sources (a read cursor, on every reposition) build
    /// once and then call [`rebuild`](Self::rebuild), which reuses both buffers.
    pub(crate) fn build(
        n: usize,
        init_fn: impl Fn(usize) -> Option<u32>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> Self {
        let mut this = Self::empty(n);
        this.rebuild(init_fn, less);
        this
    }

    /// Re-play the tournament over the same `n` sources at their current rows,
    /// reusing both buffers. O(n) compares, no allocation.
    ///
    /// A pure sequential walk-up rebuild (no auxiliary array) is tempting for
    /// code-sharing with the hot path, but it cannot distinguish "sentinel cur
    /// travelling up because the padding leaf produced nothing" from "sentinel
    /// cur travelling up because a real source just became a placeholder lower
    /// in the tree": both look identical to the walk yet require opposite
    /// handling at higher internal nodes. The bottom-up scheme makes the
    /// subtree-winner explicit and avoids the ambiguity.
    pub(crate) fn rebuild(
        &mut self,
        init_fn: impl Fn(usize) -> Option<u32>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        let n_pad = self.tree.len();
        // winners[idx] holds the current champion of the subtree rooted at
        // `idx`. Leaves live at `n_pad..2*n_pad`; for `i < n` with a live row,
        // leaf `n_pad + i` carries the source's `(i, row)`. Only the leaves need
        // clearing — every internal slot is written by the match loop below.
        self.winners[n_pad..].fill(SENTINEL_NODE);
        for i in 0..self.n {
            if let Some(row) = init_fn(i) {
                self.winners[n_pad + i] = HeapNode {
                    source_idx: i as u32,
                    row,
                };
            }
        }

        // Bottom-up: each internal node plays the match between its two
        // subtree winners. Loser → tree[idx]; winner → winners[idx],
        // propagated up to the next match.
        for idx in (1..n_pad).rev() {
            let a = self.winners[2 * idx];
            let b = self.winners[2 * idx + 1];
            let (winner, loser) = match (a.source_idx == SENTINEL, b.source_idx == SENTINEL) {
                (true, _) => (b, a),
                (_, true) => (a, b),
                (false, false) => {
                    if less(&a, &b) {
                        (a, b)
                    } else {
                        (b, a)
                    }
                }
            };
            self.winners[idx] = winner;
            self.tree[idx] = loser;
        }

        // For n_pad == 1 (n ∈ {0, 1}), the loop above is empty and
        // winners[1] is the leaf value (or sentinel for n=0).
        self.tree[0] = self.winners[1];
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.tree[0].source_idx == SENTINEL
    }

    #[inline]
    pub(crate) fn peek(&self) -> &HeapNode {
        &self.tree[0]
    }

    /// Steady-state walk-up. PRECONDITION: `cur.source_idx != SENTINEL`
    /// OR `idx == 0` (in which case the loop is a no-op and `cur` is
    /// returned unchanged). The pop_top phase-1/phase-2 split relies on
    /// the latter case to handle a fully-drained tree: phase 1 leaves
    /// `idx == 0` with a sentinel cur, and walk_up safely returns it.
    /// Any other sentinel-cur entry would invoke `less(loser, cur)`,
    /// which indexes `cursors[cur.source_idx]` in the merge drivers and panics
    /// on the sentinel index.
    ///
    /// `inline(always)` so this flattens into the public-API callers, which run
    /// one comparison per tournament level. (The comparator itself is a separate
    /// question — `merge_less`'s body is large enough that LLVM leaves it
    /// out-of-line.)
    #[inline(always)]
    fn walk_up(&mut self, mut cur: HeapNode, mut idx: usize, less: &impl Fn(&HeapNode, &HeapNode) -> bool) -> HeapNode {
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
    pub(crate) fn replace_top(&mut self, new_row: u32, less: &impl Fn(&HeapNode, &HeapNode) -> bool) {
        debug_assert!(!self.is_empty(), "replace_top on empty tree");
        let source_idx = self.tree[0].source_idx;
        let cur = HeapNode {
            source_idx,
            row: new_row,
        };
        let idx = (self.tree.len() + source_idx as usize) >> 1;
        self.tree[0] = self.walk_up(cur, idx, less);
    }

    /// Step the champion to `next_row`, or drop it when its source is exhausted.
    /// The one spelling of the transition, so no caller has to know that a
    /// `None` needs `pop_top`'s two-phase sentinel walk rather than
    /// `replace_top`.
    #[inline(always)]
    pub(crate) fn step_top(&mut self, next_row: Option<u32>, less: &impl Fn(&HeapNode, &HeapNode) -> bool) {
        match next_row {
            Some(row) => self.replace_top(row, less),
            None => self.pop_top(less),
        }
    }

    /// Remove the champion (its source is exhausted). After return, the
    /// new champion (if any) is at `tree[0]`; otherwise `is_empty()`
    /// returns true.
    #[inline]
    pub(crate) fn pop_top(&mut self, less: &impl Fn(&HeapNode, &HeapNode) -> bool) {
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
///   boundary.
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
) where
    ADV: FnMut(usize) -> Option<u32>,
    SP: FnMut(usize, usize, usize, usize) -> bool,
    EQ: FnMut(usize, usize, usize, usize) -> bool,
    W: FnMut(usize, usize) -> i64,
    EM: FnMut(usize, usize, i64) -> std::ops::ControlFlow<()>,
{
    loop {
        if heap.is_empty() {
            return;
        }

        let (group_src, group_row) = {
            let top = heap.peek();
            (top.source_idx as usize, top.row as usize)
        };

        // Open the group: account for the root's weight and step past it.
        // No `same_pk`/`eq_payload` test on the first row — by construction it is
        // the group exemplar, so the tests would be tautologically true and
        // `eq_payload` walks every payload column (expensive on wide rows).
        let mut net_weight: i64 = weight(group_src, group_row);
        heap.step_top(advance(group_src), &less);

        // Fold tied rows: each iteration peeks the new root, breaks on a PK or
        // payload mismatch, otherwise accumulates weight and steps again. The PK
        // term (`same_pk`) is exact at every width, so a low-16-prefix collision
        // cannot false-merge two distinct wide PKs.
        while !heap.is_empty() {
            let (cur_src, cur_row) = {
                let top = heap.peek();
                (top.source_idx as usize, top.row as usize)
            };
            if !same_pk(group_src, group_row, cur_src, cur_row) || !eq_payload(group_src, group_row, cur_src, cur_row) {
                break;
            }
            net_weight += weight(cur_src, cur_row);
            heap.step_top(advance(cur_src), &less);
        }

        if net_weight != 0 && emit(group_src, group_row, net_weight).is_break() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "tests/heap.rs"]
mod tests;
