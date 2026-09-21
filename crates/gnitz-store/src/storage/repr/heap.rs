//! N-way min-merge tournament: a loser tree. Each internal node holds the loser
//! of the last match between its two subtrees' champions, so `tree[0]` is the
//! overall champion and replacing it costs one compare per level.

/// A player: an index into the caller's sources, and its current row.
#[derive(Clone, Copy)]
pub(crate) struct HeapNode {
    pub source_idx: u32,
    pub row: u32,
}

/// Not a player: a padding leaf, or a source that has run out. `row` is unread.
const SENTINEL: u32 = u32::MAX;

const SENTINEL_NODE: HeapNode = HeapNode { source_idx: SENTINEL, row: 0 };

/// `a` wins its match against `b`. A sentinel loses to every real player, so
/// `less` never sees one.
#[inline(always)]
fn beats(a: &HeapNode, b: &HeapNode, less: &impl Fn(&HeapNode, &HeapNode) -> bool) -> bool {
    a.source_idx != SENTINEL && (b.source_idx == SENTINEL || less(a, b))
}

pub(crate) struct LoserTree {
    /// `tree[0]` is the champion; `tree[1..]` the loser of each internal node's
    /// last match. Its length is a power of two and never changes.
    tree: Vec<HeapNode>,
    /// Champion of the subtree rooted at each index — [`rebuild`](Self::rebuild)'s
    /// scratch, reused across rebuilds.
    winners: Vec<HeapNode>,
    /// Sources the tournament was built for.
    n: usize,
}

impl LoserTree {
    /// Buffers for a tournament over `n` sources, every leaf a sentinel — empty
    /// until [`rebuild`](Self::rebuild) plays it.
    pub(crate) fn empty(n: usize) -> Self {
        debug_assert!(n < u32::MAX as usize, "source count must be < u32::MAX, the sentinel");
        let n_pad = n.next_power_of_two().max(1);
        Self {
            tree: vec![SENTINEL_NODE; n_pad],
            winners: vec![SENTINEL_NODE; 2 * n_pad],
            n,
        }
    }

    /// Allocate a tournament over `n` sources and play it.
    pub(crate) fn build(
        n: usize,
        init_fn: impl Fn(usize) -> Option<u32>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> Self {
        let mut this = Self::empty(n);
        this.rebuild(init_fn, less);
        this
    }

    /// Re-play the tournament over the same `n` sources at their current rows.
    /// O(n) compares, no allocation.
    pub(crate) fn rebuild(
        &mut self,
        init_fn: impl Fn(usize) -> Option<u32>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        let n_pad = self.tree.len();
        let leaf = |i: usize| n_pad + i;

        self.winners[leaf(0)..].fill(SENTINEL_NODE);
        for i in 0..self.n {
            if let Some(row) = init_fn(i) {
                self.winners[leaf(i)] = HeapNode { source_idx: i as u32, row };
            }
        }
        for idx in (1..n_pad).rev() {
            let (a, b) = (self.winners[2 * idx], self.winners[2 * idx + 1]);
            let (winner, loser) = if beats(&a, &b, &less) { (a, b) } else { (b, a) };
            self.winners[idx] = winner;
            self.tree[idx] = loser;
        }
        self.tree[0] = self.winners[1];
    }

    /// The current champion, or `None` once every source is exhausted.
    #[inline]
    pub(crate) fn peek(&self) -> Option<HeapNode> {
        let top = self.tree[0];
        (top.source_idx != SENTINEL).then_some(top)
    }

    /// The internal node above leaf `source_idx`.
    #[inline(always)]
    fn leaf_parent(&self, source_idx: u32) -> usize {
        let n_pad = self.tree.len();
        (n_pad + (source_idx as usize & (n_pad - 1))) >> 1
    }

    /// Step the champion to `next_row`, or drop it when its source is exhausted.
    #[inline(always)]
    pub(crate) fn step_top(&mut self, next_row: Option<u32>, less: &impl Fn(&HeapNode, &HeapNode) -> bool) {
        let source_idx = self.tree[0].source_idx;
        debug_assert_ne!(source_idx, SENTINEL, "step_top on a drained tree");
        let mut cur = match next_row {
            Some(row) => HeapNode { source_idx, row },
            None => SENTINEL_NODE,
        };
        let mut idx = self.leaf_parent(source_idx);
        while idx > 0 {
            // SAFETY: `leaf_parent` masks into `0..tree.len()`; `idx >>= 1` only decreases it.
            let loser = unsafe { self.tree.get_unchecked_mut(idx) };
            if beats(loser, &cur, less) {
                std::mem::swap(&mut cur, loser);
            }
            idx >>= 1;
        }
        self.tree[0] = cur;
    }
}

#[cfg(test)]
#[path = "tests/heap.rs"]
mod tests;
