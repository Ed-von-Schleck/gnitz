//! N-way min-merge tournament: a loser tree. Each internal node holds the loser
//! of the last match between its two subtrees' champions, so `tree[0]` is the
//! overall champion and replacing it costs one compare per level.

/// A player: an index into the caller's sources, its current row, and the key
/// the caller gave that row.
#[derive(Clone, Copy)]
pub(crate) struct Player {
    /// A lower key wins its match outright; the caller's `less` settles a tie.
    /// Two words, most significant first, so the player carries no padding.
    key: [u64; 2],
    pub(crate) source_idx: u32,
    pub(crate) row: u32,
}

// `step_top` swaps whole players on every merge step.
const _: () = assert!(std::mem::size_of::<Player>() == 24);

impl Player {
    #[inline(always)]
    fn new(key: u128, source_idx: u32, row: u32) -> Self {
        Player {
            key: [(key >> 64) as u64, key as u64],
            source_idx,
            row,
        }
    }

    #[inline(always)]
    pub(crate) fn key(&self) -> u128 {
        (self.key[0] as u128) << 64 | self.key[1] as u128
    }
}

/// Not a player: a padding leaf, or a source that has run out. `row` is unread.
const SENTINEL: u32 = u32::MAX;

const SENTINEL_PLAYER: Player = Player {
    key: [u64::MAX; 2],
    source_idx: SENTINEL,
    row: 0,
};

/// `a` wins its match against `b`. A sentinel holds the highest key, so it
/// loses to every real player below that key and ties with one at it, where
/// `source_idx` tells them apart: `less` never sees a sentinel.
#[inline(always)]
fn beats(a: &Player, b: &Player, less: &impl Fn(&Player, &Player) -> bool) -> bool {
    if a.key != b.key {
        return a.key < b.key;
    }
    a.source_idx != SENTINEL && (b.source_idx == SENTINEL || less(a, b))
}

pub(crate) struct LoserTree {
    /// `tree[0]` is the champion; `tree[1..]` the loser of each internal node's
    /// last match. Its length is a power of two and never changes.
    tree: Vec<Player>,
    /// Champion of the subtree rooted at each index — [`rebuild`](Self::rebuild)'s
    /// scratch, reused across rebuilds.
    winners: Vec<Player>,
    /// Sources the tournament was built for.
    n: usize,
    /// A leading key word no player but the champion holds a lower one than —
    /// keys only rise — so a champion stepped below it keeps winning without a
    /// replay.
    bound: u64,
}

impl LoserTree {
    /// Buffers for a tournament over `n` sources, every leaf a sentinel — empty
    /// until [`rebuild`](Self::rebuild) plays it.
    pub(crate) fn empty(n: usize) -> Self {
        debug_assert!(n < u32::MAX as usize, "source count must be < u32::MAX, the sentinel");
        let n_pad = n.next_power_of_two().max(1);
        Self {
            tree: vec![SENTINEL_PLAYER; n_pad],
            winners: vec![SENTINEL_PLAYER; 2 * n_pad],
            n,
            bound: 0,
        }
    }

    /// Re-play the tournament over the same `n` sources at their current rows.
    /// O(n) compares, no allocation.
    pub(crate) fn rebuild(
        &mut self,
        init_fn: impl Fn(usize) -> Option<(u32, u128)>,
        less: impl Fn(&Player, &Player) -> bool,
    ) {
        let n_pad = self.tree.len();
        let leaf = |i: usize| n_pad + i;

        self.winners[leaf(0)..].fill(SENTINEL_PLAYER);
        for i in 0..self.n {
            if let Some((row, key)) = init_fn(i) {
                self.winners[leaf(i)] = Player::new(key, i as u32, row);
            }
        }
        for idx in (1..n_pad).rev() {
            let (a, b) = (self.winners[2 * idx], self.winners[2 * idx + 1]);
            let (winner, loser) = if beats(&a, &b, &less) { (a, b) } else { (b, a) };
            self.winners[idx] = winner;
            self.tree[idx] = loser;
        }
        self.tree[0] = self.winners[1];
        self.bound = 0;
    }

    /// The current champion, or `None` once every source is exhausted.
    #[inline]
    pub(crate) fn peek(&self) -> Option<Player> {
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
    ///
    /// A champion that stays one keeps its path's losers, whose least key is
    /// then the bound under which its next steps skip the replay: a run that does
    /// not interleave with the others is walked at one compare per row.
    #[inline(always)]
    pub(crate) fn step_top(&mut self, next_row: Option<(u32, u128)>, less: &impl Fn(&Player, &Player) -> bool) {
        let source_idx = self.tree[0].source_idx;
        debug_assert_ne!(source_idx, SENTINEL, "step_top on a drained tree");
        let mut cur = match next_row {
            Some((row, key)) if ((key >> 64) as u64) < self.bound => {
                self.tree[0] = Player::new(key, source_idx, row);
                return;
            }
            Some((row, key)) => Player::new(key, source_idx, row),
            None => SENTINEL_PLAYER,
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
        // A player swapped into the tree never leaves it on this replay, so the
        // champion is still `source_idx` exactly when it beat every loser. Its
        // path's losers are then the champions of every other subtree.
        if cur.source_idx == source_idx {
            self.bound = self.path_least(source_idx);
        }
    }

    /// The least leading key word a loser on `source_idx`'s path holds.
    #[inline(never)]
    fn path_least(&self, source_idx: u32) -> u64 {
        let (mut least, mut idx) = (u64::MAX, self.leaf_parent(source_idx));
        while idx > 0 {
            least = least.min(self.tree[idx].key[0]);
            idx >>= 1;
        }
        least
    }
}

#[cfg(test)]
#[path = "tests/loser_tree.rs"]
mod tests;
