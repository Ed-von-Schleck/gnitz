//! N-way min-merge tournament: a loser tree. Each internal node holds the loser
//! of the last match between its two subtrees' champions; the overall champion
//! is held apart, and replacing it costs one compare per level.

use std::cell::Cell;
use std::mem::MaybeUninit;

use crate::repr::pool::with_scratch;

/// A player: an index into the caller's sources, its current row, and the key
/// the caller gave that row.
#[derive(Clone, Copy)]
pub struct Player {
    /// A lower key wins its match outright; the caller's `less` settles a tie.
    /// Two words, most significant first, so the player carries no padding.
    key: [u64; 2],
    pub source_idx: u32,
    pub row: u32,
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
    pub(super) fn key(&self) -> u128 {
        (self.key[0] as u128) << 64 | self.key[1] as u128
    }
}

/// The leaf count up to which [`LoserTree::rebuild`] keeps its scratch on the
/// stack; past it, in the thread's reused [`WINNERS`].
const STACK_LEAVES: usize = 64;

thread_local! {
    /// [`LoserTree::rebuild`]'s scratch past [`STACK_LEAVES`] leaves.
    static WINNERS: Cell<Vec<MaybeUninit<Player>>> = const { Cell::new(Vec::new()) };
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

pub struct LoserTree {
    /// The champion: the overall min, or a sentinel once every source is out.
    top: Player,
    /// For each internal node `1..n_pad`, the loser of its last match; `[0]` is
    /// unused. Empty when `n_pad == 1`: one leaf has no internal node.
    losers: Vec<Player>,
    /// Leaf count: `n` rounded up to a power of two, at least 1.
    n_pad: usize,
    /// Sources the tournament was built for.
    n: usize,
    /// A key no player but the champion holds a lower one than — keys only
    /// rise — so a champion stepped below it keeps winning without a replay;
    /// `0`, which no key is below, when none is known.
    bound: u128,
}

impl LoserTree {
    /// A tournament over `n` sources in `losers`' allocation, whatever it held
    /// before; every leaf is a sentinel, so it is empty until
    /// [`rebuild`](Self::rebuild) plays it. Allocates the losers only for
    /// `n >= 2`.
    pub fn with_buffer(n: usize, mut losers: Vec<Player>) -> Self {
        debug_assert!(n < u32::MAX as usize, "source count must be < u32::MAX, the sentinel");
        let n_pad = n.next_power_of_two().max(1);
        losers.clear();
        if n_pad > 1 {
            losers.resize(n_pad, SENTINEL_PLAYER);
        }
        Self {
            top: SENTINEL_PLAYER,
            losers,
            n_pad,
            n,
            bound: 0,
        }
    }

    /// The losers' allocation, for a later [`Self::with_buffer`]; leaves the
    /// tree without one.
    pub fn take_buffer(&mut self) -> Vec<Player> {
        std::mem::take(&mut self.losers)
    }

    /// Re-play the tournament over the same `n` sources at their current rows.
    /// O(n) compares, no allocation once the thread's scratch has grown.
    pub fn rebuild(&mut self, init_fn: impl Fn(usize) -> Option<(u32, u128)>, less: impl Fn(&Player, &Player) -> bool) {
        let slots = 2 * self.n_pad;
        if self.n_pad <= STACK_LEAVES {
            let mut stack = [MaybeUninit::<Player>::uninit(); 2 * STACK_LEAVES];
            self.replay(&mut stack[..slots], &init_fn, &less);
        } else {
            with_scratch(&WINNERS, |winners| {
                winners.resize(slots, MaybeUninit::uninit());
                self.replay(winners, &init_fn, &less);
            });
        }
    }

    /// Play every match bottom-up. `winners` holds `2 * n_pad` slots: the
    /// champion of the subtree at each index, leaves at `n_pad..`, `[0]` unused.
    #[inline(always)]
    fn replay(
        &mut self,
        winners: &mut [MaybeUninit<Player>],
        init_fn: &impl Fn(usize) -> Option<(u32, u128)>,
        less: &impl Fn(&Player, &Player) -> bool,
    ) {
        let n_pad = self.n_pad;
        let (real, padding) = winners[n_pad..].split_at_mut(self.n);
        for (i, leaf) in real.iter_mut().enumerate() {
            leaf.write(match init_fn(i) {
                Some((row, key)) => Player::new(key, i as u32, row),
                None => SENTINEL_PLAYER,
            });
        }
        // A padding leaf never reaches `init_fn`.
        padding.fill(MaybeUninit::new(SENTINEL_PLAYER));
        for idx in (1..n_pad).rev() {
            // SAFETY: both children lie above `idx`: leaves, written above, or
            // internal nodes, written by an earlier iteration.
            let (a, b) = unsafe { (winners[2 * idx].assume_init(), winners[2 * idx + 1].assume_init()) };
            let (winner, loser) = if beats(&a, &b, less) { (a, b) } else { (b, a) };
            winners[idx].write(winner);
            self.losers[idx] = loser;
        }
        // SAFETY: index 1 is the root, written by the loop when `n_pad > 1`,
        // else the one leaf.
        self.top = unsafe { winners[1].assume_init() };
        self.bound = 0;
    }

    /// The current champion, or `None` once every source is exhausted.
    #[inline]
    pub fn peek(&self) -> Option<Player> {
        let top = self.top;
        (top.source_idx != SENTINEL).then_some(top)
    }

    /// The key below which the champion's source holds the least row of every
    /// source; `0`, which no key is below, when none is known.
    #[inline(always)]
    pub(crate) fn bound(&self) -> u128 {
        self.bound
    }

    /// The internal node above leaf `source_idx`.
    #[inline(always)]
    fn leaf_parent(&self, source_idx: u32) -> usize {
        (self.n_pad + (source_idx as usize & (self.n_pad - 1))) >> 1
    }

    /// Step the champion to `next_row`, or drop it when its source is exhausted.
    ///
    /// A champion that stays one keeps its path's losers, whose least key is
    /// then the bound under which its next steps skip the replay: a run that does
    /// not interleave with the others is walked at one compare per row.
    ///
    /// Returns whether the step took that skip: the new champion is then below
    /// [`bound`](Self::bound), and so below every other source's head.
    #[inline(always)]
    pub fn step_top(&mut self, next_row: Option<(u32, u128)>, less: &impl Fn(&Player, &Player) -> bool) -> bool {
        let source_idx = self.top.source_idx;
        debug_assert_ne!(source_idx, SENTINEL, "step_top on a drained tree");
        let mut cur = match next_row {
            Some((row, key)) if key < self.bound => {
                self.top = Player::new(key, source_idx, row);
                return true;
            }
            Some((row, key)) => Player::new(key, source_idx, row),
            None => SENTINEL_PLAYER,
        };
        let mut idx = self.leaf_parent(source_idx);
        while idx > 0 {
            // SAFETY: `leaf_parent` masks into `0..n_pad` and `idx >>= 1` only
            // decreases it; an `idx > 0` exists only when `n_pad > 1`, and then
            // `losers.len() == n_pad`.
            let loser = unsafe { self.losers.get_unchecked_mut(idx) };
            if beats(loser, &cur, less) {
                std::mem::swap(&mut cur, loser);
            }
            idx >>= 1;
        }
        self.top = cur;
        // A player swapped into the tree never leaves it on this replay, so the
        // champion is still `source_idx` exactly when it beat every loser. Its
        // path's losers are then the champions of every other subtree. A
        // champion that stays one at its bound swapped nothing, so its bound
        // still holds; one above it would have lost to the loser holding it.
        if cur.source_idx != source_idx {
            self.bound = 0;
        } else if cur.key() != self.bound {
            self.bound = self.path_least(source_idx);
        }
        false
    }

    /// The least key a loser on `source_idx`'s path holds.
    #[inline(never)]
    fn path_least(&self, source_idx: u32) -> u128 {
        let (mut least, mut idx) = (u128::MAX, self.leaf_parent(source_idx));
        while idx > 0 {
            least = least.min(self.losers[idx].key());
            idx >>= 1;
        }
        least
    }
}

#[cfg(test)]
#[path = "tests/loser_tree.rs"]
mod tests;
