//! A fed view's delta feed: the view's recent rounds, each the delta one round
//! applied to it, oldest first.

use std::collections::VecDeque;
use std::rc::Rc;

use gnitz_zset::repr::Batch;

/// What a feed is charged for holding `delta`: its buffers as allocated, the
/// batch behind its reference counts, and its place in the queue.
fn held(delta: &Batch) -> usize {
    delta.allocated_bytes() + std::mem::size_of::<Batch>() + 2 * std::mem::size_of::<usize>() + ENTRY
}

const ENTRY: usize = std::mem::size_of::<(u64, Rc<Batch>)>();

/// The rounds a fed view retains, in the view's own schema. A round is every
/// delta captured under one round number; the oldest go once what is held
/// passes the budget, the newest round never.
pub(crate) struct Feed {
    /// `(round, delta)`, rounds non-decreasing; each delta consolidated.
    rounds: VecDeque<(u64, Rc<Batch>)>,
    /// What `rounds` is charged, each round as [`held`].
    bytes: usize,
    budget: usize,
    /// The highest round dropped; 0 until the first drop.
    dropped_through: u64,
}

impl Feed {
    pub(super) fn new(budget: u64) -> Self {
        Feed {
            rounds: VecDeque::new(),
            bytes: 0,
            budget: usize::try_from(budget).unwrap_or(usize::MAX),
            dropped_through: 0,
        }
    }

    /// Retain `delta` as captured in `round`, and drop the oldest rounds past
    /// the budget.
    pub(super) fn record(&mut self, round: u64, delta: Rc<Batch>) {
        debug_assert!(delta.is_consolidated() && !delta.is_empty());
        debug_assert!(self.rounds.back().is_none_or(|&(last, _)| last <= round));
        self.bytes += held(&delta);
        self.rounds.push_back((round, delta));
        while self.bytes > self.budget {
            let oldest = self.rounds[0].0;
            if oldest == round {
                break;
            }
            self.dropped_through = oldest;
            while let Some((_, gone)) = self.rounds.pop_front_if(|&mut (r, _)| r == oldest) {
                self.bytes -= held(&gone);
            }
        }
    }

    /// The highest round this feed no longer holds: a cursor below it has lost
    /// rounds.
    pub(crate) fn dropped_through(&self) -> u64 {
        self.dropped_through
    }

    /// The deltas of the rounds after `after`, oldest first.
    pub(crate) fn rounds(&self, after: u64) -> impl Iterator<Item = &Rc<Batch>> + Clone {
        let first = self.rounds.partition_point(|&(r, _)| r <= after);
        self.rounds.range(first..).map(|(_, delta)| delta)
    }

    /// What the rounds retained are charged.
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
#[path = "tests/feed.rs"]
mod tests;
