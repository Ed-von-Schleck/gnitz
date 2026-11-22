//! The tick schedule: the steps a run takes over the view-dependency graph.

use super::*;
use gnitz_wire::ReindexSlot;

/// One edge of a tick's schedule: `producer`'s output feeds `view`.
#[derive(Clone, Copy)]
pub(super) struct Step {
    pub(super) view: u64,
    pub(super) producer: u64,
    /// No later step reads `producer`'s own output: one sharing a relay round
    /// another step ran reads what the round gathered instead.
    pub(super) last: bool,
    /// The tick's one round of `producer`'s delta that `view`'s relay reads,
    /// where its scan of `producer` is relayed by a round.
    pub(super) relay: Option<RelaySlot>,
}

/// One relay round of a tick, shared by every step relaying one producer's delta
/// by one scatter key: the first of them runs it, and the rest read what it
/// gathered. Fixed by the schedule and the plans alone, so every worker shares
/// the same rounds, which start in slot order.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) struct RelaySlot {
    /// The round's index among the tick's relay rounds.
    pub(super) at: usize,
    /// No later step reads the round.
    pub(super) last: bool,
    /// Some step reading the round folds what it gathered, so it gathers by merge.
    pub(super) fold: bool,
}

/// `(scatter key, folds)` of the round view `view` relays producer `producer`'s
/// delta by: the key the round's plan is built from, and whether the view folds
/// what it gathers. `None` where no round relays it.
pub(super) type RelayKey<'k> = Option<(&'k [ReindexSlot], bool)>;

/// `edges`, each `(view, producer)`, as one schedule. Sorting puts a view after every
/// view it reads, because ids ascend along scan edges. `relay_key`: each edge's
/// relay round.
fn schedule<'k>(mut edges: Vec<(u64, u64)>, relay_key: impl Fn(u64, u64) -> RelayKey<'k>) -> Rc<[Step]> {
    edges.sort_unstable();
    // `(producer, key, folds)` of each round.
    let mut rounds: Vec<(u64, &[ReindexSlot], bool)> = Vec::new();
    let mut steps: Vec<Step> = edges
        .into_iter()
        .map(|(view, producer)| {
            // Whether the step reads the producer's own rows: it relays none, or
            // runs its round. Narrowed to the last such step below.
            let mut own = true;
            let relay = relay_key(view, producer).map(|(key, fold)| {
                let shared = rounds.iter().position(|&(p, k, _)| (p, k) == (producer, key));
                own = shared.is_none();
                let at = shared.unwrap_or(rounds.len());
                if own {
                    rounds.push((producer, key, false));
                }
                rounds[at].2 |= fold;
                RelaySlot { at, last: false, fold: false }
            });
            Step { view, producer, last: own, relay }
        })
        .collect();
    let mut read: FxHashSet<u64> = FxHashSet::default();
    let mut relayed: FxHashSet<usize> = FxHashSet::default();
    for step in steps.iter_mut().rev() {
        // Every producer still enters `read`: each round has a step that runs it.
        step.last = step.last && read.insert(step.producer);
        if let Some(slot) = &mut step.relay {
            slot.last = relayed.insert(slot.at);
            slot.fold = rounds[slot.at].2;
        }
    }
    steps.into()
}

impl DagEngine {
    /// One [`Step`] per dependency edge out of `source`'s forward closure, in
    /// execution order. Worker-identical, which keeps the workers in lockstep.
    /// A tick runs these every time, so they are derived once per shape of the
    /// graph.
    pub(super) fn tick_schedule(&mut self, source: u64) -> Rc<[Step]> {
        match self.tick_steps.get(&source) {
            Some(steps) => Rc::clone(steps),
            None => self.derive_tick_schedule(source),
        }
    }

    /// [`Self::tick_schedule`] on a miss: derived and kept.
    // Out of line: a tick reads the kept steps, and the derivation inlined into
    // it costs every tick.
    #[cold]
    #[inline(never)]
    fn derive_tick_schedule(&mut self, source: u64) -> Rc<[Step]> {
        let steps = self.schedule_from(&[source], Vec::new());
        self.tick_steps.insert(source, Rc::clone(&steps));
        steps
    }

    /// `edges`, each `(view, producer)`, and one per dependency edge out of each
    /// of `seeds` and out of every view their forward closure reaches, as one
    /// schedule, kept by no one. A tick's of `source` is `([source], [])`.
    pub(super) fn schedule_from(&self, seeds: &[u64], mut edges: Vec<(u64, u64)>) -> Rc<[Step]> {
        let mut reached: FxHashSet<u64> = seeds.iter().copied().collect();
        let mut stack = seeds.to_vec();
        while let Some(producer) = stack.pop() {
            for &view in self.dependents_of(producer) {
                edges.push((view, producer));
                if reached.insert(view) {
                    stack.push(view);
                }
            }
        }
        schedule(edges, |view, producer| {
            let code = &self.views.get(&view)?.code;
            let scan = code.scan(producer);
            Some((&*scan.relay.as_ref()?.key, code.vm.folds(scan.seed)))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/schedule.rs"]
mod tests;
