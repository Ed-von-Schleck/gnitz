//! The view-dependency graph, the closures over it, and the tick schedule
//! derived from it.

use super::*;
use gnitz_wire::ReindexSlot;
use std::collections::hash_map::Entry;

/// One edge of a tick's schedule: `producer`'s output feeds `view`.
#[derive(Clone, Copy)]
pub(super) struct Step {
    pub(super) view: u64,
    pub(super) producer: u64,
    /// No later step reads `producer`'s own output: one sharing a relay round
    /// another step ran reads what the round gathered instead.
    pub(super) last: bool,
    /// A later step reads `view`'s output.
    pub(super) needed: bool,
    /// The tick's one round of `producer`'s delta that `view`'s relay reads,
    /// where its scan of `producer` is relayed by a round.
    pub(super) relay: Option<RelaySlot>,
}

/// One relay round of a tick, shared by every step relaying one producer's delta
/// by one scatter key: the first of them runs it, and the rest read what it
/// gathered. Fixed by the schedule and the plans alone, so every worker shares
/// the same rounds.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) struct RelaySlot {
    /// The round's index among the tick's relay rounds.
    pub(super) at: usize,
    /// This step runs the round: no earlier step reads it.
    pub(super) first: bool,
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
        .map(|(view, producer)| Step {
            view,
            producer,
            last: false,
            needed: false,
            relay: relay_key(view, producer).map(|(key, fold)| {
                let shared = rounds.iter().position(|&(p, k, _)| (p, k) == (producer, key));
                let at = shared.unwrap_or(rounds.len());
                if shared.is_none() {
                    rounds.push((producer, key, false));
                }
                rounds[at].2 |= fold;
                RelaySlot {
                    at,
                    first: shared.is_none(),
                    last: false,
                    fold: false,
                }
            }),
        })
        .collect();
    let mut read: FxHashSet<u64> = FxHashSet::default();
    let mut relayed: FxHashSet<usize> = FxHashSet::default();
    for step in steps.iter_mut().rev() {
        // Every producer still enters `read`: each round has a step that runs it.
        step.last = step.relay.is_none_or(|slot| slot.first) && read.insert(step.producer);
        if let Some(slot) = &mut step.relay {
            slot.last = relayed.insert(slot.at);
            slot.fold = rounds[slot.at].2;
        }
    }
    for step in steps.iter_mut() {
        step.needed = read.contains(&step.view);
    }
    steps.into()
}

/// The source → dependents half of the view-dependency graph, kept current by every
/// view registration and drop; each view's plan holds the other half. A `forward`
/// entry exists only while it holds an id.
#[derive(Default)]
pub(super) struct DepMap {
    /// source_table_id → view_ids
    forward: FxHashMap<u64, Vec<u64>>,
    /// source id → [`DagEngine::tick_schedule`] of it, each kept until an edge
    /// moves.
    tick_steps: FxHashMap<u64, Rc<[Step]>>,
}

impl DepMap {
    /// Link `view` to each of `sources`, the relations its plan scans, each once.
    pub(super) fn link(&mut self, view: u64, sources: impl IntoIterator<Item = u64>) {
        self.tick_steps.clear();
        for source in sources {
            self.forward.entry(source).or_default().push(view);
        }
    }

    /// Forget the edges from each of `sources` to `view`.
    pub(super) fn unlink(&mut self, view: u64, sources: impl IntoIterator<Item = u64>) {
        self.tick_steps.clear();
        for source in sources {
            if let Entry::Occupied(mut views) = self.forward.entry(source) {
                views.get_mut().retain(|&v| v != view);
                if views.get().is_empty() {
                    views.remove();
                }
            }
        }
    }

    /// The views that scan `id` directly. Empty when none do.
    fn forward_of(&self, id: u64) -> &[u64] {
        self.forward.get(&id).map_or(&[], Vec::as_slice)
    }

    /// Every view one or more `source → dependents` edges from some seed — a seed
    /// only when another seed reaches it: the views a tick of the seeds reaches.
    pub(super) fn downstream(&self, seeds: impl IntoIterator<Item = u64>) -> FxHashSet<u64> {
        let mut reached: FxHashSet<u64> = FxHashSet::default();
        let mut stack: Vec<u64> = seeds.into_iter().collect();
        while let Some(id) = stack.pop() {
            for &view in self.forward_of(id) {
                if reached.insert(view) {
                    stack.push(view);
                }
            }
        }
        reached
    }
}

impl DagEngine {
    /// One [`Step`] per dependency edge out of `source`'s forward closure, in
    /// execution order. Worker-identical, which keeps the workers in lockstep.
    /// A tick runs these every time, so they are derived once per shape of the
    /// graph.
    pub(super) fn tick_schedule(&mut self, source: u64) -> Rc<[Step]> {
        match self.dep.tick_steps.get(&source) {
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
        let (views, dep) = (&self.views, &mut self.dep);
        // A step's view scans its producer.
        let relay_key = |view: u64, producer: u64| {
            let code = &views.get(&view)?.code;
            let scan = code.scan(producer);
            Some((&*scan.relay.as_ref()?.key, code.vm.folds(scan.seed)))
        };
        let producers = std::iter::once(source).chain(dep.downstream([source]));
        let edges = producers
            .flat_map(|producer| dep.forward_of(producer).iter().map(move |&view| (view, producer)))
            .collect();
        let steps = schedule(edges, relay_key);
        dep.tick_steps.insert(source, Rc::clone(&steps));
        steps
    }

    // ── Dependency map ──────────────────────────────────────────────────

    /// The views that scan `id` directly. Empty when none do.
    pub(crate) fn dependents_of(&self, id: u64) -> &[u64] {
        self.dep.forward_of(id)
    }

    /// Whether any view scans `id`.
    pub(crate) fn is_scanned(&self, id: u64) -> bool {
        !self.dependents_of(id).is_empty()
    }

    /// The relations `view_id` scans directly, each once, in its plan's scan order.
    /// Empty when it is no registered view.
    pub(crate) fn sources_of(&self, view_id: u64) -> impl Iterator<Item = u64> {
        self.views.get(&view_id).into_iter().flat_map(|v| v.code.source_ids())
    }

    /// Every relation that is no view and reaches `view` through the relations it
    /// scans — the relations whose ticks reach it — sorted. Empty for an id that
    /// is no registered view.
    pub(crate) fn roots_of(&self, view: u64) -> &[u64] {
        self.views.get(&view).map_or(&[], |v| &v.roots)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
