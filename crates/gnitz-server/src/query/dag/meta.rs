//! The view-dependency graph and the closures over it.

use super::*;
use gnitz_wire::ReindexSlot;
use std::collections::hash_map::Entry;

/// One edge of a tick's schedule: `producer`'s output feeds `view`.
#[derive(Clone, Copy)]
pub(super) struct Step {
    pub(super) view: u64,
    pub(super) producer: u64,
    /// No later step reads `producer`'s output.
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
                let at = match rounds.iter().position(|&(p, k, _)| (p, k) == (producer, key)) {
                    Some(at) => at,
                    None => {
                        rounds.push((producer, key, false));
                        rounds.len() - 1
                    }
                };
                rounds[at].2 |= fold;
                RelaySlot { at, last: false, fold: false }
            }),
        })
        .collect();
    let mut read: FxHashSet<u64> = FxHashSet::default();
    let mut relayed: FxHashSet<usize> = FxHashSet::default();
    for step in steps.iter_mut().rev() {
        step.last = read.insert(step.producer);
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
    /// source id → [`Self::tick_steps`] of it, each kept until an edge moves.
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

    /// One [`Step`] per dependency edge out of `source`'s forward closure, in
    /// execution order. A tick runs these every time, so they are derived once
    /// per shape of the graph.
    fn tick_steps<'k>(&mut self, source: u64, relay_key: impl Fn(u64, u64) -> RelayKey<'k>) -> Rc<[Step]> {
        match self.tick_steps.get(&source) {
            Some(steps) => Rc::clone(steps),
            None => self.derive_tick_steps(source, relay_key),
        }
    }

    /// [`Self::tick_steps`] on a miss: derived and kept.
    // Out of line: a tick reads the kept steps, and the derivation inlined into
    // it costs every tick.
    #[cold]
    #[inline(never)]
    fn derive_tick_steps<'k>(&mut self, source: u64, relay_key: impl Fn(u64, u64) -> RelayKey<'k>) -> Rc<[Step]> {
        let producers = std::iter::once(source).chain(closure(|id| self.forward_of(id).iter().copied(), [source]));
        let edges = producers
            .flat_map(|producer| self.forward_of(producer).iter().map(move |&view| (view, producer)))
            .collect();
        let steps = schedule(edges, relay_key);
        self.tick_steps.insert(source, Rc::clone(&steps));
        steps
    }

    /// The views that scan `id` directly. Empty when none do.
    fn forward_of(&self, id: u64) -> &[u64] {
        self.forward.get(&id).map_or(&[], Vec::as_slice)
    }
}

/// Every id one or more `next` edges from some seed — a seed only when another
/// seed reaches it. Seeds with no edge allocate nothing.
fn closure<I: IntoIterator<Item = u64>>(
    next: impl Fn(u64) -> I,
    seeds: impl IntoIterator<Item = u64>,
) -> FxHashSet<u64> {
    let mut reachable: FxHashSet<u64> = FxHashSet::default();
    let mut stack: Vec<u64> = seeds
        .into_iter()
        .filter(|&id| next(id).into_iter().next().is_some())
        .collect();
    while let Some(id) = stack.pop() {
        for n in next(id) {
            if reachable.insert(n) {
                stack.push(n);
            }
        }
    }
    reachable
}

impl DagEngine {
    /// One [`Step`] per dependency edge out of `source_id`'s forward closure, in
    /// execution order, skipping non-resumable views: their backfill fills them.
    /// The skipped set is closed under dependents (`rebuilt_with` closes it,
    /// `backfill` keeps it so), so no step reads a skipped producer.
    /// Worker-identical, which keeps the workers in lockstep.
    pub(super) fn tick_schedule(&mut self, source_id: u64) -> Rc<[Step]> {
        let views = &self.views;
        // A step's view scans its producer.
        let relay_key = |view: u64, producer: u64| {
            let code = &views.get(&view)?.code;
            let scan = code.scan(producer);
            Some((&*scan.relay.as_ref()?.key, code.vm.folds(scan.seed)))
        };
        let steps = self.dep.tick_steps(source_id, relay_key);
        match self.rebuild.is_empty() {
            true => steps,
            false => schedule(
                steps
                    .iter()
                    .filter(|s| !self.awaits_rebuild(s.view))
                    .map(|s| (s.view, s.producer))
                    .collect(),
                relay_key,
            ),
        }
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
    pub(crate) fn sources_of(&self, view_id: u64) -> impl Iterator<Item = u64> + '_ {
        self.views.get(&view_id).into_iter().flat_map(|v| v.code.source_ids())
    }

    /// Every relation one or more `view → sources` edges from `view` — through
    /// view sources, down to the bases — sorted, without `view` itself. Empty for
    /// an id that is no registered view.
    pub(crate) fn source_closure_of(&self, view: u64) -> &[u64] {
        self.views.get(&view).map_or(&[], |v| &v.sources)
    }

    /// `id`, then [`Self::source_closure_of`] it.
    pub(crate) fn self_and_sources(&self, id: u64) -> impl Iterator<Item = u64> + '_ {
        std::iter::once(id).chain(self.source_closure_of(id).iter().copied())
    }

    /// The other direction over the same edges: every view one or more
    /// `source → dependents` edges from some seed — which views a tick of these
    /// sources reaches.
    pub(crate) fn dependent_closure(&self, seeds: impl IntoIterator<Item = u64>) -> FxHashSet<u64> {
        closure(|id| self.dependents_of(id).iter().copied(), seeds)
    }

    /// Whether a tick of `id` drives a view: one scans it that does not await its
    /// rebuild.
    pub(crate) fn is_ticked(&self, id: u64) -> bool {
        self.dependents_of(id).iter().any(|v| !self.rebuild.contains(v))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
