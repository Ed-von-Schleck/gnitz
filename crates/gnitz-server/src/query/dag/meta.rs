//! The view-dependency graph and the closures over it.

use super::*;
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
}

/// `edges`, each `(view, producer)`, as one schedule. Sorting puts a view after every
/// view it reads, because ids ascend along scan edges.
fn schedule(mut edges: Vec<(u64, u64)>) -> Rc<[Step]> {
    edges.sort_unstable();
    let mut steps: Vec<Step> = edges
        .into_iter()
        .map(|(view, producer)| Step {
            view,
            producer,
            last: false,
            needed: false,
        })
        .collect();
    let mut read: FxHashSet<u64> = FxHashSet::default();
    for step in steps.iter_mut().rev() {
        step.last = read.insert(step.producer);
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
    fn tick_steps(&mut self, source: u64) -> Rc<[Step]> {
        if let Some(steps) = self.tick_steps.get(&source) {
            return Rc::clone(steps);
        }
        let producers = std::iter::once(source).chain(closure(|id| self.forward_of(id).iter().copied(), [source]));
        let edges = producers
            .flat_map(|producer| self.forward_of(producer).iter().map(move |&view| (view, producer)))
            .collect();
        let steps = schedule(edges);
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
        let steps = self.dep.tick_steps(source_id);
        match self.rebuild.is_empty() {
            true => steps,
            false => schedule(
                steps
                    .iter()
                    .filter(|s| !self.awaits_rebuild(s.view))
                    .map(|s| (s.view, s.producer))
                    .collect(),
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

    /// Every relation one or more `view → sources` edges from some seed —
    /// through view sources, down to the bases.
    pub(crate) fn source_closure(&self, seeds: impl IntoIterator<Item = u64>) -> FxHashSet<u64> {
        closure(|id| self.sources_of(id), seeds)
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
