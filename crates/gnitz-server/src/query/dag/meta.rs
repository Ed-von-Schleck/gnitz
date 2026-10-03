//! The view-dependency graph and the closures over it.

use super::*;
use std::collections::hash_map::Entry;

/// One edge of a tick's schedule: `producer`'s output feeds `view`. Field order
/// is the sort order, and sorting puts a view after every view it reads, because
/// ids ascend along scan edges.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) struct Step {
    pub(super) view: u64,
    pub(super) producer: u64,
}

/// The bidirectional view-dependency index, kept current by every view registration
/// and drop. A `forward` / `reverse` entry exists only while it holds an id.
#[derive(Default)]
pub(super) struct DepMap {
    /// source_table_id → [view_ids]
    forward: FxHashMap<u64, Vec<u64>>,
    /// view_id → [source_table_ids]
    reverse: FxHashMap<u64, Vec<u64>>,
    /// source id → [`Self::tick_steps`] of it, each kept until an edge moves.
    tick_steps: FxHashMap<u64, Rc<[Step]>>,
}

impl DepMap {
    /// Link `view` to each relation its circuit scans. A source may repeat.
    pub(super) fn link(&mut self, view: u64, sources: impl Iterator<Item = u64>) {
        self.tick_steps.clear();
        for source in sources {
            let srcs = self.reverse.entry(view).or_default();
            if !srcs.contains(&source) {
                srcs.push(source);
                self.forward.entry(source).or_default().push(view);
            }
        }
    }

    /// Forget `view`'s edges.
    pub(super) fn unlink(&mut self, view: u64) {
        self.tick_steps.clear();
        for source in self.reverse.remove(&view).into_iter().flatten() {
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
    pub(super) fn tick_steps(&mut self, source: u64) -> Rc<[Step]> {
        if let Some(steps) = self.tick_steps.get(&source) {
            return Rc::clone(steps);
        }
        let producers = std::iter::once(source).chain(Self::closure(&self.forward, vec![source]));
        let mut steps: Vec<Step> = producers
            .flat_map(|producer| {
                let views = self.forward.get(&producer).into_iter().flatten();
                views.map(move |&view| Step { view, producer })
            })
            .collect();
        steps.sort_unstable();
        let steps: Rc<[Step]> = steps.into();
        self.tick_steps.insert(source, Rc::clone(&steps));
        steps
    }

    /// Every id one or more edges from some seed over one half of the map — a
    /// seed only when another seed reaches it.
    fn closure(edges: &FxHashMap<u64, Vec<u64>>, seeds: Vec<u64>) -> FxHashSet<u64> {
        let mut reachable: FxHashSet<u64> = FxHashSet::default();
        let mut stack = seeds;
        while let Some(id) = stack.pop() {
            for &next in edges.get(&id).into_iter().flatten() {
                if reachable.insert(next) {
                    stack.push(next);
                }
            }
        }
        reachable
    }
}

impl DagEngine {
    // ── Dependency map ──────────────────────────────────────────────────

    /// The views that scan `id` directly. Empty when none do.
    pub(crate) fn dependents_of(&self, id: u64) -> &[u64] {
        self.dep.forward.get(&id).map_or(&[], Vec::as_slice)
    }

    /// Whether any view scans `id`.
    pub(crate) fn is_scanned(&self, id: u64) -> bool {
        !self.dependents_of(id).is_empty()
    }

    /// The relations `view_id` scans directly. Empty when it scans none.
    pub(crate) fn sources_of(&self, view_id: u64) -> &[u64] {
        self.dep.reverse.get(&view_id).map_or(&[], Vec::as_slice)
    }

    /// Every relation one or more `view → sources` edges from some seed —
    /// through view sources, down to the bases.
    pub(crate) fn source_closure(&self, seeds: Vec<u64>) -> FxHashSet<u64> {
        DepMap::closure(&self.dep.reverse, seeds)
    }

    /// The other direction over the same edges: every view one or more
    /// `source → dependents` edges from some seed — which views a tick of these
    /// sources reaches.
    pub(crate) fn dependent_closure(&self, seeds: Vec<u64>) -> FxHashSet<u64> {
        DepMap::closure(&self.dep.forward, seeds)
    }

    /// The base tables — not streams — that `seeds`' source chains reach through
    /// view sources, sorted.
    pub(crate) fn base_tables_reachable_from(&self, registry: &RelationRegistry, seeds: Vec<u64>) -> Vec<u64> {
        base_tables_among(registry, self.source_closure(seeds))
    }

    /// The base tables the circuits of `circuits` — a `CIRCUIT_TAB` delta not applied
    /// yet — reach through view sources, sorted.
    pub(crate) fn base_tables_scanned_by(
        &self,
        registry: &RelationRegistry,
        circuits: &Batch,
    ) -> Result<Vec<u64>, String> {
        let mut reached = FxHashSet::default();
        for i in circuits.live_rows() {
            let cell = gnitz_expr::payload_bytes(circuits, i, gnitz_wire::CIRCTAB_PAY_CIRCUIT);
            let circuit =
                gnitz_wire::Circuit::decode(cell).map_err(|e| format!("view {}: {e}", circuits.get_pk(i) as u64))?;
            reached.extend(circuit.sources());
        }
        let through_views = self.source_closure(reached.iter().copied().collect());
        reached.extend(through_views);
        Ok(base_tables_among(registry, reached))
    }
}

/// The base tables — not streams — among `ids`, sorted.
fn base_tables_among(registry: &RelationRegistry, ids: FxHashSet<u64>) -> Vec<u64> {
    let mut bases: Vec<u64> = ids
        .into_iter()
        .filter(|&s| {
            registry
                .relation(s)
                .map(Relation::kind)
                .is_some_and(|k| k.is_base_table())
        })
        .collect();
    bases.sort_unstable();
    bases
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
