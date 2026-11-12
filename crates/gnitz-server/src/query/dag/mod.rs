//! DagEngine: every registered view's compiled plan, the operator state of the
//! views this process runs, and the decision of the boot verdict on which views
//! rebuild rather than resume. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency graph and the tick schedules in `meta`.

use std::borrow::Cow;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::compiler::{self, CompileOutput};
use crate::query::vm;
use gnitz_store::relation::{CircuitState, Cut, RelationRegistry, StateLayout};
use gnitz_zset::algebra::{self, Placement, ScatterPlan, Slot};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

mod exec;
mod hydrate;
mod meta;

pub(crate) use exec::{backfill, tick};

use meta::{DepMap, Step};

/// One view's rows of an exchange round: lent, or given to the round, which
/// frees them once they are sent.
pub(crate) struct Lane<'b> {
    pub(crate) view: u64,
    pub(crate) batch: Cow<'b, Batch>,
    pub(crate) plan: &'b ScatterPlan,
    /// The rows land where they fold: gathered by merge from consolidated
    /// senders, concatenated otherwise.
    pub(crate) fold: bool,
}

/// What a drive needs from the process running it: the engine it drives, and
/// the other workers.
pub(crate) trait DriveHost {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry);
    /// Trade `lanes` with every worker, which trades the same lanes in the same
    /// order, and push to `gathered`, per lane, this worker's share of every
    /// worker's batch under its plan. Answers whether every worker passed
    /// `drained`. The lanes share their waits.
    fn exchange<'b>(
        &mut self,
        lanes: impl ExactSizeIterator<Item = Lane<'b>>,
        drained: bool,
        gathered: &mut Vec<Batch>,
    ) -> bool;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

/// One registered view: its compiled plan, and the operator state the plan runs over.
struct RegisteredView {
    code: CompileOutput,
    /// Opened by the first epoch this process runs of the view.
    state: Option<CircuitState>,
    /// The user view whose chain holds this view: its owner, or itself.
    chain: u64,
    /// Every relation that is no view one or more `view → sources` edges from this
    /// view, sorted and each once: the relations whose ticks reach it. Fixed from
    /// registration to drop, since a source registers before and drops after it.
    roots: Box<[u64]>,
}

#[derive(Default)]
pub(crate) struct DagEngine {
    dep: DepMap,
    /// Every registered view, from its registration to its drop.
    views: FxHashMap<u64, RegisteredView>,
    /// The running tick's state; between ticks empty, kept for its capacity.
    tick: exec::TickRun,
}

impl DagEngine {
    // ── Registration ────────────────────────────────────────────────────

    /// Compile `circuit` as `view_id`'s plan and link the view to its sources, both
    /// kept until [`Self::forget`], and answer the placement its store registers
    /// under. `chain`: the user view whose chain holds it — itself for a user view.
    pub(crate) fn register_view(
        &mut self,
        registry: &RelationRegistry,
        view_id: u64,
        view: &SchemaDescriptor,
        bounded: bool,
        chain: u64,
        circuit: &gnitz_wire::Circuit,
    ) -> Result<Placement, String> {
        // Tick scheduling and backfill take ascending id order as dependency order.
        if let Some(src) = circuit.sources().find(|&s| s >= view_id) {
            return Err(format!("scans relation {src}, which is not older than it"));
        }
        let (code, placement) = compiler::compile_view(circuit, registry, view_id, view, bounded)?;
        // A source not registered here is a root only if it is no view.
        debug_assert!(code
            .source_ids()
            .all(|s| self.views.contains_key(&s) || !registry.relation(s).is_some_and(|r| r.kind().is_view())));
        let mut roots = Vec::new();
        for source in code.source_ids() {
            match self.views.get(&source) {
                Some(view) => roots.extend_from_slice(&view.roots),
                None => roots.push(source),
            }
        }
        roots.sort_unstable();
        roots.dedup();
        self.dep.link(view_id, code.source_ids());
        let roots = roots.into_boxed_slice();
        self.views
            .insert(view_id, RegisteredView { code, state: None, chain, roots });
        Ok(placement)
    }

    /// View `view_id`'s registration.
    fn registered(&mut self, view_id: u64) -> Result<&mut RegisteredView, String> {
        self.views
            .get_mut(&view_id)
            .ok_or_else(|| format!("view {view_id} is not registered"))
    }

    /// Whether view `id` is a chain segment whose store no reduce of its own
    /// reads back.
    fn passes_through(&self, id: u64) -> bool {
        self.views
            .get(&id)
            .is_some_and(|v| v.chain != id && !v.code.vm.reads_view_store())
    }

    /// The operator state the compiled circuit of view `id` opens; `None` for an
    /// id that is no view here.
    pub(crate) fn layout_of(&self, id: u64) -> Option<&StateLayout> {
        self.views.get(&id).map(|v| v.code.vm.layout())
    }

    /// The user view whose chain holds view `id`: its owner, or `id` itself.
    pub(crate) fn chain_of(&self, id: u64) -> u64 {
        self.views.get(&id).map_or(id, |v| v.chain)
    }

    /// Drop everything this layer holds for relation `id`.
    pub(crate) fn forget(&mut self, id: u64) {
        if let Some(view) = self.views.remove(&id) {
            self.dep.unlink(id, view.code.source_ids());
        }
    }

    // ── The boot verdict ────────────────────────────────────────────────

    /// Decide which views this boot rebuilds rather than resumes — their
    /// checkpointed state, output stores and operator traces alike. A view is
    /// **valid** (resumed) iff:
    ///   * its output child and every trace child its compiled circuit declares
    ///     are stamped with `g`, the committed checkpoint generation, which
    ///     [`RelationRegistry::view_children_resumable`] answers. A child is named
    ///     by its worker's slot and stamped under the operator-state format, so
    ///     one written at another worker count or format is none of these. The
    ///     durable generation is past `g` before any store opens when the boot
    ///     replays a push; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store; and
    ///   * every view of its chain is valid — a segment's rows are dropped once
    ///     its chain is built, so a chain member's rebuild needs them rebuilt; and
    ///   * none of its sources is a STREAM or a system family — its state
    ///     integrates stream rows that are gone, or catalog rows no boot replays
    ///     into it.
    ///
    /// Each view's **local** validity is decided first (roots + child
    /// manifests), then [`Self::rebuilt_with`] propagates invalidity to any view
    /// scanning an invalid source and to the whole chain of any invalid view.
    pub(crate) fn decide_rebuild(&self, registry: &RelationRegistry, g: u64) -> FxHashSet<u64> {
        // A view over a stream or a system family needs no manifest read. An
        // unreadable manifest reads as a mismatch, the verdict a child whose
        // manifest a previous open erased must get: its siblings may still be at `g`.
        let mut invalid = self.over_unreplayed_sources(registry);
        let stale: Vec<u64> = self
            .views
            .iter()
            .filter(|(id, v)| !invalid.contains(*id) && !registry.view_children_resumable(**id, g, v.code.vm.layout()))
            .map(|(&id, _)| id)
            .collect();
        invalid.extend(stale);
        self.rebuilt_with(invalid)
    }

    /// The views reached by a relation whose deltas no boot replays into a kept
    /// view. Boot replays a base table's tail alone: a stream's rows are gone, and
    /// a system family's tail is applied before any view exists.
    fn over_unreplayed_sources(&self, registry: &RelationRegistry) -> FxHashSet<u64> {
        let unreplayed = |root: &u64| registry.relation(*root).is_some_and(|r| !r.kind().is_base_table());
        self.views
            .iter()
            .filter(|(_, v)| v.roots.iter().any(unreplayed))
            .map(|(&id, _)| id)
            .collect()
    }

    /// Every view a rebuild of `invalid` takes with it: the least set holding
    /// `invalid`, every view that scans a member, and every view of a chain
    /// holding one.
    fn rebuilt_with(&self, invalid: FxHashSet<u64>) -> FxHashSet<u64> {
        let mut rebuilt = FxHashSet::default();
        let mut chains = FxHashSet::default();
        let mut stack: Vec<u64> = invalid.into_iter().collect();
        while let Some(id) = stack.pop() {
            if !rebuilt.insert(id) {
                continue;
            }
            stack.extend_from_slice(self.dependents_of(id));
            let chain = self.chain_of(id);
            if chains.insert(chain) {
                stack.extend(self.views.iter().filter(|(_, v)| v.chain == chain).map(|(&m, _)| m));
            }
        }
        rebuilt
    }

    // ── Operator state ──────────────────────────────────────────────────

    /// The checkpoint round at `generation`: persist the operator traces, output
    /// store and indexes of every view a boot can resume, and every table's
    /// indexes. The views every boot rebuilds whatever their stores hold — those a
    /// stream or a system family reaches, and their chains — are a subset of what
    /// the boot verdict rejects, so the round publishes none of them.
    ///
    /// The round that stamps a view's output store stamps its traces with it, run
    /// or not, so the next boot finds them at one generation. A view whose state
    /// this process never opened has traces untouched since they were published at
    /// the mark its output store resumed from; when that mark is `generation` they
    /// are already stamped, so the round leaves them closed, and otherwise opens
    /// them to restamp them.
    pub(crate) fn checkpoint_ephemeral(
        &mut self,
        registry: &mut RelationRegistry,
        generation: u64,
    ) -> Result<(), String> {
        let never = self.rebuilt_with(self.over_unreplayed_sources(registry));
        let resumes = |id: u64| !never.contains(&id);
        let stamped = |id: u64| {
            registry
                .relation(id)
                .is_some_and(|r| r.loaded_mark() == Some(generation))
        };
        let states = self
            .views
            .iter_mut()
            .filter(|(&id, v)| resumes(id) && (v.state.is_some() || !stamped(id)))
            .map(|(&id, v)| open_state(&mut v.state, registry, id, v.code.vm.layout()))
            .collect::<Result<Vec<_>, String>>()?;
        registry.checkpoint_ephemeral(states, generation, resumes)
    }

    /// Whether this process has opened view `view_id`'s operator state.
    #[cfg(test)]
    pub(crate) fn state_opened(&self, view_id: u64) -> bool {
        self.views.get(&view_id).is_some_and(|v| v.state.is_some())
    }
}

/// `state`, the operator state of view `view_id`, opened on a miss.
fn open_state<'a>(
    state: &'a mut Option<CircuitState>,
    registry: &RelationRegistry,
    view_id: u64,
    layout: &StateLayout,
) -> Result<&'a mut CircuitState, String> {
    if state.is_none() {
        let open = CircuitState::open(registry, view_id, layout);
        *state = Some(open.map_err(|e| format!("view_id={view_id}: open operator state: {e}"))?);
    }
    Ok(state.as_mut().expect("opened above"))
}

impl RegisteredView {
    /// The plan of this view, `view_id`, beside the stores a run of it reads: its
    /// operator state, opened on a miss, and `unfed`, the sources it has been fed
    /// no row of.
    fn opened<'a>(
        &'a mut self,
        registry: &'a RelationRegistry,
        view_id: u64,
        unfed: &'a [u64],
    ) -> Result<(&'a mut CompileOutput, vm::Stores<'a>), String> {
        let own = open_state(&mut self.state, registry, view_id, self.code.vm.layout())?;
        Ok((&mut self.code, vm::Stores { own, registry, unfed }))
    }
}
