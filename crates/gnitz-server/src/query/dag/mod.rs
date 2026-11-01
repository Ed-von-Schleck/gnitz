//! DagEngine: every registered view's compiled plan, the operator state of the
//! views this process runs, and the boot verdict on which views rebuild rather
//! than resume. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency map in `meta`.

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

/// What a drive needs from the process running it: the engine it drives, and
/// the other workers.
pub(crate) trait DriveHost {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry);
    /// This worker's share of every worker's `batch` for `view_id` under `plan`,
    /// merged from consolidated slices where `fold` and concatenated otherwise,
    /// and whether every worker passed `drained`.
    fn exchange(
        &mut self,
        view_id: u64,
        batch: Cow<'_, Batch>,
        plan: &ScatterPlan,
        fold: bool,
        drained: bool,
    ) -> (Batch, bool);
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
    /// Every relation one or more `view → sources` edges from this view, sorted and
    /// each once: source views and bases, never the view itself. Fixed from
    /// registration to drop, since a source registers before and drops after it.
    sources: Box<[u64]>,
}

#[derive(Default)]
pub(crate) struct DagEngine {
    dep: DepMap,
    /// Every registered view, from its registration to its drop.
    views: FxHashMap<u64, RegisteredView>,
    /// Views the boot verdict ([`Self::decide_rebuild`]) rejected, closed under
    /// dependents and chains, each until its rebuild backfill starts.
    rebuild: FxHashSet<u64>,
    /// The batch each producer of the running tick has emitted, keyed by
    /// producer; empty between ticks, kept for its capacity.
    tick_outputs: FxHashMap<u64, Batch>,
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
        // The closure below is exact only if every source view is registered here.
        debug_assert!(code
            .source_ids()
            .all(|s| self.views.contains_key(&s) || !registry.relation(s).is_some_and(|r| r.kind().is_view())));
        let mut sources: Vec<u64> = code.source_ids().flat_map(|s| self.self_and_sources(s)).collect();
        sources.sort_unstable();
        sources.dedup();
        self.dep.link(view_id, code.source_ids());
        let sources = sources.into_boxed_slice();
        self.views
            .insert(view_id, RegisteredView { code, state: None, chain, sources });
        Ok(placement)
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
        self.rebuild.remove(&id);
    }

    // ── The boot rebuild set ────────────────────────────────────────────

    /// Whether `id` awaits its rebuild backfill.
    pub(crate) fn awaits_rebuild(&self, id: u64) -> bool {
        self.rebuild.contains(&id)
    }

    /// Decide which views this boot rebuilds rather than resumes — their
    /// checkpointed state, output stores and operator traces alike. A view is
    /// **valid** (resumed) iff:
    ///   * `resume_at` is `Some`. It is `None` when the recorded topology (worker
    ///     count, STATE_FORMAT) is not the launched one: every view rebuilds and
    ///     no manifest is read;
    ///   * its output child and every trace child its compiled circuit declares
    ///     are stamped with `resume_at`'s G, the committed checkpoint generation,
    ///     which [`RelationRegistry::view_children_resumable`] answers. The
    ///     durable generation is `G+1` before any store opens when the boot
    ///     replays a push; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store; and
    ///   * every view of its chain is valid — a segment's rows are dropped once
    ///     its chain is built, so a chain member's rebuild needs them rebuilt; and
    ///   * none of its sources is a STREAM or a system family — its state
    ///     integrates stream rows that are gone, or catalog rows no boot replays
    ///     into it.
    ///
    /// Each view's **local** validity is decided first (direct sources + child
    /// manifests), then [`Self::rebuilt_with`] propagates invalidity to any view
    /// scanning an invalid source and to the whole chain of any invalid view.
    pub(crate) fn decide_rebuild(&mut self, registry: &RelationRegistry, resume_at: Option<u64>) {
        let Some(g) = resume_at else {
            self.rebuild = self.views.keys().copied().collect();
            return;
        };
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
        self.rebuild = self.rebuilt_with(invalid);
    }

    /// The views every boot rebuilds whatever their stores hold: those a stream or
    /// a system family reaches, and their chains — a subset of what the boot
    /// verdict rejects, so a checkpoint round has no reason to publish one.
    pub(crate) fn never_resumed(&self, registry: &RelationRegistry) -> FxHashSet<u64> {
        self.rebuilt_with(self.over_unreplayed_sources(registry))
    }

    /// The views with a direct source whose deltas no boot replays into a kept view.
    /// Boot replays a base table's tail alone: a stream's rows are gone, and a system
    /// family's tail is applied before any view exists.
    fn over_unreplayed_sources(&self, registry: &RelationRegistry) -> FxHashSet<u64> {
        let unreplayed = |s: &u64| {
            registry.relation(*s).is_some_and(|r| {
                let k = r.kind();
                !(k.is_view() || k.is_base_table())
            })
        };
        self.views
            .keys()
            .copied()
            .filter(|&vid| self.sources_of(vid).any(|s| unreplayed(&s)))
            .collect()
    }

    /// Every view a rebuild of `invalid` takes with it: each view downstream of one,
    /// then every view of a chain holding one. `tick_schedule` relies on this closure.
    fn rebuilt_with(&self, mut invalid: FxHashSet<u64>) -> FxHashSet<u64> {
        let downstream = self.dependent_closure(invalid.iter().copied());
        invalid.extend(downstream);
        let chains: FxHashSet<u64> = invalid.iter().map(|&v| self.chain_of(v)).collect();
        self.views
            .iter()
            .filter(|(_, v)| chains.contains(&v.chain))
            .map(|(&id, _)| id)
            .collect()
    }

    /// Every view awaiting a rebuild backfill, removed.
    pub(crate) fn take_rebuild(&mut self) -> FxHashSet<u64> {
        std::mem::take(&mut self.rebuild)
    }

    // ── Operator state ──────────────────────────────────────────────────

    /// The operator state of every view `checkpointed` picks, each
    /// opened on a miss: the round that stamps a view's output store stamps its
    /// traces with it, run or not, so the next boot finds them at one generation.
    pub(crate) fn ephemeral_states<'a>(
        &'a mut self,
        registry: &RelationRegistry,
        checkpointed: impl Fn(u64) -> bool,
    ) -> Result<Vec<&'a mut CircuitState>, String> {
        let views = self.views.iter_mut().filter(|(&id, _)| checkpointed(id));
        views
            .map(|(&id, v)| open_state(&mut v.state, registry, id, v.code.vm.layout()))
            .collect()
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

/// `view_id`'s plan, beside the stores a run of it reads: its operator state,
/// opened on a miss, and `unfed`, the sources it has been fed no row of.
fn opened<'a>(
    views: &'a mut FxHashMap<u64, RegisteredView>,
    registry: &'a RelationRegistry,
    view_id: u64,
    unfed: &'a [u64],
) -> Result<(&'a mut CompileOutput, vm::Stores<'a>), String> {
    let RegisteredView { code, state, .. } = views
        .get_mut(&view_id)
        .ok_or_else(|| format!("view {view_id} is not registered"))?;
    let own = open_state(state, registry, view_id, code.vm.layout())?;
    Ok((code, vm::Stores { own, registry, unfed }))
}
