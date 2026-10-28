//! DagEngine: every registered view's compiled plan, and the operator state of
//! the views this process runs. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency map in `meta`.

use std::borrow::Cow;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::compiler::{self, CompileOutput};
use crate::query::vm;
use gnitz_store::relation::{CircuitState, Cut, RelationRegistry, StateLayout};
use gnitz_zset::algebra::Placement;
use gnitz_zset::algebra::{self, ScatterPlan, Slot};
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
}

#[derive(Default)]
pub(crate) struct DagEngine {
    dep: DepMap,
    /// Every registered view, from its registration to its drop.
    views: FxHashMap<u64, RegisteredView>,
    /// Views the boot verdict rejected, each until its rebuild backfill starts.
    rebuild: FxHashSet<u64>,
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
        self.dep.link(view_id, circuit.sources());
        self.views.insert(view_id, RegisteredView { code, state: None, chain });
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
        self.dep.unlink(id);
        self.views.remove(&id);
        self.rebuild.remove(&id);
    }

    // ── The boot rebuild set ────────────────────────────────────────────

    /// Whether `id` awaits its rebuild backfill.
    pub(crate) fn awaits_rebuild(&self, id: u64) -> bool {
        self.rebuild.contains(&id)
    }

    /// Replace the set of views awaiting a rebuild backfill.
    pub(crate) fn set_rebuild(&mut self, ids: impl IntoIterator<Item = u64>) {
        self.rebuild = ids.into_iter().collect();
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
