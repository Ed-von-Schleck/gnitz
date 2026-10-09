//! DagEngine: every registered view's compiled plan, and the operator state of
//! the views this process runs. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency map in `meta`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::borrow::Cow;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::compiler::{self, CompileOutput};
use crate::query::vm;
use gnitz_store::relation::{CircuitState, Cut, RelationRegistry};
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
    /// under. `owner`: the user view whose chain it is a segment of, `None` for a
    /// user view.
    pub(crate) fn register_view(
        &mut self,
        registry: &RelationRegistry,
        view_id: u64,
        view: &SchemaDescriptor,
        bounded: bool,
        owner: Option<u64>,
        circuit: &gnitz_wire::Circuit,
    ) -> Result<Placement, String> {
        // Tick scheduling and backfill take ascending id order as dependency order.
        if let Some(src) = circuit.sources().find(|&s| s >= view_id) {
            return Err(format!("scans relation {src}, which is not older than it"));
        }
        let (code, placement) = compiler::compile_view(circuit, registry, view_id, view, bounded)?;
        self.dep.link(view_id, circuit.sources());
        let chain = owner.unwrap_or(view_id);
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
    pub(crate) fn set_rebuild(&mut self, ids: FxHashSet<u64>) {
        self.rebuild = ids;
    }

    /// Every view awaiting a rebuild backfill, removed.
    pub(crate) fn take_rebuild(&mut self) -> FxHashSet<u64> {
        std::mem::take(&mut self.rebuild)
    }

    // ── Operator state ──────────────────────────────────────────────────

    /// Every opened view's operator state, by view id.
    pub(crate) fn ephemeral_states(&mut self) -> impl Iterator<Item = (u64, &mut CircuitState)> {
        self.views
            .iter_mut()
            .filter_map(|(&id, v)| Some((id, v.state.as_mut()?)))
    }
}

fn unregistered(view_id: u64) -> String {
    format!("view {view_id} is not registered")
}

/// `view_id`'s plan and its operator state, opened on a miss.
fn opened<'a>(
    views: &'a mut FxHashMap<u64, RegisteredView>,
    registry: &RelationRegistry,
    view_id: u64,
) -> Result<(&'a mut CompileOutput, &'a mut CircuitState), String> {
    let RegisteredView { code, state, .. } = views.get_mut(&view_id).ok_or_else(|| unregistered(view_id))?;
    if state.is_none() {
        let open = CircuitState::open(registry, view_id, &code.layout);
        *state = Some(open.map_err(|e| format!("view_id={view_id}: open operator state: {e}"))?);
    }
    Ok((code, state.as_mut().expect("opened above")))
}

#[cfg(test)]
#[path = "tests/dag.rs"]
mod tests;
