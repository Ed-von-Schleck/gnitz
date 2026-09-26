//! DagEngine: every registered view's routing metadata and the compiled plans
//! of the views this process runs. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency map in `meta`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::compiler::{self, CompileOutput, SubPlan, ViewMeta};
use crate::query::vm;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, Relation, RelationRegistry, StateLayout};
use gnitz_store::schema::Placement;
use gnitz_store::storage::Batch;

mod exec;
mod hydrate;
mod meta;

pub(crate) use exec::{drive, Drive};

use meta::DepMap;

/// What a drive needs from the process running it: the engine it drives, and
/// the other workers.
pub(crate) trait DriveHost {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry);
    /// This worker's `batch` for `view_id`, repartitioned under `key`: the rows
    /// it owns.
    fn exchange(&mut self, view_id: i64, batch: Batch, key: i64) -> Batch;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

/// One view's compiled plan and the operator state its sub-plans share.
struct ViewPlan {
    code: CompileOutput,
    state: CircuitState,
}

/// One registered view: its routing metadata, and its plan once compiled.
struct RegisteredView {
    meta: ViewMeta,
    plan: Option<ViewPlan>,
}

#[derive(Default)]
pub(crate) struct DagEngine {
    dep: DepMap,
    /// Every registered view, from its registration to its drop. `plan` is `None`
    /// until its first compile.
    views: FxHashMap<i64, RegisteredView>,
    /// Each relation's effective ingests since its last tick: what its store holds
    /// beyond the state every view over it was last maintained at.
    unticked: FxHashMap<i64, Batch>,
}

impl DagEngine {
    // ── Registration ────────────────────────────────────────────────────

    /// Derive `view_id`'s routing metadata, keep it until [`Self::forget`], and
    /// answer the placement its store registers under.
    pub(crate) fn register_view(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        pk_arity: usize,
    ) -> Result<Placement, String> {
        let loaded = compiler::load_circuit(registry, view_id)?;
        let (meta, placement) = ViewMeta::derive(&loaded, registry, pk_arity)?;
        self.views.insert(view_id, RegisteredView { meta, plan: None });
        Ok(placement)
    }

    /// Drop everything this layer holds for relation `id`.
    pub(crate) fn forget(&mut self, id: i64) {
        self.views.remove(&id);
        self.unticked.remove(&id);
    }

    // ── Unticked deltas ─────────────────────────────────────────────────

    /// Append `delta`, one effective ingest into `tid`, to what its next tick
    /// drains.
    pub(crate) fn buffer_unticked(&mut self, tid: i64, delta: Batch) {
        match self.unticked.get_mut(&tid) {
            Some(existing) => existing.append_batch(&delta, 0, delta.len()),
            None => {
                self.unticked.insert(tid, delta);
            }
        }
    }

    /// Everything buffered for `tid` since its last tick, removed.
    pub(crate) fn take_unticked(&mut self, tid: i64) -> Option<Batch> {
        self.unticked.remove(&tid)
    }

    /// Apply one `CircuitNodes` delta to the dependency map.
    pub(crate) fn apply_circuit_delta(&mut self, batch: &Batch) {
        self.dep.apply(batch);
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// [`ensure_compiled`] for a caller that cannot name a compiled plan:
    /// compile `view_id` now and open its operator state.
    pub(crate) fn open_plan(&mut self, registry: &RelationRegistry, view_id: i64) -> Result<(), String> {
        ensure_compiled(&mut self.views, registry, view_id).map(drop)
    }

    /// The routing metadata derived when `view_id` registered. `Err` for an id
    /// that is not a registered view.
    pub(crate) fn view_meta(&self, view_id: i64) -> Result<&ViewMeta, String> {
        self.views
            .get(&view_id)
            .map(|v| &v.meta)
            .ok_or_else(|| unregistered(view_id))
    }

    /// `view_id`'s plan, if it is registered and compiled.
    fn plan_mut(&mut self, view_id: i64) -> Option<&mut ViewPlan> {
        self.views.get_mut(&view_id).and_then(|v| v.plan.as_mut())
    }

    /// Every compiled view's operator state.
    pub(crate) fn ephemeral_states(&mut self) -> impl Iterator<Item = &mut CircuitState> {
        self.views
            .values_mut()
            .filter_map(|v| v.plan.as_mut())
            .map(|p| &mut p.state)
    }
}

fn unregistered(view_id: i64) -> String {
    format!("view {view_id} is not registered")
}

/// Load and compile `view`'s circuit. Opens nothing.
fn compile(registry: &RelationRegistry, view: &Relation) -> Result<(CompileOutput, StateLayout), String> {
    let loaded = compiler::load_circuit(registry, view.id())?;
    compiler::compile_view(&loaded, registry, &view.schema(), view.is_bounded())
}

/// This view's metadata and its compiled plan, compiling and opening its
/// operator state on a miss.
fn ensure_compiled<'a>(
    views: &'a mut FxHashMap<i64, RegisteredView>,
    registry: &RelationRegistry,
    view_id: i64,
) -> Result<(&'a ViewMeta, &'a mut ViewPlan), String> {
    let RegisteredView { meta, plan } = views.get_mut(&view_id).ok_or_else(|| unregistered(view_id))?;
    if plan.is_none() {
        let view = registry.relation_or_err(view_id)?;
        let (code, layout) = compile(registry, view)
            .map_err(|e| format!("view_id={view_id} does not compile from its durable circuit: {e}"))?;
        let state = CircuitState::open(registry, view_id, layout).map_err(|e| {
            format!(
                "view_id={view_id}: its derived state is corrupt or unreadable, or resources are \
                 exhausted: {e}"
            )
        })?;
        gnitz_debug!("dag: compiled view_id={}", view_id);
        *plan = Some(ViewPlan { code, state });
    }
    Ok((meta, plan.as_mut().expect("filled above")))
}

/// Whether registered view `view_id`'s circuit compiles. Keeps and opens
/// nothing, so the master can ask before a DDL is durable.
pub(crate) fn preflight_compile(registry: &RelationRegistry, view_id: i64) -> Result<(), String> {
    compile(registry, registry.relation_or_err(view_id)?).map(drop)
}

#[cfg(test)]
#[path = "tests/dag.rs"]
mod tests;
