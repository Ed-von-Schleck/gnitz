//! DagEngine: every registered view's routing metadata and the compiled plans
//! of the views this process runs. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency map in `meta`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::borrow::Cow;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::compiler::{self, CompileOutput, Relay, SubPlan, ViewMeta};
use crate::query::vm;
use gnitz_store::relation::{CircuitState, Relation, RelationRegistry, StateLayout};
use gnitz_zset::algebra::{self, ScatterPlan};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{Placement, SchemaDescriptor};

mod exec;
mod hydrate;
mod meta;

pub(crate) use exec::{drive, Drive};

use meta::DepMap;

/// What a drive needs from the process running it: the engine it drives, and
/// the other workers.
pub(crate) trait DriveHost {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry);
    /// This worker's share of every worker's `batch` for `view_id` under
    /// `plan`. `fold`: the share lands in a register that folds it, so gathering
    /// consolidated slices merges them, saving that fold its sort; otherwise it
    /// concatenates.
    fn exchange(&mut self, view_id: u64, batch: Cow<'_, Batch>, plan: &ScatterPlan, fold: bool) -> Batch;
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
    views: FxHashMap<u64, RegisteredView>,
    /// Views the boot verdict rejected, each until its rebuild backfill's first
    /// drive.
    rebuild: FxHashSet<u64>,
}

impl DagEngine {
    // ── Registration ────────────────────────────────────────────────────

    /// Derive `view_id`'s routing metadata and link it to its sources, both kept until
    /// [`Self::forget`], and answer the placement its store registers under.
    pub(crate) fn register_view(
        &mut self,
        registry: &RelationRegistry,
        view_id: u64,
        view: &SchemaDescriptor,
    ) -> Result<Placement, String> {
        let loaded = compiler::load_circuit(registry, view_id)?;
        // Tick scheduling and backfill take ascending id order as dependency order.
        if let Some(src) = loaded.sources().find(|&s| s >= view_id) {
            return Err(format!("scans relation {src}, which is not older than it"));
        }
        let (meta, placement) = ViewMeta::derive(&loaded, registry, view)?;
        self.dep.link(view_id, loaded.sources());
        self.views.insert(view_id, RegisteredView { meta, plan: None });
        Ok(placement)
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

    // ── Compilation ─────────────────────────────────────────────────────

    /// The routing metadata derived when `view_id` registered. `Err` for an id
    /// that is not a registered view.
    pub(crate) fn view_meta(&self, view_id: u64) -> Result<&ViewMeta, String> {
        self.views
            .get(&view_id)
            .map(|v| &v.meta)
            .ok_or_else(|| unregistered(view_id))
    }

    /// `view_id`'s plan, if it is registered and compiled.
    fn plan_mut(&mut self, view_id: u64) -> Option<&mut ViewPlan> {
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

fn unregistered(view_id: u64) -> String {
    format!("view {view_id} is not registered")
}

/// Load and compile `view`'s circuit. Opens nothing.
fn compile(registry: &RelationRegistry, view: &Relation) -> Result<(CompileOutput, StateLayout), String> {
    let loaded = compiler::load_circuit(registry, view.id())?;
    compiler::compile_view(
        &loaded,
        registry,
        &view.schema(),
        view.placement(),
        view.kind().is_bounded(),
    )
}

/// This view's metadata and its compiled plan, compiling and opening its
/// operator state on a miss.
fn ensure_compiled<'a>(
    views: &'a mut FxHashMap<u64, RegisteredView>,
    registry: &RelationRegistry,
    view_id: u64,
) -> Result<(&'a ViewMeta, &'a mut ViewPlan), String> {
    let RegisteredView { meta, plan } = views.get_mut(&view_id).ok_or_else(|| unregistered(view_id))?;
    if plan.is_none() {
        let view = registry.relation_or_err(view_id)?;
        let (code, layout) = compile(registry, view)
            .map_err(|e| format!("view_id={view_id} does not compile from its durable circuit: {e}"))?;
        let state = CircuitState::open(registry, view_id, layout)
            .map_err(|e| format!("view_id={view_id}: open operator state: {e}"))?;
        gnitz_debug!("dag: compiled view_id={}", view_id);
        *plan = Some(ViewPlan { code, state });
    }
    Ok((meta, plan.as_mut().expect("filled above")))
}

/// Whether registered view `view_id`'s circuit compiles. Keeps and opens
/// nothing, so the master can ask before a DDL is durable.
pub(crate) fn preflight_compile(registry: &RelationRegistry, view_id: u64) -> Result<(), String> {
    compile(registry, registry.relation_or_err(view_id)?).map(drop)
}

#[cfg(test)]
#[path = "tests/dag.rs"]
mod tests;
