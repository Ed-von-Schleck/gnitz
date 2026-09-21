//! DagEngine: every registered view's routing metadata and the compiled plans
//! of the views this process runs. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, the dependency map and the placement fold in `meta`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::collections::hash_map::Entry;

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
pub(super) struct ViewPlan {
    pub(super) code: CompileOutput,
    pub(super) state: CircuitState,
}

pub(crate) struct DagEngine {
    dep: DepMap,
    /// Every registered view's routing metadata, from its registration to its drop.
    metas: FxHashMap<i64, ViewMeta>,
    /// A view's compiled plan and the operator state it opened, from the view's
    /// first epoch, backfill or hydration to its drop.
    plans: FxHashMap<i64, ViewPlan>,
}

impl DagEngine {
    pub(crate) fn new() -> Self {
        DagEngine {
            dep: DepMap::default(),
            metas: FxHashMap::default(),
            plans: FxHashMap::default(),
        }
    }

    // ── Registration ────────────────────────────────────────────────────

    /// Derive `view_id`'s routing metadata, keep it until [`Self::forget`], and
    /// answer the placement its store registers under.
    pub(crate) fn register_view(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        pk_arity: usize,
    ) -> Result<Placement, String> {
        let meta = derive_meta(registry, view_id)?;
        let placement = self.placement_of(registry, view_id, &meta, pk_arity);
        self.metas.insert(view_id, meta);
        Ok(placement)
    }

    /// Drop everything this layer holds for relation `id`.
    pub(crate) fn forget(&mut self, id: i64) {
        self.metas.remove(&id);
        self.plans.remove(&id);
    }

    /// Apply one `CircuitNodes` delta: the dependency map, and the metadata and
    /// plan of every registered view whose circuit it adds rows to.
    pub(crate) fn apply_circuit_delta(&mut self, registry: &RelationRegistry, batch: &Batch) -> Result<(), String> {
        self.dep.apply(batch);
        let mut views: Vec<i64> = batch
            .live_rows()
            .map(|i| compiler::read_circuit_node_row(batch, i).view_id as i64)
            .filter(|v| self.metas.contains_key(v))
            .collect();
        views.sort_unstable();
        views.dedup();
        for view_id in views {
            self.metas.insert(view_id, derive_meta(registry, view_id)?);
            self.plans.remove(&view_id);
        }
        Ok(())
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// [`Self::ensure_compiled`] for a caller that cannot name a compiled plan:
    /// compile `view_id` now and keep it.
    pub(crate) fn compile_view(&mut self, registry: &RelationRegistry, view_id: i64) -> Result<(), String> {
        self.ensure_compiled(registry, view_id).map(drop)
    }

    /// The routing metadata derived when `view_id` registered. `Err` for an id
    /// that is not a registered view.
    pub(crate) fn view_meta(&self, view_id: i64) -> Result<&ViewMeta, String> {
        registered(&self.metas, view_id)
    }

    /// This view's metadata and its compiled plan, compiling and opening its
    /// operator state on a miss.
    pub(in crate::query) fn ensure_compiled(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
    ) -> Result<(&ViewMeta, &mut ViewPlan), String> {
        let meta = registered(&self.metas, view_id)?;
        let plan = match self.plans.entry(view_id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(slot) => {
                let view = registry.relation_or_err(view_id)?;
                let (code, layout) = compile(registry, view).map_err(|err| {
                    format!(
                        "view_id={view_id} does not compile from its durable circuit — this build no \
                         longer accepts that circuit or its expr blobs: {err}"
                    )
                })?;
                let state = CircuitState::open(registry, view_id, layout).map_err(|e| {
                    format!(
                        "view_id={view_id}: its derived state is corrupt or unreadable, or resources are \
                         exhausted: {e}"
                    )
                })?;
                gnitz_debug!("dag: compiled view_id={}", view_id);
                slot.insert(ViewPlan { code, state })
            }
        };
        Ok((meta, plan))
    }

    /// Every compiled view's operator state.
    pub(crate) fn ephemeral_states(&mut self) -> impl Iterator<Item = &mut CircuitState> {
        self.plans.values_mut().map(|p| &mut p.state)
    }
}

/// `view_id`'s entry in `metas`.
fn registered(metas: &FxHashMap<i64, ViewMeta>, view_id: i64) -> Result<&ViewMeta, String> {
    metas
        .get(&view_id)
        .ok_or_else(|| format!("view {view_id} is not registered"))
}

/// Load `view_id`'s circuit and derive its routing metadata.
fn derive_meta(registry: &RelationRegistry, view_id: i64) -> Result<ViewMeta, String> {
    ViewMeta::derive(&compiler::load_circuit(registry, view_id)?, registry)
}

/// Load and compile `view`'s circuit. Opens nothing.
fn compile(registry: &RelationRegistry, view: &Relation) -> Result<(CompileOutput, StateLayout), String> {
    let loaded = compiler::load_circuit(registry, view.id())?;
    compiler::compile_view(&loaded, registry, &view.schema(), view.is_bounded())
}

/// Whether registered view `view_id`'s circuit compiles. Keeps and opens
/// nothing, so the master can ask before a DDL is durable.
pub(crate) fn preflight_compile(registry: &RelationRegistry, view_id: i64) -> Result<(), String> {
    let view = registry
        .relation_or_err(view_id)
        .map_err(|e| format!("pre-flight: {e}"))?;
    compile(registry, view).map(drop)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/dag.rs"]
mod tests;
