//! DagEngine: one memo per view — its routing metadata and its compiled plan —
//! and the compilation entry point. Epoch execution lives in `exec`, per-key
//! hydration in `hydrate`, and the dependency map and the placement fold in
//! `meta`.
//!
//! Which relations exist, and the stores behind them, are the `relation` rung's
//! — a sibling, not a field. Every method here that reaches a relation takes the
//! registry as a parameter; `CatalogEngine` splits the borrow by destructuring
//! itself.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::collections::hash_map::Entry;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::compiler::{self, CompileOutput, SubPlan, ViewMeta};
use crate::query::vm;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, Relation, RelationRegistry};
use gnitz_store::schema::{Placement, SchemaDescriptor};
use gnitz_store::storage::Batch;

mod exec;
mod hydrate;
mod meta;

pub(crate) use exec::Drive;

use meta::DepMap;

// ---------------------------------------------------------------------------
// ExchangeCallback — trait for multi-worker exchange IPC
// ---------------------------------------------------------------------------

/// How a multi-worker operator reaches the other workers.
///
/// Given this worker's pre-exchange output for `view_id`, return the batch this
/// worker owns once every worker's output has been repartitioned. The transport
/// is entirely the implementor's: this crate computes, and never names a channel.
///
/// That is what lets the whole exchange path sit below the process model:
/// `gnitz-server` realizes it by sending to the master over W2M and receiving
/// the relay over the SAL, and this rung names neither.
pub(crate) trait ExchangeCallback {
    fn do_exchange(&mut self, view_id: i64, batch: Batch, source_id: i64) -> Batch;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

/// One view's memo: the routing metadata any process may ask for, and the
/// compiled plan the worker executing it adds on first epoch.
struct ViewEntry {
    meta: ViewMeta,
    plan: Option<CompileOutput>,
}

pub(crate) struct DagEngine {
    dep: DepMap,
    /// Boxed because an epoch holds `&mut CompileOutput` across its exchange
    /// waits, and a read served inside one can insert another entry.
    views: FxHashMap<i64, Box<ViewEntry>>,
}

impl DagEngine {
    pub(crate) fn new() -> Self {
        DagEngine {
            dep: DepMap::default(),
            views: FxHashMap::default(),
        }
    }

    // ── Registry-coupled operations ─────────────────────────────────────

    /// Drop `table_id` from the registry and, with it, everything the DBSP layer
    /// memoized about it. The two halves are one call because a plan left behind
    /// would hold the removed relation's stores open.
    pub(crate) fn unregister_table(&mut self, registry: &mut RelationRegistry, table_id: i64) {
        registry.unregister(table_id);
        self.invalidate(table_id);
    }

    /// Empty `view_id`'s output store and drop the plan compiled against it. The
    /// two halves are one call because the next backfill must recompile against
    /// the freshly-emptied store, not against the plan still holding the old one.
    pub(crate) fn reset_view_for_rebuild(
        &mut self,
        registry: &mut RelationRegistry,
        view_id: i64,
    ) -> Result<(), String> {
        registry.reset_view(view_id)?;
        self.invalidate(view_id);
        Ok(())
    }

    /// [`RelationRegistry::swap_schema`] under the RESTRICT check, which
    /// needs the dependency map this layer owns and is what lets the swap leave
    /// the memos alone.
    ///
    /// A real check, not a `debug_assert!`: `ddl_sync` and SAL replay reach the
    /// alter hook with the precheck bypassed, and a violation there leaves a
    /// cached VM scanning a store whose region count moved under it. Boot never
    /// trips it, so the `Err` is a fail-stop taken over serving wrong reads.
    pub(crate) fn swap_schema(
        &mut self,
        registry: &mut RelationRegistry,
        table_id: i64,
        schema: SchemaDescriptor,
    ) -> Result<(), String> {
        if !self.dependents_of(table_id).is_empty() {
            return Err(format!(
                "swap_schema: table {table_id} has dependent views; RESTRICT should have rejected the ALTER"
            ));
        }
        registry.swap_schema(table_id, schema).map_err(String::from)
    }

    // ── Memo management ─────────────────────────────────────────────────

    /// Drop one view's memo, leaving it registered. The recovery output reset
    /// needs this: the next backfill must recompile the view against its
    /// freshly-emptied store and scratch, not against the plan that still holds
    /// the old ones open.
    pub(crate) fn invalidate(&mut self, view_id: i64) {
        self.views.remove(&view_id);
    }

    /// Apply one `CircuitNodes` delta to the dependency map.
    pub(crate) fn apply_circuit_delta(&mut self, batch: &Batch) {
        self.dep.apply(batch);
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// [`Self::ensure_compiled`] for a caller that cannot name a compiled plan:
    /// compile `view_id` now and keep it.
    pub(crate) fn compile_view(&mut self, registry: &RelationRegistry, view_id: i64) -> Result<(), String> {
        self.ensure_compiled(registry, view_id).map(drop)
    }

    /// The memoized per-view routing metadata, computed on first touch. Cheaper
    /// than full compilation: no code emission. An unreadable or unroutable
    /// circuit is not memoized, so a later touch retries.
    pub(crate) fn view_meta(&mut self, registry: &RelationRegistry, view_id: i64) -> Result<&ViewMeta, String> {
        if !self.views.contains_key(&view_id) {
            let loaded = compiler::load_circuit(registry, view_id as u64)?;
            self.memoize_meta(registry, view_id, &loaded)?;
        }
        Ok(&self.views.get(&view_id).expect("memoized above").meta)
    }

    /// Memoize `loaded`'s routing metadata under `view_id`. Skipped when a memo
    /// exists: a live view's circuit rows never change.
    fn memoize_meta(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        loaded: &compiler::LoadedCircuit,
    ) -> Result<(), String> {
        if let Entry::Vacant(slot) = self.views.entry(view_id) {
            slot.insert(Box::new(ViewEntry {
                meta: ViewMeta::derive(loaded, registry)?,
                plan: None,
            }));
        }
        Ok(())
    }

    /// This view's memo, with its plan compiled. `Err` when `view_id` is not a
    /// registered relation, or when a registered view did not compile.
    ///
    /// Nothing re-pre-flights the circuit here, and this compile opens resumed
    /// operator state the pre-flight's throwaway root never had. An `Err` is
    /// therefore unrecoverable for a server — the alternative to aborting is a
    /// view that has stopped integrating while still answering reads.
    pub(in crate::query) fn ensure_compiled(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
    ) -> Result<(&ViewMeta, &mut CompileOutput), String> {
        if self.views.get(&view_id).is_none_or(|e| e.plan.is_none()) {
            let relation = registry.relation_or_err(view_id)?;
            let loaded = compiler::load_circuit(registry, view_id as u64)?;
            self.memoize_meta(registry, view_id, &loaded)?;
            let output =
                compile_circuit(&loaded, registry, view_id, relation.directory(), relation).map_err(|err| {
                    format!(
                        "view_id={view_id} does not compile from its durable circuit — this build no \
                         longer accepts that circuit or its expr blobs, its derived state is \
                         corrupt or unreadable, or resources are exhausted: {err}"
                    )
                })?;
            gnitz_debug!("dag: compiled view_id={}", view_id);
            self.views.get_mut(&view_id).expect("filled above").plan = Some(output);
        }
        let entry = self.views.get_mut(&view_id).expect("compiled above");
        Ok((&entry.meta, entry.plan.as_mut().expect("compiled above")))
    }

    /// The backfill-scan bound for `source` under `view_id`: `ReadBound::None` unless
    /// its circuit carries one.
    pub(crate) fn source_scan_bound(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        source: i64,
    ) -> gnitz_wire::ReadBound {
        self.view_meta(registry, view_id)
            .ok()
            .and_then(|m| m.source_bounds.get(&source).cloned())
            .unwrap_or(gnitz_wire::ReadBound::None)
    }

    /// Decide whether a just-registered view's circuit compiles, keeping nothing.
    /// Called on the master inside the DDL, before the bundle reaches the SAL, so
    /// a rejection takes the ordinary ingest failure path and reaches the client
    /// with the view never created. The worker-side compile that follows happens
    /// after the DDL is durable and has no channel back to the waiting client.
    ///
    /// `root` is a throwaway directory the caller creates the path for and
    /// removes again — see `catalog::utils::preflight_dir` for why it is not the
    /// view's own. The plan is dropped here, so the child stores it opened under
    /// `root` are closed — and their directories removed — before this returns.
    ///
    /// The verdict is worker-independent: it compiles under the master's slot,
    /// `root` overrides the one path component the slot contributes, and no
    /// rejection reads it.
    pub(crate) fn preflight_compile(
        &self,
        registry: &RelationRegistry,
        view_id: i64,
        root: &str,
    ) -> Result<(), String> {
        // `hook_relation_register` ran earlier in this bundle's ingest loop, so a
        // registered `+1` VIEW_TAB row is always in the registry; a miss is an
        // engine bug, surfaced as a DDL rejection rather than an unchecked compile.
        let entry = registry
            .relation_or_err(view_id)
            .map_err(|e| format!("pre-flight: {e}"))?;
        let loaded = compiler::load_circuit(registry, view_id as u64)?;
        // `map(drop)` closes the plan — and the child stores it opened under
        // `root` — before the caller removes the directory.
        compile_circuit(&loaded, registry, view_id, root, entry).map(drop)
    }

    /// Every compiled view plan's operator state, for the ephemeral checkpoint
    /// round to force-persist ahead of the registry's own output stores. Only a
    /// view has circuit rows, so only a view's memo can hold a plan.
    pub(crate) fn collect_ephemeral_state(&mut self) -> Vec<&mut CircuitState> {
        let mut state: Vec<&mut CircuitState> = Vec::new();
        for plan in self.views.values_mut().filter_map(|e| e.plan.as_mut()) {
            for sub in plan.sub_plans_mut() {
                state.push(&mut sub.vm.state);
            }
        }
        state
    }
}

/// Compile `view_id`'s loaded circuit, homing every scratch child under `dir`.
/// The directory is a parameter and not read off `entry` because the pre-flight
/// compiles into a throwaway root.
fn compile_circuit(
    loaded: &compiler::LoadedCircuit,
    registry: &RelationRegistry,
    view_id: i64,
    dir: &str,
    entry: &Relation,
) -> Result<CompileOutput, String> {
    let site = compiler::ViewSite { dir, id: view_id as u64, registry };
    // The compiler layer sees only the circuit system table, never `VIEW_TAB`,
    // so it cannot derive whether the view is capacity-bounded.
    compiler::compile_view(loaded, site, &entry.schema(), entry.is_bounded())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/dag.rs"]
mod tests;
