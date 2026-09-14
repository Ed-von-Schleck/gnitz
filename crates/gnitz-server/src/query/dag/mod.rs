//! DagEngine: the plan cache, the memoized view metadata, and the compilation
//! entry point. Epoch execution lives in `exec`, per-key hydration in `hydrate`,
//! and the dependency map and the plan-free metadata queries in `meta`.
//!
//! Which relations exist, and the stores behind them, are the `relation` rung's
//! — a sibling, not a field. Every method here that reaches a relation takes the
//! registry as a parameter; `CatalogEngine` splits the borrow by destructuring
//! itself.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use rustc_hash::{FxHashMap, FxHashSet};
use std::rc::Rc;

use crate::query::compiler::{self, CompileOutput, SubPlan, ViewMeta};
use crate::query::vm;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, Relation, RelationRegistry};
use gnitz_store::schema::{Placement, SchemaDescriptor};
use gnitz_store::storage::Batch;

mod exec;
mod hydrate;
mod meta;

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
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

pub(crate) struct DagEngine {
    cache: FxHashMap<i64, CompileOutput>,
    dep: DepMap,
    /// Memoized plan-free per-view circuit metadata (see `compiler::ViewMeta`).
    meta: FxHashMap<i64, Rc<ViewMeta>>,
}

impl DagEngine {
    pub(crate) fn new() -> Self {
        DagEngine {
            cache: FxHashMap::default(),
            dep: DepMap::default(),
            meta: FxHashMap::default(),
        }
    }

    // ── Registry-coupled operations ─────────────────────────────────────

    /// Drop `table_id` from the registry and, with it, everything the DBSP layer
    /// memoized about it. The two halves are one call because a plan left behind
    /// would hold the removed relation's stores open.
    pub(crate) fn unregister_table(&mut self, registry: &mut RelationRegistry, table_id: i64) {
        registry.unregister(table_id);
        self.invalidate(table_id);
        self.dep.invalidate();
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
    /// the plan cache alone.
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
        if self.has_dependents(registry, table_id) {
            return Err(format!(
                "swap_schema: table {table_id} has dependent views; RESTRICT should have rejected the ALTER"
            ));
        }
        registry.swap_schema(table_id, schema).map_err(String::from)
    }

    // ── Cache management ────────────────────────────────────────────────

    /// Drop one view's cached plan + memoized view metadata, leaving it
    /// registered. The recovery output reset needs this: the next backfill must
    /// recompile the view against its freshly-emptied store and scratch, not
    /// against the plan that still holds the old ones open.
    pub(crate) fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.evict_meta(view_id);
    }

    /// Drop every memo. Only `close` and the tests that assert a cold
    /// recompile want this: a memo is a pure function of the circuit and the
    /// registry's schemas, so clearing one discards work rather than staleness.
    #[cfg(test)]
    pub(crate) fn invalidate_all(&mut self) {
        self.cache.clear();
        self.meta.clear();
        self.dep.invalidate();
    }

    pub(crate) fn invalidate_dep_map(&mut self) {
        self.dep.invalidate();
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// Ensure a view's plan is compiled. `Ok(false)` means `view_id` is not a
    /// registered relation; `Err` means a registered view did not compile.
    ///
    /// Nothing re-pre-flights the circuit here, and this compile opens resumed
    /// operator state the pre-flight's throwaway root never had. An `Err` is
    /// therefore unrecoverable for a server — the alternative to aborting is a
    /// view that has stopped integrating while still answering reads.
    ///
    /// The error is `String` rather than `CompileError`, which is `pub(crate)`
    /// and so cannot appear in a `pub fn`'s signature.
    pub(crate) fn ensure_compiled(&mut self, registry: &RelationRegistry, view_id: i64) -> Result<bool, String> {
        if self.cache.contains_key(&view_id) {
            return Ok(true);
        }
        let Some(entry) = registry.relation(view_id) else {
            return Ok(false);
        };
        let compiled = self
            .compile_circuit(registry, view_id, entry.directory(), entry)
            .map_err(|err| {
                format!(
                    "view_id={view_id} does not compile from its durable circuit — this build no \
                     longer accepts that circuit or its expr blobs, its derived state is \
                     corrupt or unreadable, or resources are exhausted: {err}"
                )
            })?;
        gnitz_debug!("dag: compiled view_id={}", view_id);
        // The compile already walked this circuit, so seed the memo from what it
        // derived rather than let the first metadata touch read the same three
        // system tables again.
        self.meta.insert(view_id, Rc::new(compiled.meta));
        self.cache.insert(view_id, compiled.output);
        Ok(true)
    }

    /// The backfill-scan bound for `source` under `view_id`, if the compiled plan
    /// pushed one down. By value (`IndexBound: Copy`) so callers can re-borrow
    /// `self` without holding this borrow.
    pub(crate) fn source_scan_bound(&self, view_id: i64, source: i64) -> Option<gnitz_wire::IndexBound> {
        self.cache
            .get(&view_id)
            .and_then(|co| co.source_bound)
            .filter(|&(s, _)| s == source)
            .map(|(_, b)| b)
    }

    /// Read `view_id`'s circuit out of the system tables and compile it, homing
    /// every scratch child under `dir`. The directory is a parameter and not read
    /// off `entry` because the pre-flight compiles into a throwaway root.
    fn compile_circuit(
        &self,
        registry: &RelationRegistry,
        view_id: i64,
        dir: &str,
        entry: &Relation,
    ) -> Result<compiler::CompiledView, compiler::CompileError> {
        let site = compiler::ViewSite { dir, id: view_id as u64, registry };
        // The compiler layer sees only the circuit system table, never `VIEW_TAB`,
        // so it cannot derive whether the view is capacity-bounded.
        compiler::compile_view(site, &entry.schema(), entry.is_bounded())
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
        // `map(drop)` closes the plan — and the child stores it opened under
        // `root` — before the caller removes the directory.
        self.compile_circuit(registry, view_id, root, entry)
            .map(drop)
            .map_err(|e| e.to_string())
    }

    /// Close the DagEngine, dropping all cached plans. Reached from the
    /// crash-semantics tests through `CatalogEngine::close`; the server flushes
    /// durably per zone and exits via abort or process teardown instead.
    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        self.invalidate_all();
    }

    /// Every compiled view plan's operator state, for the ephemeral checkpoint
    /// round to force-persist ahead of the registry's own output stores.
    ///
    /// The registry is read only for the `is_view` filter on each cached plan;
    /// the explicit lifetime is what ends that borrow at the call, so the caller
    /// can take `&mut` on the registry while this vector is still alive.
    pub(crate) fn collect_ephemeral_state<'a>(&'a mut self, registry: &RelationRegistry) -> Vec<&'a mut CircuitState> {
        // Iterate the (smaller) plan cache and consult the registry for each
        // plan's kind. Every `cache` entry has a matching registry entry
        // (`ensure_compiled` requires it first; `unregister_table` removes both),
        // so this misses no view's state.
        let mut state: Vec<&mut CircuitState> = Vec::new();
        for (tid, plan) in self.cache.iter_mut() {
            if !registry.relation(*tid).map(Relation::kind).is_some_and(|k| k.is_view()) {
                continue;
            }
            for sub in plan.sub_plans_mut() {
                // Drop the bound cursors before the fold so none holds a stale
                // snapshot.
                sub.vm.reset_trace_cursors();
                state.push(&mut sub.vm.state);
            }
        }
        state
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/dag.rs"]
mod tests;
