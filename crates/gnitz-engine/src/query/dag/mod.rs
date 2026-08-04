//! DagEngine: the relation registry, plan cache, and compilation entry point.
//! Epoch execution lives in `exec`, ingestion in `ingest`, and the plan-free
//! view metadata (dependency map + `ViewMeta`) in `meta`.

use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::UnsafeCell;
use std::rc::Rc;

use crate::ops;
use crate::query::compiler::{self, CompileOutput, SubPlan};
use crate::query::vm;
use crate::schema::{Placement, SchemaDescriptor};
use crate::storage::{Batch, PartitionedTable, RecoverySource, StorageError, Table};
use gnitz_wire::PkColList;

mod exec;
mod ingest;
mod meta;
mod store_handle;

use meta::DepMap;
pub(crate) use meta::ViewMeta;

pub(crate) use store_handle::{StoreHandle, StoreProbe};

// ---------------------------------------------------------------------------
// Index circuit entry
// ---------------------------------------------------------------------------

/// A secondary index on a column.
/// Owns the index Table via Box — dropping the entry drops the table.
pub struct IndexCircuitEntry {
    /// The index's declared column list, in order. A 1-element list is the
    /// single-column case. Dedup/lookup is exact ordered-list equality on
    /// `col_indices.as_slice()`; order is significant (it drives leading-prefix
    /// seeks).
    pub col_indices: PkColList,
    /// The index_id of the IDX_TAB row that caused Table::new to be called.
    /// When a second index promotes an incumbent circuit (UNIQUE+FK case), no
    /// new directory is created; this field identifies the actual on-disk path
    /// so the retraction branch queues the correct directory for deletion.
    pub index_id: i64,
    pub index_table: UnsafeCell<Box<Table>>,
    pub index_schema: SchemaDescriptor,
    /// Full-arity span-encode plan, precomputed at registration (owner and
    /// index schemas are immutable post-registration — no ALTER exists) so the
    /// per-push consumers do no per-call spec rebuild. Deliberately does NOT
    /// bake in `is_unique` (live promotion/demotion via
    /// `set_index_circuit_uniqueness`); consumers filter on the live flag.
    pub key_spec: crate::schema::IndexKeySpec,
    pub is_unique: bool,
}

impl IndexCircuitEntry {
    /// Interior-mutable access to the owned index Table.
    ///
    /// `index_table` is an `UnsafeCell`, so `get()` yields a `*mut` that is
    /// legal to mutate through even via `&self`. Single-threaded; callers
    /// must ensure no aliasing `&mut` into the same Table is live.
    #[allow(clippy::mut_from_ref)]
    pub fn table_mut(&self) -> &mut Table {
        unsafe { &mut *self.index_table.get() }
    }

    /// The source column list of a unique circuit, `None` for a non-unique one
    /// — the accessor DDL validation reads a unique circuit's columns through.
    /// The returned slice has length ≥ 1: a
    /// single-column unique index yields a 1-element list, a composite
    /// `UNIQUE (a, b, …)` the full ordered list. Order is significant (it drives
    /// the leading-key span encoding and prefix seeks).
    #[inline]
    pub fn unique_cols(&self) -> Option<&[u32]> {
        if !self.is_unique {
            return None;
        }
        Some(self.col_indices.as_slice())
    }
}

// ---------------------------------------------------------------------------
// Relation kind — what a top-level relation *is*
// ---------------------------------------------------------------------------

/// What a top-level relation *is*. Bundles every per-kind property that used
/// to be set independently, so the nonsense combinations are unconstructable:
/// a durable relation that also rebuilds from source (double count), an
/// ephemeral one that never rebuilds (permanently empty).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelationKind {
    /// System catalog table: durable, single-partition, never rebuilt from
    /// upstream sources (it has none; recovery is LSN-gated SAL replay).
    SystemCatalog,
    /// User base table: durable, partitioned, never rebuilt from upstream
    /// sources; owns a DML-enforced PK, so `enforce_unique_pk` runs on every
    /// ingest and its accumulated per-PK weight is always in {0, 1}.
    BaseTable,
    /// Materialised view: ephemeral, partitioned, rebuilt from its sources via
    /// the compiled circuit at open and on live CREATE.
    View,
}

impl RelationKind {
    /// How this relation's tail is recovered. `SalReplay` kinds load shards from
    /// the manifest and replay the SAL tail; view output stores are
    /// `RederiveCheckpointed` — erased on open and rebuilt today, but persisted
    /// with generation-stamped manifests by the ephemeral checkpoint round.
    #[inline]
    pub fn recovery_source(self) -> RecoverySource {
        match self {
            RelationKind::SystemCatalog | RelationKind::BaseTable => RecoverySource::SalReplay,
            RelationKind::View => RecoverySource::rederive_checkpointed_now(),
        }
    }

    /// True iff this is a user base table. Gates what only base tables do:
    /// run `enforce_unique_pk` on ingest, and own secondary index circuits
    /// (index projection runs only on the base-table DML paths).
    #[inline]
    pub fn is_base_table(self) -> bool {
        matches!(self, RelationKind::BaseTable)
    }

    /// True iff this is a materialised view. Gates the read-your-writes drain (a
    /// view derives from source pushes through the DAG, so a read must flush
    /// pending ticks first) and the ephemeral checkpoint round (only a view's
    /// output store / operator traces are persisted there); base tables and the
    /// system catalog do neither.
    #[inline]
    pub fn is_view(self) -> bool {
        matches!(self, RelationKind::View)
    }
}

// ---------------------------------------------------------------------------
// Table entry — per-table metadata in the entity registry
// ---------------------------------------------------------------------------

pub struct TableEntry {
    pub handle: StoreHandle,
    pub schema: SchemaDescriptor,
    pub kind: RelationKind,
    pub depth: i32,
    pub directory: String,
    pub index_circuits: Vec<IndexCircuitEntry>,
}

impl TableEntry {
    /// The index circuit covering exactly `cols`, if one exists. The circuit
    /// list is deduped by ordered column list, so at most one entry matches —
    /// this is the one place that match is spelled.
    pub fn index_circuit_on(&self, cols: &[u32]) -> Option<&IndexCircuitEntry> {
        self.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols)
    }

    pub fn index_circuit_on_mut(&mut self, cols: &[u32]) -> Option<&mut IndexCircuitEntry> {
        self.index_circuits
            .iter_mut()
            .find(|ic| ic.col_indices.as_slice() == cols)
    }
}

// ---------------------------------------------------------------------------
// System table references
// ---------------------------------------------------------------------------

/// Handles for the four system tables that the compiler reads: CircuitNodes,
/// CircuitEdges, CircuitNodeColumns, and DepTab. No schemas are threaded — the
/// compiler's cursor readers source each table's schema from the cursor itself
/// (`ReadCursor::schema`), and `get_dep_map` reads DepTab's compound PK directly
/// from the cursor's PK bytes.
pub struct SysTableRefs {
    // Table handles (DagEngine borrows them).
    pub nodes: *mut Table,
    pub edges: *mut Table,
    pub node_columns: *mut Table,
    pub dep_tab: *mut Table,
}

// SAFETY: same single-thread guarantee.
unsafe impl Send for SysTableRefs {}

impl SysTableRefs {
    fn null() -> Self {
        SysTableRefs {
            nodes: std::ptr::null_mut(),
            edges: std::ptr::null_mut(),
            node_columns: std::ptr::null_mut(),
            dep_tab: std::ptr::null_mut(),
        }
    }
}

// ---------------------------------------------------------------------------
// ExchangeCallback — trait for multi-worker exchange IPC
// ---------------------------------------------------------------------------

/// Callback trait for exchange IPC between workers.
///
/// The worker event loop implements this trait to send pre-exchange output
/// to the master (via W2M) and receive relayed data (via SAL).
/// This breaks the circular dependency between worker.rs ↔ dag.rs.
pub trait ExchangeCallback {
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch;
}

// ---------------------------------------------------------------------------
// DagEngine
// ---------------------------------------------------------------------------

pub struct DagEngine {
    cache: FxHashMap<i64, CompileOutput>,
    dep: DepMap,
    /// Memoized plan-free per-view circuit metadata (see `meta::ViewMeta`).
    meta: FxHashMap<i64, Rc<ViewMeta>>,
    pub(crate) tables: FxHashMap<i64, TableEntry>,
    sys: SysTableRefs,
}

// SAFETY: DagEngine is only accessed from a single thread.
unsafe impl Send for DagEngine {}

impl DagEngine {
    pub fn new() -> Self {
        DagEngine {
            cache: FxHashMap::default(),
            dep: DepMap::default(),
            meta: FxHashMap::default(),
            tables: FxHashMap::default(),
            sys: SysTableRefs::null(),
        }
    }

    // ── System table setup ──────────────────────────────────────────────

    pub fn set_sys_tables(&mut self, sys: SysTableRefs) {
        self.sys = sys;
    }

    // ── Table registry ──────────────────────────────────────────────────

    pub fn register_table(
        &mut self,
        table_id: i64,
        handle: StoreHandle,
        schema: SchemaDescriptor,
        kind: RelationKind,
        depth: i32,
        directory: String,
    ) {
        self.tables.insert(
            table_id,
            TableEntry {
                handle,
                schema,
                kind,
                depth,
                directory,
                index_circuits: Vec::new(),
            },
        );
    }

    pub fn unregister_table(&mut self, table_id: i64) {
        self.tables.remove(&table_id);
        self.cache.remove(&table_id);
        self.evict_meta(table_id);
        self.dep.invalidate();
    }

    pub fn add_index_circuit(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        index_id: i64,
        index_table: Box<Table>,
        index_schema: SchemaDescriptor,
        is_unique: bool,
    ) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            entry.index_circuits.push(IndexCircuitEntry {
                col_indices: PkColList::from_slice(col_indices),
                index_id,
                index_table: UnsafeCell::new(index_table),
                index_schema,
                key_spec: crate::schema::IndexKeySpec::new(col_indices, &entry.schema, &index_schema),
                is_unique,
            });
        }
    }

    pub fn remove_index_circuit(&mut self, table_id: i64, col_indices: &[u32]) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            // retain() drops non-matching entries, which drops Box<Table> automatically.
            entry
                .index_circuits
                .retain(|ic| ic.col_indices.as_slice() != col_indices);
        }
    }

    /// Replace the index circuit's owned Table (worker-boot re-home to the rank
    /// subdir): drop the fork-inherited parent-dir table and install `t`.
    /// Returns the new table pointer for the backfill; `None` if no circuit on
    /// `col_indices` matches. Dropping the old `Box` closes the inherited table.
    /// Sound: pointer consumers (`get_index_store_handle`, cursors) are fetched
    /// per request, and the swap runs before the worker serves anything.
    pub fn replace_index_table(&mut self, table_id: i64, col_indices: &[u32], t: Box<Table>) -> Option<*mut Table> {
        let ic = self.tables.get_mut(&table_id)?.index_circuit_on_mut(col_indices)?;
        ic.index_table = UnsafeCell::new(t);
        Some(ic.table_mut() as *mut Table)
    }

    /// Set the uniqueness flag of the index circuit on `col_indices` in place
    /// (the circuit list is deduped by column list, so at most one entry
    /// matches). Promotion (`true`) folds a UNIQUE index into an existing
    /// non-unique circuit when both target the same column list; demotion
    /// (`false`) is used by the DROP INDEX retraction path when the UNIQUE index
    /// is dropped but another index (e.g. an FK auto-index) still covers it.
    pub fn set_index_circuit_uniqueness(&mut self, table_id: i64, col_indices: &[u32], is_unique: bool) {
        if let Some(ic) = self
            .tables
            .get_mut(&table_id)
            .and_then(|e| e.index_circuit_on_mut(col_indices))
        {
            ic.is_unique = is_unique;
        }
    }

    /// Publish a new comparator schema for a registered base table in place
    /// (ALTER … DROP NOT NULL). Equal-region by construction — the region count,
    /// PK columns and column widths are unchanged; only a column's nullability
    /// flips, which downgrades the whole-schema payload comparator
    /// `FixedIntNonnull → Generic`. Infallible: a pure descriptor replacement is
    /// the only safe shape (a fallible partial swap is the sole route to a
    /// persistent half-swap where a NULL could consolidate against a real `0`).
    ///
    /// Updates the registry copy (`TableEntry.schema`) and pushes the same value
    /// down through the store's owned copies —
    /// `PartitionedTable` → each partition `Table` (table/memtable/shard-index) —
    /// plus dropping `Table::cached_full_scan`. All copies are `Copy`, so each
    /// assignment is a byte copy. No run rebuild, no shard reopen, no data
    /// motion: existing (never-null) rows stay correctly sorted (§2).
    pub(crate) fn swap_table_schema(&mut self, table_id: i64, schema: SchemaDescriptor) {
        let entry = self
            .tables
            .get_mut(&table_id)
            .expect("swap_table_schema: table must be registered");
        // Post-fork the master holds no user partitions, so `as_partitioned_mut`
        // returns a store with an empty partition set and the swap is the
        // descriptor-only `TableEntry.schema` update; on every worker the store
        // has live partitions and the swap fans out to each.
        if let Some(pt) = entry.handle.as_partitioned_mut() {
            pt.swap_schema(schema);
        }
        entry.schema = schema;
        // §1 RESTRICT invariant, made load-bearing: no compiled circuit scans an
        // altered base table (dependent views are rejected at plan time and by
        // the catalog precheck arm), so no plan-cache invalidation is required.
        debug_assert!(
            {
                let deps = self.get_dep_map();
                deps.get(&table_id).is_none_or(|v| v.is_empty())
            },
            "swap_table_schema: table {table_id} has dependent views; RESTRICT should have rejected the ALTER",
        );
    }

    // ── Cache management ────────────────────────────────────────────────

    /// Drop one view's cached plan + memoized view metadata, leaving it
    /// registered. Callers: the test-only `drop_view`, the dag tests, and the
    /// recovery step-4 output reset (`reset_view_output_for_rebuild`), which needs
    /// the next backfill to recompile the view against its freshly-emptied store
    /// and scratch.
    pub fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.evict_meta(view_id);
        self.dep.invalidate();
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.meta.clear();
        self.dep.invalidate();
    }

    pub fn invalidate_dep_map(&mut self) {
        self.dep.invalidate();
    }

    // ── Compilation ─────────────────────────────────────────────────────

    /// Ensure a view's plan is compiled. Returns true if compilation succeeded.
    pub fn ensure_compiled(&mut self, view_id: i64) -> bool {
        if self.cache.contains_key(&view_id) {
            return true;
        }
        match self.compile_view_internal(view_id) {
            Some(plan) => {
                self.cache.insert(view_id, plan);
                true
            }
            None => false,
        }
    }

    /// The backfill-scan bound for `source` under `view_id`, if the compiled plan
    /// pushed one down. By value (`ScanBound: Copy`) so callers can re-borrow
    /// `self` without holding this borrow.
    pub fn source_scan_bound(&self, view_id: i64, source: i64) -> Option<gnitz_wire::ScanBound> {
        self.cache
            .get(&view_id)
            .and_then(|co| co.source_bound)
            .filter(|&(s, _)| s == source)
            .map(|(_, b)| b)
    }

    /// The registered relations visible to a compile: table id → schema.
    fn ext_tables(&self) -> compiler::ExtTables {
        self.tables.iter().map(|(&tid, te)| (tid, te.schema)).collect()
    }

    /// Read `view_id`'s circuit out of the system tables and compile it, homing
    /// every scratch child under `view_dir`. The directory is a parameter and not
    /// read off the entry because the pre-flight compiles into a throwaway root.
    fn compile_circuit(
        &self,
        view_id: i64,
        view_dir: &str,
        view_schema: &SchemaDescriptor,
    ) -> Result<CompileOutput, compiler::CompileError> {
        let ext_tables = self.ext_tables();
        unsafe {
            compiler::compile_view(
                view_id as u64,
                self.sys.nodes,
                self.sys.edges,
                self.sys.node_columns,
                view_dir,
                view_schema,
                &ext_tables,
            )
        }
    }

    /// Decide whether a just-registered view's circuit compiles, keeping nothing.
    /// Called on the master inside the DDL, before the bundle reaches the SAL, so
    /// a rejection takes the ordinary ingest failure path and reaches the client
    /// with the view never created. The worker-side compile that follows happens
    /// after the DDL is durable and has no channel back to the waiting client.
    ///
    /// `root` is a throwaway directory, removed before this returns — see
    /// `catalog::utils::preflight_dir` for why it is not the view's own.
    ///
    /// The verdict is worker-independent: worker context reaches the compile only
    /// as the scratch path component (which `root` overrides), as `PartitionFilter`
    /// and `ReducePlan` operands baked into instructions nothing here executes, and
    /// as the committed generation a manifest-less directory makes moot.
    pub(crate) fn preflight_compile(&self, view_id: i64, root: &str) -> Result<(), compiler::CompileError> {
        // `hook_view_register` ran earlier in this bundle's ingest loop, so a
        // registered `+1` VIEW_TAB row is always in `tables`; a miss is an engine
        // bug, surfaced as a DDL rejection rather than an unchecked compile.
        let Some(entry) = self.tables.get(&view_id) else {
            return Err(compiler::CompileError::Rejected("pre-flight: view is not registered"));
        };
        // `map(drop)` closes the plan — and the `Table`s it holds open under
        // `root` — before the directory is removed.
        let verdict = self.compile_circuit(view_id, root, &entry.schema).map(drop);
        let _ = std::fs::remove_dir_all(root);
        verdict
    }

    /// Compile a view by reading system tables and calling `compiler::compile_view`.
    ///
    /// `None` means only "not a registered relation". A registered view's circuit
    /// already compiled on the master under `preflight_compile` before the CREATE
    /// was made durable, and no DDL can change that verdict afterwards: DROP COLUMN
    /// and DROP NOT NULL on a table with dependent views are RESTRICTed, and no DDL
    /// widens or re-types a registered column. So a rejection here is this process
    /// failing to build the view's operator state — out of disk or file
    /// descriptors — or an engine bug. Both are unrecoverable: the only alternative
    /// to aborting is serving a view that is silently and permanently empty.
    fn compile_view_internal(&self, view_id: i64) -> Option<CompileOutput> {
        let entry = self.tables.get(&view_id)?;
        match self.compile_circuit(view_id, &entry.directory, &entry.schema) {
            Ok(output) => {
                gnitz_debug!("dag: compiled view_id={}", view_id);
                Some(output)
            }
            Err(err) => {
                gnitz_fatal_abort!(
                    "view_id={} compiled on the master at CREATE VIEW but not here — \
                     its operator state cannot be built on this node (out of disk or \
                     file descriptors?), or this is an engine bug: {}",
                    view_id,
                    err
                );
            }
        }
    }

    /// Close the DagEngine, dropping all cached plans. Test-only, like the
    /// `CatalogEngine::close` that drives it: the server never closes gracefully.
    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        self.cache.clear();
        self.tables.clear();
        self.meta.clear();
        self.dep = DepMap::default();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::posix_io::raise_fd_limit_for_tests;

    fn dag_test_dir(name: &str) -> String {
        crate::test_support::scratch_dir("dag", name)
    }

    fn make_test_table(name: &str) -> Box<Table> {
        let schema = SchemaDescriptor::default();
        let dir = dag_test_dir(name);
        let _ = std::fs::remove_dir_all(&dir);
        Box::new(Table::new(&dir, schema, 99, 256 * 1024, RecoverySource::Rederive).unwrap())
    }

    #[test]
    fn test_dag_engine_lifecycle() {
        let mut dag = DagEngine::new();
        assert!(dag.cache.is_empty());
        assert!(dag.tables.is_empty());
        dag.close();
    }

    #[test]
    fn test_register_unregister_table() {
        let mut dag = DagEngine::new();
        let schema = SchemaDescriptor::default();
        let mut tbl = make_test_table("reg_unreg");
        dag.register_table(
            100,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            schema,
            RelationKind::BaseTable,
            0,
            String::new(),
        );
        assert!(dag.tables.contains_key(&100));

        dag.unregister_table(100);
        assert!(!dag.tables.contains_key(&100));
        let _ = std::fs::remove_dir_all(dag_test_dir("reg_unreg"));
    }

    /// A `ViewMeta` fixture whose join-shard map names `src` as a source.
    fn meta_with_source(src: i64) -> Rc<ViewMeta> {
        let mut jm: FxHashMap<i64, Rc<[(i32, u8)]>> = FxHashMap::default();
        jm.insert(src, Rc::from([]));
        Rc::new(ViewMeta {
            shard_cols: None,
            join_shard_map: jm,
            range_join_n_eq: None,
            has_join: false,
        })
    }

    #[test]
    fn test_invalidation() {
        let mut dag = DagEngine::new();
        dag.meta.insert(42, meta_with_source(7));
        dag.dep.valid = true;

        dag.invalidate(42);
        assert!(!dag.meta.contains_key(&42));
        assert!(!dag.dep.valid);

        dag.dep.valid = true;
        dag.invalidate_dep_map();
        assert!(!dag.dep.valid);

        dag.meta.insert(99, meta_with_source(7));
        dag.invalidate_all();
        assert!(dag.meta.is_empty());
    }

    /// `evict_meta` must drop the metadata mentioning the id as the owning view
    /// OR as a join source of another view's map — a dropped relation can be
    /// either, and a stale entry would disagree with the live circuit
    /// (over-eviction is safe; entries are recomputed on next touch).
    #[test]
    fn test_view_meta_eviction() {
        let mut dag = DagEngine::new();
        dag.meta.insert(42, meta_with_source(7)); // 42 as the view
        dag.meta.insert(7, meta_with_source(42)); // 42 as a source of view 7
        dag.evict_meta(42);
        assert!(!dag.meta.contains_key(&42));
        assert!(
            !dag.meta.contains_key(&7),
            "evict must drop views whose map mentions the id as a source"
        );

        // Wiring: the production table/view-drop path routes through evict_meta.
        let mut dag = DagEngine::new();
        dag.meta.insert(43, meta_with_source(1));
        dag.unregister_table(43);
        assert!(!dag.meta.contains_key(&43), "unregister_table must evict");
    }

    #[test]
    fn test_add_remove_index_circuit() {
        let mut dag = DagEngine::new();
        // A real 3-column owner schema: registration precomputes the circuit's
        // `key_spec` from it, which locates indexed column 2.
        let schema = SchemaDescriptor::new(
            &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 3],
            &[0],
        );
        let mut tbl = make_test_table("idx_parent");
        dag.register_table(
            50,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            schema,
            RelationKind::BaseTable,
            0,
            String::new(),
        );
        let idx_tbl = make_test_table("idx_child");
        dag.add_index_circuit(50, &[2], 999, idx_tbl, schema, false);
        assert_eq!(dag.tables[&50].index_circuits.len(), 1);

        dag.remove_index_circuit(50, &[2]);
        assert_eq!(dag.tables[&50].index_circuits.len(), 0);
        dag.close();
        let _ = std::fs::remove_dir_all(dag_test_dir("idx_parent"));
        let _ = std::fs::remove_dir_all(dag_test_dir("idx_child"));
    }

    // `test_validate_graph_*` tests previously exercised
    // `DagEngine::validate_graph_structure`. The function is no longer
    // load-bearing — its only non-test caller (`engine.create_view`) was
    // removed alongside the circuit-graph schema redesign. The compiler's
    // `topo_sort` already rejects cycles, and the typed `OpNode` enum makes
    // "missing primary input" and "missing INTEGRATE sink" structurally
    // checkable at the wire-construction site instead.

    #[test]
    fn test_dep_map_empty() {
        let mut dag = DagEngine::new();
        dag.get_dep_map();
        assert!(dag.dep.forward.is_empty());
        assert!(dag.dep.valid);
    }

    #[test]
    fn test_source_ids_empty() {
        let mut dag = DagEngine::new();
        assert!(dag.get_source_ids(42).is_empty());
    }

    /// Install `edges` (source → view) into an already-valid dep map, the same
    /// pair of entries `DepMap::get_or_rebuild` writes per DepTab row.
    fn dag_with_deps(edges: &[(i64, i64)]) -> DagEngine {
        let mut dag = DagEngine::new();
        for &(src, view) in edges {
            dag.dep.forward.entry(src).or_default().push(view);
            dag.dep.reverse.entry(view).or_default().push(src);
        }
        dag.dep.valid = true;
        dag
    }

    /// `source_closure` walks `view → sources` transitively, over a chain, a
    /// diamond and a disconnected pair. A seed is not its own source, so neither
    /// direction reports it.
    #[test]
    fn test_source_closure_walks_sources_transitively() {
        // chain 1 → 2 → 3, diamond 10 → {11,12} → 13, disconnected pair 20 → 21.
        let mut dag = dag_with_deps(&[(1, 2), (2, 3), (10, 11), (10, 12), (11, 13), (12, 13), (20, 21)]);

        assert!(dag.source_closure(vec![]).is_empty());
        assert_eq!(dag.source_closure(vec![3]), [1i64, 2].into_iter().collect());
        assert_eq!(dag.source_closure(vec![13]), [10i64, 11, 12].into_iter().collect());
        assert_eq!(dag.source_closure(vec![21]), [20i64].into_iter().collect());
        assert!(dag.source_closure(vec![1]).is_empty());
        assert!(dag.source_closure(vec![99]).is_empty());
        // The other direction over the same edges, so a walk that read the wrong
        // half of `DepMap` cannot pass both.
        dag.get_dep_map();
        assert_eq!(
            DepMap::closure(&dag.dep.forward, vec![1]),
            [2i64, 3].into_iter().collect::<rustc_hash::FxHashSet<i64>>()
        );
    }

    #[test]
    fn test_flush_includes_index_circuits() {
        let mut dag = DagEngine::new();
        // A real 2-column owner schema: registration precomputes the circuit's
        // `key_spec` from it, which locates indexed column 1.
        let parent_schema = SchemaDescriptor::new(
            &[crate::schema::SchemaColumn::new(crate::schema::type_code::U64, 0); 2],
            &[0],
        );
        let mut tbl = make_test_table("flush_ic_parent");
        dag.register_table(
            70,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            parent_schema,
            RelationKind::BaseTable,
            0,
            String::new(),
        );

        // Durable index table: flush writes shard_*.db only if called.
        let idx_schema = crate::schema::SchemaDescriptor::minimal_u64();
        let idx_dir = dag_test_dir("flush_ic_idx");
        let _ = std::fs::remove_dir_all(&idx_dir);
        let idx_tbl = Box::new(Table::new(&idx_dir, idx_schema, 1, 256 * 1024, RecoverySource::SalReplay).unwrap());
        dag.add_index_circuit(70, &[1], 999, idx_tbl, idx_schema, false);

        // Put one row in the index table's memtable.
        {
            let entry = dag.tables.get_mut(&70).unwrap();
            let mut batch = Batch::with_capacity(idx_schema, 1);
            batch.extend_pk(1u128);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.count += 1;
            entry.index_circuits[0].table_mut().ingest_owned_batch(batch).unwrap();
        }

        // With the fix, flush propagates to index circuits and writes a shard.
        // Without the fix, index circuits are skipped and no shard is written.
        dag.flush(70).unwrap();
        let shard_count = std::fs::read_dir(&idx_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().unwrap_or("").starts_with("shard_"))
            .count();
        assert!(shard_count > 0, "index circuit shard must be written by flush");

        dag.close();
        let _ = std::fs::remove_dir_all(dag_test_dir("flush_ic_parent"));
        let _ = std::fs::remove_dir_all(idx_dir);
    }

    // R2 regression: a DELETE/UPDATE retraction on a *signed* (I64) PK must
    // actually retract the stored row. The pre-fix narrow arm fed `get_pk`
    // (OPK-widened, sign-flipped) to `retract_pk(u128)`, which re-OPK-encoded it
    // (a second sign flip) so the probe matched no stored row and the retraction
    // was silently dropped. The byte path keys on verbatim OPK and is correct.
    #[test]
    fn test_enforce_unique_pk_signed_negative_retraction() {
        use crate::schema::{type_code, SchemaColumn};
        raise_fd_limit_for_tests();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0), // signed PK
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0],
        );
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_signed");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            1234,
            crate::storage::Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        // Seed the store with a negative-PK row (PK=-5, payload=100).
        let mut seed = Batch::with_capacity(schema, 1);
        seed.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        seed.extend_weight(&1i64.to_le_bytes());
        seed.extend_null_bmp(&0u64.to_le_bytes());
        seed.extend_col(0, &100i64.to_le_bytes());
        seed.count += 1;
        pt.ingest_owned_batch(seed).unwrap();
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
        assert!(pt.has_pk_bytes(&opk), "seed row must be present");

        // DELETE PK=-5: a -1 retraction batch.
        let mut del = Batch::with_capacity(schema, 1);
        del.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        del.extend_weight(&(-1i64).to_le_bytes());
        del.extend_null_bmp(&0u64.to_le_bytes());
        del.extend_col(0, &100i64.to_le_bytes());
        del.count += 1;

        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, del);

        // The store row must have been *found*: the effective batch carries a
        // single net -1 for PK=-5 with the stored payload. (`retract_pk_bytes` is
        // read-only — the store row is removed when the effective batch is
        // re-ingested, mirroring the real DML pipeline below.)
        assert_eq!(effective.count, 1, "one net retraction row expected");
        assert_eq!(effective.get_weight(0), -1, "effective weight must be -1");
        assert_eq!(effective.get_pk_bytes(0), &opk[..], "retraction PK must be OPK(-5)");

        // Re-ingest the effective batch as the DML pipeline does; the stored
        // negative-PK row then nets to zero and is gone.
        pt.ingest_owned_batch(effective).unwrap();
        assert!(
            !pt.has_pk_bytes(&opk),
            "stored negative-PK row must be gone after retraction"
        );
    }

    // Base-table contract: per-PK accumulated weight ∈ {0, 1}. A pushed row at
    // |w| > 1 is the row repeated; retract-before-insert collapses repeats to
    // one live instance, so the effective batch must carry unit weights —
    // otherwise a weight-2 PK row lands (two live instances the -1-normalized
    // retraction arm can never fully delete) and the CREATE UNIQUE INDEX PK
    // short-circuit's premise breaks.
    #[test]
    fn test_enforce_unique_pk_weight_normalized() {
        use crate::schema::{type_code, SchemaColumn};
        raise_fd_limit_for_tests();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // PK
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0],
        );
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_weight_norm");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            1234,
            crate::storage::Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        let row_pk1 = |payload: i64, weight: i64| {
            let mut b = Batch::with_capacity(schema, 1);
            b.extend_pk_opk(&schema, &[1u128]);
            b.extend_weight(&weight.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
            b
        };
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&1u64.to_le_bytes(), type_code::U64, &mut opk);

        // Case 1 — fresh insert at weight 2: one effective row at weight 1.
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, row_pk1(100, 2));
        assert_eq!(effective.count, 1, "one effective insert row expected");
        assert_eq!(effective.get_weight(0), 1, "insert weight must be normalized to 1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(pt.has_pk_bytes(&opk), "row must be live after the clamped insert");

        // Case 2 — upsert at weight 2 over the committed row: retraction of the
        // stored payload at -1, then the new payload at 1.
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, row_pk1(200, 2));
        assert_eq!(effective.count, 2, "stored retraction + new insert expected");
        assert_eq!(effective.get_weight(0), -1, "stored-row retraction must be -1");
        assert_eq!(effective.get_weight(1), 1, "upsert weight must be normalized to 1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(pt.has_pk_bytes(&opk), "row must be live after the upsert");

        // Case 3 — DELETE at weight -3: one -1 retraction; re-ingest nets the
        // store to exactly zero (no ghost weight survives, no negative net).
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, row_pk1(200, -3));
        assert_eq!(effective.count, 1, "one net retraction row expected");
        assert_eq!(
            effective.get_weight(0),
            -1,
            "retraction weight must be normalized to -1"
        );
        pt.ingest_owned_batch(effective).unwrap();
        assert!(!pt.has_pk_bytes(&opk), "row must be fully gone after the delete");
    }

    // Positivity regression: a retraction of a key that is absent (never inserted)
    // or tombstoned (inserted then removed) must NOT pass a negative-weight phantom
    // row through to the store. Pre-fix the `else if !found` arm appended the raw
    // `(-1, filler)` row, leaving a base table at net weight -1.
    #[test]
    fn test_enforce_unique_pk_absent_key_drops_phantom() {
        use crate::schema::{type_code, SchemaColumn};
        raise_fd_limit_for_tests();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0), // signed PK
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0],
        );
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_absent");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            1234,
            crate::storage::Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        // Seed an unrelated row (PK=-5) so the store is non-empty.
        let mut seed = Batch::with_capacity(schema, 1);
        seed.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        seed.extend_weight(&1i64.to_le_bytes());
        seed.extend_null_bmp(&0u64.to_le_bytes());
        seed.extend_col(0, &100i64.to_le_bytes());
        seed.count += 1;
        pt.ingest_owned_batch(seed).unwrap();

        let retract_pk7 = || {
            let mut del = Batch::with_capacity(schema, 1);
            del.extend_pk_opk(&schema, &[7u128]);
            del.extend_weight(&(-1i64).to_le_bytes());
            del.extend_null_bmp(&0u64.to_le_bytes());
            del.extend_col(0, &0i64.to_le_bytes());
            del.count += 1;
            del
        };

        // Case 1 — absent key: PK=7 was never inserted. The phantom pass-through
        // is dropped, so the effective batch is empty.
        let effective = DagEngine::enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(effective.count, 0, "absent-key retraction must emit no phantom row");

        // Case 2 — tombstoned key: insert PK=7, retract to net zero, then retract
        // a second time. The first retraction finds the stored row (count 1); the
        // second finds nothing and emits no phantom.
        let mut ins = Batch::with_capacity(schema, 1);
        ins.extend_pk_opk(&schema, &[7u128]);
        ins.extend_weight(&1i64.to_le_bytes());
        ins.extend_null_bmp(&0u64.to_le_bytes());
        ins.extend_col(0, &42i64.to_le_bytes());
        ins.count += 1;
        pt.ingest_owned_batch(ins).unwrap();

        let eff1 = DagEngine::enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(
            eff1.count, 1,
            "retracting a present key emits the stored-row retraction"
        );
        pt.ingest_owned_batch(eff1).unwrap(); // PK=7 now nets to zero (tombstoned)

        let eff2 = DagEngine::enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(eff2.count, 0, "tombstoned-key retraction must emit no phantom row");
    }

    // Wide-PK enforcement: a >16-byte compound PK must key the intra-batch
    // `seen` map, the store probe, and the emitted retraction on its full OPK
    // bytes. The other enforce_unique_pk unit tests use narrow single-column
    // PKs, so this is the only coverage of `&[u8]` slice keying at pk_stride > 16.
    #[test]
    fn test_enforce_unique_pk_wide_pk() {
        use crate::test_support::{opk_pk, wide_pk_3xu64_schema, wide_row};
        raise_fd_limit_for_tests();

        let schema = wide_pk_3xu64_schema();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_wide");
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            555,
            crate::storage::Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        let pk24 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);

        // Seed the store with K=(1,2,3) → 100.
        let eff = DagEngine::enforce_unique_pk(&mut pt, &schema, wide_row(&schema, &pk24(1, 2, 3), 1, 100));
        assert_eq!(eff.count, 1, "fresh wide-PK insert is a single +1 row");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(1, 2, 3)), "seed row must be live");

        // Cross-batch upsert: a +1 on the same wide PK retracts the stored
        // payload (keyed and emitted on the full 24 bytes) and inserts the new.
        let eff = DagEngine::enforce_unique_pk(&mut pt, &schema, wide_row(&schema, &pk24(1, 2, 3), 1, 200));
        assert_eq!(eff.count, 2, "stored-row retraction + new insert");
        assert_eq!(eff.get_weight(0), -1, "stored retraction at -1");
        assert_eq!(
            eff.get_pk_bytes(0),
            &pk24(1, 2, 3)[..],
            "retraction keys on the full 24-byte PK"
        );
        assert_eq!(eff.get_weight(1), 1, "new insert at +1");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(1, 2, 3)), "row stays live after the upsert");

        // Intra-batch +1, -1, +1 on a fresh wide PK K2=(7,8,9): the delete must
        // clear the `seen` entry so the re-insert is not re-negated. Net +1.
        let mut b = Batch::with_capacity(schema, 3);
        for (payload, w) in [(10i64, 1i64), (10, -1), (20, 1)] {
            b.extend_pk_bytes(&pk24(7, 8, 9));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
        }
        let eff = DagEngine::enforce_unique_pk(&mut pt, &schema, b);
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(7, 8, 9)), "K2 survives +1,-1,+1 at net +1");
    }

    /// `StoreHandle::Partitioned` must dispatch `recovery_lsn` → the table's
    /// `min_flushed_lsn` (recovery watermark) and `current_lsn` → its `current_lsn`
    /// (the LSN-allocator max). Built on the partial-flush fixture where the two
    /// diverge, so a swapped dispatch is caught. (The underlying min/max
    /// aggregation is pinned storage-side by
    /// `min_flushed_lsn_floors_recovery_watermark_after_partial_flush`.)
    #[test]
    fn store_handle_partitioned_lsn_dispatch() {
        let f = crate::storage::partial_flush_lsn_fixture();
        let (recovery, current) = (f.recovery_lsn, f.current_lsn);
        assert!(recovery < current, "fixture must have min < max to distinguish the two");

        let handle = StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(f.pt)));
        assert_eq!(
            handle.recovery_lsn(),
            recovery,
            "Partitioned recovery_lsn → min_flushed_lsn"
        );
        assert_eq!(
            handle.current_lsn(),
            current,
            "Partitioned current_lsn → max current_lsn"
        );
    }

    // A storage error while applying committed data in `ingest_store_and_indices`
    // must _exit(134) (fail-stop; recovery is restart + SAL replay). Driven via
    // the `GNITZ_INJECT_INGEST_APPLY_ERROR` debug seam. The `index`-stage
    // variant is exercised end-to-end by the
    // `test_ingest_apply_error_aborts_and_replays` e2e test.
    #[test]
    fn test_ingest_apply_error_abort_exit_status() {
        crate::test_support::assert_test_aborts_134(
            "ingest_apply_error_abort_internal",
            &[
                ("GNITZ_RUN_INGEST_ABORT_TEST", "1"),
                ("GNITZ_INJECT_INGEST_APPLY_ERROR", "store"),
            ],
        );
    }

    // Guard: runs the seam-armed ingest only under GNITZ_RUN_INGEST_ABORT_TEST
    // (set by the parent). Registers a view and ingests one row; the armed
    // "store" seam substitutes Err for the store ingest, tripping the abort.
    // `View`, not `BaseTable`: a base table must be a `Partitioned` handle (it
    // runs `enforce_unique_pk`), and this fixture holds a `Borrowed` one.
    #[test]
    fn ingest_apply_error_abort_internal() {
        if std::env::var("GNITZ_RUN_INGEST_ABORT_TEST").is_err() {
            return;
        }
        let mut dag = DagEngine::new();
        let schema = crate::schema::SchemaDescriptor::minimal_u64();
        let dir = dag_test_dir("seam_abort");
        let _ = std::fs::remove_dir_all(&dir);
        let mut tbl = Box::new(Table::new(&dir, schema, 99, 256 * 1024, RecoverySource::Rederive).unwrap());
        dag.register_table(
            70,
            StoreHandle::Borrowed(&mut *tbl as *mut Table),
            schema,
            RelationKind::View,
            0,
            String::new(),
        );
        let mut batch = Batch::with_capacity(schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;
        dag.ingest_relation(70, batch);
        unreachable!("ingest_store_and_indices must abort when the seam is armed");
    }

    // ── ViewMeta relay routing ───────────────────────────────────────────────

    /// `ViewMeta::from_loaded` must derive a pure-range join's relay routing from
    /// the circuit alone: a memo miss that read an empty circuit would answer
    /// `range_join_n_eq = None` + empty `shard_cols`, silently dropping the
    /// `op_relay_broadcast` a pure-range join's input relay needs (n_eq == 0 ⇒
    /// broadcast, not scatter), losing rows.
    #[test]
    fn view_meta_from_loaded_carries_range_join_relay_routing() {
        use gnitz_wire::{JoinKind, OpNode, RangeRel};

        // A pure-range join: ScanDelta → ExchangeShard → Join(DeltaTraceRange
        // { n_eq: 0 }) → IntegrateSink.
        let mut nodes: std::collections::HashMap<i32, OpNode> = std::collections::HashMap::new();
        nodes.insert(0, OpNode::ScanDelta { source: 7, bound: None });
        nodes.insert(1, OpNode::ExchangeShard { shard_cols: vec![1] });
        nodes.insert(
            2,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 0,
                rel: RangeRel::Lt,
            }),
        );
        nodes.insert(3, OpNode::IntegrateSink);
        let edges = vec![(0, 1, 0), (1, 2, 0), (2, 3, 0)];

        let loaded = compiler::loaded_for_test(nodes, edges);

        let meta = ViewMeta::from_loaded(&loaded);
        assert_eq!(
            meta.range_join_n_eq,
            Some(0),
            "a pure-range join must be discriminated so the relay broadcasts instead of scattering"
        );
        assert_eq!(
            meta.shard_cols.as_deref(),
            Some(&[1][..]),
            "the output ExchangeShard's shard cols must survive"
        );
        assert!(meta.has_join);
    }
}
