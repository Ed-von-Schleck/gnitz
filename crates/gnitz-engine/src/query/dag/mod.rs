//! DagEngine: the relation registry, plan cache, and compilation entry point.
//! Epoch execution lives in `exec`, ingestion in `ingest`, and the plan-free
//! view metadata (dependency map + `ViewMeta`) in `meta`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::UnsafeCell;
use std::rc::Rc;

use crate::ops;
use crate::query::compiler::{self, CompileOutput, SubPlan, SysTableRefs};
use crate::query::vm;
use crate::schema::{Placement, SchemaDescriptor};
use crate::storage::{Batch, RecoverySource, StorageError, Table};
use gnitz_wire::PkColList;

mod exec;
mod hydrate;
mod ingest;
mod meta;
mod store_handle;

use meta::DepMap;
pub use meta::RelayRoute;
pub(crate) use meta::ViewMeta;

pub(crate) use store_handle::StoreHandle;

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
    pub(crate) index_id: i64,
    pub(crate) index_table: UnsafeCell<Box<Table>>,
    pub index_schema: SchemaDescriptor,
    /// Full-arity span-encode plan, precomputed at registration so the per-push
    /// consumers do no per-call spec rebuild. It survives every column ALTER of
    /// the owner: an index schema folds all its columns into the PK, so it has
    /// no payload columns, and a trailing append moves no existing column's OPK
    /// offset or payload slot. Deliberately does NOT bake in `is_unique` (live
    /// promotion/demotion via `set_index_circuit_uniqueness`); consumers filter
    /// on the live flag.
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

/// What a top-level relation *is*. Bundling every per-kind property here is what
/// makes the nonsense combinations unconstructable: a durable relation that also
/// rebuilds from source (double count), an ephemeral one that never rebuilds
/// (permanently empty).
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
    /// Ingestion point: storeless, partitioned, append-only. Pushed rows exist
    /// only as the deltas they produce; nothing is retained, so there is nothing
    /// to recover and no store to read.
    Stream,
}

impl RelationKind {
    /// Which kind a `TABLE_TAB.flags` word describes — the one read of the
    /// `stream` bit. `Placement::from_table_flags` is the peer decoder over the
    /// same word, homed with `Placement` in `schema`; they stay apart because
    /// only one of this one's two callers wants a placement. VIEW_TAB rows do
    /// not come through here.
    #[inline]
    pub fn from_table_flags(flags: u64) -> RelationKind {
        if gnitz_wire::TableProps::from_flags(flags).stream {
            RelationKind::Stream
        } else {
            RelationKind::BaseTable
        }
    }

    /// What to call this relation in a message to the user.
    #[inline]
    pub fn noun(self) -> &'static str {
        match self {
            RelationKind::SystemCatalog | RelationKind::BaseTable => "table",
            RelationKind::View => "view",
            RelationKind::Stream => "stream",
        }
    }

    /// True iff this kind has a store, and so a directory, on a process that owns
    /// stores. A stream holds no rows: its definition rides on the system tables
    /// like any other row, and there is nothing to recover.
    #[inline]
    pub fn owns_store(self) -> bool {
        !matches!(self, RelationKind::Stream)
    }

    /// True iff this is a user base table. Gates what only base tables do:
    /// run `enforce_unique_pk` on ingest, and own secondary index circuits
    /// (index projection runs only on the base-table DML paths).
    #[inline]
    pub fn is_base_table(self) -> bool {
        matches!(self, RelationKind::BaseTable)
    }

    /// True iff this relation's rows arrive from a client push rather than being
    /// derived by a circuit.
    #[inline]
    pub fn is_ingestion_point(self) -> bool {
        matches!(self, RelationKind::BaseTable | RelationKind::Stream)
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

/// The `WITH (…)` byte budgets a view can carry. They travel as one value
/// because every path that opens a relation's store must carry both: a
/// `delta_bytes` that reached the catalog but not the store builder would leave
/// a view the catalog calls fed with no delta store on any worker, on every
/// boot, with no error anywhere.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(crate) struct ViewBudgets {
    /// `WITH (capacity = …)`, in bytes; `None` for every unbounded relation.
    pub capacity_bytes: Option<u64>,
    /// `WITH (delta = …)`, in bytes; `None` for every relation with no feed.
    pub delta_bytes: Option<u64>,
}

/// A fed view's **delta store**: the second store beside its output store,
/// holding the deltas the view emitted, each row stamped with the tick round
/// that produced it.
///
/// Held behind a `Box` wherever a relation owns one, because it embeds a
/// `SchemaDescriptor` — 360 bytes, pinned, and paid for by every relation in the
/// registry if this sat inline in [`TableEntry`], fed or not.
pub(crate) struct DeltaFeed {
    /// The derived `_tick ‖ view PK ‖ view payload` schema
    /// ([`crate::schema::make_delta_schema`]). Held rather than re-derived per
    /// read: it is a pure function of the view's schema, and a read needs it to
    /// open the cursor and to guard the client's authored reply schema.
    pub schema: SchemaDescriptor,
    pub handle: StoreHandle,
}

impl DeltaFeed {
    /// This feed's rows over `[start, end]`, in its own derived schema. There is
    /// no unbounded spelling because every delta read is a `(after_tick, cut]`
    /// range — and no caller-supplied schema, because the schema a cursor opens
    /// under is never the caller's to choose.
    pub(crate) fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.handle.open_cursor_in_range(&self.schema, start, end)
    }
}

/// The stores one relation owns on this process: its own, plus the delta store
/// of a fed view. `build_relation_store` returns both, so no path can open one
/// and forget the other.
pub(crate) struct RelationStores {
    pub handle: StoreHandle,
    pub delta: Option<Box<DeltaFeed>>,
}

/// The registry answers the compiler's schema lookups in place — the compiler
/// names only its own trait, so this is the one edge from `dag` down to it.
impl compiler::SchemaSource for FxHashMap<i64, TableEntry> {
    fn schema_of(&self, tid: i64) -> Option<SchemaDescriptor> {
        self.get(&tid).map(|te| te.schema)
    }
}

pub(crate) struct TableEntry {
    pub handle: StoreHandle,
    /// The delta store, when this process holds one — `None` on the post-fork
    /// master, which holds no user store at all, and for every relation with no
    /// feed. `delta_bytes` is what says a feed *exists*; this is what says this
    /// process can read it.
    pub delta: Option<Box<DeltaFeed>>,
    pub schema: SchemaDescriptor,
    pub kind: RelationKind,
    pub depth: i32,
    pub directory: String,
    pub index_circuits: Vec<IndexCircuitEntry>,
    /// `CREATE VIEW … WITH (capacity = …)` in bytes; `None` for every other
    /// relation. The registry's copy: the resolve reply's bounded flag and the
    /// engine-side leaf rule run on the post-fork master, where every user
    /// relation is `StoreHandle::Detached` and there is no `Table` to ask.
    /// `build_relation_store` stamps it onto each store it opens.
    pub capacity_bytes: Option<u64>,
    /// `CREATE VIEW … WITH (delta = …)` in bytes; `None` for every other
    /// relation. The registry's copy, for the same reason `capacity_bytes` keeps
    /// one: the resolve reply's `delta` flag is answered on the post-fork master.
    pub delta_bytes: Option<u64>,
}

impl TableEntry {
    /// A registry entry with no index circuits yet. Both budgets are `None` for
    /// everything but a bounded or a fed view.
    pub(crate) fn new(
        stores: RelationStores,
        schema: SchemaDescriptor,
        kind: RelationKind,
        depth: i32,
        directory: String,
        budgets: ViewBudgets,
    ) -> Self {
        TableEntry {
            handle: stores.handle,
            delta: stores.delta,
            schema,
            kind,
            depth,
            directory,
            index_circuits: Vec::new(),
            capacity_bytes: budgets.capacity_bytes,
            delta_bytes: budgets.delta_bytes,
        }
    }

    /// The budgets this relation was registered with — what a rehome or a rebuild
    /// must hand back to `build_relation_store` so neither store comes back
    /// unbounded or missing.
    pub(crate) fn budgets(&self) -> ViewBudgets {
        ViewBudgets {
            capacity_bytes: self.capacity_bytes,
            delta_bytes: self.delta_bytes,
        }
    }

    /// What the client is told this relation is. Lives beside the two fields that
    /// decide it, so a new [`RelationKind`] is a compile error here rather than a
    /// silent `Table` at the wire boundary. A bounded view is its own wire class:
    /// the client's leaf rule refuses to bind one inside a view body, and
    /// `ALTER VIEW … AS` refuses to retarget it.
    pub(crate) fn class(&self) -> gnitz_wire::RelClass {
        use gnitz_wire::RelClass;
        match self.kind {
            RelationKind::Stream => RelClass::Stream,
            RelationKind::View if self.capacity_bytes.is_some() => RelClass::BoundedView,
            RelationKind::View => RelClass::View,
            RelationKind::BaseTable | RelationKind::SystemCatalog => RelClass::Table,
        }
    }

    /// Non-compacting cursor over this relation's store. The entry's own schema
    /// is what a detached handle opens empty in, so reading through here is what
    /// keeps that answer in the relation's shape without the handle holding a
    /// second copy of the descriptor to keep in step across an ALTER.
    pub(crate) fn open_cursor(&self) -> crate::storage::ReadCursor {
        self.handle.open_cursor(&self.schema)
    }

    /// This relation's rows over `[start, end]` only — see
    /// [`Table::open_cursor_in_range`](crate::storage::Table::open_cursor_in_range).
    pub(crate) fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.handle.open_cursor_in_range(&self.schema, start, end)
    }

    /// Materialize every positive-weight row of this relation's store.
    pub(crate) fn full_scan(&self) -> std::rc::Rc<Batch> {
        self.handle.full_scan(&self.schema)
    }

    /// Whether reads of this relation must go through hydration — i.e. whether its
    /// store actually holds a skeleton row. This, not `capacity_bytes`, is what
    /// every read path branches on: a bounded view under its cap has dehydrated
    /// nothing and reads exactly like an unbounded one, so it keeps the ordinary
    /// cached/bulk read paths, and the branch cannot disagree with what the
    /// capacity sweep did.
    pub(crate) fn needs_hydration(&self) -> bool {
        self.handle.has_skeleton_rows()
    }

    /// The index circuit covering exactly `cols`, if one exists. The circuit
    /// list is deduped by ordered column list, so at most one entry matches.
    pub(crate) fn index_circuit_on(&self, cols: &[u32]) -> Option<&IndexCircuitEntry> {
        self.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols)
    }

    fn index_circuit_on_mut(&mut self, cols: &[u32]) -> Option<&mut IndexCircuitEntry> {
        self.index_circuits
            .iter_mut()
            .find(|ic| ic.col_indices.as_slice() == cols)
    }
}

// ---------------------------------------------------------------------------
// ExchangeCallback — trait for multi-worker exchange IPC
// ---------------------------------------------------------------------------

/// How a multi-worker operator reaches the other workers.
///
/// Given this worker's pre-exchange output for `view_id`, return the batch this
/// worker owns once every worker's output has been repartitioned. The transport
/// is entirely the implementor's: this crate computes, and never names a channel.
///
/// That is what lets the whole exchange path sit below the process model —
/// `gnitz-server` realizes it by sending to the master over W2M and receiving
/// the relay over the SAL, and a single-process embedder implements it as the
/// identity.
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

impl DagEngine {
    pub(crate) fn new() -> Self {
        DagEngine {
            cache: FxHashMap::default(),
            dep: DepMap::default(),
            meta: FxHashMap::default(),
            tables: FxHashMap::default(),
            sys: SysTableRefs::null(),
        }
    }

    // ── System table setup ──────────────────────────────────────────────

    pub(crate) fn set_sys_tables(&mut self, sys: SysTableRefs) {
        self.sys = sys;
    }

    // ── Table registry ──────────────────────────────────────────────────

    /// Enter a relation in the registry. `index_circuits` starts empty and is
    /// filled by `add_index_circuit`.
    pub(crate) fn register_table(&mut self, table_id: i64, entry: TableEntry) {
        self.tables.insert(table_id, entry);
    }

    pub(crate) fn unregister_table(&mut self, table_id: i64) {
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
    /// Sound: consumers resolve the circuit (and open their cursors) per
    /// request, and the swap runs before the worker serves anything.
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

    /// Publish a new column schema for a registered base table in place (any
    /// column ALTER). Three of the four are equal-region — RENAME COLUMN, DROP
    /// COLUMN and DROP NOT NULL change only a column's name, `is_hidden` or
    /// `is_nullable`, the last of which downgrades the whole-schema payload
    /// comparator `FixedIntNonnull → Generic` — and move no bytes at all. ADD
    /// COLUMN grows the region count: resident runs are widened with a NULL tail
    /// and every registered shard is re-opened so it can pad the appended
    /// columns (`MappedShard::null_pad_mask`).
    ///
    /// Updates the registry copy (`TableEntry.schema`) and pushes the same value
    /// down into the owned `Table` (memtable, RAM tier, shard index), plus
    /// dropping `Table::cached_full_scan`.
    pub(crate) fn swap_table_schema(&mut self, table_id: i64, schema: SchemaDescriptor) -> Result<(), String> {
        let entry = self
            .tables
            .get_mut(&table_id)
            .expect("swap_table_schema: table must be registered");
        // A worker reaches its `Table`; the post-fork master has none.
        if let Some(store) = entry.handle.as_owned_mut() {
            store
                .swap_schema(schema)
                .map_err(|e| format!("ALTER on table {table_id}: reopening shards failed: {e}"))?;
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
        Ok(())
    }

    // ── Cache management ────────────────────────────────────────────────

    /// Drop one view's cached plan + memoized view metadata, leaving it
    /// registered. The recovery output reset needs this: the next backfill must
    /// recompile the view against its freshly-emptied store and scratch, not
    /// against the plan that still holds the old ones open.
    pub fn invalidate(&mut self, view_id: i64) {
        self.cache.remove(&view_id);
        self.evict_meta(view_id);
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

    /// Ensure a view's plan is compiled. `Ok(false)` means `view_id` is not a
    /// registered relation; `Err` means a registered view did not compile.
    ///
    /// The error is `String` rather than `CompileError`, which is `pub(crate)`
    /// and so cannot appear in a `pub fn`'s signature.
    pub fn ensure_compiled(&mut self, view_id: i64) -> Result<bool, String> {
        if self.cache.contains_key(&view_id) {
            return Ok(true);
        }
        match self.compile_view_internal(view_id)? {
            Some(compiler::CompiledView { output, facts }) => {
                // The compile already walked this circuit, so seed the memo from
                // what it derived rather than let the first metadata touch read
                // the same three system tables again.
                self.meta.insert(view_id, Rc::new(ViewMeta::from_facts(facts)));
                self.cache.insert(view_id, output);
                Ok(true)
            }
            None => Ok(false),
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

    /// Read `view_id`'s circuit out of the system tables and compile it, homing
    /// every scratch child under `dir`. The directory is a parameter and not read
    /// off `entry` because the pre-flight compiles into a throwaway root.
    ///
    /// The scratch children open under the policy `entry`'s own output store was
    /// opened with, so a view's operator traces cannot end up looking for a
    /// different manifest generation than its output. An entry holding no owned
    /// store — the post-fork master's, and the pre-flight's — resumes nothing,
    /// which is what a compile into a directory with no manifest concludes anyway.
    fn compile_circuit(
        &self,
        view_id: i64,
        dir: &str,
        entry: &TableEntry,
    ) -> Result<compiler::CompiledView, compiler::CompileError> {
        let site = compiler::ViewSite {
            dir,
            id: view_id as u64,
            recovery: entry
                .handle
                .as_owned()
                .map_or(RecoverySource::Rederive { resume_at: None }, Table::recovery_source),
        };
        // The compiler layer sees only the circuit system tables, never `VIEW_TAB`,
        // so it cannot derive whether the view is capacity-bounded.
        unsafe {
            compiler::compile_view(
                site,
                self.sys,
                &entry.schema,
                &self.tables,
                entry.capacity_bytes.is_some(),
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
    /// as the scratch path component (which `root` overrides), as `WorkerFilter`
    /// and `ReducePlan` operands baked into instructions nothing here executes,
    /// and — since a `WorkerFilter` emits no instruction at `W == 1` — as
    /// instruction *offsets*, which only a bounded view's hydration plan reads and
    /// no rejection depends on.
    pub(crate) fn preflight_compile(&self, view_id: i64, root: &str) -> Result<(), compiler::CompileError> {
        // `hook_relation_register` ran earlier in this bundle's ingest loop, so a
        // registered `+1` VIEW_TAB row is always in `tables`; a miss is an engine
        // bug, surfaced as a DDL rejection rather than an unchecked compile.
        let Some(entry) = self.tables.get(&view_id) else {
            return Err(compiler::CompileError::Rejected("pre-flight: view is not registered"));
        };
        // `map(drop)` closes the plan — and the `Table`s it holds open under
        // `root` — before the directory is removed.
        let verdict = self.compile_circuit(view_id, root, entry).map(drop);
        let _ = std::fs::remove_dir_all(root);
        verdict
    }

    /// Compile a view by reading system tables and calling `compiler::compile_view`.
    ///
    /// `Ok(None)` means only "not a registered relation". Nothing re-pre-flights
    /// the circuit here — replay, fork inheritance, checkpoint resume, rebuild and
    /// relayout all reach this with no pre-flight in the process, and this compile
    /// additionally opens resumed operator state the master's throwaway root never
    /// had. An `Err` is therefore unrecoverable for a server: the alternative to
    /// aborting is a view that has stopped integrating while still answering reads
    /// with stale rows. The message names the causes.
    fn compile_view_internal(&self, view_id: i64) -> Result<Option<compiler::CompiledView>, String> {
        let Some(entry) = self.tables.get(&view_id) else {
            return Ok(None);
        };
        match self.compile_circuit(view_id, &entry.directory, entry) {
            Ok(output) => {
                gnitz_debug!("dag: compiled view_id={}", view_id);
                Ok(Some(output))
            }
            Err(err) => Err(format!(
                "view_id={view_id} does not compile from its durable circuit — this build no \
                 longer accepts that circuit or its expr blobs, its derived state is \
                 corrupt or unreadable, or resources are exhausted: {err}"
            )),
        }
    }

    /// Close the DagEngine, dropping all cached plans. Reached only through
    /// `CatalogEngine::close`, which the server never calls — it flushes durably
    /// per zone and exits via abort or process teardown.
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
#[path = "tests/dag.rs"]
mod tests;
