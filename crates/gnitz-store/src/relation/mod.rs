//! L4 relation registry — what relations this process holds, and the stores
//! behind them.
//!
//! The registry is the state the DBSP layer *reads*: it owns each relation's
//! schema, kind, directory, store handle, secondary-index circuits and delta
//! feed, plus the two resume words (`resume_generation`, `recorded_topology`)
//! every rederived store opens against. Nothing here compiles a circuit or runs
//! an epoch — `query` does that, and reaches this rung by taking a registry
//! reference as a parameter.
//!
//! Two hosts drive one registry: `CatalogEngine` (from a TABLE_TAB / VIEW_TAB
//! row) and `gnitz-mirror` (from its own record file). Both enter a relation
//! through [`RelationRegistry::register`], so neither can drift on the child
//! address, the recovery source, the capacity stamp or whether a delta store is
//! opened.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover.

use rustc_hash::FxHashMap;

use crate::schema::SchemaDescriptor;

/// The id at or above which every relation is a user relation. `gnitz-wire`
/// declares it; the signed form is what a registry id is.
pub(crate) const FIRST_USER_TABLE_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;
use crate::storage::{Batch, ChildAddr, RecoverySource, StorageError, Table};
use gnitz_wire::PkColList;

mod build;
mod dirs;
mod ingest;
mod store_handle;
mod store_lsn;

pub use dirs::{ensure_dir, is_table_dir_name, lock_data_dir, relation_dir, staged_dir};
pub use ingest::IngestError;
pub(crate) use store_handle::StoreHandle;

/// Default rows per `drain_chunk` call on a chunked scan. Bounds peak scan
/// memory at O(chunk × row_width).
pub const DDL_SCAN_CHUNK_ROWS: usize = 65_536;

/// Default per-worker distinct-group cap for the ad-hoc aggregate fold.
pub(crate) const ADHOC_GROUP_CAP: usize = 65_536;

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
    pub(crate) handle: StoreHandle,
    pub index_schema: SchemaDescriptor,
    /// Full-arity span-encode plan, precomputed at registration so the per-push
    /// consumers do no per-call spec rebuild. It survives every column ALTER of
    /// the owner — the `debug_assert!` in
    /// [`RelationRegistry::swap_table_schema`] is the tripwire on that.
    /// Deliberately does NOT bake in `is_unique` (live promotion/demotion via
    /// `set_index_circuit_uniqueness`); consumers filter on the live flag.
    pub key_spec: crate::schema::IndexKeySpec,
    pub is_unique: bool,
}

impl IndexCircuitEntry {
    /// Non-compacting cursor over this circuit's index table, in the index
    /// schema — a detached circuit (the post-fork master's) opens empty.
    pub fn open_cursor(&self) -> crate::storage::ReadCursor {
        self.handle.open_cursor(&self.index_schema)
    }

    /// [`Self::open_cursor`] over `[start, end]` only.
    pub fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.handle.open_cursor_in_range(&self.index_schema, start, end)
    }

    /// Ingest an owned batch of index rows. The projection path drives this per
    /// push; a detached circuit absorbs nothing.
    pub fn ingest_owned_batch(&self, batch: Batch) -> Result<(), StorageError> {
        self.handle.ingest_owned_batch(batch)
    }
}

// ---------------------------------------------------------------------------
// Relation kind — what a top-level relation *is*
// ---------------------------------------------------------------------------

/// What a top-level relation *is*.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelationKind {
    /// System catalog table: durable, single-partition, never rebuilt from
    /// upstream sources (it has none; recovery is LSN-gated SAL replay).
    SystemCatalog,
    /// User base table: durable, partitioned, never rebuilt from upstream
    /// sources; owns a DML-enforced PK, so `enforce_unique_pk` runs on every
    /// ingest and its accumulated per-PK weight is always in {0, 1}.
    BaseTable,
    /// Materialised view: ephemeral, rebuilt from its sources, and resumed from
    /// a generation-stamped manifest when the ephemeral checkpoint round left
    /// one. Partitioned.
    View,
    /// Ingestion point: storeless, partitioned, append-only. Pushed rows exist
    /// only as the deltas they produce; nothing is retained, so there is nothing
    /// to recover and no store to read.
    Stream,
}

impl RelationKind {
    /// What to call this relation in a message to the user.
    #[inline]
    pub fn noun(self) -> &'static str {
        match self {
            RelationKind::SystemCatalog | RelationKind::BaseTable => "table",
            RelationKind::View => "view",
            RelationKind::Stream => "stream",
        }
    }

    /// True iff this is a user base table.
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

    /// True iff this is a materialised view.
    #[inline]
    pub fn is_view(self) -> bool {
        matches!(self, RelationKind::View)
    }
}

// ---------------------------------------------------------------------------
// Table entry — per-table metadata in the entity registry
// ---------------------------------------------------------------------------

/// The `WITH (…)` byte budgets a view can carry. One value rather than two
/// arguments, so every path that opens a relation's store carries both or
/// neither.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ViewBudgets {
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
    pub(crate) handle: StoreHandle,
}

impl DeltaFeed {
    /// This feed's rows over `[start, end]`, in its own derived schema — every
    /// delta read is a `(after_tick, cut]` range, so there is no unbounded
    /// spelling.
    pub(crate) fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.handle.open_cursor_in_range(&self.schema, start, end)
    }

    /// The highest tick round this feed's capacity sweep has dropped.
    pub(crate) fn dropped_through(&self) -> u64 {
        self.handle.dropped_through()
    }
}

/// The stores one relation owns on this process: its own, plus the delta store
/// of a fed view. `build_relation_store` returns both, so no path can open one
/// and forget the other.
pub(crate) struct RelationStores {
    pub handle: StoreHandle,
    pub delta: Option<Box<DeltaFeed>>,
}

pub struct TableEntry {
    pub(crate) handle: StoreHandle,
    /// The delta store, when this process holds one — `None` on the post-fork
    /// master, which holds no user store at all, and for every relation with no
    /// feed. `delta_bytes` is what says a feed *exists*; this is what says this
    /// process can read it.
    pub(crate) delta: Option<Box<DeltaFeed>>,
    pub schema: SchemaDescriptor,
    pub kind: RelationKind,
    pub depth: i32,
    pub directory: String,
    pub index_circuits: Vec<IndexCircuitEntry>,
    /// The budgets this relation was registered with: what a rehome or a rebuild
    /// hands back to `build_relation_store`, and the answer the post-fork master
    /// gives when it has no `Table` to ask.
    pub budgets: ViewBudgets,
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
            budgets,
        }
    }

    /// This relation's delta feed, when this process holds one.
    pub(crate) fn delta_feed(&self) -> Option<&DeltaFeed> {
        self.delta.as_deref()
    }

    /// [`Self::delta_feed`] as a `Result` — the one message for "this process
    /// holds no delta store", so the ad-hoc read's two lookups (the source
    /// schema and the cursor) cannot render it two ways.
    pub(crate) fn delta_feed_or_err(&self, id: i64) -> Result<&DeltaFeed, String> {
        self.delta_feed()
            .ok_or_else(|| format!("scan_spec: this process holds no delta store for relation {id}"))
    }

    /// The `Table` this process owns outright for this relation, if any.
    pub fn owned_store(&self) -> Option<&Table> {
        self.handle.as_owned()
    }

    /// True when this process holds no store for this relation at all — a
    /// stream, which holds none anywhere, or the post-fork master, which
    /// detached every user relation. A `Borrowed` system table is not one: its
    /// store is reachable, just owned elsewhere.
    pub fn is_storeless(&self) -> bool {
        self.handle.is_detached()
    }

    /// What the client is told this relation is. Lives beside the two fields that
    /// decide it, so a new [`RelationKind`] is a compile error here rather than a
    /// silent `Table` at the wire boundary. A bounded view is its own wire class:
    /// the client's leaf rule refuses to bind one inside a view body, and
    /// `ALTER VIEW … AS` refuses to retarget it.
    pub fn class(&self) -> gnitz_wire::RelClass {
        use gnitz_wire::RelClass;
        match self.kind {
            RelationKind::Stream => RelClass::Stream,
            RelationKind::View if self.budgets.capacity_bytes.is_some() => RelClass::BoundedView,
            RelationKind::View => RelClass::View,
            RelationKind::BaseTable | RelationKind::SystemCatalog => RelClass::Table,
        }
    }

    /// Non-compacting cursor over this relation's store. The entry's own schema
    /// is what a detached handle opens empty in, so reading through here is what
    /// keeps that answer in the relation's shape without the handle holding a
    /// second copy of the descriptor to keep in step across an ALTER.
    pub fn open_cursor(&self) -> crate::storage::ReadCursor {
        self.handle.open_cursor(&self.schema)
    }

    /// This relation's rows over `[start, end]` only — see
    /// [`Table::open_cursor_in_range`](crate::storage::Table::open_cursor_in_range).
    pub fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.handle.open_cursor_in_range(&self.schema, start, end)
    }

    /// Materialize every positive-weight row of this relation's store.
    pub fn full_scan(&self) -> std::rc::Rc<Batch> {
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
    pub fn index_circuit_on(&self, cols: &[u32]) -> Option<&IndexCircuitEntry> {
        self.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols)
    }

    fn index_circuit_on_mut(&mut self, cols: &[u32]) -> Option<&mut IndexCircuitEntry> {
        self.index_circuits
            .iter_mut()
            .find(|ic| ic.col_indices.as_slice() == cols)
    }

    /// The store's LSN counter — see [`StoreHandle::current_lsn`].
    pub fn current_lsn(&self) -> u64 {
        self.handle.current_lsn()
    }

    /// Durable ingest of a borrowed `Batch` into this relation's own store,
    /// bypassing index projection. The delta-stamp and the checkpoint paths take
    /// it; ordinary ingestion goes through the registry.
    pub fn ingest_borrowed_batch(&self, batch: &Batch) -> Result<(), StorageError> {
        self.handle.ingest_borrowed_batch(batch)
    }

    /// [`Self::ingest_borrowed_batch`] for an owned batch.
    pub fn ingest_owned_batch(&self, batch: Batch) -> Result<(), StorageError> {
        self.handle.ingest_owned_batch(batch)
    }
}

// ---------------------------------------------------------------------------
// The relation to register
// ---------------------------------------------------------------------------

/// Everything [`RelationRegistry::register`] needs to enter a relation and open
/// its store — [`TableEntry::new`]'s parameter list plus the id and minus the
/// stores it is `register`'s job to open, so the two cannot disagree on what a
/// registration carries.
pub struct RelationSpec {
    pub id: i64,
    pub kind: RelationKind,
    pub schema: SchemaDescriptor,
    /// `<root>/<schema_name>/<t|v>_<id>`, from [`relation_dir`]. The parent of
    /// the `ChildAddr` subdir the store itself opens.
    pub directory: String,
    /// Depth in the view dependency chain; 0 for a base table and for any
    /// relation whose rows arrive from outside. Only the DBSP layer computes a
    /// non-zero value.
    pub depth: i32,
    pub budgets: ViewBudgets,
}

// ---------------------------------------------------------------------------
// RelationRegistry
// ---------------------------------------------------------------------------

/// Which relations this process holds, and the stores behind them.
pub struct RelationRegistry {
    pub(crate) tables: FxHashMap<i64, TableEntry>,
    /// Launched worker count. Threaded in rather than read off `worker_ctx`,
    /// which is 1 in the master pre-fork: a store built from the ambient value
    /// is named `w0of1` while the boot repartition wrote `w0of{W}`, and the
    /// child-dir sweep then deletes it as unowned.
    pub(crate) num_workers: u32,
    /// True while this process owns its relations' stores. The post-fork master
    /// detaches every user relation; anything reading local base data checks
    /// this first.
    pub(crate) owns_stores: bool,
    /// The generation a manifest must carry to be resumed from.
    pub(crate) resume_generation: u64,
    /// `worker_count << 32 | STATE_FORMAT` as last recorded.
    pub(crate) recorded_topology: u64,
    /// Rows per `drain_chunk` on a chunked scan; bounds peak scan memory at
    /// O(chunk × row_width). `GNITZ_DDL_SCAN_CHUNK_ROWS` overrides.
    pub(crate) ddl_scan_chunk_rows: usize,
    /// Per-worker distinct-group cap for the ad-hoc aggregate fold.
    /// `GNITZ_ADHOC_GROUP_CAP` overrides.
    pub(crate) adhoc_group_cap: usize,
}

impl RelationRegistry {
    /// An empty registry plus the two chunk/cap knobs read from the environment.
    /// The one constructor: `CatalogEngine::open` calls it with the launched
    /// worker count, `Mirror::open` with 1.
    pub fn new(num_workers: u32) -> Self {
        use crate::foundation::env::env_num;
        RelationRegistry {
            tables: FxHashMap::default(),
            num_workers,
            owns_stores: true,
            resume_generation: 0,
            recorded_topology: 0,
            // Rows per chunk for the chunked DDL scans (view + index backfill).
            // `GNITZ_DDL_SCAN_CHUNK_ROWS` overrides the default — chiefly so
            // multi-worker E2E tests can shrink it to force many chunked backfill
            // rounds (lockstep padding, SAL reclaim) over small tables. A 0 or
            // unparseable value falls back to the default: a zero chunk size
            // drains nothing, so a backfill would never make progress.
            ddl_scan_chunk_rows: env_num("GNITZ_DDL_SCAN_CHUNK_ROWS", DDL_SCAN_CHUNK_ROWS),
            adhoc_group_cap: env_num("GNITZ_ADHOC_GROUP_CAP", ADHOC_GROUP_CAP),
        }
    }

    // ── Table registry ──────────────────────────────────────────────────

    /// Enter a relation over a `Table` the caller keeps owning — the system
    /// families, whose stores the catalog holds in its own array. At depth 0
    /// with no budgets and no index circuit.
    ///
    /// # Safety
    ///
    /// `store` must outlive the registration — the handle keeps a raw pointer,
    /// so the reference's lifetime is erased here.
    pub unsafe fn register_borrowed(
        &mut self,
        id: i64,
        store: &mut Table,
        schema: SchemaDescriptor,
        kind: RelationKind,
        directory: String,
    ) {
        self.tables.insert(
            id,
            TableEntry::new(
                RelationStores {
                    handle: StoreHandle::Borrowed(store),
                    delta: None,
                },
                schema,
                kind,
                0,
                directory,
                ViewBudgets::default(),
            ),
        );
    }

    /// Drop `table_id`'s entry, and with it its owned `Box<Table>` and fds.
    pub fn unregister(&mut self, table_id: i64) {
        self.tables.remove(&table_id);
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
                handle: StoreHandle::owned(index_table),
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
        ic.handle = StoreHandle::owned(t);
        ic.handle.as_owned_mut().map(|t| t as *mut Table)
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
    pub fn swap_table_schema(&mut self, table_id: i64, schema: SchemaDescriptor) -> Result<(), String> {
        let entry = self
            .tables
            .get_mut(&table_id)
            .expect("swap_table_schema: table must be registered");
        // Tripwire on `IndexCircuitEntry.key_spec`, baked at index registration
        // and never rebuilt: only a trailing append leaves every existing OPK
        // offset and payload slot where it was. PK membership is compared because
        // it moves payload slots too.
        debug_assert!(
            schema.pk_indices() == entry.schema.pk_indices()
                && schema.num_columns() >= entry.schema.num_columns()
                && (0..entry.schema.num_columns())
                    .all(|i| schema.columns[i].type_code == entry.schema.columns[i].type_code),
            "swap_table_schema: table {table_id}'s new descriptor is not a trailing append; \
             every index circuit's baked key_spec is now stale",
        );
        // A worker reaches its `Table`; the post-fork master has none.
        if let Some(store) = entry.handle.as_owned_mut() {
            store
                .swap_schema(schema)
                .map_err(|e| format!("ALTER on table {table_id}: reopening shards failed: {e}"))?;
        }
        entry.schema = schema;
        Ok(())
    }

    /// Drop every entry, and with it every owned `Box<Table>` and its fds.
    pub fn close(&mut self) {
        self.tables.clear();
    }

    // ── Registry reads ──────────────────────────────────────────────────

    pub fn has_id(&self, table_id: i64) -> bool {
        self.tables.contains_key(&table_id)
    }

    pub fn num_workers(&self) -> u32 {
        self.num_workers
    }

    pub fn owns_stores(&self) -> bool {
        self.owns_stores
    }

    /// Rows per `drain_chunk` call on every chunked scan this process drives.
    pub fn ddl_scan_chunk_rows(&self) -> usize {
        self.ddl_scan_chunk_rows
    }

    /// Override the chunk size after construction. The backfills are invoked
    /// from catalog hooks, which a test can only reach through `submit`, so
    /// shrinking this is how a test exercises chunk boundaries over a small
    /// table without the environment variable a whole process would share.
    pub fn set_ddl_scan_chunk_rows(&mut self, rows: usize) {
        self.ddl_scan_chunk_rows = rows;
    }

    /// The registry entry for `table_id`, or `None` for an unknown id — the
    /// shape every probing caller wants, and what the projections below read
    /// through.
    pub fn entry(&self, table_id: i64) -> Option<&TableEntry> {
        self.tables.get(&table_id)
    }

    /// [`Self::entry`] plus the shared "Unknown table_id" error every
    /// hard-resolving store path reports.
    pub fn table_entry(&self, table_id: i64) -> Result<&TableEntry, String> {
        self.entry(table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))
    }

    /// A registered relation's wire class, or `None` for an unknown id — the
    /// shape a `FLAG_RESOLVE` descriptor reports. `RelClass` is `Copy`, so the
    /// `tables` borrow ends with the call.
    pub fn relation_class(&self, id: i64) -> Option<gnitz_wire::RelClass> {
        self.entry(id).map(|e| e.class())
    }

    /// A registered relation's kind, or `None` for an unknown id. `RelationKind`
    /// is `Copy`, so the `tables` borrow ends with the call — callers may await
    /// on the result.
    pub fn relation_kind(&self, id: i64) -> Option<RelationKind> {
        self.entry(id).map(|e| e.kind)
    }

    /// True iff `id`'s output is a full copy on every worker — read off the
    /// [`Placement`] stamped on its schema at registration. The one spelling of the
    /// replication probe, so the write broadcast, the read single-sourcing, and the
    /// store shape all read one answer.
    ///
    /// Any **gather** of a replicated relation must therefore single-source it,
    /// taking one worker's copy instead of N identical ones — both the scan
    /// dispatch and the exchange relay read this for that. SEEK already unicasts
    /// to one worker, so it needs no check.
    pub fn relation_is_replicated(&self, id: i64) -> bool {
        self.entry(id).is_some_and(|e| e.schema.placement().is_replicated())
    }

    /// True iff at least one registered view carries a delta feed. The master's
    /// idle-poll bookkeeping — the forward-closure walk and the last-round map —
    /// is skipped outright when this is false, which is every server that does not
    /// use the feature. A walk of the registry rather than a maintained counter:
    /// it runs once per emitted tick group, against a relation count in the tens,
    /// beside a SAL write and an eventfd.
    pub fn any_delta_feed(&self) -> bool {
        self.tables.values().any(|e| e.budgets.delta_bytes.is_some())
    }

    /// True iff `id` is a relation carrying a delta feed. Answered off the
    /// registry, so it is the same answer on the post-fork master — which holds no
    /// store — as on a worker.
    pub fn relation_has_delta_feed(&self, id: i64) -> bool {
        self.entry(id).is_some_and(|e| e.budgets.delta_bytes.is_some())
    }

    /// Every registered view id.
    pub fn view_ids(&self) -> Vec<i64> {
        self.tables
            .iter()
            .filter(|(_, e)| e.kind.is_view())
            .map(|(&id, _)| id)
            .collect()
    }

    /// All index circuits on a table (empty when none) — the one-pass
    /// accessor for consumers that walk every circuit (e.g. the master's
    /// unique-filter descriptors).
    pub fn index_circuits(&self, table_id: i64) -> &[IndexCircuitEntry] {
        self.tables
            .get(&table_id)
            .map(|e| e.index_circuits.as_slice())
            .unwrap_or(&[])
    }

    /// The secondary index circuit on `cols` of `table_id`, if one exists. The
    /// SEEK_BY_INDEX handler matches the `Option` once — `None` answers
    /// STATUS_NO_INDEX (so the SQL planner falls back to a scan or a CREATE INDEX
    /// hint without a prior catalog probe), `Some` broadcasts the seek.
    pub fn index_circuit_for_cols(&self, table_id: i64, cols: &[u32]) -> Option<&IndexCircuitEntry> {
        self.entry(table_id)?.index_circuit_on(cols)
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.index_circuits(table_id).iter().any(|ic| ic.is_unique)
    }

    /// Reject a column list that is malformed or names a column outside
    /// `table_id`'s schema. The one admission test for every frame carrying a
    /// `pack_pk_cols` word: the master applies it as an early client-facing
    /// reject, the worker as its trust boundary, and both render the same error.
    pub fn validate_index_cols(&self, table_id: i64, cols: &PkColList, op: &str) -> Result<(), String> {
        let in_range = |s: &SchemaDescriptor| {
            cols.is_well_formed() && cols.as_slice().iter().all(|&c| (c as usize) < s.num_columns())
        };
        match self.get_schema_desc(table_id) {
            Some(s) if in_range(&s) => Ok(()),
            _ => Err(format!("{op}: invalid column list for table {table_id}")),
        }
    }

    /// Whether `tid`'s store came back from a checkpoint manifest at this open,
    /// rather than being erased or created empty. `false` for a relation this
    /// process holds no owned store for.
    pub fn store_resumed(&self, tid: i64) -> bool {
        self.entry(tid)
            .and_then(|e| e.owned_store())
            .is_some_and(|t| t.resumed_from_checkpoint())
    }

    /// Get schema descriptor for a table. Registry-uniform: system tables are
    /// pre-registered before any caller can run, and an unknown id in the
    /// system range (the 8-10 gap) resolves to a graceful `None` instead of a
    /// panic.
    pub fn get_schema_desc(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.entry(table_id).map(|e| e.schema)
    }

    /// `table_id`'s schema, or the one "the catalog has no schema for a table a
    /// live request names" error. Every caller is a fail-stop — reaching it
    /// means the catalog diverged from the request that named the table — so
    /// `op` labels which path observed the divergence.
    pub fn schema_or_err(&self, table_id: i64, op: &str) -> Result<SchemaDescriptor, String> {
        self.get_schema_desc(table_id)
            .ok_or_else(|| format!("{op}: no schema for table {table_id}"))
    }

    /// The on-disk directory of a user table (`{base_dir}/{schema}/{name}_{tid}`),
    /// the parent of its child store subdirs (`ChildAddr`). Guaranteed to exist on
    /// the data filesystem once the table is created, so it anchors an
    /// `O_TMPFILE` spill (e.g. the CREATE UNIQUE INDEX pre-flight external sort)
    /// onto the same disk as the table's data. `None` for an unknown table.
    pub fn table_directory(&self, table_id: i64) -> Option<&str> {
        self.entry(table_id).map(|e| e.directory.as_str())
    }

    /// Every registered relation's directory, in no defined order.
    pub fn directories(&self) -> impl Iterator<Item = &str> + '_ {
        self.tables.values().map(|e| e.directory.as_str())
    }

    /// Every registered relation's `(directory, index circuits)`, in no defined
    /// order — what the boot orphan sweep names its live index directories from.
    pub fn entries(&self) -> impl Iterator<Item = (i64, &TableEntry)> + '_ {
        self.tables.iter().map(|(&id, e)| (id, e))
    }

    // ── The resume fence ────────────────────────────────────────────────

    /// The one writer of `resume_generation`, and the one writer of
    /// `recorded_topology`. Kept apart because their callers set them at
    /// different moments: the worker latches a generation off a `FLAG_FLUSH_EPH`
    /// header with no topology in hand, and `record_topology` writes a
    /// `_sequences` row the registry knows nothing about.
    pub fn set_resume_generation(&mut self, g: u64) {
        self.resume_generation = g;
    }

    pub fn set_recorded_topology(&mut self, word: u64) {
        self.recorded_topology = word;
    }

    /// The topology word last recorded — `0` until something records one.
    pub fn recorded_topology(&self) -> u64 {
        self.recorded_topology
    }

    /// `worker_count << 32 | STATE_FORMAT` for the count this process launched
    /// with — what a persisted record of derived state must carry to be honoured.
    pub fn launched_topology_word(&self) -> u64 {
        crate::storage::topology_word(self.num_workers)
    }

    /// The generation a manifest must carry to be resumed from.
    pub fn resume_generation(&self) -> u64 {
        self.resume_generation
    }

    /// True when this boot's `(worker count, STATE_FORMAT)` is the one the
    /// persisted derived state was written under. Half of every resume verdict:
    /// a change on either axis invalidates every rederived relation regardless
    /// of what generation its manifest carries.
    pub fn topology_matches(&self) -> bool {
        self.recorded_topology == crate::storage::topology_word(self.num_workers)
    }

    /// The recovery policy for a rederived relation — a view's output store and
    /// operator traces, a secondary index: resume from a manifest at this
    /// registry's resume generation, and only while the topology it was written
    /// under still holds. The one constructor of a generation-bearing
    /// `RecoverySource`, so no consumer can sample a generation of its own at a
    /// second moment.
    pub fn rederive_source(&self) -> RecoverySource {
        RecoverySource::Rederive {
            resume_at: self.topology_matches().then_some(self.resume_generation),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/relation.rs"]
mod tests;
