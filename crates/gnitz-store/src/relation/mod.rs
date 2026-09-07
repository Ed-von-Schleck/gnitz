//! L4 relation registry — what relations this process holds, and the stores
//! behind them.
//!
//! The registry is the state the DBSP layer *reads*: it owns each relation's
//! schema, kind, directory, store handle, secondary-index circuits, delta feed
//! and resume policy. Nothing here compiles a circuit or runs an epoch.
//!
//! A user relation enters through [`RelationRegistry::register`], which is the
//! one site deciding its child address, recovery source, capacity stamp and
//! delta store; the system families through
//! [`RelationRegistry::register_owned`].

use rustc_hash::FxHashMap;

use crate::schema::SchemaDescriptor;

use crate::storage::{Batch, ChildAddr, RamBudgets, RecoverySource, Slot, StorageError, StoreError, Table};
use gnitz_wire::PkColList;

mod build;
mod dirs;
mod ingest;
mod store_handle;
mod store_lsn;

pub use dirs::{ensure_dir, lock_data_dir, relation_dir, staged_dir, DIR_LOCK_RETRY_FOR};
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
    pub index_id: i64,
    pub(crate) handle: StoreHandle,
    pub index_schema: SchemaDescriptor,
    /// Full-arity span-encode plan, precomputed at registration so the per-push
    /// consumers do no per-call spec rebuild. It survives every column ALTER of
    /// the owner — [`RelationRegistry::swap_table_schema`] rejects any descriptor
    /// that would not leave it valid.
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
    pub(crate) fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.handle.open_cursor_in_range(&self.index_schema, start, end)
    }

    /// Ingest an owned batch of index rows. The projection path drives this per
    /// push; a detached circuit absorbs nothing.
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.handle.ingest_owned_batch(batch)
    }

    /// Whether this process's store of the index came back from a checkpoint
    /// manifest at its open — the open this process made, or the one it
    /// inherited through the fork; `false` for a detached circuit.
    pub fn resumed_from_checkpoint(&self) -> bool {
        self.handle.as_owned().is_some_and(Table::resumed_from_checkpoint)
    }

    /// Rows this process's store estimates it holds; `0` for a detached circuit.
    pub fn estimated_rows(&self) -> usize {
        self.handle.as_owned().map_or(0, Table::estimated_rows)
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

impl RelationStores {
    /// One owned `Table` and no feed — what a caller that opened its own store
    /// hands [`RelationRegistry::register_owned`].
    pub(crate) fn owned(store: Box<Table>) -> Self {
        RelationStores {
            handle: StoreHandle::owned(store),
            delta: None,
        }
    }

    /// No store in any process.
    pub(crate) fn storeless() -> Self {
        RelationStores {
            handle: StoreHandle::Storeless,
            delta: None,
        }
    }

    /// No store in this process; another one holds it.
    pub(crate) fn elsewhere() -> Self {
        RelationStores {
            handle: StoreHandle::Elsewhere,
            delta: None,
        }
    }
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
    /// This relation's on-disk directory, parent of its `ChildAddr` subdirs. It
    /// exists once the relation is created, so it anchors an `O_TMPFILE` spill
    /// onto the same disk as the relation's data.
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
    pub(crate) fn new(spec: RelationSpec, stores: RelationStores) -> Self {
        TableEntry {
            handle: stores.handle,
            delta: stores.delta,
            schema: spec.schema,
            kind: spec.kind,
            directory: spec.directory,
            index_circuits: Vec::new(),
            budgets: spec.budgets,
        }
    }

    /// Install both stores at once. The only writer of either field after
    /// construction, so nothing can replace one and leave the other — which for a
    /// fed view is a `delta_bytes` in the catalog with no delta store anywhere.
    pub(crate) fn set_stores(&mut self, stores: RelationStores) {
        self.handle = stores.handle;
        self.delta = stores.delta;
    }

    /// This relation's delta feed — the one message for "this process holds no
    /// delta store", so the ad-hoc read's two lookups (the source schema and the
    /// cursor) cannot render it two ways.
    pub(crate) fn delta_feed_or_err(&self, id: i64) -> Result<&DeltaFeed, StoreError> {
        self.delta.as_deref().ok_or_else(|| {
            StoreError::rejected(format!(
                "scan_spec: this process holds no delta store for relation {id}"
            ))
        })
    }

    /// The `Table` this process holds for this relation, if any. `None` for a
    /// stream, which holds one nowhere, and on the post-fork master, which
    /// detached every user relation.
    pub fn owned_store(&self) -> Option<&Table> {
        self.handle.as_owned()
    }

    /// [`Self::owned_store`] as `&mut`, statically checked — the route a caller
    /// holding the entry mutably takes to mutate its store in place.
    pub fn owned_store_mut(&mut self) -> Option<&mut Table> {
        self.handle.owned_mut()
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
    pub budgets: ViewBudgets,
}

// ---------------------------------------------------------------------------
// RelationRegistry
// ---------------------------------------------------------------------------

/// Everything a registry is tuned by. `Default` is the production value of every
/// field; the server overrides fields from its environment before constructing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StoreConfig {
    pub ram: RamBudgets,
    /// Rows per `drain_chunk` on every chunked scan this process drives; bounds
    /// peak scan memory at O(chunk × row_width). Never zero.
    pub scan_chunk_rows: usize,
    /// Per-worker distinct-group cap for the ad-hoc aggregate fold; bounds the
    /// accumulator matrix at `cap × aggregates × size_of::<Accumulator>()`.
    pub adhoc_group_cap: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            ram: RamBudgets::default(),
            scan_chunk_rows: 65_536,
            adhoc_group_cap: 65_536,
        }
    }
}

/// Which relations this process holds, and the stores behind them.
///
/// **Thread contract.** `!Send + !Sync` by auto-trait: every store holds its
/// runs behind `Rc`. Nothing beneath a registry is thread-affine, so a host may
/// move one to another thread while it has exclusive access to it and no
/// `Rc`-bearing value it handed out is still alive outside it: those refcounts
/// are non-atomic.
pub struct RelationRegistry {
    pub(crate) tables: FxHashMap<i64, TableEntry>,
    /// Which worker this process is, of how many: what names the `w{k}of{n}`
    /// child every store of this process opens. The pre-fork master is
    /// `(0, W)`, so it opens the child worker 0 will inherit; a worker becomes
    /// its own slot through [`Self::rehome`].
    pub(crate) slot: Slot,
    pub(crate) config: StoreConfig,
    /// True while this process owns its relations' stores. The post-fork master
    /// detaches every user relation; anything reading local base data checks
    /// this first.
    pub(crate) owns_stores: bool,
    /// Set by `rehome`, never cleared. "Pre-fork" is `owns_stores && !rehomed`:
    /// the post-fork master detached, every worker rehomed, and only the
    /// pre-fork master and a standalone process have done neither.
    pub(crate) rehomed: bool,
    /// The generation a manifest must carry to be resumed from.
    pub(crate) resume_generation: u64,
    /// `worker_count << 32 | STATE_FORMAT` as last recorded.
    pub(crate) recorded_topology: u64,
}

impl RelationRegistry {
    /// An empty registry at `slot`, tuned by `config`. The one constructor:
    /// `CatalogEngine::open` calls it with `(0, launched worker count)` and the
    /// server's environment overrides, `Mirror::open` with [`Slot::SOLO`].
    /// `config.scan_chunk_rows` is clamped to at least one row.
    pub fn new(slot: Slot, config: StoreConfig) -> Self {
        RelationRegistry {
            tables: FxHashMap::default(),
            slot,
            config: StoreConfig {
                scan_chunk_rows: config.scan_chunk_rows.max(1),
                ..config
            },
            owns_stores: true,
            rehomed: false,
            resume_generation: 0,
            recorded_topology: 0,
        }
    }

    // ── Table registry ──────────────────────────────────────────────────

    /// Enter a relation over a `Table` the caller already opened, taking ownership
    /// — the system families, built at bootstrap before there is a registry to
    /// build them through.
    pub fn register_owned(&mut self, spec: RelationSpec, store: Box<Table>) {
        self.enter(spec, RelationStores::owned(store));
    }

    /// Record `spec` against `stores`. The one insert, so the two entry points
    /// above cannot come to record a registration differently.
    fn enter(&mut self, spec: RelationSpec, stores: RelationStores) {
        self.tables.insert(spec.id, TableEntry::new(spec, stores));
    }

    /// Drop `table_id`'s entry, and with it its owned `Box<Table>` and fds.
    pub fn unregister(&mut self, table_id: i64) {
        self.tables.remove(&table_id);
    }

    /// Enter a secondary index on `owner` over `cols` and open this process's
    /// store for it under `<owner dir>/idx_<index_id>/w{rank}of{n}`. The index
    /// directory is created on every process, so the post-fork master — which
    /// registers no store — still owns the path a later DROP removes.
    pub fn add_index(&mut self, owner: i64, index_id: i64, cols: &[u32], is_unique: bool) -> Result<(), StoreError> {
        let (owner_schema, owner_dir) = {
            let e = self.table_entry(owner)?;
            if e.index_circuit_on(cols).is_some() {
                return Err(StoreError::rejected(format!(
                    "table {owner} already carries an index on {cols:?}"
                )));
            }
            (e.schema, e.directory.clone())
        };
        let key_spec = crate::schema::IndexKeySpec::new(cols, &owner_schema).map_err(StoreError::rejected)?;
        let index_schema = key_spec
            .output_schema(&owner_schema)
            .ok_or_else(|| StoreError::rejected("Index: composite key is not a valid primary key".to_string()))?;
        let idx_dir = ChildAddr::Index { id: index_id }.dir(&owner_dir);
        ensure_dir(&idx_dir)?;
        let handle = match self.owns_stores {
            false => StoreHandle::Elsewhere,
            true => StoreHandle::owned(Self::open_index_table(
                self.slot,
                self.rederive_source(),
                self.config.ram,
                &idx_dir,
                index_id,
                index_schema,
            )?),
        };
        self.tables
            .get_mut(&owner)
            .expect("resolved above")
            .index_circuits
            .push(IndexCircuitEntry {
                col_indices: PkColList::from_slice(cols),
                index_id,
                handle,
                index_schema,
                key_spec,
                is_unique,
            });
        Ok(())
    }

    /// This process's store of one index: at `slot`'s child of `idx_dir`, under
    /// the given rederive policy and RAM budgets. An associated function so a
    /// caller holding `&mut` into `tables` can open without a second borrow of
    /// `self`.
    fn open_index_table(
        slot: Slot,
        recovery: RecoverySource,
        ram: RamBudgets,
        idx_dir: &str,
        index_id: i64,
        schema: SchemaDescriptor,
    ) -> Result<Box<Table>, StoreError> {
        Table::with_budgets(
            &ChildAddr::worker(slot).dir(idx_dir),
            schema,
            index_id as u32,
            recovery,
            ram,
        )
        .map(Box::new)
        .map_err(|e| StoreError::storage(format!("open index {index_id} (dir={idx_dir})"), e))
    }

    pub fn remove_index_circuit(&mut self, table_id: i64, col_indices: &[u32]) {
        if let Some(entry) = self.tables.get_mut(&table_id) {
            // retain() drops non-matching entries, which drops Box<Table> automatically.
            entry
                .index_circuits
                .retain(|ic| ic.col_indices.as_slice() != col_indices);
        }
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
    /// column ALTER): update the registry copy and push the same value down into
    /// the owned `Table`, which re-opens its shards if the region count grew.
    pub fn swap_table_schema(&mut self, table_id: i64, schema: SchemaDescriptor) -> Result<(), StoreError> {
        let entry = self
            .tables
            .get_mut(&table_id)
            .expect("swap_table_schema: table must be registered");
        // Checked, not asserted: what a stale `key_spec` produces is a silently
        // wrong index projection, which release codegen would not guard at all.
        if !schema.is_trailing_append_of(&entry.schema) {
            return Err(StoreError::rejected(format!(
                "ALTER on table {table_id}: the new descriptor is not a trailing append, \
                 which every index circuit's baked key_spec requires"
            )));
        }
        // A worker reaches its `Table`; the post-fork master has none.
        if let Some(store) = entry.handle.owned_mut() {
            store
                .swap_schema(schema)
                .map_err(|e| StoreError::storage(format!("ALTER on table {table_id}: reopening shards"), e))?;
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

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn config(&self) -> StoreConfig {
        self.config
    }

    /// `slot().of`.
    pub fn num_workers(&self) -> u32 {
        self.slot.of
    }

    pub fn owns_stores(&self) -> bool {
        self.owns_stores
    }

    /// See [`StoreConfig::scan_chunk_rows`].
    pub fn scan_chunk_rows(&self) -> usize {
        self.config.scan_chunk_rows
    }

    /// Set the chunk size, clamped to at least one row. Per registry, not
    /// process-wide. Production sets it once through [`StoreConfig`].
    pub fn set_scan_chunk_rows(&mut self, rows: usize) {
        self.config.scan_chunk_rows = rows.max(1);
    }

    /// The registry entry for `table_id`, or `None` for an unknown id — the
    /// shape every probing caller wants, and what the projections below read
    /// through.
    pub fn entry(&self, table_id: i64) -> Option<&TableEntry> {
        self.tables.get(&table_id)
    }

    /// [`Self::entry`] as `&mut` — what a caller mutating one relation's store
    /// in place takes, so the mutation is checked rather than contracted.
    pub fn entry_mut(&mut self, table_id: i64) -> Option<&mut TableEntry> {
        self.tables.get_mut(&table_id)
    }

    /// [`Self::entry`] plus the one "not registered" sentence — spelled the same
    /// by the mutating paths, so which verb asked cannot change what a client reads.
    pub fn table_entry(&self, table_id: i64) -> Result<&TableEntry, StoreError> {
        self.entry(table_id)
            .ok_or_else(|| StoreError::rejected(format!("relation {table_id} is not registered")))
    }

    /// A registered relation's kind, or `None` for an unknown id. `RelationKind`
    /// is `Copy`, so the `tables` borrow ends with the call — callers may await
    /// on the result.
    pub fn relation_kind(&self, id: i64) -> Option<RelationKind> {
        self.entry(id).map(|e| e.kind)
    }

    /// True iff `id`'s output is a full copy on every worker — [`Placement`]'s own
    /// probe, for a caller holding an id rather than the schema.
    ///
    /// Any **gather** of a replicated relation must single-source it, taking one
    /// worker's copy instead of N identical ones. SEEK already unicasts to one
    /// worker, so it needs no check.
    pub fn relation_is_replicated(&self, id: i64) -> bool {
        self.entry(id).is_some_and(|e| e.schema.placement().is_replicated())
    }

    /// True iff at least one registered relation carries a delta feed.
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

    /// [`Self::index_circuit_for_cols`] as `&mut`.
    pub fn index_circuit_for_cols_mut(&mut self, table_id: i64, cols: &[u32]) -> Option<&mut IndexCircuitEntry> {
        self.entry_mut(table_id)?.index_circuit_on_mut(cols)
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.index_circuits(table_id).iter().any(|ic| ic.is_unique)
    }

    /// The column list a frame's `pack_pk_cols` word names, admitted against
    /// `table_id`'s schema. The one decode-and-admit for every frame carrying such
    /// a word: the master applies it as an early client-facing reject, the worker
    /// as its trust boundary, and both render the same error.
    pub fn index_cols(&self, table_id: i64, packed: u64, op: &str) -> Result<PkColList, StoreError> {
        let cols = gnitz_wire::unpack_pk_cols(packed)
            .map_err(|_| StoreError::rejected(format!("{op}: invalid column list for table {table_id}")))?;
        self.bound_cols_against(table_id, cols, op)
    }

    /// Admit an already-unpacked column list against `table_id`'s schema. The
    /// client owns the list, and only the registry holds a schema to bound it
    /// against, so every path taking one takes this test — [`Self::index_cols`]
    /// for a frame carrying the packed word, this for one whose wire decoder
    /// (`IndexBound`) already unpacked it.
    pub fn bound_cols_against(&self, table_id: i64, cols: PkColList, op: &str) -> Result<PkColList, StoreError> {
        match self.entry(table_id) {
            Some(e) if cols.as_slice().iter().all(|&c| (c as usize) < e.schema.num_columns()) => Ok(cols),
            _ => Err(StoreError::rejected(format!(
                "{op}: invalid column list for table {table_id}"
            ))),
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

    /// Every registered relation's `(id, entry)`, in no defined order — what the
    /// boot orphan sweep names its live table and index directories from.
    pub fn entries(&self) -> impl Iterator<Item = (i64, &TableEntry)> + '_ {
        self.tables.iter().map(|(&id, e)| (id, e))
    }

    // ── The resume fence ────────────────────────────────────────────────

    /// The one writer of `resume_generation`, and the one writer of
    /// `recorded_topology`. Kept apart because their callers set them at
    /// different moments: the worker latches a generation off a `FlushEph`
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
        crate::storage::topology_word(self.slot.of)
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
        self.recorded_topology == self.launched_topology_word()
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
