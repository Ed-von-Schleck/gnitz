//! L4 relation registry — what relations this process holds, and the stores
//! behind them.
//!
//! Every relation enters through [`RelationRegistry::register`], which is the
//! one site deciding its child address, recovery source, capacity stamp and
//! delta store.

use rustc_hash::FxHashMap;

use crate::schema::SchemaDescriptor;

use crate::storage::{
    Batch, ChildAddr, RecoverySource, Slot, StorageError, StoreBudgets, StoreError, StoredRow, Table,
};
use gnitz_wire::PkColList;

mod build;
mod circuit_state;
mod dirs;
mod ingest;
mod store;
mod store_lsn;
mod unique_pk;

pub use build::OnRegister;
pub use circuit_state::{CircuitState, StateIdx};
pub use dirs::{ensure_dir, lock_data_dir, relation_dir, staged_dir, DIR_LOCK_RETRY_FOR};
pub(crate) use store::Store;

// ---------------------------------------------------------------------------
// Secondary index
// ---------------------------------------------------------------------------

/// A secondary index on a column list of one relation. Owns this process's store
/// for it — dropping the index drops the store.
pub struct SecondaryIndex {
    store: Store,
    /// The index's declared column list, in order. A 1-element list is the
    /// single-column case. Dedup/lookup is exact ordered-list equality on
    /// `cols.as_slice()`; order is significant (it drives leading-prefix seeks).
    cols: PkColList,
    /// The index_id of the IDX_TAB row that caused this store to be opened.
    /// When a second index promotes an incumbent circuit (UNIQUE+FK case), no
    /// new directory is created; this field identifies the actual on-disk path
    /// so the retraction branch queues the correct directory for deletion.
    index_id: i64,
    /// Full-arity span-encode plan, precomputed at registration so the per-push
    /// consumers do no per-call spec rebuild. It survives every column ALTER of
    /// the owner — [`RelationRegistry::swap_schema`] rejects any descriptor that
    /// would not leave it valid.
    /// Deliberately does NOT bake in `is_unique` (live promotion/demotion via
    /// `set_index_unique`); consumers filter on the live flag.
    key_spec: crate::schema::IndexKeySpec,
    is_unique: bool,
}

impl SecondaryIndex {
    /// By value, not as a slice: `PkColList` is `Copy`, so the list can be keyed
    /// on or held past the borrow of the registry that produced it.
    pub fn cols(&self) -> PkColList {
        self.cols
    }

    pub fn id(&self) -> i64 {
        self.index_id
    }

    pub fn schema(&self) -> SchemaDescriptor {
        self.store.schema()
    }

    pub fn key_spec(&self) -> crate::schema::IndexKeySpec {
        self.key_spec
    }

    pub fn is_unique(&self) -> bool {
        self.is_unique
    }

    /// Non-compacting cursor over this index's store, in the index schema — a
    /// process holding no store opens empty.
    pub fn cursor(&self) -> crate::storage::ReadCursor {
        self.store.cursor()
    }

    /// [`Self::cursor`] over `[start, end]` only.
    pub(crate) fn cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.store.cursor_in_range(start, end)
    }

    /// Write index rows directly. The one write to an index that does not ride a
    /// base-table push, so the caller owns the `key_spec` projection.
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.store.ingest_owned_batch(batch)
    }

    /// Whether this process's store of the index came back from a checkpoint
    /// manifest at its open — the open this process made, or the one it
    /// inherited through the fork.
    pub fn resumed(&self) -> bool {
        self.store.resumed_from_checkpoint()
    }

    /// Rows this process's store estimates it holds; `0` where it holds none.
    pub fn estimated_rows(&self) -> usize {
        self.store.estimated_rows()
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
// Residency — what this process is to the stores it registered
// ---------------------------------------------------------------------------

/// What a process is to the stores it registered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Residency {
    /// Opened its own stores and has not re-homed: the pre-fork master, and any
    /// standalone host. The only residency that may read or delete across ranks,
    /// because it is the only one that can speak for a rank it is not.
    Origin,
    /// Re-homed onto this worker's own slot. Owns its stores; speaks for itself.
    Worker,
    /// Holds no user store: the post-fork master, which detached every one.
    Detached,
}

impl Residency {
    /// True while this process owns the stores it registered.
    #[inline]
    pub fn owns_stores(self) -> bool {
        !matches!(self, Residency::Detached)
    }
}

// ---------------------------------------------------------------------------
// Relation — one relation in this process
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

/// One relation in this process: its identity, its shape, and this process's
/// store for it. Every read below answers for a process that holds no store —
/// empty cursor, empty scan, `false`, `0` — so no caller branches on residency
/// to ask a question.
pub struct Relation {
    /// The relation id — the key it is registered under, held here too so a
    /// borrowed `Relation` names itself: nothing that has one needs the id
    /// threaded in beside it to log or to phrase an error.
    id: i64,
    store: Store,
    /// The delta store, when this process holds one — `budgets.delta_bytes` is
    /// what says a feed *exists*. `Box` because it embeds a second
    /// `SchemaDescriptor` (360 bytes), which every relation would pay inline.
    delta: Option<Box<Store>>,
    indexes: Vec<SecondaryIndex>,
    kind: RelationKind,
    /// This relation's on-disk directory, parent of its `ChildAddr` subdirs. It
    /// exists once the relation is created, so it anchors an `O_TMPFILE` spill
    /// onto the same disk as the relation's data.
    directory: String,
    /// The budgets this relation was registered with: what a rehome or a rebuild
    /// hands back to `build_relation_store`, and the answer the post-fork master
    /// gives when it has no `Table` to ask.
    budgets: ViewBudgets,
}

impl Relation {
    /// A registry entry with no secondary indexes yet — the one insert
    /// [`RelationRegistry::register`] makes. Both budgets are `None` for
    /// everything but a bounded or a fed view.
    pub(crate) fn new(spec: RelationSpec, stores: (Store, Option<Box<Store>>)) -> Self {
        Relation {
            id: spec.id,
            store: stores.0,
            delta: stores.1,
            kind: spec.kind,
            directory: spec.directory,
            indexes: Vec::new(),
            budgets: spec.budgets,
        }
    }

    /// Install both stores at once. The only writer of either field after
    /// construction, so nothing can replace one and leave the other — which for a
    /// fed view is a `delta_bytes` in the catalog with no delta store anywhere.
    pub(crate) fn set_stores(&mut self, stores: (Store, Option<Box<Store>>)) {
        self.store = stores.0;
        self.delta = stores.1;
    }

    /// This relation's delta feed, or the one message for "this process holds no
    /// delta store" — so no two reads can render that answer differently.
    pub(crate) fn delta_or_err(&self) -> Result<&Store, StoreError> {
        self.delta.as_deref().ok_or_else(|| {
            StoreError::rejected(format!(
                "scan_spec: this process holds no delta store for relation {}",
                self.id
            ))
        })
    }

    /// By value, for the reason [`SecondaryIndex::cols`] is: the `tables` borrow
    /// ends with the call, so the descriptor survives a later `&mut` on the
    /// registry.
    pub fn schema(&self) -> SchemaDescriptor {
        self.store.schema()
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn kind(&self) -> RelationKind {
        self.kind
    }

    pub fn directory(&self) -> &str {
        &self.directory
    }

    pub fn budgets(&self) -> ViewBudgets {
        self.budgets
    }

    /// Whether this relation retains its recent deltas for a feed reader. The
    /// budget is what says a feed *exists*; whether this process holds the store
    /// for it is a separate question, [`Self::delta_or_err`]'s.
    pub fn has_delta_feed(&self) -> bool {
        self.budgets.delta_bytes.is_some()
    }

    /// Whether a capacity bounds this relation's registered shard bytes, so its
    /// sweep may leave skeleton rows behind.
    pub fn is_bounded(&self) -> bool {
        self.budgets.capacity_bytes.is_some()
    }

    /// Whether every worker holds the whole relation rather than a partition of
    /// it — so a read of it must single-source, and a write of it broadcasts.
    pub fn is_replicated(&self) -> bool {
        self.schema().placement().is_replicated()
    }

    /// Whether any secondary index on this relation enforces uniqueness. An FK
    /// auto-index is not one, so this is narrower than "carries an index".
    pub fn has_unique_index(&self) -> bool {
        self.indexes.iter().any(SecondaryIndex::is_unique)
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
            RelationKind::View if self.is_bounded() => RelClass::BoundedView,
            RelationKind::View => RelClass::View,
            RelationKind::BaseTable | RelationKind::SystemCatalog => RelClass::Table,
        }
    }

    /// Non-compacting cursor over this relation's store.
    pub fn cursor(&self) -> crate::storage::ReadCursor {
        self.store.cursor()
    }

    /// This relation's rows over `[start, end]` only — see
    /// [`Table::open_cursor_in_range`](crate::storage::Table::open_cursor_in_range).
    pub fn cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> crate::storage::ReadCursor {
        self.store.cursor_in_range(start, end)
    }

    /// Materialize every positive-weight row of this relation's store.
    pub fn full_scan(&self) -> std::rc::Rc<Batch> {
        self.store.full_scan()
    }

    /// Whether a live row carries this OPK key.
    pub fn has_pk(&self, key: &[u8]) -> bool {
        self.store.table().is_some_and(|t| t.has_pk_bytes(key))
    }

    /// The net weight at `key`, and the live row if there is one.
    pub fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        self.store.table().map_or((0, None), |t| t.live_row_at(key))
    }

    /// This relation's recovery watermark: the highest LSN its store has flushed;
    /// `0` where this process holds no store. `0` is not "exclude me" — a
    /// relation admitted at it would make the worker skip its own replay.
    pub fn current_lsn(&self) -> u64 {
        self.store.current_lsn()
    }

    /// Whether this store came back from a checkpoint manifest at its open.
    pub fn resumed(&self) -> bool {
        self.store.resumed_from_checkpoint()
    }

    /// Every secondary index on this relation, in registration order.
    pub fn indexes(&self) -> &[SecondaryIndex] {
        &self.indexes
    }

    /// The secondary index covering exactly `cols`, if one exists. The index
    /// list is deduped by ordered column list, so at most one entry matches.
    pub fn index_on(&self, cols: &[u32]) -> Option<&SecondaryIndex> {
        self.indexes.iter().find(|ix| ix.cols.as_slice() == cols)
    }

    /// [`Self::index_on`] as `&mut`.
    pub fn index_on_mut(&mut self, cols: &[u32]) -> Option<&mut SecondaryIndex> {
        self.indexes.iter_mut().find(|ix| ix.cols.as_slice() == cols)
    }

    /// This process's store, for the crate's own read and write paths.
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }
}

// ---------------------------------------------------------------------------
// The relation to register
// ---------------------------------------------------------------------------

/// Everything [`RelationRegistry::register`] needs to enter a relation and open
/// its store — [`Relation::new`]'s parameter list plus the id and minus the
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
// Topology
// ---------------------------------------------------------------------------

/// Operator-state format version. Bump on any change to an operator-state
/// schema; a mismatch (recorded in `_sequences` via `SEQ_ID_TOPOLOGY`) marks
/// every Rederive view invalid at boot, so its state is rebuilt. Shard and
/// manifest layout changes are carried by their own version words.
const STATE_FORMAT: u32 = 8;

/// The durable topology word recorded in `_sequences` (`SEQ_ID_TOPOLOGY`):
/// `(worker_count << 32) | STATE_FORMAT`. The single packer shared by the
/// boot-time recorder and the resume-verdict validator, so the two can never
/// drift on the encoding.
pub fn topology_word(worker_count: u32) -> u64 {
    ((worker_count as u64) << 32) | STATE_FORMAT as u64
}

// ---------------------------------------------------------------------------
// RelationRegistry
// ---------------------------------------------------------------------------

/// Everything a registry is tuned by. `Default` is the production value of every
/// field; the server overrides fields from its environment before constructing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StoreConfig {
    /// The RAM-tier ceiling every store this registry opens is sized to.
    pub ram_tier_bytes: usize,
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
            ram_tier_bytes: crate::storage::DEFAULT_RAM_TIER_BYTES,
            scan_chunk_rows: 65_536,
            adhoc_group_cap: 65_536,
        }
    }
}

/// Which relations this process holds, the stores behind them, and what this
/// process is to those stores. One type rather than several because every verb
/// on it reads or writes the same `id -> Relation` map under the same
/// [`Residency`], and a split would hand two owners `&mut` over it.
///
/// Nothing here compiles a circuit or runs an epoch — those are
/// `gnitz-server`'s, and the crate graph is what says so.
///
/// **Thread contract.** `!Send + !Sync` by auto-trait: every store holds its
/// runs behind `Rc`. Nothing beneath a registry is thread-affine, so a host may
/// move one to another thread while it has exclusive access to it and no
/// `Rc`-bearing value it handed out is still alive outside it: those refcounts
/// are non-atomic.
pub struct RelationRegistry {
    pub(crate) tables: FxHashMap<i64, Relation>,
    /// Which worker this process is, of how many: what names the `w{k}of{n}`
    /// child every store of this process opens. The pre-fork master is
    /// `(0, W)`, so it opens the child worker 0 will inherit; a worker becomes
    /// its own slot through [`Self::rehome`].
    pub(crate) slot: Slot,
    pub(crate) config: StoreConfig,
    /// What this process is to the stores it registered.
    pub(crate) residency: Residency,
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
            residency: Residency::Origin,
            resume_generation: 0,
            recorded_topology: 0,
        }
    }

    // ── Relation registry ───────────────────────────────────────────────

    /// Drop `id`'s entry, and with it its owned stores and fds.
    pub fn unregister(&mut self, id: i64) {
        self.tables.remove(&id);
    }

    /// Enter a secondary index on `owner` over `cols` and open this process's
    /// store for it under `<owner dir>/idx_<index_id>/w{rank}of{n}`. The index
    /// directory is created on every process, so the post-fork master — which
    /// registers no store — still owns the path a later DROP removes.
    pub fn add_index(&mut self, owner: i64, index_id: i64, cols: &[u32], is_unique: bool) -> Result<(), StoreError> {
        let (owner_schema, owner_dir) = {
            let e = self.relation_or_err(owner)?;
            if e.index_on(cols).is_some() {
                return Err(StoreError::rejected(format!(
                    "table {owner} already carries an index on {cols:?}"
                )));
            }
            (e.schema(), e.directory.clone())
        };
        let key_spec = crate::schema::IndexKeySpec::new(cols, &owner_schema).map_err(StoreError::rejected)?;
        let index_schema = key_spec
            .output_schema(&owner_schema)
            .ok_or_else(|| StoreError::rejected("Index: composite key is not a valid primary key".to_string()))?;
        let idx_dir = ChildAddr::Index { id: index_id }.dir(&owner_dir);
        ensure_dir(&idx_dir)?;
        let store = match self.residency.owns_stores() {
            false => Store::detached(index_schema),
            true => Store::owned(
                Self::open_index_table(
                    self.slot,
                    self.rederive_source(),
                    self.store_budgets(),
                    &idx_dir,
                    index_id,
                    index_schema,
                )?,
                index_schema,
            ),
        };
        self.tables
            .get_mut(&owner)
            .expect("resolved above")
            .indexes
            .push(SecondaryIndex {
                cols: PkColList::from_slice(cols),
                index_id,
                store,
                key_spec,
                is_unique,
            });
        Ok(())
    }

    /// This process's store of one index: at `slot`'s child of `idx_dir`, under
    /// the given rederive policy and store budgets. An associated function so a
    /// caller holding `&mut` into `tables` can open without a second borrow of
    /// `self`.
    fn open_index_table(
        slot: Slot,
        recovery: RecoverySource,
        budgets: StoreBudgets,
        idx_dir: &str,
        index_id: i64,
        schema: SchemaDescriptor,
    ) -> Result<Box<Table>, StoreError> {
        Table::new(
            &ChildAddr::worker(slot).dir(idx_dir),
            schema,
            index_id as u32,
            recovery,
            budgets,
        )
        .map(Box::new)
        .map_err(|e| StoreError::storage(format!("open index {index_id} (dir={idx_dir})"), e))
    }

    pub fn remove_index(&mut self, id: i64, cols: &[u32]) {
        if let Some(entry) = self.tables.get_mut(&id) {
            // retain() drops non-matching entries, which drops their stores.
            entry.indexes.retain(|ix| ix.cols.as_slice() != cols);
        }
    }

    /// Set the uniqueness flag of the index on `cols` in place (the index list
    /// is deduped by column list, so at most one entry matches). Promotion
    /// (`true`) folds a UNIQUE index into an existing non-unique one when both
    /// target the same column list; demotion (`false`) is used by the DROP INDEX
    /// retraction path when the UNIQUE index is dropped but another index (e.g.
    /// an FK auto-index) still covers it.
    pub fn set_index_unique(&mut self, id: i64, cols: &[u32], is_unique: bool) {
        if let Some(ix) = self.relation_mut(id).and_then(|e| e.index_on_mut(cols)) {
            ix.is_unique = is_unique;
        }
    }

    /// Publish a new column schema for a registered base table in place (any
    /// column ALTER): update the registry copy and push the same value down into
    /// the owned `Table`, which re-opens its shards if the region count grew.
    pub fn swap_schema(&mut self, id: i64, schema: SchemaDescriptor) -> Result<(), StoreError> {
        let entry = self
            .tables
            .get_mut(&id)
            .expect("swap_schema: relation must be registered");
        // Checked, not asserted: what a stale `key_spec` produces is a silently
        // wrong index projection, which release codegen would not guard at all.
        if !schema.is_trailing_append_of(&entry.store.schema()) {
            return Err(StoreError::rejected(format!(
                "ALTER on table {id}: the new descriptor is not a trailing append, \
                 which every index circuit's baked key_spec requires"
            )));
        }
        entry
            .store
            .swap_schema(schema)
            .map_err(|e| StoreError::storage(format!("ALTER on table {id}: reopening shards"), e))
    }

    /// Drop every entry, and with it every owned store and its fds.
    pub fn close(&mut self) {
        self.tables.clear();
    }

    // ── Registry reads ──────────────────────────────────────────────────

    pub fn has_id(&self, id: i64) -> bool {
        self.tables.contains_key(&id)
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// What this process is to the stores it registered.
    pub fn residency(&self) -> Residency {
        self.residency
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

    /// The relation `id` names, or `None` for an unknown id — the shape every
    /// probing caller wants.
    pub fn relation(&self, id: i64) -> Option<&Relation> {
        self.tables.get(&id)
    }

    /// [`Self::relation`] as `&mut` — the one mutable route into a relation, so
    /// navigation reads the same in both directions.
    pub fn relation_mut(&mut self, id: i64) -> Option<&mut Relation> {
        self.tables.get_mut(&id)
    }

    /// [`Self::relation`] plus the one "not registered" sentence — spelled the
    /// same by the mutating paths, so which verb asked cannot change what a
    /// client reads.
    pub fn relation_or_err(&self, id: i64) -> Result<&Relation, StoreError> {
        self.relation(id)
            .ok_or_else(|| StoreError::rejected(format!("relation {id} is not registered")))
    }

    /// Every registered relation, in no defined order — what the boot orphan
    /// sweep names its live table and index directories from. Each names its own
    /// id, so the walk yields the relation alone.
    pub fn relations(&self) -> impl Iterator<Item = &Relation> + '_ {
        self.tables.values()
    }

    /// True iff at least one registered relation carries a delta feed.
    pub fn any_delta_feed(&self) -> bool {
        self.tables.values().any(Relation::has_delta_feed)
    }

    /// Every registered view id.
    pub fn view_ids(&self) -> Vec<i64> {
        self.tables
            .iter()
            .filter(|(_, e)| e.kind.is_view())
            .map(|(&id, _)| id)
            .collect()
    }

    /// The column list a frame's `pack_pk_cols` word names, admitted against
    /// `id`'s schema. The one decode-and-admit for every frame carrying such a
    /// word: the master applies it as an early client-facing reject, the worker
    /// as its trust boundary, and both render the same error.
    pub fn index_cols(&self, id: i64, packed: u64, op: &str) -> Result<PkColList, StoreError> {
        let cols = gnitz_wire::unpack_pk_cols(packed)
            .map_err(|_| StoreError::rejected(format!("{op}: invalid column list for table {id}")))?;
        self.bound_cols_against(id, cols, op)
    }

    /// Admit an already-unpacked column list against `id`'s schema. The client
    /// owns the list, and only the registry holds a schema to bound it against,
    /// so every path taking one takes this test — [`Self::index_cols`] for a
    /// frame carrying the packed word, this for one whose wire decoder
    /// (`IndexBound`) already unpacked it.
    pub(crate) fn bound_cols_against(&self, id: i64, cols: PkColList, op: &str) -> Result<PkColList, StoreError> {
        match self.relation(id) {
            Some(e) if cols.as_slice().iter().all(|&c| (c as usize) < e.schema().num_columns()) => Ok(cols),
            _ => Err(StoreError::rejected(format!(
                "{op}: invalid column list for table {id}"
            ))),
        }
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
    pub fn launched_topology(&self) -> u64 {
        topology_word(self.slot.of)
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
        self.recorded_topology == self.launched_topology()
    }

    /// What every store this registry opens starts from; the two bounded kinds
    /// narrow it.
    pub(crate) fn store_budgets(&self) -> StoreBudgets {
        StoreBudgets::new(self.config.ram_tier_bytes)
    }

    /// The recovery policy for a rederived relation — a view's output store and
    /// operator traces, a secondary index: resume from a manifest at this
    /// registry's resume generation, and only while the topology it was written
    /// under still holds. The one constructor of a generation-bearing
    /// `RecoverySource`, so no consumer can sample a generation of its own at a
    /// second moment.
    pub(crate) fn rederive_source(&self) -> RecoverySource {
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
