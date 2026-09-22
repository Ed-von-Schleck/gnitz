//! L4 relation registry — what relations this process holds, and the stores
//! behind them.
//!
//! Every relation enters through [`RelationRegistry::register`].

use gnitz_foundation::env::env_num;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::schema::key::{key_range_between_cuts, KeyCut, PkBuf};
use crate::schema::SchemaDescriptor;

use crate::storage::{
    Batch, ChildAddr, ChildKind, ReadCursor, RecoverySource, Slot, StorageError, StoreBudgets, StoreError, StoredRow,
    Table,
};
use gnitz_wire::{PkColList, ViewProps};

mod build;
mod circuit_state;
mod dirs;
mod ingest;
mod store;
mod store_lsn;
mod unique_pk;

pub use circuit_state::{CircuitState, StateIdx, StateLayout};
pub(crate) use dirs::ensure_dir;
pub use dirs::{lock_data_dir, relation_dir, relations_dir, DIR_LOCK_RETRY_FOR};
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
    /// The id this store's directory is named by — the creator's, even after a
    /// second index promoted it.
    index_id: i64,
    /// Full-arity span-encode plan, precomputed at registration so the per-push
    /// consumers do no per-call spec rebuild. It survives every column ALTER of
    /// the owner — [`RelationRegistry::swap_schema`] rejects any descriptor that
    /// would not leave it valid.
    /// Excludes `is_unique`, which changes while the index lives.
    key_spec: crate::schema::IndexKeySpec,
    is_unique: bool,
    /// Whether `cols` covers the owner's PK, so the index can never collide.
    covers_pk: bool,
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

    /// This process's store of the index, for the crate's own read paths.
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// Write index rows directly, in the index's own layout — the one write that
    /// does not ride a projection of the owner. For tests that need an entry no
    /// projection of the owner could produce.
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.store.ingest_owned_batch(batch)
    }

    /// Project `source` into this index's layout and ingest the result.
    /// `Ok(false)` when the projection was empty: the key spec rejects rows, so
    /// a non-empty `source` can still project to nothing.
    pub(crate) fn project_and_ingest(&mut self, source: &Batch) -> Result<bool, StorageError> {
        let projected = source.project_index(&self.key_spec, &self.store.schema());
        if projected.is_empty() {
            return Ok(false);
        }
        self.store.ingest_owned_batch(projected).map(|()| true)
    }

    /// Whether this process's store of the index came back from a checkpoint
    /// manifest at its open.
    pub fn resumed(&self) -> bool {
        self.store.resumed_from_checkpoint()
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
    /// User base table: durable, never rebuilt from upstream
    /// sources; owns a DML-enforced PK, so `enforce_unique_pk` runs on every
    /// ingest and its accumulated per-PK weight is always in {0, 1}.
    BaseTable,
    /// Materialised view, with its `WITH (…)` options: ephemeral, resumed from a
    /// generation-stamped manifest when the ephemeral checkpoint round left one.
    ///
    /// Not necessarily *rebuilt* from its sources: a mirror registers its fed
    /// copies under this kind too.
    View(ViewProps),
    /// Ingestion point: storeless, append-only. Pushed rows exist
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
            RelationKind::View(_) => "view",
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
        matches!(self, RelationKind::View(_))
    }
}

// ---------------------------------------------------------------------------
// Residency — what this process is to the stores it registered
// ---------------------------------------------------------------------------

/// What a process is to the stores it registered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Residency {
    /// The server's master: holds the system families' stores and no user store,
    /// which is what lets it relay and reclaim any rank's children.
    Master,
    /// A standalone host — a mirror, a unit-test engine or fixture: owns every
    /// store it registers, at its slot, and may read any rank's manifests.
    Origin,
    /// A forked worker, at its own slot. Owns its stores; speaks for itself.
    Worker,
}

impl Residency {
    /// True iff this process opens a store for every relation it registers.
    #[inline]
    pub fn owns_stores(self) -> bool {
        !matches!(self, Residency::Master)
    }
}

// ---------------------------------------------------------------------------
// Relation — one relation in this process
// ---------------------------------------------------------------------------

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
    /// The delta store, on the ranks that serve this view's feed. Boxed: it embeds
    /// a second `SchemaDescriptor`.
    delta: Option<Box<Store>>,
    indexes: Vec<SecondaryIndex>,
    kind: RelationKind,
    /// This relation's on-disk directory, parent of its `ChildAddr` subdirs. It
    /// exists once the relation is created, so it anchors an `O_TMPFILE` spill
    /// onto the same disk as the relation's data.
    directory: String,
}

impl Relation {
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

    /// Whether this relation keeps a delta feed, whether or not this process holds
    /// its store.
    pub fn has_delta_feed(&self) -> bool {
        matches!(self.kind, RelationKind::View(ViewProps::Fed { .. }))
    }

    /// Whether a capacity bounds this relation's registered shard bytes, so its
    /// sweep may leave skeleton rows behind.
    pub fn is_bounded(&self) -> bool {
        matches!(self.kind, RelationKind::View(ViewProps::Bounded { .. }))
    }

    /// Whether every worker holds the whole relation rather than a partition of
    /// it — so a read of it must single-source, and a write of it broadcasts.
    pub fn is_replicated(&self) -> bool {
        self.schema().placement().is_replicated()
    }

    /// The unique secondary indexes a write must still check: one covering the
    /// PK cannot collide, so it is not among them.
    pub fn unique_indexes_to_check(&self) -> impl Iterator<Item = &SecondaryIndex> + '_ {
        self.indexes.iter().filter(|ic| ic.is_unique && !ic.covers_pk)
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

    /// Visit every positive-weight row whose OPK key begins with `prefix`, through a
    /// cursor that gathers only the runs overlapping that key band.
    pub fn for_each_positive_with_prefix(&self, prefix: &[u8], f: impl FnMut(&ReadCursor)) {
        let band = key_range_between_cuts(KeyCut::min_of(prefix), KeyCut::above(prefix), self.schema().pk_stride());
        if let Some((start, end)) = band {
            self.cursor_in_range(start.pk_bytes(), end.as_ref().map(PkBuf::pk_bytes))
                .for_each_positive_with_prefix(prefix, f);
        }
    }

    /// Materialize every positive-weight row of this relation's store.
    pub fn full_scan(&self) -> std::rc::Rc<Batch> {
        self.store.full_scan()
    }

    /// Whether a live row carries this OPK key.
    pub fn has_pk(&self, key: &[u8]) -> bool {
        self.store.has_pk_bytes(key)
    }

    /// The net weight at `key`, and the live row if there is one.
    pub fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        self.store.live_row_at(key)
    }

    /// This relation's store LSN counter; `0` where this process holds no store.
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

/// A relation to [`RelationRegistry::register`].
pub struct RelationSpec {
    pub id: i64,
    pub kind: RelationKind,
    pub schema: SchemaDescriptor,
}

// ---------------------------------------------------------------------------
// RelationRegistry
// ---------------------------------------------------------------------------

/// Everything a registry is tuned by. `Default` is the production value of every
/// field; a host overrides them through [`StoreConfig::from_env`].
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

impl StoreConfig {
    /// Every field from `<prefix>RAM_TIER_BYTES`, `<prefix>SCAN_CHUNK_ROWS` and
    /// `<prefix>ADHOC_GROUP_CAP`, each falling back to [`Default`].
    pub fn from_env(prefix: &str) -> Self {
        let d = StoreConfig::default();
        StoreConfig {
            ram_tier_bytes: env_num(&format!("{prefix}RAM_TIER_BYTES"), d.ram_tier_bytes),
            scan_chunk_rows: env_num(&format!("{prefix}SCAN_CHUNK_ROWS"), d.scan_chunk_rows),
            adhoc_group_cap: env_num(&format!("{prefix}ADHOC_GROUP_CAP"), d.adhoc_group_cap),
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
    /// The data directory every relation's [`relation_dir`] sits under.
    pub(crate) base_dir: String,
    /// Which worker this process is, of how many: names the `w{k}of{n}` child
    /// its stores open under.
    pub(crate) slot: Slot,
    pub(crate) config: StoreConfig,
    /// What this process is to the stores it registered.
    pub(crate) residency: Residency,
    /// Set by [`Self::reconcile_child_dirs`]; [`Self::open_stores`] requires it.
    pub(crate) children_reconciled: bool,
    /// The generation a manifest must carry to be resumed from.
    pub(crate) resume_generation: u64,
    /// See [`RelationRegistry::set_resume_enabled`]. `false` until a host says
    /// otherwise, so one that never answers rebuilds.
    pub(crate) resume_enabled: bool,
    pub(crate) non_resumable: FxHashSet<i64>,
}

impl RelationRegistry {
    /// An empty [`Residency::Origin`] registry over the data directory `base_dir`.
    pub fn new(base_dir: &str, slot: Slot, config: StoreConfig) -> Self {
        RelationRegistry {
            tables: FxHashMap::default(),
            base_dir: base_dir.to_string(),
            slot,
            config: StoreConfig {
                scan_chunk_rows: config.scan_chunk_rows.max(1),
                ..config
            },
            residency: Residency::Origin,
            children_reconciled: false,
            resume_generation: 0,
            resume_enabled: false,
            non_resumable: FxHashSet::default(),
        }
    }

    /// An empty [`Residency::Master`] registry over `base_dir` for `worker_count` workers.
    pub fn master(base_dir: &str, worker_count: u32, config: StoreConfig) -> Self {
        RelationRegistry {
            residency: Residency::Master,
            ..Self::new(base_dir, Slot::new(0, worker_count), config)
        }
    }

    // ── Relation registry ───────────────────────────────────────────────

    /// Drop `id`'s entry, and with it its owned stores and fds.
    pub fn unregister(&mut self, id: i64) {
        self.tables.remove(&id);
    }

    /// Enter a secondary index on `owner` over `cols`, filled from this process's
    /// slice of the owner, or promote the one already on `cols` to unique. On
    /// `Err` nothing is entered. `is_unique` is trusted: a duplicate can straddle
    /// two workers' slices, so only the caller can check it.
    pub fn add_index(&mut self, owner: i64, index_id: i64, cols: &[u32], is_unique: bool) -> Result<(), StoreError> {
        let (owner_schema, owner_dir) = {
            let e = self.index_owner(owner)?;
            (e.schema(), e.directory.clone())
        };
        if let Some(ix) = self.relation_mut(owner).and_then(|e| e.index_on_mut(cols)) {
            ix.is_unique |= is_unique;
            return Ok(());
        }
        let (key_spec, index_schema) =
            crate::schema::index_spec_and_schema(cols, &owner_schema).map_err(StoreError::rejected)?;
        let store = match self.residency.owns_stores() {
            false => Store::detached(index_schema),
            true => Store::owned(
                Self::open_index_table(
                    self.slot,
                    RecoverySource::Rederive { resume_at: None },
                    self.store_budgets(),
                    &owner_dir,
                    index_id,
                    index_schema,
                )?,
                index_schema,
            ),
        };
        let mut ix = SecondaryIndex {
            cols: PkColList::from_slice(cols),
            index_id,
            store,
            key_spec,
            is_unique,
            covers_pk: owner_schema.covers_pk(cols),
        };
        let entry = self.tables.get_mut(&owner).expect("resolved above");
        ingest::fill_indexes(&entry.store, self.config.scan_chunk_rows, owner, &mut [&mut ix])?;
        entry.indexes.push(ix);
        Ok(())
    }

    /// `slot`'s store of index `index_id` over the relation at `owner_dir`.
    fn open_index_table(
        slot: Slot,
        recovery: RecoverySource,
        budgets: StoreBudgets,
        owner_dir: &str,
        index_id: i64,
        schema: SchemaDescriptor,
    ) -> Result<Box<Table>, StoreError> {
        let dir = ChildAddr { kind: ChildKind::Index(index_id), slot }.dir(owner_dir);
        Table::new(&dir, schema, recovery, budgets)
            .map(Box::new)
            .map_err(|e| StoreError::storage(format!("open index {index_id} (dir={dir})"), e))
    }

    /// Remove `id`'s circuit on `cols`, dropping its store. A no-op when no such
    /// circuit is registered.
    pub fn remove_index(&mut self, id: i64, cols: &[u32]) {
        if let Some(entry) = self.tables.get_mut(&id) {
            entry.indexes.retain(|ix| ix.cols.as_slice() != cols);
        }
    }

    /// Set the uniqueness flag of the one circuit on `cols` in place.
    pub fn set_index_unique(&mut self, id: i64, cols: &[u32], is_unique: bool) {
        if let Some(ix) = self.relation_mut(id).and_then(|e| e.index_on_mut(cols)) {
            ix.is_unique = is_unique;
        }
    }

    /// Publish a new column schema for a registered base table in place (any
    /// column ALTER): update the registry copy and push the same value down into
    /// the owned `Table`, which rebinds its shards if the region count grew.
    pub fn swap_schema(&mut self, id: i64, schema: SchemaDescriptor) -> Result<(), StoreError> {
        let entry = self.relation_mut_or_err(id)?;
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
            .map_err(|e| StoreError::storage(format!("ALTER on table {id}: rebinding shards"), e))
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
        self.relation(id).ok_or_else(|| Self::unregistered(id))
    }

    /// `owner`, if it may carry a secondary index: only a base table can.
    pub fn index_owner(&self, owner: i64) -> Result<&Relation, StoreError> {
        let e = self.relation_or_err(owner)?;
        if !e.kind().is_base_table() {
            return Err(StoreError::rejected(format!(
                "Index: owner {owner} is a {}; only a base table can be indexed",
                e.kind().noun()
            )));
        }
        Ok(e)
    }

    /// [`Self::relation_or_err`] as `&mut`.
    pub fn relation_mut_or_err(&mut self, id: i64) -> Result<&mut Relation, StoreError> {
        self.relation_mut(id).ok_or_else(|| Self::unregistered(id))
    }

    fn unregistered(id: i64) -> StoreError {
        StoreError::rejected(format!("relation {id} is not registered"))
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

    /// The one writer of `resume_generation`. A worker latches it off a
    /// `FlushEph` header, with no verdict in hand.
    pub fn set_resume_generation(&mut self, g: u64) {
        self.resume_generation = g;
    }

    /// Whether persisted derived state may be resumed this boot. The host decides
    /// what invalidates it.
    pub fn set_resume_enabled(&mut self, enabled: bool) {
        self.resume_enabled = enabled;
    }

    /// Replace the set of views whose persisted state must not be resumed: their
    /// stores open empty.
    pub fn set_non_resumable(&mut self, ids: &[i64]) {
        self.non_resumable = ids.iter().copied().collect();
    }

    /// Whether `id` is marked by [`Self::set_non_resumable`] and not yet cleared.
    pub fn is_non_resumable(&self, id: i64) -> bool {
        self.non_resumable.contains(&id)
    }

    /// Every id [`Self::is_non_resumable`] holds for, in no defined order.
    pub fn non_resumable_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.non_resumable.iter().copied()
    }

    /// The generation a manifest must carry to be resumed from.
    pub fn resume_generation(&self) -> u64 {
        self.resume_generation
    }

    /// What every store this registry opens starts from; the two bounded kinds
    /// narrow it.
    pub(crate) fn store_budgets(&self) -> StoreBudgets {
        StoreBudgets::new(self.config.ram_tier_bytes)
    }

    /// The recovery policy for a rederived relation — a view's output store and
    /// operator traces, a secondary index: resume from a manifest at this
    /// registry's resume generation, and only while the host's own verdict
    /// admits a resume at all. The one constructor of a generation-bearing
    /// `RecoverySource`, so no consumer can sample a generation of its own at a
    /// second moment.
    pub(crate) fn rederive_source(&self) -> RecoverySource {
        RecoverySource::Rederive {
            resume_at: self.resume_enabled.then_some(self.resume_generation),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/relation.rs"]
mod tests;
