//! The relation registry — what relations this process holds, and the stores
//! behind them.
//!
//! Every relation enters through [`RelationRegistry::register`].

use gnitz_foundation::env::env_num;
use rustc_hash::FxHashMap;

use gnitz_zset::algebra::index_entries;
use gnitz_zset::schema::key::{key_range_between_cuts, pk_in_range, KeyCut};
use gnitz_zset::schema::{index_spec_and_schema, KeySpec, SchemaDescriptor};

use crate::storage::{RecoverySource, StoreBudgets, Table};
use gnitz_wire::{PkColList, PkKeys, ViewProps};
use gnitz_zset::repr::{Batch, PkSetGather, ReadCursor, StorageError, StoredRow};
use gnitz_zset::schema::Slot;

mod build;
mod circuit_state;
mod delta;
mod dirs;
mod ingest;
mod repartition;
mod store;
mod store_lifecycle;
mod unique_pk;

pub use circuit_state::{CircuitState, StateIdx, StateLayout};
pub(crate) use delta::{delta_round, delta_round_prefix};
pub(crate) use dirs::ensure_dir;
pub use dirs::{lock_data_dir, relation_dir, relations_dir, ChildAddr, ChildKind, DirLock};
pub(crate) use store::Store;

// ---------------------------------------------------------------------------
// Secondary index
// ---------------------------------------------------------------------------

/// Who holds a secondary-index circuit; it lives while one remains.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IndexClaim {
    /// An FK circuit on the owner's column list.
    ForeignKey,
    /// A catalog index.
    Index { id: u64, unique: bool },
}

/// A secondary index on a column list of one relation. Owns this process's store
/// for it — dropping the index drops the store.
pub struct SecondaryIndex {
    store: Store,
    /// The index's declared column list, in order. A 1-element list is the
    /// single-column case. Dedup/lookup is exact ordered-list equality on
    /// `cols.as_slice()`; order is significant (it drives leading-prefix seeks).
    cols: PkColList,
    /// Full-arity span-encode plan, precomputed at registration so the per-push
    /// consumers do no per-call spec rebuild. It survives every column ALTER of
    /// the owner — [`RelationRegistry::swap_schema`] rejects any descriptor that
    /// would not leave it valid.
    key_spec: KeySpec,
    /// Every holder of this circuit.
    claims: Vec<IndexClaim>,
    /// Whether `cols` covers the owner's PK, so the index can never collide.
    covers_pk: bool,
}

impl SecondaryIndex {
    /// By value, not as a slice: `PkColList` is `Copy`, so the list can be keyed
    /// on or held past the borrow of the registry that produced it.
    pub fn cols(&self) -> PkColList {
        self.cols
    }

    pub fn schema(&self) -> SchemaDescriptor {
        *self.store.schema()
    }

    pub fn key_spec(&self) -> KeySpec {
        self.key_spec
    }

    pub fn is_unique(&self) -> bool {
        self.claims
            .iter()
            .any(|c| matches!(c, IndexClaim::Index { unique: true, .. }))
    }

    /// Every holder of this circuit.
    pub fn claims(&self) -> &[IndexClaim] {
        &self.claims
    }

    /// Non-compacting cursor over this index's store, in the index schema.
    pub fn cursor(&self) -> ReadCursor {
        self.store.held().open_cursor()
    }

    /// A cursor positioned on the key band `r` names under this index's key spec.
    pub(crate) fn cursor_over(&self, r: &gnitz_wire::KeyRange) -> ReadCursor {
        let t = self.store.held();
        t.range_cursor(self.key_spec.range_keys(t.schema().pk_stride(), r))
    }

    /// Project `source` into this index's layout and ingest the result.
    /// `Ok(false)` when the projection was empty: the key spec rejects rows, so
    /// a non-empty `source` can still project to nothing.
    pub(crate) fn project_and_ingest(&mut self, source: &Batch) -> Result<bool, StorageError> {
        let table = self.store.held_mut();
        let projected = index_entries(source, &self.key_spec, table.schema());
        if projected.is_empty() {
            return Ok(false);
        }
        table.ingest_owned_batch(projected).map(|()| true)
    }

    /// Whether this process's store of the index came back from a checkpoint
    /// manifest at its open.
    pub fn resumed(&self) -> bool {
        self.store.held().resumed_from_checkpoint()
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
    /// What a client is told this relation is.
    #[inline]
    pub fn class(self) -> gnitz_wire::RelClass {
        match self {
            RelationKind::Stream => gnitz_wire::RelClass::Stream,
            RelationKind::View(props) => props.into(),
            RelationKind::BaseTable | RelationKind::SystemCatalog => gnitz_wire::RelClass::Table,
        }
    }

    /// What to call this relation in a message to the user.
    #[inline]
    pub fn noun(self) -> &'static str {
        self.class().noun()
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
/// store for it.
pub struct Relation {
    /// The relation id — the key it is registered under, held here too so a
    /// borrowed `Relation` names itself: nothing that has one needs the id
    /// threaded in beside it to log or to phrase an error.
    id: u64,
    store: Store,
    delta: Option<Box<Table>>,
    indexes: Vec<SecondaryIndex>,
    kind: RelationKind,
}

impl Relation {
    /// This relation's delta store, if this process serves its feed.
    pub(crate) fn delta(&self) -> Option<&Table> {
        self.delta.as_deref()
    }

    /// By value, for the reason [`SecondaryIndex::cols`] is: the `tables` borrow
    /// ends with the call, so the descriptor survives a later `&mut` on the
    /// registry.
    pub fn schema(&self) -> SchemaDescriptor {
        *self.store.schema()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn kind(&self) -> RelationKind {
        self.kind
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
        self.indexes.iter().filter(|ic| ic.is_unique() && !ic.covers_pk)
    }

    /// Non-compacting cursor over this relation's store.
    pub fn cursor(&self) -> ReadCursor {
        self.store.held().open_cursor()
    }

    /// Every live row of `keys`. With `unticked`, the store is read as it was before
    /// those ingests.
    pub fn gather(&self, keys: PkKeys, unticked: Option<&Batch>) -> PkSetGather {
        let table = self.store.held();
        let undo = unticked.zip(keys.bounds()).map(|(b, (first, last))| {
            let in_range: Vec<u32> = (0..b.len())
                .filter(|&i| b.get_weight(i) != 0 && pk_in_range(first, last, b.get_pk_bytes(i)))
                .map(|i| i as u32)
                .collect();
            let undo = b.ascending_subset(&in_range).into_consolidated().negated();
            std::rc::Rc::new(undo)
        });
        table.gather(keys, undo)
    }

    /// Visit every positive-weight row whose OPK key begins with `prefix`.
    pub fn for_each_positive_with_prefix(&self, prefix: &[u8], f: impl FnMut(&ReadCursor)) {
        let table = self.store.held();
        let band = key_range_between_cuts(
            KeyCut::min_of(prefix),
            KeyCut::above(prefix),
            table.schema().pk_stride(),
        );
        table.range_cursor(band).for_each_positive_while(|_| true, f);
    }

    /// Materialize every row of this relation's store whose net weight is non-zero.
    pub fn full_scan(&self) -> std::rc::Rc<Batch> {
        self.store.held().full_scan()
    }

    /// Whether a live row carries this OPK key.
    pub fn has_pk(&self, key: &[u8]) -> bool {
        self.store.held().has_pk_bytes(key)
    }

    /// The net weight at `key`, and the live row if there is one.
    pub fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        self.store.held().live_row_at(key)
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
    pub(crate) fn index_on_mut(&mut self, cols: &[u32]) -> Option<&mut SecondaryIndex> {
        self.indexes.iter_mut().find(|ix| ix.cols.as_slice() == cols)
    }

    /// This process's store, for the crate's own read and write paths.
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// `cols`, if each names a column of this relation.
    pub(crate) fn bound_cols(&self, cols: PkColList, op: &str) -> Result<PkColList, String> {
        let schema = self.schema();
        match cols.as_slice().iter().all(|&c| schema.column(c as usize).is_some()) {
            true => Ok(cols),
            false => Err(format!("{op}: invalid column list for table {}", self.id)),
        }
    }
}

// ---------------------------------------------------------------------------
// The relation to register
// ---------------------------------------------------------------------------

/// A relation to [`RelationRegistry::register`].
#[derive(Clone, Copy)]
pub struct RelationSpec {
    pub id: u64,
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
    pub(crate) tables: FxHashMap<u64, Relation>,
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
    pub fn unregister(&mut self, id: u64) {
        self.tables.remove(&id);
    }

    /// Enter a secondary index on `owner` over `cols`, filled from this process's
    /// slice of the owner, or add a claim to the one already on `cols`. On
    /// `Err` nothing is entered. A claim's `unique` is trusted: a duplicate can
    /// straddle two workers' slices, so only the caller can check it.
    pub fn add_index(&mut self, owner: u64, claim: IndexClaim, cols: &[u32]) -> Result<(), String> {
        let owner_schema = self.index_owner(owner)?.schema();
        if let Some(ix) = self.relation_mut(owner).and_then(|e| e.index_on_mut(cols)) {
            // The IDX_TAB net bound keeps one live row per id, applied once per process.
            debug_assert!(!ix.claims.contains(&claim), "{claim:?} claimed twice");
            ix.claims.push(claim);
            return Ok(());
        }
        let (key_spec, index_schema) = index_spec_and_schema(cols, &owner_schema)?;
        let mut ix = SecondaryIndex {
            cols: PkColList::from_slice(cols),
            store: Store::Absent(Box::new(index_schema)),
            key_spec,
            claims: vec![claim],
            covers_pk: owner_schema.covers_pk(cols),
        };
        if self.residency.owns_stores() {
            ix.store = Store::Held(Box::new(self.open_child(
                owner,
                ChildKind::Index(ix.cols),
                index_schema,
                self.rederive_source(false),
                self.store_budgets(),
            )?));
            let owner_store = &self.tables[&owner].store;
            ingest::fill_indexes(owner_store, self.config.scan_chunk_rows, owner, &mut [&mut ix])?;
        }
        self.tables.get_mut(&owner).expect("resolved above").indexes.push(ix);
        Ok(())
    }

    /// Remove catalog index `index_id`'s claim from whichever of `owner`'s
    /// circuits holds it, dropping a circuit left with none. A no-op when the
    /// owner or claim is absent.
    pub fn release_index(&mut self, owner: u64, index_id: u64) {
        let Some(entry) = self.tables.get_mut(&owner) else {
            return;
        };
        for ix in &mut entry.indexes {
            ix.claims
                .retain(|c| !matches!(*c, IndexClaim::Index { id, .. } if id == index_id));
        }
        entry.indexes.retain(|ix| !ix.claims.is_empty());
    }

    /// Publish a new column schema for a registered base table in place (any
    /// column ALTER): update the registry copy and push the same value down into
    /// the owned `Table`, which rebinds its shards if the region count grew.
    pub fn swap_schema(&mut self, id: u64, schema: SchemaDescriptor) -> Result<(), String> {
        let entry = self.relation_mut_or_err(id)?;
        // Checked, not asserted: what a stale `key_spec` produces is a silently
        // wrong index projection, which release codegen would not guard at all.
        if !schema.is_trailing_append_of(entry.store.schema()) {
            return Err(format!(
                "ALTER on table {id}: the new descriptor is not a trailing append, \
                 which every index circuit's baked key_spec requires"
            ));
        }
        entry
            .store
            .swap_schema(schema)
            .map_err(|e| format!("ALTER on table {id}: rebinding shards: {e}"))
    }

    // ── Registry reads ──────────────────────────────────────────────────

    pub fn has_id(&self, id: u64) -> bool {
        self.tables.contains_key(&id)
    }

    /// The data directory every relation's [`relation_dir`] sits under.
    pub fn base_dir(&self) -> &str {
        &self.base_dir
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// This process's `kind` child directory of relation `id`.
    pub fn child_dir(&self, id: u64, kind: ChildKind<'_>) -> String {
        ChildAddr { kind, slot: self.slot }.dir(&relation_dir(&self.base_dir, id))
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
    pub fn relation(&self, id: u64) -> Option<&Relation> {
        self.tables.get(&id)
    }

    /// [`Self::relation`] as `&mut` — the one mutable route into a relation, so
    /// navigation reads the same in both directions.
    pub(crate) fn relation_mut(&mut self, id: u64) -> Option<&mut Relation> {
        self.tables.get_mut(&id)
    }

    /// [`Self::relation`] plus the one "not registered" sentence — spelled the
    /// same by the mutating paths, so which verb asked cannot change what a
    /// client reads.
    pub fn relation_or_err(&self, id: u64) -> Result<&Relation, String> {
        self.relation(id).ok_or_else(|| Self::unregistered(id))
    }

    /// `owner`, if it may carry a secondary index: only a base table can.
    pub fn index_owner(&self, owner: u64) -> Result<&Relation, String> {
        let e = self.relation_or_err(owner)?;
        if !e.kind().is_base_table() {
            return Err(format!(
                "Index: owner {owner} is a {}; only a base table can be indexed",
                e.kind().noun()
            ));
        }
        Ok(e)
    }

    /// [`Self::relation_or_err`] as `&mut`.
    pub(crate) fn relation_mut_or_err(&mut self, id: u64) -> Result<&mut Relation, String> {
        self.relation_mut(id).ok_or_else(|| Self::unregistered(id))
    }

    fn unregistered(id: u64) -> String {
        format!("relation {id} is not registered")
    }

    /// True iff at least one registered relation carries a delta feed.
    pub fn any_delta_feed(&self) -> bool {
        self.tables.values().any(Relation::has_delta_feed)
    }

    /// Every registered view id.
    pub fn view_ids(&self) -> Vec<u64> {
        self.tables
            .iter()
            .filter(|(_, e)| e.kind.is_view())
            .map(|(&id, _)| id)
            .collect()
    }

    // ── The resume fence ────────────────────────────────────────────────

    /// The one writer of `resume_generation`. A worker latches it off a
    /// `FlushEph` header, with no verdict in hand.
    pub fn set_resume_generation(&mut self, g: u64) {
        self.resume_generation = g;
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

    /// A rederived store's recovery: rebuilt, or with `resume` from a manifest at
    /// this registry's resume generation.
    pub(crate) fn rederive_source(&self, resume: bool) -> RecoverySource {
        RecoverySource::Rederive {
            resume_at: resume.then_some(self.resume_generation),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/relation.rs"]
mod tests;
