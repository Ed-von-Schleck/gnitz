//! The relation registry — what relations this process holds and the stores
//! behind them, and the [`Snapshot`]s of those stores it hands out.
//!
//! Every relation enters through [`RelationRegistry::register_at`].

use gnitz_foundation::env::env_num;
use rustc_hash::FxHashMap;

use gnitz_zset::algebra::{KeySpec, index_entries};
use gnitz_zset::schema::SchemaDescriptor;

pub use crate::storage::Cut;
use crate::storage::{RecoverySource, Table};
use gnitz_trace::runs::{PkSetGather, ReadCursor, StorageError, StoredRow};
use gnitz_wire::{PkColList, PkKeys, ViewBudget};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::repr::Batch;

mod build;
mod circuit_state;
mod dirs;
mod disk_usage;
mod feed;
mod ingest;
mod repartition;
mod snapshot;
mod store;
mod unique_pk;

pub use circuit_state::{CircuitState, StateIdx, StateLayout};
pub use dirs::{ChildAddr, ChildKind, DirLock, lock_data_dir, relation_dir};
pub use disk_usage::{DiskUsage, disk_usage};
use feed::Feed;
pub use snapshot::Snapshot;
use store::{Rows, not_held};
pub use unique_pk::PkRule;

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
    /// The `CREATE UNIQUE INDEX` pre-flight, which reads its spans off the index
    /// and leaves it for its DDL to claim on commit.
    Preflight,
}

/// What backs one process's entries of a secondary index.
enum IndexStore {
    /// A store of its own, which this process holds.
    Own(Box<Table>),
    /// The owner's rows: the index's columns lead the owner's PK, so an owner
    /// row's entry `[span ‖ PK]` is its PK with the span's leading bytes
    /// repeated, in the owner's PK order. No process keeps a store for it.
    OwnerPk,
    /// A store of its own, which this process does not hold because it holds no
    /// store of the owner's rows: the master's state.
    NotHeld,
}

/// A secondary index on a column list of one relation. Owns this process's store
/// for it, if it keeps one — dropping the index drops the store.
pub struct SecondaryIndex {
    store: IndexStore,
    /// The index's declared column list, in order, which drives leading-prefix
    /// seeks. A relation holds at most one index per list.
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
    pub fn cols(&self) -> PkColList {
        self.cols
    }

    pub fn key_spec(&self) -> &KeySpec {
        &self.key_spec
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

    /// Whether the index's entries are read off its owner's rows, so no process
    /// keeps a store for it.
    pub fn reads_owner_pk(&self) -> bool {
        matches!(self.store, IndexStore::OwnerPk)
    }

    /// This process's store of the index; panics when it holds none.
    #[track_caller]
    fn held(&self) -> &Table {
        match &self.store {
            IndexStore::Own(t) => t,
            IndexStore::OwnerPk | IndexStore::NotHeld => not_held(),
        }
    }

    /// Non-compacting cursor over this index's own store, in the index schema;
    /// panics on an index that [reads its owner's PK](Self::reads_owner_pk).
    pub fn cursor(&self) -> ReadCursor {
        self.held().open_cursor(Cut::Now)
    }

    /// A non-compacting cursor whose rows each lead their key with one entry's
    /// span, in span order: this index's store, or `owner`'s.
    pub(crate) fn span_cursor(&self, owner: &Relation) -> ReadCursor {
        match self.store {
            // A span is a prefix of the owner's PK, so no payload column is read.
            IndexStore::OwnerPk => owner.open_cursor(Cut::Now, 0),
            _ => self.cursor(),
        }
    }

    /// Call `f` on each live entry `[span ‖ owner PK]` of `spans`, each this
    /// index's leading key bytes, in span order and at most `cap` of them.
    pub(crate) fn for_each_entry(&self, owner: &Relation, spans: PkKeys, cap: usize, mut f: impl FnMut(&[u8])) {
        match self.store {
            IndexStore::OwnerPk => {
                let key_size = self.key_spec.key_size();
                let mut entry = Vec::with_capacity(key_size + owner.schema().pk_stride());
                // A span is a prefix of the owner's PK, so no payload column is read.
                owner.gather(spans, Cut::Now, 0).for_each_positive_capped(cap, |c| {
                    let pk = c.current_pk_bytes();
                    entry.clear();
                    entry.extend_from_slice(&pk[..key_size]);
                    entry.extend_from_slice(pk);
                    f(&entry);
                });
            }
            _ => self
                .held()
                .gather(spans, Cut::Now)
                .for_each_positive_capped(cap, |c| f(c.current_pk_bytes())),
        }
    }

    /// A cursor positioned on the key band `r` walks over this index's own
    /// store. An index that reads its owner's PK has none: a range on its
    /// columns walks the owner's PK.
    pub(crate) fn cursor_over(&self, r: &gnitz_wire::KeyRange) -> ReadCursor {
        self.held().range_cursor(r, Cut::Now)
    }

    /// This process's own store of the index, if it holds one.
    fn table_mut(&mut self) -> Option<&mut Table> {
        match &mut self.store {
            IndexStore::Own(t) => Some(t),
            IndexStore::OwnerPk | IndexStore::NotHeld => None,
        }
    }

    /// Project `source` into this index's layout and ingest the result. A
    /// zero-weight row, or one NULL in an indexed column, has no entry, so a
    /// non-empty `source` can project to nothing. A no-op for an index that
    /// reads its owner's PK.
    fn project_and_ingest(&mut self, source: &Batch) -> Result<(), StorageError> {
        let table = match &mut self.store {
            IndexStore::Own(t) => t,
            IndexStore::OwnerPk => return Ok(()),
            IndexStore::NotHeld => not_held(),
        };
        let projected = index_entries(source, &self.key_spec, table.schema());
        if projected.is_empty() {
            return Ok(());
        }
        table.ingest(projected)
    }

    /// Whether this process's own store of the index came back from a
    /// checkpoint manifest at its open; `false` for one that reads its owner's
    /// PK.
    pub fn resumed(&self) -> bool {
        match &self.store {
            IndexStore::Own(t) => t.loaded_mark().is_some(),
            IndexStore::OwnerPk => false,
            IndexStore::NotHeld => not_held(),
        }
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
    /// sources; owns a DML-enforced PK, so every ingest is held to a
    /// [`PkRule`] and its accumulated per-PK weight is always in {0, 1}.
    BaseTable,
    /// Materialised view, with its `WITH (…)` options: ephemeral, resumed from a
    /// generation-stamped manifest when the ephemeral checkpoint round left one.
    ///
    /// Not necessarily *rebuilt* from its sources: a mirror registers its fed
    /// copies under this kind too.
    View(ViewBudget),
    /// Ingestion point: storeless, append-only. Pushed rows exist
    /// only as the deltas they produce; nothing is retained, so there is nothing
    /// to recover and no store to read.
    Stream,
}

impl RelationKind {
    /// Whether every key of a store of this kind nets 0 or 1, so the newest run
    /// holding a key decides it.
    #[inline]
    pub(crate) fn nets_unit_per_key(self) -> bool {
        matches!(self, RelationKind::BaseTable | RelationKind::SystemCatalog)
    }

    /// What a client is told this relation is.
    #[inline]
    pub fn class(self) -> gnitz_wire::RelClass {
        match self {
            RelationKind::Stream => gnitz_wire::RelClass::Stream,
            RelationKind::View(props) => props.into(),
            RelationKind::BaseTable => gnitz_wire::RelClass::Table,
            RelationKind::SystemCatalog => gnitz_wire::RelClass::Catalog,
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

    /// Whether this kind of relation may carry a secondary index.
    fn admits_index(self) -> bool {
        match self {
            RelationKind::BaseTable | RelationKind::View(ViewBudget::Plain | ViewBudget::Fed { .. }) => true,
            // A bounded view's sweep drops the payload an entry is projected from.
            RelationKind::View(ViewBudget::Bounded { .. }) | RelationKind::Stream | RelationKind::SystemCatalog => {
                false
            }
        }
    }

    /// Whether this relation keeps a delta feed, whether or not this process holds
    /// its store.
    #[inline]
    pub fn has_delta_feed(self) -> bool {
        matches!(self, RelationKind::View(ViewBudget::Fed { .. }))
    }

    /// Whether a capacity bounds this relation's registered shard bytes, so its
    /// sweep may leave skeleton rows behind.
    #[inline]
    pub fn is_bounded(self) -> bool {
        matches!(self, RelationKind::View(ViewBudget::Bounded { .. }))
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
    /// A standalone host — a mirror, a unit-test engine or fixture: the one
    /// process on its data directory, owns every store it registers, at its
    /// slot, and may read any rank's manifests.
    Origin,
    /// A forked worker, at its own slot. Owns its stores; speaks for itself.
    Worker,
}

impl Residency {
    /// Whether this process writes, and answers for, the stores of a `kind`
    /// relation: the master the system families', a worker every user
    /// relation's and index's (it holds its copies of the system families in
    /// RAM only), an origin all of them. A stream has no store anywhere, so the
    /// answer for one opens nothing.
    #[inline]
    fn publishes(self, kind: RelationKind) -> bool {
        match self {
            Residency::Master => kind == RelationKind::SystemCatalog,
            Residency::Worker => kind != RelationKind::SystemCatalog,
            Residency::Origin => true,
        }
    }
}

// ---------------------------------------------------------------------------
// Relation — one relation in this process
// ---------------------------------------------------------------------------

/// One relation in this process: its identity, its shape, and this process's
/// store for it.
pub struct Relation {
    /// What it was registered as. Its id is the key it is registered under, held
    /// here too so a borrowed `Relation` names itself: nothing that has one needs
    /// the id threaded in beside it to log or to phrase an error.
    spec: RelationSpec,
    rows: Rows,
    indexes: Vec<SecondaryIndex>,
    /// The operator traces a view's compiled circuit keeps, each a child store
    /// of its own; none for any other relation.
    traces: StateLayout,
}

impl Relation {
    /// This relation's delta feed, if this process serves it.
    pub(crate) fn feed(&self) -> Option<&Feed> {
        self.rows.feed()
    }

    /// By value: the borrow of `relations` ends with the call, so the descriptor
    /// survives a later `&mut` on the registry.
    pub fn schema(&self) -> SchemaDescriptor {
        *self.rows.schema()
    }

    pub fn id(&self) -> u64 {
        self.spec.id
    }

    pub fn kind(&self) -> RelationKind {
        self.spec.kind
    }

    /// Where this relation's rows live.
    pub fn placement(&self) -> Placement {
        self.spec.placement
    }

    /// `Ok` iff this relation may carry a secondary index, `unique` or not.
    fn admit_index(&self, unique: bool) -> Result<(), String> {
        let (id, kind) = (self.id(), self.kind());
        if !kind.admits_index() {
            return Err(format!(
                "Index: owner {id} is a {}; only a base table or a view without a capacity can be indexed",
                kind.noun()
            ));
        }
        if unique && kind.is_view() {
            return Err(format!(
                "Index: view {id} cannot carry a UNIQUE index: its circuit cannot refuse a duplicate"
            ));
        }
        // An index walk gathers every row under an in-range entry's source PK, so
        // a second row under that PK would come back whatever its own key.
        if self.pk_repeats() {
            return Err(format!(
                "Index: owner {id} repeats its primary key, so an index entry does not name one row"
            ));
        }
        Ok(())
    }

    /// Whether two of its rows may share a PK.
    pub fn pk_repeats(&self) -> bool {
        self.spec.pk_repeats
    }

    /// The payload columns, as payload bits, a drain of this relation's store
    /// writes as zeroes for a reader of `read` alone: every other one where no
    /// two live rows share a PK, whose rows then stay distinct and its chunks
    /// consolidated with those columns zeroed; none otherwise.
    pub(crate) fn unread(&self, read: u64) -> u64 {
        match self.pk_repeats() {
            true => 0,
            false => self.schema().payload_slots() & !read,
        }
    }

    /// The unique secondary indexes a write must still check: one covering the
    /// PK cannot collide, so it is not among them.
    pub fn unique_indexes_to_check(&self) -> impl Iterator<Item = &SecondaryIndex> {
        self.indexes.iter().filter(|ic| ic.is_unique() && !ic.covers_pk)
    }

    /// Non-compacting cursor over this relation's store.
    pub fn cursor(&self) -> ReadCursor {
        self.table().open_cursor(Cut::Now)
    }

    /// Non-compacting cursor over the rows `cut` reads, for a reader of the
    /// payload columns `read` names, as payload bits: the cursor may write every
    /// other column as zeroes.
    pub(crate) fn open_cursor(&self, cut: Cut, read: u64) -> ReadCursor {
        let mut cursor = self.table().open_cursor(cut);
        cursor.skip_payload(self.unread(read));
        cursor
    }

    /// Every live row of `keys` — whole PKs, or the same leading columns of one,
    /// each then naming every row it prefixes — among the rows `cut` reads, for
    /// a reader of the payload columns `read` names, as payload bits.
    pub fn gather(&self, keys: PkKeys, cut: Cut, read: u64) -> PkSetGather {
        let mut gather = self.table().gather(keys, cut);
        gather.skip_payload(self.unread(read));
        gather
    }

    /// A cursor for probing at the keys in `[first, last]` — whole PKs, or the
    /// same leading bytes of one — positioned on the band they span, over the
    /// rows `cut` reads.
    pub fn cursor_between(&self, first: &[u8], last: &[u8], cut: Cut) -> ReadCursor {
        self.table().cursor_between(first, last, cut)
    }

    /// Materialize every row of this relation's store whose net weight is non-zero.
    pub fn full_scan(&self) -> std::rc::Rc<Batch> {
        self.table().full_scan()
    }

    /// The live row at `key`, if there is one, of a base table or a system
    /// family, whose every key nets 0 or 1.
    pub fn unique_live_row_at(&self, key: &[u8]) -> Option<StoredRow> {
        debug_assert!(self.kind().nets_unit_per_key());
        self.table().unique_live_row_at(key)
    }

    /// The checkpoint mark this process's store of the relation resumed from;
    /// `None` when its open loaded no manifest. A view's operator traces resume
    /// at this mark. Panics when this process holds no store of the relation.
    pub fn loaded_mark(&self) -> Option<u64> {
        self.table().loaded_mark()
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

    /// Remove `claim` from every index, dropping an index left with none.
    fn release(&mut self, claim: IndexClaim) {
        self.indexes.retain_mut(|ix| {
            ix.claims.retain(|&c| c != claim);
            !ix.claims.is_empty()
        });
    }

    /// [`Self::index_on`], `Err` when there is none.
    pub(crate) fn index_or_err(&self, cols: &[u32]) -> Result<&SecondaryIndex, String> {
        self.index_on(cols)
            .ok_or_else(|| format!("relation {} has no index on columns {cols:?}", self.id()))
    }

    /// This process's store of the relation; panics when it holds none.
    #[track_caller]
    pub(crate) fn table(&self) -> &Table {
        let Some(t) = self.rows.table() else { not_held() };
        t
    }

    /// [`Self::table`] as `&mut`.
    #[track_caller]
    fn table_mut(&mut self) -> &mut Table {
        let Some(t) = self.rows.table_mut() else { not_held() };
        t
    }

    /// [`Self::table`], `Err` when this process holds no store of the relation.
    pub(crate) fn table_or_err(&self) -> Result<&Table, String> {
        self.rows.table().ok_or_else(|| no_store(self.kind(), self.id()))
    }
}

/// Why this process holds no store of relation `id`.
fn no_store(kind: RelationKind, id: u64) -> String {
    match kind {
        RelationKind::Stream => {
            format!("relation {id} is a stream: a stream holds no rows and cannot be read")
        }
        _ => format!("relation {id} holds no store in this process"),
    }
}

// ---------------------------------------------------------------------------
// The relation to register
// ---------------------------------------------------------------------------

/// What a relation is registered as: everything about it that is fixed from its
/// registration to its drop. Its schema is not — a column ALTER swaps it — so it
/// is registered beside the spec and lives in the relation's rows.
#[derive(Clone, Copy)]
pub struct RelationSpec {
    pub id: u64,
    pub kind: RelationKind,
    pub placement: Placement,
    /// Whether two of its rows may share a PK. `false` is a promise the index
    /// gather and a drain that zeroes unread columns rely on, so a host that
    /// cannot vouch for it passes `true`.
    pub pk_repeats: bool,
    /// `WITH (compress = true)`: its rows' store codes its shards' heaps under
    /// a symbol table where that pays.
    pub compress: bool,
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
    /// peak scan memory at O(chunk × row_width). A registry clamps it to at
    /// least one row.
    pub scan_chunk_rows: usize,
    /// Per-worker distinct-group cap for the ad-hoc aggregate fold; bounds the
    /// fold's running values at one per aggregate per group.
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
/// process is to those stores.
///
/// **Thread contract.** `!Send + !Sync` by auto-trait: every store holds its
/// runs behind `Rc`. Nothing beneath a registry is thread-affine, so a host may
/// move one to another thread while it has exclusive access to it and no
/// `Rc`-bearing value it handed out is still alive outside it: those refcounts
/// are non-atomic.
pub struct RelationRegistry {
    relations: FxHashMap<u64, Relation>,
    base_dir: String,
    /// Which worker this process is, of how many: names the `w{k}of{n}` child
    /// its stores open under.
    slot: Slot,
    pub(crate) config: StoreConfig,
    /// What this process is to the stores it registered.
    residency: Residency,
}

impl RelationRegistry {
    /// An empty [`Residency::Origin`] registry over the data directory `base_dir`.
    pub fn new(base_dir: &str, slot: Slot, config: StoreConfig) -> Self {
        RelationRegistry {
            relations: FxHashMap::default(),
            base_dir: base_dir.to_string(),
            slot,
            config: StoreConfig {
                scan_chunk_rows: config.scan_chunk_rows.max(1),
                ..config
            },
            residency: Residency::Origin,
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
        self.relations.remove(&id);
    }

    /// Remove `claim` from whichever of `owner`'s circuits hold it, dropping a
    /// circuit left with none. A no-op when the owner or claim is absent.
    pub fn release_index(&mut self, owner: u64, claim: IndexClaim) {
        if let Some(entry) = self.relations.get_mut(&owner) {
            entry.release(claim);
        }
    }

    /// [`Self::release_index`] over every relation.
    pub fn release_claim(&mut self, claim: IndexClaim) {
        self.relations.values_mut().for_each(|entry| entry.release(claim));
    }

    /// Publish a new column schema for base table `id` in place (any column
    /// ALTER) into its store, where a held `Table` rebinds its shards to it if
    /// it changed. Refuses any other kind of relation, and a descriptor that is
    /// not a trailing append of the stored one.
    pub fn swap_schema(&mut self, id: u64, schema: SchemaDescriptor) -> Result<(), String> {
        let entry = self.relation_mut_or_err(id)?;
        if !entry.kind().is_base_table() {
            return Err(format!("ALTER on relation {id}: not a base table"));
        }
        // Checked, not asserted: what a stale `key_spec` produces is a silently
        // wrong index projection, which release codegen would not guard at all.
        if !schema.is_trailing_append_of(entry.rows.schema()) {
            return Err(format!(
                "ALTER on table {id}: the new descriptor is not a trailing append of the stored one"
            ));
        }
        entry
            .rows
            .swap_schema(schema)
            .map_err(|e| format!("ALTER on table {id}: rebinding shards: {e}"))
    }

    // ── Registry reads ──────────────────────────────────────────────────

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

    /// See [`StoreConfig::scan_chunk_rows`].
    pub fn scan_chunk_rows(&self) -> usize {
        self.config.scan_chunk_rows
    }

    /// The relation `id` names, or `None` for an unknown id.
    pub fn relation(&self, id: u64) -> Option<&Relation> {
        self.relations.get(&id)
    }

    /// [`Self::relation`] plus the one "not registered" sentence — spelled the
    /// same by the mutating paths.
    pub fn relation_or_err(&self, id: u64) -> Result<&Relation, String> {
        self.relation(id).ok_or_else(|| Self::unregistered(id))
    }

    /// `owner`, if it may carry a secondary index, `unique` or not.
    pub fn index_owner(&self, owner: u64, unique: bool) -> Result<&Relation, String> {
        let e = self.relation_or_err(owner)?;
        e.admit_index(unique)?;
        Ok(e)
    }

    /// [`Self::relation_or_err`] as `&mut`.
    fn relation_mut_or_err(&mut self, id: u64) -> Result<&mut Relation, String> {
        self.relations.get_mut(&id).ok_or_else(|| Self::unregistered(id))
    }

    fn unregistered(id: u64) -> String {
        format!("relation {id} is not registered")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/relation.rs"]
mod tests;
