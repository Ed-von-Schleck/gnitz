//! `StoreHandle` — the storage adapter for a registered relation, and the
//! single inbound target catalog reaches for. `Owned` owns this worker's whole
//! slice of the relation as one boxed `Table`; `Borrowed` is a non-owning
//! pointer to a system table owned by `CatalogEngine`; `Detached` is a relation
//! this process holds no store for. There is no custom `Drop`: the `Owned` box
//! is freed by the default drop glue when its registry entry is removed.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor, StorageError, Table};
use std::cell::UnsafeCell;

/// Storage handle of a registered relation.
pub(crate) enum StoreHandle {
    /// Owned `Table` — this worker's whole slice of a base table or view.
    /// Wrapped in `UnsafeCell` so the interior-mutable accessors can hand out
    /// `&mut` through a shared `&self` without violating Stacked Borrows (a raw
    /// pointer derived from a shared reference may not be used to mutate).
    Owned(UnsafeCell<Box<Table>>),
    /// Registered, but this process holds no store for this relation: every read
    /// through it is empty and every write a no-op. Either the post-fork master,
    /// which moves every user relation here so master and worker 0 do not both hold
    /// a live `Table` on `w0of{W}`, or a stream, which holds no store anywhere.
    Detached,
    /// Non-owning pointer to a `Table` owned elsewhere (system tables).
    Borrowed(*mut Table),
}

impl StoreHandle {
    // ------------------------------------------------------------------
    // Interior-mutable accessors
    //
    // DagEngine/CatalogEngine are single-threaded (!Sync) and the
    // HashMap<id, TableEntry> stores owning Boxes whose heap allocations have
    // stable addresses, so the mutation is race-free. But the registry HashMap
    // is read via immutable get(), which would normally prevent handing out
    // &mut to the owned Table. These three encapsulate the raw-pointer re-borrow
    // that reconciles the lookup API with the mutation need. The &mut is handed
    // out under the contract below, not derived from &self by reborrow — so
    // clippy's mut_from_ref does not apply.
    //
    // SAFETY contract for every method below: no aliasing &mut into the same
    // storage may be live across the call. `table` is the shared reborrow and
    // `table_mut` the unique one; routing a read through the latter would widen
    // that contract to "no reference at all live", silently.
    // ------------------------------------------------------------------

    /// Every variant's `Table`, or `None` when this process holds no store.
    fn table(&self) -> Option<&Table> {
        match self {
            StoreHandle::Owned(cell) => Some(unsafe { &**cell.get() }),
            StoreHandle::Borrowed(ptr) => Some(unsafe { &**ptr }),
            StoreHandle::Detached => None,
        }
    }

    /// [`Self::table`] as `&mut`.
    #[allow(clippy::mut_from_ref)]
    fn table_mut(&self) -> Option<&mut Table> {
        match self {
            StoreHandle::Owned(cell) => Some(unsafe { &mut **cell.get() }),
            StoreHandle::Borrowed(ptr) => Some(unsafe { &mut **ptr }),
            StoreHandle::Detached => None,
        }
    }

    /// The `Table` this process owns **outright** — `None` for a borrowed system
    /// table as well as for a detached relation. A different question from
    /// [`Self::table_mut`], and the reason its callers cannot move to it: a
    /// worker must not barrier-flush the `_sys` copy it inherited
    /// (`collect_base_flush_tables`), and a column ALTER must not publish its new
    /// descriptor into a `Table` whose real owner publishes too
    /// (`swap_table_schema`).
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn as_owned_mut(&self) -> Option<&mut Table> {
        match self {
            StoreHandle::Owned(cell) => Some(unsafe { &mut **cell.get() }),
            _ => None,
        }
    }

    /// True for a storeless handle — the post-fork master's, or a stream's.
    /// Test-only: production asks a total accessor what it can reach instead of
    /// branching on the variant.
    #[cfg(test)]
    pub(crate) fn is_detached(&self) -> bool {
        matches!(self, StoreHandle::Detached)
    }

    /// Dispatched non-compacting `open_cursor` across all variants. A detached
    /// relation opens an empty cursor of `schema` — the same answer a store
    /// holding none of the requested rows gives. Infallible, non-mutating.
    /// Callers reach this through [`TableEntry::open_cursor`], which supplies
    /// the registry's own schema.
    pub(super) fn open_cursor(&self, schema: &SchemaDescriptor) -> ReadCursor {
        match self.table() {
            Some(t) => t.open_cursor(),
            None => crate::storage::empty_cursor(*schema),
        }
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// A detached relation reads empty, so it never can.
    pub(super) fn has_skeleton_rows(&self) -> bool {
        self.table().is_some_and(Table::has_skeleton_rows)
    }

    /// Materialize every positive-weight row, delegating to `Table::full_scan`
    /// so its `Rc` snapshot cache is preserved; a detached relation materializes
    /// an empty batch. Reached through [`TableEntry::full_scan`].
    pub(super) fn full_scan(&self, schema: &SchemaDescriptor) -> std::rc::Rc<Batch> {
        match self.table_mut() {
            Some(t) => t.full_scan(),
            None => self.open_cursor(schema).materialize(),
        }
    }

    /// Dispatched durable ingest of a borrowed `Batch` — the single-copy path
    /// for callers that keep reading the batch (see
    /// `Table::ingest_borrowed_batch`).
    pub(crate) fn ingest_borrowed_batch(&self, batch: &Batch) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.ingest_borrowed_batch(batch),
            // Nothing to drop: the master routes every user write to a worker, and a
            // stream's batch is buffered as a delta by the caller before it lands here.
            None => Ok(()),
        }
    }

    /// Enforce unique-PK semantics on an ingest batch against this relation's
    /// store. A relation this process holds no store for enforces nothing: it has
    /// no stored row to retract against, and the worker that does own the store
    /// runs the same walk on the same batch.
    pub(crate) fn enforce_unique_pk(&self, schema: &SchemaDescriptor, batch: Batch) -> Batch {
        // Not `map_or`: `batch` would have to move into both arms.
        match self.table_mut() {
            Some(t) => crate::storage::enforce_unique_pk(t, schema, batch),
            None => batch,
        }
    }

    /// Dispatched flush across all variants. Deliberately `&mut self` and NOT
    /// routed through [`Self::table_mut`]: `cell.get_mut()` is the one
    /// statically-checked mutation in this file, and keeping it costs the single
    /// caller nothing (it already holds `&mut self`).
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.flush(),
            StoreHandle::Owned(cell) => cell.get_mut().flush(),
            StoreHandle::Detached => Ok(()),
        }
    }

    /// The store's LSN counter: the next-shard name seed, the master's zone-LSN
    /// allocator floor, and the recovery watermark below which committed SAL
    /// zones may be skipped on replay. One number for all three, because a
    /// relation holds exactly one `Table` per worker. The cluster-wide minimum a
    /// repartition stamps is a different quantity, taken by the master across a
    /// relation's per-worker children.
    pub(crate) fn current_lsn(&self) -> u64 {
        self.table().map_or(0, Table::current_lsn)
    }
}
