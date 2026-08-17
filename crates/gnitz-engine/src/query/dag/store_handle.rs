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
pub enum StoreHandle {
    /// Owned `Table` — this worker's whole slice of a base table or view.
    /// Wrapped in `UnsafeCell` so the interior-mutable accessors can hand out
    /// `&mut` through a shared `&self` without violating Stacked Borrows (a raw
    /// pointer derived from a shared reference may not be used to mutate).
    Owned(UnsafeCell<Box<Table>>),
    /// Registered but storeless: the post-fork master, which moves every user
    /// relation here so master and worker 0 do not both hold a live `Table` on
    /// `w0of{W}` — two processes writing one directory is the hazard `naming.rs`
    /// exists to prevent. Every read through it is empty and every write a no-op.
    Detached,
    /// Non-owning pointer to a `Table` owned elsewhere (system tables).
    Borrowed(*mut Table),
}

// SAFETY: Borrowed wraps a raw pointer that is only accessed on the
// thread that owns the DagEngine. The DagEngine itself is never shared
// across threads.
unsafe impl Send for StoreHandle {}

impl StoreHandle {
    // ------------------------------------------------------------------
    // Interior-mutable accessors
    //
    // DagEngine/CatalogEngine are single-threaded (!Sync) and the
    // HashMap<id, TableEntry> stores owning Boxes whose heap allocations
    // have stable addresses, so the mutation is race-free. But the
    // registry HashMap is read via immutable get(), which would normally
    // prevent handing out &mut to the owned Table.
    // These accessors encapsulate the raw-pointer re-borrow that
    // reconciles the lookup API with the mutation need, so call sites
    // stop reimplementing it inline.
    //
    // SAFETY contract for every method below: no aliasing &mut into the
    // same storage may be live across the call.
    // ------------------------------------------------------------------

    /// This relation's owned `Table`, or `None` for a borrowed system table or a
    /// detached relation.
    // Interior mutability through UnsafeCell: the `&mut` is handed out under the
    // SAFETY contract documented above (no live aliasing &mut), not derived from
    // `&self` by reborrow — so clippy's mut_from_ref does not apply.
    #[allow(clippy::mut_from_ref)]
    pub fn as_owned_mut(&self) -> Option<&mut Table> {
        match self {
            StoreHandle::Owned(cell) => Some(unsafe { &mut **cell.get() }),
            _ => None,
        }
    }

    /// True for the post-fork master's storeless handle.
    pub fn is_detached(&self) -> bool {
        matches!(self, StoreHandle::Detached)
    }

    /// Dispatched `has_pk` that works for every variant. Takes a **native**
    /// `u128`; routes via `opk_key` internally.
    #[cfg(test)] // sole caller is the test-only inline FK check (validate_fk_inline)
    pub fn has_pk(&self, key: u128) -> bool {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { (**ptr).has_pk(key) },
            StoreHandle::Owned(cell) => unsafe { (**cell.get()).has_pk(key) },
            StoreHandle::Detached => false,
        }
    }

    /// Dispatched non-compacting `open_cursor` across all variants. A detached
    /// relation opens an empty cursor of `schema` — the same answer a store
    /// holding none of the requested rows gives. Infallible, non-mutating.
    /// Callers reach this through [`TableEntry::open_cursor`], which supplies
    /// the registry's own schema.
    pub(super) fn open_cursor(&self, schema: &SchemaDescriptor) -> ReadCursor {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { (**ptr).open_cursor() },
            StoreHandle::Owned(cell) => unsafe { (**cell.get()).open_cursor() },
            StoreHandle::Detached => crate::storage::empty_cursor(*schema),
        }
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// A detached relation reads empty, so it never can.
    pub(super) fn has_skeleton_rows(&self) -> bool {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.has_skeleton_rows(),
            StoreHandle::Owned(cell) => unsafe { (**cell.get()).has_skeleton_rows() },
            StoreHandle::Detached => false,
        }
    }

    /// Materialize every positive-weight row. `Owned` and `Borrowed` delegate to
    /// `Table::full_scan`, preserving its `Rc` snapshot cache; a detached
    /// relation materializes an empty batch. Reached through
    /// [`TableEntry::full_scan`].
    pub(super) fn full_scan(&self, schema: &SchemaDescriptor) -> std::rc::Rc<Batch> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.full_scan(),
            StoreHandle::Owned(cell) => unsafe { &mut *cell.get() }.full_scan(),
            StoreHandle::Detached => self.open_cursor(schema).materialize(),
        }
    }

    /// Dispatched durable ingest of a borrowed `Batch` — the single-copy path
    /// for callers that keep reading the batch (see
    /// `Table::ingest_borrowed_batch`).
    pub fn ingest_borrowed_batch(&self, batch: &Batch) -> Result<(), StorageError> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { (**ptr).ingest_borrowed_batch(batch) },
            StoreHandle::Owned(cell) => unsafe { (**cell.get()).ingest_borrowed_batch(batch) },
            // The post-fork master routes every user write to a worker and
            // ingests none itself, so there is nothing here to drop.
            StoreHandle::Detached => Ok(()),
        }
    }

    /// Dispatched flush across all variants.
    pub fn flush(&mut self) -> Result<(), StorageError> {
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
    pub fn current_lsn(&self) -> u64 {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn(),
            StoreHandle::Owned(cell) => unsafe { (**cell.get()).current_lsn() },
            StoreHandle::Detached => 0,
        }
    }
}
