//! `StoreHandle` — the storage adapter behind every `Table` the registry
//! reaches: a relation's own ([`TableEntry`](super::TableEntry)), a fed view's
//! delta store ([`DeltaFeed`](super::DeltaFeed)) and a secondary index's
//! ([`IndexCircuitEntry`](super::IndexCircuitEntry)). Each owner pairs it with
//! the schema a detached handle opens empty in and forwards through it, so no
//! caller names the variant.
//!
//! `Owned` owns one boxed `Table`; `Detached` is a store this process does not
//! hold. There is no custom `Drop`: the `Owned` box is freed by the default drop
//! glue when its owner is removed.

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
}

impl StoreHandle {
    /// An `Owned` handle over `table`. The one spelling of the representation,
    /// so no caller repeats the `UnsafeCell` the interior-mutable accessors below
    /// depend on.
    pub(crate) fn owned(table: Box<Table>) -> StoreHandle {
        StoreHandle::Owned(UnsafeCell::new(table))
    }

    // ------------------------------------------------------------------
    // Interior-mutable accessors
    //
    // The registry is read through immutable `get()` but must mutate the owned
    // `Table`; it is !Sync and its `Box`es have stable addresses, so the
    // raw-pointer reborrows below are race-free. The &mut is handed out under the
    // contract, not derived from &self by reborrow — so clippy's mut_from_ref
    // does not apply.
    //
    // SAFETY contract for every method here: no aliasing &mut into the same
    // storage may be live across the call. `as_owned` is the shared reborrow and
    // `as_owned_mut` the unique one; routing a read through the latter would
    // widen that contract to "no reference at all live", silently.
    // ------------------------------------------------------------------

    /// This process's `Table` for the relation, or `None` when it holds none.
    pub(crate) fn as_owned(&self) -> Option<&Table> {
        match self {
            StoreHandle::Owned(cell) => Some(unsafe { &**cell.get() }),
            StoreHandle::Detached => None,
        }
    }

    /// [`Self::as_owned`] as `&mut` out of a shared `&self` — sound by the
    /// contract above, not by the borrow checker.
    ///
    /// Private, and reached only by the two ingest verbs below, which the registry
    /// drives through a shared `&TableEntry`. Anything holding a `&mut` takes
    /// [`Self::owned_mut`] instead.
    #[allow(clippy::mut_from_ref)]
    fn as_owned_mut(&self) -> Option<&mut Table> {
        match self {
            StoreHandle::Owned(cell) => Some(unsafe { &mut **cell.get() }),
            StoreHandle::Detached => None,
        }
    }

    /// [`Self::as_owned`] as `&mut`, checked: `&mut self` already proves no other
    /// reference into this handle is live, so `cell.get_mut()` needs no reborrow
    /// out of the `UnsafeCell`.
    pub(crate) fn owned_mut(&mut self) -> Option<&mut Table> {
        match self {
            StoreHandle::Owned(cell) => Some(&mut **cell.get_mut()),
            StoreHandle::Detached => None,
        }
    }

    /// Dispatched non-compacting `open_cursor` across all variants. A detached
    /// store opens an empty cursor of `schema` — the same answer a store holding
    /// none of the requested rows gives. Infallible, non-mutating; the owner
    /// supplies the schema.
    pub(crate) fn open_cursor(&self, schema: &SchemaDescriptor) -> ReadCursor {
        match self.as_owned() {
            Some(t) => t.open_cursor(),
            None => crate::storage::empty_cursor(*schema),
        }
    }

    /// [`Self::open_cursor`] over `[start, end]` only — see
    /// [`Table::open_cursor_in_range`].
    pub(crate) fn open_cursor_in_range(
        &self,
        schema: &SchemaDescriptor,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> ReadCursor {
        match self.as_owned() {
            Some(t) => t.open_cursor_in_range(start, end),
            None => crate::storage::empty_cursor(*schema),
        }
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// A detached relation reads empty, so it never can.
    pub(crate) fn has_skeleton_rows(&self) -> bool {
        self.as_owned().is_some_and(Table::has_skeleton_rows)
    }

    /// Materialize every positive-weight row, delegating to `Table::full_scan`
    /// so its `Rc` snapshot cache is preserved; a detached relation materializes
    /// an empty batch. Reached through [`TableEntry::full_scan`].
    pub(crate) fn full_scan(&self, schema: &SchemaDescriptor) -> std::rc::Rc<Batch> {
        match self.as_owned() {
            Some(t) => t.full_scan(),
            None => std::rc::Rc::new(Batch::empty_with_schema(schema)),
        }
    }

    /// The highest tick round this store's capacity sweep has dropped — `0` for
    /// every store but a fed view's delta store, and for one that has dropped
    /// nothing. See [`Table::dropped_through`].
    pub(crate) fn dropped_through(&self) -> u64 {
        self.as_owned().map_or(0, Table::dropped_through)
    }

    /// Dispatched ingest of an owned `Batch` — what the delta stamp takes,
    /// because the stamped batch arrives `Consolidated` and passing it borrowed
    /// would have `ingest_borrowed_batch` clone it, blob heap included, for
    /// nothing.
    pub(crate) fn ingest_owned_batch(&self, batch: Batch) -> Result<(), StorageError> {
        match self.as_owned_mut() {
            Some(t) => t.ingest_owned_batch(batch),
            None => Ok(()),
        }
    }

    /// Dispatched durable ingest of a borrowed `Batch` — the single-copy path
    /// for callers that keep reading the batch (see
    /// `Table::ingest_borrowed_batch`).
    pub(crate) fn ingest_borrowed_batch(&self, batch: &Batch) -> Result<(), StorageError> {
        match self.as_owned_mut() {
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
        match self.as_owned() {
            Some(t) => crate::storage::enforce_unique_pk(t, schema, batch),
            None => batch,
        }
    }

    /// Dispatched flush. Deliberately `&mut self` and routed through
    /// [`Self::owned_mut`]: it costs the single caller nothing (it already holds
    /// `&mut self`) and keeps the mutation statically checked.
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        match self.owned_mut() {
            Some(t) => t.flush(),
            None => Ok(()),
        }
    }

    /// The store's LSN counter: the next-shard name seed, the master's zone-LSN
    /// allocator floor, and the recovery watermark below which committed SAL
    /// zones may be skipped on replay. One number for all three, because a
    /// relation holds exactly one `Table` per worker. The cluster-wide minimum a
    /// repartition stamps is a different quantity, taken by the master across a
    /// relation's per-worker children.
    pub(crate) fn current_lsn(&self) -> u64 {
        self.as_owned().map_or(0, Table::current_lsn)
    }
}
