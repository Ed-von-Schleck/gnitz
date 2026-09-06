//! `StoreHandle` — the storage adapter behind every `Table` the registry
//! reaches: a relation's own ([`TableEntry`](super::TableEntry)), a fed view's
//! delta store ([`DeltaFeed`](super::DeltaFeed)) and a secondary index's
//! ([`IndexCircuitEntry`](super::IndexCircuitEntry)). Each owner pairs it with
//! the schema a detached handle opens empty in and forwards through it, so no
//! caller names the variant.
//!
//! Both store-less variants read empty and absorb nothing. They are named apart
//! because that empty means different things, and a caller for whom the
//! difference matters must test it rather than read through.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor, StorageError, Table};

/// Storage handle of a registered relation.
pub(crate) enum StoreHandle {
    /// Owned `Table` — this worker's whole slice of a base table or view.
    Owned(Box<Table>),
    /// No store in any process: the relation holds no rows anywhere.
    Storeless,
    /// This process holds no store, but another one does.
    Elsewhere,
}

impl StoreHandle {
    /// An `Owned` handle over `table`.
    pub(crate) fn owned(table: Box<Table>) -> StoreHandle {
        StoreHandle::Owned(table)
    }

    /// This process's `Table` for the relation, or `None` when it holds none.
    pub(crate) fn as_owned(&self) -> Option<&Table> {
        match self {
            StoreHandle::Owned(t) => Some(t),
            StoreHandle::Storeless | StoreHandle::Elsewhere => None,
        }
    }

    /// [`Self::as_owned`] as `&mut`.
    pub(crate) fn owned_mut(&mut self) -> Option<&mut Table> {
        match self {
            StoreHandle::Owned(t) => Some(t),
            StoreHandle::Storeless | StoreHandle::Elsewhere => None,
        }
    }

    /// Non-compacting cursor; with no store here, an empty one of `schema`.
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

    /// Whether this store actually holds a skeleton row. Every read path branches
    /// on this rather than on the configured capacity.
    pub(crate) fn has_skeleton_rows(&self) -> bool {
        self.as_owned().is_some_and(Table::has_skeleton_rows)
    }

    /// Every positive-weight row; with no store here, an empty batch.
    pub(crate) fn full_scan(&self, schema: &SchemaDescriptor) -> std::rc::Rc<Batch> {
        match self.as_owned() {
            Some(t) => t.full_scan(),
            None => std::rc::Rc::new(Batch::empty_with_schema(schema)),
        }
    }

    /// The highest tick round this store's capacity sweep has dropped; `0` if it
    /// has dropped nothing.
    pub(crate) fn dropped_through(&self) -> u64 {
        self.as_owned().map_or(0, Table::dropped_through)
    }

    /// Ingest a `Batch` by move — no copy, and the caller does not keep it.
    pub(crate) fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        match self.owned_mut() {
            Some(t) => t.ingest_owned_batch(batch),
            None => Ok(()),
        }
    }

    /// Ingest a `Batch` the caller keeps reading; costs one copy.
    pub(crate) fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        match self.owned_mut() {
            Some(t) => t.ingest_borrowed_batch(batch),
            None => Ok(()),
        }
    }

    /// Enforce unique-PK semantics against this relation's store. With no store
    /// here there is nothing to retract against, so the batch passes through.
    pub(crate) fn enforce_unique_pk(&self, schema: &SchemaDescriptor, batch: Batch) -> Batch {
        match self.as_owned() {
            Some(t) => crate::storage::enforce_unique_pk(t, schema, batch),
            None => batch,
        }
    }

    /// Dispatched flush.
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        match self.owned_mut() {
            Some(t) => t.flush(),
            None => Ok(()),
        }
    }

    /// The store's LSN counter; `0` where this process holds no store.
    pub(crate) fn current_lsn(&self) -> u64 {
        self.as_owned().map_or(0, Table::current_lsn)
    }
}
