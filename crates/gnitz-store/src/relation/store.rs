//! `Store` — one store this process may hold, and the schema it is read in.
//! Every store the registry holds is one of these, so no owner and no caller
//! names a residency.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor, StorageError, Table};

/// One store this process may hold, and the schema it is read in. A stream has
/// one nowhere and the post-fork master detached every one; both read empty and
/// absorb nothing.
///
/// The schema is held beside the `Table` rather than read off it: a process
/// holding no store has no `Table` to ask, and inlining the descriptor in the
/// held case too keeps `Store` one size and every read one field access.
/// [`Self::swap_schema`] is the only writer of either, so they cannot drift.
pub(crate) struct Store {
    table: Option<Box<Table>>,
    schema: SchemaDescriptor,
}

impl Store {
    /// A store this process holds.
    pub(crate) fn owned(table: Box<Table>, schema: SchemaDescriptor) -> Store {
        Store { table: Some(table), schema }
    }

    /// A store this process does not hold: a stream's, which is nowhere, or one
    /// another process owns.
    pub(crate) fn detached(schema: SchemaDescriptor) -> Store {
        Store { table: None, schema }
    }

    /// The schema this store's rows are read in.
    pub(crate) fn schema(&self) -> SchemaDescriptor {
        self.schema
    }

    /// Publish a new schema for this store: here, and down into the `Table`,
    /// which re-opens its shards if the region count grew.
    pub(crate) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        if let Some(t) = self.table.as_mut() {
            t.swap_schema(schema)?;
        }
        self.schema = schema;
        Ok(())
    }

    /// This process's `Table`, or `None` when it holds none.
    pub(crate) fn table(&self) -> Option<&Table> {
        self.table.as_deref()
    }

    /// [`Self::table`] as `&mut`.
    pub(crate) fn table_mut(&mut self) -> Option<&mut Table> {
        self.table.as_deref_mut()
    }

    /// Non-compacting cursor; with no store here, an empty one of the schema.
    pub(crate) fn cursor(&self) -> ReadCursor {
        match self.table() {
            Some(t) => t.open_cursor(),
            None => crate::storage::empty_cursor(self.schema),
        }
    }

    /// [`Self::cursor`] over `[start, end]` only — see
    /// [`Table::open_cursor_in_range`].
    pub(crate) fn cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> ReadCursor {
        match self.table() {
            Some(t) => t.open_cursor_in_range(start, end),
            None => crate::storage::empty_cursor(self.schema),
        }
    }

    /// Whether this store actually holds a skeleton row. Every read path branches
    /// on this rather than on the configured capacity.
    pub(crate) fn has_skeleton_rows(&self) -> bool {
        self.table().is_some_and(Table::has_skeleton_rows)
    }

    /// Every positive-weight row; with no store here, an empty batch.
    pub(crate) fn full_scan(&self) -> std::rc::Rc<Batch> {
        match self.table() {
            Some(t) => t.full_scan(),
            None => std::rc::Rc::new(Batch::empty_with_schema(&self.schema)),
        }
    }

    /// The highest tick round this store's capacity sweep has dropped; `0` if it
    /// has dropped nothing.
    pub(crate) fn dropped_through(&self) -> u64 {
        self.table().map_or(0, Table::dropped_through)
    }

    /// Ingest a `Batch` by move — no copy, and the caller does not keep it.
    pub(crate) fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.ingest_owned_batch(batch),
            None => Ok(()),
        }
    }

    /// Ingest a `Batch` the caller keeps reading; costs one copy.
    pub(crate) fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.ingest_borrowed_batch(batch),
            None => Ok(()),
        }
    }

    /// Enforce unique-PK semantics against this store. With no store here there
    /// is nothing to retract against, so the batch passes through.
    pub(crate) fn enforce_unique_pk(&self, batch: Batch) -> Batch {
        match self.table() {
            Some(t) => super::unique_pk::enforce_unique_pk(t, &self.schema, batch),
            None => batch,
        }
    }

    /// Dispatched flush.
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.flush(),
            None => Ok(()),
        }
    }

    /// Dispatched [`Table::flush_to_ram`] — the fold, spill, compaction and
    /// capacity sweep, with no manifest publish and no barrier.
    pub(crate) fn flush_to_ram(&mut self) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.flush_to_ram(),
            None => Ok(()),
        }
    }

    /// Pin the store's LSN counter to a DDL zone's, so recovery's dedup check
    /// matches the SAL group LSN that carried the write. Inert where this
    /// process holds no store.
    pub(crate) fn pin_lsn(&mut self, lsn: std::num::NonZeroU64) {
        if let Some(t) = self.table_mut() {
            t.pin_lsn(lsn);
        }
    }

    /// The store's LSN counter; `0` where this process holds no store.
    pub(crate) fn current_lsn(&self) -> u64 {
        self.table().map_or(0, Table::current_lsn)
    }

    /// Whether this process's store came back from a checkpoint manifest at its
    /// open; `false` where it holds none.
    pub(crate) fn resumed_from_checkpoint(&self) -> bool {
        self.table().is_some_and(Table::resumed_from_checkpoint)
    }

    /// Rows this process's store estimates it holds; `0` where it holds none.
    pub(crate) fn estimated_rows(&self) -> usize {
        self.table().map_or(0, Table::estimated_rows)
    }
}
