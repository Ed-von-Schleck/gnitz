//! `Store` — one store this process may hold, and the schema it is read in.
//! Every question about one is answered here for the detached case too, so no
//! caller decides what a process holding no store answers.

use crate::schema::key::{leading_u64, PkBuf};
use crate::schema::{IndexKeySpec, SchemaDescriptor};
use crate::storage::{Batch, ReadCursor, RecoverySource, StorageError, StoredRow, Table};

/// One store this process may hold, and the schema it is read in. A stream has
/// one nowhere and the master opens no user store; both read empty and
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
    /// which rebinds its shards if the region count grew.
    pub(crate) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        if let Some(t) = self.table.as_mut() {
            t.swap_schema(schema)?;
        }
        self.schema = schema;
        Ok(())
    }

    /// This process's `Table`, or `None` when it holds none.
    pub(in crate::relation) fn table(&self) -> Option<&Table> {
        self.table.as_deref()
    }

    /// [`Self::table`] as `&mut`, for a caller that needs the `Table` itself
    /// rather than a verb here.
    pub(in crate::relation) fn table_mut(&mut self) -> Option<&mut Table> {
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

    /// A cursor positioned on the OPK range `[start, end)`, and the raw entry
    /// count in it — an upper bound on the live groups the walk emits.
    pub(crate) fn range_cursor(&self, start: &[u8], end: Option<&[u8]>) -> (ReadCursor, usize) {
        let mut cursor = self.cursor_in_range(start, end);
        let matches = cursor.seek_range_bytes(start, end);
        (cursor, matches)
    }

    /// [`Self::range_cursor`] over the key range `range` names under `spec`; a
    /// provably-empty range is an empty cursor.
    pub(crate) fn cursor_over(
        &self,
        spec: &IndexKeySpec,
        range: &gnitz_wire::RangeDescriptor,
    ) -> Result<(ReadCursor, usize), String> {
        let Some((start, end)) = spec.range_keys(self.schema.pk_stride(), range)? else {
            return Ok((crate::storage::empty_cursor(self.schema), 0));
        };
        Ok(self.range_cursor(start.pk_bytes(), end.as_ref().map(PkBuf::pk_bytes)))
    }

    /// [`Self::cursor_over`] this store's own PK space.
    pub(crate) fn pk_range_cursor(&self, range: &gnitz_wire::RangeDescriptor) -> Result<ReadCursor, String> {
        Ok(self.cursor_over(&IndexKeySpec::for_pk(&self.schema), range)?.0)
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

    /// The highest tick round this store's capacity sweep has dropped — the
    /// `_tick` leading a delta store's highest dropped key; `0` if none.
    pub(crate) fn dropped_through(&self) -> u64 {
        self.table().map_or(0, |t| leading_u64(t.dropped_max().pk_bytes()))
    }

    /// Ingest a `Batch` by move — no copy, and the caller does not keep it.
    /// `#[inline]` for [`Table::ingest_owned_batch`]'s reason.
    #[inline]
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

    /// Whether a live row carries this OPK key; a detached store holds no key.
    pub(crate) fn has_pk_bytes(&self, key: &[u8]) -> bool {
        self.table().is_some_and(|t| t.has_pk_bytes(key))
    }

    /// The net weight at `key` and the live row if there is one; a detached
    /// store has neither.
    pub(crate) fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        self.table().map_or((0, None), |t| t.live_row_at(key))
    }

    /// The policy this store was opened under, frozen at that open. A detached
    /// store rederives: it publishes nothing a later open could resume from.
    pub(crate) fn recovery_source(&self) -> RecoverySource {
        self.table()
            .map_or(RecoverySource::Rederive { resume_at: None }, Table::recovery_source)
    }

    /// Keep this store's shards in RAM, publishing none. A detached store holds
    /// nothing to keep.
    pub(crate) fn hold_in_ram(&mut self) {
        if let Some(t) = self.table_mut() {
            t.hold_in_ram();
        }
    }

    /// Unlink this store's checkpoint manifest, so the next open reads `None`.
    /// A detached store published none.
    pub(crate) fn unlink_manifest(&mut self) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.unlink_manifest(),
            None => Ok(()),
        }
    }

    /// Dispatched [`Table::fold_to_ram`] — the fold, spill, compaction and
    /// capacity sweep, with no manifest publish and no barrier.
    pub(crate) fn fold_to_ram(&mut self) -> Result<(), StorageError> {
        match self.table_mut() {
            Some(t) => t.fold_to_ram(),
            None => Ok(()),
        }
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
