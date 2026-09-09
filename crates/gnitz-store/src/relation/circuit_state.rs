//! The rederived operator state of one compiled circuit: the stores its
//! stateful operators read and write, owned by the plan that compiled them.

use super::RelationRegistry;
use crate::schema::SchemaDescriptor;
use crate::storage::{
    create_child, remove_child, Batch, ChildAddr, ReadCursor, RecoverySource, StorageError, StoreError, Table,
};

/// The policy `view_id`'s **output store** was opened under, frozen at that open
/// — so a view's operator traces cannot look for a different manifest generation
/// than the output they feed, which the registry's *current* verdict would give.
fn output_store_recovery(reg: &RelationRegistry, view_id: i64) -> RecoverySource {
    reg.relation(view_id)
        .and_then(|r| r.store().table())
        .map_or(RecoverySource::Rederive { resume_at: None }, Table::recovery_source)
}

/// A `u16` index into one [`CircuitState`], minted only by
/// [`CircuitState::open_child`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StateIdx(u16);

/// The rederived operator state of one compiled circuit: the stores its stateful
/// operators read and write, addressed by [`StateIdx`]. Owned by the plan that
/// compiled them, so a compile that keeps nothing closes its stores when the
/// plan drops.
///
/// **Drop removes every directory this state created** and [`Self::commit`] has
/// not released — never one that was already there, which would silently strip a
/// checkpointed trace out of the next boot's resume verdict.
#[derive(Default)]
pub struct CircuitState {
    tables: Vec<Table>,
    created: Vec<String>,
}

impl Drop for CircuitState {
    fn drop(&mut self) {
        // Close every store before its directory goes. A committed state drained
        // `created` at the commit, so this removes nothing.
        self.tables.clear();
        for dir in self.created.drain(..) {
            remove_child(&dir);
        }
    }
}

impl CircuitState {
    pub fn new() -> Self {
        CircuitState::default()
    }

    /// Create and open one rederived child store of `view_id`, named `child` at
    /// this registry's own rank under `root` — the view's own directory, or a
    /// throwaway root for a compile that keeps nothing.
    ///
    /// The budgets are the registry's unbounded ones: a bounded view's skeleton
    /// rows are recomputed from these traces, so a capacity here would let the
    /// sweep skeletonize what the hydration reads back.
    pub fn open_child(
        &mut self,
        reg: &RelationRegistry,
        view_id: i64,
        root: &str,
        child: &str,
        schema: SchemaDescriptor,
    ) -> Result<StateIdx, StoreError> {
        let recovery = output_store_recovery(reg, view_id);
        let dir = ChildAddr::Scratch { child, rank: reg.slot().rank }.dir(root);
        if create_child(&dir).map_err(|e| StoreError::storage(format!("create child store '{dir}'"), e))? {
            self.created.push(dir.clone());
        }
        let table = Table::new(&dir, schema, view_id as u32, recovery, reg.store_budgets())
            .map_err(|e| StoreError::storage(format!("open child store '{dir}'"), e))?;
        let idx = StateIdx(u16::try_from(self.tables.len()).expect("a plan holds far fewer than 65536 child stores"));
        self.tables.push(table);
        Ok(idx)
    }

    /// Keep what this state created: release the paths, so drop removes none of
    /// them. Freed rather than flagged, because a committed state lives as long
    /// as the cached plan that owns it and never reads them again.
    pub fn commit(&mut self) {
        self.created = Vec::new();
    }

    pub fn cursor(&self, idx: StateIdx) -> ReadCursor {
        self.at(idx).open_cursor()
    }

    pub fn cursor_in_range(&self, idx: StateIdx, start: &[u8], end: Option<&[u8]>) -> ReadCursor {
        self.at(idx).open_cursor_in_range(start, end)
    }

    pub fn ingest_owned(&mut self, idx: StateIdx, batch: Batch) -> Result<(), StorageError> {
        self.at_mut(idx).ingest_owned_batch(batch)
    }

    pub fn ingest_borrowed(&mut self, idx: StateIdx, batch: &Batch) -> Result<(), StorageError> {
        self.at_mut(idx).ingest_borrowed_batch(batch)
    }

    /// Ingest, compact, then open a cursor — in that order, so a prefix seek
    /// over an operator's own index sees the rows this epoch just wrote. A
    /// caller that opened the cursor first would read its own writes out.
    pub fn ingest_then_cursor(&mut self, idx: StateIdx, batch: Batch) -> Result<ReadCursor, StorageError> {
        let t = self.at_mut(idx);
        t.ingest_owned_batch(batch)?;
        t.compact_if_needed()?;
        Ok(t.open_cursor())
    }

    /// Bound the L0 fan-in of every child. The epoch path's job, not a read's: a
    /// compaction mutates shard state, and an `Err` leaves the shard index
    /// unchanged, so a cursor opened afterwards still sees a consistent snapshot.
    pub fn compact_all(&mut self) {
        for t in &mut self.tables {
            let _ = t.compact_if_needed();
        }
    }

    /// True iff this state holds at least one child and every one came back from
    /// a manifest at the generation it was opened for — the in-process twin of
    /// [`RelationRegistry::view_children_resumable`]'s on-disk peek. A state
    /// holding no child answers `false`, not the vacuous `true` of "every one of
    /// zero resumed".
    pub fn resumed(&self) -> bool {
        !self.tables.is_empty() && self.tables.iter().all(Table::resumed_from_checkpoint)
    }

    /// Every child store, for the checkpoint round that publishes them.
    pub(crate) fn tables_mut(&mut self) -> impl Iterator<Item = &mut Table> {
        self.tables.iter_mut()
    }

    fn at(&self, idx: StateIdx) -> &Table {
        &self.tables[idx.0 as usize]
    }

    fn at_mut(&mut self, idx: StateIdx) -> &mut Table {
        &mut self.tables[idx.0 as usize]
    }
}
