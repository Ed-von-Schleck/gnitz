//! The rederived operator state of one compiled circuit: the children its
//! compile declares, and the stores it opens for them.

use super::RelationRegistry;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ChildAddr, ReadCursor, StorageError, StoreError, Table};

/// A `u16` index into one [`CircuitState`], minted only by
/// [`StateLayout::declare`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StateIdx(u16);

/// The children one compiled circuit declares, in [`StateIdx`] order.
#[derive(Default)]
pub struct StateLayout {
    children: Vec<(String, SchemaDescriptor)>,
}

impl StateLayout {
    /// Declare one rederived child store named `child`.
    pub fn declare(&mut self, child: String, schema: SchemaDescriptor) -> StateIdx {
        let idx =
            StateIdx(u16::try_from(self.children.len()).expect("a circuit declares far fewer than 65536 children"));
        self.children.push((child, schema));
        idx
    }

    /// The schema of the child `idx` names.
    pub fn schema_of(&self, idx: StateIdx) -> &SchemaDescriptor {
        &self.children[idx.0 as usize].1
    }
}

/// The rederived operator state of one compiled circuit: the stores its stateful
/// operators read and write, addressed by [`StateIdx`].
#[derive(Default)]
pub struct CircuitState {
    tables: Vec<Table>,
}

impl CircuitState {
    /// Open every child `layout` declares, in registered view `view_id`'s
    /// directory at this registry's rank.
    pub fn open(reg: &RelationRegistry, view_id: i64, layout: StateLayout) -> Result<Self, StoreError> {
        let view = reg.relation_or_err(view_id)?;
        // The output store's policy, not the registry's current one: the traces
        // must resume from the generation the output they feed resumed from.
        let recovery = view.store().recovery_source();
        let tables = layout
            .children
            .into_iter()
            .map(|(child, schema)| {
                let dir = ChildAddr::Scratch { child: &child, rank: reg.slot().rank }.dir(view.directory());
                // Unbounded: a bounded view's hydration reads these traces back.
                Table::new(&dir, schema, recovery, reg.store_budgets())
                    .map_err(|e| StoreError::storage(format!("open child store '{dir}'"), e))
            })
            .collect::<Result<_, _>>()?;
        Ok(CircuitState { tables })
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

    /// Ingest, then open a cursor — in that order, so a prefix seek over an
    /// operator's own index sees the rows this epoch just wrote. A caller that
    /// opened the cursor first would read its own writes out.
    pub fn ingest_then_cursor(&mut self, idx: StateIdx, batch: Batch) -> Result<ReadCursor, StorageError> {
        let t = self.at_mut(idx);
        t.ingest_owned_batch(batch)?;
        Ok(t.open_cursor())
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
