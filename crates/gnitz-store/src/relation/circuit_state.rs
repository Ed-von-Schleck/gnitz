//! The rederived operator state of one compiled circuit: the children its
//! compile declares, and the stores it opens for them.

use super::ChildKind;
use super::RelationRegistry;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, PkSetGather, ReadCursor, StorageError, Table};
use gnitz_wire::PkKeys;

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
pub struct CircuitState {
    tables: Vec<Table>,
}

impl CircuitState {
    /// Open every child `layout` declares, in registered view `view_id`'s
    /// directory at this registry's rank.
    pub fn open(reg: &RelationRegistry, view_id: u64, layout: StateLayout) -> Result<Self, String> {
        let view = reg.relation_or_err(view_id)?;
        // The output store's policy, not the registry's current one: the traces
        // must resume from the generation the output they feed resumed from.
        let recovery = view.store().held().recovery_source();
        let tables = layout
            .children
            .into_iter()
            .map(|(child, schema)| {
                // Unbounded: a bounded view's hydration reads these traces back.
                reg.open_child_as(
                    view_id,
                    ChildKind::Scratch(&child),
                    schema,
                    recovery,
                    reg.store_budgets(),
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(CircuitState { tables })
    }

    pub fn cursor(&self, idx: StateIdx) -> ReadCursor {
        self.at(idx).open_cursor()
    }

    /// Every live row of `keys` in `idx`.
    pub fn gather(&self, idx: StateIdx, keys: PkKeys) -> PkSetGather {
        self.at(idx).gather(keys, None)
    }

    /// A cursor over `idx` ranged to the PKs of `keys`, for probing at them.
    pub fn cursor_for_keys(&self, idx: StateIdx, keys: &Batch) -> ReadCursor {
        debug_assert!(keys.is_consolidated() && keys.count > 0);
        self.at(idx)
            .open_cursor_in_range(keys.get_pk_bytes(0), keys.get_pk_bytes(keys.count - 1))
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
