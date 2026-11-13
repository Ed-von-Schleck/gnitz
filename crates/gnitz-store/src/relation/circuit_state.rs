//! The rederived operator state of one compiled circuit: the children its
//! compile declares, and the stores it opens for them.

use super::{ChildKind, RelationRegistry};
use crate::storage::{Cut, RecoverySource, Table};
use gnitz_wire::PkKeys;
use gnitz_zset::repr::{Batch, PkSetGather, ReadCursor, StorageError};
use gnitz_zset::schema::{encode_schema_block, SchemaDescriptor};

/// An index into one [`CircuitState`], minted only by
/// [`StateLayout::declare`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StateIdx(usize);

/// The children one compiled circuit declares, in [`StateIdx`] order.
#[derive(Default)]
pub struct StateLayout {
    children: Vec<(String, SchemaDescriptor)>,
}

impl StateLayout {
    /// Declare one rederived child store for `child`: a name of its own, since
    /// each child opens the store it names. The store's name carries a digest of
    /// `schema`, so a checkpoint taken under another schema is not resumed.
    pub fn declare(&mut self, child: String, schema: SchemaDescriptor) -> StateIdx {
        let digest = gnitz_wire::checksum(&encode_schema_block(&schema));
        let name = format!("{child}_{digest:016x}");
        debug_assert!(
            self.children
                .iter()
                .all(|(n, _)| n.rsplit_once('_').is_none_or(|(c, _)| c != child)),
            "child {child} declared twice"
        );
        let idx = StateIdx(self.children.len());
        self.children.push((name, schema));
        idx
    }

    /// The name of every child declared, in [`StateIdx`] order.
    pub(super) fn names(&self) -> impl Iterator<Item = &str> {
        self.children.iter().map(|(name, _)| name.as_str())
    }

    /// Whether no child is declared.
    pub(super) fn is_empty(&self) -> bool {
        self.children.is_empty()
    }
}

/// The rederived operator state of one compiled circuit: the stores its stateful
/// operators read and write, addressed by [`StateIdx`].
pub struct CircuitState {
    tables: Vec<Table>,
}

impl CircuitState {
    /// Open every trace registered view `view_id` keeps, in its directory at
    /// this registry's rank.
    pub fn open(reg: &RelationRegistry, view_id: u64) -> Result<Self, String> {
        let view = reg.relation_or_err(view_id)?;
        // The traces resume from the generation the output they feed resumed from.
        let recovery = RecoverySource::Rederive { resume_at: view.loaded_mark() };
        let tables = view
            .traces
            .children
            .iter()
            .map(|(child, schema)| {
                // Unbounded: a bounded view's hydration reads these traces back.
                reg.open_child(view_id, ChildKind::Scratch(child), *schema, recovery, None, false)
            })
            .collect::<Result<_, _>>()?;
        Ok(CircuitState { tables })
    }

    /// Every live row of `keys` in `idx`.
    pub fn gather(&self, idx: StateIdx, keys: PkKeys) -> PkSetGather {
        self.tables[idx.0].gather(keys, Cut::Now)
    }

    /// A cursor over `idx` for probing at the keys in `[first, last]` — whole
    /// PKs, or the same leading bytes of one — positioned on the band they span.
    pub fn cursor_between(&self, idx: StateIdx, first: &[u8], last: &[u8]) -> ReadCursor {
        self.tables[idx.0].cursor_between(first, last, Cut::Now)
    }

    pub fn ingest(&mut self, idx: StateIdx, batch: Batch) -> Result<(), StorageError> {
        self.tables[idx.0].ingest(batch)
    }

    /// Every child store, for the checkpoint round that publishes them.
    pub(super) fn tables_mut(&mut self) -> impl Iterator<Item = &mut Table> {
        self.tables.iter_mut()
    }
}
