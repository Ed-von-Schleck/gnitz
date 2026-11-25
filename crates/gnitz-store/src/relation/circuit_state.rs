//! The rederived operator state of one compiled circuit: the children its
//! compile declares, and the stores it opens for them.

use std::rc::Rc;

use super::{ChildKind, RelationRegistry};
use crate::storage::{Cut, RecoverySource, Table};
use gnitz_trace::runs::{PkSetGather, ReadCursor, StorageError};
use gnitz_wire::{PkBuf, PkKeys};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// An index into one [`CircuitState`], minted only by
/// [`StateLayout::declare`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StateIdx(usize);

/// The children one compiled circuit declares, in [`StateIdx`] order.
#[derive(Default)]
pub struct StateLayout {
    pub(super) children: Vec<(String, SchemaDescriptor)>,
}

impl StateLayout {
    /// Declare one rederived child store for `child`: a name of its own, since
    /// each child opens the store it names. The store's name carries a digest of
    /// `schema`, so a checkpoint taken under another schema is not resumed.
    pub fn declare(&mut self, child: String, schema: SchemaDescriptor) -> StateIdx {
        let digest = schema.identity_digest();
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
}

/// The rederived operator state of one compiled circuit: the stores its stateful
/// operators read and write, addressed by [`StateIdx`].
pub struct CircuitState {
    /// Every child store, in [`StateIdx`] order.
    pub(super) tables: Vec<Table>,
    /// Each child's floor, set by [`Self::raise_floor`]: no row under it is held.
    floors: Vec<Option<PkBuf>>,
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
        let floors = vec![None; view.traces.children.len()];
        Ok(CircuitState { tables, floors })
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

    /// Add `batch` into `idx`, less its rows under the child's floor.
    pub fn ingest(&mut self, idx: StateIdx, batch: Batch) -> Result<(), StorageError> {
        let batch = match &self.floors[idx.0] {
            None => batch,
            Some(floor) => {
                let batch = batch.into_consolidated();
                match batch.seek(floor.pk_bytes(), None) {
                    0 => batch,
                    at if at == batch.len() => return Ok(()),
                    at => Batch::from_ranges(&batch, &[(at, batch.len())], 0),
                }
            }
        };
        self.tables[idx.0].ingest(batch)
    }

    /// Raise `idx`'s floor to `floor`: retract every row it holds under it, and
    /// drop the rows a later ingest carries under it. For a child no reader
    /// reads under the floor again; a floor at or below the current one is a
    /// no-op. Costs the rows the raise passes.
    pub fn raise_floor(&mut self, idx: StateIdx, floor: PkBuf) -> Result<(), StorageError> {
        let table = &mut self.tables[idx.0];
        let from = match self.floors[idx.0] {
            Some(cur) if cur >= floor => return Ok(()),
            Some(cur) => cur,
            None => PkBuf::zeroed(table.schema().pk_stride()),
        };
        let passed = Rc::unwrap_or_clone(table.band_rows(from, floor));
        if !passed.is_empty() {
            table.ingest(passed.negated())?;
        }
        self.floors[idx.0] = Some(floor);
        Ok(())
    }
}
