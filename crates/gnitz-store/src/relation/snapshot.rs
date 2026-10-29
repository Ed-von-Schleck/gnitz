//! Pinned snapshots: the runs of chosen relations' stores as they stood at one
//! moment, held so reads at that moment keep answering while the stores move on.
//!
//! Pinning copies nothing. A store's runs are immutable and shared — a RAM run
//! is reference-counted, a shard is a mapping of a file no publish rewrites — so
//! a pin is the list of them. A fold or compaction that replaces a run leaves
//! the pinned one alive, and a shard file unlinked under a pin stays readable
//! through its mapping, so what a pin costs is that memory and disk are given
//! back only when it is released.

use gnitz_zset::repr::Run;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;

use super::{RelationKind, RelationRegistry};
use crate::storage::Cut;

/// One relation's store at the moment of a pin.
pub(crate) struct PinnedStore {
    pub(crate) schema: SchemaDescriptor,
    pub(crate) runs: Vec<Run>,
}

/// The stores one pin holds, by relation id.
pub(super) type Pinned = FxHashMap<u64, PinnedStore>;

/// The relations a pin takes: kinds whose stores hold no skeleton rows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PinScope {
    BaseTables,
    SystemCatalog,
    /// The base tables and the system catalog.
    Durable,
}

impl PinScope {
    fn takes(self, kind: RelationKind) -> bool {
        match self {
            PinScope::BaseTables => kind == RelationKind::BaseTable,
            PinScope::SystemCatalog => kind == RelationKind::SystemCatalog,
            PinScope::Durable => matches!(kind, RelationKind::BaseTable | RelationKind::SystemCatalog),
        }
    }
}

impl RelationRegistry {
    /// Pin, as `snapshot`, the store of every relation `scope` takes whose
    /// store this process holds. A replicated relation's rows are
    /// pinned at its owner alone, every other copy pinned empty, so a page read
    /// from every worker reads each row once. Refused for an id already pinned.
    pub fn pin(&mut self, snapshot: u64, scope: PinScope) -> Result<(), String> {
        if self.snapshots.contains_key(&snapshot) {
            return Err(format!("snapshot {snapshot} is already pinned"));
        }
        let pinned = self
            .relations
            .iter()
            .filter(|(_, rel)| scope.takes(rel.kind()))
            .filter_map(|(&id, rel)| {
                let table = rel.rows.table()?;
                let runs = match rel.placement().counts_on(self.slot.rank) {
                    true => table.runs(Cut::Now).collect(),
                    false => Vec::new(),
                };
                Some((id, PinnedStore { schema: *table.schema(), runs }))
            })
            .collect();
        self.snapshots.insert(snapshot, pinned);
        Ok(())
    }

    /// Release `snapshot`, if it is pinned.
    pub fn unpin(&mut self, snapshot: u64) {
        self.snapshots.remove(&snapshot);
    }

    /// Whether `snapshot` pinned relation `id` here.
    pub fn pins(&self, snapshot: u64, id: u64) -> bool {
        self.snapshots.get(&snapshot).is_some_and(|p| p.contains_key(&id))
    }

    /// Relation `id`'s store as `snapshot` pinned it.
    pub(crate) fn pinned_store(&self, snapshot: u64, id: u64) -> Result<&PinnedStore, String> {
        let pinned = self
            .snapshots
            .get(&snapshot)
            .ok_or_else(|| format!("snapshot {snapshot} is not pinned"))?;
        pinned
            .get(&id)
            .ok_or_else(|| format!("snapshot {snapshot} holds no relation {id}"))
    }
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
