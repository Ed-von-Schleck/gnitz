//! Pinned snapshots: the runs of chosen relations' stores as they stood at one
//! moment, held so reads at that moment keep answering while the stores move on.
//!
//! Pinning copies nothing. A store's runs are immutable and shared — a RAM run
//! is reference-counted, a shard is a mapping of a file no publish rewrites — so
//! a pin is the list of them. A fold or compaction that replaces a run leaves
//! the pinned one alive, and a shard file unlinked under a pin stays readable
//! through its mapping, so what a pin costs is that memory and disk are given
//! back only when it is released.

use gnitz_trace::runs::Run;
use gnitz_zset::algebra::Slot;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;

use super::{RelationKind, RelationRegistry};

/// One relation's store at the moment of a pin.
pub(crate) struct PinnedStore {
    pub(crate) schema: SchemaDescriptor,
    pub(crate) runs: Vec<Run>,
    /// Which of how many stores a page request's cursors and limit are split
    /// over this one is: [`Slot::SOLO`] for a system family, whose one store is
    /// unpartitioned; this process's slot for a base table.
    pub(crate) share: Slot,
}

/// The stores one pin holds, by relation id; they stay readable for as long as
/// it is held. Holds `Rc` runs, so it is bound to its registry's thread
/// contract.
pub struct Snapshot {
    pub(crate) stores: FxHashMap<u64, PinnedStore>,
}

impl Snapshot {
    /// Whether this snapshot pinned relation `id`.
    pub fn holds(&self, id: u64) -> bool {
        self.stores.contains_key(&id)
    }
}

impl RelationRegistry {
    /// Pin the store of every relation whose keys each net 0 or 1 — a base
    /// table or a system family, so no pinned store holds skeleton rows — and
    /// whose store this process publishes. A replicated relation's rows are
    /// pinned at its owner alone, every other copy pinned empty, so a page read
    /// from every worker reads each row once.
    pub fn pin(&self) -> Snapshot {
        let stores = self
            .relations
            .iter()
            .filter(|(_, rel)| rel.kind().nets_unit_per_key() && self.residency.publishes(rel.kind()))
            .filter_map(|(&id, rel)| {
                let table = rel.table.as_ref()?;
                // A system family's one store is unpartitioned and holds all of it.
                let (counted, share) = match rel.kind() {
                    RelationKind::SystemCatalog => (true, Slot::SOLO),
                    _ => (rel.placement().counts_on(self.slot.rank), self.slot),
                };
                let runs = match counted {
                    true => table.runs().collect(),
                    false => Vec::new(),
                };
                Some((id, PinnedStore { schema: rel.schema(), runs, share }))
            })
            .collect();
        Snapshot { stores }
    }
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
