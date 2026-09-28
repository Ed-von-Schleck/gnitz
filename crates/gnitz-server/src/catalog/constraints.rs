//! The constraints a user write is validated against: FK edges in both
//! directions, and the unique secondary indexes a write must check.

use super::*;
use gnitz_store::schema::IndexKeySpec;
use gnitz_wire::PkColList;

/// Every constraint on one table whose validation reads committed state.
pub(crate) struct RowConstraints {
    pub(crate) fks_as_child: Vec<FkEdge>,
    pub(crate) fks_as_parent: Vec<FkEdge>,
    pub(crate) uniques: Vec<(PkColList, SchemaDescriptor, IndexKeySpec)>,
}

impl RowConstraints {
    pub(crate) fn is_empty(&self) -> bool {
        self.fks_as_child.is_empty() && self.fks_as_parent.is_empty() && self.uniques.is_empty()
    }
}

impl CatalogEngine {
    /// All FK edges where `table_id` is the child.
    pub(crate) fn fk_constraints_of(&self, table_id: u64) -> &[FkEdge] {
        self.caches.relations.get(&table_id).map_or(&[], |e| e.fks.as_slice())
    }

    /// All FK edges where `parent_id` is the parent (empty when none).
    pub(crate) fn fk_children_of(&self, parent_id: u64) -> &[FkEdge] {
        self.caches.fk_by_parent.get(&parent_id).map_or(&[], Vec::as_slice)
    }

    /// The tables a write to `table_id` locks exclusively: itself, its FK parents
    /// (against a parent DELETE) and its FK children (against a child INSERT).
    pub(crate) fn fk_lock_set(&self, table_id: u64) -> impl Iterator<Item = u64> + '_ {
        std::iter::once(table_id)
            .chain(self.fk_constraints_of(table_id).iter().map(|e| e.parent_tid))
            .chain(self.fk_children_of(table_id).iter().map(|e| e.child_tid))
    }

    /// FK edges as child, FK edges as parent, and the relation whose unique
    /// indexes a write checks.
    fn constraint_sources(&self, tid: u64) -> (&[FkEdge], &[FkEdge], Option<&Relation>) {
        (
            self.fk_constraints_of(tid),
            self.fk_children_of(tid),
            self.registry.relation(tid),
        )
    }

    /// Whether [`Self::row_constraints`] of `tid` is non-empty, without the copy.
    pub(crate) fn has_row_constraints(&self, tid: u64) -> bool {
        let (as_child, as_parent, rel) = self.constraint_sources(tid);
        !as_child.is_empty()
            || !as_parent.is_empty()
            || rel.is_some_and(|r| r.unique_indexes_to_check().next().is_some())
    }

    /// Every constraint on `tid` whose validation reads committed state.
    pub(crate) fn row_constraints(&self, tid: u64) -> RowConstraints {
        let (as_child, as_parent, rel) = self.constraint_sources(tid);
        RowConstraints {
            fks_as_child: as_child.to_vec(),
            fks_as_parent: as_parent.to_vec(),
            uniques: rel.map_or_else(Vec::new, |r| {
                r.unique_indexes_to_check()
                    .map(|ic| (ic.cols(), ic.schema(), ic.key_spec()))
                    .collect()
            }),
        }
    }

    /// Does validating a write of `mode` to `table_id` read committed state? One
    /// that does not may hold its table lock shared.
    pub(crate) fn push_reads_committed_state(&self, table_id: u64, mode: gnitz_wire::WireConflictMode) -> bool {
        matches!(mode, gnitz_wire::WireConflictMode::Error) || self.has_row_constraints(table_id)
    }
}
