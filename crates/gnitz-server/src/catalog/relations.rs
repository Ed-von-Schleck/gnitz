//! The state every relation registration enters and every unregistration
//! removes — the relation's named record, its FK edges in both directions, its
//! serial flag and its RESOLVE token — together with the readers of that state;
//! and the name index, which follows every TABLE_TAB and VIEW_TAB delta instead,
//! rename pairs and the system families' own rows included.
//!
//! Among those readers are the constraints a user write is validated against:
//! the FK edges from this state, and the unique secondary indexes a write must
//! check, read from the registry.

use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use gnitz_store::relation::Relation;
use gnitz_wire::schema_block::check_same_types;
use gnitz_wire::{
    payload_str, payload_u64, FkAction, PkColList, RelDescriptorBlob, RelIndex, Target, WireFault, WireStatus,
    RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID,
};
use gnitz_zset::algebra::KeySpec;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;

use super::CatalogEngine;

/// What one registered relation's lifetime owns: entered by its registration,
/// removed by its unregistration.
pub(in crate::catalog) struct RelationEntry {
    /// The relation's [`encode_record`](super::sys_tables::encode_record).
    pub(in crate::catalog) record: Rc<[u8]>,
    /// The FK edges this relation declares as a child.
    pub(in crate::catalog) fks: Vec<FkEdge>,
    /// [`gnitz_wire::TableProps::serial`].
    pub(in crate::catalog) serial: bool,
    /// The token of [`CatalogEngine::resolve_answer`], computed on first use; `0` until then.
    /// Reset by every hook of a family that can change a RESOLVE answer.
    pub(in crate::catalog) token: Cell<u64>,
}

impl RelationEntry {
    pub(in crate::catalog) fn new(record: Rc<[u8]>, fks: Vec<FkEdge>, serial: bool) -> Self {
        RelationEntry { record, fks, serial, token: Cell::new(0) }
    }
}

impl CatalogEngine {
    /// The id of relation `name` in schema `schema_id`, from the name index.
    pub(crate) fn relation_id(&self, schema_id: u64, name: &str) -> Option<u64> {
        self.caches.relation_by_name.get(&schema_id)?.get(name).copied()
    }

    fn relation_entry(&self, tid: u64) -> &RelationEntry {
        self.caches
            .relations
            .get(&tid)
            .expect("every registered relation has an entry")
    }

    /// `tid`'s relation and its named record; `None` for an unregistered id.
    pub(crate) fn wire_target(&self, tid: u64) -> Option<(&Relation, &Rc<[u8]>)> {
        Some((self.registry.relation(tid)?, &self.caches.relations.get(&tid)?.record))
    }

    /// What `frame_record` decodes to, when it lays out `tid`'s columns.
    pub(crate) fn known_decode(&self, tid: u64, frame_record: &[u8]) -> Option<SchemaDescriptor> {
        let (rel, record) = self.wire_target(tid)?;
        check_same_types(frame_record, record).ok()?;
        Some(rel.schema())
    }

    /// Whether a frame whose record matched `seen` still lays out `tid`'s columns.
    pub(crate) fn recheck_record(&self, tid: u64, seen: &[u8]) -> Result<(), String> {
        check_same_types(seen, &self.relation_entry(tid).record)
    }

    /// What a RESOLVE of `tid` answers: its encoded descriptor, its named record
    /// and its descriptor token. `None` for an unregistered id.
    ///
    /// The token digests the relation's qualified name and the other two, so it
    /// changes exactly when a RESOLVE of that name would answer differently. It is
    /// never `0`.
    pub(crate) fn resolve_answer(&self, tid: u64) -> Option<(Vec<u8>, Rc<[u8]>, u64)> {
        let rel = self.registry.relation(tid)?;
        let entry = self.relation_entry(tid);
        let desc = RelDescriptorBlob {
            class: rel.kind().class(),
            pk_repeats: rel.pk_repeats(),
            serial: entry.serial,
            indexes: rel
                .indexes()
                .iter()
                .map(|ic| RelIndex {
                    cols: ic.cols(),
                    is_unique: ic.is_unique(),
                })
                .collect(),
        };
        let desc = desc.encode();
        if entry.token.get() == 0 {
            let mut h = gnitz_wire::ChecksumStream::default();
            h.update(self.qualified_name(tid).as_bytes());
            h.update(&desc);
            h.update(&entry.record);
            entry.token.set(h.digest() | 1);
        }
        Some((desc, entry.record.clone(), entry.token.get()))
    }

    /// Refuse a request built under `token` once `tid` answers a RESOLVE
    /// differently. `0` is a request built from no RESOLVE.
    pub(crate) fn check_token(&self, Target { tid, token }: Target) -> Result<(), WireFault> {
        if token == 0 {
            return Ok(());
        }
        let current = match self.caches.relations.get(&tid).map(|e| e.token.get()) {
            Some(0) => self.resolve_answer(tid).map(|(_, _, t)| t),
            t => t,
        };
        if current == Some(token) {
            return Ok(());
        }
        Err(WireFault {
            status: WireStatus::StaleCatalog,
            text: format!("relation {tid} no longer resolves as this request was planned; resolve it again"),
        })
    }
}

#[derive(Default)]
pub(in crate::catalog) struct CatalogCacheSet {
    /// TABLE_TAB and VIEW_TAB by `(schema_id, name)`. A schema's entry exists only
    /// while it holds a relation.
    pub(in crate::catalog) relation_by_name: FxHashMap<u64, FxHashMap<String, u64>>,
    pub(in crate::catalog) relations: FxHashMap<u64, RelationEntry>,
    /// Every [`RelationEntry::fks`] edge, keyed by its parent.
    fk_by_parent: FxHashMap<u64, Vec<FkEdge>>,
}

impl CatalogCacheSet {
    /// Enter registered relation `id`'s entry, and index its FK edges by parent.
    pub(in crate::catalog) fn enter(&mut self, id: u64, entry: RelationEntry) {
        for e in &entry.fks {
            self.fk_by_parent.entry(e.parent_tid).or_default().push(*e);
        }
        self.relations.insert(id, entry);
    }

    /// Remove unregistered relation `id`'s entry and its FK edges.
    pub(in crate::catalog) fn remove(&mut self, id: u64) {
        let Some(entry) = self.relations.remove(&id) else {
            return;
        };
        for e in entry.fks {
            if let Entry::Occupied(mut edges) = self.fk_by_parent.entry(e.parent_tid) {
                edges.get_mut().retain(|x| x.child_tid != id);
                if edges.get().is_empty() {
                    edges.remove();
                }
            }
        }
    }

    /// Apply a TABLE_TAB or VIEW_TAB delta to [`Self::relation_by_name`]: the two
    /// share the leading `(schema_id, name)` payload prefix. Retracted first: one
    /// delta may hand a name from one row to another.
    pub(in crate::catalog) fn apply_relation_names(&mut self, batch: &Batch) {
        let key = |i| {
            (
                payload_u64(batch, i, RELTAB_PAY_SCHEMA_ID),
                payload_str(batch, i, RELTAB_PAY_NAME).expect("a relation name passed the precheck"),
            )
        };
        let by_schema = &mut self.relation_by_name;
        for i in batch.retracted_rows() {
            let (sid, name) = key(i);
            if let Entry::Occupied(mut names) = by_schema.entry(sid) {
                names.get_mut().remove(&*name);
                if names.get().is_empty() {
                    names.remove();
                }
            }
        }
        for i in batch.live_rows() {
            let (sid, name) = key(i);
            by_schema
                .entry(sid)
                .or_default()
                .insert(name.into_owned(), gnitz_wire::sys_leading_id(batch.get_pk_bytes(i)));
        }
    }
}

/// One FK constraint as a directed edge, identical whichever end it was reached
/// from: [`CatalogEngine::fks_as_child`] reads the edges a table declares,
/// [`CatalogEngine::fks_as_parent`] those whose `parent_tid` it is. The two
/// column positions are easy to transpose, so they are named fields.
#[derive(Clone, Copy)]
pub(crate) struct FkEdge {
    /// Referencing child table id.
    pub(crate) child_tid: u64,
    /// Child column position.
    pub(crate) fk_col: usize,
    /// Referenced parent table id.
    pub(crate) parent_tid: u64,
    /// Referenced parent column position.
    pub(crate) parent_col: usize,
    /// What a delete of the referenced row does to the child's.
    pub(crate) on_delete: FkAction,
}

impl FkEdge {
    /// The column list of the child's FK index, which a parent's RESTRICT and
    /// CASCADE probes read the child through.
    pub(crate) fn child_index_cols(&self) -> PkColList {
        PkColList::from_slice(&[self.fk_col as u32])
    }
}

/// Every constraint on one table whose validation reads committed state.
pub(crate) struct RowConstraints {
    pub(crate) fks_as_child: Vec<FkEdge>,
    pub(crate) fks_as_parent: Vec<FkEdge>,
    pub(crate) uniques: Vec<(PkColList, KeySpec)>,
}

impl CatalogEngine {
    /// All FK edges where `table_id` is the child.
    pub(in crate::catalog) fn fks_as_child(&self, table_id: u64) -> &[FkEdge] {
        self.caches.relations.get(&table_id).map_or(&[], |e| e.fks.as_slice())
    }

    /// All FK edges where `parent_id` is the parent (empty when none).
    pub(crate) fn fks_as_parent(&self, parent_id: u64) -> &[FkEdge] {
        self.caches.fk_by_parent.get(&parent_id).map_or(&[], Vec::as_slice)
    }

    /// The tables a write to `table_id` locks exclusively: itself; its FK parents,
    /// against a parent DELETE; and its FK children, against a child INSERT. One
    /// that `deletes` rows takes the children of every table its deletes cascade
    /// to as well.
    pub(crate) fn write_lock_set(&self, table_id: u64, deletes: bool) -> Vec<u64> {
        let mut set: Vec<u64> = self.fks_as_child(table_id).iter().map(|e| e.parent_tid).collect();
        let mut deleted_from = vec![table_id];
        let mut at = 0;
        while let Some(&tid) = deleted_from.get(at) {
            at += 1;
            for e in self.fks_as_parent(tid) {
                set.push(e.child_tid);
                if deletes && e.on_delete == FkAction::Cascade && !deleted_from.contains(&e.child_tid) {
                    deleted_from.push(e.child_tid);
                }
            }
        }
        set.push(table_id);
        set
    }

    /// Every constraint on `tid` whose validation reads committed state.
    pub(crate) fn row_constraints(&self, tid: u64) -> RowConstraints {
        RowConstraints {
            fks_as_child: self.fks_as_child(tid).to_vec(),
            fks_as_parent: self.fks_as_parent(tid).to_vec(),
            uniques: self.registry.relation(tid).map_or_else(Vec::new, |r| {
                r.unique_indexes_to_check()
                    .map(|ic| (ic.cols(), ic.key_spec().clone()))
                    .collect()
            }),
        }
    }

    /// Whether `tid` has a constraint whose validation reads committed state:
    /// whether [`Self::row_constraints`] would list any.
    fn has_row_constraints(&self, tid: u64) -> bool {
        !self.fks_as_child(tid).is_empty()
            || !self.fks_as_parent(tid).is_empty()
            || self
                .registry
                .relation(tid)
                .is_some_and(|r| r.unique_indexes_to_check().next().is_some())
    }

    /// Does validating a write of `mode` to `tid` read committed state? One that
    /// does not may hold its table lock shared.
    pub(crate) fn push_reads_committed_state(&self, tid: u64, mode: gnitz_wire::WireConflictMode) -> bool {
        matches!(mode, gnitz_wire::WireConflictMode::Error) || self.has_row_constraints(tid)
    }
}
