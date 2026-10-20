use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use gnitz_store::relation::Relation;
use gnitz_wire::control::Target;
use gnitz_wire::schema_block::check_same_types;
use gnitz_wire::{payload_str, payload_u64};
use gnitz_wire::{RelDescriptorBlob, RelIndex, WireFault, WireStatus, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;

use super::constraints::FkEdge;
use super::CatalogEngine;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

/// What one registered relation's lifetime owns: entered by its registration,
/// removed by its unregistration.
pub(in crate::catalog) struct RelationEntry {
    /// The relation's [`encode_record`](super::sys_tables::encode_record).
    pub(in crate::catalog) record: Rc<[u8]>,
    /// The FK edges this relation declares as a child.
    pub(in crate::catalog) fks: Vec<FkEdge>,
    /// [`gnitz_wire::TableProps::serial`].
    pub(in crate::catalog) serial: bool,
    /// [`CatalogEngine::resolve_token`], computed on first use; `0` until then.
    /// Reset by every hook of a family that can change a RESOLVE answer.
    pub(in crate::catalog) token: Cell<u64>,
}

impl RelationEntry {
    pub(in crate::catalog) fn new(record: Rc<[u8]>, fks: Vec<FkEdge>, serial: bool) -> Self {
        RelationEntry { record, fks, serial, token: Cell::new(0) }
    }
}

impl CatalogEngine {
    fn relation_entry(&self, tid: u64) -> &RelationEntry {
        self.caches
            .relations
            .get(&tid)
            .expect("every registered relation has an entry")
    }

    /// `tid`'s record; `None` for an unregistered id.
    pub(crate) fn schema_record(&self, tid: u64) -> Option<Rc<[u8]>> {
        self.caches.relations.get(&tid).map(|e| e.record.clone())
    }

    /// What `frame_record` decodes to, when it lays out `tid`'s columns.
    pub(crate) fn known_decode(&self, tid: u64, frame_record: &[u8]) -> Option<SchemaDescriptor> {
        check_same_types(frame_record, &self.caches.relations.get(&tid)?.record).ok()?;
        self.registry.relation(tid).map(Relation::schema)
    }

    /// Whether a frame whose record matched `seen` still lays out `tid`'s columns.
    pub(crate) fn recheck_record(&self, tid: u64, seen: &Rc<[u8]>) -> Result<(), String> {
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
            let mut h = gnitz_wire::RowHasher::default();
            h.update(self.qualified_name(tid).as_bytes());
            h.update(&desc);
            h.update(&entry.record);
            entry.token.set(h.digest() | 1);
        }
        Some((desc, entry.record.clone(), entry.token.get()))
    }

    /// `tid`'s descriptor token (see [`Self::resolve_answer`]); `None` for an
    /// unregistered id.
    pub(in crate::catalog) fn resolve_token(&self, tid: u64) -> Option<u64> {
        match self.caches.relations.get(&tid)?.token.get() {
            0 => self.resolve_answer(tid).map(|(_, _, token)| token),
            token => Some(token),
        }
    }

    /// Refuse a request built under `token` once `tid` answers a RESOLVE
    /// differently. `0` is a request built from no RESOLVE.
    pub(crate) fn check_token(&self, Target { tid, token }: Target) -> Result<(), WireFault> {
        if token == 0 || self.resolve_token(tid) == Some(token) {
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
    pub(in crate::catalog) fk_by_parent: FxHashMap<u64, Vec<FkEdge>>,
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
                .insert(name.into_owned(), batch.get_pk(i) as u64);
        }
    }
}
