use super::*;
use gnitz_expr::payload_str;
use gnitz_wire::schema_block::check_same_types;
use gnitz_wire::{RelDescriptorBlob, RelIndex};
use gnitz_wire::{IDXTAB_PAY_NAME, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, SCHEMATAB_PAY_NAME};
use rustc_hash::FxHashMap;
use std::hash::Hash;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

/// What a RESOLVE reports of a relation beyond its schema and indexes.
#[derive(Clone, Copy, Default)]
pub(in crate::catalog) struct RelFacts {
    pub(in crate::catalog) pk_repeats: bool,
    /// [`gnitz_wire::TableProps::serial`].
    pub(in crate::catalog) serial: bool,
}

/// What one registered relation's lifetime owns: entered by its registration,
/// removed by its unregistration.
pub(in crate::catalog) struct RelationEntry {
    /// The relation's named record — the one every client-bound reply and every
    /// push/DDL SAL slot carries.
    pub(in crate::catalog) record: Rc<[u8]>,
    /// The FK edges this relation declares as a child.
    pub(in crate::catalog) fks: Vec<FkEdge>,
    pub(in crate::catalog) facts: RelFacts,
}

/// The named record of a relation keyed `pk` over `defs`.
pub(in crate::catalog) fn encode_record(pk: &[u32], defs: &[CatalogColumn]) -> Rc<[u8]> {
    Rc::from(gnitz_wire::schema_block::encode(
        defs.iter().map(|c| c.def.block_col()),
        pk,
    ))
}

impl RelationEntry {
    pub(in crate::catalog) fn new(pk: &[u32], defs: &[CatalogColumn], fks: Vec<FkEdge>, facts: RelFacts) -> Self {
        RelationEntry {
            record: encode_record(pk, defs),
            fks,
            facts,
        }
    }

    /// Re-encode the record for a changed column set.
    pub(in crate::catalog) fn reschema(&mut self, pk: &[u32], defs: &[CatalogColumn]) {
        self.record = encode_record(pk, defs);
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
        let current = &self.relation_entry(tid).record;
        if Rc::ptr_eq(seen, current) {
            return Ok(());
        }
        check_same_types(seen, current)
    }

    /// What a RESOLVE of `tid` answers: its descriptor and its named record.
    /// `None` for an unregistered id.
    pub(crate) fn resolve_answer(&self, tid: u64) -> Option<(RelDescriptorBlob, Rc<[u8]>)> {
        let rel = self.registry.relation(tid)?;
        let entry = self.relation_entry(tid);
        let desc = RelDescriptorBlob {
            class: rel.kind().class(),
            pk_repeats: entry.facts.pk_repeats,
            serial: entry.facts.serial,
            indexes: rel
                .indexes()
                .iter()
                .map(|ic| RelIndex {
                    cols: ic.cols(),
                    is_unique: ic.is_unique(),
                })
                .collect(),
        };
        Some((desc, entry.record.clone()))
    }
}

#[derive(Default)]
pub(in crate::catalog) struct CatalogCacheSet {
    pub(in crate::catalog) schema_by_name: FxHashMap<String, u64>,
    pub(in crate::catalog) schema_by_id: FxHashMap<u64, String>,
    pub(in crate::catalog) entity_by_qname: FxHashMap<String, u64>,
    pub(in crate::catalog) index_by_name: FxHashMap<String, u64>,
    pub(in crate::catalog) relations: FxHashMap<u64, RelationEntry>,
    /// Every [`RelationEntry::fks`] edge, keyed by its parent.
    pub(in crate::catalog) fk_by_parent: FxHashMap<u64, Vec<FkEdge>>,
}

// ---------------------------------------------------------------------------
// Cache delta appliers on CatalogEngine
// ---------------------------------------------------------------------------

/// Apply `batch` to a map holding one `row(i)` entry per live row. Retractions go
/// first: one delta may hand a key from one row to another.
fn apply_map_delta<K: Eq + Hash, V>(map: &mut FxHashMap<K, V>, batch: &Batch, row: impl Fn(usize) -> (K, V)) {
    for i in batch.retracted_rows() {
        map.remove(&row(i).0);
    }
    for i in batch.live_rows() {
        let (k, v) = row(i);
        map.insert(k, v);
    }
}

impl CatalogEngine {
    pub(in crate::catalog) fn apply_schema_caches(&mut self, batch: &Batch) {
        let name = |i| payload_string(batch, i, SCHEMATAB_PAY_NAME);
        let sid = |i| batch.get_pk(i) as u64;
        apply_map_delta(&mut self.caches.schema_by_name, batch, |i| (name(i), sid(i)));
        apply_map_delta(&mut self.caches.schema_by_id, batch, |i| (sid(i), name(i)));
    }

    /// TABLE_TAB and VIEW_TAB share the leading `(schema_id, name)` payload prefix.
    pub(in crate::catalog) fn apply_entity_caches(&mut self, batch: &Batch) {
        let CatalogCacheSet { schema_by_id, entity_by_qname, .. } = &mut self.caches;
        apply_map_delta(entity_by_qname, batch, |i| {
            let sid = payload_u64(batch, i, RELTAB_PAY_SCHEMA_ID);
            let schema = schema_by_id.get(&sid).expect("a relation row names a live schema");
            let qualified = gnitz_wire::qualified_key(schema, payload_str(batch, i, RELTAB_PAY_NAME));
            (qualified, batch.get_pk(i) as u64)
        });
    }

    pub(in crate::catalog) fn apply_index_caches(&mut self, batch: &Batch) {
        apply_map_delta(&mut self.caches.index_by_name, batch, |i| {
            (payload_string(batch, i, IDXTAB_PAY_NAME), batch.get_pk(i) as u64)
        });
    }
}
