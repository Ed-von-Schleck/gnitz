use super::*;
use gnitz_expr::payload_str;
use gnitz_store::schema::decode_schema_block;
use gnitz_wire::schema_block::{check_same_types, ColMeta, SchemaBlockCol};
use gnitz_wire::{RelDescriptorBlob, RelIndex};
use gnitz_wire::{IDXTAB_PAY_NAME, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, SCHEMATAB_PAY_NAME};
use rustc_hash::FxHashMap;
use std::hash::Hash;
use std::num::NonZeroU16;

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
    /// The token a client's cached schema record is validated against on a
    /// read; a client sends 0 when it holds none.
    pub(in crate::catalog) schema_version: NonZeroU16,
    pub(in crate::catalog) record: CatalogRecord,
    /// The FK edges this relation declares as a child.
    pub(in crate::catalog) fks: Vec<FkEdge>,
    pub(in crate::catalog) facts: RelFacts,
}

/// The named record for columns `defs` keyed by `pk`: every column's type, scale,
/// nullability, hidden flag and name.
pub(in crate::catalog) fn named_record(defs: &[ColumnDef], pk: &[u32]) -> Vec<u8> {
    gnitz_wire::schema_block::encode(
        defs.iter().map(|d| SchemaBlockCol {
            ty: d.ty,
            meta: ColMeta {
                nullable: d.is_nullable,
                hidden: d.is_hidden,
            },
            name: d.name.as_bytes(),
        }),
        pk,
    )
}

/// A relation's named record — the one every client-bound reply and every
/// push/DDL SAL slot carries — and its decode.
#[derive(Clone)]
pub(crate) struct CatalogRecord {
    pub(crate) bytes: Rc<[u8]>,
    decode: SchemaDescriptor,
}

impl CatalogRecord {
    pub(in crate::catalog) fn new(pk: &[u32], defs: &[ColumnDef]) -> Self {
        let bytes = named_record(defs, pk);
        let decode = decode_schema_block(&bytes)
            .expect("a catalog relation's columns were admitted by the bounds its record's decode checks");
        CatalogRecord { bytes: Rc::from(bytes), decode }
    }

    /// What `frame_record` decodes to, unless it lays out other columns.
    pub(crate) fn decode_of(&self, frame_record: &[u8]) -> Result<SchemaDescriptor, String> {
        check_same_types(frame_record, &self.bytes).map(|()| self.decode)
    }
}

impl RelationEntry {
    pub(in crate::catalog) fn new(pk: &[u32], defs: &[ColumnDef], fks: Vec<FkEdge>, facts: RelFacts) -> Self {
        RelationEntry {
            schema_version: NonZeroU16::MIN,
            record: CatalogRecord::new(pk, defs),
            fks,
            facts,
        }
    }

    /// Re-encode the record for a changed column set, under a new version.
    pub(in crate::catalog) fn reschema(&mut self, pk: &[u32], defs: &[ColumnDef]) {
        self.schema_version = self.schema_version.checked_add(1).unwrap_or(NonZeroU16::MIN);
        self.record = CatalogRecord::new(pk, defs);
    }
}

impl CatalogEngine {
    fn relation_entry(&self, tid: i64) -> &RelationEntry {
        self.caches
            .relations
            .get(&tid)
            .expect("every registered relation has an entry")
    }

    /// `tid`'s record; `None` for an unregistered id.
    pub(crate) fn schema_record(&self, tid: i64) -> Option<CatalogRecord> {
        self.caches.relations.get(&tid).map(|e| e.record.clone())
    }

    /// What `frame_record` decodes to, when it lays out `tid`'s columns.
    pub(crate) fn known_decode(&self, tid: i64, frame_record: &[u8]) -> Option<SchemaDescriptor> {
        self.caches.relations.get(&tid)?.record.decode_of(frame_record).ok()
    }

    /// Whether a frame whose record matched `seen` still lays out `tid`'s columns.
    pub(crate) fn recheck_record(&self, tid: i64, seen: &Rc<[u8]>) -> Result<(), String> {
        let current = &self.relation_entry(tid).record.bytes;
        if Rc::ptr_eq(seen, current) {
            return Ok(());
        }
        check_same_types(seen, current)
    }

    /// The schema record a reply to a client at `client_version` carries, and the
    /// version its flags report.
    pub(crate) fn negotiated_schema_block(&self, tid: i64, client_version: u16) -> (Option<Rc<[u8]>>, u16) {
        let entry = self.relation_entry(tid);
        let version = entry.schema_version.get();
        let block = (client_version != version).then(|| entry.record.bytes.clone());
        (block, version)
    }

    /// What a RESOLVE of `tid` answers: its descriptor, and its named record with
    /// that record's version. `None` for an unregistered id.
    pub(crate) fn resolve_answer(&self, tid: i64) -> Option<(RelDescriptorBlob, Rc<[u8]>, u16)> {
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
        Some((desc, entry.record.bytes.clone(), entry.schema_version.get()))
    }
}

#[derive(Default)]
pub(in crate::catalog) struct CatalogCacheSet {
    pub(in crate::catalog) schema_by_name: FxHashMap<String, i64>,
    pub(in crate::catalog) schema_by_id: FxHashMap<i64, String>,
    pub(in crate::catalog) entity_by_qname: FxHashMap<String, i64>,
    pub(in crate::catalog) index_by_name: FxHashMap<String, i64>,
    pub(in crate::catalog) relations: FxHashMap<i64, RelationEntry>,
    /// Every [`RelationEntry::fks`] edge, keyed by its parent.
    pub(in crate::catalog) fk_by_parent: FxHashMap<i64, Vec<FkEdge>>,
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
        let sid = |i| batch.get_pk(i) as i64;
        apply_map_delta(&mut self.caches.schema_by_name, batch, |i| (name(i), sid(i)));
        apply_map_delta(&mut self.caches.schema_by_id, batch, |i| (sid(i), name(i)));
    }

    /// TABLE_TAB and VIEW_TAB share the leading `(schema_id, name)` payload prefix.
    pub(in crate::catalog) fn apply_entity_caches(&mut self, batch: &Batch) {
        let CatalogCacheSet { schema_by_id, entity_by_qname, .. } = &mut self.caches;
        apply_map_delta(entity_by_qname, batch, |i| {
            let sid = payload_u64(batch, i, RELTAB_PAY_SCHEMA_ID) as i64;
            let schema = schema_by_id.get(&sid).expect("a relation row names a live schema");
            let qualified = gnitz_wire::qualified_key(schema, payload_str(batch, i, RELTAB_PAY_NAME));
            (qualified, batch.get_pk(i) as i64)
        });
    }

    pub(in crate::catalog) fn apply_index_caches(&mut self, batch: &Batch) {
        apply_map_delta(&mut self.caches.index_by_name, batch, |i| {
            (payload_string(batch, i, IDXTAB_PAY_NAME), batch.get_pk(i) as i64)
        });
    }
}
