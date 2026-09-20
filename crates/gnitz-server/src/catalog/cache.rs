use super::schema_block::encode_named_schema_block;
use super::*;
use gnitz_expr::payload_str;
use gnitz_wire::{IDXTAB_PAY_NAME, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, SCHEMATAB_PAY_NAME};
use rustc_hash::FxHashMap;
use std::hash::Hash;
use std::num::NonZeroU16;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

/// What one registered relation's lifetime owns: entered by its registration,
/// removed by its unregistration.
pub(in crate::catalog) struct RelationEntry {
    /// The token a client's cached schema block is validated against; a client
    /// sends 0 when it holds none.
    pub(in crate::catalog) schema_version: NonZeroU16,
    pub(in crate::catalog) schema_block: Rc<Vec<u8>>,
    /// The FK edges this relation declares as a child.
    pub(in crate::catalog) fks: Vec<FkEdge>,
    pub(in crate::catalog) pk_repeats: bool,
}

impl RelationEntry {
    pub(in crate::catalog) fn new(
        id: i64,
        schema: &SchemaDescriptor,
        defs: &[ColumnDef],
        fks: Vec<FkEdge>,
        pk_repeats: bool,
    ) -> Self {
        RelationEntry {
            schema_version: NonZeroU16::MIN,
            schema_block: Rc::new(encode_named_schema_block(schema, defs, id as u32)),
            fks,
            pk_repeats,
        }
    }

    /// Re-encode the schema block for a changed column set, under a new version.
    pub(in crate::catalog) fn reschema(&mut self, id: i64, schema: &SchemaDescriptor, defs: &[ColumnDef]) {
        self.schema_version = self.schema_version.checked_add(1).unwrap_or(NonZeroU16::MIN);
        self.schema_block = Rc::new(encode_named_schema_block(schema, defs, id as u32));
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
