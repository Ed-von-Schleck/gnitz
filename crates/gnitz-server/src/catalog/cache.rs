use super::*;
use gnitz_expr::payload_str;
use gnitz_wire::control::Target;
use gnitz_wire::schema_block::check_same_types;
use gnitz_wire::{RelDescriptorBlob, RelIndex, WireFault, WireStatus};
use gnitz_wire::{RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, SCHEMATAB_PAY_NAME};
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::collections::hash_map::Entry;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

/// What one registered relation's lifetime owns: entered by its registration,
/// removed by its unregistration.
pub(in crate::catalog) struct RelationEntry {
    /// The relation's named record — the one a RESOLVE reply and every push/DDL
    /// SAL slot carries.
    pub(in crate::catalog) record: Rc<[u8]>,
    /// The FK edges this relation declares as a child.
    pub(in crate::catalog) fks: Vec<FkEdge>,
    /// [`gnitz_wire::TableProps::serial`].
    pub(in crate::catalog) serial: bool,
}

/// The named record of a relation keyed `pk` over `defs`.
pub(in crate::catalog) fn encode_record(pk: &[u32], defs: &[CatalogColumn]) -> Rc<[u8]> {
    Rc::from(gnitz_wire::schema_block::encode(
        defs.iter().map(|c| c.def.block_col()),
        pk,
    ))
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
        let token = *self.caches.resolve_tokens.borrow_mut().entry(tid).or_insert_with(|| {
            let mut h = gnitz_wire::RowHasher::default();
            h.update(self.qualified_name(tid).as_bytes());
            h.update(&desc);
            h.update(&entry.record);
            h.digest() | 1
        });
        Some((desc, entry.record.clone(), token))
    }

    /// `tid`'s descriptor token (see [`Self::resolve_answer`]); `None` for an
    /// unregistered id.
    pub(crate) fn resolve_token(&self, tid: u64) -> Option<u64> {
        let memo = self.caches.resolve_tokens.borrow().get(&tid).copied();
        memo.or_else(|| self.resolve_answer(tid).map(|(_, _, token)| token))
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
    /// SCHEMA_TAB by name.
    pub(in crate::catalog) schema_by_name: FxHashMap<String, u64>,
    /// TABLE_TAB and VIEW_TAB by `(schema_id, name)`. A schema's entry exists only
    /// while it holds a relation.
    pub(in crate::catalog) relation_by_name: FxHashMap<u64, FxHashMap<String, u64>>,
    pub(in crate::catalog) relations: FxHashMap<u64, RelationEntry>,
    /// Every [`RelationEntry::fks`] edge, keyed by its parent.
    pub(in crate::catalog) fk_by_parent: FxHashMap<u64, Vec<FkEdge>>,
    /// [`CatalogEngine::resolve_token`] by relation, computed on first use and
    /// emptied by every hook of a family that can change a RESOLVE answer.
    pub(in crate::catalog) resolve_tokens: RefCell<FxHashMap<u64, u64>>,
}

// ---------------------------------------------------------------------------
// Name-index delta appliers on CatalogEngine
// ---------------------------------------------------------------------------

impl CatalogEngine {
    // Both appliers retract first: one delta may hand a name from one row to another.

    pub(in crate::catalog) fn apply_schema_names(&mut self, batch: &Batch) {
        let names = &mut self.caches.schema_by_name;
        for i in batch.retracted_rows() {
            names.remove(payload_str(batch, i, SCHEMATAB_PAY_NAME));
        }
        for i in batch.live_rows() {
            names.insert(payload_string(batch, i, SCHEMATAB_PAY_NAME), batch.get_pk(i) as u64);
        }
    }

    /// TABLE_TAB and VIEW_TAB share the leading `(schema_id, name)` payload prefix.
    pub(in crate::catalog) fn apply_relation_names(&mut self, batch: &Batch) {
        let key = |i| {
            (
                payload_u64(batch, i, RELTAB_PAY_SCHEMA_ID),
                payload_str(batch, i, RELTAB_PAY_NAME),
            )
        };
        let by_schema = &mut self.caches.relation_by_name;
        for i in batch.retracted_rows() {
            let (sid, name) = key(i);
            if let Entry::Occupied(mut names) = by_schema.entry(sid) {
                names.get_mut().remove(name);
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
                .insert(name.to_string(), batch.get_pk(i) as u64);
        }
    }
}
