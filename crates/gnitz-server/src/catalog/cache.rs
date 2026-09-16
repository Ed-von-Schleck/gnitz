use super::*;
use gnitz_wire::{
    COLTAB_PAY_FK_TABLE_ID, IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID,
    SCHEMATAB_PAY_NAME,
};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

/// Cached schema wire data for one table: the encoded block and the schema
/// version it was built at. Both share one invalidation lifecycle
/// (`clear_col_cache_no_bump` drops the entry whole, and the version bump that
/// follows means a surviving entry always carries its build-time version).
#[derive(Clone)]
pub(crate) struct SchemaWireEntry {
    pub(crate) block: Rc<Vec<u8>>,
    /// The owning table's schema version when the block was built.
    pub(crate) version: u16,
}

#[derive(Default)]
pub(in crate::catalog) struct CatalogCacheSet {
    pub(in crate::catalog) schema_by_name: FxHashMap<String, i64>,
    pub(in crate::catalog) schema_by_id: FxHashMap<i64, String>,
    pub(in crate::catalog) entity_by_qname: FxHashMap<String, i64>,
    pub(in crate::catalog) entity_by_id: FxHashMap<i64, (String, String)>,
    /// Live member relations (tables and views alike) per schema id. Only the
    /// count is read — the non-empty-schema DROP guard — but the members are
    /// held as a set so a re-applied `+1` on one id stays idempotent.
    pub(in crate::catalog) members_by_schema: FxHashMap<i64, FxHashSet<i64>>,
    /// Full decoded column definitions per table, filled on demand by
    /// `read_column_defs` — the one COL_TAB read every name / hidden-flag
    /// consumer goes through.
    pub(in crate::catalog) col_defs: FxHashMap<i64, Rc<Vec<ColumnDef>>>,
    /// Cached schema wire data per table. Built from (SchemaDescriptor,
    /// col_defs) and reused across SEEK/SCAN responses. Invalidated alongside
    /// col_defs when DDL modifies the table schema.
    pub(in crate::catalog) schema_wire_cache: FxHashMap<i64, SchemaWireEntry>,
    /// Per-table schema version — the token a client's cached schema block is
    /// validated against. Bumped on every applied COL_TAB delta, wrapping
    /// 65535 → 1; absent means 1, and 0 is reserved for "client has no cached
    /// schema". Not durable: it restarts from 1 and the boot COL_TAB replay
    /// leaves every table at 2. Clients cache it per connection, and a restart
    /// drops every connection, so the reset cannot alias.
    pub(in crate::catalog) schema_version: FxHashMap<i64, u16>,
    pub(in crate::catalog) index_by_name: FxHashMap<String, i64>,
    pub(in crate::catalog) indices_by_owner: FxHashMap<i64, Vec<i64>>,
    /// The internal chain segments each user view owns, from `VIEW_TAB`'s
    /// `owner_view_id` — what the drop cascade retracts. Flat by construction,
    /// so the cascade cannot recurse.
    pub(in crate::catalog) segments_by_owner: FxHashMap<i64, Vec<i64>>,
    pub(in crate::catalog) fk_by_child: FxHashMap<i64, Vec<FkEdge>>,
    pub(in crate::catalog) fk_by_parent: FxHashMap<i64, Vec<FkEdge>>,
    /// Tables whose writes need the push lock, each with its lock set — itself plus
    /// its FK parents and children, sorted and deduped. Maintained by `recompute_needs_lock`.
    pub(in crate::catalog) needs_lock: FxHashMap<i64, Vec<i64>>,
}

/// Append `v` at `key` unless `same` matches an element already there — a
/// re-applied row must not leave a duplicate [`remove_where`] cannot fully undo.
fn insert_owned<K: Eq + std::hash::Hash, V>(map: &mut FxHashMap<K, Vec<V>>, key: K, v: V, same: impl Fn(&V) -> bool) {
    let owned = map.entry(key).or_default();
    if !owned.iter().any(same) {
        owned.push(v);
    }
}

/// Remove the first element matching `pred` from the Vec at `key`, dropping
/// the map entry once the Vec empties — [`insert_owned`]'s inverse, and the
/// shared retract shape of every Vec-valued cache (`indices_by_owner`,
/// `segments_by_owner`, `fk_by_child`, `fk_by_parent`).
fn remove_where<K: Eq + std::hash::Hash, V>(map: &mut FxHashMap<K, Vec<V>>, key: K, pred: impl Fn(&V) -> bool) {
    if let Entry::Occupied(mut e) = map.entry(key) {
        let items = e.get_mut();
        if let Some(pos) = items.iter().position(&pred) {
            items.swap_remove(pos);
        }
        if e.get().is_empty() {
            e.remove();
        }
    }
}

impl CatalogCacheSet {
    /// Remove the per-table column caches without bumping the schema version.
    /// Use for table drop (no new schema to advertise) or as the inner step of
    /// `invalidate_col_names`.
    pub(in crate::catalog) fn clear_col_cache_no_bump(&mut self, id: i64) {
        self.col_defs.remove(&id);
        self.schema_wire_cache.remove(&id);
    }

    pub(in crate::catalog) fn invalidate_col_names(&mut self, id: i64) {
        self.clear_col_cache_no_bump(id);
        // Wraps 65535 → 1: 0 is reserved for "client has no cached schema".
        let v = self.schema_version.entry(id).or_insert(1);
        *v = if *v == u16::MAX { 1 } else { *v + 1 };
    }

    /// Return the current schema version for `id`. Absent = version 1.
    pub(crate) fn get_schema_version(&self, id: i64) -> u16 {
        self.schema_version.get(&id).copied().unwrap_or(1)
    }

    /// Drop the per-table schema version when a table/view is fully removed.
    /// Table ids are monotonic and never reused, so a counter left behind here
    /// would become permanent dead memory.
    pub(in crate::catalog) fn purge_schema_version(&mut self, id: i64) {
        self.schema_version.remove(&id);
    }
}

// ---------------------------------------------------------------------------
// Cache delta appliers on CatalogEngine
// ---------------------------------------------------------------------------

impl CatalogEngine {
    /// Maintain `schema_by_name` and `schema_by_id` from one pass over a
    /// SCHEMA_TAB delta — the two caches share their lifecycle and key data.
    pub(in crate::catalog) fn apply_schema_caches(&mut self, batch: &Batch) {
        for i in 0..batch.len() {
            let weight = batch.get_weight(i);
            let sid = batch.get_pk(i) as i64;
            let name = payload_string(batch, i, SCHEMATAB_PAY_NAME);

            if weight > 0 {
                self.caches.schema_by_name.insert(name.clone(), sid);
                self.caches.schema_by_id.insert(sid, name);
            } else {
                self.caches.schema_by_name.remove(&name);
                self.caches.schema_by_id.remove(&sid);
            }
        }
    }

    /// Maintain `entity_by_qname`, `entity_by_id` and `members_by_schema` from
    /// one pass over a TABLE_TAB or VIEW_TAB delta — the two families share the
    /// leading `(schema_id, name)` payload prefix.
    pub(in crate::catalog) fn apply_entity_caches(&mut self, family: SysFamily, batch: &Batch) {
        for i in 0..batch.len() {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = payload_u64(batch, i, RELTAB_PAY_SCHEMA_ID) as i64;

            if weight > 0 {
                let name = payload_string(batch, i, RELTAB_PAY_NAME);
                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let qualified = gnitz_wire::qualified_key(&schema_name, &name);
                self.caches.entity_by_qname.insert(qualified, tid);
                self.caches.entity_by_id.insert(tid, (schema_name, name));
                self.caches.members_by_schema.entry(sid).or_default().insert(tid);
                self.apply_segment_owner(family, batch, i, tid, weight);
            } else {
                self.apply_segment_owner(family, batch, i, tid, weight);
                if let Entry::Occupied(mut e) = self.caches.members_by_schema.entry(sid) {
                    e.get_mut().remove(&tid);
                    if e.get().is_empty() {
                        e.remove();
                    }
                }
                // Retract sequence, in this order: read the old name
                // from entity_by_id → remove the qname → clear the per-table
                // column caches → remove the id entry.
                if let Some((sn, en)) = self.caches.entity_by_id.get(&tid) {
                    let qualified = gnitz_wire::qualified_key(sn, en);
                    self.caches.entity_by_qname.remove(&qualified);
                }
                // Entity dropped: clear per-table cache entries without bumping
                // the schema version (there is no new schema to advertise). The
                // counter is purged by `unregister_relation`.
                self.caches.clear_col_cache_no_bump(tid);
                self.caches.entity_by_id.remove(&tid);
            }
        }
    }

    /// Drop the cached column defs of every owner the COL_TAB delta touches, and bump
    /// the schema version of the registered ones: only those have a schema a client cached.
    pub(in crate::catalog) fn apply_col_names_invalidate(&mut self, batch: &Batch) {
        let mut owners: Vec<i64> = (0..batch.len())
            .map(|i| SysFamily::Column.leading_id(batch.get_pk(i)))
            .collect();
        owners.sort_unstable();
        owners.dedup();
        for owner in owners {
            if self.registry.has_id(owner) {
                self.caches.invalidate_col_names(owner);
            } else {
                self.caches.clear_col_cache_no_bump(owner);
            }
        }
    }

    /// Maintain `segments_by_owner` from one VIEW_TAB row. A no-op for
    /// TABLE_TAB, which shares `apply_entity_caches` but carries no owner
    /// column.
    fn apply_segment_owner(&mut self, family: SysFamily, batch: &Batch, i: usize, vid: i64, weight: i64) {
        if family != SysFamily::View {
            return;
        }
        let owner = payload_u64(batch, i, gnitz_wire::VIEWTAB_PAY_OWNER_VIEW_ID) as i64;
        if owner == 0 {
            return;
        }
        if weight > 0 {
            insert_owned(&mut self.caches.segments_by_owner, owner, vid, |&x| x == vid);
        } else {
            remove_where(&mut self.caches.segments_by_owner, owner, |&id| id == vid);
        }
    }

    /// Maintain `index_by_name` and `indices_by_owner` from one pass over an
    /// IDX_TAB delta — both key off the same row and share their lifecycle.
    pub(in crate::catalog) fn apply_index_caches(&mut self, batch: &Batch) {
        for i in 0..batch.len() {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = payload_u64(batch, i, IDXTAB_PAY_OWNER_ID) as i64;
            let name = payload_string(batch, i, IDXTAB_PAY_NAME);

            if weight > 0 {
                self.caches.index_by_name.insert(name, idx_id);
                insert_owned(&mut self.caches.indices_by_owner, owner_id, idx_id, |&x| x == idx_id);
            } else {
                self.caches.index_by_name.remove(&name);
                remove_where(&mut self.caches.indices_by_owner, owner_id, |&id| id == idx_id);
            }
        }
    }

    /// All three derived states an FK-carrying COL_TAB row feeds, off one decode
    /// of it: both `fk_by_*` indexes, and the lock set of either end (which names
    /// the other). Gated on the FK word before the decode: at boot replay the
    /// batch is every live column record in the database.
    pub(in crate::catalog) fn apply_fk_edges_and_locks(&mut self, batch: &Batch) {
        let caches = &mut self.caches;
        let mut tids: Vec<i64> = Vec::new();
        for i in 0..batch.len() {
            if payload_u64(batch, i, COLTAB_PAY_FK_TABLE_ID) == 0 {
                continue;
            }
            let ident = read_col_tab_ident(batch, i);
            if !ident.declares_fk() {
                continue;
            }
            let edge = FkEdge {
                child_tid: ident.owner_id,
                fk_col: ident.col_idx as usize,
                parent_tid: ident.fk_table_id,
                parent_col: ident.fk_col_idx as usize,
            };
            // One edge per (child, child column), whichever end it is indexed by.
            let same = |e: &FkEdge| e.child_tid == edge.child_tid && e.fk_col == edge.fk_col;

            if batch.get_weight(i) > 0 {
                insert_owned(&mut caches.fk_by_child, edge.child_tid, edge, same);
                insert_owned(&mut caches.fk_by_parent, edge.parent_tid, edge, same);
            } else {
                remove_where(&mut caches.fk_by_child, edge.child_tid, same);
                remove_where(&mut caches.fk_by_parent, edge.parent_tid, same);
            }

            tids.push(edge.child_tid);
            tids.push(edge.parent_tid);
        }
        self.relock_all(tids);
    }

    /// Recompute the lock set of each id exactly once (sort + dedup first: an
    /// id may be named by several rows of one delta).
    fn relock_all(&mut self, mut tids: Vec<i64>) {
        tids.sort_unstable();
        tids.dedup();
        for tid in tids {
            self.recompute_needs_lock(tid);
        }
    }

    /// Recompute needs_lock for `tid` from current cache + dag state, and
    /// materialize its sorted lock set (see `CatalogCacheSet::needs_lock`).
    pub(in crate::catalog) fn recompute_needs_lock(&mut self, tid: i64) {
        let fk_child_count = self.caches.fk_by_child.get(&tid).map_or(0, |v| v.len());
        let fk_parent_count = self.caches.fk_by_parent.get(&tid).map_or(0, |v| v.len());
        // Every base table needs the lock: its writes run `enforce_unique_pk`
        // against the store, and only a base table can own a unique secondary
        // index. Views and system tables need it only as an FK endpoint.
        let is_base_table = self.registry.relation(tid).is_some_and(|e| e.kind().is_base_table());

        let needs = fk_child_count > 0 || fk_parent_count > 0 || is_base_table;
        if needs {
            let mut tids = vec![tid];
            if let Some(edges) = self.caches.fk_by_child.get(&tid) {
                tids.extend(edges.iter().map(|e| e.parent_tid));
            }
            if let Some(edges) = self.caches.fk_by_parent.get(&tid) {
                tids.extend(edges.iter().map(|e| e.child_tid));
            }
            tids.sort_unstable();
            tids.dedup();
            self.caches.needs_lock.insert(tid, tids);
        } else {
            self.caches.needs_lock.remove(&tid);
        }
    }
}
