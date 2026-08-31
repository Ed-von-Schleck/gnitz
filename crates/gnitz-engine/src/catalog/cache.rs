use super::*;
use gnitz_wire::{
    COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND,
    IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID, SCHEMATAB_PAY_NAME, TABTAB_PAY_NAME, TABTAB_PAY_SCHEMA_ID,
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
pub struct SchemaWireEntry {
    pub block: Rc<Vec<u8>>,
    /// The owning table's schema version when the block was built.
    pub version: u16,
}

#[derive(Default)]
pub(crate) struct CatalogCacheSet {
    pub(crate) schema_by_name: FxHashMap<String, i64>,
    pub(crate) schema_by_id: FxHashMap<i64, String>,
    pub(crate) entity_by_qname: FxHashMap<String, i64>,
    pub(crate) entity_by_id: FxHashMap<i64, (String, String)>,
    /// Live member relations (tables and views alike) per schema id. Only the
    /// count is read — the non-empty-schema DROP guard — but the members are
    /// held as a set so a re-applied `+1` on one id stays idempotent.
    pub(crate) members_by_schema: FxHashMap<i64, FxHashSet<i64>>,
    /// Full decoded column definitions per table, filled on demand by
    /// `read_column_defs` — the one COL_TAB read every name / hidden-flag
    /// consumer goes through.
    pub(crate) col_defs: FxHashMap<i64, Rc<Vec<ColumnDef>>>,
    /// Cached schema wire data per table. Built from (SchemaDescriptor,
    /// col_defs) and reused across SEEK/SCAN responses. Invalidated alongside
    /// col_defs when DDL modifies the table schema.
    pub(crate) schema_wire_cache: FxHashMap<i64, SchemaWireEntry>,
    /// Per-table schema version — the token a client's cached schema block is
    /// validated against. Bumped on every applied COL_TAB delta, wrapping
    /// 65535 → 1; absent means 1, and 0 is reserved for "client has no cached
    /// schema". Not durable: it restarts from 1 and the boot COL_TAB replay
    /// leaves every table at 2. Clients cache it per connection, and a restart
    /// drops every connection, so the reset cannot alias.
    pub(crate) schema_version: FxHashMap<i64, u16>,
    pub(crate) index_by_name: FxHashMap<String, i64>,
    pub(crate) indices_by_owner: FxHashMap<i64, Vec<i64>>,
    pub(crate) fk_by_child: FxHashMap<i64, Vec<FkEdge>>,
    pub(crate) fk_by_parent: FxHashMap<i64, Vec<FkEdge>>,
    /// Tables whose writes need the push lock, each with its materialized lock
    /// set: the table itself plus all FK parents and children, sorted ascending
    /// and deduped for deadlock-free acquisition. Recomputed by
    /// `recompute_needs_lock` on every trigger (`relock_from_table_delta` /
    /// `relock_from_column_delta`), so `fk_lock_set` is a plain borrow on the
    /// push path.
    pub(crate) needs_lock: FxHashMap<i64, Vec<i64>>,
}

/// Remove the first element matching `pred` from the Vec at `key`, dropping
/// the map entry once the Vec empties — the shared retract shape of the
/// Vec-valued caches (`indices_by_owner`, `fk_by_child`, `fk_by_parent`).
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

/// Does COL_TAB row `i` declare a foreign key? An FK constrains a *base
/// table's* column; a view's `COL_TAB` rows are clones of the projected source
/// defs, so they carry the source's `fk_table_id` without being a constraint
/// themselves — reading one as a child would put a view id in a base table's
/// lock set and fail every parent DELETE on the view's missing FK index.
fn coltab_row_declares_fk(batch: &Batch, i: usize) -> bool {
    batch.read_payload_u64(i, COLTAB_PAY_FK_TABLE_ID) != 0
        && batch.read_payload_u64(i, COLTAB_PAY_OWNER_KIND) as i64 == OWNER_KIND_TABLE
}

impl CatalogCacheSet {
    /// Remove the per-table column caches without bumping the schema version.
    /// Use for table drop (no new schema to advertise) or as the inner step of
    /// `invalidate_col_names`.
    pub(crate) fn clear_col_cache_no_bump(&mut self, id: i64) {
        self.col_defs.remove(&id);
        self.schema_wire_cache.remove(&id);
    }

    pub(crate) fn invalidate_col_names(&mut self, id: i64) {
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
    /// Call this at the tail of the drop hook — *after* the column cascade,
    /// whose `invalidate_col_names` bump would otherwise `or_insert` the counter
    /// straight back. Table ids are monotonic and never reused, so a counter
    /// left behind here would become permanent dead memory.
    pub(crate) fn purge_schema_version(&mut self, id: i64) {
        self.schema_version.remove(&id);
    }
}

// ---------------------------------------------------------------------------
// Cache delta appliers on CatalogEngine
// ---------------------------------------------------------------------------

impl CatalogEngine {
    /// Maintain `schema_by_name` and `schema_by_id` from one pass over a
    /// SCHEMA_TAB delta — the two caches share their lifecycle and key data.
    pub(crate) fn apply_schema_caches(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let sid = batch.get_pk(i) as i64;
            let name = batch.read_payload_string(i, SCHEMATAB_PAY_NAME);

            if weight > 0 {
                self.caches.schema_by_name.insert(name.clone(), sid);
                self.caches.schema_by_id.insert(sid, name);
                // Re-derive next_schema_id from the durable SCHEMA_TAB row so a
                // crash-before-checkpoint never re-issues it (advance_sequence is
                // memtable-only; this row is fsync'd at CREATE). The Schema family
                // has no register hook, so this applier is its re-derive site.
                raise_id_counter(&mut self.next_schema_id, sid);
            } else {
                self.caches.schema_by_name.remove(&name);
                self.caches.schema_by_id.remove(&sid);
            }
        }
    }

    /// Maintain `entity_by_qname` and `entity_by_id` from one pass over a
    /// TABLE_TAB or VIEW_TAB delta (the two families share the leading
    /// `(schema_id, name)` payload prefix).
    pub(crate) fn apply_entity_caches(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;

            if weight > 0 {
                let sid = batch.read_payload_u64(i, TABTAB_PAY_SCHEMA_ID) as i64;
                let name = batch.read_payload_string(i, TABTAB_PAY_NAME);
                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let qualified = format!("{schema_name}.{name}");
                self.caches.entity_by_qname.insert(qualified, tid);
                self.caches.entity_by_id.insert(tid, (schema_name, name));
            } else {
                // Retract sequence, in this order: read the old name
                // from entity_by_id → remove the qname → clear the per-table
                // column caches → remove the id entry.
                if let Some((sn, en)) = self.caches.entity_by_id.get(&tid) {
                    let qualified = format!("{sn}.{en}");
                    self.caches.entity_by_qname.remove(&qualified);
                }
                // Entity dropped: clear per-table cache entries without bumping
                // the schema version (there is no new schema to advertise).
                // `clear_col_cache_no_bump` is the only column-cache cleanup on
                // the rollback path (`ctx.in_rollback()`) where
                // `cascade_retract_columns` is skipped.
                // The schema_version counter is NOT removed here: the column
                // cascade fires AFTER this applier (it runs before
                // hook_relation_register) and would `or_insert` it straight back.
                // It is purged post-cascade by `purge_schema_version` at the
                // tail of the drop hook.
                self.caches.clear_col_cache_no_bump(tid);
                self.caches.entity_by_id.remove(&tid);
            }
        }
    }

    /// Maintain `members_by_schema` from a TABLE_TAB or VIEW_TAB delta. The set
    /// drops once it empties, so `schema_member_count` returns 0 exactly when
    /// no member remains.
    pub(crate) fn apply_schema_members(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = batch.read_payload_u64(i, TABTAB_PAY_SCHEMA_ID) as i64;

            if weight > 0 {
                self.caches.members_by_schema.entry(sid).or_default().insert(tid);
            } else if let Entry::Occupied(mut e) = self.caches.members_by_schema.entry(sid) {
                e.get_mut().remove(&tid);
                if e.get().is_empty() {
                    e.remove();
                }
            }
        }
    }

    /// Drop the cached column defs of every owner the COL_TAB delta touches.
    /// A batch typically carries one owner's columns in a run (the PK is
    /// `pack_column_id(owner, col)`), so skipping a repeat of the previous owner
    /// collapses the run to one invalidation. Correct for any row order — an
    /// interleaved batch just invalidates an owner more than once.
    pub(crate) fn apply_col_names_invalidate(&mut self, batch: &Batch) {
        let mut last: Option<i64> = None;
        for i in 0..batch.count {
            let owner_id = batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64;
            if last != Some(owner_id) {
                self.caches.invalidate_col_names(owner_id);
                last = Some(owner_id);
            }
        }
    }

    /// Maintain `index_by_name` and `indices_by_owner` from one pass over an
    /// IDX_TAB delta — both key off the same row and share their lifecycle.
    pub(crate) fn apply_index_caches(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64;
            let name = batch.read_payload_string(i, IDXTAB_PAY_NAME);

            if weight > 0 {
                self.caches.index_by_name.insert(name, idx_id);
                // Idempotent under a re-applied `+1`, like its two siblings: the
                // SAL dedupe filter under-dedupes by design and `remove_where`
                // drops only the first match, so a duplicate would be permanent.
                let owned = self.caches.indices_by_owner.entry(owner_id).or_default();
                if !owned.contains(&idx_id) {
                    owned.push(idx_id);
                }
            } else {
                self.caches.index_by_name.remove(&name);
                remove_where(&mut self.caches.indices_by_owner, owner_id, |&id| id == idx_id);
            }
        }
    }

    /// Maintain `fk_by_child` and `fk_by_parent` from a single pass over a
    /// COL_TAB delta — both hold the same [`FkEdge`], indexed from either end,
    /// and at boot replay the batch is the full sys_columns scan, so decoding
    /// it once matters.
    ///
    /// Only base-table rows carry a constraint (`coltab_row_declares_fk`).
    pub(crate) fn apply_fk_constraints(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            if !coltab_row_declares_fk(batch, i) {
                continue;
            }
            let edge = FkEdge {
                child_tid: batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64,
                fk_col: batch.read_payload_u64(i, COLTAB_PAY_COL_IDX) as usize,
                parent_tid: batch.read_payload_u64(i, COLTAB_PAY_FK_TABLE_ID) as i64,
                parent_col: batch.read_payload_u64(i, COLTAB_PAY_FK_COL_IDX) as usize,
            };
            // One edge per (child, child column), whichever end it is indexed by.
            let same = |e: &FkEdge| e.child_tid == edge.child_tid && e.fk_col == edge.fk_col;

            if batch.get_weight(i) > 0 {
                let by_child = self.caches.fk_by_child.entry(edge.child_tid).or_default();
                if !by_child.iter().any(same) {
                    by_child.push(edge);
                }
                let by_parent = self.caches.fk_by_parent.entry(edge.parent_tid).or_default();
                if !by_parent.iter().any(same) {
                    by_parent.push(edge);
                }
            } else {
                remove_where(&mut self.caches.fk_by_child, edge.child_tid, same);
                remove_where(&mut self.caches.fk_by_parent, edge.parent_tid, same);
            }
        }
    }

    /// A TABLE_TAB delta relocks every relation it names: the lock rule reads
    /// `the registry[tid].kind`, which the register/teardown hooks just changed.
    pub(crate) fn relock_from_table_delta(&mut self, batch: &Batch) {
        let mut tids = Vec::with_capacity(batch.count);
        for i in 0..batch.count {
            tids.push(batch.get_pk(i) as i64);
        }
        self.relock_all(tids);
    }

    /// An FK-carrying COL_TAB delta relocks both ends of each edge it declares:
    /// the lock set of either end names the other.
    pub(crate) fn relock_from_column_delta(&mut self, batch: &Batch) {
        let mut tids = Vec::with_capacity(batch.count * 2);
        for i in 0..batch.count {
            if !coltab_row_declares_fk(batch, i) {
                continue;
            }
            let fk_table_id = batch.read_payload_u64(i, COLTAB_PAY_FK_TABLE_ID) as i64;
            tids.push(batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64);
            if self.registry.has_id(fk_table_id) {
                tids.push(fk_table_id);
            }
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
    pub(crate) fn recompute_needs_lock(&mut self, tid: i64) {
        let fk_child_count = self.caches.fk_by_child.get(&tid).map_or(0, |v| v.len());
        let fk_parent_count = self.caches.fk_by_parent.get(&tid).map_or(0, |v| v.len());
        // Every base table needs the lock: its writes run `enforce_unique_pk`
        // against the store, and only a base table can own a unique secondary
        // index. Views and system tables need it only as an FK endpoint.
        let is_base_table = self
            .registry
            .table_entry(tid)
            .ok()
            .is_some_and(|e| e.kind.is_base_table());

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
