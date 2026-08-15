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

/// Cached schema wire data for one table: the encoded block, the schema
/// version it was built at, and the derived wire properties reused across
/// SEEK/SCAN responses. All fields share one invalidation lifecycle
/// (`clear_col_cache_no_bump` drops the entry whole, and the version bump that
/// follows it means a surviving entry always carries its build-time version).
#[derive(Clone)]
pub(crate) struct SchemaWireEntry {
    pub(crate) block: Rc<Vec<u8>>,
    /// The owning table's schema version when the block was built.
    pub(crate) version: u16,
    /// True when every column has a fixed-width 8-aligned stride and no
    /// German-string (STRING or BLOB) columns. Drives the `scatter_wire_group`
    /// fast path.
    pub(crate) wire_safe: bool,
    /// Sum of pk_stride + 8 (weight) + 8 (null_bmp) + every payload column's
    /// stride. Only meaningful when `wire_safe`.
    pub(crate) wire_row_fixed_stride: u32,
}

#[derive(Default)]
pub(crate) struct CatalogCacheSet {
    pub(crate) schema_by_name: FxHashMap<String, i64>,
    pub(crate) schema_by_id: FxHashMap<i64, String>,
    pub(crate) entity_by_qname: FxHashMap<String, i64>,
    pub(crate) entity_by_id: FxHashMap<i64, (String, String)>,
    /// Live member relations (tables and views alike) per schema id. Production
    /// reads only the count — the non-empty-schema DROP guard — and the two
    /// kinds are told apart through `dag.tables[id].kind` where it matters.
    pub(crate) members_by_schema: FxHashMap<i64, FxHashSet<i64>>,
    /// Full decoded column definitions per table (`fill_column_caches`). The
    /// one COL_TAB read every name / hidden-flag consumer goes through.
    pub(crate) col_defs: FxHashMap<i64, Rc<Vec<ColumnDef>>>,
    /// Cached schema wire data per table. Built from (SchemaDescriptor,
    /// col_defs) and reused across SEEK/SCAN responses. Invalidated alongside
    /// col_defs when DDL modifies the table schema.
    pub(crate) schema_wire_cache: FxHashMap<i64, SchemaWireEntry>,
    /// Monotonically increasing schema version per table (wraps 65535→1, never 0).
    /// Absent entries implicitly resolve to version 1 (base version).
    /// Version 0 is reserved as "client has no cached schema".
    pub(crate) schema_version: FxHashMap<i64, u16>,
    /// Per-table index-metadata version (wraps 255 → 1, never 0).
    /// Absent ⇒ 1 (base). 0 is the client sentinel "no cached index list".
    /// `u8` because it travels in 8 free wire bits (bits 40-47); a wider
    /// counter truncated to 8 bits would both alias distinct values and emit
    /// the reserved `0` sentinel on overflow.
    pub(crate) index_version: FxHashMap<i64, u8>,
    pub(crate) index_by_name: FxHashMap<String, i64>,
    pub(crate) index_by_id: FxHashMap<i64, String>,
    pub(crate) indices_by_owner: FxHashMap<i64, Vec<i64>>,
    pub(crate) fk_by_child: FxHashMap<i64, Vec<FkEdge>>,
    pub(crate) fk_by_parent: FxHashMap<i64, Vec<FkEdge>>,
    /// Tables whose writes need the push lock, each with its materialized lock
    /// set: the table itself plus all FK parents and children, sorted ascending
    /// and deduped for deadlock-free acquisition. Recomputed by
    /// `recompute_needs_lock` on every trigger (`apply_needs_lock` fires on
    /// TABLE_TAB and FK-carrying COL_TAB deltas), so `fk_lock_set` is a plain
    /// borrow on the push path.
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
        // Absent entries implicitly resolve to version 1 (the base sentinel);
        // first invalidation produces 2, so a client holding version 1 always
        // sees a mismatch. Version 0 is reserved for "client has no cached
        // schema". The bump wraps 65535 → 1, never 0.
        let v = self.schema_version.entry(id).or_insert(1);
        *v = if *v == u16::MAX { 1 } else { *v + 1 };
    }

    /// Return the current schema version for `id`. Absent = version 1.
    pub(crate) fn get_schema_version(&self, id: i64) -> u16 {
        self.schema_version.get(&id).copied().unwrap_or(1)
    }

    /// Return the current index-metadata version for `id`. Absent = version 1.
    pub(crate) fn get_index_version(&self, id: i64) -> u8 {
        self.index_version.get(&id).copied().unwrap_or(1)
    }

    /// Drop both per-table version counters when a table/view is fully removed.
    /// Call this at the tail of the drop hook — *after* the column / index
    /// cascade, whose `invalidate_col_names` / `apply_index_caches` bumps would
    /// otherwise `or_insert` the counters straight back. Table ids are
    /// monotonic and never reused, so a counter left behind here would become
    /// permanent dead memory.
    pub(crate) fn purge_table_versions(&mut self, id: i64) {
        self.schema_version.remove(&id);
        self.index_version.remove(&id);
    }
}

// ---------------------------------------------------------------------------
// Cache delta appliers on CatalogEngine
// ---------------------------------------------------------------------------

impl CatalogEngine {
    /// Maintain `schema_by_name` and `schema_by_id` from one pass over a
    /// SCHEMA_TAB delta — the two caches share their lifecycle and key data.
    pub(crate) fn apply_schema_caches(&mut self, batch: &Batch) -> Result<(), String> {
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
                // has no hook_schema_register, so this applier is its re-derive
                // site — the role hook_{table,index}_register play for their ids.
                raise_id_counter(&mut self.next_schema_id, sid);
            } else {
                self.caches.schema_by_name.remove(&name);
                self.caches.schema_by_id.remove(&sid);
            }
        }
        Ok(())
    }

    /// Maintain `entity_by_qname` and `entity_by_id` from one pass over a
    /// TABLE_TAB or VIEW_TAB delta (the two families share the leading
    /// `(schema_id, name)` payload prefix).
    pub(crate) fn apply_entity_caches(&mut self, batch: &Batch) -> Result<(), String> {
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
                // The schema_version / index_version counters are NOT removed
                // here: the column/index cascade fires AFTER this applier
                // (it runs before hook_table_register) and would `or_insert`
                // them straight back. They are purged post-cascade by
                // `purge_table_versions` at the tail of the drop hook.
                self.caches.clear_col_cache_no_bump(tid);
                self.caches.entity_by_id.remove(&tid);
            }
        }
        Ok(())
    }

    /// Maintain `members_by_schema` from a TABLE_TAB or VIEW_TAB delta. The set
    /// drops once it empties, so `schema_member_count` returns 0 exactly when
    /// no member remains.
    pub(crate) fn apply_schema_members(&mut self, batch: &Batch) -> Result<(), String> {
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
        Ok(())
    }

    /// Drop the cached column defs of every owner the COL_TAB delta touches.
    /// A batch typically carries one owner's columns in a run (the PK is
    /// `pack_column_id(owner, col)`), so skipping a repeat of the previous owner
    /// collapses the run to one invalidation. Correct for any row order — an
    /// interleaved batch just invalidates an owner more than once.
    pub(crate) fn apply_col_names_invalidate(&mut self, batch: &Batch) -> Result<(), String> {
        let mut last: Option<i64> = None;
        for i in 0..batch.count {
            let owner_id = batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64;
            if last != Some(owner_id) {
                self.caches.invalidate_col_names(owner_id);
                last = Some(owner_id);
            }
        }
        Ok(())
    }

    /// Maintain `index_by_name`, `index_by_id`, `indices_by_owner` and the
    /// per-owner index version from one pass over an IDX_TAB delta — all four
    /// key off the same row and share their lifecycle.
    pub(crate) fn apply_index_caches(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64;
            let name = batch.read_payload_string(i, IDXTAB_PAY_NAME);

            if weight > 0 {
                self.caches.index_by_name.insert(name.clone(), idx_id);
                self.caches.index_by_id.insert(idx_id, name);
                self.caches.indices_by_owner.entry(owner_id).or_default().push(idx_id);
            } else {
                self.caches.index_by_name.remove(&name);
                self.caches.index_by_id.remove(&idx_id);
                remove_where(&mut self.caches.indices_by_owner, owner_id, |&id| id == idx_id);
            }
            // Bump unconditionally so DROP-then-recreate on the same column
            // changes the epoch even when the net (col_idx, is_unique) set looks
            // identical. Per-row (not per-batch) matches schema_version, which
            // also bumps per row in apply_col_names_invalidate. Over-bumping
            // within one DDL only ever costs a client a spurious re-fetch — it
            // is never incorrect, because the epoch is always validated against
            // a fresh fetch. The post-cascade purge (purge_table_versions)
            // removes the entry an owner drop re-creates here. The bump wraps
            // 255 → 1, never 0 (the client sentinel "no cached index list").
            let v = self.caches.index_version.entry(owner_id).or_insert(1);
            *v = if *v == u8::MAX { 1 } else { *v + 1 };
        }
        Ok(())
    }

    /// Maintain `fk_by_child` and `fk_by_parent` from a single pass over a
    /// COL_TAB delta — both hold the same [`FkEdge`], indexed from either end,
    /// and at boot replay the batch is the full sys_columns scan, so decoding
    /// it once matters.
    ///
    /// Only base-table rows carry a constraint (`coltab_row_declares_fk`).
    pub(crate) fn apply_fk_constraints(&mut self, batch: &Batch) -> Result<(), String> {
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
        Ok(())
    }

    pub(crate) fn apply_needs_lock(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        if batch.count == 0 {
            return Ok(());
        }

        let mut to_recompute = Vec::new();

        match family {
            SysFamily::Table => {
                to_recompute.reserve_exact(batch.count);
                for i in 0..batch.count {
                    to_recompute.push(batch.get_pk(i) as i64);
                }
            }
            SysFamily::Column => {
                to_recompute.reserve(batch.count * 2);
                for i in 0..batch.count {
                    if !coltab_row_declares_fk(batch, i) {
                        continue;
                    }
                    let fk_table_id = batch.read_payload_u64(i, COLTAB_PAY_FK_TABLE_ID) as i64;
                    to_recompute.push(batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64);
                    if self.dag.tables.contains_key(&fk_table_id) {
                        to_recompute.push(fk_table_id);
                    }
                }
            }
            _ => return Ok(()),
        }

        if !to_recompute.is_empty() {
            // Deduplication guarantees expensive cache lookups run exactly once per ID
            to_recompute.sort_unstable();
            to_recompute.dedup();

            for tid in to_recompute {
                self.recompute_needs_lock(tid);
            }
        }

        Ok(())
    }

    /// Recompute needs_lock for `tid` from current cache + dag state, and
    /// materialize its sorted lock set (see `CatalogCacheSet::needs_lock`).
    pub(crate) fn recompute_needs_lock(&mut self, tid: i64) {
        let fk_child_count = self.caches.fk_by_child.get(&tid).map_or(0, |v| v.len());
        let fk_parent_count = self.caches.fk_by_parent.get(&tid).map_or(0, |v| v.len());
        // Every base table needs the lock: its writes run `enforce_unique_pk`
        // against the store, and only a base table can own a unique secondary
        // index. Views and system tables need it only as an FK endpoint.
        let is_base_table = self.dag.tables.get(&tid).is_some_and(|e| e.kind.is_base_table());

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
