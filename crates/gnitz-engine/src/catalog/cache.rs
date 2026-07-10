use super::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

/// Cached schema wire data for one table: the encoded block plus the derived
/// wire properties reused across SEEK/SCAN responses. All fields share one
/// invalidation lifecycle (`clear_col_cache_no_bump` drops the entry whole).
pub(crate) struct SchemaWireEntry {
    pub(crate) block: Rc<Vec<u8>>,
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
    pub(crate) schema_of: FxHashMap<i64, i64>,
    pub(crate) tables_by_schema: FxHashMap<i64, FxHashSet<i64>>,
    pub(crate) views_by_schema: FxHashMap<i64, FxHashSet<i64>>,
    pub(crate) pk_col_of: FxHashMap<i64, PkColList>,
    /// Full decoded column definitions per table — the one COL_TAB read the
    /// three views below (`col_names`, `col_names_bytes`, `col_hidden`) are
    /// derived from at fill time (`fill_column_caches`).
    pub(crate) col_defs: FxHashMap<i64, Rc<Vec<ColumnDef>>>,
    pub(crate) col_names: FxHashMap<i64, Rc<Vec<String>>>,
    pub(crate) col_names_bytes: FxHashMap<i64, Rc<Vec<Vec<u8>>>>,
    /// Per-table hidden-column bitmask (bit N ⇔ column N is a hidden key slot;
    /// u128 covers MAX_COLUMNS = 65). Echoed into reply schema blocks as
    /// `META_FLAG_HIDDEN`; filled from COL_TAB alongside `col_names`.
    pub(crate) col_hidden: FxHashMap<i64, u128>,
    /// Cached schema wire data per table. Built from (SchemaDescriptor,
    /// col_names) and reused across SEEK/SCAN responses. Invalidated alongside
    /// col_names when DDL modifies the table schema.
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
    pub(crate) fk_by_child: FxHashMap<i64, Vec<FkConstraint>>,
    pub(crate) fk_by_parent: FxHashMap<i64, Vec<FkParentRef>>,
    /// Tables whose writes need the push lock, each with its materialized lock
    /// set: the table itself plus all FK parents and children, sorted ascending
    /// and deduped for deadlock-free acquisition. Recomputed by
    /// `recompute_needs_lock` on every trigger (`apply_needs_lock` fires on
    /// both FK endpoints, IDX_TAB, and TABLE_TAB deltas), so `fk_lock_set` is
    /// a plain borrow on the push path.
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

impl CatalogCacheSet {
    /// Remove the derived column caches without bumping the schema version.
    /// Use for table drop (no new schema to advertise) or as the inner step of
    /// `invalidate_col_names`.
    pub(crate) fn clear_col_cache_no_bump(&mut self, id: i64) {
        self.col_defs.remove(&id);
        self.col_names.remove(&id);
        self.col_names_bytes.remove(&id);
        self.col_hidden.remove(&id);
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
    /// cascade, whose `invalidate_col_names` / `apply_index_by_id` bumps would
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
                // Retract sequence (order is load-bearing): read the old name
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

    pub(crate) fn apply_schema_of(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let is_table = family == SysFamily::Table;

        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = batch.read_payload_u64(i, TABTAB_PAY_SCHEMA_ID) as i64;
            let members = if is_table {
                &mut self.caches.tables_by_schema
            } else {
                &mut self.caches.views_by_schema
            };

            if weight > 0 {
                members.entry(sid).or_default().insert(tid);
                self.caches.schema_of.insert(tid, sid);
            } else {
                if let Entry::Occupied(mut e) = members.entry(sid) {
                    e.get_mut().remove(&tid);
                    if e.get().is_empty() {
                        e.remove();
                    }
                }
                self.caches.schema_of.remove(&tid);
            }
        }
        Ok(())
    }

    /// `pk_pay_idx` is the payload index of the family's packed-PK column
    /// (`TABTAB_PAY_PK_COL_IDX` / `VIEWTAB_PAY_PK_COL_IDX`).
    pub(crate) fn apply_pk_col_of(&mut self, pk_pay_idx: usize, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;

            if weight > 0 {
                // Malformed wire values (count 0 or 5..=15) are NOT defended
                // here: hook_table_register / hook_view_register fires
                // immediately after this applier in the same call, retracts the
                // row via the matching -1 weight, and removes the entry before
                // any other code can observe it. `PkColList::as_slice` is
                // panic-free regardless. A view's PK is its persisted leading-k
                // list (0..k for a compound-source projection, bare 0 otherwise).
                let pk_list = unpack_pk_cols(batch.read_payload_u64(i, pk_pay_idx));
                // Invalidate only on a real change: retract-reinsert with an
                // identical PK list would otherwise spuriously bump the version.
                let old = self.caches.pk_col_of.insert(tid, pk_list);
                if old != Some(pk_list) {
                    self.caches.invalidate_col_names(tid);
                }
            } else {
                self.caches.pk_col_of.remove(&tid);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_col_names_invalidate(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let owner_id = batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64;
            self.caches.invalidate_col_names(owner_id);
        }
        Ok(())
    }

    pub(crate) fn apply_index_by_name(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let name = batch.read_payload_string(i, IDXTAB_PAY_NAME);

            if weight > 0 {
                self.caches.index_by_name.insert(name, idx_id);
            } else {
                self.caches.index_by_name.remove(&name);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_index_by_id(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64;

            if weight > 0 {
                let name = batch.read_payload_string(i, IDXTAB_PAY_NAME);
                self.caches.index_by_id.insert(idx_id, name);
                self.caches.indices_by_owner.entry(owner_id).or_default().push(idx_id);
            } else {
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
    /// COL_TAB delta — both caches key off the same FK fields and share their
    /// lifecycle, and at boot replay the batch is the full sys_columns scan,
    /// so decoding it once matters.
    pub(crate) fn apply_fk_constraints(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let fk_table_id = batch.read_payload_u64(i, COLTAB_PAY_FK_TABLE_ID) as i64;
            if fk_table_id == 0 {
                continue;
            }

            let weight = batch.get_weight(i);
            let owner_id = batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64;
            let col_idx = batch.read_payload_u64(i, COLTAB_PAY_COL_IDX) as usize; // child col
            let target_col_idx = batch.read_payload_u64(i, COLTAB_PAY_FK_COL_IDX) as usize; // parent col

            if weight > 0 {
                let constraints = self.caches.fk_by_child.entry(owner_id).or_default();
                if !constraints.iter().any(|c| c.fk_col_idx == col_idx) {
                    constraints.push(FkConstraint {
                        fk_col_idx: col_idx,
                        target_table_id: fk_table_id,
                        target_col_idx,
                    });
                }
                let parents = self.caches.fk_by_parent.entry(fk_table_id).or_default();
                if !parents
                    .iter()
                    .any(|r| r.child_tid == owner_id && r.fk_col_idx == col_idx)
                {
                    parents.push(FkParentRef {
                        child_tid: owner_id,
                        fk_col_idx: col_idx,
                        parent_col_idx: target_col_idx,
                    });
                }
            } else {
                remove_where(&mut self.caches.fk_by_child, owner_id, |c| c.fk_col_idx == col_idx);
                remove_where(&mut self.caches.fk_by_parent, fk_table_id, |r| {
                    r.child_tid == owner_id && r.fk_col_idx == col_idx
                });
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
            SysFamily::Index => {
                to_recompute.reserve_exact(batch.count);
                for i in 0..batch.count {
                    to_recompute.push(batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64);
                }
            }
            SysFamily::Column => {
                to_recompute.reserve(batch.count * 2);
                for i in 0..batch.count {
                    let fk_table_id = batch.read_payload_u64(i, COLTAB_PAY_FK_TABLE_ID) as i64;
                    if fk_table_id != 0 {
                        to_recompute.push(batch.read_payload_u64(i, COLTAB_PAY_OWNER_ID) as i64);
                        if self.dag.tables.contains_key(&fk_table_id) {
                            to_recompute.push(fk_table_id);
                        }
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
        let (has_unique_index, unique_pk) = self.dag.tables.get(&tid).map_or((false, false), |e| {
            (e.index_circuits.iter().any(|ic| ic.is_unique), e.unique_pk())
        });

        let needs = fk_child_count > 0 || fk_parent_count > 0 || has_unique_index || unique_pk;
        if needs {
            let mut tids = vec![tid];
            if let Some(constraints) = self.caches.fk_by_child.get(&tid) {
                tids.extend(constraints.iter().map(|c| c.target_table_id));
            }
            if let Some(children) = self.caches.fk_by_parent.get(&tid) {
                tids.extend(children.iter().map(|r| r.child_tid));
            }
            tids.sort_unstable();
            tids.dedup();
            self.caches.needs_lock.insert(tid, tids);
        } else {
            self.caches.needs_lock.remove(&tid);
        }
    }
}
