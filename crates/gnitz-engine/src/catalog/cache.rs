use super::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct CatalogCacheSet {
    pub(crate) schema_by_name:   FxHashMap<String, i64>,
    pub(crate) schema_by_id:     FxHashMap<i64, String>,
    pub(crate) entity_by_qname:  FxHashMap<String, i64>,
    pub(crate) entity_by_id:     FxHashMap<i64, (String, String)>,
    pub(crate) schema_of:        FxHashMap<i64, i64>,
    pub(crate) tables_by_schema: FxHashMap<i64, FxHashSet<i64>>,
    pub(crate) views_by_schema:  FxHashMap<i64, FxHashSet<i64>>,
    pub(crate) pk_col_of:        FxHashMap<i64, u32>,
    pub(crate) col_names:        FxHashMap<i64, Vec<String>>,
    pub(crate) col_names_bytes:  FxHashMap<i64, Rc<Vec<Vec<u8>>>>,
    /// Cached encoded schema wire block per table. Built from
    /// (SchemaDescriptor, col_names) and reused across SEEK/SCAN responses.
    /// Invalidated alongside col_names when DDL modifies the table schema.
    pub(crate) schema_wire_block: FxHashMap<i64, Rc<Vec<u8>>>,
    pub(crate) index_by_name:    FxHashMap<String, i64>,
    pub(crate) index_by_id:      FxHashMap<i64, String>,
    pub(crate) indices_by_owner: FxHashMap<i64, Vec<i64>>,
    pub(crate) fk_by_child:      FxHashMap<i64, Vec<FkConstraint>>,
    pub(crate) fk_by_parent:     FxHashMap<i64, Vec<(i64, usize)>>,
    pub(crate) needs_lock:       FxHashSet<i64>,
    pub(crate) cascade_enabled:  bool,
}

impl CatalogCacheSet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn invalidate_col_names(&mut self, id: i64) {
        self.col_names.remove(&id);
        self.col_names_bytes.remove(&id);
        self.schema_wire_block.remove(&id);
    }
}

// ---------------------------------------------------------------------------
// Cache delta appliers on CatalogEngine
// ---------------------------------------------------------------------------

impl CatalogEngine {
    pub(crate) fn apply_schema_by_name(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let sid = batch.get_pk(i) as i64;
            let name = self.read_batch_string(batch, i, 0);
            
            if weight > 0 {
                self.caches.schema_by_name.insert(name, sid);
            } else {
                self.caches.schema_by_name.remove(&name);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_schema_by_id(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let sid = batch.get_pk(i) as i64;
            
            if weight > 0 {
                let name = self.read_batch_string(batch, i, 0);
                self.caches.schema_by_id.insert(sid, name);
            } else {
                self.caches.schema_by_id.remove(&sid);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_entity_by_qname(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            
            if weight > 0 {
                let sid = self.read_batch_u64(batch, i, 0) as i64;
                let name = self.read_batch_string(batch, i, 1);
                let schema_name = self.caches.schema_by_id.get(&sid).map_or("", String::as_str);
                let qualified = format!("{}.{}", schema_name, name);
                self.caches.entity_by_qname.insert(qualified, tid);
            } else {
                // entity_by_id is still present (ENTITY_BY_QNAME fires before ENTITY_BY_ID)
                if let Some((sn, en)) = self.caches.entity_by_id.get(&tid) {
                    let qualified = format!("{}.{}", sn, en);
                    self.caches.entity_by_qname.remove(&qualified);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_entity_by_id(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            
            if weight > 0 {
                let sid = self.read_batch_u64(batch, i, 0) as i64;
                let name = self.read_batch_string(batch, i, 1);
                let schema_name = self.caches.schema_by_id.get(&sid).map_or_else(String::new, String::clone);
                self.caches.entity_by_id.insert(tid, (schema_name, name));
            } else {
                self.caches.invalidate_col_names(tid);
                self.caches.entity_by_id.remove(&tid);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_schema_of(&mut self, source_tid: i64, batch: &Batch) -> Result<(), String> {
        let is_table = source_tid == TABLE_TAB_ID;

        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            
            if weight > 0 {
                self.caches.schema_of.insert(tid, sid);
                if is_table {
                    self.caches.tables_by_schema.entry(sid).or_default().insert(tid);
                } else {
                    self.caches.views_by_schema.entry(sid).or_default().insert(tid);
                }
            } else {
                self.caches.schema_of.remove(&tid);
                if is_table {
                    if let Entry::Occupied(mut e) = self.caches.tables_by_schema.entry(sid) {
                        e.get_mut().remove(&tid);
                        if e.get().is_empty() { e.remove(); }
                    }
                } else if let Entry::Occupied(mut e) = self.caches.views_by_schema.entry(sid) {
                    e.get_mut().remove(&tid);
                    if e.get().is_empty() { e.remove(); }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_pk_col_of(&mut self, source_tid: i64, batch: &Batch) -> Result<(), String> {
        let is_table = source_tid == TABLE_TAB_ID;

        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            
            if weight > 0 {
                let pk_col = if is_table {
                    self.read_batch_u64(batch, i, 3) as u32 // TABLETAB_COL_PK_COL_IDX
                } else {
                    0u32 // views always pk_col=0
                };
                self.caches.pk_col_of.insert(tid, pk_col);
            } else {
                self.caches.pk_col_of.remove(&tid);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_col_names_invalidate(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            self.caches.invalidate_col_names(owner_id);
        }
        Ok(())
    }

    pub(crate) fn apply_index_by_name(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let name = self.read_batch_string(batch, i, 3); // IDXTAB_COL_NAME
            
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
            let owner_id = self.read_batch_u64(batch, i, 0) as i64; // IDXTAB_COL_OWNER_ID
            
            if weight > 0 {
                let name = self.read_batch_string(batch, i, 3);
                self.caches.index_by_id.insert(idx_id, name);
                self.caches.indices_by_owner.entry(owner_id).or_default().push(idx_id);
            } else {
                self.caches.index_by_id.remove(&idx_id);
                if let Entry::Occupied(mut e) = self.caches.indices_by_owner.entry(owner_id) {
                    let ids = e.get_mut();
                    if let Some(pos) = ids.iter().position(|&id| id == idx_id) {
                        ids.swap_remove(pos);
                    }
                    if e.get().is_empty() { e.remove(); }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_fk_by_child(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let fk_table_id = self.read_batch_u64(batch, i, 6) as i64;
            if fk_table_id == 0 { continue; }

            let weight = batch.get_weight(i);
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let col_idx = self.read_batch_u64(batch, i, 2) as usize;

            if weight > 0 {
                let constraints = self.caches.fk_by_child.entry(owner_id).or_default();
                if !constraints.iter().any(|c| c.fk_col_idx == col_idx) {
                    constraints.push(FkConstraint { fk_col_idx: col_idx, target_table_id: fk_table_id });
                }
            } else if let Entry::Occupied(mut e) = self.caches.fk_by_child.entry(owner_id) {
                let constraints = e.get_mut();
                if let Some(pos) = constraints.iter().position(|c| c.fk_col_idx == col_idx) {
                    constraints.swap_remove(pos);
                }
                if e.get().is_empty() { e.remove(); }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_fk_by_parent(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let fk_table_id = self.read_batch_u64(batch, i, 6) as i64;
            if fk_table_id == 0 { continue; }

            let weight = batch.get_weight(i);
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let col_idx = self.read_batch_u64(batch, i, 2) as usize;
            
            if weight > 0 {
                let parents = self.caches.fk_by_parent.entry(fk_table_id).or_default();
                if !parents.iter().any(|&(t, c)| t == owner_id && c == col_idx) {
                    parents.push((owner_id, col_idx));
                }
            } else if let Entry::Occupied(mut e) = self.caches.fk_by_parent.entry(fk_table_id) {
                let parents = e.get_mut();
                if let Some(pos) = parents.iter().position(|&(t, c)| t == owner_id && c == col_idx) {
                    parents.swap_remove(pos);
                }
                if e.get().is_empty() { e.remove(); }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_needs_lock(&mut self, source_tid: i64, batch: &Batch) -> Result<(), String> {
        if batch.count == 0 { return Ok(()); }
        
        let mut to_recompute = Vec::new();

        match source_tid {
            TABLE_TAB_ID => {
                to_recompute.reserve_exact(batch.count);
                for i in 0..batch.count {
                    to_recompute.push(batch.get_pk(i) as i64);
                }
            }
            IDX_TAB_ID => {
                to_recompute.reserve_exact(batch.count);
                for i in 0..batch.count {
                    to_recompute.push(self.read_batch_u64(batch, i, 0) as i64);
                }
            }
            COL_TAB_ID => {
                to_recompute.reserve(batch.count * 2);
                for i in 0..batch.count {
                    let fk_table_id = self.read_batch_u64(batch, i, 6) as i64;
                    if fk_table_id != 0 {
                        to_recompute.push(self.read_batch_u64(batch, i, 0) as i64);
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

    /// Recompute and store needs_lock for `tid` from current cache + dag state.
    pub(crate) fn recompute_needs_lock(&mut self, tid: i64) {
        let fk_child_count = self.caches.fk_by_child.get(&tid).map_or(0, |v| v.len());
        let fk_parent_count = self.caches.fk_by_parent.get(&tid).map_or(0, |v| v.len());
        let (has_unique_index, unique_pk) = self.dag.tables.get(&tid)
            .map_or((false, false), |e| (e.index_circuits.iter().any(|ic| ic.is_unique), e.unique_pk));
        
        let needs = fk_child_count > 0 || fk_parent_count > 0 || has_unique_index || unique_pk;
        if needs {
            self.caches.needs_lock.insert(tid);
        } else {
            self.caches.needs_lock.remove(&tid);
        }
    }
}
