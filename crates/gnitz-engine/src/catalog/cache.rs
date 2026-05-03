use super::*;
use rustc_hash::{FxHashMap, FxHashSet};

// ---------------------------------------------------------------------------
// CatalogCacheSet — all typed caches for one CatalogEngine
// ---------------------------------------------------------------------------

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
        CatalogCacheSet {
            schema_by_name:    FxHashMap::default(),
            schema_by_id:      FxHashMap::default(),
            entity_by_qname:   FxHashMap::default(),
            entity_by_id:      FxHashMap::default(),
            schema_of:         FxHashMap::default(),
            tables_by_schema:  FxHashMap::default(),
            views_by_schema:   FxHashMap::default(),
            pk_col_of:         FxHashMap::default(),
            col_names:         FxHashMap::default(),
            col_names_bytes:   FxHashMap::default(),
            schema_wire_block: FxHashMap::default(),
            index_by_name:     FxHashMap::default(),
            index_by_id:       FxHashMap::default(),
            indices_by_owner:  FxHashMap::default(),
            fk_by_child:       FxHashMap::default(),
            fk_by_parent:      FxHashMap::default(),
            needs_lock:        FxHashSet::default(),
            cascade_enabled:   false,
        }
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
            let name = self.read_batch_string(batch, i, 0);
            if weight > 0 {
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
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);
            if weight > 0 {
                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let qualified = format!("{}.{}", schema_name, name);
                self.caches.entity_by_qname.insert(qualified, tid);
            } else {
                // entity_by_id still present at this point (ENTITY_BY_QNAME fires before ENTITY_BY_ID)
                if let Some((sn, en)) = self.caches.entity_by_id.get(&tid).cloned() {
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
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);
            if weight > 0 {
                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                self.caches.entity_by_id.insert(tid, (schema_name, name));
            } else {
                self.caches.invalidate_col_names(tid);
                self.caches.entity_by_id.remove(&tid);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_schema_of(&mut self, source_tid: i64, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            if weight > 0 {
                self.caches.schema_of.insert(tid, sid);
                if source_tid == TABLE_TAB_ID {
                    self.caches.tables_by_schema.entry(sid).or_default().insert(tid);
                } else {
                    self.caches.views_by_schema.entry(sid).or_default().insert(tid);
                }
            } else {
                self.caches.schema_of.remove(&tid);
                if source_tid == TABLE_TAB_ID {
                    if let Some(set) = self.caches.tables_by_schema.get_mut(&sid) {
                        set.remove(&tid);
                    }
                } else if let Some(set) = self.caches.views_by_schema.get_mut(&sid) {
                    set.remove(&tid);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_pk_col_of(&mut self, source_tid: i64, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            if weight > 0 {
                let pk_col = if source_tid == TABLE_TAB_ID {
                    self.read_batch_u64(batch, i, 3) as u32 // TABLETAB_COL_PK_COL_IDX payload index
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
            let name = self.read_batch_string(batch, i, 3); // IDXTAB_COL_NAME payload index
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
            let name = self.read_batch_string(batch, i, 3);          // IDXTAB_COL_NAME
            if weight > 0 {
                self.caches.index_by_id.insert(idx_id, name);
                self.caches.indices_by_owner.entry(owner_id).or_default().push(idx_id);
            } else {
                self.caches.index_by_id.remove(&idx_id);
                if let Some(ids) = self.caches.indices_by_owner.get_mut(&owner_id) {
                    ids.retain(|&id| id != idx_id);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_fk_by_child(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let col_idx = self.read_batch_u64(batch, i, 2) as usize;
            let fk_table_id = self.read_batch_u64(batch, i, 6) as i64;
            if fk_table_id == 0 { continue; }
            if weight > 0 {
                let constraints = self.caches.fk_by_child.entry(owner_id).or_default();
                if !constraints.iter().any(|c| c.fk_col_idx == col_idx) {
                    constraints.push(FkConstraint { fk_col_idx: col_idx, target_table_id: fk_table_id });
                }
            } else if let Some(constraints) = self.caches.fk_by_child.get_mut(&owner_id) {
                constraints.retain(|c| c.fk_col_idx != col_idx);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_fk_by_parent(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let col_idx = self.read_batch_u64(batch, i, 2) as usize;
            let fk_table_id = self.read_batch_u64(batch, i, 6) as i64;
            if fk_table_id == 0 { continue; }
            if weight > 0 {
                let parents = self.caches.fk_by_parent.entry(fk_table_id).or_default();
                if !parents.iter().any(|&(t, c)| t == owner_id && c == col_idx) {
                    parents.push((owner_id, col_idx));
                }
            } else if let Some(parents) = self.caches.fk_by_parent.get_mut(&fk_table_id) {
                parents.retain(|&(t, c)| !(t == owner_id && c == col_idx));
            }
        }
        Ok(())
    }

    pub(crate) fn apply_needs_lock(&mut self, source_tid: i64, batch: &Batch) -> Result<(), String> {
        match source_tid {
            TABLE_TAB_ID => {
                for i in 0..batch.count {
                    let tid = batch.get_pk(i) as i64;
                    self.recompute_needs_lock(tid);
                }
            }
            IDX_TAB_ID => {
                for i in 0..batch.count {
                    let owner_id = self.read_batch_u64(batch, i, 0) as i64;
                    self.recompute_needs_lock(owner_id);
                }
            }
            COL_TAB_ID => {
                for i in 0..batch.count {
                    let owner_id = self.read_batch_u64(batch, i, 0) as i64;
                    let fk_table_id = self.read_batch_u64(batch, i, 6) as i64;
                    if fk_table_id != 0 {
                        self.recompute_needs_lock(owner_id);
                        if self.dag.tables.contains_key(&fk_table_id) {
                            self.recompute_needs_lock(fk_table_id);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Recompute and store needs_lock for `tid` from current cache + dag state.
    pub(crate) fn recompute_needs_lock(&mut self, tid: i64) {
        let fk_child_count = self.caches.fk_by_child.get(&tid).map(|v| v.len()).unwrap_or(0);
        let fk_parent_count = self.caches.fk_by_parent.get(&tid).map(|v| v.len()).unwrap_or(0);
        let (has_unique_index, unique_pk) = self.dag.tables.get(&tid)
            .map(|e| (e.index_circuits.iter().any(|ic| ic.is_unique), e.unique_pk))
            .unwrap_or((false, false));
        let needs = fk_child_count > 0 || fk_parent_count > 0 || has_unique_index || unique_pk;
        if needs {
            self.caches.needs_lock.insert(tid);
        } else {
            self.caches.needs_lock.remove(&tid);
        }
    }
}
