use super::*;

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------

    pub(crate) fn fire_hooks(&mut self, sys_table_id: i64, batch: &Batch) -> Result<(), String> {
        if sys_table_id == CATALOG_CACHES_TAB_ID {
            return self.on_catalog_caches_delta(batch);
        }
        let effects = self.caches.source_index.get(&sys_table_id).cloned().unwrap_or_default();
        for (effect_id, kind) in effects {
            match kind {
                CCTAB_KIND_DATA => self.apply_cache_delta(effect_id, sys_table_id, batch)?,
                CCTAB_KIND_HOOK => self.apply_hook(effect_id, batch)?,
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn on_catalog_caches_delta(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let pk = batch.get_pk(i);
            let effect_id = pk as u64;
            let source_tid = (pk >> 64) as i64;
            let kind = self.read_batch_u64(batch, i, 1); // payload[1] = kind
            let effects = self.caches.source_index.entry(source_tid).or_default();
            if weight > 0 {
                if !effects.iter().any(|&(id, _)| id == effect_id) {
                    effects.push((effect_id, kind));
                }
            } else {
                effects.retain(|&(id, _)| id != effect_id);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_cache_delta(&mut self, effect_id: u64, source_tid: i64, batch: &Batch) -> Result<(), String> {
        match effect_id {
            CACHE_ID_SCHEMA_BY_NAME          => self.apply_schema_by_name(batch),
            CACHE_ID_SCHEMA_BY_ID            => self.apply_schema_by_id(batch),
            CACHE_ID_ENTITY_BY_QNAME         => self.apply_entity_by_qname(batch),
            CACHE_ID_ENTITY_BY_ID            => self.apply_entity_by_id(batch),
            CACHE_ID_SCHEMA_OF               => self.apply_schema_of(batch),
            CACHE_ID_PK_COL_OF               => self.apply_pk_col_of(source_tid, batch),
            CACHE_ID_COL_NAMES | CACHE_ID_COL_NAMES_BYTES => self.apply_col_names_invalidate(batch),
            CACHE_ID_INDEX_BY_NAME           => self.apply_index_by_name(batch),
            CACHE_ID_INDEX_BY_ID             => self.apply_index_by_id(batch),
            CACHE_ID_FK_BY_CHILD             => self.apply_fk_by_child(batch),
            CACHE_ID_FK_BY_PARENT            => self.apply_fk_by_parent(batch),
            CACHE_ID_NEEDS_LOCK              => self.apply_needs_lock(source_tid, batch),
            _                                => Ok(()),
        }
    }

    pub(crate) fn apply_hook(&mut self, effect_id: u64, batch: &Batch) -> Result<(), String> {
        match effect_id {
            HOOK_ID_SCHEMA_DIR     => self.hook_schema_dir(batch),
            HOOK_ID_TABLE_REGISTER => self.hook_table_register(batch),
            HOOK_ID_VIEW_REGISTER  => self.hook_view_register(batch),
            HOOK_ID_INDEX_REGISTER => self.hook_index_register(batch),
            HOOK_ID_CASCADE_FK     => self.hook_cascade_fk(batch),
            HOOK_ID_DEP_INVALIDATE => { self.dag.invalidate_dep_map(); Ok(()) },
            _                      => Ok(()),
        }
    }

    // -- Hook handlers ---------------------------------------------------------

    fn hook_schema_dir(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            if weight > 0 {
                let name = self.read_batch_string(batch, i, 0);
                let path = format!("{}/{}", self.base_dir, name);
                ensure_dir(&path)?;
                fsync_dir(&self.base_dir);
            }
        }
        Ok(())
    }

    fn hook_table_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);
            let pk_col_idx = self.read_batch_u64(batch, i, 3) as u32;
            let flags = self.read_batch_u64(batch, i, 5);
            let is_unique = (flags & TABLETAB_FLAG_UNIQUE_PK) != 0;

            if weight > 0 {
                if self.dag.tables.contains_key(&tid) { continue; }

                let col_defs = self.read_column_defs(tid)?;
                if col_defs.is_empty() {
                    gnitz_warn!("catalog: cannot register table '{}': columns not found", name);
                    continue;
                }

                if (pk_col_idx as usize) < col_defs.len() {
                    let pk_type = col_defs[pk_col_idx as usize].type_code;
                    if pk_type != type_code::U64 && pk_type != type_code::U128 {
                        return Err(format!("Primary Key must be TYPE_U64 or TYPE_U128, got type_code={}", pk_type));
                    }
                }

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = format!("{}/{}/{}_{}", self.base_dir, schema_name, name, tid);
                let tbl_schema = self.build_schema_from_col_defs(&col_defs, pk_col_idx);

                let num_parts = if tid < FIRST_USER_TABLE_ID { 1 } else { NUM_PARTITIONS };
                let arena = partition_arena_size(num_parts);
                gnitz_debug!("catalog: creating table dir={} name={} tid={} parts={}", directory, name, tid, num_parts);
                let pt = PartitionedTable::new(
                    &directory, &name, tbl_schema, tid as u32, num_parts,
                    true, self.active_part_start, self.active_part_end, arena,
                ).map_err(|e| format!("Failed to create table '{}': error {} (dir={})", name, e, directory))?;

                fsync_dir(&format!("{}/{}", self.base_dir, schema_name));
                self.dag.register_table(tid, StoreHandle::Partitioned(Box::new(pt)), tbl_schema, 0, is_unique, directory);
                if tid + 1 > self.next_table_id { self.next_table_id = tid + 1; }
                // Ensure needs_lock is set for unique_pk tables regardless of effect dispatch order
                if is_unique { self.recompute_needs_lock(tid); }
            } else if self.dag.tables.contains_key(&tid) {
                let children = self.caches.fk_by_parent.get(&tid).map(|v| v.as_slice()).unwrap_or(&[]);
                if let Some(&(child_tid, _)) = children.first() {
                    let (sn, tn) = self.caches.entity_by_id.get(&child_tid).cloned().unwrap_or_default();
                    return Err(format!("Integrity violation: table referenced by '{}.{}'", sn, tn));
                }
                self.dag.unregister_table(tid);
            }
        }
        Ok(())
    }

    fn hook_view_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let vid = batch.get_pk(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);

            if weight > 0 {
                if self.dag.tables.contains_key(&vid) { continue; }

                let col_defs = self.read_column_defs(vid)?;
                if col_defs.is_empty() {
                    gnitz_warn!("catalog: cannot register view '{}': columns not found", name);
                    continue;
                }

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = format!("{}/{}/view_{}_{}", self.base_dir, schema_name, name, vid);
                let view_schema = self.build_schema_from_col_defs(&col_defs, 0);

                let num_parts = if vid < FIRST_USER_TABLE_ID { 1 } else { NUM_PARTITIONS };
                let arena = partition_arena_size(num_parts);
                let et = PartitionedTable::new(
                    &directory, &name, view_schema, vid as u32, num_parts,
                    false, self.active_part_start, self.active_part_end, arena,
                ).map_err(|e| format!("Failed to create view '{}': error {}", name, e))?;

                let source_ids = self.dag.get_source_ids(vid);
                let max_depth = source_ids.iter()
                    .filter_map(|id| self.dag.tables.get(id))
                    .map(|e| e.depth + 1)
                    .max()
                    .unwrap_or(0);

                self.dag.register_table(vid, StoreHandle::Partitioned(Box::new(et)), view_schema, max_depth, false, directory);
                if vid + 1 > self.next_table_id { self.next_table_id = vid + 1; }

                if self.active_part_start != self.active_part_end
                    && self.dag.ensure_compiled(vid)
                    && !self.dag.view_needs_exchange(vid)
                {
                    self.backfill_view(vid);
                }
            } else if self.dag.tables.contains_key(&vid) {
                self.dag.unregister_table(vid);
            }
        }
        Ok(())
    }

    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let source_col_idx = self.read_batch_u64(batch, i, 2) as u32;
            let name = self.read_batch_string(batch, i, 3);
            let is_unique = self.read_batch_u64(batch, i, 4) != 0;

            if weight > 0 {
                // Use dag presence check since apply_index_by_name may have already fired
                let already_registered = self.dag.tables.get(&owner_id)
                    .map(|e| e.index_circuits.iter().any(|ic| ic.col_idx == source_col_idx))
                    .unwrap_or(false);
                if already_registered { continue; }

                let entry = self.dag.tables.get(&owner_id)
                    .ok_or_else(|| format!("Index: owner table {} not found", owner_id))?;
                let owner_schema = entry.schema;
                let source_col_type = owner_schema.columns[source_col_idx as usize].type_code;
                let source_pk_type = owner_schema.columns[owner_schema.pk_index as usize].type_code;

                let index_key_type = get_index_key_type(source_col_type)?;
                let idx_schema = make_index_schema(index_key_type, source_pk_type);

                let owner_dir = self.dag.tables.get(&owner_id)
                    .map(|e| e.directory.clone()).unwrap_or_default();
                let idx_dir = format!("{}/idx_{}", owner_dir, idx_id);

                let idx_table = Table::new(
                    &idx_dir, &format!("_idx_{}", idx_id), idx_schema, idx_id as u32,
                    SYS_TABLE_ARENA, false,
                ).map_err(|e| format!("Failed to create index table: error {}", e))?;

                let mut idx_table_box = Box::new(idx_table);
                let idx_table_ptr = &mut *idx_table_box as *mut Table;
                self.backfill_index(owner_id, source_col_idx, is_unique, idx_table_ptr, &idx_schema)?;
                self.dag.add_index_circuit(owner_id, source_col_idx, idx_table_box, idx_schema, is_unique);
                // Ensure needs_lock is correct after index registration
                if is_unique { self.recompute_needs_lock(owner_id); }
                let _ = name; // name captured via apply_index_by_name
            } else {
                let is_registered = self.dag.tables.get(&owner_id)
                    .map(|e| e.index_circuits.iter().any(|ic| ic.col_idx == source_col_idx))
                    .unwrap_or(false);
                if is_registered {
                    self.dag.remove_index_circuit(owner_id, source_col_idx);
                    // Recompute after removal so needs_lock reflects the updated state
                    if self.dag.tables.contains_key(&owner_id) {
                        self.recompute_needs_lock(owner_id);
                    }
                }
                let _ = name;
            }
        }
        Ok(())
    }

    fn hook_cascade_fk(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            if weight <= 0 { continue; }
            let tid = batch.get_pk(i) as i64;
            if self.caches.cascade_enabled && self.dag.tables.contains_key(&tid) {
                self.create_fk_indices(tid)?;
            }
        }
        Ok(())
    }
}
