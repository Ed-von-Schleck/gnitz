use super::*;

impl CatalogEngine {
    // -- DDL: CREATE/DROP SCHEMA -------------------------------------------

    pub fn create_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if self.caches.schema_by_name.contains_key(name) {
            return Err(format!("Schema already exists: {}", name));
        }
        let sid = self.allocate_schema_id();

        // Write schema record
        let schema = schema_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(sid as u128, 1);
        bb.put_string(name);
        bb.end_row();
        let batch = bb.finish();

        // Ingest into schemas family (triggers hook)
        self.ingest_to_family(SCHEMA_TAB_ID, &batch)?;

        self.advance_sequence(SEQ_ID_SCHEMAS, sid - 1, sid);
        Ok(())
    }

    /// Drop a schema and every table, view, and index it contains
    /// (PostgreSQL's `DROP SCHEMA ... CASCADE` semantics).
    pub fn drop_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if !self.caches.schema_by_name.contains_key(name) {
            return Err("Schema does not exist".into());
        }
        if name == "_system" {
            return Err("Forbidden: cannot drop system schema".into());
        }
        let sid = self.get_schema_id(name);

        // 1. Cascade: collect every view and table in the schema, then
        //    drop them in dependency-aware order. Views first (they may
        //    depend on tables + other views), retrying until stable to
        //    handle view-on-view chains. Then tables (retry for FK
        //    chains).
        let (view_names, table_names) = self.collect_schema_members(sid);
        self.drain_drop_targets(view_names, |catalog, q| catalog.drop_view(q))?;
        self.drain_drop_targets(table_names, |catalog, q| catalog.drop_table(q))?;

        // 2. Now the schema is guaranteed empty; emit the schema-drop
        //    delta exactly as before.
        let schema = schema_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(sid as u128, -1);
        bb.put_string(name);
        bb.end_row();
        let batch = bb.finish();

        self.ingest_to_family(SCHEMA_TAB_ID, &batch)?;
        Ok(())
    }

    /// Collect qualified names of every view and table in schema `sid`
    /// from in-memory caches. Views and tables are returned separately
    /// because drop_schema drops views first (to handle view-on-view deps)
    /// and tables second (to handle FK chains).
    fn collect_schema_members(&mut self, sid: i64) -> (Vec<String>, Vec<String>) {
        let view_ids: Vec<i64> = self.caches.views_by_schema.get(&sid)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let table_ids: Vec<i64> = self.caches.tables_by_schema.get(&sid)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let views = view_ids.iter()
            .filter_map(|&id| self.caches.entity_by_id.get(&id)
                .map(|(sn, en)| format!("{}.{}", sn, en)))
            .collect();
        let tables = table_ids.iter()
            .filter_map(|&id| self.caches.entity_by_id.get(&id)
                .map(|(sn, en)| format!("{}.{}", sn, en)))
            .collect();
        (views, tables)
    }

    /// Repeatedly attempt to drop each target; on dependency-related
    /// errors, move the target to the back of the queue and retry.
    /// Finishes when the queue is empty or no progress was made in a
    /// full pass (then returns the last error).
    fn drain_drop_targets<F>(
        &mut self,
        targets: Vec<String>,
        mut drop_fn: F,
    ) -> Result<(), String>
    where F: FnMut(&mut Self, &str) -> Result<(), String>,
    {
        let mut pending: Vec<String> = targets;
        loop {
            if pending.is_empty() { return Ok(()); }
            let before = pending.len();
            let mut retry: Vec<String> = Vec::new();
            let mut last_err: Option<String> = None;
            for q in pending.drain(..) {
                match drop_fn(self, &q) {
                    Ok(()) => {}
                    Err(e) => {
                        last_err = Some(e);
                        retry.push(q);
                    }
                }
            }
            if retry.len() == before {
                return Err(last_err.unwrap_or_else(|| "unknown cascade failure".into()));
            }
            pending = retry;
        }
    }

    // -- DDL: CREATE/DROP TABLE --------------------------------------------

    /// Test-only in-process shortcut for building a table directly in the
    /// catalog. **Not** a production code path: every real `CREATE TABLE`
    /// (SQL planner, C-API, `gnitz-py`) goes through
    /// `gnitz-core::Client::create_table`, which writes the raw `TABLE_TAB`
    /// batch client-side and pushes it over the wire; the engine then
    /// ingests it via `ingest_to_family → fire_hooks → hook_table_register`.
    /// `#[cfg(test)]` keeps this absent from production builds so it cannot
    /// be misread as the persisted-PK write path.
    #[cfg(test)]
    pub(crate) fn create_table(
        &mut self,
        qualified_name: &str,
        col_defs: &[ColumnDef],
        pk_cols: &[u32],
        unique_pk: bool,
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        if !self.caches.schema_by_name.contains_key(schema_name) {
            return Err("Schema does not exist".into());
        }
        let qualified = format!("{}.{}", schema_name, table_name);
        if self.caches.entity_by_qname.contains_key(&qualified) {
            return Err("Table already exists".into());
        }
        if col_defs.len() > crate::schema::MAX_COLUMNS {
            return Err(format!("Maximum {} columns supported", crate::schema::MAX_COLUMNS));
        }

        let raw_pk_cols = pack_pk_cols(pk_cols);
        let pk = unpack_pk_cols(raw_pk_cols);
        validate_pk_cols(col_defs, &pk)?;

        let tid = self.allocate_table_id();
        let sid = self.get_schema_id(schema_name);
        // Index the validated copy, not the raw `pk_cols` argument:
        // `validate_pk_cols` ran against `pk`, so `pk.as_slice()[0]` is
        // the value proven in-bounds and PK-eligible.
        let first_pk = pk.as_slice()[0];
        let self_pk_type = col_defs[first_pk as usize].type_code;

        // Validate FK columns. Compound-parent FK resolution is out of
        // scope; single-PK behaviour is preserved via the first PK column.
        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id != 0 {
                self.validate_fk_column(cd, col_idx, tid, first_pk, self_pk_type)?;
            }
        }

        let directory = table_dir(&self.base_dir, schema_name, table_name, tid);
        let flags = if unique_pk { TABLETAB_FLAG_UNIQUE_PK } else { 0 };

        // Write columns first (table hook reads them via sys_columns)
        self.write_column_records(tid, OWNER_KIND_TABLE, col_defs)?;

        // Write table record (triggers hook)
        {
            let schema = table_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(tid as u128, 1);
            bb.put_u64(sid as u64);
            bb.put_string(table_name);
            bb.put_string(&directory);
            bb.put_u64(raw_pk_cols);
            bb.put_u64(0); // created_lsn
            bb.put_u64(flags);
            bb.end_row();
            let batch = bb.finish();
            self.ingest_to_family(TABLE_TAB_ID, &batch)?;
        }

        self.advance_sequence(SEQ_ID_TABLES, tid - 1, tid);
        Ok(tid)
    }

    pub fn drop_table(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        let qualified = format!("{}.{}", schema_name, table_name);
        let tid = *self.caches.entity_by_qname.get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {}", qualified))?;

        let schema = table_tab_schema();
        let batch = retract_single_row(&self.sys_tables, &schema, tid as u128);
        if batch.count == 0 {
            return Err(format!("Table does not exist: {}", qualified));
        }
        self.ingest_to_family(TABLE_TAB_ID, &batch)
    }

    // -- DDL: CREATE/DROP VIEW ---------------------------------------------

    pub fn drop_view(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, view_name) = parse_qualified_name(qualified_name, "public");
        let qualified = format!("{}.{}", schema_name, view_name);
        let vid = *self.caches.entity_by_qname.get(&qualified)
            .ok_or_else(|| format!("View does not exist: {}", qualified))?;

        self.dag.invalidate(vid);

        // Remove the view's dependency rows from sys_view_deps first. dag.dep_map
        // is rebuilt by scanning sys_view_deps; ghost entries for a dropped view
        // would accumulate and force re-evaluation of dead dependencies on every
        // upstream change. Route through ingest_to_family so dep retractions
        // enter pending_broadcasts and are visible to compensate_stage_a rollback.
        let dep_batch = retract_rows_by_view(&self.sys_view_deps, &dep_tab_schema(), vid as u64);
        if dep_batch.count > 0 {
            self.ingest_to_family(DEP_TAB_ID, &dep_batch)?;
        }

        let schema = view_tab_schema();
        let batch = retract_single_row(&self.sys_views, &schema, vid as u128);
        if batch.count == 0 {
            return Err(format!("View does not exist: {}", qualified));
        }
        self.ingest_to_family(VIEW_TAB_ID, &batch)?;
        // hook_view_register queues the view's directory into
        // pending_dir_deletions; the executor removes it after the DDL zone is
        // durable. (No synchronous delete: that would race the WAL fdatasync.)
        Ok(())
    }

    // -- DDL: CREATE/DROP INDEX --------------------------------------------

    pub fn create_index(
        &mut self,
        qualified_owner: &str,
        col_name: &str,
        is_unique: bool,
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_owner, "public");
        let qualified = format!("{}.{}", schema_name, table_name);
        let owner_id = *self.caches.entity_by_qname.get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {}", qualified))?;

        // Find column
        let col_defs = self.read_column_defs(owner_id);
        let col_idx = col_defs.iter().position(|cd| cd.name == col_name)
            .ok_or("Column not found in owner")?;

        // STRING and BLOB values cannot be reduced to a comparable u128 key
        // without collision; the distributed uniqueness-check pipeline would
        // silently bypass or falsely reject rows. (Non-unique FK indices use the
        // xxhash-based extract_col_key path and are unaffected.)
        if is_unique && gnitz_wire::is_german_string(col_defs[col_idx].type_code) {
            return Err(
                "UNIQUE index on STRING or BLOB columns is not supported".into()
            );
        }

        let index_name = make_secondary_index_name(schema_name, table_name, col_name);
        // Reject a duplicate before allocating an index_id. Otherwise
        // apply_index_by_name silently overwrites the cache entry and orphans
        // the previous index circuit.
        if self.caches.index_by_name.contains_key(&index_name) {
            return Err(format!("Index already exists: {}", index_name));
        }
        let index_id = self.allocate_index_id();

        // Write index record (triggers hook). If the hook fails — most
        // notably when is_unique and the source column contains duplicates —
        // we retract the +1 so sys_indices stays consistent and the next
        // restart's replay doesn't try to reconstruct a broken index.
        {
            let schema = idx_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(index_id as u128, 1);
            bb.put_u64(owner_id as u64);
            bb.put_u64(OWNER_KIND_TABLE as u64);
            bb.put_u64(col_idx as u64);
            bb.put_string(&index_name);
            bb.put_u64(if is_unique { 1 } else { 0 });
            bb.put_string("");
            bb.end_row();
            let batch = bb.finish();
            if let Err(e) = self.ingest_to_family(IDX_TAB_ID, &batch) {
                // The +1 failed in hook_index_register *before* it was enqueued
                // into pending_broadcasts, so it was never broadcast to workers.
                // Route the undo through ingest_to_family_no_broadcast: it fires
                // the cache-reversal hooks (index_by_name / index_by_id) but does
                // NOT enqueue the −1. Broadcasting the −1 would deliver a phantom
                // retraction to workers that never saw the +1, leaving an
                // orphaned negative-weight row in their sys_indices.
                let mut undo = BatchBuilder::new(schema);
                undo.begin_row(index_id as u128, -1);
                undo.put_u64(owner_id as u64);
                undo.put_u64(OWNER_KIND_TABLE as u64);
                undo.put_u64(col_idx as u64);
                undo.put_string(&index_name);
                undo.put_u64(if is_unique { 1 } else { 0 });
                undo.put_string("");
                undo.end_row();
                let undo_batch = undo.finish();
                let _ = self.ingest_to_family_no_broadcast(IDX_TAB_ID, &undo_batch);
                return Err(e);
            }
        }

        self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        Ok(index_id)
    }

    pub fn drop_index(&mut self, index_name: &str) -> Result<(), String> {
        if index_name.contains(FK_INDEX_INFIX) {
            return Err("Forbidden: cannot drop internal FK index".into());
        }
        let idx_id = *self.caches.index_by_name.get(index_name)
            .ok_or_else(|| format!("Index does not exist: {}", index_name))?;

        // Read the full index record from sys_indices to retract it
        // (sys_indices has a single U64 PK; seek by the zero-extended id).
        let mut cursor = self.sys_indices.open_cursor();
        // sys_indices has a single U64 PK; OPK == big-endian.
        cursor.cursor.seek_bytes(&(idx_id as u64).to_be_bytes());

        if !cursor.cursor.valid || cursor.cursor.current_key as u64 != idx_id as u64 {
            return Err(format!("Index {} not found in catalog", index_name));
        }

        let owner_id = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
        let owner_kind = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_KIND);
        let source_col_idx = cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COL_IDX);
        let name = cursor_read_string(&cursor, IDXTAB_COL_NAME);
        let is_unique = cursor_read_u64(&cursor, IDXTAB_COL_IS_UNIQUE);
        let cache_dir = cursor_read_string(&cursor, IDXTAB_COL_CACHE_DIRECTORY);

        let schema = idx_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(idx_id as u128, -1);
        bb.put_u64(owner_id as u64);
        bb.put_u64(owner_kind);
        bb.put_u64(source_col_idx);
        bb.put_string(&name);
        bb.put_u64(is_unique);
        bb.put_string(&cache_dir);
        bb.end_row();
        let batch = bb.finish();
        self.ingest_to_family(IDX_TAB_ID, &batch)?;
        Ok(())
    }

    // -- View backfill (scan source, feed through plan) --------------------

    pub(crate) fn backfill_view(&mut self, vid: i64) {
        if !self.dag.ensure_compiled(vid) { return; }

        let source_ids = self.dag.get_source_ids(vid);
        for source_id in source_ids {
            if !self.dag.tables.contains_key(&source_id) { continue; }

            let entry = match self.dag.tables.get(&source_id) {
                Some(e) => e,
                None => continue,
            };
            let schema = entry.schema;

            // Scan source table into a batch
            let scan_batch = self.scan_store(source_id, &schema);
            if scan_batch.count == 0 { continue; }

            // Execute view plan on the batch
            let owned = Rc::try_unwrap(scan_batch).unwrap_or_else(|a| (*a).clone());
            let out_handle = self.dag.execute_epoch(vid, owned, source_id);
            if let Some(result) = out_handle {
                if result.count > 0 {
                    self.dag.ingest_to_family(vid, result);
                    let _ = self.dag.flush(vid);
                }
            }
        }

        // execute_epoch clears deltas only at epoch start, so the scanned
        // source batch and intermediate delta registers remain pinned in the
        // view's regfile after the last source. Release them now.
        self.dag.clear_view_regfile_deltas(vid);
    }

    // -- Index backfill (scan source, project into index table) ------------

    pub(crate) fn backfill_index(
        &mut self,
        owner_id: i64,
        col_idx: u32,
        is_unique: bool,
        idx_table: *mut Table,
        idx_schema: &SchemaDescriptor,
    ) -> Result<(), String> {
        let entry = match self.dag.tables.get(&owner_id) {
            Some(e) => e,
            None => return Ok(()),
        };
        let owner_schema = entry.schema;

        // Scan source and project into index
        let scan = self.scan_store(owner_id, &owner_schema);
        if scan.count == 0 { return Ok(()); }

        let projected = DagEngine::batch_project_index(&scan, col_idx, &owner_schema, idx_schema);
        if projected.count == 0 { return Ok(()); }

        if is_unique {
            // Compound-PK index layout: index PK is `(indexed_key, src_pk…)`.
            // Uniqueness applies to the leading `indexed_key` slot only —
            // two rows differing only in their source-PK suffix represent
            // two source rows sharing the indexed value. `make_index_schema`
            // always promotes the leading key to ≤16 bytes, so a u128 dedup
            // token suffices (zero-padded LE for narrower types).
            let key_size = idx_schema.columns[0].size() as usize;
            debug_assert!(key_size <= 16,
                "unique index key size {} exceeds 16-byte dedup buffer", key_size);
            let mut seen: HashSet<u128> = HashSet::with_capacity(projected.count);
            for row in 0..projected.count {
                if projected.get_weight(row) <= 0 { continue; }
                let pk_bytes = projected.get_pk_bytes(row);
                let mut buf = [0u8; 16];
                buf[..key_size].copy_from_slice(&pk_bytes[..key_size]);
                if !seen.insert(u128::from_le_bytes(buf)) {
                    return Err(self.unique_create_dup_err(owner_id, col_idx as usize));
                }
            }
        }

        let table = unsafe { &mut *idx_table };
        let _ = table.ingest_owned_batch(projected);
        Ok(())
    }

    // -- FK auto-index creation -------------------------------------------

    pub(crate) fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        let col_defs = self.read_column_defs(table_id);
        let pk_list = self.caches.pk_col_of.get(&table_id)
            .copied().unwrap_or_else(|| PkColList::single(0));

        let (schema_name, table_name) = self.caches.entity_by_id.get(&table_id)
            .cloned().unwrap_or_default();

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 { continue; }
            // Skip every PK column: the PK region already stores them, so an
            // FK whose column is part of the PK needs no separate auto-index.
            if pk_list.as_slice().contains(&(col_idx as u32)) { continue; }
            let index_name = make_fk_index_name(&schema_name, &table_name, &cd.name);
            if self.caches.index_by_name.contains_key(&index_name) { continue; }

            let index_id = self.allocate_index_id();

            // Write index record to sys_indices
            let idx_schema = idx_tab_schema();
            let mut bb = BatchBuilder::new(idx_schema);
            bb.begin_row(index_id as u128, 1);
            bb.put_u64(table_id as u64);           // owner_id
            bb.put_u64(OWNER_KIND_TABLE as u64);   // owner_kind
            bb.put_u64(col_idx as u64);            // source_col_idx
            bb.put_string(&index_name);            // name
            bb.put_u64(0);                         // is_unique (FK indices are not unique)
            bb.put_string("");                     // cache_directory
            bb.end_row();
            let batch = bb.finish();
            // hook_cascade_fk fires on both master and workers, so each side
            // computes its own FK indices locally. Routing this through
            // `ingest_to_family` would broadcast IDX_TAB before TABLE_TAB and
            // duplicate the rows the worker already produced.
            ingest_batch_into(&mut self.sys_indices, &batch);
            if let Err(e) = self.fire_hooks(IDX_TAB_ID, &batch) {
                // The +1 is already in sys_indices but its directory/cache
                // setup failed. Reverse the storage write and re-fire hooks on
                // the −1 so any partial cache updates are undone; otherwise the
                // next boot's replay opens a missing index directory and crashes.
                let mut ub = BatchBuilder::new(idx_schema);
                ub.begin_row(index_id as u128, -1);
                ub.put_u64(table_id as u64);
                ub.put_u64(OWNER_KIND_TABLE as u64);
                ub.put_u64(col_idx as u64);
                ub.put_string(&index_name);
                ub.put_u64(0);
                ub.put_string("");
                ub.end_row();
                let undo = ub.finish();
                ingest_batch_into(&mut self.sys_indices, &undo);
                self.fire_hooks(IDX_TAB_ID, &undo).ok();
                return Err(e);
            }

            self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        }
        Ok(())
    }

    // -- Write helpers for system tables -----------------------------------

    pub(crate) fn build_col_batch(&self, owner_id: i64, kind: i64, col_defs: &[ColumnDef], weight: i64) -> Batch {
        let schema = col_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        for (i, cd) in col_defs.iter().enumerate() {
            let pk = pack_column_id(owner_id, i as i64);
            bb.begin_row(pk as u128, weight);
            bb.put_u64(owner_id as u64);
            bb.put_u64(kind as u64);
            bb.put_u64(i as u64);
            bb.put_string(&cd.name);
            bb.put_u64(cd.type_code as u64);
            bb.put_u64(if cd.is_nullable { 1 } else { 0 });
            bb.put_u64(cd.fk_table_id as u64);
            bb.put_u64(cd.fk_col_idx as u64);
            bb.end_row();
        }
        bb.finish()
    }

    pub(crate) fn write_column_records(&mut self, owner_id: i64, kind: i64, col_defs: &[ColumnDef]) -> Result<(), String> {
        let batch = self.build_col_batch(owner_id, kind, col_defs, 1);
        self.ingest_to_family(COL_TAB_ID, &batch)
    }

    pub(crate) fn write_view_deps(&mut self, vid: i64, dep_ids: &[i64]) -> Result<(), String> {
        let schema = dep_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        for &dep_tid in dep_ids {
            // Compound PK (view_id, dep_table_id); dep_view_id is the only payload.
            bb.begin_row(pack_view_pk(vid, dep_tid as u64), 1);
            bb.put_u64(0); // dep_view_id
            bb.end_row();
        }
        let batch = bb.finish();
        if batch.count > 0 {
            self.ingest_to_family(DEP_TAB_ID, &batch)?;
        }
        Ok(())
    }

}
