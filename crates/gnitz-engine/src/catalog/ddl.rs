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

    pub(crate) fn create_table(
        &mut self,
        qualified_name: &str,
        col_defs: &[ColumnDef],
        pk_col_idx: u32,
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
        if pk_col_idx as usize >= col_defs.len() {
            return Err("Primary Key index out of bounds".into());
        }

        // Validate PK type (must be U64, U128, or UUID)
        let pk_type = col_defs[pk_col_idx as usize].type_code;
        if pk_type != type_code::U64 && pk_type != type_code::U128 && pk_type != type_code::UUID {
            return Err("Primary Key must be TYPE_U64, TYPE_U128, or UUID".into());
        }

        let tid = self.allocate_table_id();
        let sid = self.get_schema_id(schema_name);
        let self_pk_type = col_defs[pk_col_idx as usize].type_code;

        // Validate FK columns
        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id != 0 {
                self.validate_fk_column(cd, col_idx, tid, pk_col_idx, self_pk_type)?;
            }
        }

        let directory = format!("{}/{}/{}_{}", self.base_dir, schema_name, table_name, tid);
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
            bb.put_u64(pk_col_idx as u64);
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
        let batch = retract_single_row(&mut self.sys_tables, &schema, tid as u128);
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

        let schema = view_tab_schema();
        let batch = retract_single_row(&mut self.sys_views, &schema, vid as u128);
        if batch.count == 0 {
            return Err(format!("View does not exist: {}", qualified));
        }
        self.ingest_to_family(VIEW_TAB_ID, &batch)
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
        let col_defs = self.read_column_defs(owner_id)?;
        let col_idx = col_defs.iter().position(|cd| cd.name == col_name)
            .ok_or("Column not found in owner")?;

        let index_name = make_secondary_index_name(schema_name, table_name, col_name);
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
                // Route the undo through ingest_to_family so the index_by_name
                // / index_by_id hooks (which already fired on the +1 before
                // hook_index_register failed) are reversed by the matching -1.
                // Writing directly to storage would leave those caches poisoned
                // with a ghost index that doesn't exist on disk.
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
                let _ = self.ingest_to_family(IDX_TAB_ID, &undo_batch);
                return Err(e);
            }
        }

        self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        Ok(index_id)
    }

    pub fn drop_index(&mut self, index_name: &str) -> Result<(), String> {
        if index_name.contains("__fk_") {
            return Err("Forbidden: cannot drop internal FK index".into());
        }
        let idx_id = *self.caches.index_by_name.get(index_name)
            .ok_or_else(|| format!("Index does not exist: {}", index_name))?;

        // Read the full index record from sys_indices to retract it
        let mut cursor = self.sys_indices.create_cursor()
            .map_err(|e| format!("cursor error: {}", e))?;
        cursor.cursor.seek(crate::util::make_pk(idx_id as u64, 0));

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
            // The projected batch's PK is the indexed column value, one row
            // per source row with net weight +1.  For a unique index to be
            // valid, every key must appear at most once.
            let mut seen: HashSet<u128> = HashSet::with_capacity(projected.count);
            for row in 0..projected.count {
                if projected.get_weight(row) <= 0 { continue; }
                let key = projected.get_pk(row);
                if !seen.insert(key) {
                    return Err(format!(
                        "cannot create unique index: column contains duplicate values (table_id={}, col_idx={})",
                        owner_id, col_idx,
                    ));
                }
            }
        }

        let table = unsafe { &mut *idx_table };
        let _ = table.ingest_owned_batch(projected);
        Ok(())
    }

    // -- FK auto-index creation -------------------------------------------

    pub(crate) fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        let col_defs = self.read_column_defs(table_id)?;
        let pk_idx = self.caches.pk_col_of.get(&table_id).copied().unwrap_or(0) as usize;

        let (schema_name, table_name) = self.caches.entity_by_id.get(&table_id)
            .cloned().unwrap_or_default();

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 || col_idx == pk_idx { continue; }
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
            self.fire_hooks(IDX_TAB_ID, &batch)?;

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

    pub(crate) fn write_view_deps(&mut self, vid: i64, dep_ids: &[i64]) {
        let schema = dep_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        for &dep_tid in dep_ids {
            let (pk_lo, pk_hi) = pack_dep_pk(vid, dep_tid);
            bb.begin_row(crate::util::make_pk(pk_lo, pk_hi), 1);
            bb.put_u64(vid as u64);
            bb.put_u64(0); // dep_view_id
            bb.put_u64(dep_tid as u64);
            bb.end_row();
        }
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_view_deps, &batch);
    }

}
