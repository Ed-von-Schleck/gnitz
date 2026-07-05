use super::*;
use crate::storage::PkBuf;

/// Build the one-row IDX_TAB batch registering an index (`weight` +1) or
/// retracting a failed registration (−1). The single writer of the 6-column
/// IDX_TAB row layout in the DDL emitters.
fn idx_tab_row(index_id: i64, owner_id: i64, packed_cols: u64, name: &str, is_unique: bool, weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(idx_tab_schema());
    bb.begin_row(index_id as u128, weight);
    bb.put_u64(owner_id as u64);
    bb.put_u64(OWNER_KIND_TABLE as u64);
    bb.put_u64(packed_cols);
    bb.put_string(name);
    bb.put_u64(if is_unique { 1 } else { 0 });
    bb.put_string(""); // cache_directory
    bb.end_row();
    bb.finish()
}

impl CatalogEngine {
    /// Locally retract an index registration whose +1 was applied but never
    /// broadcast. A failed rollback leaves the +1 in sys_indices while the
    /// client is handed an error — a permanently diverged catalog, and the
    /// next boot's replay would open a missing index directory — so it
    /// fail-stops, matching `compensate_stage_a`'s rollback abort.
    fn rollback_index_registration(&mut self, undo: Batch, index_id: i64) {
        if let Err(undo_err) = self.submit_local(SysFamily::Index, undo) {
            crate::gnitz_fatal_abort!(
                "catalog: index registration rollback failed (index_id={}): {} \
                 — catalog state permanently diverged; aborting",
                index_id,
                undo_err,
            );
        }
    }

    // -- DDL: CREATE/DROP SCHEMA -------------------------------------------

    // Like `create_table` below, the direct-call DDL entry points in this file
    // are test-only: production DDL arrives as system-table deltas over the
    // wire (`ingest_to_family`), never through these wrappers. `#[cfg(test)]`
    // keeps them absent from production builds.
    #[cfg(test)]
    pub(crate) fn create_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if self.caches.schema_by_name.contains_key(name) {
            return Err(format!("Schema already exists: {name}"));
        }
        let sid = self.allocate_schema_id();

        // Write schema record
        let schema = schema_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(sid as u128, 1);
        bb.put_string(name);
        bb.end_row();
        let batch = bb.finish();

        // Submit the schemas-family delta (triggers hook).
        self.submit(SysFamily::Schema, batch)?;

        self.advance_sequence(SEQ_ID_SCHEMAS, sid - 1, sid);
        Ok(())
    }

    /// Drop a schema and every table, view, and index it contains
    /// (PostgreSQL's `DROP SCHEMA ... CASCADE` semantics).
    #[cfg(test)]
    pub(crate) fn drop_schema(&mut self, name: &str) -> Result<(), String> {
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

        self.submit(SysFamily::Schema, batch)?;
        Ok(())
    }

    /// Collect qualified names of every view and table in schema `sid`
    /// from in-memory caches. Views and tables are returned separately
    /// because drop_schema drops views first (to handle view-on-view deps)
    /// and tables second (to handle FK chains).
    #[cfg(test)]
    fn collect_schema_members(&mut self, sid: i64) -> (Vec<String>, Vec<String>) {
        let view_ids: Vec<i64> = self
            .caches
            .views_by_schema
            .get(&sid)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let table_ids: Vec<i64> = self
            .caches
            .tables_by_schema
            .get(&sid)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let views = view_ids
            .iter()
            .filter_map(|&id| self.caches.entity_by_id.get(&id).map(|(sn, en)| format!("{sn}.{en}")))
            .collect();
        let tables = table_ids
            .iter()
            .filter_map(|&id| self.caches.entity_by_id.get(&id).map(|(sn, en)| format!("{sn}.{en}")))
            .collect();
        (views, tables)
    }

    /// Repeatedly attempt to drop each target; on dependency-related
    /// errors, move the target to the back of the queue and retry.
    /// Finishes when the queue is empty or no progress was made in a
    /// full pass (then returns the last error).
    #[cfg(test)]
    fn drain_drop_targets<F>(&mut self, targets: Vec<String>, mut drop_fn: F) -> Result<(), String>
    where
        F: FnMut(&mut Self, &str) -> Result<(), String>,
    {
        let mut pending: Vec<String> = targets;
        loop {
            if pending.is_empty() {
                return Ok(());
            }
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
        let qualified = format!("{schema_name}.{table_name}");
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
        // This in-process test shortcut always builds partitioned, full-PK-distributed
        // tables (`replicated = false`, `k = 0` = default). REPLICATED and CLUSTER BY
        // routing are exercised through the catalog hook / SQL planner, not here.
        let flags = gnitz_wire::pack_table_flags(unique_pk, false, 0);

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
            self.submit(SysFamily::Table, batch)?;
        }

        self.advance_sequence(SEQ_ID_TABLES, tid - 1, tid);
        Ok(tid)
    }

    #[cfg(test)]
    pub(crate) fn drop_table(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        let qualified = format!("{schema_name}.{table_name}");
        let tid = *self
            .caches
            .entity_by_qname
            .get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {qualified}"))?;

        // Retract only the TABLE_TAB row. Its -1 fires hook_table_register,
        // which cascades cascade_retract_indices + cascade_retract_columns. The
        // cascade lives in the hook (not inline) so WAL replay and worker sync —
        // which re-apply the -1 without calling drop_table — clean up
        // identically.
        self.submit_retraction(SysFamily::Table, tid as u128)
    }

    // -- DDL: CREATE/DROP VIEW ---------------------------------------------

    #[cfg(test)]
    pub(crate) fn drop_view(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, view_name) = parse_qualified_name(qualified_name, "public");
        let qualified = format!("{schema_name}.{view_name}");
        let vid = *self
            .caches
            .entity_by_qname
            .get(&qualified)
            .ok_or_else(|| format!("View does not exist: {qualified}"))?;

        self.dag.invalidate(vid);

        // Retract only the VIEW_TAB row. Its -1 fires hook_view_register, which
        // cascades cascade_retract_circuit_and_deps (sys_circuit_* AND
        // sys_view_deps) + cascade_retract_columns, and queues the view
        // directory for deferred deletion (the executor removes it after the DDL
        // zone is durable — no synchronous delete that would race the WAL
        // fdatasync). dag.invalidate(vid) only clears the plan caches — it does
        // NOT unregister the view — so the cascade's dag.tables.contains_key(vid)
        // guard holds and the -1 cleans up identically on replay and worker sync.
        // The view's sys_view_deps rows are retracted by that cascade, so no
        // separate DEP_TAB retraction is needed here.
        self.submit_retraction(SysFamily::View, vid as u128)
    }

    // -- DDL: CREATE/DROP INDEX --------------------------------------------

    #[cfg(test)]
    pub(crate) fn create_index(
        &mut self,
        qualified_owner: &str,
        col_names: &[&str],
        is_unique: bool,
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_owner, "public");
        let qualified = format!("{schema_name}.{table_name}");
        let owner_id = *self
            .caches
            .entity_by_qname
            .get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {qualified}"))?;

        // Resolve each column name to its index, in declared order.
        let col_defs = self.read_column_defs(owner_id);
        let col_indices: Vec<u32> = col_names
            .iter()
            .map(|name| {
                col_defs
                    .iter()
                    .position(|cd| cd.name == *name)
                    .map(|p| p as u32)
                    .ok_or_else(|| format!("Column not found in owner: {name}"))
            })
            .collect::<Result<_, _>>()?;

        // STRING and BLOB values cannot be reduced to a comparable u128 key
        // without collision; the distributed uniqueness-check pipeline would
        // silently bypass or falsely reject rows. (Non-unique FK indices don't
        // run this uniqueness-check pipeline, so they are unaffected.)
        if is_unique
            && col_indices
                .iter()
                .any(|&c| gnitz_wire::is_german_string(col_defs[c as usize].type_code))
        {
            return Err("UNIQUE index on STRING or BLOB columns is not supported".into());
        }

        let index_name = make_secondary_index_name(schema_name, table_name, &col_names.join("_"));
        // Reject a duplicate before allocating an index_id. Otherwise
        // apply_index_by_name silently overwrites the cache entry and orphans
        // the previous index circuit.
        if self.caches.index_by_name.contains_key(&index_name) {
            return Err(format!("Index already exists: {index_name}"));
        }
        let index_id = self.allocate_index_id();

        // Write index record (triggers hook). If the hook fails — most
        // notably when is_unique and the source column contains duplicates —
        // we retract the +1 so sys_indices stays consistent and the next
        // restart's replay doesn't try to reconstruct a broken index.
        {
            let packed_cols = gnitz_wire::pack_pk_cols(&col_indices);
            let batch = idx_tab_row(index_id, owner_id, packed_cols, &index_name, is_unique, 1);
            if let Err(e) = self.submit(SysFamily::Index, batch) {
                // The +1 failed in hook_index_register *before* it was enqueued
                // into pending_broadcasts, so it was never broadcast to workers.
                // Route the undo through submit_local: it fires the cache-reversal
                // hooks (index_by_name / index_by_id) but does NOT enqueue the −1.
                // Broadcasting the −1 would deliver a phantom retraction to
                // workers that never saw the +1, leaving an orphaned
                // negative-weight row in their sys_indices.
                let undo = idx_tab_row(index_id, owner_id, packed_cols, &index_name, is_unique, -1);
                self.rollback_index_registration(undo, index_id);
                // The hook pre-staged the index directory into
                // pending_dir_deletions before Table::new; when backfill_index
                // fails the circuit is never registered, so the -1 retraction
                // hook does not queue the directory and it would leak. Drain
                // here (existence-guarded remove_dir_all; safe even if Table::new
                // failed before creating the directory).
                self.drain_pending_dir_deletions();
                return Err(e);
            }
        }

        self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        Ok(index_id)
    }

    #[cfg(test)]
    pub(crate) fn drop_index(&mut self, index_name: &str) -> Result<(), String> {
        if index_name.contains(FK_INDEX_INFIX) {
            return Err("Forbidden: cannot drop internal FK index".into());
        }
        let idx_id = *self
            .caches
            .index_by_name
            .get(index_name)
            .ok_or_else(|| format!("Index does not exist: {index_name}"))?;

        // precheck_sys_ingest enforces the FK-target uniqueness guard on the -1;
        // the cascade (circuit demotion/deletion) is the applier's reaction in
        // hook_index_register.
        self.submit_retraction(SysFamily::Index, idx_id as u128)
    }

    // -- View backfill (scan source, feed through plan) --------------------

    pub(crate) fn backfill_view(&mut self, vid: i64) {
        // Backfill is the sole population of ephemeral storage (erased at
        // open). Running it against a durable relation would double-count the
        // shards loaded from its manifest.
        debug_assert!(
            self.dag
                .tables
                .get(&vid)
                .is_none_or(|e| e.kind.recovery_source() != RecoverySource::SalReplay),
            "backfill_view on durable relation {vid}: would double-count loaded shards",
        );
        if !self.dag.ensure_compiled(vid) {
            return;
        }

        let chunk_rows = self.ddl_scan_chunk_rows;
        let source_ids = self.dag.get_source_ids(vid);
        for source_id in source_ids {
            // Feed the source chunk-wise through the incremental plan — the
            // normal delta push path, so the chunk sum equals the whole-batch
            // result. The cursor owns its sources via `Rc` and stays valid
            // while epochs ingest into the view family; the scanned source
            // itself is never written here.
            let Some(mut handle) = self.open_store_cursor(source_id) else {
                continue;
            };
            let mut touched = false;
            let mut first = true;
            loop {
                let chunk = match handle.cursor.drain_chunk(chunk_rows) {
                    Some(chunk) => chunk,
                    // Source dry on the first iteration: feed one empty epoch
                    // through the same body so a global-aggregate reduce's n==0
                    // ground seed fires (op_reduce mints its one SQL-required row
                    // only when it receives an epoch). A no-op for every other view
                    // — all operators early-return on an empty delta. This mirrors
                    // the partitioned exchange-backfill's trailing pad round; it
                    // must live here because `backfill_view` drives BOTH live CREATE
                    // and the post-recovery rebuild of non-exchange views, so a call
                    // site would lose the ground row on restart of an empty view.
                    None if first => match self.dag.tables.get(&source_id).map(|e| e.schema) {
                        Some(schema) => Batch::empty_with_schema(&schema),
                        None => break,
                    },
                    None => break,
                };
                first = false;
                if let Some(result) = self.dag.execute_epoch(vid, chunk, source_id) {
                    if result.count > 0 {
                        self.dag.ingest_to_family(vid, result);
                        touched = true;
                    }
                }
            }
            if touched {
                self.dag.flush_view_or_abort(vid);
            }
        }

        // execute_epoch clears deltas only at epoch start, so each chunk's
        // epoch releases what the previous chunk pinned — but the LAST
        // epoch's input and intermediate delta registers remain pinned in
        // the view's regfile after the final source. Release them now.
        self.dag.clear_view_regfile_deltas(vid);
    }

    // -- Index backfill (scan source, project into index table) ------------

    pub(crate) fn backfill_index(
        &mut self,
        owner_id: i64,
        col_indices: &[u32],
        idx_table: *mut Table,
        idx_schema: &SchemaDescriptor,
        check_dups: bool,
    ) -> Result<(), String> {
        // The relation rebuilt here is the *index table* (`owner_id` is the
        // indexed relation, a durable base table). Backfilling into durable
        // storage would double-count its loaded shards on the next open.
        debug_assert!(
            unsafe { &*idx_table }.recovery_source() == RecoverySource::Rederive,
            "backfill_index into durable storage (owner {owner_id}): would double-count",
        );
        self.stream_index_projection(owner_id, col_indices, Some(idx_table), idx_schema, check_dups)
    }

    /// Worker-boot index rebuild: for every registered index circuit, re-create
    /// its Table at THIS process's `index_table_dir` (replacing the
    /// fork-inherited parent-dir table) and backfill it from the trimmed/rehomed
    /// base slice — one scan per owner table, each chunk projected into all of
    /// its index tables. Must run after trim/rehome and BEFORE SAL replay —
    /// replay projects the unflushed committed tail into the index exactly once
    /// through `ingest_store_and_indices`, so a rebuild *after* replay would
    /// double-count every replayed row. Boot data was validated at original
    /// write time and a slice-local check cannot see a duplicate that
    /// legitimately straddles two workers' slices, so the rebuild skips the dup
    /// check entirely. Fail-fast: an error aborts worker boot via the startup
    /// ACK.
    ///
    /// In a Standalone process this is a legal idempotent re-create-and-rebuild
    /// at the parent dir — unit-testable without touching the global role.
    pub fn backfill_all_indexes(&mut self) -> Result<(), String> {
        // Snapshot the worklist first: each owner's rebuild mutably borrows
        // self.dag (`replace_index_table`), so no borrow of `dag.tables` may be
        // held across the loop. Only base tables carry index circuits, so
        // views/system tables contribute nothing.
        struct IndexWork {
            cols: PkColList,
            index_id: i64,
            idx_schema: SchemaDescriptor,
        }
        let worklist: Vec<(i64, String, Vec<IndexWork>)> = self
            .dag
            .tables
            .iter()
            .filter(|(_, entry)| !entry.index_circuits.is_empty())
            .map(|(&owner_id, entry)| {
                let works = entry
                    .index_circuits
                    .iter()
                    .map(|ic| IndexWork {
                        cols: ic.col_indices,
                        index_id: ic.index_id,
                        idx_schema: ic.index_schema,
                    })
                    .collect();
                (owner_id, entry.directory.clone(), works)
            })
            .collect();

        for (owner_id, owner_dir, works) in worklist {
            // Re-create and install every index table before opening the scan.
            let mut targets: Vec<(PkColList, SchemaDescriptor, *mut Table)> = Vec::with_capacity(works.len());
            for w in &works {
                let table = new_index_table(&index_dir(&owner_dir, w.index_id), w.index_id, w.idx_schema)
                    .map_err(|e| format!("index table re-create failed (owner {owner_id}): {e}"))?;
                let ptr = self
                    .dag
                    .replace_index_table(owner_id, w.cols.as_slice(), Box::new(table))
                    .ok_or_else(|| format!("index circuit vanished during rebuild (owner {owner_id})"))?;
                targets.push((w.cols, w.idx_schema, ptr));
            }

            // One base-slice scan feeds all of the owner's indexes.
            let Some(owner_schema) = self.dag.tables.get(&owner_id).map(|e| e.schema) else {
                continue;
            };
            let chunk_rows = self.ddl_scan_chunk_rows;
            let Some(mut handle) = self.open_store_cursor(owner_id) else {
                continue;
            };
            while let Some(chunk) = handle.cursor.drain_chunk(chunk_rows) {
                for (cols, idx_schema, table) in &targets {
                    let projected = DagEngine::batch_project_index(&chunk, cols.as_slice(), &owner_schema, idx_schema);
                    if projected.count == 0 {
                        continue;
                    }
                    unsafe { &mut **table }
                        .ingest_owned_batch(projected)
                        .map_err(|e| format!("index rebuild: ingest failed (owner {owner_id}): {e}"))?;
                }
            }
        }
        Ok(())
    }

    /// Shared streaming pass for `backfill_index` and `promote_index_to_unique`:
    /// scan `owner_id` chunk-wise, project each chunk into the index layout,
    /// reject duplicate keys when `check_dups`, and (backfill only) ingest each
    /// projected chunk into `idx_table`. Peak memory is O(chunk × row_width)
    /// plus, when checking, the cross-chunk `seen` set (32 B per scanned key).
    ///
    /// `check_dups` is a caller policy (hoisted out of this function): the live
    /// CREATE-INDEX/promote paths pass `is_unique && ctx.is_live()` / `true`; the
    /// worker/standalone boot rebuild passes `false`. A boot-replayed IDX_TAB
    /// `+1` was validated at original write time and every later INSERT went
    /// through the unique filter, so skipping the check at boot cannot admit a
    /// duplicate; it only avoids an O(rows × 32 B) `seen` set. A slice-local
    /// rebuild also cannot false-positive (a global duplicate may legitimately
    /// straddle two workers' slices), so the boot skip is doubly justified. The
    /// ingest is unconditional — it IS the ephemeral index rebuild.
    ///
    /// On a duplicate found mid-stream the partially-ingested index table is
    /// discarded whole: it is ephemeral, registered nowhere, and its directory
    /// is pre-staged in `pending_dir_deletions` by `hook_index_register`.
    fn stream_index_projection(
        &mut self,
        owner_id: i64,
        col_indices: &[u32],
        idx_table: Option<*mut Table>,
        idx_schema: &SchemaDescriptor,
        check_dups: bool,
    ) -> Result<(), String> {
        let owner_schema = match self.dag.tables.get(&owner_id) {
            Some(e) => e.schema,
            None => return Ok(()),
        };
        let chunk_rows = self.ddl_scan_chunk_rows;
        // The duplicate check applies to the full composite leading span.
        let idx_key_size = idx_schema.leading_key_size(col_indices.len());
        let mut seen: rustc_hash::FxHashSet<PkBuf> = rustc_hash::FxHashSet::default();

        let Some(mut handle) = self.open_store_cursor(owner_id) else {
            return Ok(());
        };
        while let Some(chunk) = handle.cursor.drain_chunk(chunk_rows) {
            let projected = DagEngine::batch_project_index(&chunk, col_indices, &owner_schema, idx_schema);
            if projected.count == 0 {
                continue;
            }
            if check_dups && projected_chunk_has_dup_keys(&projected, idx_key_size, &mut seen) {
                return Err(self.unique_create_dup_err(owner_id, col_indices));
            }
            if let Some(table) = idx_table {
                unsafe { &mut *table }
                    .ingest_owned_batch(projected)
                    .map_err(|e| format!("index backfill: ingest failed (owner {owner_id}): {e}"))?;
            }
        }
        Ok(())
    }

    /// Promote the existing index circuit on `col_idx` to unique, after verifying
    /// the committed base rows contain no duplicate keys. Used when a UNIQUE index
    /// registers over a column that already has a circuit (an FK auto-index, or a
    /// prior non-unique index): the per-column dedup keeps one circuit, so the
    /// uniqueness is folded into the incumbent — no second index table is built
    /// (`make_index_schema` does not depend on `is_unique`; uniqueness is the flag
    /// plus the duplicate check, not a different storage layout). Empty base table
    /// → pure flag flip. Skips the scan outside the live phase (boot shard replay)
    /// because data was validated at original write time (mirrors the same guard
    /// in `hook_cascade_fk`). The flag flip below runs unconditionally.
    pub(crate) fn promote_index_to_unique(&mut self, owner_id: i64, col_indices: &[u32]) -> Result<(), String> {
        if self.ctx.is_live() {
            let owner_schema = self
                .dag
                .tables
                .get(&owner_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Index promote: owner table {owner_id} not found"))?;
            let idx_schema = make_index_schema(col_indices, &owner_schema)?;
            self.stream_index_projection(owner_id, col_indices, None, &idx_schema, true)?;
        }
        self.dag.set_index_circuit_uniqueness(owner_id, col_indices, true);
        Ok(())
    }

    // -- FK auto-index creation -------------------------------------------

    pub(crate) fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        let col_defs = self.read_column_defs(table_id);
        let pk_list = self
            .caches
            .pk_col_of
            .get(&table_id)
            .copied()
            .unwrap_or_else(|| PkColList::single(0));

        let (schema_name, table_name) = self.caches.entity_by_id.get(&table_id).cloned().unwrap_or_default();

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 {
                continue;
            }
            // Skip every PK column: the PK region already stores them, so an
            // FK whose column is part of the PK needs no separate auto-index.
            if pk_list.as_slice().contains(&(col_idx as u32)) {
                continue;
            }
            let index_name = make_fk_index_name(&schema_name, &table_name, &cd.name);
            if self.caches.index_by_name.contains_key(&index_name) {
                continue;
            }

            let index_id = self.allocate_index_id();

            // Write index record to sys_indices (FK indices are not unique).
            let packed_cols = gnitz_wire::pack_pk_cols(&[col_idx as u32]);
            let batch = idx_tab_row(index_id, table_id, packed_cols, &index_name, false, 1);
            // hook_cascade_fk fires on master and every worker, so each side
            // creates its own FK indices locally; submit_local applies + fires
            // hooks without a broadcast. submit would broadcast IDX_TAB before
            // TABLE_TAB and duplicate the rows the worker already produced.
            if let Err(e) = self.submit_local(SysFamily::Index, batch) {
                // The +1 reached sys_indices but its directory/cache setup failed
                // and was never broadcast. Submit the matching -1 locally to
                // reverse the storage write and any partial cache updates;
                // otherwise the next boot's replay opens a missing index
                // directory and crashes.
                let undo = idx_tab_row(index_id, table_id, packed_cols, &index_name, false, -1);
                self.rollback_index_registration(undo, index_id);
                return Err(e);
            }

            self.advance_sequence(SEQ_ID_INDICES, index_id - 1, index_id);
        }
        Ok(())
    }

    // -- Write helpers for system tables -----------------------------------

    #[cfg(test)]
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
            bb.put_u64(0); // is_serial (engine ColumnDef has no SERIAL marker)
            bb.end_row();
        }
        bb.finish()
    }

    #[cfg(test)]
    pub(crate) fn write_column_records(
        &mut self,
        owner_id: i64,
        kind: i64,
        col_defs: &[ColumnDef],
    ) -> Result<(), String> {
        let batch = self.build_col_batch(owner_id, kind, col_defs, 1);
        self.submit(SysFamily::Column, batch)
    }

    #[cfg(test)]
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
            self.submit(SysFamily::ViewDep, batch)?;
        }
        Ok(())
    }
}

/// True if a positive-weight row in a projected index chunk shares its leading
/// index key (the first `key_size` bytes of the index PK) with another row of
/// this chunk or any earlier chunk recorded in `seen`.
///
/// Compound-PK index layout: index PK is `(indexed_key…, src_pk…)`. Uniqueness
/// applies to the leading `indexed_key` span only — two rows differing only in
/// their source-PK suffix represent two source rows sharing the indexed value.
/// For a composite `UNIQUE (a, b, …)` the span is the sum of every promoted
/// column's width and can exceed 16 bytes, so the dedup token is a `PkBuf`
/// holding the raw span (a stack key, no per-row heap allocation), not a
/// truncating `u128`.
///
/// `seen` is caller-owned because the scan is chunked: cross-chunk duplicates
/// are only visible through state carried across calls. Shared by
/// `backfill_index` (fresh unique index) and `promote_index_to_unique`
/// (UNIQUE folded into an existing circuit) so both gate on the same predicate.
fn projected_chunk_has_dup_keys(projected: &Batch, key_size: usize, seen: &mut rustc_hash::FxHashSet<PkBuf>) -> bool {
    for row in 0..projected.count {
        let weight = projected.get_weight(row);
        if weight <= 0 {
            continue;
        }
        // Base-table scan chunks are consolidated: weight ≥ 2 is the same
        // (PK, payload) row inserted multiple times — that many live
        // instances of the same index key. NULL-valued rows never reach
        // here (batch_project_index skips them).
        if weight > 1 {
            return true;
        }
        let pk_bytes = projected.get_pk_bytes(row);
        if !seen.insert(PkBuf::from_bytes(&pk_bytes[..key_size])) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod dup_key_tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    /// Build a projected-index batch whose PK region is each supplied span
    /// (here the whole index PK; the dedup keys on the leading `key_size`).
    fn idx_batch(spans: &[[u8; 24]]) -> Batch {
        // Three U64 PK columns → a 24-byte composite span (> 16 bytes).
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0); 3], &[0, 1, 2]);
        let mut b = Batch::with_schema(schema, spans.len().max(1));
        for s in spans {
            b.ensure_row_capacity();
            b.extend_pk_bytes(s);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b
    }

    /// A >16-byte composite span is deduped on its FULL width: two spans sharing
    /// their leading 16 bytes but differing in the trailing 8 are distinct (the
    /// `u128` truncation this replaced would have falsely reported a duplicate),
    /// while an exact repeat is a duplicate.
    #[test]
    fn dup_keys_over_16_bytes_no_truncation() {
        let span = |tail: u64| {
            let mut s = [0u8; 24];
            s[..16].copy_from_slice(&[7u8; 16]); // shared leading 16 bytes
            s[16..].copy_from_slice(&tail.to_be_bytes());
            s
        };

        let mut seen = rustc_hash::FxHashSet::default();
        assert!(
            !projected_chunk_has_dup_keys(&idx_batch(&[span(1), span(2)]), 24, &mut seen),
            "distinct 24-byte spans sharing a 16-byte prefix are NOT duplicates",
        );

        let mut seen2 = rustc_hash::FxHashSet::default();
        assert!(
            projected_chunk_has_dup_keys(&idx_batch(&[span(1), span(1)]), 24, &mut seen2),
            "identical 24-byte spans are duplicates",
        );
    }

    /// `seen` carries cross-chunk state: a duplicate split across two chunk
    /// calls is caught on the second chunk.
    #[test]
    fn dup_keys_cross_chunk() {
        let span = |tail: u64| {
            let mut s = [0u8; 24];
            s[16..].copy_from_slice(&tail.to_be_bytes());
            s
        };
        let mut seen = rustc_hash::FxHashSet::default();
        assert!(!projected_chunk_has_dup_keys(&idx_batch(&[span(1)]), 24, &mut seen));
        assert!(
            projected_chunk_has_dup_keys(&idx_batch(&[span(1)]), 24, &mut seen),
            "the same span in a later chunk is a cross-chunk duplicate",
        );
    }
}
