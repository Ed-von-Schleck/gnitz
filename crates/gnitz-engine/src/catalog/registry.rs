//! Catalog id-registry — table / schema / index id allocation and lookup, the
//! `sys_columns` → `ColumnDef` readers, and the `sys_sequences` writes: user
//! SERIAL ranges, the object-id high-waters, and the checkpoint-generation and
//! topology records that ride the same family.

use super::*;

/// The one place COL_TAB column records become a `SchemaDescriptor`, so it is
/// where their admissibility is enforced — not at the callers.
/// [`check_col_defs`] rather than `assert!`: the rows are catalog data a corrupt
/// SAL can carry, and `hook_column_alter` plus the register hooks under boot
/// replay / worker `ddl_sync` run on paths that skip the DDL precheck entirely.
///
/// `placement` is where the relation's rows live: the table-register path folds
/// it out of `TABLE_TAB.flags`, the view path out of its sources' own stamped
/// placements (`DagEngine::view_placement`).
pub(crate) fn build_schema_from_col_defs(
    col_defs: &[ColumnDef],
    pk_cols: &[u32],
    placement: Placement,
) -> Result<SchemaDescriptor, String> {
    check_col_defs(col_defs)?;
    let cols: Vec<SchemaColumn> = col_defs
        .iter()
        .map(|cd| SchemaColumn::new(cd.type_code, cd.is_nullable as u8))
        .collect();
    Ok(SchemaDescriptor::new_with_placement(&cols, pk_cols, placement))
}

impl CatalogEngine {
    // -- Iteration helpers ----------------------------------------------------

    /// Collect all user table IDs.
    pub fn iter_user_table_ids(&self) -> Vec<i64> {
        self.dag
            .tables
            .keys()
            .filter(|&&tid| tid >= FIRST_USER_TABLE_ID)
            .copied()
            .collect()
    }

    // -- Read column definitions from sys_columns --------------------------

    /// Scan sys_columns for every positive-weight column record owned by
    /// `owner_id`, in column-index order. When `check_contiguity` is set the
    /// column indices (lower 9 bits of the packed PK) must run 0,1,2,… with no
    /// gap or duplicate — a gap would silently mismap columns in
    /// `build_schema_from_col_defs`, so the create-precheck path rejects it.
    /// The non-checking form is infallible by construction.
    pub(crate) fn scan_column_defs(&self, owner_id: i64, check_contiguity: bool) -> Result<Vec<ColumnDef>, String> {
        let (start_pk, end_pk) = column_id_band(owner_id);
        let mut cursor = self.sys_store(SysFamily::Column).open_cursor();
        // sys_columns has a single U64 PK; OPK == big-endian. The range clamp
        // exhausts the cursor at `end_pk`, so the walk needs no bound test.
        cursor.seek_range_bytes(&start_pk.to_be_bytes(), Some(&end_pk.to_be_bytes()));

        let mut defs = Vec::new();
        let mut expected: i64 = 0;
        while cursor.valid {
            if cursor.current_weight > 0 {
                let pk = cursor.current_key_narrow() as u64;
                if check_contiguity {
                    let actual = gnitz_wire::unpack_col_id(pk).1 as i64;
                    if actual != expected {
                        return Err(format!(
                            "entity (owner_id={owner_id}): column records are non-contiguous; \
                             expected index {expected}, got {actual}"
                        ));
                    }
                    expected += 1;
                }
                defs.push(read_col_tab_cursor_row(&cursor));
            }
            cursor.advance();
        }
        Ok(defs)
    }

    /// Column definitions for `owner_id`, cached until the next COL_TAB delta
    /// (`invalidate_col_names` fires on every path — live, replay, ddl_sync,
    /// rollback). Returns an `Rc` snapshot: callers routinely touch the catalog
    /// while holding it, so a borrow would not do. Uses the infallible
    /// non-checking scan — the contiguity-checking form stays a direct storage
    /// scan at its precheck call sites.
    pub(crate) fn read_column_defs(&mut self, owner_id: i64) -> Rc<Vec<ColumnDef>> {
        if let Some(defs) = self.caches.col_defs.get(&owner_id) {
            return defs.clone();
        }
        let defs = Rc::new(self.scan_column_defs(owner_id, false).unwrap());
        self.caches.col_defs.insert(owner_id, defs.clone());
        defs
    }

    // -- Registry query methods -----------------------------------------------

    pub fn has_id(&self, table_id: i64) -> bool {
        self.dag.tables.contains_key(&table_id)
    }

    pub(crate) fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    #[cfg(test)]
    pub(crate) fn get_schema_id(&self, name: &str) -> i64 {
        self.caches.schema_by_name.get(name).copied().unwrap_or(-1)
    }

    /// Number of live member relations (tables + views) in schema `sid`. Reads
    /// the `members_by_schema` cache `apply_schema_members` maintains, which
    /// drops its set once it empties — so this returns 0 exactly when no member
    /// remains. The engine-side non-empty-schema DROP guard (`precheck_family`)
    /// and the `#[cfg(test)]` `schema_is_empty` share this one probe.
    pub(crate) fn schema_member_count(&self, sid: i64) -> usize {
        self.caches.members_by_schema.get(&sid).map_or(0, |s| s.len())
    }

    #[cfg(test)]
    pub(crate) fn schema_is_empty(&self, schema_name: &str) -> bool {
        match self.caches.schema_by_name.get(schema_name) {
            Some(&sid) => self.schema_member_count(sid) == 0,
            None => true,
        }
    }

    // Each object-id allocation owns its durability half: the id is handed to
    // memory AND durably advanced in `sys_sequences` as one operation, so no
    // caller can take an id without the advance. A create that later fails
    // burns the id — harmless, since recovery maxes over positive rows and gaps
    // are fine.
    pub fn allocate_schema_id(&mut self) -> i64 {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        self.advance_sequence(SEQ_ID_SCHEMAS, sid);
        sid
    }

    /// Allocate the next durable relation id (tables and views share this
    /// counter; indices have their own).
    ///
    /// Panics on reaching `RELATION_ID_CEILING` rather than issuing an id at or
    /// above it. Reaching it needs 2^31 durable CREATEs, so this is a tripwire,
    /// not a live limit — and it is the *secondary* guard: `precheck_family`
    /// rejects a ceiling id at the point one enters `dag.tables`, which covers the
    /// caller-chosen ids the register hooks `raise_id_counter` from and which
    /// never pass through here.
    pub fn allocate_table_id(&mut self) -> i64 {
        let tid = self.next_table_id;
        assert!(
            tid < sys_tables::RELATION_ID_CEILING,
            "durable relation ids exhausted: {tid} would reach the relation-id ceiling"
        );
        self.next_table_id += 1;
        self.advance_sequence(SEQ_ID_TABLES, tid);
        tid
    }

    pub fn allocate_index_id(&mut self) -> i64 {
        let iid = self.next_index_id;
        self.next_index_id += 1;
        self.advance_sequence(SEQ_ID_INDICES, iid);
        iid
    }

    pub fn get_qualified_name(&self, table_id: i64) -> Option<(&str, &str)> {
        self.caches
            .entity_by_id
            .get(&table_id)
            .map(|(s, t)| (s.as_str(), t.as_str()))
    }

    /// The qualified `(schema, name)` of `table_id`, or `("?", "?")` when the
    /// catalog has no entry — the one fallback every constraint-violation
    /// message renders.
    pub(crate) fn qualified_name_or_unknown(&self, table_id: i64) -> (&str, &str) {
        self.get_qualified_name(table_id).unwrap_or(("?", "?"))
    }

    /// The entity id registered under a canonical `"schema.relation"` key.
    pub(crate) fn entity_id_by_qname(&self, qname: &str) -> Option<i64> {
        self.caches.entity_by_qname.get(qname).copied()
    }

    #[cfg(test)]
    pub(crate) fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<i64> {
        self.entity_id_by_qname(&format!("{schema_name}.{table_name}"))
    }

    #[cfg(test)]
    pub(crate) fn has_index_by_name(&self, name: &str) -> bool {
        self.caches.index_by_name.contains_key(name)
    }

    // -- Sequence management -----------------------------------------------

    pub(crate) fn advance_sequence(&mut self, seq_id: i64, new_val: i64) {
        let batch = self.build_seq_delta(seq_id, new_val);
        // The caller has already handed the allocated id to memory; un-allocating
        // is impossible, and replying an error while keeping the bump would
        // re-issue the id after restart. Fail-stop — same as the serial-range
        // sequence ingest in `executor.rs`.
        if let Err(e) = self.sys_store_mut(SysFamily::Sequence).ingest_borrowed_batch(&batch) {
            gnitz_fatal_abort!(
                "sys_sequences ingest (object id advance, seq_id={}) failed: {} \
                 — allocated id would be reissued after restart; aborting",
                seq_id,
                e,
            );
        }
    }

    /// Reserve `count` ids for a user-table SERIAL sequence. Returns
    /// `(base, delta, zone_floor)` where the reserved range is
    /// `[base, base + count)` and `delta` is the `sys_sequences` retract+insert
    /// the caller must durably persist (via the DDL SAL commit path). Unlike
    /// `advance_sequence` this updates the in-memory `user_sequences` high-water
    /// synchronously — that map is the source of truth for live allocation and,
    /// because the caller holds `catalog_rwlock.write()`, its `&mut self` also
    /// serializes concurrent reserves — and returns the delta rather than
    /// writing the memtable itself.
    ///
    /// `zone_floor` is `sys_sequences`' own `current_lsn` — the ONE family this
    /// delta writes (its `hook_sequence_register` folds a pure in-memory map
    /// with no cascade into other families), and thus the only counter the
    /// caller's durable zone LSN must dominate: un-pinned object-id
    /// `advance_sequence`s drift it, and recovery dedups per family
    /// (`msg.lsn <= flushed[target]`). Computed here so that fact lives beside
    /// the code that builds the delta.
    ///
    /// On an absent sequence `hw = 0` and the delta is the bare `+1` insert.
    /// `saturating_add` avoids an i64 debug-panic at the (unreachable) ~2^63
    /// boundary; the client overflow guard rejects the id long before then.
    pub fn reserve_user_sequence(&mut self, seq_id: i64, count: i64) -> (i64, Batch, u64) {
        let hw = self.user_sequences.get(&seq_id).copied().unwrap_or(0);
        let new_hw = hw.saturating_add(count);
        let delta = self.build_seq_delta(seq_id, new_hw);
        self.user_sequences.insert(seq_id, new_hw);
        (hw + 1, delta, self.sys_store(SysFamily::Sequence).current_lsn())
    }

    /// Fold an observed `sys_sequences` high-water into `user_sequences`,
    /// ignoring catalog sequences (`seq_id < FIRST_USER_TABLE_ID`, which recover
    /// via the object-id hooks). Monotone — never lowers an existing high-water.
    /// Single-sources the "what is a user sequence" threshold shared by
    /// `hook_sequence_register` (live and SAL-replayed advances) and
    /// `recover_sequences` (the flushed-shard scan).
    pub(super) fn observe_user_sequence(&mut self, seq_id: i64, high_water: i64) {
        if seq_id < FIRST_USER_TABLE_ID {
            return;
        }
        let e = self.user_sequences.entry(seq_id).or_insert(0);
        *e = (*e).max(high_water);
    }

    // -- Checkpoint records -------------------------------------------------

    /// Bump the committed checkpoint generation, durably record it in
    /// `_sequences` (seq id 4), and publish it to `worker_ctx` so every manifest
    /// this master publishes from now on carries the new stamp. Returns the new
    /// generation. Synchronous (memtable ingest + blocking flush), so it runs as
    /// an atomic block between the committer's checkpoint steps with no
    /// interleaving. From this instant every existing Rederive manifest is stale;
    /// a crash below rebuilds views instead of silently staleifying them.
    pub fn bump_checkpoint_generation(&mut self) -> u64 {
        let new = self.durable_generation + 1;
        // `advance_sequence` fatal-aborts on ingest failure.
        self.advance_sequence(SEQ_ID_CHECKPOINT_GEN, new as i64);
        self.durable_generation = new;
        self.set_resume_generation(new);
        // The row must be shard-durable before the SAL reset that follows
        // discards the memtable copy. A flush failure here is fatal: resetting
        // the SAL on a swallowed failure destroys the only durable copy.
        if let Err(e) = self.flush_all_system_tables() {
            gnitz_fatal_abort!("checkpoint generation flush failed: {} — aborting before SAL reset", e);
        }
        new
    }

    /// Durably advance the checkpoint generation by one **at recovery start**,
    /// WITHOUT publishing it to `worker_ctx`. This closes the
    /// reset→`boot_checkpoint` crash window: recovery resets the SAL before its
    /// master-driven tick sweep, so a crash in that gap would otherwise leave the
    /// base durable at `G + tail` while every un-checkpointed view is cleanly
    /// stamped `G` and would silently resume as stale. With the durable
    /// generation at `G+1`, the per-child verdict forces a rebuild instead.
    ///
    /// `worker_ctx::committed_generation()` stays at `G` on purpose: the resume
    /// load and the boot verdict both compare view manifests against it, so a
    /// clean restart still resumes. `self.durable_generation` IS advanced, so the
    /// next bump retracts `G+1` rather than `G`.
    pub fn recovery_start_generation_bump(&mut self) -> Result<(), String> {
        let g = self.durable_generation;
        self.advance_sequence(SEQ_ID_CHECKPOINT_GEN, (g + 1) as i64);
        self.durable_generation = g + 1;
        // Durable before the SAL reset that follows discards the memtable copy.
        self.flush_all_system_tables()
    }

    /// The one writer of the resume generation: the field and the process-global
    /// mirror every no-catalog `Table::new` caller reads always move together.
    pub(crate) fn set_resume_generation(&mut self, g: u64) {
        self.resume_generation = g;
        crate::foundation::worker_ctx::set_committed_generation(g);
    }

    /// True when this boot's `(worker count, STATE_FORMAT)` is the one the
    /// persisted derived state was written under. Half of every resume verdict:
    /// a change on either axis invalidates every rederived relation regardless
    /// of what generation its manifest carries.
    pub(crate) fn topology_matches(&self) -> bool {
        self.recorded_topology == crate::storage::topology_word(self.num_workers)
    }

    /// Build the `sys_sequences` delta that moves one sequence to `new_val`: a
    /// `-1` copy of whatever row is live at `seq_id` (nothing, on first use)
    /// followed by the `+1` insert. Retracting the *live* row rather than a
    /// caller-guessed previous value is what keeps `_sequences` free of net `-1`
    /// ghosts — it has no `enforce_unique_pk`, so an unmatched retraction would
    /// persist forever and violate base-table positivity. Shared by
    /// `advance_sequence` (ingests to the memtable) and `reserve_user_sequence`
    /// (returns it for the durable commit).
    fn build_seq_delta(&self, seq_id: i64, new_val: i64) -> Batch {
        let schema = SysFamily::Sequence.schema();
        let mut batch = retract_single_row(self.sys_store(SysFamily::Sequence), &schema, seq_id as u128);
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(seq_id as u128, 1);
        bb.put_u64(new_val as u64);
        bb.end_row();
        let insert = bb.finish();
        batch.append_batch(&insert, 0, insert.count);
        batch
    }

    /// Record the cluster topology (`worker_count << 32 | STATE_FORMAT`) in
    /// `_sequences` (seq id 5). Idempotent: a same-topology restart already
    /// holds the current value, so the write is skipped. Does not flush — the
    /// sole caller (`boot_checkpoint`) bumps the checkpoint generation right
    /// after, and that flush carries this row to the same shard.
    pub fn record_topology(&mut self, worker_count: u32) {
        let value = crate::storage::topology_word(worker_count);
        if self.recorded_topology == value {
            return;
        }
        self.advance_sequence(SEQ_ID_TOPOLOGY, value as i64);
        self.recorded_topology = value;
    }
}

/// Raise a monotonic catalog id counter so the next allocation lands strictly
/// past `allocated`, the largest id known to be in use. Monotone and idempotent,
/// so a retract+reinsert replay is a no-op.
///
/// This is what stops a durably registered id from being re-issued after a crash
/// lost the memtable-only `advance_sequence`: every path that observes an id in
/// use — the register hooks, `apply_schema_caches`, and `recover_sequences`'
/// flushed-shard scan — raises the counter through here. A free fn so callers can
/// pass `&mut self.next_*_id` while `self` is otherwise borrowed.
#[inline]
pub(super) fn raise_id_counter(counter: &mut i64, allocated: i64) {
    *counter = (*counter).max(allocated + 1);
}
