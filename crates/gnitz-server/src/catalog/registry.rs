//! Catalog id-registry — table / schema / index id allocation and lookup, the
//! `sys_columns` → `ColumnDef` readers, and the `sys_sequences` writes: user
//! SERIAL ranges, the object-id high-waters, and the checkpoint-generation and
//! topology records that ride the same family.

use super::*;

/// The one place COL_TAB column records become a `SchemaDescriptor`, and total
/// over every `(col_defs, pk_cols)` pair: it rejects everything
/// `new_with_placement` would `assert!` on, so no caller can abort the process
/// with catalog data a corrupt SAL carried. That matters because
/// `hook_column_alter` and the register hooks under boot replay / worker
/// `ddl_sync` all skip the DDL precheck — and because the ALTER path pairs an
/// already-built descriptor's PK list with freshly re-read column records, which
/// a `DROP NOT NULL` on a PK column would have made inadmissible.
///
/// `placement` is where the relation's rows live: the table-register path folds
/// it out of `TABLE_TAB.flags`, the view path out of its sources' own stamped
/// placements (`DagEngine::view_placement`).
pub(in crate::catalog) fn build_schema_from_col_defs(
    col_defs: &[ColumnDef],
    pk_cols: &[u32],
    placement: Placement,
) -> Result<SchemaDescriptor, String> {
    check_col_defs(col_defs)?;
    validate_pk_against_cols(col_defs, pk_cols)?;
    let cols: Vec<SchemaColumn> = col_defs
        .iter()
        .map(|cd| SchemaColumn::new(cd.type_code, cd.is_nullable as u8))
        .collect();
    Ok(SchemaDescriptor::new_with_placement(&cols, pk_cols, placement))
}

impl CatalogEngine {
    // -- Read column definitions from sys_columns --------------------------

    /// Scan sys_columns for every positive-weight column record owned by
    /// `owner_id`, in column-index order. When `check_contiguity` is set the
    /// column indices (lower 9 bits of the packed PK) must run 0,1,2,… with no
    /// gap or duplicate — a gap would silently mismap columns in
    /// `build_schema_from_col_defs`, so the create-precheck path rejects it.
    /// The non-checking form is infallible by construction.
    pub(in crate::catalog) fn scan_column_defs(
        &self,
        owner_id: i64,
        check_contiguity: bool,
    ) -> Result<Vec<ColumnDef>, String> {
        let (start_pk, end_pk) = column_id_band(owner_id);
        // sys_columns has a single U64 PK; OPK == big-endian. The range clamp
        // exhausts the cursor at `end_pk`, so the walk needs no bound test.
        let (start, end) = (start_pk.to_be_bytes(), end_pk.to_be_bytes());
        let mut cursor = self
            .sys_store(SysFamily::Column)
            .open_cursor_in_range(&start, Some(&end));
        cursor.seek_range_bytes(&start, Some(&end));

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
                let (src, row) = cursor.current_row_source();
                defs.push(read_col_tab_row(src, row));
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

    /// The id this catalog holds for schema `name`, or `None` when it holds no
    /// such schema.
    pub(in crate::catalog) fn schema_id(&self, name: &str) -> Option<i64> {
        self.caches.schema_by_name.get(name).copied()
    }

    pub(crate) fn has_schema(&self, name: &str) -> bool {
        self.schema_id(name).is_some()
    }

    /// Number of live member relations (tables + views) in schema `sid`. Reads
    /// the `members_by_schema` cache `apply_schema_members` maintains, which
    /// drops its set once it empties — so this returns 0 exactly when no member
    /// remains. The engine-side non-empty-schema DROP guard (`precheck_family`)
    /// and the `#[cfg(test)]` `schema_is_empty` share this one probe.
    pub(in crate::catalog) fn schema_member_count(&self, sid: i64) -> usize {
        self.caches.members_by_schema.get(&sid).map_or(0, |s| s.len())
    }

    #[cfg(test)]
    pub(in crate::catalog) fn schema_is_empty(&self, schema_name: &str) -> bool {
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
    pub(crate) fn allocate_schema_id(&mut self) -> Result<i64, StorageError> {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        self.advance_sequence(SEQ_ID_SCHEMAS, sid)?;
        Ok(sid)
    }

    /// Allocate the next durable relation id (tables and views share this
    /// counter; indices have their own).
    ///
    /// Panics on reaching `RELATION_ID_CEILING` rather than issuing an id at or
    /// above it. Reaching it needs 2^31 durable CREATEs, so this is a tripwire,
    /// not a live limit — and it is the *secondary* guard: `precheck_family`
    /// rejects a ceiling id at the point one enters the registry, which covers the
    /// caller-chosen ids the register hooks `raise_id_counter` from and which
    /// never pass through here.
    #[cfg(test)]
    pub(crate) fn allocate_table_id(&mut self) -> Result<i64, StorageError> {
        self.allocate_table_ids(1)
    }

    /// Allocate a contiguous run of `count` relation ids, returning its base: the
    /// caller owns `[base, base + count)`. One durable sequence advance for the
    /// whole run, which is what lets a view chain draw every segment's id in one
    /// round trip instead of one per segment.
    ///
    /// `count` is clamped to at least 1 — the run length is a client-supplied
    /// wire field, and a `0` would move the durable sequence *backwards*.
    pub(crate) fn allocate_table_ids(&mut self, count: u64) -> Result<i64, StorageError> {
        let base = self.next_table_id;
        let last = base + count.max(1) as i64 - 1;
        assert!(
            last < sys_tables::RELATION_ID_CEILING,
            "durable relation ids exhausted: {last} would reach the relation-id ceiling"
        );
        self.next_table_id = last + 1;
        self.advance_sequence(SEQ_ID_TABLES, last)?;
        Ok(base)
    }

    pub(in crate::catalog) fn allocate_index_id(&mut self) -> Result<i64, StorageError> {
        self.allocate_index_ids(1)
    }

    /// [`Self::allocate_table_ids`] for the index-id counter, under the same
    /// clamp.
    pub(crate) fn allocate_index_ids(&mut self, count: u64) -> Result<i64, StorageError> {
        let base = self.next_index_id;
        let last = base + count.max(1) as i64 - 1;
        self.next_index_id = last + 1;
        self.advance_sequence(SEQ_ID_INDICES, last)?;
        Ok(base)
    }

    pub(crate) fn get_qualified_name(&self, table_id: i64) -> Option<(&str, &str)> {
        self.caches
            .entity_by_id
            .get(&table_id)
            .map(|(s, t)| (s.as_str(), t.as_str()))
    }

    /// The qualified `(schema, name)` of `table_id`, or `("?", "?")` when the
    /// catalog has no entry — the one fallback every constraint-violation
    /// message renders.
    pub(in crate::catalog) fn qualified_name_or_unknown(&self, table_id: i64) -> (&str, &str) {
        self.get_qualified_name(table_id).unwrap_or(("?", "?"))
    }

    /// The entity id registered under a canonical `"schema.relation"` key.
    pub(crate) fn entity_id_by_qname(&self, qname: &str) -> Option<i64> {
        self.caches.entity_by_qname.get(qname).copied()
    }

    #[cfg(test)]
    pub(in crate::catalog) fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<i64> {
        self.entity_id_by_qname(&format!("{schema_name}.{table_name}"))
    }

    #[cfg(test)]
    pub(in crate::catalog) fn has_index_by_name(&self, name: &str) -> bool {
        self.caches.index_by_name.contains_key(name)
    }

    // -- Sequence management -----------------------------------------------

    /// `Err` means the bump did not reach the sequence store, so a restart would
    /// reissue whatever this call allocated. The caller must not go on to use it:
    /// every allocator above propagates, and the id it burned is harmless because
    /// recovery maxes over the live rows and gaps are fine.
    pub(in crate::catalog) fn advance_sequence(&mut self, seq_id: i64, new_val: i64) -> Result<(), StorageError> {
        let batch = self.build_seq_delta(seq_id, new_val);
        self.sys_store_mut(SysFamily::Sequence).ingest_owned_batch(batch)
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
    pub(crate) fn reserve_user_sequence(&mut self, seq_id: i64, count: i64) -> (i64, Batch, u64) {
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
    /// `_sequences` (seq id 4), and move this engine's resume generation onto it,
    /// so every manifest published from now on carries the new stamp. Returns the new
    /// generation. Synchronous (memtable ingest + blocking flush), so it runs as
    /// an atomic block between the committer's checkpoint steps with no
    /// interleaving.
    ///
    /// **A fence.** Nothing is written at generation `g` until `g` is durable, so
    /// a crash below leaves the durable generation at or ahead of every manifest
    /// and every record a caller keeps beside them. Each is honoured only at
    /// equality with [`Self::resume_generation`], so a half-finished checkpoint
    /// rebuilds instead of resuming something stale.
    pub(crate) fn bump_checkpoint_generation(&mut self) -> Result<u64, String> {
        let new = self.durable_generation + 1;
        self.advance_sequence(SEQ_ID_CHECKPOINT_GEN, new as i64)
            .map_err(|e| format!("sys_sequences ingest (checkpoint generation) failed: {e}"))?;
        self.durable_generation = new;
        self.registry.set_resume_generation(new);
        // The row must be shard-durable before the SAL reset that follows
        // discards the memtable copy: resetting the SAL on a swallowed failure
        // destroys the only durable copy, so the caller must not proceed to it.
        self.flush_all_system_tables()
            .map_err(|e| format!("checkpoint generation flush failed: {e}"))?;
        Ok(new)
    }

    /// Durably advance the checkpoint generation by one **at recovery start**,
    /// WITHOUT moving the resume generation. This closes the
    /// reset→`boot_checkpoint` crash window: recovery resets the SAL before its
    /// master-driven tick sweep, so a crash in that gap would otherwise leave the
    /// base durable at `G + tail` while every un-checkpointed view is cleanly
    /// stamped `G` and would silently resume as stale. With the durable
    /// generation at `G+1`, the per-child verdict forces a rebuild instead.
    ///
    /// [`Self::resume_generation`] stays at `G` on purpose: the resume load and
    /// the boot verdict both compare view manifests against it, so a clean
    /// restart still resumes. `self.durable_generation` IS advanced, so the next
    /// bump retracts `G+1` rather than `G`.
    pub(crate) fn recovery_start_generation_bump(&mut self) -> Result<(), String> {
        let g = self.durable_generation;
        self.advance_sequence(SEQ_ID_CHECKPOINT_GEN, (g + 1) as i64)
            .map_err(|e| format!("sys_sequences ingest (recovery generation bump) failed: {e}"))?;
        self.durable_generation = g + 1;
        // Durable before the SAL reset that follows discards the memtable copy.
        self.flush_all_system_tables()
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
        let mut batch = retract_pk_list(self.sys_store(SysFamily::Sequence), &schema, vec![seq_id as u128]);
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
    /// server's caller (`boot_checkpoint`) bumps the checkpoint generation right
    /// after, and that flush carries this row to the same shard.
    pub(crate) fn record_topology(&mut self, worker_count: u32) -> Result<(), StorageError> {
        let value = gnitz_store::storage::topology_word(worker_count);
        if self.registry.recorded_topology() == value {
            return Ok(());
        }
        self.advance_sequence(SEQ_ID_TOPOLOGY, value as i64)?;
        self.registry.set_recorded_topology(value);
        Ok(())
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
