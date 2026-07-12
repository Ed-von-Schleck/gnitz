//! Catalog id-registry — table / schema / index id allocation and lookup,
//! the `sys_columns` → `ColumnDef` / schema readers, batch field decoders,
//! and sequence advancement.

use super::*;

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
        let start_pk = pack_column_id(owner_id, 0);
        let end_pk = pack_column_id(owner_id + 1, 0);
        let mut cursor = self.sys_store(SysFamily::Column).open_cursor();
        // sys_columns has a single U64 PK; OPK == big-endian.
        cursor.seek_bytes(&start_pk.to_be_bytes());

        let mut defs = Vec::new();
        let mut expected: i64 = 0;
        while cursor.valid {
            let pk = cursor.current_key_narrow() as u64;
            if pk >= end_pk {
                break;
            }
            if cursor.current_weight > 0 {
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
                defs.push(ColumnDef {
                    name: cursor_read_string(&cursor, COLTAB_COL_NAME),
                    type_code: cursor_read_u64(&cursor, COLTAB_COL_TYPE_CODE) as u8,
                    is_nullable: cursor_read_u64(&cursor, COLTAB_COL_IS_NULLABLE) != 0,
                    fk_table_id: cursor_read_u64(&cursor, COLTAB_COL_FK_TABLE_ID) as i64,
                    fk_col_idx: cursor_read_u64(&cursor, COLTAB_COL_FK_COL_IDX) as u32,
                    is_hidden: cursor_read_u64(&cursor, COLTAB_COL_IS_HIDDEN) != 0,
                });
            }
            cursor.advance();
        }
        Ok(defs)
    }

    /// Column definitions for `owner_id`, cached until the next COL_TAB delta
    /// (`invalidate_col_names` fires on every path — live, replay, ddl_sync,
    /// rollback). One COL_TAB scan also fills the three derived views
    /// (`col_names`, `col_names_bytes`, `col_hidden`).
    pub(crate) fn read_column_defs(&mut self, owner_id: i64) -> Rc<Vec<ColumnDef>> {
        if let Some(defs) = self.caches.col_defs.get(&owner_id) {
            return defs.clone();
        }
        self.fill_column_caches(owner_id)
    }

    /// Scan COL_TAB once for `owner_id` and fill the column-def cache plus its
    /// three derived views. Uses the infallible non-checking scan — the
    /// contiguity-checking form stays a direct storage scan at its precheck
    /// call sites.
    pub(crate) fn fill_column_caches(&mut self, owner_id: i64) -> Rc<Vec<ColumnDef>> {
        let defs = Rc::new(self.scan_column_defs(owner_id, false).unwrap());
        let mut hidden: u128 = 0;
        for (i, cd) in defs.iter().enumerate() {
            if cd.is_hidden {
                hidden |= 1 << i;
            }
        }
        let names: Vec<String> = defs.iter().map(|cd| cd.name.clone()).collect();
        let bytes: Vec<Vec<u8>> = names.iter().map(|n| n.as_bytes().to_vec()).collect();
        self.caches.col_hidden.insert(owner_id, hidden);
        self.caches.col_names.insert(owner_id, Rc::new(names));
        self.caches.col_names_bytes.insert(owner_id, Rc::new(bytes));
        self.caches.col_defs.insert(owner_id, defs.clone());
        defs
    }

    /// `dist_prefix_len` is the persisted distribution prefix length `k`
    /// (`0` = default = full PK; clamped by `new_with_dist`). The table-register
    /// path passes the decoded `k`; the view path passes the full-PK default
    /// (views are not distributed by a chosen key).
    pub(crate) fn build_schema_from_col_defs(
        &self,
        col_defs: &[ColumnDef],
        pk_cols: &[u32],
        dist_prefix_len: usize,
    ) -> SchemaDescriptor {
        assert!(
            col_defs.len() <= crate::schema::MAX_COLUMNS,
            "build_schema_from_col_defs: too many columns ({}) for entity (type_codes: {:?})",
            col_defs.len(),
            col_defs.iter().map(|c| c.type_code).collect::<Vec<_>>(),
        );
        let mut cols = [zero_col(); crate::schema::MAX_COLUMNS];
        for (i, cd) in col_defs.iter().enumerate() {
            cols[i] = SchemaColumn::new(cd.type_code, if cd.is_nullable { 1 } else { 0 });
        }
        SchemaDescriptor::new_with_dist(&cols[..col_defs.len()], pk_cols, dist_prefix_len)
    }

    // -- Registry query methods -----------------------------------------------

    pub fn has_id(&self, table_id: i64) -> bool {
        self.dag.tables.contains_key(&table_id)
    }

    // The following registry getters are exercised only by the catalog tests;
    // production code reads the caches/DAG entries directly.
    #[cfg(test)]
    pub(crate) fn get_schema(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id).map(|e| e.schema)
    }

    #[cfg(test)]
    pub(crate) fn get_schema_name_by_id(&self, schema_id: i64) -> &str {
        self.caches
            .schema_by_id
            .get(&schema_id)
            .map(|s| s.as_str())
            .unwrap_or("")
    }

    #[cfg(test)]
    pub(crate) fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    #[cfg(test)]
    pub(crate) fn get_schema_id(&self, name: &str) -> i64 {
        self.caches.schema_by_name.get(name).copied().unwrap_or(-1)
    }

    /// Number of live member relations (tables + views) in schema `sid`.
    /// Reads the per-schema caches `apply_schema_of` maintains in production
    /// (`tables_by_schema` / `views_by_schema`), each of which drops its set once
    /// it empties — so this returns 0 exactly when no member remains. The
    /// engine-side non-empty-schema DROP guard (`precheck_sys_ingest`) and the
    /// `#[cfg(test)]` `schema_is_empty` share this one probe.
    pub(crate) fn schema_member_count(&self, sid: i64) -> usize {
        self.caches.tables_by_schema.get(&sid).map_or(0, |s| s.len())
            + self.caches.views_by_schema.get(&sid).map_or(0, |s| s.len())
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
    // burns the id (harmless — recovery maxes over positive rows; gaps are
    // fine), and the unconditional advance keeps every retract matched: a
    // skipped advance would leave the NEXT advance's retract unmatched — a
    // net −1 ghost row violating base-table positivity.
    pub fn allocate_schema_id(&mut self) -> i64 {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        self.advance_sequence(SEQ_ID_SCHEMAS, sid - 1, sid);
        sid
    }

    pub fn allocate_table_id(&mut self) -> i64 {
        let tid = self.next_table_id;
        self.next_table_id += 1;
        self.advance_sequence(SEQ_ID_TABLES, tid - 1, tid);
        tid
    }

    pub fn allocate_index_id(&mut self) -> i64 {
        let iid = self.next_index_id;
        self.next_index_id += 1;
        self.advance_sequence(SEQ_ID_INDICES, iid - 1, iid);
        iid
    }

    pub fn get_qualified_name(&self, table_id: i64) -> Option<(&str, &str)> {
        self.caches
            .entity_by_id
            .get(&table_id)
            .map(|(s, t)| (s.as_str(), t.as_str()))
    }

    #[cfg(test)]
    pub(crate) fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<i64> {
        let qualified = format!("{schema_name}.{table_name}");
        self.caches.entity_by_qname.get(&qualified).copied()
    }

    #[cfg(test)]
    pub(crate) fn has_index_by_name(&self, name: &str) -> bool {
        self.caches.index_by_name.contains_key(name)
    }

    // -- Sequence management -----------------------------------------------

    pub(crate) fn advance_sequence(&mut self, seq_id: i64, old_val: i64, new_val: i64) {
        let batch = build_seq_delta(seq_id, old_val, new_val);
        // The caller has already handed the allocated id to memory; un-allocating
        // is impossible, and replying an error while keeping the bump would
        // re-issue the id after restart. Fail-stop — same as the serial-range
        // sequence ingest in `executor.rs`.
        if let Err(e) = self.sys_store_mut(SysFamily::Sequence).ingest_borrowed_batch(&batch) {
            crate::gnitz_fatal_abort!(
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
    /// On an absent sequence `hw = 0`: the retract of a non-existent `(seq_id, 0)`
    /// row nets to zero under consolidation, leaving exactly the `+1` insert —
    /// the catalog's seed-on-first-use pattern. `saturating_add` avoids an i64
    /// debug-panic at the (unreachable) ~2^63 boundary; the client overflow guard
    /// rejects the id long before then.
    pub fn reserve_user_sequence(&mut self, seq_id: i64, count: i64) -> (i64, Batch, u64) {
        let hw = self.user_sequences.get(&seq_id).copied().unwrap_or(0);
        let new_hw = hw.saturating_add(count);
        self.user_sequences.insert(seq_id, new_hw);
        (
            hw + 1,
            build_seq_delta(seq_id, hw, new_hw),
            self.sys_store(SysFamily::Sequence).current_lsn(),
        )
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
        let new = self.committed_generation + 1;
        // Retract the old high-water, insert the new. On the first bump the
        // retract of a non-existent (4, 0) row nets to zero (seed-on-first-use).
        // `advance_sequence` fatal-aborts on ingest failure.
        self.advance_sequence(SEQ_ID_CHECKPOINT_GEN, self.committed_generation as i64, new as i64);
        self.committed_generation = new;
        crate::foundation::worker_ctx::set_committed_generation(new);
        // The row must be shard-durable before the SAL reset that follows
        // discards the memtable copy. A flush failure here is fatal: resetting
        // the SAL on a swallowed failure destroys the only durable copy.
        if let Err(e) = self.flush_all_system_tables() {
            crate::gnitz_fatal_abort!("checkpoint generation flush failed: {} — aborting before SAL reset", e);
        }
        new
    }

    /// Durably advance the checkpoint generation by one **at recovery start**,
    /// WITHOUT publishing it to `worker_ctx`. The non-windowed recovery resets the
    /// SAL before its master-driven tick sweep, so a crash after the child boot
    /// flush + reset but before `boot_checkpoint`'s own gen bump would otherwise
    /// leave base = `G + tail` durable while every un-checkpointed view is cleanly
    /// stamped `G` (→ silently resumed as stale) with the SAL already consumed.
    /// Advancing the durable `_sequences` generation from the recovered `G` to
    /// `G+1` here means any crash from now until `boot_checkpoint` leaves durable
    /// gen ≥ `G+1` while those views are stamped `G`, so the per-partition
    /// generation verdict forces a full rebuild. Every crash window is covered:
    /// windows inside `boot_checkpoint` by its own gen bump before the ephemeral
    /// stamp, the reset→boot_checkpoint gap by this one.
    ///
    /// `worker_ctx::committed_generation()` is left at `G` on purpose: the resume
    /// load (`Table::new`) and the boot verdict both compare view manifests
    /// against `G`, so a clean restart still resumes. `self.committed_generation`
    /// IS advanced to `G+1` so `boot_checkpoint`'s `bump_checkpoint_generation`
    /// retracts `G+1` (not `G`) and stamps the resumed+rebuilt views at `G+2`,
    /// keeping `_sequences` clean (each generation row nets to zero but the latest).
    ///
    /// Monotonic, so recovery reads it back through `recover_sequences`' `.max()`
    /// arm — no toggle, no stuck-dirty residue in the no-unique-PK `_sequences`.
    pub fn recovery_start_generation_bump(&mut self) -> Result<(), String> {
        let g = self.committed_generation;
        // On a fresh DB `g == 0` and the retract of a non-existent `(4, 0)` row
        // nets to zero (seed-on-first-use), leaving exactly `(4, 1)`.
        self.advance_sequence(SEQ_ID_CHECKPOINT_GEN, g as i64, (g + 1) as i64);
        self.committed_generation = g + 1;
        // Durable before the SAL reset that follows discards the memtable copy.
        self.flush_all_system_tables()
    }

    /// Record the cluster topology (`worker_count << 32 | STATE_FORMAT`) in
    /// `_sequences` (seq id 5). Idempotent: a same-topology restart already
    /// holds the current value, so the write is skipped. Does not flush — the
    /// sole caller (`boot_checkpoint`) bumps the checkpoint generation right
    /// after, and that flush carries this row to the same shard.
    pub fn record_topology(&mut self, worker_count: u32) {
        let value = ((worker_count as u64) << 32) | crate::storage::STATE_FORMAT as u64;
        if self.recorded_topology == value {
            return;
        }
        self.advance_sequence(SEQ_ID_TOPOLOGY, self.recorded_topology as i64, value as i64);
        self.recorded_topology = value;
    }
}

/// Raise a monotonic catalog id counter so the next allocation lands strictly
/// past `allocated` (the largest id known to be in use). Monotone and
/// idempotent — never lowers the counter, so a retract+reinsert replay is a
/// no-op. Scalar-counter analog of `observe_user_sequence`: single-sources the
/// "never re-issue a durably registered id after a crash lost the memtable-only
/// `advance_sequence`" invariant shared by the object-register replay paths
/// (`hook_{table,view,index}_register` and `apply_schema_by_id`, from the
/// fsync'd TABLE_TAB / VIEW_TAB / IDX_TAB / SCHEMA_TAB row) and
/// `recover_sequences` (the flushed `sys_sequences` scan). A free fn rather than
/// a method so callers can pass `&mut self.next_*_id` while `self` is otherwise
/// borrowed.
#[inline]
pub(super) fn raise_id_counter(counter: &mut i64, allocated: i64) {
    *counter = (*counter).max(allocated + 1);
}

/// Build the `sys_sequences` retract-old + insert-new delta for one sequence.
/// Shared by `advance_sequence` (ingests to the memtable) and
/// `reserve_user_sequence` (returns it for the durable commit).
fn build_seq_delta(seq_id: i64, old_val: i64, new_val: i64) -> Batch {
    let schema = SysFamily::Sequence.schema();
    let mut bb = BatchBuilder::new(schema);
    // Retract old
    bb.begin_row(seq_id as u128, -1);
    bb.put_u64(old_val as u64);
    bb.end_row();
    // Insert new
    bb.begin_row(seq_id as u128, 1);
    bb.put_u64(new_val as u64);
    bb.end_row();
    bb.finish()
}
