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
        let mut cursor = self.sys_columns.open_cursor();
        // sys_columns has a single U64 PK; OPK == big-endian.
        cursor.cursor.seek_bytes(&start_pk.to_be_bytes());

        let mut defs = Vec::new();
        let mut expected: i64 = 0;
        while cursor.cursor.valid {
            let pk = cursor.cursor.current_key_narrow() as u64;
            if pk >= end_pk {
                break;
            }
            if cursor.cursor.current_weight > 0 {
                if check_contiguity {
                    // pack_column_id layout: owner_id << 9 | col_idx (lower 9 bits).
                    let actual = (pk & 0x1FF) as i64;
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
                });
            }
            cursor.cursor.advance();
        }
        Ok(defs)
    }

    pub(crate) fn read_column_defs(&self, owner_id: i64) -> Vec<ColumnDef> {
        self.scan_column_defs(owner_id, false).unwrap()
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

    // -- Batch field readers -----------------------------------------------

    pub(crate) fn read_batch_u64(&self, batch: &Batch, row: usize, payload_col: usize) -> u64 {
        let off = row * 8;
        let col = batch.col_data(payload_col);
        if off + 8 > col.len() {
            return 0;
        }
        u64::from_le_bytes(col[off..off + 8].try_into().unwrap_or([0; 8]))
    }

    pub(crate) fn read_batch_string(&self, batch: &Batch, row: usize, payload_col: usize) -> String {
        let off = row * 16;
        let data = batch.col_data(payload_col);
        if off + 16 > data.len() {
            return String::new();
        }
        let st: [u8; 16] = data[off..off + 16].try_into().unwrap_or([0; 16]);
        let bytes = crate::schema::try_decode_german_string(&st, &batch.blob).unwrap_or_default();
        String::from_utf8(bytes).unwrap_or_default()
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

    pub fn allocate_schema_id(&mut self) -> i64 {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        sid
    }

    pub fn allocate_table_id(&mut self) -> i64 {
        let tid = self.next_table_id;
        self.next_table_id += 1;
        tid
    }

    pub fn allocate_index_id(&mut self) -> i64 {
        let iid = self.next_index_id;
        self.next_index_id += 1;
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

    pub fn advance_sequence(&mut self, seq_id: i64, old_val: i64, new_val: i64) {
        let batch = build_seq_delta(seq_id, old_val, new_val);
        ingest_batch_into(&mut self.sys_sequences, &batch);
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
            self.sys_sequences.current_lsn,
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
    let schema = seq_tab_schema();
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
