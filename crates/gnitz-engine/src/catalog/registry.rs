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
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_sequences, &batch);
    }
}
