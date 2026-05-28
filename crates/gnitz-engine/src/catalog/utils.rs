use super::*;

// ---------------------------------------------------------------------------
// BatchBuilder — construct Batch rows for system table mutations
// ---------------------------------------------------------------------------

/// Lightweight row-by-row builder for constructing Batch in Rust.
/// Operates on Batch directly.
pub(crate) struct BatchBuilder {
    pub(crate) batch: Batch,
    pub(crate) schema: SchemaDescriptor,
    // per-row state
    pub(crate) curr_null_word: u64,
    pub(crate) curr_col: usize,
}

impl BatchBuilder {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        let batch = Batch::with_schema(schema, 8);
        BatchBuilder {
            batch,
            schema,
            curr_null_word: 0,
            curr_col: 0,
        }
    }

    /// Begin a new row with the given PK and weight.
    pub(crate) fn begin_row(&mut self, pk: u128, weight: i64) {
        self.batch.ensure_row_capacity();
        self.batch.extend_pk(pk);
        self.batch.extend_weight(&weight.to_le_bytes());
        self.curr_null_word = 0;
        self.curr_col = 0;
    }

    /// Put a u64 value for the current payload column.
    pub(crate) fn put_u64(&mut self, val: u64) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a string value for the current payload column.
    pub(crate) fn put_string(&mut self, s: &str) {
        let st = crate::schema::encode_german_string(s.as_bytes(), &mut self.batch.blob);
        self.batch.extend_col(self.curr_col, &st);
        self.curr_col += 1;
    }

    /// Put a u128 value for the current payload column.
    pub(crate) fn put_u128(&mut self, val: u128) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u8 value for the current payload column.
    pub(crate) fn put_u8(&mut self, val: u8) {
        self.batch.extend_col(self.curr_col, &[val]);
        self.curr_col += 1;
    }

    /// Put a u16 value for the current payload column.
    pub(crate) fn put_u16(&mut self, val: u16) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u32 value for the current payload column.
    pub(crate) fn put_u32(&mut self, val: u32) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a NULL value for the current payload column.
    pub(crate) fn put_null(&mut self) {
        let col_size = self.schema.columns[self.physical_col_idx()].size() as usize;
        self.batch.fill_col_zero(self.curr_col, col_size);
        self.curr_null_word |= 1u64 << self.curr_col;
        self.curr_col += 1;
    }

    /// Finish the current row (writes null bitmap).
    pub(crate) fn end_row(&mut self) {
        self.batch.extend_null_bmp(&self.curr_null_word.to_le_bytes());
        self.batch.count += 1;
        self.batch.sorted = false;
        self.batch.consolidated = false;
    }

    /// Convenience: begin + put columns + end for a simple row.
    pub(crate) fn finish(self) -> Batch {
        self.batch
    }

    fn physical_col_idx(&self) -> usize {
        self.schema.payload_col_idx(self.curr_col)
    }
}

// ---------------------------------------------------------------------------
// Identifier validation
// ---------------------------------------------------------------------------

fn is_valid_ident_char(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

pub(crate) fn validate_user_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Identifier cannot be empty".into());
    }
    if name.as_bytes()[0] == b'_' {
        return Err(format!(
            "User identifiers cannot start with '_' (reserved for system prefix): {}", name
        ));
    }
    for &ch in name.as_bytes() {
        if !is_valid_ident_char(ch) {
            return Err(format!("Identifier contains invalid characters: {}", name));
        }
    }
    Ok(())
}

pub(crate) fn parse_qualified_name<'a>(name: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    if let Some(dot_pos) = name.find('.') {
        (&name[..dot_pos], &name[dot_pos + 1..])
    } else {
        (default_schema, name)
    }
}

// ---------------------------------------------------------------------------
// Index key type promotion
// ---------------------------------------------------------------------------

pub(crate) fn get_index_key_type(field_type_code: u8) -> Result<u8, String> {
    match field_type_code {
        type_code::U128 => Ok(type_code::U128),
        type_code::UUID => Ok(type_code::UUID),
        type_code::U64 => Ok(type_code::U64),
        type_code::I64 | type_code::U32 | type_code::I32 |
        type_code::U16 | type_code::I16 | type_code::U8 | type_code::I8 => Ok(type_code::U64),
        type_code::F32 | type_code::F64 | type_code::STRING => {
            Err(format!("Secondary index on column type {} not supported", field_type_code))
        }
        _ => Err(format!("Unknown column type code: {}", field_type_code)),
    }
}

/// Build a compound-PK index schema for a secondary index on `source`.
///
/// Layout: `(indexed_col: <promoted>, src_pk_0, src_pk_1, …)` with every
/// column in the PK and zero payload columns. `seek_by_index` walks the
/// index by prefix-scanning the leading indexed column, then reads the
/// source PK bytes directly out of the index PK region.
pub(crate) fn make_index_schema(
    index_key_type: u8,
    source: &SchemaDescriptor,
) -> SchemaDescriptor {
    let src_pk = source.pk_indices();
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(1 + src_pk.len());
    let mut pk_indices: Vec<u32> = Vec::with_capacity(1 + src_pk.len());
    cols.push(SchemaColumn::new(index_key_type, 0));
    pk_indices.push(0);
    for (i, &ci) in src_pk.iter().enumerate() {
        cols.push(SchemaColumn::new(source.columns[ci as usize].type_code, 0));
        pk_indices.push((i + 1) as u32);
    }
    SchemaDescriptor::new(&cols, &pk_indices)
}

/// Infix that marks an index as an internal FK-backing index. The single
/// source of truth for both name construction and the DROP INDEX integrity
/// guard that protects these indices.
pub(crate) const FK_INDEX_INFIX: &str = "__fk_";

pub(crate) fn make_fk_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}{}{}", schema_name, table_name, FK_INDEX_INFIX, col_name)
}

pub(crate) fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}__idx_{}", schema_name, table_name, col_name)
}

// ---------------------------------------------------------------------------
// Free function: ingest a batch into a table (avoids borrow conflicts)
// ---------------------------------------------------------------------------

pub(crate) fn ingest_batch_into(table: &mut Table, batch: &Batch) {
    if batch.count == 0 { return; }
    let _ = table.ingest_owned_batch(batch.clone_batch());
}

/// Flush a system table, surfacing the error with a contextual message.
/// Used on catalog paths whose only durability mechanism is flush (namely
/// sys_circuit_* and sys_view_deps, which bypass SAL broadcast).
pub(crate) fn flush_sys_table(table: &mut Table, name: &str) -> Result<(), String> {
    table.flush()
        .map_err(|e| format!("flush {} failed: {:?}", name, e))?;
    // These tables (sys_circuit_*, sys_view_deps) are flushed only here and
    // never via flush_family, so without compaction their L0 shards grow
    // unbounded; they're scanned on every boot and DDL op.
    table.compact_if_needed()
        .map_err(|e| format!("compaction {} failed: {:?}", name, e))
}

// ---------------------------------------------------------------------------
// Helper: read column data from cursor
// ---------------------------------------------------------------------------

/// Read a u64 from a cursor column. `logical_col` is the schema column index.
pub(crate) fn cursor_read_u64(cursor: &CursorHandle, logical_col: usize) -> u64 {
    let ptr = cursor.cursor.col_ptr(logical_col, 8);
    if ptr.is_null() { return 0; }
    let slice = unsafe { std::slice::from_raw_parts(ptr, 8) };
    u64::from_le_bytes(slice.try_into().unwrap_or([0; 8]))
}

/// Read a German string from a cursor column. `logical_col` is the schema column index.
pub(crate) fn cursor_read_string(cursor: &CursorHandle, logical_col: usize) -> String {
    let ptr = cursor.cursor.col_ptr(logical_col, 16);
    if ptr.is_null() { return String::new(); }
    let st: [u8; 16] = unsafe { std::slice::from_raw_parts(ptr, 16) }
        .try_into().unwrap_or([0; 16]);
    // For out-of-line strings, build a blob slice from the cursor's blob pointer
    let blob_ptr = cursor.cursor.blob_ptr();
    let blob_slice = if !blob_ptr.is_null() {
        let blob_len = cursor.cursor.blob_len();
        unsafe { std::slice::from_raw_parts(blob_ptr, blob_len) }
    } else {
        &[]
    };
    let bytes = crate::schema::decode_german_string(&st, blob_slice);
    String::from_utf8(bytes).unwrap_or_default()
}

/// Scan `table` for rows whose `schema_col` equals `sid`, returning a
/// `"schema.name"` qualified string for each hit via `id_to_qualified`.
pub(crate) fn collect_for_schema(
    table: &mut Table,
    schema_col: usize,
    sid: i64,
    id_to_qualified: &HashMap<i64, (String, String)>,
) -> Vec<String> {
    let mut result = Vec::new();
    let mut c = table.open_cursor();
    while c.cursor.valid {
        if c.cursor.current_weight > 0 {
            let row_sid = cursor_read_u64(&c, schema_col) as i64;
            if row_sid == sid {
                let eid = c.cursor.current_key as u64 as i64;
                if let Some((sn, en)) = id_to_qualified.get(&eid) {
                    result.push(format!("{}.{}", sn, en));
                }
            }
        }
        c.cursor.advance();
    }
    result
}

// ---------------------------------------------------------------------------
// Helper: filesystem
// ---------------------------------------------------------------------------

pub(crate) fn ensure_dir(path: &str) -> Result<(), String> {
    // Reject any embedded NUL bytes (should never happen with clean paths)
    if path.contains('\0') {
        return Err(format!("Path contains NUL byte: {:?}", path));
    }
    match fs::create_dir_all(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(format!("Failed to create directory '{}': {}", path, e)),
    }
}

pub(crate) fn fsync_dir(path: &str) {
    if let Ok(dir) = fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

// ---------------------------------------------------------------------------
// Copy/retract helpers
// ---------------------------------------------------------------------------

/// Copy a single cursor row into `batch` with an explicit weight.
/// Handles out-of-line German strings by copying blob data and rewriting offsets.
/// The schema is taken from the cursor (always matches the table schema).
pub(crate) fn copy_cursor_row_with_weight(
    cursor: &CursorHandle,
    batch: &mut Batch,
    weight: i64,
) {
    cursor.cursor.copy_current_row_into(batch, weight);
}

/// Seek a system table by PK, copy the matching row with weight=-1.
/// Returns a single-row retraction batch (or empty batch if PK not found).
pub(crate) fn retract_single_row(table: &Table, schema: &SchemaDescriptor, pk: u128) -> Batch {
    let mut batch = Batch::with_schema(*schema, 1);
    let mut cursor = table.open_cursor();
    // OPK-encode the native PK; correct for single-column and compound system PKs.
    let (opk, stride) = crate::storage::opk_key(schema, pk);
    cursor.cursor.seek_bytes(&opk[..stride]);
    if cursor.cursor.valid
        && cursor.cursor.current_pk_bytes() == &opk[..stride]
        && cursor.cursor.current_weight > 0
    {
        copy_cursor_row_with_weight(&cursor, &mut batch, -1);
    }
    batch
}

/// Scan `[start, pk_end)` and emit a weight=-1 batch of every positive-weight
/// row in the range. Used for U64-PK system tables where rows belonging to one
/// owner share a packed PK prefix (e.g. `sys_columns` keyed by
/// `pack_column_id(owner, col)`).
pub(crate) fn retract_rows_in_pk_range(
    table: &Table,
    schema: &SchemaDescriptor,
    start: u128,
    pk_end: u128,
) -> Batch {
    let mut batch = Batch::with_schema(*schema, 8);
    let mut cursor = table.open_cursor();
    // U64-PK system table: OPK == big-endian; the native-value range
    // comparisons below (`current_key`/`get_pk` vs `pk_end`) stay valid.
    cursor.cursor.seek_bytes(&(start as u64).to_be_bytes());

    // Bulk path: single consolidated MemBatch source.
    if let Some((src, start_idx)) = cursor.cursor.single_mem_batch() {
        let mut end = start_idx;
        while end < src.count && src.get_pk(end) < pk_end {
            end += 1;
        }
        batch.append_mem_batch_range(&src, start_idx, end, Some(-1));
        return batch;
    }

    // Row-at-a-time fallback for multi-source cursors.
    while cursor.cursor.valid {
        if cursor.cursor.current_key >= pk_end { break; }
        if cursor.cursor.current_weight > 0 {
            copy_cursor_row_with_weight(&cursor, &mut batch, -1);
        }
        cursor.cursor.advance();
    }
    batch
}

/// Scan all positive-weight rows for `view_id` and emit each as a retraction
/// (weight=-1). The circuit/dep catalog tables use a compound `(view_id, sub)`
/// PK, so all of a view's rows share the `view_id` byte prefix and are
/// contiguous in compound-PK sort order — `compare_pk_bytes` reproduces the
/// `(view_id, sub)` ordering natively (no `(pk_hi, pk_lo)` u128 exploit).
pub(crate) fn retract_rows_by_view(table: &Table, schema: &SchemaDescriptor, view_id: u64) -> Batch {
    let mut batch = Batch::with_schema(*schema, 8);
    // The (view_id, sub) PK is OPK-at-rest; the leading view_id column (U64) is
    // big-endian, so the prefix must be OPK (BE), not native LE.
    let prefix = view_id.to_be_bytes();
    let mut cursor = table.open_cursor();
    let mut hit = cursor.cursor.seek_first_positive_with_prefix(&prefix);
    while hit {
        copy_cursor_row_with_weight(&cursor, &mut batch, -1);
        cursor.cursor.advance();
        hit = cursor.cursor.walk_to_positive_with_prefix(&prefix);
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compound-PK regression guard for the precomputed payload→logical
    // mapping. 4-column schema with pk_indices=[1, 2]: payload slot 0
    // must map to logical column 0, slot 1 to logical column 3 — never
    // 0 and 1 (the bug the previous single-PK reimplementation would
    // have introduced for any non-leading compound PK).
    #[test]
    fn batch_builder_physical_col_idx_compound_pk() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let schema = SchemaDescriptor::new(&cols, &[1, 2]);
        let mut bb = BatchBuilder::new(schema);
        assert_eq!(bb.curr_col, 0);
        assert_eq!(bb.physical_col_idx(), 0);
        bb.curr_col = 1;
        assert_eq!(bb.physical_col_idx(), 3);
    }
}

