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
    pub(crate) fn begin_row(&mut self, pk_lo: u64, pk_hi: u64, weight: i64) {
        self.batch.ensure_row_capacity();
        self.batch.extend_pk_lo(&pk_lo.to_le_bytes());
        self.batch.extend_pk_hi(&pk_hi.to_le_bytes());
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

    /// Put a u128 value (lo, hi) for the current payload column.
    pub(crate) fn put_u128(&mut self, lo: u64, hi: u64) {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&lo.to_le_bytes());
        buf[8..16].copy_from_slice(&hi.to_le_bytes());
        self.batch.extend_col(self.curr_col, &buf);
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
        let col_size = self.schema.columns[self.physical_col_idx()].size as usize;
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
        // payload col index maps to schema col index (skip PK)
        let pk_idx = self.schema.pk_index as usize;
        if self.curr_col < pk_idx {
            self.curr_col
        } else {
            self.curr_col + 1
        }
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
        type_code::U64 => Ok(type_code::U64),
        type_code::I64 | type_code::U32 | type_code::I32 |
        type_code::U16 | type_code::I16 | type_code::U8 | type_code::I8 => Ok(type_code::U64),
        type_code::F32 | type_code::F64 | type_code::STRING => {
            Err(format!("Secondary index on column type {} not supported", field_type_code))
        }
        _ => Err(format!("Unknown column type code: {}", field_type_code)),
    }
}

pub(crate) fn make_index_schema(index_key_type: u8, source_pk_type: u8) -> SchemaDescriptor {
    let key_col = SchemaColumn {
        type_code: index_key_type,
        size: crate::schema::type_size(index_key_type),
        nullable: 0,
        _pad: 0,
    };
    let pk_col = SchemaColumn {
        type_code: source_pk_type,
        size: crate::schema::type_size(source_pk_type),
        nullable: 0,
        _pad: 0,
    };
    make_schema(&[key_col, pk_col], 0)
}

pub(crate) fn make_fk_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}__fk_{}", schema_name, table_name, col_name)
}

pub(crate) fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}__idx_{}", schema_name, table_name, col_name)
}

// ---------------------------------------------------------------------------
// Free function: ingest a batch into a table (avoids borrow conflicts)
// ---------------------------------------------------------------------------

pub(crate) fn ingest_batch_into(table: &mut Table, batch: &Batch) {
    if batch.count == 0 { return; }
    let npc = batch.num_payload_cols();
    let (ptrs, sizes) = batch.to_region_ptrs();
    let _ = table.ingest_batch_from_regions(&ptrs, &sizes, batch.count as u32, npc);
}

/// Flush a system table, surfacing the error with a contextual message.
/// Used on catalog paths whose only durability mechanism is flush (namely
/// sys_circuit_* and sys_view_deps, which bypass SAL broadcast).
pub(crate) fn flush_sys_table(table: &mut Table, name: &str) -> Result<(), String> {
    table.flush()
        .map(|_| ())
        .map_err(|e| format!("flush {} failed: {:?}", name, e))
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
    if let Ok(mut c) = table.create_cursor() {
        while c.cursor.valid {
            if c.cursor.current_weight > 0 {
                let row_sid = cursor_read_u64(&c, schema_col) as i64;
                if row_sid == sid {
                    let eid = c.cursor.current_key_lo as i64;
                    if let Some((sn, en)) = id_to_qualified.get(&eid) {
                        result.push(format!("{}.{}", sn, en));
                    }
                }
            }
            c.cursor.advance();
        }
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
pub(crate) fn retract_single_row(table: &mut Table, schema: &SchemaDescriptor, pk_lo: u64, pk_hi: u64) -> Batch {
    let mut batch = Batch::with_schema(*schema, 1);
    let mut cursor = match table.create_cursor() {
        Ok(c) => c,
        Err(_) => return batch,
    };
    cursor.cursor.seek(crate::util::make_pk(pk_lo, pk_hi));
    if cursor.cursor.valid
        && cursor.cursor.current_key_lo == pk_lo
        && cursor.cursor.current_key_hi == pk_hi
        && cursor.cursor.current_weight > 0
    {
        copy_cursor_row_with_weight(&cursor, &mut batch, -1);
    }
    batch
}

/// Scan `[pk_lo_start, pk_lo_end)` on `pk_hi == 0` and emit a weight=-1 batch
/// of every positive-weight row in the range. Used for U64-PK system tables
/// where rows belonging to one owner share a packed PK prefix (e.g.
/// `sys_columns` keyed by `pack_column_id(owner, col)`).
pub(crate) fn retract_rows_by_pk_lo_range(
    table: &mut Table,
    schema: &SchemaDescriptor,
    pk_lo_start: u64,
    pk_lo_end: u64,
) -> Batch {
    let mut batch = Batch::with_schema(*schema, 8);
    let mut cursor = match table.create_cursor() {
        Ok(c) => c,
        Err(_) => return batch,
    };
    cursor.cursor.seek(crate::util::make_pk(pk_lo_start, 0));

    // Bulk path: single consolidated MemBatch source.
    if let Some((src, start)) = cursor.cursor.single_mem_batch() {
        let mut end = start;
        while end < src.count
            && crate::util::read_u64_le(src.pk_lo, end * 8) < pk_lo_end
        {
            end += 1;
        }
        batch.append_mem_batch_range(&src, start, end, -1);
        return batch;
    }

    // Row-at-a-time fallback for multi-source cursors.
    while cursor.cursor.valid {
        if cursor.cursor.current_key_lo >= pk_lo_end { break; }
        if cursor.cursor.current_weight > 0 {
            copy_cursor_row_with_weight(&cursor, &mut batch, -1);
        }
        cursor.cursor.advance();
    }
    batch
}

/// Seek to `(0, pk_hi)` and scan all positive-weight rows with that pk_hi,
/// emitting each as a retraction (weight=-1). Exploits U128 sort order
/// `(pk_hi, pk_lo)` which makes all circuit rows for a given view contiguous.
pub(crate) fn retract_rows_by_pk_hi(table: &mut Table, schema: &SchemaDescriptor, pk_hi: u64) -> Batch {
    let mut batch = Batch::with_schema(*schema, 8);
    let mut cursor = match table.create_cursor() {
        Ok(c) => c,
        Err(_) => return batch,
    };
    cursor.cursor.seek(crate::util::make_pk(0, pk_hi));

    // Bulk path: single consolidated MemBatch source. Consolidated rows always
    // have positive weight, so the weight>0 filter is trivially satisfied and
    // we get one copy_from_slice per column instead of one call per row.
    if let Some((src, start)) = cursor.cursor.single_mem_batch() {
        let mut end = start;
        while end < src.count
            && crate::util::read_u64_le(src.pk_hi, end * 8) == pk_hi
        {
            end += 1;
        }
        batch.append_mem_batch_range(&src, start, end, -1);
        return batch;
    }

    // Row-at-a-time fallback for multi-source cursors.
    while cursor.cursor.valid && cursor.cursor.current_key_hi == pk_hi {
        if cursor.cursor.current_weight > 0 {
            copy_cursor_row_with_weight(&cursor, &mut batch, -1);
        }
        cursor.cursor.advance();
    }
    batch
}

