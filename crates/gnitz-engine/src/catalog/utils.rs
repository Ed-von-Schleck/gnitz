use super::*;

// ---------------------------------------------------------------------------
// BatchBuilder — construct Batch rows for system table mutations
// ---------------------------------------------------------------------------

/// Lightweight row-by-row builder for constructing Batch in Rust.
/// Operates on Batch directly; the schema lives on the batch itself.
pub(crate) struct BatchBuilder {
    pub(crate) batch: Batch,
    // per-row state
    pub(crate) curr_null_word: u64,
    pub(crate) curr_col: usize,
}

impl BatchBuilder {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        BatchBuilder {
            batch: Batch::with_schema(schema, 8),
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

    // The non-u64/string put variants are exercised only by the catalog tests
    // (production system-table rows are u64/string-shaped); `#[cfg(test)]`
    // keeps them out of production builds, mirroring `ddl.rs::create_table`.

    /// Put a u128 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u128(&mut self, val: u128) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u8 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u8(&mut self, val: u8) {
        self.batch.extend_col(self.curr_col, &[val]);
        self.curr_col += 1;
    }

    /// Put a u16 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u16(&mut self, val: u16) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u32 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u32(&mut self, val: u32) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a NULL value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_null(&mut self) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
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

    #[cfg(test)]
    fn schema(&self) -> &SchemaDescriptor {
        self.batch.schema.as_ref().expect("BatchBuilder batch always carries a schema")
    }

    #[cfg(test)]
    fn physical_col_idx(&self) -> usize {
        self.schema().payload_col_idx(self.curr_col)
    }
}

// ---------------------------------------------------------------------------
// Identifier validation
//
// `FK_INDEX_INFIX` and `validate_user_identifier` live in `gnitz-wire` (shared
// with the SQL planner) and are re-bound in `catalog/mod.rs`.
// ---------------------------------------------------------------------------

/// Only the test-only direct DDL entry points (`ddl.rs`) take qualified-name
/// strings; the wire path ships schema and entity ids separately.
#[cfg(test)]
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

/// Promote one base-table column type to its secondary-index leading-key type.
/// Thin re-export of `gnitz_wire::index_key_type` (the single source of truth,
/// shared with the SQL planner's CREATE INDEX limit pre-check) so the engine and
/// the planner can never disagree on a column's promoted width.
pub(crate) fn get_index_key_type(field_type_code: u8) -> Result<u8, String> {
    gnitz_wire::index_key_type(field_type_code)
}

/// Build a compound-PK index schema for a secondary index on `source_cols`
/// of `source`, validating the column list along the way.
///
/// Layout: `(promoted_c0, promoted_c1, …, src_pk_0, src_pk_1, …)` — every
/// indexed column promoted independently and packed in declared order, then the
/// source PK columns, all in the PK with zero payload columns. The leading
/// indexed-key region is `Σ promoted widths`; `seek_by_index` prefix-scans it
/// (full or leading-prefix), then reads the source PK bytes directly out of the
/// index PK suffix. The 1-element list is the single-column index.
///
/// Bounds-checks every column, promotes it (rejecting STRING/BLOB/float), and
/// validates the index-schema PK limits — all **before** calling
/// `SchemaDescriptor::new`: that constructor is a `const fn` whose `assert!`s
/// fire in release and abort the master. An over-limit schema is reachable only
/// for a *composite* index (a single-column index — including every FK
/// auto-index — always fits, since `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` reserves
/// the prefix slot), via a raw `gnitz-core` client or a crafted/over-range
/// persisted row replayed at boot, neither of which goes through the SQL
/// planner's pre-check. Validating here converts the abort into a clean ingest
/// `Err` for every path (defence in depth at the catalog trust boundary).
pub(crate) fn make_index_schema(
    source_cols: &[u32],
    source: &SchemaDescriptor,
) -> Result<SchemaDescriptor, String> {
    let mut col_types: Vec<u8> = Vec::with_capacity(source_cols.len());
    for &c in source_cols {
        if c as usize >= source.num_columns() {
            return Err(format!(
                "Index: column index {} out of bounds (columns={})",
                c, source.num_columns()));
        }
        col_types.push(source.columns[c as usize].type_code);
    }
    let src_pk = source.pk_indices();
    // Shared with the SQL planner's CREATE INDEX pre-check, so the promotion
    // rule and the arity/stride limits can never disagree across the layers.
    let promoted = gnitz_wire::index_key_types(
        &col_types, src_pk.len(), source.pk_stride() as usize)?;
    let n = promoted.len();
    let arity = n + src_pk.len();
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(arity);
    let mut pk_indices: Vec<u32> = Vec::with_capacity(arity);
    for (i, &t) in promoted.iter().enumerate() {
        cols.push(SchemaColumn::new(t, 0));
        pk_indices.push(i as u32);
    }
    for (j, &ci) in src_pk.iter().enumerate() {
        cols.push(SchemaColumn::new(source.columns[ci as usize].type_code, 0));
        pk_indices.push((n + j) as u32);
    }
    Ok(SchemaDescriptor::new(&cols, &pk_indices))
}

/// Wire schema for the GET_INDICES descriptor list: `(packed_cols PK, is_unique)`.
/// The PK carries `pack_pk_cols(&col_indices)` — unique per circuit (circuits
/// dedup by column list), so a valid PK. The server ships this block on the data
/// path; the client decodes against the wire schema and reads columns by position.
pub(crate) fn index_meta_schema_desc() -> SchemaDescriptor {
    let u64c = SchemaColumn::new(type_code::U64, 0);
    SchemaDescriptor::new(&[u64c, u64c], &[0])   // [packed_cols (PK), is_unique]
}
pub(crate) const INDEX_META_COL_NAMES: [&[u8]; 2] = [b"cols", b"is_unique"];

pub(crate) fn make_fk_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}{}{}", schema_name, table_name, FK_INDEX_INFIX, col_name)
}

/// Production index names arrive pre-built over the wire; only the test-only
/// `ddl.rs::create_index` path names them engine-side.
#[cfg(test)]
pub(crate) fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{}__{}__idx_{}", schema_name, table_name, col_name)
}

// ---------------------------------------------------------------------------
// On-disk directory naming conventions
//
// Every entity directory is *built* and *parsed back* here, so the creation
// hooks and the boot-time orphan sweep (`gc_orphan_directories`) can never
// disagree on the shape. Each `*_dir` builder has a matching `is_*_dir_name`
// recognizer where the sweep needs to classify an on-disk name.
// ---------------------------------------------------------------------------

/// `<base_dir>/<schema_name>` — a schema's directory. Name-based (no id): a
/// DROP+CREATE of the same schema name reuses the path, which is why recreation
/// must cancel a pending deletion of it.
pub(crate) fn schema_dir(base_dir: &str, schema_name: &str) -> String {
    format!("{}/{}", base_dir, schema_name)
}

/// `<base_dir>/<schema_name>/<name>_<tid>` — a table's directory.
pub(crate) fn table_dir(base_dir: &str, schema_name: &str, name: &str, tid: i64) -> String {
    format!("{}/{}/{}_{}", base_dir, schema_name, name, tid)
}

/// `<base_dir>/<schema_name>/view_<name>_<vid>` — a view's directory.
pub(crate) fn view_dir(base_dir: &str, schema_name: &str, name: &str, vid: i64) -> String {
    format!("{}/{}/view_{}_{}", base_dir, schema_name, name, vid)
}

/// `<owner_dir>/idx_<idx_id>` — an index's directory, nested in its owner.
pub(crate) fn index_dir(owner_dir: &str, idx_id: i64) -> String {
    format!("{}/idx_{}", owner_dir, idx_id)
}

/// True if `name` is shaped like an index directory (`idx_<digits>`).
pub(crate) fn is_index_dir_name(name: &str) -> bool {
    name.strip_prefix("idx_").is_some_and(has_numeric_id)
}

/// True if `name` is shaped like a table or view directory — both end in
/// `_<digits>` (`<name>_<tid>` and `view_<name>_<vid>` respectively).
pub(crate) fn is_table_dir_name(name: &str) -> bool {
    name.rsplit_once('_').is_some_and(|(_, id)| has_numeric_id(id))
}

/// A directory-name id component: non-empty and all ASCII digits.
fn has_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Immediate sub-directory names of `path`. Empty if `path` is missing or
/// unreadable — both mean "nothing to scan" for the orphan sweep. Non-directory
/// entries are skipped.
pub(crate) fn subdir_names(path: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(path) else { return Vec::new() };
    entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Free function: ingest a batch into a table (avoids borrow conflicts)
// ---------------------------------------------------------------------------

pub(crate) fn ingest_batch_into(table: &mut Table, batch: &Batch) {
    if batch.count == 0 { return; }
    let _ = table.ingest_owned_batch(batch.clone_batch());
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

