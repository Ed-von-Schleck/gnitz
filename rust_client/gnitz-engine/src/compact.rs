//! Self-contained shard compaction: N-way merge of sorted shard files.
//!
//! Replaces the RPython compactor.py + tournament_tree.py + shard_table.py (read) +
//! writer_table.py (write) + comparator.py for the compaction path.

use std::collections::HashMap;
use std::ffi::CStr;

use crate::columnar;
use crate::heap::MergeHeap;
use crate::shard_reader::MappedShard;
use crate::util::{read_u32_le, read_u64_le};
use crate::xxh;
#[cfg(test)]
use crate::util::read_i64_le;

// Type codes (from core/types.py). All defined for completeness;
// the compare_rows dispatch only uses F32/F64/STRING/U128 explicitly.
#[allow(dead_code)]
pub(crate) mod type_code {
    pub const U8: u8 = 1;
    pub const I8: u8 = 2;
    pub const U16: u8 = 3;
    pub const I16: u8 = 4;
    pub const U32: u8 = 5;
    pub const I32: u8 = 6;
    pub const F32: u8 = 7;
    pub const U64: u8 = 8;
    pub const I64: u8 = 9;
    pub const F64: u8 = 10;
    pub const STRING: u8 = 11;
    pub const U128: u8 = 12;
}
use type_code::STRING as TYPE_STRING;
#[cfg(test)]
use type_code::{I64 as TYPE_I64, U64 as TYPE_U64};

pub(crate) const SHORT_STRING_THRESHOLD: usize = 12;

// ---------------------------------------------------------------------------
// Schema descriptor (passed from RPython via FFI)
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SchemaColumn {
    pub type_code: u8,
    pub size: u8,
    pub nullable: u8,
    pub _pad: u8,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    pub num_columns: u32,
    pub pk_index: u32,
    pub columns: [SchemaColumn; 64],
}

// ---------------------------------------------------------------------------
// Guard output result (returned from merge_and_route)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct GuardResult {
    pub guard_key_lo: u64,
    pub guard_key_hi: u64,
    pub filename: [u8; 256], // null-terminated
}

impl GuardResult {
    pub fn zeroed() -> Self {
        GuardResult {
            guard_key_lo: 0,
            guard_key_hi: 0,
            filename: [0u8; 256],
        }
    }

    pub fn filename_str(&self) -> &str {
        let end = self
            .filename
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.filename.len());
        std::str::from_utf8(&self.filename[..end]).unwrap_or("")
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Shard cursor (position + ghost skip)
// ---------------------------------------------------------------------------

struct ShardCursor {
    shard_idx: usize,
    position: usize,
    count: usize,
}

impl ShardCursor {
    fn new(shard_idx: usize, shard: &MappedShard) -> Self {
        let mut c = ShardCursor {
            shard_idx,
            position: 0,
            count: shard.count,
        };
        c.skip_ghosts(shard);
        c
    }

    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self, shard: &MappedShard) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(shard);
        }
    }

    fn skip_ghosts(&mut self, shard: &MappedShard) {
        while self.position < self.count {
            if shard.get_weight(self.position) != 0 {
                return;
            }
            self.position += 1;
        }
    }

    fn peek_key(&self, shard: &MappedShard) -> u128 {
        if self.is_valid() {
            shard.get_pk(self.position)
        } else {
            u128::MAX
        }
    }

    fn weight(&self, shard: &MappedShard) -> i64 {
        if self.is_valid() {
            shard.get_weight(self.position)
        } else {
            0
        }
    }
}

fn is_null(shard: &MappedShard, row: usize, col_idx: usize, pk_index: usize) -> bool {
    let null_word = shard.get_null_word(row);
    let payload_idx = if col_idx < pk_index { col_idx } else { col_idx - 1 };
    (null_word >> payload_idx) & 1 != 0
}

#[inline]
pub(crate) fn read_signed(bytes: &[u8], size: usize) -> i64 {
    match size {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => 0,
    }
}

#[inline]
pub(crate) fn compare_german_strings(a: &[u8], blob_a: &[u8], b: &[u8], blob_b: &[u8]) -> std::cmp::Ordering {
    let len_a = read_u32_le(a, 0) as usize;
    let len_b = read_u32_le(b, 0) as usize;
    let min_len = len_a.min(len_b);

    // Compare prefix bytes (bytes 4..8 of the 16-byte struct)
    let limit = min_len.min(4);
    for i in 0..limit {
        if a[4 + i] != b[4 + i] {
            return a[4 + i].cmp(&b[4 + i]);
        }
    }

    if min_len <= 4 {
        return len_a.cmp(&len_b);
    }

    // Compare remaining bytes (after first 4) — inline access, no heap allocation
    for i in 4..min_len {
        let ca = string_byte(a, blob_a, len_a, i);
        let cb = string_byte(b, blob_b, len_b, i);
        if ca != cb {
            return ca.cmp(&cb);
        }
    }
    len_a.cmp(&len_b)
}

/// Read byte `i` of a German string (zero-alloc).
/// Short strings (≤12 bytes): suffix bytes are inline at struct offset 8.
/// Long strings (>12 bytes): data is in the blob heap at the stored offset.
#[inline]
pub(crate) fn string_byte(struct_bytes: &[u8], blob: &[u8], length: usize, i: usize) -> u8 {
    if length <= SHORT_STRING_THRESHOLD {
        struct_bytes[8 + (i - 4)]
    } else {
        let heap_offset = read_u64_le(struct_bytes, 8) as usize;
        blob[heap_offset + i]
    }
}


// ---------------------------------------------------------------------------
// Shard writer (compaction-internal, row-at-a-time from MappedShard)
// ---------------------------------------------------------------------------

struct ShardWriter {
    pk_lo: Vec<u8>,
    pk_hi: Vec<u8>,
    weight: Vec<u8>,
    null_bitmap: Vec<u8>,
    col_bufs: Vec<Vec<u8>>,
    blob_heap: Vec<u8>,
    blob_cache: HashMap<(u64, usize), usize>,
    count: usize,
    schema: SchemaDescriptor,
}

impl ShardWriter {
    fn new(schema: &SchemaDescriptor) -> Self {
        let mut col_bufs = Vec::with_capacity(schema.num_columns as usize);
        for ci in 0..schema.num_columns as usize {
            if ci == schema.pk_index as usize {
                col_bufs.push(Vec::new());
            } else {
                col_bufs.push(Vec::with_capacity(1024 * schema.columns[ci].size as usize));
            }
        }
        ShardWriter {
            pk_lo: Vec::with_capacity(8 * 1024),
            pk_hi: Vec::with_capacity(8 * 1024),
            weight: Vec::with_capacity(8 * 1024),
            null_bitmap: Vec::with_capacity(8 * 1024),
            col_bufs,
            blob_heap: Vec::with_capacity(4096),
            blob_cache: HashMap::new(),
            count: 0,
            schema: *schema,
        }
    }

    fn add_row_from_shard(
        &mut self,
        key: u128,
        weight: i64,
        shard: &MappedShard,
        row: usize,
    ) {
        if weight == 0 {
            return;
        }
        self.count += 1;
        let key_lo = key as u64;
        let key_hi = (key >> 64) as u64;
        self.pk_lo.extend_from_slice(&key_lo.to_le_bytes());
        self.pk_hi.extend_from_slice(&key_hi.to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());

        let pk_index = self.schema.pk_index as usize;
        let null_word = shard.get_null_word(row);
        self.null_bitmap.extend_from_slice(&null_word.to_le_bytes());

        let mut payload_idx: usize = 0;
        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let col_is_null = (null_word >> payload_idx) & 1 != 0;
            self.write_column(ci, payload_idx, col_is_null, shard, row);
            payload_idx += 1;
        }
    }

    fn write_column(
        &mut self,
        col_idx: usize,
        payload_col_idx: usize,
        is_null: bool,
        shard: &MappedShard,
        row: usize,
    ) {
        let col = &self.schema.columns[col_idx];
        let col_size = col.size as usize;

        if is_null {
            let len = self.col_bufs[col_idx].len();
            self.col_bufs[col_idx].resize(len + col_size, 0);
            return;
        }

        let src = shard.get_col_ptr(row, payload_col_idx, col_size);

        if col.type_code == TYPE_STRING {
            self.write_string_column(col_idx, src, shard.blob_slice());
        } else {
            self.col_bufs[col_idx].extend_from_slice(src);
        }
    }

    fn write_string_column(&mut self, col_idx: usize, src_struct: &[u8], src_blob: &[u8]) {
        let length = read_u32_le(src_struct, 0) as usize;

        let mut dest = [0u8; 16];
        dest[0..4].copy_from_slice(&(length as u32).to_le_bytes());
        dest[4..8].copy_from_slice(&src_struct[4..8]);

        if length <= SHORT_STRING_THRESHOLD {
            let suffix_len = if length > 4 { length - 4 } else { 0 };
            if suffix_len > 0 {
                dest[8..8 + suffix_len].copy_from_slice(&src_struct[8..8 + suffix_len]);
            }
        } else {
            let old_offset = read_u64_le(src_struct, 8) as usize;
            let src_data = &src_blob[old_offset..old_offset + length];
            let new_offset = self.get_or_append_blob(src_data);
            dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
        }

        self.col_bufs[col_idx].extend_from_slice(&dest);
    }

    fn get_or_append_blob(&mut self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        let h = xxh::checksum(data);
        let cache_key = (h, data.len());
        if let Some(&existing_offset) = self.blob_cache.get(&cache_key) {
            let existing = &self.blob_heap[existing_offset..existing_offset + data.len()];
            if existing == data {
                return existing_offset;
            }
        }
        let new_offset = self.blob_heap.len();
        self.blob_heap.extend_from_slice(data);
        self.blob_cache.insert(cache_key, new_offset);
        new_offset
    }

    fn finalize(&self, path: &CStr, table_id: u32) -> i32 {
        let pk_index = self.schema.pk_index as usize;
        let mut regions: Vec<(*const u8, usize)> = vec![
            (self.pk_lo.as_ptr(), self.pk_lo.len()),
            (self.pk_hi.as_ptr(), self.pk_hi.len()),
            (self.weight.as_ptr(), self.weight.len()),
            (self.null_bitmap.as_ptr(), self.null_bitmap.len()),
        ];
        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            regions.push((self.col_bufs[ci].as_ptr(), self.col_bufs[ci].len()));
        }
        regions.push((self.blob_heap.as_ptr(), self.blob_heap.len()));

        let image = crate::shard_file::build_shard_image(
            table_id, self.count as u32, &regions,
        );
        crate::shard_file::write_shard_at(
            libc::AT_FDCWD, path, &image, true,
        )
    }
}

// ---------------------------------------------------------------------------
// Guard key routing (binary search)
// ---------------------------------------------------------------------------

fn find_guard_for_key(guard_keys: &[(u64, u64)], key: u128) -> usize {
    let n = guard_keys.len();
    if n == 0 {
        return 0;
    }
    let mut lo = 0usize;
    let mut hi = n - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let (gk_lo, gk_hi) = guard_keys[mid];
        let gk = ((gk_hi as u128) << 64) | (gk_lo as u128);
        if gk <= key {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// ---------------------------------------------------------------------------
// Shared merge infrastructure
// ---------------------------------------------------------------------------

/// Open input shards, build cursors and heap, run N-way merge loop.
/// Calls `emit(key, net_weight, shard, row)` for each non-ghost consolidated row.
fn open_and_merge(
    input_files: &[&CStr],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(u128, i64, &MappedShard, usize),
) -> Result<(), i32> {
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema, false) {
            Ok(s) => shards.push(s),
            Err(e) => return Err(e),
        }
    }

    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, &shards[i]));
    }

    let key_less = |a: &crate::heap::HeapNode, b: &crate::heap::HeapNode| a.key < b.key;

    let mut tree = MergeHeap::build(
        cursors.len(),
        |i| {
            if cursors[i].is_valid() {
                Some(cursors[i].peek_key(&shards[cursors[i].shard_idx]))
            } else {
                None
            }
        },
        &key_less,
    );

    let mut advance_buf: Vec<usize> = Vec::with_capacity(shards.len());
    while !tree.is_empty() {
        let min_key = tree.min_key();

        let num_min = tree.collect_min_indices(
            &|a: &crate::heap::HeapNode, b: &crate::heap::HeapNode| -> std::cmp::Ordering {
                let key_ord = a.key.cmp(&b.key);
                if key_ord != std::cmp::Ordering::Equal {
                    return key_ord;
                }
                let ca = &cursors[a.idx];
                let cb = &cursors[b.idx];
                columnar::compare_rows(
                    schema,
                    &shards[ca.shard_idx], ca.position,
                    &shards[cb.shard_idx], cb.position,
                )
            },
        );

        let mut net_weight: i64 = 0;
        for i in 0..num_min {
            let ci = tree.min_indices[i];
            net_weight += cursors[ci].weight(&shards[cursors[ci].shard_idx]);
        }

        if net_weight != 0 {
            let exemplar = tree.min_indices[0];
            let shard_idx = cursors[exemplar].shard_idx;
            let row = cursors[exemplar].position;
            emit(min_key, net_weight, &shards[shard_idx], row);
        }

        advance_buf.clear();
        advance_buf.extend_from_slice(&tree.min_indices[..num_min]);
        for &ci in &advance_buf {
            let shard = &shards[cursors[ci].shard_idx];
            cursors[ci].advance(shard);
            let new_key = if cursors[ci].is_valid() {
                Some(cursors[ci].peek_key(&shards[cursors[ci].shard_idx]))
            } else {
                None
            };
            tree.advance(ci, new_key, &key_less);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
) -> i32 {
    let mut writer = ShardWriter::new(schema);
    if let Err(e) = open_and_merge(input_files, schema, |key, weight, shard, row| {
        writer.add_row_from_shard(key, weight, shard, row);
    }) {
        return e;
    }
    writer.finalize(output_file, table_id)
}

pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[(u64, u64)],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    lsn_tag: u64,
    out_results: &mut [GuardResult],
) -> i32 {
    let num_guards = guard_keys.len();
    let out_dir_str = output_dir.to_str().unwrap_or("");

    let mut writers: Vec<ShardWriter> = Vec::with_capacity(num_guards);
    let mut out_filenames: Vec<String> = Vec::with_capacity(num_guards);
    for i in 0..num_guards {
        writers.push(ShardWriter::new(schema));
        out_filenames.push(format!(
            "{}/shard_{}_{}_L{}_G{}.db",
            out_dir_str, table_id, lsn_tag, level_num, i
        ));
    }

    if let Err(e) = open_and_merge(input_files, schema, |key, weight, shard, row| {
        let guard_idx = find_guard_for_key(guard_keys, key);
        writers[guard_idx].add_row_from_shard(key, weight, shard, row);
    }) {
        return e;
    }

    let mut result_count: i32 = 0;
    for i in 0..num_guards {
        if writers[i].count == 0 {
            continue;
        }
        let cpath = std::ffi::CString::new(out_filenames[i].as_str()).unwrap();
        let frc = writers[i].finalize(&cpath, table_id);
        if frc != 0 {
            return frc;
        }
        if !std::path::Path::new(&out_filenames[i]).exists() {
            continue;
        }
        let ri = result_count as usize;
        if ri < out_results.len() {
            out_results[ri].guard_key_lo = guard_keys[i].0;
            out_results[ri].guard_key_hi = guard_keys[i].1;
            let name_bytes = out_filenames[i].as_bytes();
            let len = name_bytes.len().min(255);
            out_results[ri].filename[..len].copy_from_slice(&name_bytes[..len]);
            out_results[ri].filename[len] = 0;
        }
        result_count += 1;
    }

    result_count
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // Helper: build a minimal shard file in memory and write to disk
    fn write_test_shard(path: &str, pks: &[u64], weights: &[i64], schema: &SchemaDescriptor) {
        let count = pks.len();
        let pk_index = schema.pk_index as usize;
        let num_cols = schema.num_columns as usize;

        let mut writer = ShardWriter::new(schema);

        // For simplicity, create a mock MappedShard-like approach:
        // We'll build the shard using the writer directly
        for i in 0..count {
            let _key = pks[i] as u128;
            writer.count += 1;
            writer.pk_lo.extend_from_slice(&pks[i].to_le_bytes());
            writer.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            writer.weight.extend_from_slice(&weights[i].to_le_bytes());

            // Write non-PK columns with dummy data
            let null_word: u64 = 0;
            for ci in 0..num_cols {
                if ci == pk_index {
                    continue;
                }
                let col_size = schema.columns[ci].size as usize;
                // Write the PK value as column data (for testing)
                let mut val_bytes = vec![0u8; col_size];
                let pk_bytes = pks[i].to_le_bytes();
                let copy_len = col_size.min(8);
                val_bytes[..copy_len].copy_from_slice(&pk_bytes[..copy_len]);
                writer.col_bufs[ci].extend_from_slice(&val_bytes);
            }
            writer.null_bitmap.extend_from_slice(&null_word.to_le_bytes());
        }

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(writer.finalize(&cpath, 0), 0);
    }

    fn make_test_schema() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 0, _pad: 0 };
        s
    }

    #[test]
    fn test_compact_basic() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_basic");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: keys 1, 3, 5
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 3, 5], &[1, 1, 1], &schema);

        // Shard 2: keys 2, 4, 6
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2, 4, 6], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        // Read back merged shard
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 6);

        // Verify sorted order
        let mut prev = 0u128;
        for i in 0..merged.count {
            let pk = merged.get_pk(i);
            assert!(pk > prev, "not sorted at row {}: {} <= {}", i, pk, prev);
            prev = pk;
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_weight_elimination() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_weight");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: insert keys 1, 2, 3
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3], &[1, 1, 1], &schema);

        // Shard 2: delete key 2 (weight = -1)
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2], &[-1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        // Key 2 should be eliminated (net weight = 0)
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2);
        assert_eq!(merged.get_pk(0), 1);
        assert_eq!(merged.get_pk(1), 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_single_shard() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_single");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[10, 20, 30], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_ghost_rows() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_ghost");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        // Shard with ghost rows (weight=0)
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3, 4, 5], &[1, 0, 1, 0, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3); // only keys 1, 3, 5

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_string_comparison() {
        // Short strings
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        // len=3, prefix="abc"
        a[0..4].copy_from_slice(&3u32.to_le_bytes());
        a[4] = b'a'; a[5] = b'b'; a[6] = b'c';
        b[0..4].copy_from_slice(&3u32.to_le_bytes());
        b[4] = b'a'; b[5] = b'b'; b[6] = b'd';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Less);

        // Equal strings
        b[6] = b'c';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Equal);

        // Different length, same prefix
        b[0..4].copy_from_slice(&4u32.to_le_bytes());
        b[7] = b'z';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_guard_routing() {
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 50), 0);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 100), 1);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 150), 1);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 200), 2);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 999), 2);
    }

    #[test]
    fn test_guard_routing_empty() {
        assert_eq!(find_guard_for_key(&[], 42), 0);
    }

    #[test]
    fn test_guard_routing_single() {
        assert_eq!(find_guard_for_key(&[(0, 0)], 0), 0);
        assert_eq!(find_guard_for_key(&[(0, 0)], 999), 0);
    }

    #[test]
    fn test_compact_empty_input() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs: [&CStr; 0] = [];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        // Output shard should exist with 0 rows
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_all_cancel() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_cancel");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: insert keys 1, 2, 3
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3], &[1, 1, 1], &schema);

        // Shard 2: delete all
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[1, 2, 3], &[-1, -1, -1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_and_route_basic() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_route");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard with keys 10, 50, 150, 250
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[10, 50, 150, 250], &[1, 1, 1, 1], &schema);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cdir = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let inputs = [cs1.as_c_str()];

        // Two guards: [0, 100)  and [100, ∞)
        let guards = [(0u64, 0u64), (100u64, 0u64)];
        let mut results = [
            GuardResult { guard_key_lo: 0, guard_key_hi: 0, filename: [0u8; 256] },
            GuardResult { guard_key_lo: 0, guard_key_hi: 0, filename: [0u8; 256] },
        ];

        let rc = merge_and_route(
            &inputs, &cdir, &guards, &schema,
            0, 1, 99, &mut results,
        );
        assert_eq!(rc, 2); // both guards should have rows

        // Guard 0 should have keys 10, 50
        let fn0_end = results[0].filename.iter().position(|&b| b == 0).unwrap_or(256);
        let fn0 = std::str::from_utf8(&results[0].filename[..fn0_end]).unwrap();
        let cfn0 = std::ffi::CString::new(fn0).unwrap();
        let g0 = MappedShard::open(&cfn0, &schema, false).unwrap();
        assert_eq!(g0.count, 2);
        assert_eq!(g0.get_pk(0), 10);
        assert_eq!(g0.get_pk(1), 50);

        // Guard 1 should have keys 150, 250
        let fn1_end = results[1].filename.iter().position(|&b| b == 0).unwrap_or(256);
        let fn1 = std::str::from_utf8(&results[1].filename[..fn1_end]).unwrap();
        let cfn1 = std::ffi::CString::new(fn1).unwrap();
        let g1 = MappedShard::open(&cfn1, &schema, false).unwrap();
        assert_eq!(g1.count, 2);
        assert_eq!(g1.get_pk(0), 150);
        assert_eq!(g1.get_pk(1), 250);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_string_column() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_string");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Schema: u64 PK + STRING payload
        let mut schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        schema.columns[1] = SchemaColumn { type_code: TYPE_STRING, size: 16, nullable: 0, _pad: 0 };

        // Build shard with short strings
        let mut writer = ShardWriter::new(&schema);
        for pk in [1u64, 2, 3] {
            writer.count += 1;
            writer.pk_lo.extend_from_slice(&pk.to_le_bytes());
            writer.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            writer.weight.extend_from_slice(&1i64.to_le_bytes());
            writer.null_bitmap.extend_from_slice(&0u64.to_le_bytes());

            // Write a short string: "hi" (2 bytes, inline)
            let mut str_struct = [0u8; 16];
            str_struct[0..4].copy_from_slice(&2u32.to_le_bytes()); // length=2
            str_struct[4] = b'h'; str_struct[5] = b'i'; // prefix
            writer.col_bufs[1].extend_from_slice(&str_struct);
        }
        let s1 = dir.join("s1.db");
        let cpath = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        assert_eq!(writer.finalize(&cpath, 0), 0);

        // Compact it (single shard, should roundtrip)
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();
        let inputs = [cpath.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3);

        // Verify string data survived
        for row in 0..3 {
            let col_data = merged.get_col_ptr(row, 0, 16);
            let str_len = read_u32_le(col_data, 0);
            assert_eq!(str_len, 2);
            assert_eq!(col_data[4], b'h');
            assert_eq!(col_data[5], b'i');
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_nullable_column() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_nullable");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Schema: u64 PK + nullable i64 payload
        let mut schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        schema.columns[1] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 1, _pad: 0 };

        // Build shard: key 1 = non-null (42), key 2 = null
        let mut writer = ShardWriter::new(&schema);
        // Row 1: non-null
        writer.count += 1;
        writer.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        writer.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        writer.weight.extend_from_slice(&1i64.to_le_bytes());
        writer.null_bitmap.extend_from_slice(&0u64.to_le_bytes()); // no nulls
        writer.col_bufs[1].extend_from_slice(&42i64.to_le_bytes());

        // Row 2: null column
        writer.count += 1;
        writer.pk_lo.extend_from_slice(&2u64.to_le_bytes());
        writer.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        writer.weight.extend_from_slice(&1i64.to_le_bytes());
        // null bit for col_idx=1, pk_index=0 → payload_idx = 0 → bit 0
        writer.null_bitmap.extend_from_slice(&1u64.to_le_bytes());
        writer.col_bufs[1].extend_from_slice(&0i64.to_le_bytes());

        let s1 = dir.join("s1.db");
        let cpath = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        assert_eq!(writer.finalize(&cpath, 0), 0);

        // Compact
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();
        let inputs = [cpath.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2);

        // Row 0: not null
        assert!(!is_null(&merged, 0, 1, 0));
        let val = read_i64_le(merged.get_col_ptr(0, 0, 8), 0);
        assert_eq!(val, 42);

        // Row 1: null
        assert!(is_null(&merged, 1, 1, 0));

        let _ = fs::remove_dir_all(&dir);
    }
}
