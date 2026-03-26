//! Row-at-a-time shard builder with blob deduplication.
//!
//! Used by both the compaction path (reading from MappedShard) and the FFI path
//! (receiving flat column data from RPython).

use std::collections::HashMap;
use std::ffi::CStr;

use crate::compact::SchemaDescriptor;
use crate::util::read_u32_le;
use crate::xxh;

const SHORT_STRING_THRESHOLD: usize = 12;
// Type code for STRING columns (must match core/types.py TYPE_STRING.code = 11)
const TYPE_STRING: u8 = 11;

pub struct ShardWriter {
    pub pk_lo: Vec<u8>,
    pub pk_hi: Vec<u8>,
    pub weight: Vec<u8>,
    pub null_bitmap: Vec<u8>,
    pub col_bufs: Vec<Vec<u8>>,
    pub blob_heap: Vec<u8>,
    pub blob_cache: HashMap<(u64, usize), usize>,
    pub count: usize,
    schema: SchemaDescriptor,
}

impl ShardWriter {
    pub fn new(schema: &SchemaDescriptor) -> Self {
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

    /// Add a row from flat column pointers (FFI path).
    ///
    /// `col_ptrs[i]` points to the raw column data for non-PK column i.
    /// `col_sizes[i]` is the byte size of that column's data.
    /// `blob_base` + `blob_len` is the source blob heap for string relocation.
    pub fn add_row_flat(
        &mut self,
        key_lo: u64,
        key_hi: u64,
        weight: i64,
        null_word: u64,
        col_ptrs: &[*const u8],
        col_sizes: &[u32],
        blob_base: *const u8,
        blob_len: usize,
    ) {
        if weight == 0 {
            return;
        }
        self.count += 1;
        self.pk_lo.extend_from_slice(&key_lo.to_le_bytes());
        self.pk_hi.extend_from_slice(&key_hi.to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());
        self.null_bitmap.extend_from_slice(&null_word.to_le_bytes());

        let pk_index = self.schema.pk_index as usize;
        let mut payload_idx: usize = 0;
        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let col = &self.schema.columns[ci];
            let col_size = col.size as usize;
            let is_null = if ci < pk_index {
                (null_word & (1u64 << ci)) != 0
            } else {
                (null_word & (1u64 << (ci - 1))) != 0
            };

            if payload_idx < col_ptrs.len() && payload_idx < col_sizes.len() {
                let src_ptr = col_ptrs[payload_idx];
                let src_size = col_sizes[payload_idx] as usize;

                if is_null || src_ptr.is_null() || src_size == 0 {
                    self.col_bufs[ci].extend(std::iter::repeat(0u8).take(col_size));
                } else if col.type_code == TYPE_STRING {
                    let src_struct = unsafe { std::slice::from_raw_parts(src_ptr, src_size.min(16)) };
                    let src_blob = if !blob_base.is_null() && blob_len > 0 {
                        unsafe { std::slice::from_raw_parts(blob_base, blob_len) }
                    } else {
                        &[]
                    };
                    self.write_string_column(ci, src_struct, src_blob);
                } else {
                    let src = unsafe { std::slice::from_raw_parts(src_ptr, src_size.min(col_size)) };
                    self.col_bufs[ci].extend_from_slice(src);
                    // Pad if source is shorter than expected column size
                    if src.len() < col_size {
                        self.col_bufs[ci].extend(std::iter::repeat(0u8).take(col_size - src.len()));
                    }
                }
            } else {
                self.col_bufs[ci].extend(std::iter::repeat(0u8).take(col_size));
            }
            payload_idx += 1;
        }
    }

    /// Add a row from a MappedShard (compaction path).
    pub fn add_row_from_shard(
        &mut self,
        key: u128,
        weight: i64,
        shard: &crate::shard_reader::MappedShard,
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
        let mut null_word: u64 = 0;
        let mut payload_idx: usize = 0;

        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let is_null = is_null_in_shard(shard, row, ci, pk_index);
            if is_null {
                let null_bit_pos = if ci < pk_index { ci } else { ci - 1 };
                null_word |= 1u64 << null_bit_pos;
            }
            self.write_column_from_shard(ci, payload_idx, is_null, shard, row);
            payload_idx += 1;
        }
        self.null_bitmap.extend_from_slice(&null_word.to_le_bytes());
    }

    fn write_column_from_shard(
        &mut self,
        col_idx: usize,
        payload_col_idx: usize,
        is_null: bool,
        shard: &crate::shard_reader::MappedShard,
        row: usize,
    ) {
        let col = &self.schema.columns[col_idx];
        let col_size = col.size as usize;
        let buf = &mut self.col_bufs[col_idx];

        if is_null {
            buf.extend(std::iter::repeat(0u8).take(col_size));
            return;
        }

        let src = shard.get_col_ptr(row, payload_col_idx, col_size);

        if col.type_code == TYPE_STRING {
            self.write_string_column(col_idx, src, shard.blob_slice());
        } else {
            buf.extend_from_slice(src);
        }
    }

    pub fn write_string_column(&mut self, col_idx: usize, src_struct: &[u8], src_blob: &[u8]) {
        let length = read_u32_le(src_struct, 0) as usize;

        let mut dest = [0u8; 16];
        dest[0..4].copy_from_slice(&(length as u32).to_le_bytes());
        dest[4..8].copy_from_slice(&src_struct[4..8]);

        if length <= SHORT_STRING_THRESHOLD {
            let suffix_len = if length > 4 { length - 4 } else { 0 };
            if suffix_len > 0 {
                dest[8..8 + suffix_len].copy_from_slice(&src_struct[8..8 + suffix_len]);
            }
        } else if !src_blob.is_empty() {
            let old_offset = crate::util::read_u64_le(src_struct, 8) as usize;
            if old_offset + length <= src_blob.len() {
                let src_data = &src_blob[old_offset..old_offset + length];
                let new_offset = self.get_or_append_blob(src_data);
                dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
            }
        }

        self.col_bufs[col_idx].extend_from_slice(&dest);
    }

    pub fn get_or_append_blob(&mut self, data: &[u8]) -> usize {
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

    pub fn build_regions(&self) -> Vec<(*const u8, usize)> {
        let pk_index = self.schema.pk_index as usize;
        let mut regions: Vec<(*const u8, usize)> = Vec::new();
        regions.push((self.pk_lo.as_ptr(), self.pk_lo.len()));
        regions.push((self.pk_hi.as_ptr(), self.pk_hi.len()));
        regions.push((self.weight.as_ptr(), self.weight.len()));
        regions.push((self.null_bitmap.as_ptr(), self.null_bitmap.len()));
        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            regions.push((self.col_bufs[ci].as_ptr(), self.col_bufs[ci].len()));
        }
        regions.push((self.blob_heap.as_ptr(), self.blob_heap.len()));
        regions
    }

    pub fn finalize(&self, path: &CStr, table_id: u32, durable: bool) -> i32 {
        let regions = self.build_regions();
        let image = crate::shard_file::build_shard_image(
            table_id, self.count as u32, &regions,
        );
        crate::shard_file::write_shard_at(
            libc::AT_FDCWD, path, &image, durable,
        )
    }
}

fn is_null_in_shard(
    shard: &crate::shard_reader::MappedShard,
    row: usize,
    col_idx: usize,
    pk_index: usize,
) -> bool {
    let null_word = shard.get_null_word(row);
    let bit_pos = if col_idx < pk_index { col_idx } else { col_idx - 1 };
    (null_word & (1u64 << bit_pos)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema() -> SchemaDescriptor {
        use crate::compact::SchemaColumn;
        let mut sd = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        sd.columns[0] = SchemaColumn { type_code: 8, size: 8, nullable: 0, _pad: 0 }; // U64 PK
        sd.columns[1] = SchemaColumn { type_code: 9, size: 8, nullable: 0, _pad: 0 }; // I64 payload
        sd
    }

    #[test]
    fn add_row_flat_basic() {
        let schema = make_schema();
        let mut w = ShardWriter::new(&schema);

        let val: i64 = 42;
        let val_bytes = val.to_le_bytes();
        let col_ptrs = vec![val_bytes.as_ptr()];
        let col_sizes = vec![8u32];

        w.add_row_flat(1, 0, 1, 0, &col_ptrs, &col_sizes, std::ptr::null(), 0);
        assert_eq!(w.count, 1);
        assert_eq!(w.pk_lo.len(), 8);
        assert_eq!(w.col_bufs[1].len(), 8);
    }

    #[test]
    fn add_row_flat_skip_zero_weight() {
        let schema = make_schema();
        let mut w = ShardWriter::new(&schema);
        w.add_row_flat(1, 0, 0, 0, &[], &[], std::ptr::null(), 0);
        assert_eq!(w.count, 0);
    }

    #[test]
    fn finalize_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_writer.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let schema = make_schema();
        let mut w = ShardWriter::new(&schema);

        let v1: i64 = 100;
        let v2: i64 = 200;
        let v1b = v1.to_le_bytes();
        let v2b = v2.to_le_bytes();

        w.add_row_flat(10, 0, 1, 0, &[v1b.as_ptr()], &[8], std::ptr::null(), 0);
        w.add_row_flat(20, 0, 1, 0, &[v2b.as_ptr()], &[8], std::ptr::null(), 0);

        let rc = w.finalize(&cpath, 7, true);
        assert_eq!(rc, 0);

        // Read back
        let shard = crate::shard_reader::MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 2);
        assert_eq!(shard.get_pk_lo(0), 10);
        assert_eq!(shard.get_pk_lo(1), 20);
    }
}
