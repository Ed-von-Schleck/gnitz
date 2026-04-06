//! Rust-owned MemTable: manages sorted runs, Bloom filter, consolidation
//! cache, PK lookups, and shard flush.  Exposed to RPython as an opaque handle
//! via `gnitz_memtable_*` FFI functions.

use std::cmp::Ordering;
use std::ffi::CStr;
use std::sync::Arc;

use crate::bloom::BloomFilter;
use crate::columnar::{self, ColumnarSource};
use crate::schema::SchemaDescriptor;
use crate::merge::{self, MemBatch};
use crate::shard_file;
use crate::util::{read_i64_le, read_u64_le};

/// Error code returned when the MemTable exceeds its capacity.
pub const ERR_CAPACITY: i32 = -2;

/// Copy a 16-byte German String struct from source to destination, relocating
/// long-string blob data into `dst_blob`. Used by `append_rows_inner` and
/// `append_indexed_rows` to avoid duplicating the blob relocation logic.
fn relocate_string_cell(
    src_struct: &[u8],
    src_blob: &[u8],
    dst_col: &mut Vec<u8>,
    dst_blob: &mut Vec<u8>,
) {
    let (mut dest, is_long) = crate::schema::prep_german_string_copy(src_struct);
    if is_long {
        let length = u32::from_le_bytes(src_struct[0..4].try_into().unwrap()) as usize;
        let old_off = u64::from_le_bytes(src_struct[8..16].try_into().unwrap()) as usize;
        let new_off = dst_blob.len();
        dst_blob.extend_from_slice(&src_blob[old_off..old_off + length]);
        dest[8..16].copy_from_slice(&(new_off as u64).to_le_bytes());
    }
    dst_col.extend_from_slice(&dest);
}

/// Owned columnar batch.  Stores the same SoA layout as RPython's
/// `ArenaZSetBatch` but in Rust `Vec<u8>` buffers.
///
/// Used as the primary FFI batch handle (`gnitz_batch_*`), as well as
/// internally by MemTable, Table, and PartitionedTable.
#[derive(Clone)]
pub struct OwnedBatch {
    pub pk_lo: Vec<u8>,
    pub pk_hi: Vec<u8>,
    pub weight: Vec<u8>,
    pub null_bmp: Vec<u8>,
    pub col_data: Vec<Vec<u8>>,
    pub blob: Vec<u8>,
    pub count: usize,
    pub sorted: bool,
    pub consolidated: bool,
    pub schema: Option<SchemaDescriptor>,
}

impl OwnedBatch {
    /// Create an empty batch with the given number of payload columns.
    pub fn empty(num_payload_cols: usize) -> Self {
        OwnedBatch {
            pk_lo: Vec::new(),
            pk_hi: Vec::new(),
            weight: Vec::new(),
            null_bmp: Vec::new(),
            col_data: (0..num_payload_cols).map(|_| Vec::new()).collect(),
            blob: Vec::new(),
            count: 0,
            sorted: true,
            consolidated: true,
            schema: None,
        }
    }

    /// Create an empty batch with schema for append operations.
    pub fn with_schema(schema: SchemaDescriptor, initial_capacity: usize) -> Self {
        let npc = schema.num_columns as usize - 1;
        let pk_index = schema.pk_index as usize;
        let cap = initial_capacity.max(1);
        let mut col_data = Vec::with_capacity(npc);
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let cs = schema.columns[ci].size as usize;
            col_data.push(Vec::with_capacity(cap * cs));
        }
        OwnedBatch {
            pk_lo: Vec::with_capacity(cap * 8),
            pk_hi: Vec::with_capacity(cap * 8),
            weight: Vec::with_capacity(cap * 8),
            null_bmp: Vec::with_capacity(cap * 8),
            col_data,
            blob: Vec::with_capacity(64),
            count: 0,
            sorted: true,
            consolidated: true,
            schema: Some(schema),
        }
    }

    /// Construct by copying data in from FFI region pointers.
    ///
    /// Region layout per batch: pk_lo, pk_hi, weight, null_bmp,
    /// payload_col_0 .. payload_col_{N-1}, blob.
    ///
    /// # Safety
    /// `ptrs[i]` must point to at least `sizes[i]` readable bytes.
    pub unsafe fn from_regions(
        ptrs: &[*const u8],
        sizes: &[u32],
        count: usize,
        num_payload_cols: usize,
    ) -> Self {
        if count == 0 {
            return Self::empty(num_payload_cols);
        }

        let copy_region = |idx: usize| -> Vec<u8> {
            let sz = sizes[idx] as usize;
            if sz == 0 || ptrs[idx].is_null() {
                return Vec::new();
            }
            let mut v = vec![0u8; sz];
            std::ptr::copy_nonoverlapping(ptrs[idx], v.as_mut_ptr(), sz);
            v
        };

        let pk_lo = copy_region(0);
        let pk_hi = copy_region(1);
        let weight = copy_region(2);
        let null_bmp = copy_region(3);

        let mut col_data = Vec::with_capacity(num_payload_cols);
        for ci in 0..num_payload_cols {
            col_data.push(copy_region(4 + ci));
        }

        let blob = copy_region(4 + num_payload_cols);

        OwnedBatch {
            pk_lo,
            pk_hi,
            weight,
            null_bmp,
            col_data,
            blob,
            count,
            sorted: false,
            consolidated: false,
            schema: None,
        }
    }

    /// Create a borrowed `MemBatch` view over this batch's data.
    pub fn as_mem_batch(&self) -> MemBatch<'_> {
        MemBatch {
            pk_lo: &self.pk_lo,
            pk_hi: &self.pk_hi,
            weight: &self.weight,
            null_bmp: &self.null_bmp,
            col_data: self.col_data.iter().map(|v| v.as_slice()).collect(),
            blob: &self.blob,
            count: self.count,
        }
    }

    /// Total bytes occupied by all buffers.
    pub fn total_bytes(&self) -> usize {
        self.pk_lo.len()
            + self.pk_hi.len()
            + self.weight.len()
            + self.null_bmp.len()
            + self.col_data.iter().map(|v| v.len()).sum::<usize>()
            + self.blob.len()
    }

    #[inline]
    pub fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(&self.pk_lo, row * 8)
    }

    #[inline]
    pub fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(&self.pk_hi, row * 8)
    }

    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        crate::util::make_pk(self.get_pk_lo(row), self.get_pk_hi(row))
    }

    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(&self.weight, row * 8)
    }

    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.null_bmp, row * 8)
    }

    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = row * col_size;
        &self.col_data[payload_col][off..off + col_size]
    }

    /// Scatter-copy selected rows from a MemBatch into a new OwnedBatch.
    pub fn from_indexed_rows(
        batch: &MemBatch,
        indices: &[u32],
        schema: &SchemaDescriptor,
    ) -> Self {
        if indices.is_empty() {
            return Self::empty(schema.num_columns as usize - 1);
        }
        let blob_cap = batch.blob.len().max(1);
        write_to_owned_batch(schema, indices.len(), blob_cap, |writer| {
            merge::scatter_copy(batch, indices, &[], writer);
        })
    }

    /// Produce region pointer/size arrays suitable for `Table::ingest_batch_from_regions`.
    ///
    /// Layout: [pk_lo, pk_hi, weight, null_bmp, col_0..col_N-1, blob].
    /// The returned vecs borrow from `self` and must not outlive it.
    pub fn to_region_ptrs(&self) -> (Vec<*const u8>, Vec<u32>) {
        let npc = self.col_data.len();
        let cap = 4 + npc + 1; // pk_lo, pk_hi, weight, null_bmp, cols, blob
        let mut ptrs = Vec::with_capacity(cap);
        let mut sizes = Vec::with_capacity(cap);

        ptrs.push(self.pk_lo.as_ptr()); sizes.push(self.pk_lo.len() as u32);
        ptrs.push(self.pk_hi.as_ptr()); sizes.push(self.pk_hi.len() as u32);
        ptrs.push(self.weight.as_ptr()); sizes.push(self.weight.len() as u32);
        ptrs.push(self.null_bmp.as_ptr()); sizes.push(self.null_bmp.len() as u32);
        for col in &self.col_data {
            ptrs.push(col.as_ptr()); sizes.push(col.len() as u32);
        }
        ptrs.push(self.blob.as_ptr()); sizes.push(self.blob.len() as u32);

        (ptrs, sizes)
    }

    /// Clone all buffers into a new independent OwnedBatch.
    pub fn clone_batch(&self) -> Self {
        OwnedBatch {
            pk_lo: self.pk_lo.clone(),
            pk_hi: self.pk_hi.clone(),
            weight: self.weight.clone(),
            null_bmp: self.null_bmp.clone(),
            col_data: self.col_data.clone(),
            blob: self.blob.clone(),
            count: self.count,
            sorted: self.sorted,
            consolidated: self.consolidated,
            schema: self.schema,
        }
    }

    pub fn find_lower_bound(&self, key_lo: u64, key_hi: u64) -> usize {
        let target = crate::util::make_pk(key_lo, key_hi);
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.get_pk(mid) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Build the region pointer/size pairs used by `build_shard_image`.
    /// Append a single row from flat column data.
    ///
    /// `col_ptrs[i]` points to the i-th payload column's value (size = col_sizes[i]).
    /// For string columns (16-byte German String struct), long strings reference
    /// `blob_src[offset..offset+len]` which is copied into the batch's own blob arena.
    ///
    /// # Safety
    /// `col_ptrs[i]` must be valid for `col_sizes[i]` bytes.
    pub unsafe fn append_row(
        &mut self,
        pk_lo: u64,
        pk_hi: u64,
        weight: i64,
        null_word: u64,
        col_ptrs: &[*const u8],
        col_sizes: &[u32],
        blob_src: &[u8],
    ) {
        self.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        self.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());
        self.null_bmp.extend_from_slice(&null_word.to_le_bytes());

        let schema = self.schema;
        let pk_index = schema.map_or(usize::MAX, |s| s.pk_index as usize);
        let mut pi = 0;

        for (ci_raw, (ptr, &sz)) in col_ptrs.iter().zip(col_sizes.iter()).enumerate() {
            // Map raw column index to schema column index (skip PK)
            let ci = if pk_index == usize::MAX {
                ci_raw
            } else if ci_raw < pk_index {
                ci_raw
            } else {
                ci_raw + 1
            };

            let is_string = schema.map_or(false, |s| {
                ci < s.num_columns as usize
                    && s.columns[ci].type_code == crate::schema::type_code::STRING
            });
            let is_null = (null_word >> pi) & 1 != 0;
            let col_size = sz as usize;

            if is_null {
                let cur_len = self.col_data[pi].len();
                self.col_data[pi].resize(cur_len + col_size, 0);
            } else if is_string && col_size == 16 {
                // German String struct: handle blob relocation
                let src = std::slice::from_raw_parts(*ptr, 16);
                let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
                let mut dest = [0u8; 16];
                dest[0..8].copy_from_slice(&src[0..8]); // length + prefix

                if length <= crate::schema::SHORT_STRING_THRESHOLD {
                    let sfx = if length > 4 { length - 4 } else { 0 };
                    if sfx > 0 {
                        dest[8..8 + sfx].copy_from_slice(&src[8..8 + sfx]);
                    }
                } else {
                    let old_off = u64::from_le_bytes(src[8..16].try_into().unwrap()) as usize;
                    if old_off + length <= blob_src.len() {
                        let str_data = &blob_src[old_off..old_off + length];
                        let new_off = self.blob.len();
                        self.blob.extend_from_slice(str_data);
                        dest[8..16].copy_from_slice(&(new_off as u64).to_le_bytes());
                    } else {
                        // Inline string data (from RowBuilder with Python strings)
                        let actual_len = length.min(blob_src.len());
                        dest[0..4].copy_from_slice(&(actual_len as u32).to_le_bytes());
                        let new_off = self.blob.len();
                        self.blob.extend_from_slice(&blob_src[..actual_len]);
                        dest[8..16].copy_from_slice(&(new_off as u64).to_le_bytes());
                    }
                }
                self.col_data[pi].extend_from_slice(&dest);
            } else {
                let src = std::slice::from_raw_parts(*ptr, col_size);
                self.col_data[pi].extend_from_slice(src);
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Append a row from RowBuilder-style value arrays.
    ///
    /// `lo_values[i]`: i64 for INT types (float-as-bits for F64/F32, lo half for U128)
    /// `hi_values[i]`: u64 hi half for U128, 0 for all other types
    /// `str_ptrs[i]`: raw string bytes for STRING, null for non-STRING
    /// `str_lens[i]`: string byte length for STRING, 0 for non-STRING
    ///
    /// Schema MUST be set on the batch (panics if None).
    ///
    /// # Safety
    /// For STRING columns, `str_ptrs[i]` must be valid for `str_lens[i]` bytes
    /// (or null with len 0).
    pub unsafe fn append_row_simple(
        &mut self,
        pk_lo: u64, pk_hi: u64, weight: i64, null_word: u64,
        lo_values: &[i64],
        hi_values: &[u64],
        str_ptrs: &[*const u8],
        str_lens: &[u32],
    ) {
        self.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        self.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());
        self.null_bmp.extend_from_slice(&null_word.to_le_bytes());

        let schema = self.schema.expect("append_row_simple requires schema");
        let pk_index = schema.pk_index as usize;

        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col = &schema.columns[ci];
            let col_size = col.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;

            if is_null {
                let cur_len = self.col_data[pi].len();
                self.col_data[pi].resize(cur_len + col_size, 0);
            } else {
                match col.type_code {
                    crate::schema::type_code::STRING => {
                        let ptr = str_ptrs[pi];
                        let slen = str_lens[pi] as usize;
                        let bytes: &[u8] = if ptr.is_null() || slen == 0 {
                            &[]
                        } else {
                            std::slice::from_raw_parts(ptr, slen)
                        };
                        let gs = crate::schema::encode_german_string(bytes, &mut self.blob);
                        self.col_data[pi].extend_from_slice(&gs);
                    }
                    crate::schema::type_code::F64 => {
                        // lo_values[pi] already contains f64 bit pattern
                        self.col_data[pi].extend_from_slice(&lo_values[pi].to_le_bytes());
                    }
                    crate::schema::type_code::F32 => {
                        // RPython stores f32 via float2longlong(f64_val) which gives f64 bits
                        let f64_val = f64::from_bits(lo_values[pi] as u64);
                        let f32_val = f64_val as f32;
                        self.col_data[pi].extend_from_slice(&f32_val.to_le_bytes());
                    }
                    crate::schema::type_code::U128 => {
                        self.col_data[pi].extend_from_slice(&(lo_values[pi] as u64).to_le_bytes());
                        self.col_data[pi].extend_from_slice(&hi_values[pi].to_le_bytes());
                    }
                    _ => {
                        // All integer types: write lo_values[pi] truncated to col_size bytes
                        let bytes = lo_values[pi].to_le_bytes();
                        self.col_data[pi].extend_from_slice(&bytes[..col_size]);
                    }
                }
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Bulk-copy rows [start, end) from another OwnedBatch (same schema).
    pub fn append_batch(&mut self, src: &OwnedBatch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, false);
    }

    /// Bulk-copy rows with negated weights.
    pub fn append_batch_negated(&mut self, src: &OwnedBatch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, true);
    }

    fn append_rows_inner(&mut self, src: &OwnedBatch, start: usize, end: usize, negate: bool) {
        let n = end - start;
        self.pk_lo.extend_from_slice(&src.pk_lo[start * 8..end * 8]);
        self.pk_hi.extend_from_slice(&src.pk_hi[start * 8..end * 8]);
        if negate {
            for i in start..end {
                self.weight.extend_from_slice(&(-src.get_weight(i)).to_le_bytes());
            }
        } else {
            self.weight.extend_from_slice(&src.weight[start * 8..end * 8]);
        }
        self.null_bmp.extend_from_slice(&src.null_bmp[start * 8..end * 8]);

        // Payload columns: per-column bulk copy with string blob relocation
        let has_schema = self.schema.is_some();
        let schema_copy = self.schema;
        let pk_index = schema_copy.map_or(usize::MAX, |s| s.pk_index as usize);

        let mut pi = 0;
        let num_cols = schema_copy.map_or(self.col_data.len(), |s| s.num_columns as usize);
        for ci in 0..num_cols {
            if ci == pk_index { continue; }
            let is_string = has_schema
                && schema_copy.unwrap().columns[ci].type_code == crate::schema::type_code::STRING;
            let cs = if has_schema {
                schema_copy.unwrap().columns[ci].size as usize
            } else if src.count > 0 {
                src.col_data[pi].len() / src.count
            } else {
                0
            };

            if is_string && cs == 16 {
                for row in start..end {
                    let off = row * 16;
                    let src_struct = &src.col_data[pi][off..off + 16];
                    relocate_string_cell(src_struct, &src.blob, &mut self.col_data[pi], &mut self.blob);
                }
            } else if cs > 0 {
                self.col_data[pi].extend_from_slice(&src.col_data[pi][start * cs..end * cs]);
            }
            pi += 1;
            if pi >= self.col_data.len() { break; }
        }

        self.count += n;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Copy the found row from a PartitionedTable into this batch.
    /// Must be called after `ptable.retract_pk()` returned found=true.
    /// The `schema` must match `self.schema`.
    pub fn append_row_from_ptable_found(
        &mut self,
        ptable: &crate::partitioned_table::PartitionedTable,
        pk_lo: u64,
        pk_hi: u64,
        weight: i64,
    ) {
        let schema = self.schema.expect("append_row_from_ptable_found requires schema");
        let pk_index = schema.pk_index as usize;
        let null_word = ptable.found_null_word();

        self.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        self.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());
        self.null_bmp.extend_from_slice(&null_word.to_le_bytes());

        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                let new_len = self.col_data[pi].len() + cs;
                self.col_data[pi].resize(new_len, 0);
            } else if col_desc.type_code == crate::schema::type_code::STRING {
                let src = ptable.found_col_ptr(pi, cs);
                assert!(!src.is_null());
                let src_slice = unsafe { std::slice::from_raw_parts(src, cs) };
                crate::ops::write_string_from_raw(
                    &mut self.col_data[pi], &mut self.blob,
                    src_slice, ptable.found_blob_ptr(),
                );
            } else {
                let src = ptable.found_col_ptr(pi, cs);
                assert!(!src.is_null());
                let src_slice = unsafe { std::slice::from_raw_parts(src, cs) };
                self.col_data[pi].extend_from_slice(src_slice);
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Scatter-copy selected rows from a MemBatch directly into this batch.
    /// Avoids the intermediate allocation of `from_indexed_rows` + `append_batch`.
    /// Requires `self.schema` to be set (use `with_schema` to create the batch).
    pub fn append_indexed_rows(
        &mut self,
        src: &merge::MemBatch,
        indices: &[u32],
        schema: &crate::schema::SchemaDescriptor,
    ) {
        if indices.is_empty() {
            return;
        }
        // Pre-compute payload column metadata to avoid per-row schema scanning
        let pki = schema.pk_index as usize;
        let mut col_meta: Vec<(usize, bool)> = Vec::with_capacity(schema.num_columns as usize - 1);
        for ci in 0..schema.num_columns as usize {
            if ci == pki {
                continue;
            }
            let col = &schema.columns[ci];
            let is_string = col.type_code == crate::schema::type_code::STRING
                && col.size == 16;
            col_meta.push((col.size as usize, is_string));
        }

        for &idx in indices {
            let row = idx as usize;
            self.pk_lo.extend_from_slice(&src.get_pk_lo(row).to_le_bytes());
            self.pk_hi.extend_from_slice(&src.get_pk_hi(row).to_le_bytes());
            self.weight.extend_from_slice(&src.get_weight(row).to_le_bytes());
            let null_word = src.get_null_word(row);
            self.null_bmp.extend_from_slice(&null_word.to_le_bytes());

            for (pi, &(cs, is_string)) in col_meta.iter().enumerate() {
                let is_null = (null_word >> pi) & 1 != 0;
                if is_null {
                    let prev_len = self.col_data[pi].len();
                    self.col_data[pi].resize(prev_len + cs, 0);
                } else if is_string {
                    let src_struct = src.get_col_ptr(row, pi, 16);
                    relocate_string_cell(src_struct, src.blob, &mut self.col_data[pi], &mut self.blob);
                } else if cs > 0 {
                    self.col_data[pi].extend_from_slice(src.get_col_ptr(row, pi, cs));
                }
            }
            self.count += 1;
        }
        self.sorted = false;
        self.consolidated = false;
    }

    /// Reset to empty without freeing buffer allocations.
    pub fn clear(&mut self) {
        self.pk_lo.clear();
        self.pk_hi.clear();
        self.weight.clear();
        self.null_bmp.clear();
        for col in &mut self.col_data {
            col.clear();
        }
        self.blob.clear();
        self.count = 0;
        self.sorted = true;
        self.consolidated = true;
    }

    /// Number of regions in the standard layout.
    pub fn num_regions(&self) -> usize {
        4 + self.col_data.len() + 1
    }

    /// Get region pointer by index. Order: pk_lo(0), pk_hi(1), weight(2),
    /// null(3), payload cols(4..), blob(last).
    pub fn region_ptr(&self, idx: usize) -> *const u8 {
        match idx {
            0 => self.pk_lo.as_ptr(),
            1 => self.pk_hi.as_ptr(),
            2 => self.weight.as_ptr(),
            3 => self.null_bmp.as_ptr(),
            i if i < 4 + self.col_data.len() => self.col_data[i - 4].as_ptr(),
            _ => self.blob.as_ptr(), // last = blob
        }
    }

    /// Get region size by index.
    pub fn region_size(&self, idx: usize) -> usize {
        match idx {
            0 => self.pk_lo.len(),
            1 => self.pk_hi.len(),
            2 => self.weight.len(),
            3 => self.null_bmp.len(),
            i if i < 4 + self.col_data.len() => self.col_data[i - 4].len(),
            _ => self.blob.len(),
        }
    }

    pub fn regions(&self) -> Vec<(*const u8, usize)> {
        let mut r = vec![
            (self.pk_lo.as_ptr(), self.pk_lo.len()),
            (self.pk_hi.as_ptr(), self.pk_hi.len()),
            (self.weight.as_ptr(), self.weight.len()),
            (self.null_bmp.as_ptr(), self.null_bmp.len()),
        ];
        for col in &self.col_data {
            r.push((col.as_ptr(), col.len()));
        }
        r.push((self.blob.as_ptr(), self.blob.len()));
        r
    }
}

impl ColumnarSource for OwnedBatch {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        OwnedBatch::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        OwnedBatch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        &self.blob
    }
}

/// Allocate output buffers, run a merge/copy operation via DirectWriter,
/// truncate to actual size, and return the result as an OwnedBatch.
pub fn write_to_owned_batch(
    schema: &SchemaDescriptor,
    max_rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut merge::DirectWriter),
) -> OwnedBatch {
    let pk_index = schema.pk_index as usize;
    let num_payload_cols = schema.num_columns as usize - 1;

    let mut out_pk_lo = vec![0u8; max_rows * 8];
    let mut out_pk_hi = vec![0u8; max_rows * 8];
    let mut out_weight = vec![0u8; max_rows * 8];
    let mut out_null = vec![0u8; max_rows * 8];
    let mut out_cols: Vec<Vec<u8>> = Vec::with_capacity(num_payload_cols);
    for ci in 0..schema.num_columns as usize {
        if ci == pk_index { continue; }
        let cs = schema.columns[ci].size as usize;
        out_cols.push(vec![0u8; max_rows * cs]);
    }
    let mut out_blob = vec![0u8; max_blob];

    let col_slices: Vec<&mut [u8]> = unsafe {
        out_cols.iter_mut()
            .map(|v| std::slice::from_raw_parts_mut(v.as_mut_ptr(), v.len()))
            .collect()
    };

    let mut writer = merge::DirectWriter::new(
        &mut out_pk_lo, &mut out_pk_hi, &mut out_weight, &mut out_null,
        col_slices, &mut out_blob, *schema,
    );

    write_fn(&mut writer);

    let actual_rows = writer.row_count();
    let actual_blob = writer.blob_written();

    out_pk_lo.truncate(actual_rows * 8);
    out_pk_hi.truncate(actual_rows * 8);
    out_weight.truncate(actual_rows * 8);
    out_null.truncate(actual_rows * 8);
    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_index { continue; }
        let cs = schema.columns[ci].size as usize;
        out_cols[pi].truncate(actual_rows * cs);
        pi += 1;
    }
    out_blob.truncate(actual_blob);

    OwnedBatch {
        pk_lo: out_pk_lo,
        pk_hi: out_pk_hi,
        weight: out_weight,
        null_bmp: out_null,
        col_data: out_cols,
        blob: out_blob,
        count: actual_rows,
        sorted: false,
        consolidated: false,
        schema: None,
    }
}

/// Merge N sorted MemBatch views into a single consolidated OwnedBatch.
fn consolidate_batches(
    batches: &[MemBatch],
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let num_payload_cols = schema.num_columns as usize - 1;
    if batches.is_empty() {
        return OwnedBatch::empty(num_payload_cols);
    }

    let total_rows: usize = batches.iter().map(|b| b.count).sum();
    let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();
    if total_rows == 0 {
        return OwnedBatch::empty(num_payload_cols);
    }

    let result = write_to_owned_batch(schema, total_rows, total_blob, |writer| {
        if batches.len() == 1 {
            merge::sort_and_consolidate(&batches[0], schema, writer);
        } else {
            merge::merge_batches(batches, schema, writer);
        }
    });
    result
}


/// Snapshot handle returned to RPython.
pub struct MemTableSnapshot {
    pub inner: Arc<OwnedBatch>,
}

pub struct MemTable {
    runs: Vec<OwnedBatch>,
    bloom: BloomFilter,
    schema: SchemaDescriptor,
    max_bytes: usize,
    total_row_count: usize,
    runs_bytes: usize,
    cached_consolidated: Option<Arc<OwnedBatch>>,
    // Last lookup result (set by lookup_pk, read by found_* accessors)
    found_run: usize,
    found_row: usize,
    has_found: bool,
}

impl MemTable {
    pub fn new(schema: SchemaDescriptor, max_bytes: usize) -> Self {
        let capacity = (max_bytes / 40).max(16); // rough estimate
        MemTable {
            runs: Vec::with_capacity(8),
            bloom: BloomFilter::new(capacity as u32),
            schema,
            max_bytes,
            total_row_count: 0,
            runs_bytes: 0,
            cached_consolidated: None,
            found_run: 0,
            found_row: 0,
            has_found: false,
        }
    }

    /// Append a pre-sorted batch as a new run.
    pub fn upsert_sorted_batch(&mut self, batch: OwnedBatch) -> Result<(), i32> {
        if batch.count == 0 {
            return Ok(());
        }
        self.check_capacity()?;

        // Add all PKs to bloom
        for i in 0..batch.count {
            self.bloom.add(batch.get_pk_lo(i), batch.get_pk_hi(i));
        }

        self.total_row_count += batch.count;
        self.runs_bytes += batch.total_bytes();
        self.runs.push(batch);
        self.invalidate_cache();
        Ok(())
    }

    /// Add a single PK to the bloom filter (for RPython accumulator rows).
    pub fn bloom_add(&mut self, key_lo: u64, key_hi: u64) {
        self.bloom.add(key_lo, key_hi);
    }

    pub fn may_contain_pk(&self, key_lo: u64, key_hi: u64) -> bool {
        self.bloom.may_contain(key_lo, key_hi)
    }

    pub fn should_flush(&self) -> bool {
        self.runs_bytes > self.max_bytes * 3 / 4
    }

    pub fn is_empty(&self) -> bool {
        self.total_row_count == 0
    }

    pub fn total_row_count(&self) -> usize {
        self.total_row_count
    }

    /// Get a consolidated snapshot.  Caches the merged result of all runs.
    /// Returns an `Arc<OwnedBatch>` — cheap to clone for multiple consumers.
    pub fn get_snapshot(&mut self) -> Arc<OwnedBatch> {
        let num_payload_cols = self.schema.num_columns as usize - 1;

        if self.cached_consolidated.is_none() && !self.runs.is_empty() {
            let batches: Vec<MemBatch> =
                self.runs.iter().map(|r| r.as_mem_batch()).collect();
            let consolidated = consolidate_batches(&batches, &self.schema);
            self.cached_consolidated = Some(Arc::new(consolidated));
        }

        match &self.cached_consolidated {
            Some(arc) => Arc::clone(arc),
            None => Arc::new(OwnedBatch::empty(num_payload_cols)),
        }
    }

    /// Look up a PK across all sorted runs.
    ///
    /// Returns `(net_weight, Some((run_idx, row_idx)))` if found, else
    /// `(0, None)`.
    pub fn lookup_pk(&mut self, key_lo: u64, key_hi: u64) -> (i64, bool) {
        let mut total_w: i64 = 0;
        self.has_found = false;

        for (ri, run) in self.runs.iter().enumerate() {
            let mut lo = run.find_lower_bound(key_lo, key_hi);
            while lo < run.count && run.get_pk_lo(lo) == key_lo && run.get_pk_hi(lo) == key_hi {
                total_w += run.get_weight(lo);
                if !self.has_found {
                    self.found_run = ri;
                    self.found_row = lo;
                    self.has_found = true;
                }
                lo += 1;
            }
        }

        (total_w, self.has_found)
    }

    fn found_entry(&self) -> Option<(&OwnedBatch, usize)> {
        if self.has_found && self.found_run < self.runs.len() {
            Some((&self.runs[self.found_run], self.found_row))
        } else {
            None
        }
    }

    pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
        match self.found_entry() {
            Some((run, row)) => run.get_col_ptr(row, payload_col, col_size).as_ptr(),
            None => std::ptr::null(),
        }
    }

    pub fn found_null_word(&self) -> u64 {
        match self.found_entry() {
            Some((run, row)) => run.get_null_word(row),
            None => 0,
        }
    }

    pub fn found_blob_ptr(&self) -> *const u8 {
        match self.found_entry() {
            Some((run, _)) => run.blob.as_ptr(),
            None => std::ptr::null(),
        }
    }

    /// Find the net weight for rows matching both PK and full payload.
    pub fn find_weight_for_row(
        &self,
        key_lo: u64,
        key_hi: u64,
        ref_batch: &MemBatch,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        for run in &self.runs {
            let mut lo = run.find_lower_bound(key_lo, key_hi);
            while lo < run.count && run.get_pk_lo(lo) == key_lo && run.get_pk_hi(lo) == key_hi {
                let ord = columnar::compare_rows(
                    &self.schema, run, lo, ref_batch, ref_row,
                );
                if ord == Ordering::Equal {
                    total_w += run.get_weight(lo);
                }
                lo += 1;
            }
        }

        total_w
    }

    /// Find the first memtable row whose (PK, payload) has positive net weight
    /// across all runs.  Sets `found_run`/`found_row`/`has_found` on success.
    /// Returns true if such a row was found, false otherwise.
    ///
    /// Used by `Table::retract_pk` to locate the live row for an UPDATE+DELETE
    /// sequence where the old payload has been cancelled but the new payload
    /// is still positive.
    pub fn find_positive_payload_row(&mut self, key_lo: u64, key_hi: u64) -> bool {
        // Pass 1: collect all PK-matching (run_idx, row_idx) pairs.
        // The iterator borrow on self.runs is fully dropped before pass 2,
        // avoiding any aliasing issue when find_weight_for_row also borrows self.runs.
        let mut candidates: Vec<(usize, usize)> = Vec::new();
        for (ri, run) in self.runs.iter().enumerate() {
            let mut lo = run.find_lower_bound(key_lo, key_hi);
            while lo < run.count
                && run.get_pk_lo(lo) == key_lo
                && run.get_pk_hi(lo) == key_hi
            {
                candidates.push((ri, lo));
                lo += 1;
            }
        }

        // Pass 2: for each candidate, compute net (PK, payload) weight.
        // The first candidate with positive net weight is the live row.
        for &(ri, row_idx) in &candidates {
            let mb = self.runs[ri].as_mem_batch();
            let net_w = self.find_weight_for_row(key_lo, key_hi, &mb, row_idx);
            if net_w > 0 {
                self.found_run = ri;
                self.found_row = row_idx;
                self.has_found = true;
                return true;
            }
        }

        self.has_found = false;
        false
    }

    /// Consolidate all runs and write to a shard file.
    ///
    /// Returns 0 on success, -1 if empty (no file written), or a negative
    /// error code from `write_shard_at`.
    pub fn flush(
        &mut self,
        dirfd: libc::c_int,
        basename: &CStr,
        table_id: u32,
        durable: bool,
    ) -> i32 {
        let snapshot = self.get_snapshot();
        if snapshot.count == 0 {
            return -1; // nothing to write
        }

        let regions = snapshot.regions();
        let image = shard_file::build_shard_image(
            table_id,
            snapshot.count as u32,
            &regions,
        );
        shard_file::write_shard_at(dirfd, basename, &image, durable)
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Clear all runs, bloom filter, and cache.  Ready for reuse.
    pub fn reset(&mut self) {
        self.runs.clear();
        self.runs_bytes = 0;
        self.total_row_count = 0;
        self.cached_consolidated = None;
        self.has_found = false;
        self.bloom.reset();
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    fn invalidate_cache(&mut self) {
        self.cached_consolidated = None;
    }

    fn check_capacity(&self) -> Result<(), i32> {
        if self.runs_bytes > self.max_bytes {
            return Err(ERR_CAPACITY);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

    fn make_u64_i64_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        // Col 0: PK (U64)
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        // Col 1: payload (I64)
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    /// Build a sorted OwnedBatch from (pk, weight, payload) triples.
    /// Assumes triples are already sorted by pk.
    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> OwnedBatch {
        let n = rows.len();
        let mut pk_lo = Vec::with_capacity(n * 8);
        let mut pk_hi = Vec::with_capacity(n * 8);
        let mut weight = Vec::with_capacity(n * 8);
        let mut null_bmp = Vec::with_capacity(n * 8);
        let mut col0 = Vec::with_capacity(n * 8);

        for &(pk, w, val) in rows {
            pk_lo.extend_from_slice(&pk.to_le_bytes());
            pk_hi.extend_from_slice(&0u64.to_le_bytes());
            weight.extend_from_slice(&w.to_le_bytes());
            null_bmp.extend_from_slice(&0u64.to_le_bytes());
            col0.extend_from_slice(&val.to_le_bytes());
        }

        OwnedBatch {
            pk_lo,
            pk_hi,
            weight,
            null_bmp,
            col_data: vec![col0],
            blob: Vec::new(),
            count: n,
            sorted: true,
            consolidated: false,
            schema: Some(*schema),
        }
    }

    #[test]
    fn owned_batch_roundtrip() {
        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        assert_eq!(batch.count, 2);
        assert_eq!(batch.get_pk_lo(0), 10);
        assert_eq!(batch.get_pk_lo(1), 20);

        let mb = batch.as_mem_batch();
        assert_eq!(mb.count, 2);
        assert_eq!(mb.get_pk_lo(0), 10);
        assert_eq!(mb.get_weight(1), 1);
    }

    #[test]
    fn memtable_upsert_and_snapshot() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        assert!(mt.is_empty());

        // Upsert two runs
        let b1 = make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 1, 200), (30, -1, 300)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();
        assert_eq!(mt.total_row_count(), 4);

        // Snapshot should consolidate: PK 30 has +1 -1 = 0 (ghost)
        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 2); // PK 10 and 20 survive
        assert_eq!(snap.get_pk_lo(0), 10);
        assert_eq!(snap.get_pk_lo(1), 20);
    }

    #[test]
    fn memtable_snapshot_caching() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let s1 = mt.get_snapshot();
        let s2 = mt.get_snapshot();
        // Should be the same Arc (cached)
        assert!(Arc::ptr_eq(&s1, &s2));

        // After new upsert, cache is invalidated
        let b2 = make_batch(&schema, &[(20, 1, 200)]);
        mt.upsert_sorted_batch(b2).unwrap();
        let s3 = mt.get_snapshot();
        assert!(!Arc::ptr_eq(&s1, &s3));
        assert_eq!(s3.count, 2);
    }

    #[test]
    fn memtable_lookup_pk() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 2, 200)]); // PK 20 appears in two runs
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        let (w, found) = mt.lookup_pk(20, 0);
        assert_eq!(w, 3); // 1 + 2
        assert!(found);

        let (w, found) = mt.lookup_pk(99, 0);
        assert_eq!(w, 0);
        assert!(!found);
    }

    #[test]
    fn memtable_bloom() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        assert!(mt.may_contain_pk(10, 0));
        assert!(mt.may_contain_pk(20, 0));
        // 99 might be a false positive, but definitely not in data
    }

    #[test]
    fn memtable_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();
        assert!(!mt.is_empty());

        mt.reset();
        assert!(mt.is_empty());
        assert_eq!(mt.total_row_count(), 0);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn snapshot_survives_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 1);

        mt.reset();
        // Snapshot still valid (Arc keeps data alive)
        assert_eq!(snap.count, 1);
        assert_eq!(snap.get_pk_lo(0), 10);
    }

    #[test]
    fn memtable_capacity_error() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 64); // tiny capacity

        // First upsert fills past max_bytes (4 rows × 40 bytes = 160 > 64)
        let b1 = make_batch(
            &schema,
            &[(10, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)],
        );
        mt.upsert_sorted_batch(b1).unwrap(); // OK — check fires before adding

        // Second upsert fails: runs_bytes (160) > max_bytes (64)
        let b2 = make_batch(&schema, &[(50, 1, 500)]);
        let rc = mt.upsert_sorted_batch(b2);
        assert_eq!(rc, Err(ERR_CAPACITY));
    }

    #[test]
    fn memtable_find_weight_for_row() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // PK 10 with payload 100
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        // PK 10 with payload 200 (different row identity)
        let b2 = make_batch(&schema, &[(10, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // Search for PK 10, payload 100 — should find weight 1
        let ref_batch = make_batch(&schema, &[(10, 1, 100)]);
        let ref_mb = ref_batch.as_mem_batch();
        let w = mt.find_weight_for_row(10, 0, &ref_mb, 0);
        assert_eq!(w, 1);

        // Search for PK 10, payload 200 — should find weight 1
        let ref_batch2 = make_batch(&schema, &[(10, 1, 200)]);
        let ref_mb2 = ref_batch2.as_mem_batch();
        let w2 = mt.find_weight_for_row(10, 0, &ref_mb2, 0);
        assert_eq!(w2, 1);

        // Search for PK 10, payload 999 — should find weight 0
        let ref_batch3 = make_batch(&schema, &[(10, 1, 999)]);
        let ref_mb3 = ref_batch3.as_mem_batch();
        let w3 = mt.find_weight_for_row(10, 0, &ref_mb3, 0);
        assert_eq!(w3, 0);
    }

    #[test]
    fn memtable_flush_to_shard() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            libc::open(
                dir.path().as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDONLY | libc::O_DIRECTORY,
            )
        };
        assert!(dirfd >= 0);

        let name = std::ffi::CString::new("test_shard.db").unwrap();
        let rc = mt.flush(dirfd, &name, 42, false);
        assert_eq!(rc, 0);

        // Verify shard file exists
        let shard_path = dir.path().join("test_shard.db");
        assert!(shard_path.exists());
        assert!(std::fs::metadata(&shard_path).unwrap().len() > 0);

        unsafe { libc::close(dirfd); }
    }

    #[test]
    fn memtable_flush_empty_returns_neg1() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            libc::open(
                dir.path().as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDONLY | libc::O_DIRECTORY,
            )
        };
        assert!(dirfd >= 0);

        let name = std::ffi::CString::new("empty.db").unwrap();
        let rc = mt.flush(dirfd, &name, 42, false);
        assert_eq!(rc, -1);
        unsafe { libc::close(dirfd); }
    }

    #[test]
    fn batch_append_batch() {
        let schema = make_u64_i64_schema();
        let src = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let mut dst = OwnedBatch::with_schema(schema, 8);

        // Append all rows
        dst.append_batch(&src, 0, 3);
        assert_eq!(dst.count, 3);
        assert_eq!(dst.get_pk_lo(0), 10);
        assert_eq!(dst.get_pk_lo(2), 30);
        assert_eq!(dst.get_weight(1), 1);

        // Append subset
        dst.clear();
        dst.append_batch(&src, 1, 2);
        assert_eq!(dst.count, 1);
        assert_eq!(dst.get_pk_lo(0), 20);
    }

    #[test]
    fn batch_append_batch_negated() {
        let schema = make_u64_i64_schema();
        let src = make_batch(&schema, &[(10, 1, 100), (20, 2, 200)]);
        let mut dst = OwnedBatch::with_schema(schema, 8);

        dst.append_batch_negated(&src, 0, 2);
        assert_eq!(dst.count, 2);
        assert_eq!(dst.get_weight(0), -1);
        assert_eq!(dst.get_weight(1), -2);
        assert_eq!(dst.get_pk_lo(0), 10);
    }

    #[test]
    fn batch_region_access() {
        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(10, 1, 100)]);

        // Schema has 2 columns: PK (U64) + payload (I64)
        // Regions: pk_lo(0), pk_hi(1), weight(2), null(3), col0(4), blob(5)
        assert_eq!(batch.num_regions(), 6);
        assert_eq!(batch.region_size(0), 8); // pk_lo: 1 row * 8 bytes
        assert_eq!(batch.region_size(4), 8); // col0: 1 row * 8 bytes
        assert!(!batch.region_ptr(0).is_null());
    }

    #[test]
    fn batch_clear() {
        let schema = make_u64_i64_schema();
        let mut batch = make_batch(&schema, &[(10, 1, 100)]);
        assert_eq!(batch.count, 1);

        batch.clear();
        assert_eq!(batch.count, 0);
        assert!(batch.sorted);
        assert!(batch.consolidated);
    }

    /// Schema matching reduce output: U128 PK (pk_index=0) + I64 group_val + I64 agg_val
    fn make_reduce_schema() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn::new(0, 0); 64],
        };
        sd.columns[0] = SchemaColumn::new(type_code::U128, 0); // U128 PK
        sd.columns[1] = SchemaColumn::new(type_code::I64, 0);  // I64 group_val
        sd.columns[2] = SchemaColumn::new(type_code::I64, 0);  // I64 agg_val
        sd
    }

    /// Build a 3-column OwnedBatch from (pk_lo, pk_hi, weight, group_val, agg_val) tuples.
    fn make_reduce_batch(rows: &[(u64, u64, i64, i64, i64)]) -> OwnedBatch {
        let n = rows.len();
        let mut pk_lo = vec![0u8; n * 8];
        let mut pk_hi = vec![0u8; n * 8];
        let mut weight = vec![0u8; n * 8];
        let null_bmp = vec![0u8; n * 8];
        let mut col0 = vec![0u8; n * 8]; // group_val (I64)
        let mut col1 = vec![0u8; n * 8]; // agg_val (I64)
        for (i, &(plo, phi, w, gv, av)) in rows.iter().enumerate() {
            pk_lo[i*8..i*8+8].copy_from_slice(&plo.to_le_bytes());
            pk_hi[i*8..i*8+8].copy_from_slice(&phi.to_le_bytes());
            weight[i*8..i*8+8].copy_from_slice(&w.to_le_bytes());
            // null_bmp stays zero (no nulls)
            col0[i*8..i*8+8].copy_from_slice(&gv.to_le_bytes());
            col1[i*8..i*8+8].copy_from_slice(&av.to_le_bytes());
        }
        let batch = OwnedBatch {
            pk_lo, pk_hi, weight, null_bmp,
            col_data: vec![col0, col1],
            blob: Vec::new(),
            count: n,
            sorted: true,
            consolidated: false,
            schema: None,
        };
        batch
    }

    /// Reproduce the reduce output pattern: insertion + retraction across ticks.
    /// After consolidation, only the LAST tick's aggregate should survive.
    #[test]
    fn test_reduce_output_consolidation_3col() {
        let schema = make_reduce_schema();

        // Tick 1: insert (PK=0, group=0, sum=5000, w=+1)
        let run1 = make_reduce_batch(&[(0, 0, 1, 0, 5000)]);
        // Tick 2: retract old, insert new
        let run2 = make_reduce_batch(&[
            (0, 0, -1, 0, 5000),   // retract sum=5000
            (0, 0,  1, 0, 10000),  // insert sum=10000
        ]);
        // Tick 3: retract old, insert new
        let run3 = make_reduce_batch(&[
            (0, 0, -1, 0, 10000),  // retract sum=10000
            (0, 0,  1, 0, 15000),  // insert sum=15000
        ]);

        let batches: Vec<crate::merge::MemBatch> = vec![
            run1.as_mem_batch(),
            run2.as_mem_batch(),
            run3.as_mem_batch(),
        ];

        let consolidated = consolidate_batches(&batches, &schema);

        // After consolidation: only (PK=0, group=0, sum=15000, w=+1) should remain
        assert_eq!(consolidated.count, 1,
            "expected 1 row after consolidation, got {}", consolidated.count);
        assert_eq!(consolidated.get_pk_lo(0), 0);
        assert_eq!(consolidated.get_weight(0), 1);
        // Check agg_val (payload column 1) = 15000
        let agg_bytes = consolidated.get_col_ptr(0, 1, 8);
        let agg_val = i64::from_le_bytes(agg_bytes.try_into().unwrap());
        assert_eq!(agg_val, 15000, "expected agg_val=15000, got {}", agg_val);
    }

    // ── append_row_simple tests ──────────────────────────────────────────

    fn make_schema_cols(cols: &[(u8, u8)], pk_index: u32) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        for (i, &(tc, nullable)) in cols.iter().enumerate() {
            columns[i] = SchemaColumn::new(tc, nullable);
        }
        SchemaDescriptor { num_columns: cols.len() as u32, pk_index, columns }
    }

    fn decode_str(batch: &OwnedBatch, row: usize, payload_col: usize) -> Vec<u8> {
        let raw = batch.get_col_ptr(row, payload_col, 16);
        let st: [u8; 16] = raw.try_into().unwrap();
        crate::schema::decode_german_string(&st, &batch.blob)
    }

    #[test]
    fn test_append_row_simple_nullable_string() {
        // Schema: U64(pk=0), STRING(nullable)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),
            (type_code::STRING, 1),
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 4);

        // Row 0: non-null string "not null"
        let s = b"not null";
        let lo = [0i64]; // not used for STRING
        let hi = [0u64];
        let ptrs = [s.as_ptr()];
        let lens = [s.len() as u32];
        unsafe {
            batch.append_row_simple(1, 0, 1, 0, &lo, &hi, &ptrs, &lens);
        }

        // Row 1: null string (null_word bit 0 set)
        let null_ptr: *const u8 = std::ptr::null();
        let ptrs2 = [null_ptr];
        let lens2 = [0u32];
        unsafe {
            batch.append_row_simple(2, 0, 1, 1, &[0i64], &[0u64], &ptrs2, &lens2);
        }

        assert_eq!(batch.count, 2);
        // Row 0: string decodes correctly
        assert_eq!(decode_str(&batch, 0, 0), b"not null");
        // Row 0: not null
        assert_eq!(batch.get_null_word(0) & 1, 0);
        // Row 1: null bit set
        assert_eq!(batch.get_null_word(1) & 1, 1);
        // Row 1: col data is zeroed
        let raw = batch.get_col_ptr(1, 0, 16);
        assert!(raw.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_append_row_simple_multi_string() {
        // Schema: U64(pk=0), STRING(name), STRING(desc)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),
            (type_code::STRING, 0),
            (type_code::STRING, 0),
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 4);

        let cases: &[(&[u8], &[u8])] = &[
            (b"Alice", b"short"),
            (b"Bob has a long name!", b"Also quite a long description"),
            (b"", b"nonempty"),
            (b"mix", b"another long one for blob storage"),
        ];

        for (pk, (name, desc)) in cases.iter().enumerate() {
            let lo = [0i64, 0i64];
            let hi = [0u64, 0u64];
            let ptrs = [name.as_ptr(), desc.as_ptr()];
            let lens = [name.len() as u32, desc.len() as u32];
            unsafe {
                batch.append_row_simple(pk as u64, 0, 1, 0, &lo, &hi, &ptrs, &lens);
            }
        }

        assert_eq!(batch.count, 4);
        for (i, (name, desc)) in cases.iter().enumerate() {
            assert_eq!(decode_str(&batch, i, 0), *name, "row {i} name mismatch");
            assert_eq!(decode_str(&batch, i, 1), *desc, "row {i} desc mismatch");
        }
    }

    #[test]
    fn test_append_row_simple_all_types() {
        // Schema: U64(pk=0), then one of each remaining type
        // Payload cols (pi): U8(0), I8(1), U16(2), I16(3), U32(4), I32(5),
        //                     F32(6), U64(7), I64(8), F64(9), STRING(10), U128(11)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),    // pk
            (type_code::U8, 0),     // pi 0
            (type_code::I8, 0),     // pi 1
            (type_code::U16, 0),    // pi 2
            (type_code::I16, 0),    // pi 3
            (type_code::U32, 0),    // pi 4
            (type_code::I32, 0),    // pi 5
            (type_code::F32, 0),    // pi 6
            (type_code::U64, 0),    // pi 7
            (type_code::I64, 0),    // pi 8
            (type_code::F64, 0),    // pi 9
            (type_code::STRING, 0), // pi 10
            (type_code::U128, 0),   // pi 11
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 1);

        let n = 12;
        let mut lo = vec![0i64; n];
        let mut hi = vec![0u64; n];
        let mut ptrs = vec![std::ptr::null::<u8>(); n];
        let mut lens = vec![0u32; n];

        lo[0] = 42;        // U8: 42
        lo[1] = -7;        // I8: -7
        lo[2] = 1000;      // U16: 1000
        lo[3] = -500;      // I16: -500
        lo[4] = 70000;     // U32: 70000
        lo[5] = -12345;    // I32: -12345
        // F32: 3.14 → store as f64 bit pattern (RPython convention)
        lo[6] = f64::to_bits(3.14f64) as i64;
        lo[7] = 0x1234_5678_9ABC_DEF0u64 as i64;  // U64
        lo[8] = -99999;    // I64
        // F64: 2.718281828 → store as bit pattern
        lo[9] = f64::to_bits(2.718281828f64) as i64;
        // STRING: "hello world!"
        let s = b"hello world!";
        ptrs[10] = s.as_ptr();
        lens[10] = s.len() as u32;
        // U128: lo=0xDEADBEEF, hi=0xCAFEBABE
        lo[11] = 0xDEADBEEFu64 as i64;
        hi[11] = 0xCAFEBABE;

        unsafe {
            batch.append_row_simple(100, 0, 1, 0, &lo, &hi, &ptrs, &lens);
        }

        assert_eq!(batch.count, 1);

        // U8
        assert_eq!(batch.get_col_ptr(0, 0, 1), &[42]);
        // I8
        assert_eq!(batch.get_col_ptr(0, 1, 1), &[(-7i8) as u8]);
        // U16
        assert_eq!(batch.get_col_ptr(0, 2, 2), &1000u16.to_le_bytes());
        // I16
        assert_eq!(batch.get_col_ptr(0, 3, 2), &(-500i16).to_le_bytes());
        // U32
        assert_eq!(batch.get_col_ptr(0, 4, 4), &70000u32.to_le_bytes());
        // I32
        assert_eq!(batch.get_col_ptr(0, 5, 4), &(-12345i32).to_le_bytes());
        // F32: 3.14f64 as f32
        let f32_bytes = batch.get_col_ptr(0, 6, 4);
        let f32_val = f32::from_le_bytes(f32_bytes.try_into().unwrap());
        assert!((f32_val - 3.14f32).abs() < 1e-5, "f32: {f32_val}");
        // U64
        assert_eq!(batch.get_col_ptr(0, 7, 8), &0x1234_5678_9ABC_DEF0u64.to_le_bytes());
        // I64
        assert_eq!(batch.get_col_ptr(0, 8, 8), &(-99999i64).to_le_bytes());
        // F64
        let f64_bytes = batch.get_col_ptr(0, 9, 8);
        let f64_val = f64::from_le_bytes(f64_bytes.try_into().unwrap());
        assert!((f64_val - 2.718281828).abs() < 1e-9, "f64: {f64_val}");
        // STRING
        assert_eq!(decode_str(&batch, 0, 10), b"hello world!");
        // U128
        let u128_bytes = batch.get_col_ptr(0, 11, 16);
        let u128_lo = u64::from_le_bytes(u128_bytes[0..8].try_into().unwrap());
        let u128_hi = u64::from_le_bytes(u128_bytes[8..16].try_into().unwrap());
        assert_eq!(u128_lo, 0xDEADBEEF);
        assert_eq!(u128_hi, 0xCAFEBABE);
    }

    /// Verify that `append_row` writes the correct length into the German String
    /// header when blob_src is shorter than the declared length field.
    #[test]
    fn test_append_row_blob_length_header() {
        let schema = make_schema_cols(&[
            (type_code::U64, 0),    // PK
            (type_code::STRING, 0), // STRING payload
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 1);

        // Build a German String struct with length=20 but only 5 blob bytes available.
        // This triggers the inline-blob fallback branch in append_row.
        let mut gs_struct = [0u8; 16];
        gs_struct[0..4].copy_from_slice(&20u32.to_le_bytes()); // declared length = 20
        gs_struct[4..8].copy_from_slice(&0u32.to_le_bytes());  // prefix bytes = 0
        gs_struct[8..16].copy_from_slice(&0u64.to_le_bytes()); // blob offset = 0

        let blob_src = b"hello"; // only 5 bytes

        let col_ptrs = [gs_struct.as_ptr()];
        let col_sizes = [16u32];
        unsafe {
            batch.append_row(1, 0, 1, 0, &col_ptrs, &col_sizes, blob_src);
        }

        // The stored German String header must have length=5 (actual bytes written),
        // not length=20 (declared length that exceeded blob_src).
        let stored_gs = batch.get_col_ptr(0, 0, 16);
        let stored_len = u32::from_le_bytes(stored_gs[0..4].try_into().unwrap());
        assert_eq!(stored_len, 5, "length header should reflect actual bytes written");

        // The blob arena should contain exactly "hello".
        assert_eq!(&batch.blob, b"hello");
    }

    /// Verify that `find_positive_payload_row` locates the row with positive
    /// net weight, skipping rows whose (PK, payload) is cancelled out.
    #[test]
    fn test_find_positive_payload_row() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Run 0: INSERT (PK=10, val=100, weight=+1)
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        // Run 1: UPDATE delta — retract val=100, insert val=200
        let b2 = make_batch(&schema, &[(10, -1, 100), (10, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // Net weights: val=100 → 1-1=0, val=200 → 1 (positive)
        let found = mt.find_positive_payload_row(10, 0);
        assert!(found, "should find a live row");
        assert!(mt.has_found);

        // The found row should have payload = 200
        let col_ptr = mt.found_col_ptr(0, 8);
        assert!(!col_ptr.is_null());
        let val = i64::from_le_bytes(
            unsafe { std::slice::from_raw_parts(col_ptr, 8) }.try_into().unwrap(),
        );
        assert_eq!(val, 200, "found row should be the live val=200 row, not the retracted val=100");

        // PK with no rows at all → not found
        let found2 = mt.find_positive_payload_row(99, 0);
        assert!(!found2);
        assert!(!mt.has_found);
    }

}
