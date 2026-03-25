//! Rust-owned MemTable: in-memory Z-Set accumulator with sorted runs,
//! bloom filter, consolidation cache, and shard flush.

use std::ffi::CStr;

use crate::bloom::BloomFilter;
use crate::compact::{compare_rows, SchemaDescriptor, SchemaColumn, ShardWriter};
use crate::cursor::{merge_to_writer, ShardRef};
use crate::shard_file;
use crate::shard_reader::MappedShard;
use crate::util::read_u64_at;

const ACCUMULATOR_THRESHOLD: usize = 64;
const SHORT_STRING_THRESHOLD: usize = 12;

pub const ERR_MEMTABLE_FULL: i32 = -10;

// ---------------------------------------------------------------------------
// OwnedBatch — Rust-owned columnar batch
// ---------------------------------------------------------------------------

pub(crate) struct OwnedBatch {
    pub pk_lo: Vec<u8>,
    pub pk_hi: Vec<u8>,
    pub weight: Vec<u8>,
    pub null_bm: Vec<u8>,
    /// Per-logical-column buffers. PK column slot is empty (len 0).
    pub cols: Vec<Vec<u8>>,
    pub blob: Vec<u8>,
    pub count: usize,
}

impl OwnedBatch {
    fn new(schema: &SchemaDescriptor) -> Self {
        let num_cols = schema.num_columns as usize;
        let mut cols = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            cols.push(Vec::new());
        }
        OwnedBatch {
            pk_lo: Vec::new(),
            pk_hi: Vec::new(),
            weight: Vec::new(),
            null_bm: Vec::new(),
            cols,
            blob: Vec::new(),
            count: 0,
        }
    }

    fn bytes(&self) -> usize {
        let mut total = self.pk_lo.len() + self.pk_hi.len()
            + self.weight.len() + self.null_bm.len() + self.blob.len();
        for c in &self.cols {
            total += c.len();
        }
        total
    }

    fn clear(&mut self) {
        self.pk_lo.clear();
        self.pk_hi.clear();
        self.weight.clear();
        self.null_bm.clear();
        for c in &mut self.cols {
            c.clear();
        }
        self.blob.clear();
        self.count = 0;
    }

    pub(crate) fn as_shard(&self, schema: &SchemaDescriptor) -> MappedShard {
        let pk_index = schema.pk_index as usize;
        let mut col_ptrs = Vec::new();
        let mut col_sizes = Vec::new();
        for ci in 0..schema.num_columns as usize {
            if ci != pk_index {
                col_ptrs.push(self.cols[ci].as_ptr());
                col_sizes.push(self.cols[ci].len());
            }
        }
        MappedShard::from_buffers(
            self.pk_lo.as_ptr(),
            self.pk_hi.as_ptr(),
            self.weight.as_ptr(),
            self.null_bm.as_ptr(),
            &col_ptrs, &col_sizes,
            self.blob.as_ptr(), self.blob.len(),
            self.count, schema,
        )
    }
}

/// Sort a batch by PK (permutation-based scatter copy).
fn sort_batch(src: &MappedShard, schema: &SchemaDescriptor) -> OwnedBatch {
    let n = src.count;
    if n == 0 {
        return OwnedBatch::new(schema);
    }

    let mut indices: Vec<usize> = (0..n).collect();
    indices.sort_unstable_by(|&a, &b| {
        let ka = src.get_pk(a);
        let kb = src.get_pk(b);
        ka.cmp(&kb)
    });

    scatter_copy(src, &indices, schema)
}

/// Copy rows from src in the order given by indices, producing a new OwnedBatch.
fn scatter_copy(src: &MappedShard, indices: &[usize], schema: &SchemaDescriptor) -> OwnedBatch {
    let n = indices.len();
    let pk_index = schema.pk_index as usize;
    let num_cols = schema.num_columns as usize;

    let mut out = OwnedBatch::new(schema);
    out.pk_lo.reserve(n * 8);
    out.pk_hi.reserve(n * 8);
    out.weight.reserve(n * 8);
    out.null_bm.reserve(n * 8);
    out.count = n;

    for &idx in indices {
        out.pk_lo.extend_from_slice(&src.get_pk_lo(idx).to_le_bytes());
        out.pk_hi.extend_from_slice(&src.get_pk_hi(idx).to_le_bytes());
        out.weight.extend_from_slice(&src.get_weight(idx).to_le_bytes());
        out.null_bm.extend_from_slice(&src.get_null_word(idx).to_le_bytes());
    }

    let mut payload_idx = 0usize;
    for ci in 0..num_cols {
        if ci == pk_index {
            continue;
        }
        let col = &schema.columns[ci];
        let col_size = col.size as usize;
        let is_string = col.type_code == 11; // TYPE_STRING
        out.cols[ci].reserve(n * col_size);

        if is_string {
            let src_blob = src.blob_slice();
            for &idx in indices {
                let src_struct = src.get_col_ptr(idx, payload_idx, 16);
                relocate_string_into(&mut out.cols[ci], &mut out.blob, src_struct, src_blob);
            }
        } else {
            for &idx in indices {
                let data = src.get_col_ptr(idx, payload_idx, col_size);
                out.cols[ci].extend_from_slice(data);
            }
        }
        payload_idx += 1;
    }

    out
}

/// Copy a 16-byte German string struct, relocating blob references into dest_blob.
fn relocate_string_into(
    dest_col: &mut Vec<u8>,
    dest_blob: &mut Vec<u8>,
    src_struct: &[u8],
    src_blob: &[u8],
) {
    let length = u32::from_le_bytes(src_struct[0..4].try_into().unwrap()) as usize;
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&src_struct[0..4]); // length
    out[4..8].copy_from_slice(&src_struct[4..8]); // prefix

    if length <= SHORT_STRING_THRESHOLD {
        let suffix_len = if length > 4 { length - 4 } else { 0 };
        out[8..8 + suffix_len].copy_from_slice(&src_struct[8..8 + suffix_len]);
    } else {
        let old_offset = u64::from_le_bytes(src_struct[8..16].try_into().unwrap()) as usize;
        let new_offset = dest_blob.len();
        if old_offset + length <= src_blob.len() {
            dest_blob.extend_from_slice(&src_blob[old_offset..old_offset + length]);
        }
        out[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
    }
    dest_col.extend_from_slice(&out);
}

// ---------------------------------------------------------------------------
// RustMemTable
// ---------------------------------------------------------------------------

pub struct RustMemTable {
    schema: SchemaDescriptor,
    pub(crate) max_bytes: usize,
    runs: Vec<OwnedBatch>,
    runs_bytes: usize,
    acc: OwnedBatch,
    total_row_count: usize,
    bloom: BloomFilter,
    cached_consolidated: Option<OwnedBatch>,
}

impl RustMemTable {
    pub fn new(schema: SchemaDescriptor, max_bytes: usize) -> Self {
        let initial_cap = max_bytes / ((schema.memtable_stride() + 32).max(1));
        let initial_cap = initial_cap.max(16) as u32;
        let acc = OwnedBatch::new(&schema);
        RustMemTable {
            schema,
            max_bytes,
            runs: Vec::new(),
            runs_bytes: 0,
            acc,
            total_row_count: 0,
            bloom: BloomFilter::new(initial_cap),
            cached_consolidated: None,
        }
    }

    fn total_bytes(&self) -> usize {
        self.runs_bytes + self.acc.bytes()
    }

    fn invalidate_cache(&mut self) {
        self.cached_consolidated = None;
    }

    /// Ingest a caller-owned batch (described by a MappedShard view).
    /// Sorts, stores as a run. Returns ERR_MEMTABLE_FULL if over capacity.
    pub fn upsert_batch(&mut self, src: &MappedShard) -> i32 {
        if src.count == 0 {
            return 0;
        }
        if self.total_bytes() > self.max_bytes {
            return ERR_MEMTABLE_FULL;
        }

        let sorted = sort_batch(src, &self.schema);

        for i in 0..src.count {
            self.bloom.add(src.get_pk_lo(i), src.get_pk_hi(i));
        }

        self.total_row_count += sorted.count;
        self.runs_bytes += sorted.bytes();
        self.runs.push(sorted);
        self.invalidate_cache();
        0
    }

    /// Append a single row from raw column pointers.
    /// col_ptrs: per-payload-column pointers (excludes PK column).
    /// source_blob: the source blob heap (for string relocation).
    pub fn append_row(
        &mut self,
        key_lo: u64, key_hi: u64, weight: i64,
        null_word: u64,
        col_ptrs: &[*const u8],
        source_blob: *const u8, source_blob_len: usize,
    ) -> i32 {
        if self.total_bytes() > self.max_bytes {
            return ERR_MEMTABLE_FULL;
        }

        self.acc.pk_lo.extend_from_slice(&key_lo.to_le_bytes());
        self.acc.pk_hi.extend_from_slice(&key_hi.to_le_bytes());
        self.acc.weight.extend_from_slice(&weight.to_le_bytes());
        self.acc.null_bm.extend_from_slice(&null_word.to_le_bytes());

        let pk_index = self.schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let col = &self.schema.columns[ci];
            let col_size = col.size as usize;
            let is_null = (null_word >> (if ci < pk_index { ci } else { ci - 1 })) & 1 != 0;

            if is_null {
                self.acc.cols[ci].extend(std::iter::repeat(0u8).take(col_size));
            } else if col.type_code == 11 { // TYPE_STRING
                let src_struct = if !col_ptrs[pi].is_null() {
                    unsafe { std::slice::from_raw_parts(col_ptrs[pi], 16) }
                } else {
                    &[0u8; 16]
                };
                let src_blob = if !source_blob.is_null() && source_blob_len > 0 {
                    unsafe { std::slice::from_raw_parts(source_blob, source_blob_len) }
                } else {
                    &[]
                };
                relocate_string_into(&mut self.acc.cols[ci], &mut self.acc.blob, src_struct, src_blob);
            } else if !col_ptrs[pi].is_null() {
                let data = unsafe { std::slice::from_raw_parts(col_ptrs[pi], col_size) };
                self.acc.cols[ci].extend_from_slice(data);
            } else {
                self.acc.cols[ci].extend(std::iter::repeat(0u8).take(col_size));
            }
            pi += 1;
        }

        self.acc.count += 1;
        self.total_row_count += 1;
        self.bloom.add(key_lo, key_hi);

        if self.acc.count >= ACCUMULATOR_THRESHOLD {
            self.flush_accumulator();
        }
        0
    }

    fn flush_accumulator(&mut self) {
        if self.acc.count == 0 {
            return;
        }
        let shard_view = self.acc.as_shard(&self.schema);
        let sorted = sort_batch(&shard_view, &self.schema);
        self.runs_bytes += sorted.bytes();
        self.runs.push(sorted);
        self.acc.clear();
        self.invalidate_cache();
    }

    pub fn get_snapshot(&mut self) -> OwnedBatch {
        if self.cached_consolidated.is_none() && !self.runs.is_empty() {
            let refs: Vec<ShardRef> = self.runs.iter()
                .map(|r| ShardRef::Borrowed(&r.as_shard(&self.schema) as *const MappedShard))
                .collect();
            // Need to keep the MappedShard views alive for the merge
            let views: Vec<MappedShard> = self.runs.iter()
                .map(|r| r.as_shard(&self.schema))
                .collect();
            let refs: Vec<ShardRef> = views.iter()
                .map(|v| ShardRef::Borrowed(v as *const MappedShard))
                .collect();
            let writer = merge_to_writer(&refs, &self.schema);
            self.cached_consolidated = Some(writer_to_owned(&writer, &self.schema));
        }

        let has_acc = self.acc.count > 0;

        if self.cached_consolidated.is_none() {
            if has_acc {
                let acc_view = self.acc.as_shard(&self.schema);
                let sorted = sort_batch(&acc_view, &self.schema);
                // Single-run "merge" — just consolidate
                let view = sorted.as_shard(&self.schema);
                let refs = vec![ShardRef::Borrowed(&view as *const MappedShard)];
                let writer = merge_to_writer(&refs, &self.schema);
                return writer_to_owned(&writer, &self.schema);
            }
            return OwnedBatch::new(&self.schema);
        }

        let cached = self.cached_consolidated.as_ref().unwrap();
        if !has_acc {
            return clone_owned(cached);
        }

        // Merge cached + sorted accumulator
        let acc_view = self.acc.as_shard(&self.schema);
        let sorted_acc = sort_batch(&acc_view, &self.schema);
        let cached_view = cached.as_shard(&self.schema);
        let sorted_view = sorted_acc.as_shard(&self.schema);
        let refs = vec![
            ShardRef::Borrowed(&cached_view as *const MappedShard),
            ShardRef::Borrowed(&sorted_view as *const MappedShard),
        ];
        let writer = merge_to_writer(&refs, &self.schema);
        writer_to_owned(&writer, &self.schema)
    }

    pub fn flush(
        &mut self,
        dirfd: i32,
        filename: &CStr,
        table_id: u32,
        durable: bool,
    ) -> Result<bool, i32> {
        self.flush_accumulator();
        let snapshot = self.get_snapshot();
        if snapshot.count == 0 {
            return Ok(false);
        }

        let pk_index = self.schema.pk_index as usize;
        let shard_view = snapshot.as_shard(&self.schema);
        let mut regions: Vec<&[u8]> = Vec::new();
        regions.push(&snapshot.pk_lo);
        regions.push(&snapshot.pk_hi);
        regions.push(&snapshot.weight);
        regions.push(&snapshot.null_bm);
        for ci in 0..self.schema.num_columns as usize {
            if ci != pk_index {
                regions.push(&snapshot.cols[ci]);
            }
        }
        regions.push(&snapshot.blob);

        let pk_lo_s = unsafe {
            std::slice::from_raw_parts(snapshot.pk_lo.as_ptr() as *const u64, snapshot.count)
        };
        let pk_hi_s = unsafe {
            std::slice::from_raw_parts(snapshot.pk_hi.as_ptr() as *const u64, snapshot.count)
        };

        shard_file::write_shard(
            dirfd, filename, table_id, snapshot.count,
            &regions, pk_lo_s, pk_hi_s, durable,
        )?;

        self.reset();
        Ok(true)
    }

    pub fn lookup_pk(&self, key_lo: u64, key_hi: u64) -> (i64, Option<(MappedShard, usize)>) {
        let mut total_w: i64 = 0;
        let mut found: Option<(MappedShard, usize)> = None;

        for run in &self.runs {
            let view = run.as_shard(&self.schema);
            let idx = view.find_lower_bound(key_lo, key_hi);
            let mut i = idx;
            while i < run.count {
                if view.get_pk_lo(i) != key_lo || view.get_pk_hi(i) != key_hi {
                    break;
                }
                total_w += view.get_weight(i);
                if found.is_none() {
                    found = Some((run.as_shard(&self.schema), i));
                }
                i += 1;
            }
        }

        // Linear scan accumulator
        for i in 0..self.acc.count {
            let lo = unsafe { read_u64_at(self.acc.pk_lo.as_ptr(), i * 8) };
            let hi = unsafe { read_u64_at(self.acc.pk_hi.as_ptr(), i * 8) };
            if lo == key_lo && hi == key_hi {
                let w = unsafe { crate::util::read_i64_at(self.acc.weight.as_ptr(), i * 8) };
                total_w += w;
                if found.is_none() {
                    found = Some((self.acc.as_shard(&self.schema), i));
                }
            }
        }

        (total_w, found)
    }

    pub fn find_weight_for_row(
        &self,
        key_lo: u64, key_hi: u64,
        query_col_ptrs: &[*const u8],
        query_col_sizes: &[usize],
        query_null_word: u64,
        query_blob: *const u8, query_blob_len: usize,
    ) -> i64 {
        // Create a 1-row MappedShard for the query
        let query_shard = MappedShard::from_buffers(
            &key_lo as *const u64 as *const u8,
            &key_hi as *const u64 as *const u8,
            &0i64 as *const i64 as *const u8,
            &query_null_word as *const u64 as *const u8,
            query_col_ptrs, query_col_sizes,
            query_blob, query_blob_len,
            1, &self.schema,
        );

        let mut total_w: i64 = 0;

        for run in &self.runs {
            let view = run.as_shard(&self.schema);
            let idx = view.find_lower_bound(key_lo, key_hi);
            let mut i = idx;
            let shards = vec![&view, &query_shard];
            while i < run.count {
                if view.get_pk_lo(i) != key_lo || view.get_pk_hi(i) != key_hi {
                    break;
                }
                if compare_rows(&self.schema, &shards, 0, i, 1, 0) == std::cmp::Ordering::Equal {
                    total_w += view.get_weight(i);
                }
                i += 1;
            }
        }

        // Linear scan accumulator
        if self.acc.count > 0 {
            let acc_view = self.acc.as_shard(&self.schema);
            let shards = vec![&acc_view, &query_shard];
            for i in 0..self.acc.count {
                let lo = unsafe { read_u64_at(self.acc.pk_lo.as_ptr(), i * 8) };
                let hi = unsafe { read_u64_at(self.acc.pk_hi.as_ptr(), i * 8) };
                if lo == key_lo && hi == key_hi {
                    if compare_rows(&self.schema, &shards, 0, i, 1, 0) == std::cmp::Ordering::Equal {
                        let w = unsafe { crate::util::read_i64_at(self.acc.weight.as_ptr(), i * 8) };
                        total_w += w;
                    }
                }
            }
        }

        total_w
    }

    pub fn may_contain_pk(&self, key_lo: u64, key_hi: u64) -> bool {
        self.bloom.may_contain(key_lo, key_hi)
    }

    pub fn should_flush(&self) -> bool {
        self.total_bytes() > self.max_bytes * 3 / 4
    }

    pub fn schema_ref(&self) -> &SchemaDescriptor {
        &self.schema
    }

    pub fn is_empty(&self) -> bool {
        self.total_row_count == 0
    }

    pub fn total_rows(&self) -> usize {
        self.total_row_count
    }

    pub fn reset(&mut self) {
        self.runs.clear();
        self.runs_bytes = 0;
        self.acc.clear();
        self.invalidate_cache();
        self.total_row_count = 0;
        self.bloom.reset();
    }
}

impl SchemaDescriptor {
    fn memtable_stride(&self) -> usize {
        let mut total = 0usize;
        for ci in 0..self.num_columns as usize {
            total += self.columns[ci].size as usize;
        }
        total + 24 // pk_lo(8) + pk_hi(8) + weight(8) + null(8) overhead
    }
}

fn writer_to_owned(writer: &ShardWriter, schema: &SchemaDescriptor) -> OwnedBatch {
    let pk_index = schema.pk_index as usize;
    let mut out = OwnedBatch::new(schema);
    out.pk_lo = writer.pk_lo.clone();
    out.pk_hi = writer.pk_hi.clone();
    out.weight = writer.weight.clone();
    out.null_bm = writer.null_bitmap.clone();
    for ci in 0..schema.num_columns as usize {
        if ci != pk_index {
            out.cols[ci] = writer.col_bufs[ci].clone();
        }
    }
    out.blob = writer.blob_heap.clone();
    out.count = writer.count;
    out
}

fn clone_owned(src: &OwnedBatch) -> OwnedBatch {
    OwnedBatch {
        pk_lo: src.pk_lo.clone(),
        pk_hi: src.pk_hi.clone(),
        weight: src.weight.clone(),
        null_bm: src.null_bm.clone(),
        cols: src.cols.clone(),
        blob: src.blob.clone(),
        count: src.count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema() -> SchemaDescriptor {
        let mut cols = [SchemaColumn { type_code: 8, size: 8, nullable: 0, _pad: 0 }; 64];
        cols[1] = SchemaColumn { type_code: 9, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns: cols }
    }

    struct TestBatch {
        pk_lo: Vec<u64>,
        pk_hi: Vec<u64>,
        weights: Vec<i64>,
        nulls: Vec<u64>,
        col1: Vec<i64>,
    }

    impl TestBatch {
        fn new(pk_lo: Vec<u64>, weights: Vec<i64>, col1: Vec<i64>) -> Self {
            let n = pk_lo.len();
            TestBatch { pk_lo, pk_hi: vec![0u64; n], weights, nulls: vec![0u64; n], col1 }
        }
        fn as_shard(&self, schema: &SchemaDescriptor) -> MappedShard {
            MappedShard::from_buffers(
                self.pk_lo.as_ptr() as *const u8,
                self.pk_hi.as_ptr() as *const u8,
                self.weights.as_ptr() as *const u8,
                self.nulls.as_ptr() as *const u8,
                &[self.col1.as_ptr() as *const u8],
                &[self.col1.len() * 8],
                std::ptr::null(), 0,
                self.pk_lo.len(), schema,
            )
        }
    }

    #[test]
    fn upsert_batch_and_snapshot() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let tb = TestBatch::new(vec![30, 10, 20], vec![1, 1, 1], vec![300, 100, 200]);
        assert_eq!(mt.upsert_batch(&tb.as_shard(&schema)), 0);
        assert_eq!(mt.total_rows(), 3);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 3);
        let view = snap.as_shard(&schema);
        assert_eq!(view.get_pk_lo(0), 10);
        assert_eq!(view.get_pk_lo(1), 20);
        assert_eq!(view.get_pk_lo(2), 30);
    }

    #[test]
    fn append_row_and_flush_accumulator() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let col_val: i64 = 42;
        let col_ptrs = [&col_val as *const i64 as *const u8];
        mt.append_row(5, 0, 1, 0, &col_ptrs, std::ptr::null(), 0);
        mt.append_row(3, 0, 1, 0, &col_ptrs, std::ptr::null(), 0);
        assert_eq!(mt.total_rows(), 2);
        assert_eq!(mt.acc.count, 2);
        assert_eq!(mt.runs.len(), 0);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 2);
        let view = snap.as_shard(&schema);
        assert_eq!(view.get_pk_lo(0), 3);
        assert_eq!(view.get_pk_lo(1), 5);
    }

    #[test]
    fn bloom_filter() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let tb = TestBatch::new(vec![100], vec![1], vec![999]);
        mt.upsert_batch(&tb.as_shard(&schema));

        assert!(mt.may_contain_pk(100, 0));
    }

    #[test]
    fn lookup_pk_basic() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let tb = TestBatch::new(vec![10, 20, 30], vec![1, 2, 3], vec![100, 200, 300]);
        mt.upsert_batch(&tb.as_shard(&schema));

        let (weight, found) = mt.lookup_pk(20, 0);
        assert_eq!(weight, 2);
        assert!(found.is_some());

        let (weight, found) = mt.lookup_pk(99, 0);
        assert_eq!(weight, 0);
        assert!(found.is_none());
    }

    #[test]
    fn weight_summation_across_runs() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let t1 = TestBatch::new(vec![10], vec![3], vec![100]);
        let t2 = TestBatch::new(vec![10], vec![5], vec![100]);
        mt.upsert_batch(&t1.as_shard(&schema));
        mt.upsert_batch(&t2.as_shard(&schema));

        let (weight, _) = mt.lookup_pk(10, 0);
        assert_eq!(weight, 8);
    }

    #[test]
    fn ghost_elimination_in_snapshot() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let t1 = TestBatch::new(vec![10, 20], vec![1, 1], vec![100, 200]);
        let t2 = TestBatch::new(vec![10, 20], vec![-1, 0], vec![100, 200]);
        mt.upsert_batch(&t1.as_shard(&schema));
        mt.upsert_batch(&t2.as_shard(&schema));

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 1);
        let view = snap.as_shard(&schema);
        assert_eq!(view.get_pk_lo(0), 20);
    }

    #[test]
    fn flush_writes_shard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_flush.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let tb = TestBatch::new(vec![5, 10, 15], vec![1, 1, 1], vec![50, 100, 150]);
        mt.upsert_batch(&tb.as_shard(&schema));

        let wrote = mt.flush(libc::AT_FDCWD, &cpath, 1, false).unwrap();
        assert!(wrote);
        assert!(mt.is_empty());

        let read_back = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(read_back.count, 3);
        assert_eq!(read_back.get_pk_lo(0), 5);
    }

    #[test]
    fn reset_clears_state() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 1 << 20);

        let tb = TestBatch::new(vec![1], vec![1], vec![10]);
        mt.upsert_batch(&tb.as_shard(&schema));
        assert!(!mt.is_empty());

        mt.reset();
        assert!(mt.is_empty());
        assert_eq!(mt.total_rows(), 0);
    }

    #[test]
    fn capacity_limit() {
        let schema = make_schema();
        let mut mt = RustMemTable::new(schema.clone(), 100);

        let tb = TestBatch::new(vec![1; 100], vec![1; 100], vec![10; 100]);
        let rc = mt.upsert_batch(&tb.as_shard(&schema));
        assert_eq!(rc, 0);

        let rc2 = mt.upsert_batch(&tb.as_shard(&schema));
        assert_eq!(rc2, ERR_MEMTABLE_FULL);
    }
}
