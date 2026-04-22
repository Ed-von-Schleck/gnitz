//! In-memory N-way merge for MemTable consolidation.
//!
//! Operates on flat columnar buffers: pk_lo[u64], pk_hi[u64], weight[i64],
//! null_bitmap[u64], payload columns, blob arena.
//!
//! The merge is a fused k-way merge + inline consolidation: rows with the same
//! (PK, payload) have their weights summed; rows whose net weight is zero are dropped.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::columnar::{self, ColumnarSource};
use crate::schema::{SchemaDescriptor, type_code};
use super::heap::MergeHeap;
use crate::util::{read_u32_le, read_u64_le};

use type_code::STRING as TYPE_STRING;

// ---------------------------------------------------------------------------
// MemBatch: a view over flat columnar buffers (one batch / sorted run)
// ---------------------------------------------------------------------------

/// A `MemBatch` that has been certified sorted by (PK, payload).
///
/// The only ways to obtain one are:
/// - `Batch::as_sorted_mem_batch()` — runtime check on the `sorted` flag
/// - `SortedMemBatch::new_unchecked()` — caller asserts the invariant
///
/// `merge_batches` requires `&[SortedMemBatch]` so the compiler enforces that
/// only certified-sorted inputs reach the N-way merge.
#[repr(transparent)]
pub struct SortedMemBatch<'a>(MemBatch<'a>);

impl<'a> SortedMemBatch<'a> {
    /// Wrap `mb` asserting it is already sorted by (PK, payload).
    /// Use `Batch::as_sorted_mem_batch()` for the checked variant.
    pub(crate) fn new_unchecked(mb: MemBatch<'a>) -> Self {
        SortedMemBatch(mb)
    }
}

impl<'a> std::ops::Deref for SortedMemBatch<'a> {
    type Target = MemBatch<'a>;
    fn deref(&self) -> &MemBatch<'a> { &self.0 }
}

#[derive(Clone)]
pub struct MemBatch<'a> {
    pub pk_lo: &'a [u8],     // count * 8
    pub pk_hi: &'a [u8],     // count * 8
    pub weight: &'a [u8],    // count * 8
    pub null_bmp: &'a [u8],  // count * 8
    pub col_data: Vec<&'a [u8]>,  // one slice per payload column
    pub blob: &'a [u8],
    pub count: usize,
}

impl<'a> MemBatch<'a> {
    #[inline]
    fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(self.pk_lo, row * 8)
    }
    #[inline]
    fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(self.pk_hi, row * 8)
    }
    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        crate::util::make_pk(self.get_pk_lo(row), self.get_pk_hi(row))
    }
    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        i64::from_le_bytes(self.weight[row * 8..row * 8 + 8].try_into().unwrap())
    }
    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.null_bmp, row * 8)
    }
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = row * col_size;
        &self.col_data[payload_col][off..off + col_size]
    }

    /// Binary search for the first row where PK >= key.
    pub fn find_lower_bound(&self, key: u128) -> usize {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.get_pk(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    #[inline]
    pub fn pk_matches(&self, row: usize, key: u128) -> bool {
        self.get_pk(row) == key
    }
}

impl<'a> ColumnarSource for MemBatch<'a> {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        self.get_null_word(row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        MemBatch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        self.blob
    }
}

// ---------------------------------------------------------------------------
// MemBatchCursor: position within a MemBatch
// ---------------------------------------------------------------------------

pub struct MemBatchCursor {
    pub batch_idx: usize,
    pub position: usize,
    pub count: usize,
}

impl MemBatchCursor {
    pub fn new(batch_idx: usize, count: usize) -> Self {
        MemBatchCursor {
            batch_idx,
            position: 0,
            count,
        }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.position < self.count
    }

    #[inline]
    pub fn advance(&mut self) {
        if self.position < self.count {
            self.position += 1;
        }
    }

    #[inline]
    pub fn peek_key(&self, batches: &[SortedMemBatch]) -> u128 {
        if self.is_valid() {
            batches[self.batch_idx].get_pk(self.position)
        } else {
            u128::MAX
        }
    }
}

// ---------------------------------------------------------------------------
// DirectWriter: writes into pre-allocated output buffers
// ---------------------------------------------------------------------------

pub struct DirectWriter<'a> {
    pk_lo: &'a mut [u8],
    pk_hi: &'a mut [u8],
    weight: &'a mut [u8],
    null_bmp: &'a mut [u8],
    col_bufs: Vec<&'a mut [u8]>,
    blob: &'a mut [u8],
    blob_offset: usize,
    blob_cache: HashMap<(u64, usize), usize>,
    count: usize,
    schema: SchemaDescriptor,
}

impl<'a> DirectWriter<'a> {
    pub fn new(
        pk_lo: &'a mut [u8],
        pk_hi: &'a mut [u8],
        weight: &'a mut [u8],
        null_bmp: &'a mut [u8],
        col_bufs: Vec<&'a mut [u8]>,
        blob: &'a mut [u8],
        schema: SchemaDescriptor,
    ) -> Self {
        DirectWriter {
            pk_lo,
            pk_hi,
            weight,
            null_bmp,
            col_bufs,
            blob,
            blob_offset: 0,
            blob_cache: HashMap::new(),
            count: 0,
            schema,
        }
    }

    pub fn write_row(
        &mut self,
        batch: &MemBatch,
        row: usize,
        weight: i64,
    ) {
        if weight == 0 {
            return;
        }
        let out_row = self.count;
        self.count += 1;

        let pk_lo = batch.get_pk_lo(row);
        let pk_hi = batch.get_pk_hi(row);
        let null_word = batch.get_null_word(row);

        self.pk_lo[out_row * 8..out_row * 8 + 8].copy_from_slice(&pk_lo.to_le_bytes());
        self.pk_hi[out_row * 8..out_row * 8 + 8].copy_from_slice(&pk_hi.to_le_bytes());
        self.weight[out_row * 8..out_row * 8 + 8].copy_from_slice(&weight.to_le_bytes());
        self.null_bmp[out_row * 8..out_row * 8 + 8].copy_from_slice(&null_word.to_le_bytes());

        let schema = self.schema;
        for (payload_idx, _ci, col) in schema.payload_columns() {
            let col_size = col.size as usize;
            let is_null = (null_word >> payload_idx) & 1 != 0;

            if is_null {
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].fill(0);
            } else if col.type_code == TYPE_STRING {
                self.write_string(payload_idx, batch, row, out_row);
            } else {
                let src = batch.get_col_ptr(row, payload_idx, col_size);
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].copy_from_slice(src);
            }
        }
    }

    fn write_string(
        &mut self,
        payload_col: usize,
        batch: &MemBatch,
        src_row: usize,
        out_row: usize,
    ) {
        let src = batch.get_col_ptr(src_row, payload_col, 16);
        let (mut dest, is_long) = crate::schema::prep_german_string_copy(src);
        if is_long {
            let length = read_u32_le(src, 0) as usize;
            let old_offset = read_u64_le(src, 8) as usize;
            let src_data = &batch.blob[old_offset..old_offset + length];
            let new_offset = self.get_or_append_blob(src_data);
            dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
        }
        let off = out_row * 16;
        self.col_bufs[payload_col][off..off + 16].copy_from_slice(&dest);
    }

    fn get_or_append_blob(&mut self, data: &[u8]) -> usize {
        let new_offset = self.blob_offset;
        match crate::schema::blob_cache_lookup(data, &mut self.blob_cache, &self.blob[..self.blob_offset], new_offset) {
            Some(off) => off,
            None => {
                self.blob[self.blob_offset..self.blob_offset + data.len()].copy_from_slice(data);
                self.blob_offset += data.len();
                new_offset
            }
        }
    }

    pub fn row_count(&self) -> usize {
        self.count
    }

    pub fn blob_written(&self) -> usize {
        self.blob_offset
    }
}

// ---------------------------------------------------------------------------
// merge_batches: the main entry point
// ---------------------------------------------------------------------------

/// Less-than predicate for the N-way merge heap: orders by PK first, then
/// by full payload via `columnar::compare_rows`.  Equal (PK, payload) entries
/// therefore surface at the heap root in adjacent iterations, which the
/// pending-group drain in `merge_batches` relies on for O(1) consolidation.
///
/// Recreated per-call by callers so the captured borrow of `cursors` stays
/// scoped to a single heap operation, leaving `cursors` free to be mutated
/// between operations.
fn merge_entry_less<'c>(
    cursors: &'c [MemBatchCursor],
    batches: &'c [SortedMemBatch],
    schema: &'c SchemaDescriptor,
) -> impl Fn(&super::heap::HeapNode, &super::heap::HeapNode) -> bool + 'c {
    move |a, b| {
        if a.key != b.key {
            return a.key < b.key;
        }
        let bi = cursors[a.idx].batch_idx;
        let bj = cursors[b.idx].batch_idx;
        let ri = cursors[a.idx].position;
        let rj = cursors[b.idx].position;
        columnar::compare_rows(schema, &batches[bi].0, ri, &batches[bj].0, rj) == Ordering::Less
    }
}

/// Perform N-way merge + consolidation of **sorted** `MemBatch` slices.
///
/// Rows with the same (PK, payload) have their weights summed; zero-weight
/// (PK, payload) groups are dropped.  The payload-aware heap ordering ensures
/// equal (PK, payload) entries appear consecutively at the root, so the
/// single-level pending-group drain below handles both intra-cursor
/// duplicates (consecutive matching rows inside one sorted batch) and
/// cross-cursor duplicates (matching rows in different batches) in one pass.
///
/// The `SortedMemBatch` parameter enforces at the call site that every input
/// is certified sorted by (PK, payload).
pub fn merge_batches(
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batches.len();
    if n == 0 {
        return;
    }

    let mut cursors: Vec<MemBatchCursor> = (0..n)
        .map(|i| MemBatchCursor::new(i, batches[i].count))
        .collect();

    let mut tree = MergeHeap::build(
        cursors.len(),
        |i| {
            if cursors[i].is_valid() {
                Some(cursors[i].peek_key(batches))
            } else {
                None
            }
        },
        &merge_entry_less(&cursors, batches, schema),
    );

    let mut has_pending = false;
    let mut pending_batch: usize = 0;
    let mut pending_row: usize = 0;
    let mut pending_pk: u128 = 0;
    let mut pending_weight: i64 = 0;

    while !tree.is_empty() {
        let ci = tree.min_idx();
        let bi = cursors[ci].batch_idx;
        let ri = cursors[ci].position;
        let cur_pk = batches[bi].get_pk(ri);
        let cur_weight = batches[bi].get_weight(ri);

        let same_group = has_pending
            && cur_pk == pending_pk
            && columnar::compare_rows(
                schema,
                &batches[pending_batch].0, pending_row,
                &batches[bi].0, ri,
            ) == Ordering::Equal;

        if same_group {
            pending_weight += cur_weight;
        } else {
            if has_pending && pending_weight != 0 {
                writer.write_row(&batches[pending_batch], pending_row, pending_weight);
            }
            pending_batch = bi;
            pending_row = ri;
            pending_pk = cur_pk;
            pending_weight = cur_weight;
            has_pending = true;
        }

        cursors[ci].advance();
        let new_key = if cursors[ci].is_valid() {
            Some(cursors[ci].peek_key(batches))
        } else {
            None
        };
        tree.advance(ci, new_key, &merge_entry_less(&cursors, batches, schema));
    }

    if has_pending && pending_weight != 0 {
        writer.write_row(&batches[pending_batch], pending_row, pending_weight);
    }
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum weights for
/// identical (PK, payload) rows, drop ghosts (net weight == 0).
///
/// Uses Rust's stable sort on an index array — no tournament tree needed.
/// Key-pointer entry: the 16-byte PK travels with the row index so the sort
/// comparator reads the key from the element being positioned, not from a
/// separate array at a random offset.
#[derive(Copy, Clone)]
struct SortEntry {
    pk: u128,
    idx: u32,
}

pub fn sort_and_consolidate(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    let mut entries: Vec<SortEntry> = (0..n as u32)
        .map(|i| SortEntry { pk: batch.get_pk(i as usize), idx: i })
        .collect();

    entries.sort_unstable_by(|a, b| match a.pk.cmp(&b.pk) {
        Ordering::Equal => columnar::compare_rows(schema, batch, a.idx as usize, batch, b.idx as usize),
        ord => ord,
    });

    let mut pending_idx = entries[0].idx as usize;
    let mut pending_pk = entries[0].pk;
    let mut pending_weight = batch.get_weight(pending_idx);

    for pos in 1..n {
        let e = entries[pos];
        let cur_idx = e.idx as usize;

        let same_group = e.pk == pending_pk
            && columnar::compare_rows(schema, batch, pending_idx, batch, cur_idx)
                == Ordering::Equal;

        if same_group {
            pending_weight += batch.get_weight(cur_idx);
        } else {
            if pending_weight != 0 {
                writer.write_row(batch, pending_idx, pending_weight);
            }
            pending_idx = cur_idx;
            pending_pk = e.pk;
            pending_weight = batch.get_weight(cur_idx);
        }
    }

    if pending_weight != 0 {
        writer.write_row(batch, pending_idx, pending_weight);
    }
}

/// Weight-fold an already-sorted batch: sum weights for identical (PK, payload)
/// rows and drop ghosts (net weight == 0). Caller must guarantee sorted input.
pub fn fold_sorted(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }
    let mut pending_idx = 0usize;
    let mut pending_pk = batch.get_pk(0);
    let mut pending_weight = batch.get_weight(0);

    for pos in 1..n {
        let cur_pk = batch.get_pk(pos);
        let same_group = cur_pk == pending_pk
            && columnar::compare_rows(schema, batch, pending_idx, batch, pos) == Ordering::Equal;

        if same_group {
            pending_weight += batch.get_weight(pos);
        } else {
            if pending_weight != 0 {
                writer.write_row(batch, pending_idx, pending_weight);
            }
            pending_idx = pos;
            pending_pk = cur_pk;
            pending_weight = batch.get_weight(pos);
        }
    }
    if pending_weight != 0 {
        writer.write_row(batch, pending_idx, pending_weight);
    }
}

/// Scatter-copy rows from a batch at the given indices.
/// Indices are NOT sorted — rows are written in the order given.
/// If `weights` is non-empty, uses weights[i] for row i; otherwise reads
/// the weight from the source batch at indices[i].
pub fn scatter_copy(
    batch: &MemBatch,
    indices: &[u32],
    weights: &[i64],
    writer: &mut DirectWriter,
) {
    let use_explicit_weights = !weights.is_empty();
    for (i, &idx) in indices.iter().enumerate() {
        let row = idx as usize;
        let w = if use_explicit_weights {
            weights[i]
        } else {
            batch.get_weight(row)
        };
        writer.write_row(batch, row, w);
    }
}

/// Sort a single batch by (PK, payload) WITHOUT consolidation.
/// All N input rows produce N output rows — no weight merging, no ghost elimination.
/// Duplicate (PK, payload) entries are preserved as separate rows.
#[allow(dead_code)]
pub fn sort_only(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    let mut entries: Vec<SortEntry> = (0..n as u32)
        .map(|i| SortEntry { pk: batch.get_pk(i as usize), idx: i })
        .collect();

    entries.sort_unstable_by(|a, b| match a.pk.cmp(&b.pk) {
        Ordering::Equal => columnar::compare_rows(schema, batch, a.idx as usize, batch, b.idx as usize),
        ord => ord,
    });

    for e in &entries {
        let idx = e.idx as usize;
        writer.write_row(batch, idx, batch.get_weight(idx));
    }
}

/// Parse a single batch from flat region arrays.
pub unsafe fn parse_single_batch_from_regions<'a>(
    in_ptrs: &[*const u8],
    in_sizes: &[u32],
    count: usize,
    num_payload_cols: usize,
) -> MemBatch<'a> {
    let pk_lo = std::slice::from_raw_parts(in_ptrs[0], in_sizes[0] as usize);
    let pk_hi = std::slice::from_raw_parts(in_ptrs[1], in_sizes[1] as usize);
    let weight = std::slice::from_raw_parts(in_ptrs[2], in_sizes[2] as usize);
    let null_bmp = std::slice::from_raw_parts(in_ptrs[3], in_sizes[3] as usize);

    let mut col_data = Vec::with_capacity(num_payload_cols);
    for ci in 0..num_payload_cols {
        let ri = 4 + ci;
        col_data.push(std::slice::from_raw_parts(in_ptrs[ri], in_sizes[ri] as usize));
    }

    let blob_ri = 4 + num_payload_cols;
    let blob = std::slice::from_raw_parts(in_ptrs[blob_ri], in_sizes[blob_ri] as usize);

    MemBatch {
        pk_lo,
        pk_hi,
        weight,
        null_bmp,
        col_data,
        blob,
        count,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor};

    fn make_schema_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        // col 0 = PK (U128, size 16)
        columns[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        // col 1 = I64 value column
        columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };

        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    fn make_batch_i64(rows: &[(u64, u64, i64, i64)]) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        let n = rows.len();
        let mut pk_lo = Vec::with_capacity(n * 8);
        let mut pk_hi = Vec::with_capacity(n * 8);
        let mut weight = Vec::with_capacity(n * 8);
        let mut null_bmp = Vec::with_capacity(n * 8);
        let mut col0 = Vec::with_capacity(n * 8);

        for &(lo, hi, w, val) in rows {
            pk_lo.extend_from_slice(&lo.to_le_bytes());
            pk_hi.extend_from_slice(&hi.to_le_bytes());
            weight.extend_from_slice(&w.to_le_bytes());
            null_bmp.extend_from_slice(&0u64.to_le_bytes());
            col0.extend_from_slice(&val.to_le_bytes());
        }

        (pk_lo, pk_hi, weight, null_bmp, col0)
    }

    fn to_mem_batch<'a>(
        pk_lo: &'a [u8],
        pk_hi: &'a [u8],
        weight: &'a [u8],
        null_bmp: &'a [u8],
        col0: &'a [u8],
        count: usize,
    ) -> MemBatch<'a> {
        MemBatch {
            pk_lo,
            pk_hi,
            weight,
            null_bmp,
            col_data: vec![col0],
            blob: &[],
            count,
        }
    }

    // Takes &[MemBatch] so test call sites don't need to construct SortedMemBatch.
    fn run_merge(batches: &[MemBatch], schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let sorted: Vec<SortedMemBatch> = batches.iter()
            .map(|mb| SortedMemBatch::new_unchecked(mb.clone()))
            .collect();
        let batches = &sorted[..];
        let total_rows: usize = batches.iter().map(|b| b.count).sum();
        let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();

        let mut out_pk_lo = vec![0u8; total_rows * 8];
        let mut out_pk_hi = vec![0u8; total_rows * 8];
        let mut out_weight = vec![0u8; total_rows * 8];
        let mut out_null = vec![0u8; total_rows * 8];
        let mut out_col0 = vec![0u8; total_rows * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob = vec![0u8; blob_cap];

        {
            let mut writer = DirectWriter {
                pk_lo: &mut out_pk_lo,
                pk_hi: &mut out_pk_hi,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_offset: 0,
                blob_cache: HashMap::new(),
                count: 0,
                schema: *schema,
            };

            merge_batches(batches, schema, &mut writer);

            let count = writer.row_count();
            let mut result = Vec::with_capacity(count);
            for i in 0..count {
                let lo = read_u64_le(&out_pk_lo, i * 8);
                let hi = read_u64_le(&out_pk_hi, i * 8);
                let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
                let v = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
                result.push((lo, hi, w, v));
            }
            return result;
        }
    }

    #[test]
    fn test_single_batch_passthrough() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, weight, null_bmp, col0) = make_batch_i64(&[
            (10, 0, 1, 100),
            (20, 0, 1, 200),
            (30, 0, 1, 300),
        ]);
        let batch = to_mem_batch(&pk_lo, &pk_hi, &weight, &null_bmp, &col0, 3);
        let result = run_merge(&[batch], &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
        assert_eq!(result[2], (30, 0, 1, 300));
    }

    #[test]
    fn test_two_batch_interleave() {
        let schema = make_schema_i64();
        let (pk_lo1, pk_hi1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
            (30, 0, 1, 300),
        ]);
        let (pk_lo2, pk_hi2, w2, n2, c2) = make_batch_i64(&[
            (20, 0, 1, 200),
            (40, 0, 1, 400),
        ]);
        let b1 = to_mem_batch(&pk_lo1, &pk_hi1, &w1, &n1, &c1, 2);
        let b2 = to_mem_batch(&pk_lo2, &pk_hi2, &w2, &n2, &c2, 2);
        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
        assert_eq!(result[2], (30, 0, 1, 300));
        assert_eq!(result[3], (40, 0, 1, 400));
    }

    #[test]
    fn test_consolidation_same_pk_same_payload() {
        let schema = make_schema_i64();
        let (pk_lo1, pk_hi1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
        ]);
        let (pk_lo2, pk_hi2, w2, n2, c2) = make_batch_i64(&[
            (10, 0, 2, 100),
        ]);
        let b1 = to_mem_batch(&pk_lo1, &pk_hi1, &w1, &n1, &c1, 1);
        let b2 = to_mem_batch(&pk_lo2, &pk_hi2, &w2, &n2, &c2, 1);
        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (10, 0, 3, 100));
    }

    #[test]
    fn test_consolidation_cancellation() {
        let schema = make_schema_i64();
        let (pk_lo1, pk_hi1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
        ]);
        let (pk_lo2, pk_hi2, w2, n2, c2) = make_batch_i64(&[
            (10, 0, -1, 100),
        ]);
        let b1 = to_mem_batch(&pk_lo1, &pk_hi1, &w1, &n1, &c1, 1);
        let b2 = to_mem_batch(&pk_lo2, &pk_hi2, &w2, &n2, &c2, 1);
        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_same_pk_different_payload() {
        let schema = make_schema_i64();
        let (pk_lo1, pk_hi1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
        ]);
        let (pk_lo2, pk_hi2, w2, n2, c2) = make_batch_i64(&[
            (10, 0, 1, 200),
        ]);
        let b1 = to_mem_batch(&pk_lo1, &pk_hi1, &w1, &n1, &c1, 1);
        let b2 = to_mem_batch(&pk_lo2, &pk_hi2, &w2, &n2, &c2, 1);
        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 10);
        assert_eq!(result[1].0, 10);
    }

    #[test]
    fn test_three_way_merge() {
        let schema = make_schema_i64();
        let (pk_lo1, pk_hi1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
            (40, 0, 1, 400),
        ]);
        let (pk_lo2, pk_hi2, w2, n2, c2) = make_batch_i64(&[
            (20, 0, 1, 200),
            (50, 0, 1, 500),
        ]);
        let (pk_lo3, pk_hi3, w3, n3, c3) = make_batch_i64(&[
            (30, 0, 1, 300),
            (60, 0, 1, 600),
        ]);
        let b1 = to_mem_batch(&pk_lo1, &pk_hi1, &w1, &n1, &c1, 2);
        let b2 = to_mem_batch(&pk_lo2, &pk_hi2, &w2, &n2, &c2, 2);
        let b3 = to_mem_batch(&pk_lo3, &pk_hi3, &w3, &n3, &c3, 2);
        let result = run_merge(&[b1, b2, b3], &schema);
        assert_eq!(result.len(), 6);
        let pks: Vec<u64> = result.iter().map(|r| r.0).collect();
        assert_eq!(pks, vec![10, 20, 30, 40, 50, 60]);
    }

    #[test]
    fn test_empty_batches() {
        let schema = make_schema_i64();
        let result = run_merge(&[], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_one_empty_one_nonempty() {
        let schema = make_schema_i64();
        let empty = MemBatch {
            pk_lo: &[],
            pk_hi: &[],
            weight: &[],
            null_bmp: &[],
            col_data: vec![&[]],
            blob: &[],
            count: 0,
        };
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (10, 0, 1, 100),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 1);
        let result = run_merge(&[empty, b], &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (10, 0, 1, 100));
    }

    #[test]
    fn test_partial_cancellation_three_batches() {
        let schema = make_schema_i64();
        // Insert PK=10 w=+1, PK=20 w=+1
        let (pk1, ph1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
            (20, 0, 1, 200),
        ]);
        // Delete PK=10 w=-1
        let (pk2, ph2, w2, n2, c2) = make_batch_i64(&[
            (10, 0, -1, 100),
        ]);
        // Insert PK=30 w=+1
        let (pk3, ph3, w3, n3, c3) = make_batch_i64(&[
            (30, 0, 1, 300),
        ]);
        let b1 = to_mem_batch(&pk1, &ph1, &w1, &n1, &c1, 2);
        let b2 = to_mem_batch(&pk2, &ph2, &w2, &n2, &c2, 1);
        let b3 = to_mem_batch(&pk3, &ph3, &w3, &n3, &c3, 1);
        let result = run_merge(&[b1, b2, b3], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (20, 0, 1, 200));
        assert_eq!(result[1], (30, 0, 1, 300));
    }

    #[test]
    fn test_pk_hi_differentiation() {
        let schema = make_schema_i64();
        let (pk_lo1, pk_hi1, w1, n1, c1) = make_batch_i64(&[
            (10, 0, 1, 100),
        ]);
        let (pk_lo2, pk_hi2, w2, n2, c2) = make_batch_i64(&[
            (10, 1, 1, 200),
        ]);
        let b1 = to_mem_batch(&pk_lo1, &pk_hi1, &w1, &n1, &c1, 1);
        let b2 = to_mem_batch(&pk_lo2, &pk_hi2, &w2, &n2, &c2, 1);
        let result = run_merge(&[b1, b2], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (10, 1, 1, 200));
    }

    #[test]
    fn test_many_duplicates_accumulate() {
        let schema = make_schema_i64();
        let mut all_rows = Vec::new();
        for _ in 0..5 {
            all_rows.push((42, 0, 1i64, 999i64));
        }
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&all_rows);
        // Put them in 5 separate single-row batches
        let mut batches = Vec::new();
        let mut bufs = Vec::new();
        for i in 0..5 {
            bufs.push((
                pk_lo[i * 8..(i + 1) * 8].to_vec(),
                pk_hi[i * 8..(i + 1) * 8].to_vec(),
                w[i * 8..(i + 1) * 8].to_vec(),
                n[i * 8..(i + 1) * 8].to_vec(),
                c[i * 8..(i + 1) * 8].to_vec(),
            ));
        }
        for buf in &bufs {
            batches.push(to_mem_batch(&buf.0, &buf.1, &buf.2, &buf.3, &buf.4, 1));
        }
        let result = run_merge(&batches, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (42, 0, 5, 999));
    }

    #[test]
    fn test_weight_zero_skip() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (10, 0, 0, 100),
            (20, 0, 1, 200),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 2);
        let result = run_merge(&[b], &schema);
        // Zero-weight rows just pass through the merge (they don't get consolidated out
        // unless they cancel with another row). A single zero-weight row is still zero.
        // Actually: our merge always outputs pending_weight != 0 check, so zero-weight
        // rows from a single batch get dropped.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (20, 0, 1, 200));
    }

    #[test]
    fn test_sorted_output_large() {
        let schema = make_schema_i64();
        // 100 rows in reverse order, split into batches of 10
        let mut bufs = Vec::new();
        for chunk in 0..10 {
            let base = (9 - chunk) * 10;
            let mut rows = Vec::new();
            for i in 0..10 {
                let pk = (base + i) as u64;
                rows.push((pk, 0u64, 1i64, (pk * 100) as i64));
            }
            // Sort within each batch (required: inputs are sorted runs)
            rows.sort_by_key(|r| r.0);
            bufs.push(make_batch_i64(&rows));
        }
        let batches: Vec<MemBatch> = bufs
            .iter()
            .map(|(pk_lo, pk_hi, w, n, c)| to_mem_batch(pk_lo, pk_hi, w, n, c, 10))
            .collect();
        let result = run_merge(&batches, &schema);
        assert_eq!(result.len(), 100);
        for i in 0..100 {
            assert_eq!(result[i].0, i as u64);
        }
    }

    #[test]
    fn test_within_cursor_duplicates() {
        // Two rows with the same PK within a single sorted batch
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (10, 0, 1, 100),
            (10, 0, 1, 100),
            (20, 0, 1, 200),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        let result = run_merge(&[b], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (10, 0, 2, 100));
        assert_eq!(result[1], (20, 0, 1, 200));
    }

    #[test]
    fn test_within_cursor_dup_different_payload() {
        // Two rows with same PK but different payload within one batch
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (10, 0, 1, 100),
            (10, 0, 1, 200),
            (20, 0, 1, 300),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        let result = run_merge(&[b], &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (10, 0, 1, 100));
        assert_eq!(result[1], (10, 0, 1, 200));
        assert_eq!(result[2], (20, 0, 1, 300));
    }

    // -----------------------------------------------------------------------
    // sort_and_consolidate tests
    // -----------------------------------------------------------------------

    fn run_consolidate(batch: &MemBatch, schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let n = batch.count;
        let total_blob = batch.blob.len();

        let mut out_pk_lo = vec![0u8; n * 8];
        let mut out_pk_hi = vec![0u8; n * 8];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob = vec![0u8; blob_cap];

        let count;
        {
            let mut writer = DirectWriter {
                pk_lo: &mut out_pk_lo,
                pk_hi: &mut out_pk_hi,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_offset: 0,
                blob_cache: HashMap::new(),
                count: 0,
                schema: *schema,
            };
            sort_and_consolidate(batch, schema, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let lo = read_u64_le(&out_pk_lo, i * 8);
            let hi = read_u64_le(&out_pk_hi, i * 8);
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let val = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_consolidate_empty() {
        let schema = make_schema_i64();
        let b = MemBatch {
            pk_lo: &[], pk_hi: &[], weight: &[], null_bmp: &[],
            col_data: vec![&[]], blob: &[], count: 0,
        };
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_consolidate_single_row() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[(5, 0, 1, 42)]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 1);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 1, 42));
    }

    #[test]
    fn test_consolidate_already_sorted_no_dups() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
            (3, 0, 1, 30),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_consolidate_unsorted_no_dups() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (3, 0, 1, 30),
            (1, 0, 1, 10),
            (2, 0, 1, 20),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 3);
        // Should be sorted by PK
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_consolidate_dup_weight_accumulation() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and +1 → merged to +2
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (5, 0, 1, 42),
            (5, 0, 1, 42),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 2);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 2, 42));
    }

    #[test]
    fn test_consolidate_ghost_elimination() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and -1 → ghost, eliminated
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (5, 0, 1, 42),
            (5, 0, -1, 42),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 2);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_consolidate_same_pk_different_payload() {
        let schema = make_schema_i64();
        // Same PK but different payloads → both survive, sorted by payload
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (5, 0, 1, 200),
            (5, 0, 1, 100),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 2);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 2);
        // Sorted by payload (I64 signed comparison: 100 < 200)
        assert_eq!(result[0], (5, 0, 1, 100));
        assert_eq!(result[1], (5, 0, 1, 200));
    }

    #[test]
    fn test_consolidate_unsorted_mixed() {
        let schema = make_schema_i64();
        // Unsorted: insert + retract + different PKs
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (10, 0, 1, 100),   // insert pk=10 val=100
            (5, 0, 1, 50),     // insert pk=5 val=50
            (10, 0, -1, 100),  // retract pk=10 val=100
            (5, 0, 1, 50),     // duplicate insert pk=5 val=50
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 4);
        let result = run_consolidate(&b, &schema);
        // pk=10 val=100: +1-1=0 → ghost
        // pk=5 val=50: +1+1=+2
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (5, 0, 2, 50));
    }

    #[test]
    fn test_consolidate_all_cancel() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (1, 0, 1, 10),
            (1, 0, -1, 10),
            (2, 0, 3, 20),
            (2, 0, -3, 20),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 4);
        let result = run_consolidate(&b, &schema);
        assert_eq!(result.len(), 0);
    }

    // -----------------------------------------------------------------------
    // sort_only tests
    // -----------------------------------------------------------------------

    fn run_sort(batch: &MemBatch, schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
        let n = batch.count;
        let total_blob = batch.blob.len();

        let mut out_pk_lo = vec![0u8; n * 8];
        let mut out_pk_hi = vec![0u8; n * 8];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob = vec![0u8; blob_cap];

        let count;
        {
            let mut writer = DirectWriter {
                pk_lo: &mut out_pk_lo,
                pk_hi: &mut out_pk_hi,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_offset: 0,
                blob_cache: HashMap::new(),
                count: 0,
                schema: *schema,
            };
            sort_only(batch, schema, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let lo = read_u64_le(&out_pk_lo, i * 8);
            let hi = read_u64_le(&out_pk_hi, i * 8);
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let val = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_sort_unsorted() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (3, 0, 1, 30),
            (1, 0, 1, 10),
            (2, 0, 1, 20),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        let result = run_sort(&b, &schema);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (1, 0, 1, 10));
        assert_eq!(result[1], (2, 0, 1, 20));
        assert_eq!(result[2], (3, 0, 1, 30));
    }

    #[test]
    fn test_sort_preserves_duplicates() {
        let schema = make_schema_i64();
        // Same (PK, payload) with +1 and -1 — both MUST survive (no consolidation)
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (5, 0, -1, 42),
            (5, 0, 1, 42),
            (10, 0, 1, 100),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        let result = run_sort(&b, &schema);
        assert_eq!(result.len(), 3); // ALL rows preserved
        assert_eq!(result[0].0, 5);
        assert_eq!(result[1].0, 5);
        assert_eq!(result[2].0, 10);
    }

    #[test]
    fn test_sort_already_sorted() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 2);
        let result = run_sort(&b, &schema);
        assert_eq!(result, vec![(1, 0, 1, 10), (2, 0, 1, 20)]);
    }

    // -----------------------------------------------------------------------
    // scatter_copy tests
    // -----------------------------------------------------------------------

    fn run_scatter(
        batch: &MemBatch,
        indices: &[u32],
        weights: &[i64],
        schema: &SchemaDescriptor,
    ) -> Vec<(u64, u64, i64, i64)> {
        let n = indices.len();
        let total_blob = batch.blob.len();
        let mut out_pk_lo = vec![0u8; n * 8];
        let mut out_pk_hi = vec![0u8; n * 8];
        let mut out_weight = vec![0u8; n * 8];
        let mut out_null = vec![0u8; n * 8];
        let mut out_col0 = vec![0u8; n * 8];
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };
        let mut out_blob = vec![0u8; blob_cap];

        let count;
        {
            let mut writer = DirectWriter {
                pk_lo: &mut out_pk_lo,
                pk_hi: &mut out_pk_hi,
                weight: &mut out_weight,
                null_bmp: &mut out_null,
                col_bufs: vec![&mut out_col0],
                blob: &mut out_blob,
                blob_offset: 0,
                blob_cache: HashMap::new(),
                count: 0,
                schema: *schema,
            };
            scatter_copy(batch, indices, weights, &mut writer);
            count = writer.row_count();
        }

        let mut result = Vec::new();
        for i in 0..count {
            let lo = read_u64_le(&out_pk_lo, i * 8);
            let hi = read_u64_le(&out_pk_hi, i * 8);
            let w = i64::from_le_bytes(out_weight[i * 8..i * 8 + 8].try_into().unwrap());
            let val = i64::from_le_bytes(out_col0[i * 8..i * 8 + 8].try_into().unwrap());
            result.push((lo, hi, w, val));
        }
        result
    }

    #[test]
    fn test_scatter_basic() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
            (3, 0, 1, 30),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 3);
        // Pick rows 2 and 0 (out of order)
        let result = run_scatter(&b, &[2, 0], &[], &schema);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (3, 0, 1, 30));
        assert_eq!(result[1], (1, 0, 1, 10));
    }

    #[test]
    fn test_scatter_empty_indices() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[(1, 0, 1, 10)]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 1);
        let result = run_scatter(&b, &[], &[], &schema);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scatter_with_explicit_weights() {
        let schema = make_schema_i64();
        let (pk_lo, pk_hi, w, n, c) = make_batch_i64(&[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
        ]);
        let b = to_mem_batch(&pk_lo, &pk_hi, &w, &n, &c, 2);
        // Override weights: row 1 gets w=5, row 0 gets w=-1
        let result = run_scatter(&b, &[1, 0], &[5, -1], &schema);
        assert_eq!(result[0], (2, 0, 5, 20));
        assert_eq!(result[1], (1, 0, -1, 10));
    }
}
