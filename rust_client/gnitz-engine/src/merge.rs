//! In-memory N-way merge for MemTable consolidation.
//!
//! Operates on flat columnar buffers (the same layout as ArenaZSetBatch in RPython):
//! pk_lo[u64], pk_hi[u64], weight[i64], null_bitmap[u64], payload columns, blob arena.
//!
//! The merge is a fused k-way merge + inline consolidation: rows with the same
//! (PK, payload) have their weights summed; rows whose net weight is zero are dropped.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::compact::{
    compare_german_strings, read_signed, SchemaDescriptor,
    type_code, SHORT_STRING_THRESHOLD,
};
use crate::util::{read_u32_le, read_u64_le};
use crate::xxh;

use type_code::{F32 as TYPE_F32, F64 as TYPE_F64, STRING as TYPE_STRING, U128 as TYPE_U128};

// ---------------------------------------------------------------------------
// MemBatch: a view over flat columnar buffers (one batch / sorted run)
// ---------------------------------------------------------------------------

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
    pub fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(self.pk_lo, row * 8)
    }
    #[inline]
    pub fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(self.pk_hi, row * 8)
    }
    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        let lo = self.get_pk_lo(row) as u128;
        let hi = self.get_pk_hi(row) as u128;
        (hi << 64) | lo
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
    pub fn peek_key(&self, batches: &[MemBatch]) -> u128 {
        if self.is_valid() {
            batches[self.batch_idx].get_pk(self.position)
        } else {
            u128::MAX
        }
    }
}

// ---------------------------------------------------------------------------
// TournamentTree (min-heap by PK)
// ---------------------------------------------------------------------------

struct HeapNode {
    key: u128,
    cursor_idx: usize,
}

pub struct TournamentTree {
    heap: Vec<HeapNode>,
    pos_map: Vec<i32>,
}

impl TournamentTree {
    pub fn build(
        cursors: &[MemBatchCursor],
        batches: &[MemBatch],
        schema: &SchemaDescriptor,
    ) -> Self {
        let n = cursors.len();
        let mut heap = Vec::with_capacity(n);
        let mut pos_map = vec![-1i32; n];

        for i in 0..n {
            if cursors[i].is_valid() {
                let idx = heap.len();
                pos_map[i] = idx as i32;
                heap.push(HeapNode {
                    key: cursors[i].peek_key(batches),
                    cursor_idx: i,
                });
            }
        }

        let mut tree = TournamentTree { heap, pos_map };
        let size = tree.heap.len();
        if size > 1 {
            for i in (0..size / 2).rev() {
                tree.sift_down(i, cursors, batches, schema);
            }
        }
        tree
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    #[inline]
    pub fn min_cursor_idx(&self) -> usize {
        self.heap[0].cursor_idx
    }

    /// Full comparison: PK first, then payload columns via compare_rows.
    /// Returns true if node i < node j in (PK, payload) order.
    fn node_less(
        &self,
        i: usize,
        j: usize,
        cursors: &[MemBatchCursor],
        batches: &[MemBatch],
        schema: &SchemaDescriptor,
    ) -> bool {
        let ki = self.heap[i].key;
        let kj = self.heap[j].key;
        if ki != kj {
            return ki < kj;
        }
        let ci = self.heap[i].cursor_idx;
        let cj = self.heap[j].cursor_idx;
        let bi = cursors[ci].batch_idx;
        let bj = cursors[cj].batch_idx;
        let ri = cursors[ci].position;
        let rj = cursors[cj].position;
        compare_rows(schema, &batches[bi], ri, &batches[bj], rj) == Ordering::Less
    }

    fn sift_down(
        &mut self,
        mut idx: usize,
        cursors: &[MemBatchCursor],
        batches: &[MemBatch],
        schema: &SchemaDescriptor,
    ) {
        loop {
            let mut smallest = idx;
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < self.heap.len() && self.node_less(left, smallest, cursors, batches, schema) {
                smallest = left;
            }
            if right < self.heap.len() && self.node_less(right, smallest, cursors, batches, schema) {
                smallest = right;
            }
            if smallest != idx {
                let ci = self.heap[idx].cursor_idx;
                let cs = self.heap[smallest].cursor_idx;
                self.heap.swap(idx, smallest);
                self.pos_map[ci] = smallest as i32;
                self.pos_map[cs] = idx as i32;
                idx = smallest;
            } else {
                break;
            }
        }
    }

    fn sift_up(
        &mut self,
        mut idx: usize,
        cursors: &[MemBatchCursor],
        batches: &[MemBatch],
        schema: &SchemaDescriptor,
    ) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.node_less(idx, parent, cursors, batches, schema) {
                let ci = self.heap[idx].cursor_idx;
                let cp = self.heap[parent].cursor_idx;
                self.heap.swap(idx, parent);
                self.pos_map[ci] = parent as i32;
                self.pos_map[cp] = idx as i32;
                idx = parent;
            } else {
                break;
            }
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

        let pk_index = self.schema.pk_index as usize;
        let mut payload_idx: usize = 0;

        for ci in 0..self.schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let col = &self.schema.columns[ci];
            let col_size = col.size as usize;
            let is_null = (null_word >> payload_idx) & 1 != 0;

            if is_null {
                let off = out_row * col_size;
                for b in &mut self.col_bufs[payload_idx][off..off + col_size] {
                    *b = 0;
                }
            } else if col.type_code == TYPE_STRING {
                self.write_string(payload_idx, batch, row, out_row);
            } else {
                let src = batch.get_col_ptr(row, payload_idx, col_size);
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].copy_from_slice(src);
            }
            payload_idx += 1;
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
        let length = read_u32_le(src, 0) as usize;

        let mut dest = [0u8; 16];
        dest[0..4].copy_from_slice(&(length as u32).to_le_bytes());
        dest[4..8].copy_from_slice(&src[4..8]);

        if length <= SHORT_STRING_THRESHOLD {
            let suffix_len = if length > 4 { length - 4 } else { 0 };
            if suffix_len > 0 {
                dest[8..8 + suffix_len].copy_from_slice(&src[8..8 + suffix_len]);
            }
        } else {
            // Resolve the source string data
            let old_offset = read_u64_le(src, 8) as usize;
            let src_blob = batch.blob;
            let src_data = &src_blob[old_offset..old_offset + length];

            let new_offset = self.get_or_append_blob(src_data);
            dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
        }

        let off = out_row * 16;
        self.col_bufs[payload_col][off..off + 16].copy_from_slice(&dest);
    }

    fn get_or_append_blob(&mut self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        let h = xxh::checksum(data);
        let cache_key = (h, data.len());
        if let Some(&existing_offset) = self.blob_cache.get(&cache_key) {
            let existing = &self.blob[existing_offset..existing_offset + data.len()];
            if existing == data {
                return existing_offset;
            }
        }
        let new_offset = self.blob_offset;
        self.blob[new_offset..new_offset + data.len()].copy_from_slice(data);
        self.blob_offset += data.len();
        self.blob_cache.insert(cache_key, new_offset);
        new_offset
    }

    pub fn row_count(&self) -> usize {
        self.count
    }

    pub fn blob_written(&self) -> usize {
        self.blob_offset
    }
}

// ---------------------------------------------------------------------------
// Row comparison (MemBatch variant)
// ---------------------------------------------------------------------------

pub fn compare_rows(
    schema: &SchemaDescriptor,
    batch_a: &MemBatch,
    row_a: usize,
    batch_b: &MemBatch,
    row_b: usize,
) -> Ordering {
    let pk_index = schema.pk_index as usize;
    let null_word_a = batch_a.get_null_word(row_a);
    let null_word_b = batch_b.get_null_word(row_b);
    let mut payload_col: usize = 0;

    for ci in 0..schema.num_columns as usize {
        if ci == pk_index {
            continue;
        }

        let null_a = (null_word_a >> payload_col) & 1 != 0;
        let null_b = (null_word_b >> payload_col) & 1 != 0;
        if null_a && null_b {
            payload_col += 1;
            continue;
        }
        if null_a {
            return Ordering::Less;
        }
        if null_b {
            return Ordering::Greater;
        }

        let col = &schema.columns[ci];
        let col_size = col.size as usize;

        let ord = match col.type_code {
            TYPE_STRING => {
                let ptr_a = batch_a.get_col_ptr(row_a, payload_col, 16);
                let ptr_b = batch_b.get_col_ptr(row_b, payload_col, 16);
                compare_german_strings(ptr_a, batch_a.blob, ptr_b, batch_b.blob)
            }
            TYPE_U128 => {
                let ba = batch_a.get_col_ptr(row_a, payload_col, 16);
                let bb = batch_b.get_col_ptr(row_b, payload_col, 16);
                let va = ((read_u64_le(ba, 8) as u128) << 64) | (read_u64_le(ba, 0) as u128);
                let vb = ((read_u64_le(bb, 8) as u128) << 64) | (read_u64_le(bb, 0) as u128);
                va.cmp(&vb)
            }
            TYPE_F64 => {
                let ba = batch_a.get_col_ptr(row_a, payload_col, 8);
                let bb = batch_b.get_col_ptr(row_b, payload_col, 8);
                let va = f64::from_bits(read_u64_le(ba, 0));
                let vb = f64::from_bits(read_u64_le(bb, 0));
                va.partial_cmp(&vb).unwrap_or(Ordering::Equal)
            }
            TYPE_F32 => {
                let ba = batch_a.get_col_ptr(row_a, payload_col, 4);
                let bb = batch_b.get_col_ptr(row_b, payload_col, 4);
                let va = f32::from_bits(read_u32_le(ba, 0));
                let vb = f32::from_bits(read_u32_le(bb, 0));
                va.partial_cmp(&vb).unwrap_or(Ordering::Equal)
            }
            _ => {
                let va = read_signed(
                    batch_a.get_col_ptr(row_a, payload_col, col_size),
                    col_size,
                );
                let vb = read_signed(
                    batch_b.get_col_ptr(row_b, payload_col, col_size),
                    col_size,
                );
                va.cmp(&vb)
            }
        };

        payload_col += 1;

        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

// ---------------------------------------------------------------------------
// merge_batches: the main entry point
// ---------------------------------------------------------------------------

/// Perform N-way merge + consolidation of MemBatch slices into a DirectWriter.
///
/// Rows with the same (PK, payload) have their weights summed.
/// Rows whose net weight is zero are dropped.
pub fn merge_batches(
    batches: &[MemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batches.len();
    if n == 0 {
        return;
    }

    let mut cursors: Vec<MemBatchCursor> = Vec::with_capacity(n);
    for i in 0..n {
        cursors.push(MemBatchCursor::new(i, batches[i].count));
    }

    let mut tree = TournamentTree::build(&cursors, batches, schema);

    let mut has_pending = false;
    let mut pending_batch: usize = 0;
    let mut pending_row: usize = 0;
    let mut pending_pk: u128 = 0;
    let mut pending_weight: i64 = 0;

    while !tree.is_empty() {
        let ci = tree.min_cursor_idx();
        let cur = &cursors[ci];
        let bi = cur.batch_idx;
        let ri = cur.position;
        let cur_pk = batches[bi].get_pk(ri);
        let cur_weight = batches[bi].get_weight(ri);

        if !has_pending {
            pending_batch = bi;
            pending_row = ri;
            pending_pk = cur_pk;
            pending_weight = cur_weight;
            has_pending = true;
        } else if cur_pk != pending_pk {
            if pending_weight != 0 {
                writer.write_row(&batches[pending_batch], pending_row, pending_weight);
            }
            pending_batch = bi;
            pending_row = ri;
            pending_pk = cur_pk;
            pending_weight = cur_weight;
        } else {
            let ord = compare_rows(
                schema,
                &batches[pending_batch],
                pending_row,
                &batches[bi],
                ri,
            );
            if ord == Ordering::Equal {
                pending_weight += cur_weight;
            } else {
                if pending_weight != 0 {
                    writer.write_row(&batches[pending_batch], pending_row, pending_weight);
                }
                pending_batch = bi;
                pending_row = ri;
                pending_pk = cur_pk;
                pending_weight = cur_weight;
            }
        }

        tree.advance_cursor(&mut cursors, batches, schema);
    }

    if has_pending && pending_weight != 0 {
        writer.write_row(&batches[pending_batch], pending_row, pending_weight);
    }
}

/// Convenience: advance the cursor at the given index in the tree.
impl TournamentTree {
    pub fn advance_cursor(
        &mut self,
        cursors: &mut [MemBatchCursor],
        batches: &[MemBatch],
        schema: &SchemaDescriptor,
    ) {
        let ci = self.min_cursor_idx();
        let heap_idx = self.pos_map[ci];
        if heap_idx < 0 {
            return;
        }
        let heap_idx = heap_idx as usize;

        cursors[ci].advance();

        if !cursors[ci].is_valid() {
            self.pos_map[ci] = -1;
            let last = self.heap.len() - 1;
            if heap_idx != last {
                let last_cursor = self.heap[last].cursor_idx;
                self.heap[heap_idx] = HeapNode {
                    key: self.heap[last].key,
                    cursor_idx: last_cursor,
                };
                self.pos_map[last_cursor] = heap_idx as i32;
                self.heap.pop();
                if !self.heap.is_empty() && heap_idx < self.heap.len() {
                    self.sift_down(heap_idx, cursors, batches, schema);
                    self.sift_up(heap_idx, cursors, batches, schema);
                }
            } else {
                self.heap.pop();
            }
        } else {
            self.heap[heap_idx].key = cursors[ci].peek_key(batches);
            self.sift_down(heap_idx, cursors, batches, schema);
        }
    }
}

// ---------------------------------------------------------------------------
// FFI entry point helper: parse flat region arrays into MemBatches + DirectWriter
// ---------------------------------------------------------------------------

/// Parse the flat region arrays from FFI into MemBatch structs.
///
/// Layout per batch: pk_lo, pk_hi, weight, null_bmp, payload_col_0..N-1, blob.
/// regions_per_batch = 4 + num_payload_cols + 1.
pub unsafe fn parse_batches_from_regions<'a>(
    in_ptrs: &[*const u8],
    in_sizes: &[u32],
    in_counts: &[u32],
    num_batches: usize,
    regions_per_batch: usize,
    num_payload_cols: usize,
) -> Vec<MemBatch<'a>> {
    let mut batches = Vec::with_capacity(num_batches);
    for bi in 0..num_batches {
        let base = bi * regions_per_batch;
        let count = in_counts[bi] as usize;

        let pk_lo = std::slice::from_raw_parts(in_ptrs[base], in_sizes[base] as usize);
        let pk_hi = std::slice::from_raw_parts(in_ptrs[base + 1], in_sizes[base + 1] as usize);
        let weight = std::slice::from_raw_parts(in_ptrs[base + 2], in_sizes[base + 2] as usize);
        let null_bmp = std::slice::from_raw_parts(in_ptrs[base + 3], in_sizes[base + 3] as usize);

        let mut col_data = Vec::with_capacity(num_payload_cols);
        for ci in 0..num_payload_cols {
            let ri = base + 4 + ci;
            col_data.push(std::slice::from_raw_parts(in_ptrs[ri], in_sizes[ri] as usize));
        }

        let blob_ri = base + 4 + num_payload_cols;
        let blob = std::slice::from_raw_parts(in_ptrs[blob_ri], in_sizes[blob_ri] as usize);

        batches.push(MemBatch {
            pk_lo,
            pk_hi,
            weight,
            null_bmp,
            col_data,
            blob,
            count,
        });
    }
    batches
}

/// Create a DirectWriter from pre-allocated output region pointers.
pub unsafe fn create_writer_from_regions<'a>(
    out_ptrs: &[*mut u8],
    _regions_per_batch: usize,
    schema: &SchemaDescriptor,
    total_rows: usize,
    total_blob: usize,
) -> DirectWriter<'a> {
    let num_payload_cols = schema.num_columns as usize - 1;

    let pk_lo = std::slice::from_raw_parts_mut(out_ptrs[0], total_rows * 8);
    let pk_hi = std::slice::from_raw_parts_mut(out_ptrs[1], total_rows * 8);
    let weight = std::slice::from_raw_parts_mut(out_ptrs[2], total_rows * 8);
    let null_bmp = std::slice::from_raw_parts_mut(out_ptrs[3], total_rows * 8);

    let pk_index = schema.pk_index as usize;
    let mut col_bufs: Vec<&mut [u8]> = Vec::with_capacity(num_payload_cols);
    let mut payload_idx = 0usize;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_index {
            continue;
        }
        let col_size = schema.columns[ci].size as usize;
        let ptr = out_ptrs[4 + payload_idx];
        col_bufs.push(std::slice::from_raw_parts_mut(ptr, total_rows * col_size));
        payload_idx += 1;
    }

    let blob_idx = 4 + num_payload_cols;
    let blob = std::slice::from_raw_parts_mut(out_ptrs[blob_idx], total_blob);

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
        schema: *schema,
    }
}

/// Fill out_sizes array with the actual bytes written per region.
pub fn fill_output_sizes(
    writer: &DirectWriter,
    schema: &SchemaDescriptor,
    out_sizes: &mut [u32],
) {
    let count = writer.row_count();
    let pk_index = schema.pk_index as usize;

    out_sizes[0] = (count * 8) as u32; // pk_lo
    out_sizes[1] = (count * 8) as u32; // pk_hi
    out_sizes[2] = (count * 8) as u32; // weight
    out_sizes[3] = (count * 8) as u32; // null_bmp

    let mut payload_idx = 0usize;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_index {
            continue;
        }
        let col_size = schema.columns[ci].size as usize;
        out_sizes[4 + payload_idx] = (count * col_size) as u32;
        payload_idx += 1;
    }

    let blob_idx = 4 + (schema.num_columns as usize - 1);
    out_sizes[blob_idx] = writer.blob_written() as u32;
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum weights for
/// identical (PK, payload) rows, drop ghosts (net weight == 0).
///
/// Uses Rust's stable sort on an index array — no tournament tree needed.
pub fn sort_and_consolidate(
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    let pks: Vec<u128> = (0..n).map(|i| batch.get_pk(i)).collect();

    let mut indices: Vec<usize> = (0..n).collect();
    indices.sort_by(|&a, &b| {
        match pks[a].cmp(&pks[b]) {
            Ordering::Equal => compare_rows(schema, batch, a, batch, b),
            ord => ord,
        }
    });

    let mut pending_idx = indices[0];
    let mut pending_pk = pks[pending_idx];
    let mut pending_weight = batch.get_weight(pending_idx);

    for pos in 1..n {
        let cur_idx = indices[pos];
        let cur_pk = pks[cur_idx];

        if cur_pk != pending_pk {
            if pending_weight != 0 {
                writer.write_row(batch, pending_idx, pending_weight);
            }
            pending_idx = cur_idx;
            pending_pk = cur_pk;
            pending_weight = batch.get_weight(cur_idx);
        } else {
            let ord = compare_rows(schema, batch, pending_idx, batch, cur_idx);
            if ord == Ordering::Equal {
                pending_weight += batch.get_weight(cur_idx);
            } else {
                if pending_weight != 0 {
                    writer.write_row(batch, pending_idx, pending_weight);
                }
                pending_idx = cur_idx;
                pending_pk = cur_pk;
                pending_weight = batch.get_weight(cur_idx);
            }
        }
    }

    // Flush last pending
    if pending_weight != 0 {
        writer.write_row(batch, pending_idx, pending_weight);
    }
}

/// Parse a single batch from flat region arrays (single-batch FFI variant).
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
    use crate::compact::{SchemaColumn, SchemaDescriptor};

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

    // Helper to run merge and collect output rows
    fn run_merge(batches: &[MemBatch], schema: &SchemaDescriptor) -> Vec<(u64, u64, i64, i64)> {
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
}
