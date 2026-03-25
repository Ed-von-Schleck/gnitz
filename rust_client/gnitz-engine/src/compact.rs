//! Self-contained shard compaction: N-way merge of sorted shard files.
//!
//! Replaces the RPython compactor.py + tournament_tree.py + shard_table.py (read) +
//! writer_table.py (write) + comparator.py for the compaction path.

use std::collections::HashMap;
use std::ffi::CStr;
#[cfg(test)]
use std::fs;

use crate::shard_reader::MappedShard;
use crate::util::{read_u32_le, read_u64_le};
#[cfg(test)]
use crate::util::read_i64_le;
use crate::xxh;

// Type codes (from core/types.py). All defined for completeness;
// the compare_rows dispatch only uses F32/F64/STRING/U128 explicitly.
#[allow(dead_code)]
mod type_code {
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
use type_code::{F32 as TYPE_F32, F64 as TYPE_F64, STRING as TYPE_STRING, U128 as TYPE_U128};
#[cfg(test)]
use type_code::{I64 as TYPE_I64, U64 as TYPE_U64};

const SHORT_STRING_THRESHOLD: usize = 12;

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

#[repr(C)]
#[derive(Clone)]
pub struct SchemaDescriptor {
    pub num_columns: u32,
    pub pk_index: u32,
    pub columns: [SchemaColumn; 64],
}

// ---------------------------------------------------------------------------
// Guard output result (returned from merge_and_route)
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone)]
pub struct GuardResult {
    pub guard_key_lo: u64,
    pub guard_key_hi: u64,
    pub filename: [u8; 256], // null-terminated
}

// ---------------------------------------------------------------------------
// Shard cursor (position + ghost skip)
// ---------------------------------------------------------------------------

pub(crate) struct ShardCursor {
    pub(crate) shard_idx: usize,
    pub(crate) position: usize,
    pub(crate) count: usize,
}

impl ShardCursor {
    pub(crate) fn new(shard_idx: usize, shard: &MappedShard) -> Self {
        let mut c = ShardCursor {
            shard_idx,
            position: 0,
            count: shard.count,
        };
        c.skip_ghosts(shard);
        c
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.position < self.count
    }

    pub(crate) fn advance(&mut self, shard: &MappedShard) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(shard);
        }
    }

    pub(crate) fn seek(&mut self, shard: &MappedShard, key_lo: u64, key_hi: u64) {
        self.position = shard.find_lower_bound(key_lo, key_hi);
        self.skip_ghosts(shard);
    }

    fn skip_ghosts(&mut self, shard: &MappedShard) {
        while self.position < self.count {
            if shard.get_weight(self.position) != 0 {
                return;
            }
            self.position += 1;
        }
    }

    pub(crate) fn peek_key(&self, shard: &MappedShard) -> u128 {
        if self.is_valid() {
            shard.get_pk(self.position)
        } else {
            u128::MAX
        }
    }

    pub(crate) fn weight(&self, shard: &MappedShard) -> i64 {
        if self.is_valid() {
            shard.get_weight(self.position)
        } else {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Row comparison
// ---------------------------------------------------------------------------

pub(crate) fn compare_rows(
    schema: &SchemaDescriptor,
    shards: &[&MappedShard],
    shard_a: usize,
    row_a: usize,
    shard_b: usize,
    row_b: usize,
) -> std::cmp::Ordering {
    let sa = &shards[shard_a];
    let sb = &shards[shard_b];
    let pk_index = schema.pk_index as usize;
    let mut payload_col_a: usize = 0;
    let mut payload_col_b: usize = 0;

    for ci in 0..schema.num_columns as usize {
        if ci == pk_index {
            continue;
        }

        // Null comparison: null < non-null, null == null
        let null_a = is_null(sa, row_a, ci, pk_index);
        let null_b = is_null(sb, row_b, ci, pk_index);
        if null_a && null_b {
            payload_col_a += 1;
            payload_col_b += 1;
            continue;
        }
        if null_a {
            return std::cmp::Ordering::Less;
        }
        if null_b {
            return std::cmp::Ordering::Greater;
        }

        let col = &schema.columns[ci];
        let col_size = col.size as usize;

        let ord = match col.type_code {
            TYPE_STRING => {
                let ptr_a = sa.get_col_ptr(row_a, payload_col_a, 16);
                let ptr_b = sb.get_col_ptr(row_b, payload_col_b, 16);
                compare_german_strings(ptr_a, sa.blob_slice(), ptr_b, sb.blob_slice())
            }
            TYPE_U128 => {
                let ba = sa.get_col_ptr(row_a, payload_col_a, 16);
                let bb = sb.get_col_ptr(row_b, payload_col_b, 16);
                let va = ((read_u64_le(ba, 8) as u128) << 64) | (read_u64_le(ba, 0) as u128);
                let vb = ((read_u64_le(bb, 8) as u128) << 64) | (read_u64_le(bb, 0) as u128);
                va.cmp(&vb)
            }
            TYPE_F64 => {
                let ba = sa.get_col_ptr(row_a, payload_col_a, 8);
                let bb = sb.get_col_ptr(row_b, payload_col_b, 8);
                let va = f64::from_bits(read_u64_le(ba, 0));
                let vb = f64::from_bits(read_u64_le(bb, 0));
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
            TYPE_F32 => {
                let ba = sa.get_col_ptr(row_a, payload_col_a, 4);
                let bb = sb.get_col_ptr(row_b, payload_col_b, 4);
                let va = f32::from_bits(read_u32_le(ba, 0));
                let vb = f32::from_bits(read_u32_le(bb, 0));
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
            _ => {
                // Signed integer comparison for all int types
                let va = read_signed(sa.get_col_ptr(row_a, payload_col_a, col_size), col_size);
                let vb = read_signed(sb.get_col_ptr(row_b, payload_col_b, col_size), col_size);
                va.cmp(&vb)
            }
        };

        payload_col_a += 1;
        payload_col_b += 1;

        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }

    std::cmp::Ordering::Equal
}

pub(crate) fn is_null(shard: &MappedShard, row: usize, col_idx: usize, pk_index: usize) -> bool {
    let null_word = shard.get_null_word(row);
    let payload_idx = if col_idx < pk_index { col_idx } else { col_idx - 1 };
    (null_word >> payload_idx) & 1 != 0
}

fn read_signed(bytes: &[u8], size: usize) -> i64 {
    match size {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => 0,
    }
}

fn compare_german_strings(a: &[u8], blob_a: &[u8], b: &[u8], blob_b: &[u8]) -> std::cmp::Ordering {
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
fn string_byte(struct_bytes: &[u8], blob: &[u8], length: usize, i: usize) -> u8 {
    if length <= SHORT_STRING_THRESHOLD {
        struct_bytes[8 + (i - 4)]
    } else {
        let heap_offset = read_u64_le(struct_bytes, 8) as usize;
        blob[heap_offset + i]
    }
}

// ---------------------------------------------------------------------------
// Tournament tree (min-heap with u128 keys)
// ---------------------------------------------------------------------------

pub(crate) struct HeapNode {
    pub(crate) key: u128,
    pub(crate) cursor_idx: usize,
}

pub(crate) struct TournamentTree {
    pub(crate) heap: Vec<HeapNode>,
    pub(crate) pos_map: Vec<i32>,
    pub(crate) min_indices: Vec<usize>,
}

impl TournamentTree {
    /// Build the heap. Requires shards/cursors/schema for full-row comparison.
    pub(crate) fn build(
        cursors: &[ShardCursor], shards: &[&MappedShard], schema: &SchemaDescriptor,
    ) -> Self {
        let n = cursors.len();
        let mut heap = Vec::with_capacity(n);
        let mut pos_map = vec![-1i32; n];

        for i in 0..n {
            if cursors[i].is_valid() {
                let idx = heap.len();
                pos_map[i] = idx as i32;
                heap.push(HeapNode {
                    key: cursors[i].peek_key(&shards[cursors[i].shard_idx]),
                    cursor_idx: i,
                });
            }
        }

        let mut tree = TournamentTree {
            heap,
            pos_map,
            min_indices: Vec::with_capacity(n),
        };
        let size = tree.heap.len();
        if size > 1 {
            for i in (0..size / 2).rev() {
                tree.sift_down(i, shards, cursors, schema);
            }
        }
        tree
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub(crate) fn min_key(&self) -> u128 {
        if self.heap.is_empty() { u128::MAX } else { self.heap[0].key }
    }

    /// Collect all cursor indices whose (PK, payload) equals the root.
    /// Uses heap pruning: if a node > root, skip its entire subtree.
    pub(crate) fn collect_min_indices(
        &mut self,
        shards: &[&MappedShard],
        cursors: &[ShardCursor],
        schema: &SchemaDescriptor,
    ) -> usize {
        self.min_indices.clear();
        if self.heap.is_empty() {
            return 0;
        }
        self.collect_equal(0, shards, cursors, schema);
        self.min_indices.len()
    }

    fn collect_equal(
        &mut self, idx: usize,
        shards: &[&MappedShard], cursors: &[ShardCursor], schema: &SchemaDescriptor,
    ) {
        if idx >= self.heap.len() { return; }
        if idx == 0 || self.compare_to_root(idx, shards, cursors, schema) == std::cmp::Ordering::Equal {
            self.min_indices.push(self.heap[idx].cursor_idx);
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < self.heap.len() {
                self.collect_equal(left, shards, cursors, schema);
            }
            if right < self.heap.len() {
                self.collect_equal(right, shards, cursors, schema);
            }
        }
        // If > root, prune entire subtree
    }

    fn compare_to_root(
        &self, idx: usize,
        shards: &[&MappedShard], cursors: &[ShardCursor], schema: &SchemaDescriptor,
    ) -> std::cmp::Ordering {
        let a = &self.heap[idx];
        let b = &self.heap[0];
        let key_ord = a.key.cmp(&b.key);
        if key_ord != std::cmp::Ordering::Equal { return key_ord; }
        let ca = &cursors[a.cursor_idx];
        let cb = &cursors[b.cursor_idx];
        compare_rows(schema, shards, ca.shard_idx, ca.position, cb.shard_idx, cb.position)
    }

    pub(crate) fn advance_cursor(
        &mut self,
        cursor_idx: usize,
        cursors: &mut [ShardCursor],
        shards: &[&MappedShard],
        schema: &SchemaDescriptor,
    ) {
        let heap_idx = self.pos_map[cursor_idx];
        if heap_idx < 0 { return; }
        let heap_idx = heap_idx as usize;

        let shard = &shards[cursors[cursor_idx].shard_idx];
        cursors[cursor_idx].advance(shard);

        if !cursors[cursor_idx].is_valid() {
            self.pos_map[cursor_idx] = -1;
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
                    self.sift_down(heap_idx, shards, &*cursors, schema);
                    self.sift_up(heap_idx, shards, &*cursors, schema);
                }
            } else {
                self.heap.pop();
            }
        } else {
            let shard = &shards[cursors[cursor_idx].shard_idx];
            self.heap[heap_idx].key = cursors[cursor_idx].peek_key(shard);
            self.sift_down(heap_idx, shards, &*cursors, schema);
        }
    }

    /// Full-row comparison: (PK, then payload). Matches RPython _compare_nodes.
    fn node_less(
        &self, i: usize, j: usize,
        shards: &[&MappedShard], cursors: &[ShardCursor], schema: &SchemaDescriptor,
    ) -> bool {
        let ki = self.heap[i].key;
        let kj = self.heap[j].key;
        if ki != kj { return ki < kj; }
        let ci = &cursors[self.heap[i].cursor_idx];
        let cj = &cursors[self.heap[j].cursor_idx];
        compare_rows(schema, shards, ci.shard_idx, ci.position, cj.shard_idx, cj.position)
            == std::cmp::Ordering::Less
    }

    fn sift_down(
        &mut self, mut idx: usize,
        shards: &[&MappedShard], cursors: &[ShardCursor], schema: &SchemaDescriptor,
    ) {
        loop {
            let mut smallest = idx;
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < self.heap.len() && self.node_less(left, smallest, shards, cursors, schema) {
                smallest = left;
            }
            if right < self.heap.len() && self.node_less(right, smallest, shards, cursors, schema) {
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
        &mut self, mut idx: usize,
        shards: &[&MappedShard], cursors: &[ShardCursor], schema: &SchemaDescriptor,
    ) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.node_less(idx, parent, shards, cursors, schema) {
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
// Shard writer
// ---------------------------------------------------------------------------

pub(crate) struct ShardWriter {
    pub(crate) pk_lo: Vec<u8>,
    pub(crate) pk_hi: Vec<u8>,
    pub(crate) weight: Vec<u8>,
    pub(crate) null_bitmap: Vec<u8>,
    pub(crate) col_bufs: Vec<Vec<u8>>,
    pub(crate) blob_heap: Vec<u8>,
    blob_cache: HashMap<(u64, usize), usize>,
    pub(crate) count: usize,
}

impl ShardWriter {
    pub(crate) fn new(schema: &SchemaDescriptor) -> Self {
        let mut col_bufs = Vec::with_capacity(schema.num_columns as usize);
        for ci in 0..schema.num_columns as usize {
            if ci == schema.pk_index as usize {
                col_bufs.push(Vec::new()); // placeholder, never written
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
        }
    }

    pub(crate) fn add_row(
        &mut self,
        key: u128,
        weight: i64,
        shard: &MappedShard,
        row: usize,
        schema: &SchemaDescriptor,
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

        let pk_index = schema.pk_index as usize;
        let mut null_word: u64 = 0;
        let mut payload_idx: usize = 0;

        for ci in 0..schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let is_null = is_null(shard, row, ci, pk_index);
            if is_null {
                let null_bit_pos = if ci < pk_index { ci } else { ci - 1 };
                null_word |= 1u64 << null_bit_pos;
            }
            self.write_column(ci, payload_idx, is_null, shard, row, schema);
            payload_idx += 1;
        }
        self.null_bitmap.extend_from_slice(&null_word.to_le_bytes());
    }

    fn write_column(
        &mut self,
        col_idx: usize,
        payload_col_idx: usize,
        is_null: bool,
        shard: &MappedShard,
        row: usize,
        schema: &SchemaDescriptor,
    ) {
        let col = &schema.columns[col_idx];
        let col_size = col.size as usize;
        let buf = &mut self.col_bufs[col_idx];

        if is_null {
            buf.extend(std::iter::repeat(0u8).take(col_size));
            return;
        }

        let src = shard.get_col_ptr(row, payload_col_idx, col_size);

        match col.type_code {
            TYPE_STRING => {
                self.write_string_column(col_idx, src, shard.blob_slice());
            }
            _ => {
                // All fixed-width types: direct copy
                buf.extend_from_slice(src);
            }
        }
    }

    fn write_string_column(&mut self, col_idx: usize, src_struct: &[u8], src_blob: &[u8]) {
        let length = read_u32_le(src_struct, 0) as usize;

        // Build the 16-byte German string struct
        let mut dest = [0u8; 16];
        // Length
        dest[0..4].copy_from_slice(&(length as u32).to_le_bytes());
        // Prefix
        dest[4..8].copy_from_slice(&src_struct[4..8]);

        if length <= SHORT_STRING_THRESHOLD {
            // Inline: copy suffix from struct bytes 8..16
            let suffix_len = if length > 4 { length - 4 } else { 0 };
            dest[8..8 + suffix_len].copy_from_slice(&src_struct[8..8 + suffix_len]);
        } else {
            // Heap: read old offset, get data, dedup into our blob heap
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
            // Collision check
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

    fn finalize(
        &self,
        path: &CStr,
        table_id: u32,
        schema: &SchemaDescriptor,
    ) -> Result<(), i32> {
        let pk_index = schema.pk_index as usize;
        let mut regions: Vec<&[u8]> = Vec::new();
        regions.push(&self.pk_lo);
        regions.push(&self.pk_hi);
        regions.push(&self.weight);
        regions.push(&self.null_bitmap);
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            regions.push(&self.col_bufs[ci]);
        }
        regions.push(&self.blob_heap);

        let (pk_lo_s, pk_hi_s) = if self.count > 0 {
            unsafe {(
                std::slice::from_raw_parts(self.pk_lo.as_ptr() as *const u64, self.count),
                std::slice::from_raw_parts(self.pk_hi.as_ptr() as *const u64, self.count),
            )}
        } else {
            (&[][..], &[][..])
        };

        crate::shard_file::write_shard(
            libc::AT_FDCWD, path, table_id, self.count,
            &regions, pk_lo_s, pk_hi_s, true,
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
// Entry points
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
) -> i32 {
    // Open all input shards
    let mut owned_shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema, false) {
            Ok(s) => owned_shards.push(s),
            Err(e) => return e,
        }
    }
    let shards: Vec<&MappedShard> = owned_shards.iter().collect();

    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, shards[i]));
    }

    let mut tree = TournamentTree::build(&cursors, &shards, schema);
    let mut writer = ShardWriter::new(schema);

    let mut advance_buf: Vec<usize> = Vec::with_capacity(shards.len());
    while !tree.is_empty() {
        let min_key = tree.min_key();
        let num_min = tree.collect_min_indices(&shards, &cursors, schema);

        let mut net_weight: i64 = 0;
        for i in 0..num_min {
            let ci = tree.min_indices[i];
            net_weight += cursors[ci].weight(shards[cursors[ci].shard_idx]);
        }

        if net_weight != 0 {
            let exemplar = tree.min_indices[0];
            let shard_idx = cursors[exemplar].shard_idx;
            let row = cursors[exemplar].position;
            writer.add_row(min_key, net_weight, shards[shard_idx], row, schema);
        }

        advance_buf.clear();
        advance_buf.extend_from_slice(&tree.min_indices[..num_min]);
        for &ci in &advance_buf {
            tree.advance_cursor(ci, &mut cursors, &shards, schema);
        }
    }

    match writer.finalize(output_file, table_id, schema) {
        Ok(()) => 0,
        Err(e) => e,
    }
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

    // Open all input shards
    let mut owned_shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema, false) {
            Ok(s) => owned_shards.push(s),
            Err(e) => return e,
        }
    }
    let shards: Vec<&MappedShard> = owned_shards.iter().collect();

    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, shards[i]));
    }

    let mut tree = TournamentTree::build(&cursors, &shards, schema);

    let mut writers: Vec<ShardWriter> = Vec::with_capacity(num_guards);
    let out_dir_str = output_dir.to_str().unwrap_or("");
    let mut out_filenames: Vec<String> = Vec::with_capacity(num_guards);
    for i in 0..num_guards {
        writers.push(ShardWriter::new(schema));
        out_filenames.push(format!(
            "{}/shard_{}_{}_{}_G{}.db",
            out_dir_str, table_id, lsn_tag, format!("L{}", level_num), i
        ));
    }

    let mut advance_buf: Vec<usize> = Vec::with_capacity(shards.len());
    while !tree.is_empty() {
        let min_key = tree.min_key();
        let num_min = tree.collect_min_indices(&shards, &cursors, schema);

        let mut net_weight: i64 = 0;
        for i in 0..num_min {
            let ci = tree.min_indices[i];
            net_weight += cursors[ci].weight(shards[cursors[ci].shard_idx]);
        }

        if net_weight != 0 {
            let guard_idx = find_guard_for_key(guard_keys, min_key);
            let exemplar = tree.min_indices[0];
            let shard_idx = cursors[exemplar].shard_idx;
            let row = cursors[exemplar].position;
            writers[guard_idx].add_row(min_key, net_weight, shards[shard_idx], row, schema);
        }

        advance_buf.clear();
        advance_buf.extend_from_slice(&tree.min_indices[..num_min]);
        for &ci in &advance_buf {
            tree.advance_cursor(ci, &mut cursors, &shards, schema);
        }
    }

    // Finalize non-empty writers, build results
    let mut result_count: i32 = 0;
    for i in 0..num_guards {
        if writers[i].count > 0 {
            let cpath = std::ffi::CString::new(out_filenames[i].as_str()).unwrap();
            match writers[i].finalize(&cpath, table_id, schema) {
                Ok(()) => {
                    // Check file exists
                    if std::path::Path::new(&out_filenames[i]).exists() {
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
                }
                Err(e) => return e,
            }
        }
    }

    result_count
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        writer.finalize(&cpath, 0, schema).unwrap();
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
        writer.finalize(&cpath, 0, &schema).unwrap();

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
        writer.finalize(&cpath, 0, &schema).unwrap();

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
