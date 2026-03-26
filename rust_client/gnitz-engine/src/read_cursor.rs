//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Replaces the RPython UnifiedCursor + TournamentTree + sub-cursors with a
//! single Rust-side opaque handle. Produces rows in (PK, payload) order with
//! inline ghost elimination (net weight=0 rows are skipped).

use std::cmp::Ordering;
use std::ptr;

use crate::columnar::{self, ColumnarSource};
use crate::compact::SchemaDescriptor;
use crate::heap::MergeHeap;
use crate::merge::MemBatch;
use crate::shard_reader::MappedShard;

// ---------------------------------------------------------------------------
// CursorSource — unified access to batch buffers or shard mmap
// ---------------------------------------------------------------------------

enum CursorSource<'a> {
    Batch(MemBatch<'a>),
    /// Borrowed pointer to a MappedShard owned by RPython's ShardHandle.
    /// The cursor does NOT own or free this.
    Shard(*const MappedShard),
}

impl<'a> CursorSource<'a> {
    fn count(&self) -> usize {
        match self {
            CursorSource::Batch(b) => b.count,
            CursorSource::Shard(s) => unsafe { (**s).count },
        }
    }

    #[inline]
    fn get_pk_lo(&self, row: usize) -> u64 {
        match self {
            CursorSource::Batch(b) => b.get_pk_lo(row),
            CursorSource::Shard(s) => unsafe { (**s).get_pk_lo(row) },
        }
    }

    #[inline]
    fn get_pk_hi(&self, row: usize) -> u64 {
        match self {
            CursorSource::Batch(b) => b.get_pk_hi(row),
            CursorSource::Shard(s) => unsafe { (**s).get_pk_hi(row) },
        }
    }

    #[inline]
    fn get_pk(&self, row: usize) -> u128 {
        let lo = self.get_pk_lo(row) as u128;
        let hi = self.get_pk_hi(row) as u128;
        (hi << 64) | lo
    }

    #[inline]
    fn get_weight(&self, row: usize) -> i64 {
        match self {
            CursorSource::Batch(b) => b.get_weight(row),
            CursorSource::Shard(s) => unsafe { (**s).get_weight(row) },
        }
    }

    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        match self {
            CursorSource::Batch(b) => b.get_null_word(row),
            CursorSource::Shard(s) => unsafe { (**s).get_null_word(row) },
        }
    }

    /// Column data as a slice, indexed by PAYLOAD column position.
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_col_ptr(row, payload_col, col_size),
            CursorSource::Shard(s) => unsafe { (**s).get_col_ptr(row, payload_col, col_size) },
        }
    }

    #[inline]
    fn blob_ptr(&self) -> *const u8 {
        match self {
            CursorSource::Batch(b) => {
                if b.blob.is_empty() {
                    ptr::null()
                } else {
                    b.blob.as_ptr()
                }
            }
            CursorSource::Shard(s) => unsafe { (**s).blob_ptr() },
        }
    }

    fn blob_slice(&self) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.blob,
            CursorSource::Shard(s) => unsafe { (**s).blob_slice() },
        }
    }

    fn find_lower_bound(&self, key_lo: u64, key_hi: u64) -> usize {
        match self {
            CursorSource::Batch(b) => {
                // Binary search on batch pk arrays
                let count = b.count;
                let mut lo = 0usize;
                let mut hi = count;
                while lo < hi {
                    let mid = (lo + hi) >> 1;
                    let mid_lo = b.get_pk_lo(mid);
                    let mid_hi = b.get_pk_hi(mid);
                    if pk_lt(mid_lo, mid_hi, key_lo, key_hi) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                lo
            }
            CursorSource::Shard(s) => unsafe {
                (**s).find_lower_bound(key_lo, key_hi)
            },
        }
    }
}

impl<'a> ColumnarSource for CursorSource<'a> {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        CursorSource::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        CursorSource::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        CursorSource::blob_slice(self)
    }
}

#[inline]
fn pk_lt(a_lo: u64, a_hi: u64, b_lo: u64, b_hi: u64) -> bool {
    if a_hi != b_hi {
        a_hi < b_hi
    } else {
        a_lo < b_lo
    }
}

// ---------------------------------------------------------------------------
// ReadCursorEntry — per-source position tracker
// ---------------------------------------------------------------------------

struct ReadCursorEntry<'a> {
    source: CursorSource<'a>,
    position: usize,
    count: usize,
    is_shard: bool,
}

impl<'a> ReadCursorEntry<'a> {
    fn new_batch(batch: MemBatch<'a>) -> Self {
        let count = batch.count;
        ReadCursorEntry {
            source: CursorSource::Batch(batch),
            position: 0,
            count,
            is_shard: false,
        }
    }

    fn new_shard(shard: *const MappedShard) -> Self {
        let count = unsafe { (*shard).count };
        let mut entry = ReadCursorEntry {
            source: CursorSource::Shard(shard),
            position: 0,
            count,
            is_shard: true,
        };
        entry.skip_ghosts();
        entry
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self) {
        if self.is_valid() {
            self.position += 1;
            if self.is_shard {
                self.skip_ghosts();
            }
        }
    }

    fn seek(&mut self, key_lo: u64, key_hi: u64) {
        self.position = self.source.find_lower_bound(key_lo, key_hi);
        if self.is_shard {
            self.skip_ghosts();
        }
    }

    fn skip_ghosts(&mut self) {
        while self.position < self.count {
            if self.source.get_weight(self.position) != 0 {
                return;
            }
            self.position += 1;
        }
    }

    #[inline]
    fn peek_key(&self) -> u128 {
        if self.is_valid() {
            self.source.get_pk(self.position)
        } else {
            u128::MAX
        }
    }

    #[inline]
    fn weight(&self) -> i64 {
        if self.is_valid() {
            self.source.get_weight(self.position)
        } else {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// ReadCursor — the opaque handle exposed via FFI
// ---------------------------------------------------------------------------

pub struct ReadCursor<'a> {
    entries: Vec<ReadCursorEntry<'a>>,
    tree: Option<MergeHeap>,
    schema: SchemaDescriptor,
    // Current row state
    pub valid: bool,
    pub current_key_lo: u64,
    pub current_key_hi: u64,
    pub current_weight: i64,
    pub current_null_word: u64,
    current_entry_idx: usize,
    current_row: usize,
    // Reusable buffer for advance indices
    advance_buf: Vec<usize>,
}

impl<'a> ReadCursor<'a> {
    fn build_tree(entries: &[ReadCursorEntry<'a>], schema: &SchemaDescriptor) -> MergeHeap {
        let node_less = |a: &crate::heap::HeapNode, b: &crate::heap::HeapNode| -> bool {
            if a.key != b.key {
                return a.key < b.key;
            }
            columnar::compare_rows(
                schema,
                &entries[a.idx].source,
                entries[a.idx].position,
                &entries[b.idx].source,
                entries[b.idx].position,
            ) == Ordering::Less
        };
        MergeHeap::build(
            entries.len(),
            |i| {
                if entries[i].is_valid() {
                    Some(entries[i].peek_key())
                } else {
                    None
                }
            },
            &node_less,
        )
    }

    pub fn new(entries: Vec<ReadCursorEntry<'a>>, schema: SchemaDescriptor) -> Self {
        let n = entries.len();
        let tree = if n > 1 {
            Some(Self::build_tree(&entries, &schema))
        } else {
            None
        };
        let mut cursor = ReadCursor {
            entries,
            tree,
            schema,
            valid: false,
            current_key_lo: 0,
            current_key_hi: 0,
            current_weight: 0,
            current_null_word: 0,
            current_entry_idx: 0,
            current_row: 0,
            advance_buf: Vec::with_capacity(n),
        };
        cursor.find_next_non_ghost();
        cursor
    }

    pub fn seek(&mut self, key_lo: u64, key_hi: u64) {
        for e in &mut self.entries {
            e.seek(key_lo, key_hi);
        }
        if self.tree.is_some() {
            self.tree = Some(Self::build_tree(&self.entries, &self.schema));
        }
        self.find_next_non_ghost();
    }

    pub fn advance(&mut self) {
        if !self.valid {
            return;
        }

        if self.entries.len() == 1 {
            self.entries[0].advance();
            self.find_next_non_ghost();
            return;
        }

        // Advance all entries that were at the minimum (PK, payload).
        // Reuse cached min_indices from the last find_next_non_ghost.
        if let Some(ref mut tree) = self.tree {
            self.advance_buf.clear();
            self.advance_buf.extend_from_slice(&tree.min_indices);
            for &ei in &self.advance_buf {
                self.entries[ei].advance();
                let new_key = if self.entries[ei].is_valid() {
                    Some(self.entries[ei].peek_key())
                } else {
                    None
                };
                let node_less = |a: &crate::heap::HeapNode, b: &crate::heap::HeapNode| -> bool {
                    if a.key != b.key {
                        return a.key < b.key;
                    }
                    columnar::compare_rows(
                        &self.schema,
                        &self.entries[a.idx].source,
                        self.entries[a.idx].position,
                        &self.entries[b.idx].source,
                        self.entries[b.idx].position,
                    ) == Ordering::Less
                };
                tree.advance(ei, new_key, &node_less);
            }
        }

        self.find_next_non_ghost();
    }

    fn find_next_non_ghost(&mut self) {
        if self.entries.len() == 1 {
            let e = &self.entries[0];
            if e.is_valid() {
                self.valid = true;
                self.current_key_lo = e.source.get_pk_lo(e.position);
                self.current_key_hi = e.source.get_pk_hi(e.position);
                self.current_weight = e.source.get_weight(e.position);
                self.current_null_word = e.source.get_null_word(e.position);
                self.current_entry_idx = 0;
                self.current_row = e.position;
            } else {
                self.valid = false;
            }
            return;
        }

        let tree = match self.tree {
            Some(ref mut t) => t,
            None => {
                self.valid = false;
                return;
            }
        };
        let schema = &self.schema;

        while !tree.is_empty() {
            let eq_root = |a: &crate::heap::HeapNode, b: &crate::heap::HeapNode| -> Ordering {
                let key_ord = a.key.cmp(&b.key);
                if key_ord != Ordering::Equal {
                    return key_ord;
                }
                columnar::compare_rows(
                    schema,
                    &self.entries[a.idx].source,
                    self.entries[a.idx].position,
                    &self.entries[b.idx].source,
                    self.entries[b.idx].position,
                )
            };
            let num_min = tree.collect_min_indices(&eq_root);
            if num_min == 0 {
                break;
            }

            let mut net_weight: i64 = 0;
            for i in 0..num_min {
                let ei = tree.min_indices[i];
                net_weight += self.entries[ei].weight();
            }

            if net_weight != 0 {
                let exemplar = tree.min_indices[0];
                self.valid = true;
                self.current_key_lo = self.entries[exemplar].source.get_pk_lo(
                    self.entries[exemplar].position,
                );
                self.current_key_hi = self.entries[exemplar].source.get_pk_hi(
                    self.entries[exemplar].position,
                );
                self.current_weight = net_weight;
                self.current_null_word = self.entries[exemplar].source.get_null_word(
                    self.entries[exemplar].position,
                );
                self.current_entry_idx = exemplar;
                self.current_row = self.entries[exemplar].position;
                return;
            }

            self.advance_buf.clear();
            self.advance_buf
                .extend_from_slice(&tree.min_indices[..num_min]);
            for &ei in &self.advance_buf {
                self.entries[ei].advance();
                let new_key = if self.entries[ei].is_valid() {
                    Some(self.entries[ei].peek_key())
                } else {
                    None
                };
                let node_less = |a: &crate::heap::HeapNode, b: &crate::heap::HeapNode| -> bool {
                    if a.key != b.key {
                        return a.key < b.key;
                    }
                    columnar::compare_rows(
                        schema,
                        &self.entries[a.idx].source,
                        self.entries[a.idx].position,
                        &self.entries[b.idx].source,
                        self.entries[b.idx].position,
                    ) == Ordering::Less
                };
                tree.advance(ei, new_key, &node_less);
            }
        }

        self.valid = false;
    }

    /// Raw column pointer for the current row, indexed by LOGICAL column index.
    pub fn col_ptr(&self, col_idx: usize, col_size: usize) -> *const u8 {
        if !self.valid {
            return ptr::null();
        }
        let entry = &self.entries[self.current_entry_idx];
        let row = self.current_row;
        let pk_index = self.schema.pk_index as usize;

        match &entry.source {
            CursorSource::Shard(s) => unsafe {
                (**s).col_ptr_by_logical(row, col_idx, col_size)
            },
            CursorSource::Batch(b) => {
                if col_idx == pk_index {
                    // PK column: return pointer into pk_lo buffer
                    unsafe { b.pk_lo.as_ptr().add(row * 8) }
                } else {
                    // Map logical → payload index
                    let payload_idx = if col_idx < pk_index {
                        col_idx
                    } else {
                        col_idx - 1
                    };
                    if payload_idx < b.col_data.len() {
                        unsafe { b.col_data[payload_idx].as_ptr().add(row * col_size) }
                    } else {
                        ptr::null()
                    }
                }
            }
        }
    }

    /// Blob arena base pointer for the current row's source.
    pub fn blob_ptr(&self) -> *const u8 {
        if !self.valid {
            return ptr::null();
        }
        self.entries[self.current_entry_idx].source.blob_ptr()
    }
}

// ---------------------------------------------------------------------------
// Public constructors for FFI
// ---------------------------------------------------------------------------

/// Build a ReadCursor from batch regions + shard handles.
pub unsafe fn create_read_cursor<'a>(
    batch_regions: &[MemBatch<'a>],
    shard_ptrs: &[*const MappedShard],
    schema: SchemaDescriptor,
) -> ReadCursor<'a> {
    let mut entries = Vec::with_capacity(batch_regions.len() + shard_ptrs.len());

    for batch in batch_regions {
        if batch.count > 0 {
            // Clone the MemBatch (it's just slice references, cheap)
            entries.push(ReadCursorEntry::new_batch(MemBatch {
                pk_lo: batch.pk_lo,
                pk_hi: batch.pk_hi,
                weight: batch.weight,
                null_bmp: batch.null_bmp,
                col_data: batch.col_data.clone(),
                blob: batch.blob,
                count: batch.count,
            }));
        }
    }

    for &shard in shard_ptrs {
        if !shard.is_null() && (*shard).count > 0 {
            entries.push(ReadCursorEntry::new_shard(shard));
        }
    }

    ReadCursor::new(entries, schema)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor};
    use crate::merge::MemBatch;

    fn make_schema_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: crate::compact::type_code::U128,
            size: 16,
            nullable: 0,
            _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: crate::compact::type_code::I64,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    fn make_batch<'a>(
        rows: &[(u64, u64, i64, i64)],
        pk_lo: &'a mut Vec<u8>,
        pk_hi: &'a mut Vec<u8>,
        weight: &'a mut Vec<u8>,
        null_bmp: &'a mut Vec<u8>,
        col0: &'a mut Vec<u8>,
    ) -> MemBatch<'a> {
        pk_lo.clear();
        pk_hi.clear();
        weight.clear();
        null_bmp.clear();
        col0.clear();
        for &(lo, hi, w, val) in rows {
            pk_lo.extend_from_slice(&lo.to_le_bytes());
            pk_hi.extend_from_slice(&hi.to_le_bytes());
            weight.extend_from_slice(&w.to_le_bytes());
            null_bmp.extend_from_slice(&0u64.to_le_bytes());
            col0.extend_from_slice(&val.to_le_bytes());
        }
        MemBatch {
            pk_lo: pk_lo.as_slice(),
            pk_hi: pk_hi.as_slice(),
            weight: weight.as_slice(),
            null_bmp: null_bmp.as_slice(),
            col_data: vec![col0.as_slice()],
            blob: &[],
            count: rows.len(),
        }
    }

    fn scan_all(cursor: &mut ReadCursor) -> Vec<(u64, u64, i64)> {
        let mut rows = Vec::new();
        while cursor.valid {
            rows.push((
                cursor.current_key_lo,
                cursor.current_key_hi,
                cursor.current_weight,
            ));
            cursor.advance();
        }
        rows
    }

    #[test]
    fn test_empty_cursor() {
        let schema = make_schema_i64();
        let entries: Vec<ReadCursorEntry> = vec![];
        let cursor = ReadCursor::new(entries, schema);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_single_batch_scan() {
        let schema = make_schema_i64();
        let mut pk_lo = Vec::new();
        let mut pk_hi = Vec::new();
        let mut w = Vec::new();
        let mut n = Vec::new();
        let mut c = Vec::new();
        let batch = make_batch(
            &[(1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30)],
            &mut pk_lo,
            &mut pk_hi,
            &mut w,
            &mut n,
            &mut c,
        );
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (1, 0, 1));
        assert_eq!(rows[1], (2, 0, 1));
        assert_eq!(rows[2], (3, 0, 1));
    }

    #[test]
    fn test_two_batch_merge() {
        let schema = make_schema_i64();
        let mut pk1 = Vec::new();
        let mut hi1 = Vec::new();
        let mut w1 = Vec::new();
        let mut n1 = Vec::new();
        let mut c1 = Vec::new();
        let b1 = make_batch(
            &[(1, 0, 1, 10), (3, 0, 1, 30)],
            &mut pk1,
            &mut hi1,
            &mut w1,
            &mut n1,
            &mut c1,
        );

        let mut pk2 = Vec::new();
        let mut hi2 = Vec::new();
        let mut w2 = Vec::new();
        let mut n2 = Vec::new();
        let mut c2 = Vec::new();
        let b2 = make_batch(
            &[(2, 0, 1, 20), (4, 0, 1, 40)],
            &mut pk2,
            &mut hi2,
            &mut w2,
            &mut n2,
            &mut c2,
        );

        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[2].0, 3);
        assert_eq!(rows[3].0, 4);
    }

    #[test]
    fn test_ghost_elimination_across_sources() {
        let schema = make_schema_i64();
        // Batch 1: pk=5 val=50 w=+1
        let mut pk1 = Vec::new();
        let mut hi1 = Vec::new();
        let mut w1 = Vec::new();
        let mut n1 = Vec::new();
        let mut c1 = Vec::new();
        let b1 = make_batch(
            &[(5, 0, 1, 50), (10, 0, 1, 100)],
            &mut pk1,
            &mut hi1,
            &mut w1,
            &mut n1,
            &mut c1,
        );

        // Batch 2: pk=5 val=50 w=-1 (retraction)
        let mut pk2 = Vec::new();
        let mut hi2 = Vec::new();
        let mut w2 = Vec::new();
        let mut n2 = Vec::new();
        let mut c2 = Vec::new();
        let b2 = make_batch(
            &[(5, 0, -1, 50)],
            &mut pk2,
            &mut hi2,
            &mut w2,
            &mut n2,
            &mut c2,
        );

        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        // pk=5 cancelled (w=+1-1=0), only pk=10 survives
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (10, 0, 1));
    }

    #[test]
    fn test_seek() {
        let schema = make_schema_i64();
        let mut pk_lo = Vec::new();
        let mut pk_hi = Vec::new();
        let mut w = Vec::new();
        let mut n = Vec::new();
        let mut c = Vec::new();
        let batch = make_batch(
            &[(1, 0, 1, 10), (5, 0, 1, 50), (10, 0, 1, 100)],
            &mut pk_lo,
            &mut pk_hi,
            &mut w,
            &mut n,
            &mut c,
        );
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

        // Seek to pk >= 5
        cursor.seek(5, 0);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key_lo, 5);

        // Seek to pk >= 7 → lands on 10
        cursor.seek(7, 0);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key_lo, 10);

        // Seek past end
        cursor.seek(100, 0);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_same_pk_different_payload_ordering() {
        let schema = make_schema_i64();
        // Two entries with same PK but different payloads
        let mut pk1 = Vec::new();
        let mut hi1 = Vec::new();
        let mut w1 = Vec::new();
        let mut n1 = Vec::new();
        let mut c1 = Vec::new();
        let b1 = make_batch(
            &[(5, 0, 1, 200)],
            &mut pk1,
            &mut hi1,
            &mut w1,
            &mut n1,
            &mut c1,
        );

        let mut pk2 = Vec::new();
        let mut hi2 = Vec::new();
        let mut w2 = Vec::new();
        let mut n2 = Vec::new();
        let mut c2 = Vec::new();
        let b2 = make_batch(
            &[(5, 0, 1, 100)],
            &mut pk2,
            &mut hi2,
            &mut w2,
            &mut n2,
            &mut c2,
        );

        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        // Both survive, sorted by payload (100 < 200)
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (5, 0, 1)); // payload=100
        assert_eq!(rows[1], (5, 0, 1)); // payload=200
    }

    #[test]
    fn test_weight_accumulation_across_sources() {
        let schema = make_schema_i64();
        // Same (PK, payload) in two batches: weights should sum
        let mut pk1 = Vec::new();
        let mut hi1 = Vec::new();
        let mut w1 = Vec::new();
        let mut n1 = Vec::new();
        let mut c1 = Vec::new();
        let b1 = make_batch(
            &[(5, 0, 3, 50)],
            &mut pk1,
            &mut hi1,
            &mut w1,
            &mut n1,
            &mut c1,
        );

        let mut pk2 = Vec::new();
        let mut hi2 = Vec::new();
        let mut w2 = Vec::new();
        let mut n2 = Vec::new();
        let mut c2 = Vec::new();
        let b2 = make_batch(
            &[(5, 0, 7, 50)],
            &mut pk2,
            &mut hi2,
            &mut w2,
            &mut n2,
            &mut c2,
        );

        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (5, 0, 10)); // 3 + 7 = 10
    }
}
