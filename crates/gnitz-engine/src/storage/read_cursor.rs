//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cmp::Ordering;
use std::ptr;
use std::sync::Arc;

use super::batch::Batch;
use super::columnar::{self, ColumnarSource};
use crate::schema::SchemaDescriptor;
use super::heap::MergeHeap;
use super::merge::MemBatch;
use super::shard_reader::MappedShard;

// ---------------------------------------------------------------------------
// CursorSource — unified access to in-memory batches or shard mmaps
// ---------------------------------------------------------------------------
//
// Both variants own their backing data via `Arc`, so a `CursorSource` (and
// therefore the enclosing `ReadCursor`) is a self-contained owning value with
// no borrow lifetime.  This is what lets us hand `CursorHandle` (and pointers
// to it) across DAG/VM boundaries without needing a `'static` transmute.

enum CursorSource {
    /// Arc-owned in-memory batch.  The Arc keeps the data alive for the
    /// cursor's lifetime; multiple cursors can share a snapshot.
    Batch(Arc<Batch>),
    /// Arc-owned reference to a MappedShard.  The Arc keeps the mmap alive.
    Shard(Arc<MappedShard>),
}

impl CursorSource {
    #[inline]
    fn get_pk(&self, row: usize) -> u128 {
        match self {
            CursorSource::Batch(b) => b.get_pk(row),
            CursorSource::Shard(s) => s.get_pk(row),
        }
    }

    #[inline]
    fn get_weight(&self, row: usize) -> i64 {
        match self {
            CursorSource::Batch(b) => b.get_weight(row),
            CursorSource::Shard(s) => s.get_weight(row),
        }
    }

    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        match self {
            CursorSource::Batch(b) => b.get_null_word(row),
            CursorSource::Shard(s) => s.get_null_word(row),
        }
    }

    /// Column data as a slice, indexed by PAYLOAD column position.
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_col_ptr(row, payload_col, col_size),
            CursorSource::Shard(s) => s.get_col_ptr(row, payload_col, col_size),
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
            CursorSource::Shard(s) => s.blob_ptr(),
        }
    }

    fn blob_slice(&self) -> &[u8] {
        match self {
            CursorSource::Batch(b) => &b.blob,
            CursorSource::Shard(s) => s.blob_slice(),
        }
    }

    fn find_lower_bound(&self, key: u128) -> usize {
        match self {
            CursorSource::Batch(b) => b.find_lower_bound(key),
            CursorSource::Shard(s) => s.find_lower_bound(key),
        }
    }
}

impl ColumnarSource for CursorSource {
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

/// Compare two heap nodes by (PK, payload) for ReadCursorEntry-based heaps.
#[inline]
fn entry_cmp(
    entries: &[ReadCursorEntry],
    schema: &SchemaDescriptor,
    a: &super::heap::HeapNode,
    b: &super::heap::HeapNode,
) -> Ordering {
    let key_ord = a.key.cmp(&b.key);
    if key_ord != Ordering::Equal {
        return key_ord;
    }
    columnar::compare_rows(
        schema,
        &entries[a.idx].source,
        entries[a.idx].position,
        &entries[b.idx].source,
        entries[b.idx].position,
    )
}

// ---------------------------------------------------------------------------
// ReadCursorEntry — per-source position tracker
// ---------------------------------------------------------------------------

pub(crate) struct ReadCursorEntry {
    source: CursorSource,
    position: usize,
    count: usize,
}

impl ReadCursorEntry {
    fn new_batch(batch: Arc<Batch>) -> Self {
        let count = batch.count;
        ReadCursorEntry {
            source: CursorSource::Batch(batch),
            position: 0,
            count,
        }
    }

    fn new_shard(shard: Arc<MappedShard>) -> Self {
        let count = shard.count;
        let mut entry = ReadCursorEntry {
            source: CursorSource::Shard(shard),
            position: 0,
            count,
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
            self.skip_ghosts();
        }
    }

    fn seek(&mut self, key: u128) {
        self.position = self.source.find_lower_bound(key);
        self.skip_ghosts();
    }

    fn skip_ghosts(&mut self) {
        // Batch sources are always consolidated — no ghost rows.
        let CursorSource::Shard(s) = &self.source else { return; };
        if !s.has_ghosts { return; }
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
// ReadCursor
// ---------------------------------------------------------------------------

pub struct ReadCursor {
    entries: Vec<ReadCursorEntry>,
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

impl ReadCursor {
    fn build_tree(entries: &[ReadCursorEntry], schema: &SchemaDescriptor) -> MergeHeap {
        let less = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| {
            entry_cmp(entries, schema, a, b).is_lt()
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
            &less,
        )
    }

    pub(crate) fn new(entries: Vec<ReadCursorEntry>, schema: SchemaDescriptor) -> Self {
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

    pub fn seek(&mut self, key: u128) {
        for e in &mut self.entries {
            e.seek(key);
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
                let less = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| {
                    entry_cmp(&self.entries, &self.schema, a, b).is_lt()
                };
                tree.advance(ei, new_key, &less);
            }
        }

        self.find_next_non_ghost();
    }

    fn find_next_non_ghost(&mut self) {
        if self.entries.len() == 1 {
            let e = &self.entries[0];
            if e.is_valid() {
                let pos = e.position;
                self.valid = true;
                let pk = e.source.get_pk(pos);
                self.current_key_lo = pk as u64;
                self.current_key_hi = (pk >> 64) as u64;
                self.current_weight = e.source.get_weight(pos);
                self.current_null_word = e.source.get_null_word(pos);
                self.current_entry_idx = 0;
                self.current_row = pos;
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
            let eq_root = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| -> Ordering {
                entry_cmp(&self.entries, schema, a, b)
            };
            let num_min = tree.collect_min_indices(&eq_root);
            if num_min == 0 {
                break;
            }

            let mut net_weight: i64 = 0;
            for i in 0..num_min {
                net_weight += self.entries[tree.min_indices[i]].weight();
            }

            if net_weight != 0 {
                let exemplar = tree.min_indices[0];
                let pos = self.entries[exemplar].position;
                self.valid = true;
                let pk = self.entries[exemplar].source.get_pk(pos);
                self.current_key_lo = pk as u64;
                self.current_key_hi = (pk >> 64) as u64;
                self.current_weight = net_weight;
                self.current_null_word = self.entries[exemplar].source.get_null_word(pos);
                self.current_entry_idx = exemplar;
                self.current_row = pos;
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
                let less = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| {
                    entry_cmp(&self.entries, schema, a, b).is_lt()
                };
                tree.advance(ei, new_key, &less);
            }
        }

        self.valid = false;
    }

    /// Approximate the number of rows remaining in this cursor (upper bound).
    /// Used by the adaptive-swap heuristic in join/semi-join operators.
    pub fn estimated_length(&self) -> usize {
        self.entries.iter().map(|e| e.count.saturating_sub(e.position)).sum()
    }

    /// Raw column pointer for the current row, indexed by LOGICAL column index.
    ///
    /// Returns null for the PK column — use `current_key_lo`/`current_key_hi`
    /// (or `current_key()`) instead.  Returning only `pk_lo` for a 16-byte PK
    /// would silently truncate the high half regardless of backing source.
    pub fn col_ptr(&self, col_idx: usize, col_size: usize) -> *const u8 {
        if !self.valid {
            return ptr::null();
        }
        if col_idx == self.schema.pk_index as usize {
            return ptr::null();
        }
        let entry = &self.entries[self.current_entry_idx];
        let row = self.current_row;
        let pk_index = self.schema.pk_index as usize;

        match &entry.source {
            CursorSource::Shard(s) => s.col_ptr_by_logical(row, col_idx, col_size),
            CursorSource::Batch(b) => {
                // Map logical → payload index
                let payload_idx = if col_idx < pk_index { col_idx } else { col_idx - 1 };
                if payload_idx < b.num_payload_cols() {
                    b.get_col_ptr(row, payload_idx, col_size).as_ptr()
                } else {
                    ptr::null()
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

    /// Blob arena length for the current row's source.
    pub fn blob_len(&self) -> usize {
        if !self.valid {
            return 0;
        }
        self.entries[self.current_entry_idx].source.blob_slice().len()
    }

    /// Current row's PK as a single u128 (convenience over current_key_lo / current_key_hi).
    #[inline]
    pub fn current_key(&self) -> u128 {
        (self.current_key_hi as u128) << 64 | self.current_key_lo as u128
    }

    /// Bulk-drain a single-source cursor into an Batch, bypassing
    /// per-row iteration. Returns `None` for multi-source cursors or
    /// sources with ghosts, signaling the caller to fall back to row-at-a-time.
    ///
    /// `limit == 0` means drain all remaining rows.
    pub(crate) fn drain_single_source(
        &mut self,
        limit: usize,
    ) -> Option<super::batch::Batch> {
        if !self.valid || self.entries.len() != 1 {
            return None;
        }
        let entry = &self.entries[0];
        let start = entry.position;
        let remaining = entry.count - start;
        let row_count = if limit > 0 { remaining.min(limit) } else { remaining };
        let schema = &self.schema;

        let batch = match &entry.source {
            CursorSource::Batch(b) => {
                // Batch sources are always consolidated (no ghosts).
                let end = start + row_count;
                let mut out = Batch::with_schema(*schema, row_count.max(1));
                out.append_batch(b, start, end);
                out.sorted = true;
                out.consolidated = true;
                out
            }
            CursorSource::Shard(s) => {
                if s.has_ghosts {
                    return None;
                }
                s.slice_to_owned_batch(start, row_count, schema)
            }
        };

        // Advance position past the drained rows
        self.entries[0].position = start + row_count;
        self.find_next_non_ghost();
        Some(batch)
    }

    /// If this cursor is backed by exactly one in-memory `Batch` source,
    /// returns a `MemBatch` view over it and the current row position
    /// (`entry.position`).  Returns `None` for multi-source or shard-backed
    /// cursors.
    ///
    /// Used by the bulk retraction path in the catalog to avoid per-row
    /// overhead when copying a contiguous range of rows from a single
    /// in-memory source.
    pub(crate) fn single_mem_batch(&self) -> Option<(MemBatch<'_>, usize)> {
        if !self.valid || self.entries.len() != 1 {
            return None;
        }
        let entry = &self.entries[0];
        match &entry.source {
            CursorSource::Batch(b) => Some((b.as_mem_batch(), entry.position)),
            CursorSource::Shard(_) => None,
        }
    }
}

/// Build a ReadCursor from in-memory batches + shard Arcs.
///
/// Both inputs are passed by `Arc`, so the cursor owns its data and has no
/// borrow lifetime — see the `CursorSource` doc comment.
pub fn create_read_cursor(
    batches: &[Arc<Batch>],
    shard_arcs: &[Arc<MappedShard>],
    schema: SchemaDescriptor,
) -> ReadCursor {
    let mut entries = Vec::with_capacity(batches.len() + shard_arcs.len());

    for batch in batches {
        if batch.count > 0 {
            entries.push(ReadCursorEntry::new_batch(Arc::clone(batch)));
        }
    }

    for shard in shard_arcs {
        if shard.count > 0 {
            entries.push(ReadCursorEntry::new_shard(Arc::clone(shard)));
        }
    }

    ReadCursor::new(entries, schema)
}

// ---------------------------------------------------------------------------
// CursorHandle — owning wrapper around ReadCursor
// ---------------------------------------------------------------------------

/// Owns a `ReadCursor`.  Since the cursor now owns its data via `Arc`s in
/// each `CursorSource`, this is a thin newtype — the separate "owned
/// snapshots" vector that previously kept `Arc<Batch>` alive alongside
/// transmuted-to-`'static` `MemBatch` views is gone.
pub struct CursorHandle {
    pub(crate) cursor: ReadCursor,
}

impl CursorHandle {
    /// Wrap a cursor.
    pub fn from_cursor(cursor: ReadCursor) -> Self {
        CursorHandle { cursor }
    }

    /// Build a CursorHandle from Rust-owned in-memory snapshots (no shards).
    pub fn from_owned(
        snapshots: &[Arc<Batch>],
        schema: crate::schema::SchemaDescriptor,
    ) -> CursorHandle {
        create_cursor_from_snapshots(snapshots, &[], schema)
    }

    /// Mutable access to the inner ReadCursor.
    pub(crate) fn cursor_mut(&mut self) -> &mut ReadCursor {
        &mut self.cursor
    }
}

/// Build a CursorHandle from Rust-owned snapshots + shard Arcs.
///
/// Each snapshot's `Arc<Batch>` and shard's `Arc<MappedShard>` is cloned into
/// the corresponding `CursorSource` entry, keeping the data alive for the
/// cursor's entire lifetime without any unsafe lifetime extension.
pub fn create_cursor_from_snapshots(
    snapshots: &[Arc<Batch>],
    shard_arcs: &[Arc<MappedShard>],
    schema: SchemaDescriptor,
) -> CursorHandle {
    let cursor = create_read_cursor(snapshots, shard_arcs, schema);
    CursorHandle { cursor }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        columns[0] = SchemaColumn {
            type_code: crate::schema::type_code::U128,
            size: 16,
            nullable: 0,
            _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: crate::schema::type_code::I64,
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

    /// Build an `Arc<Batch>` with i64-payload rows.  Tests pre-sort their
    /// inputs and have at most one row per (PK, payload), so we mark the
    /// batch as sorted+consolidated.
    fn make_batch(rows: &[(u64, u64, i64, i64)]) -> Arc<Batch> {
        let schema = make_schema_i64();
        let mut b = Batch::with_schema(schema, rows.len().max(1));
        for &(lo, hi, w, val) in rows {
            b.extend_pk_lo(&lo.to_le_bytes());
            b.extend_pk_hi(&hi.to_le_bytes());
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        Arc::new(b)
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
        let batch = make_batch(&[(1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30)]);
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
        let b1 = make_batch(&[(1, 0, 1, 10), (3, 0, 1, 30)]);
        let b2 = make_batch(&[(2, 0, 1, 20), (4, 0, 1, 40)]);
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
        // Batch 1: pk=5 val=50 w=+1, pk=10 val=100 w=+1
        let b1 = make_batch(&[(5, 0, 1, 50), (10, 0, 1, 100)]);
        // Batch 2: pk=5 val=50 w=-1 (retraction)
        let b2 = make_batch(&[(5, 0, -1, 50)]);
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
        let batch = make_batch(&[(1, 0, 1, 10), (5, 0, 1, 50), (10, 0, 1, 100)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

        // Seek to pk >= 5
        cursor.seek(5);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key_lo, 5);

        // Seek to pk >= 7 → lands on 10
        cursor.seek(7);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key_lo, 10);

        // Seek past end
        cursor.seek(100);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_same_pk_different_payload_ordering() {
        let schema = make_schema_i64();
        // Two entries with same PK but different payloads
        let b1 = make_batch(&[(5, 0, 1, 200)]);
        let b2 = make_batch(&[(5, 0, 1, 100)]);
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
        let b1 = make_batch(&[(5, 0, 3, 50)]);
        let b2 = make_batch(&[(5, 0, 7, 50)]);
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (5, 0, 10)); // 3 + 7 = 10
    }

    #[test]
    fn test_drain_single_source_full() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

        let result = cursor.drain_single_source(0);
        assert!(result.is_some());
        let out = result.unwrap();
        assert_eq!(out.count, 3);
        assert_eq!(out.get_pk(0), 1);
        assert_eq!(out.get_pk(2), 3);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_drain_single_source_with_limit() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30), (4, 0, 1, 40)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

        // Drain first 2
        let out1 = cursor.drain_single_source(2).unwrap();
        assert_eq!(out1.count, 2);
        assert_eq!(out1.get_pk(0), 1);
        assert_eq!(out1.get_pk(1), 2);
        assert!(cursor.valid);

        // Drain remaining 2
        let out2 = cursor.drain_single_source(0).unwrap();
        assert_eq!(out2.count, 2);
        assert_eq!(out2.get_pk(0), 3);
        assert_eq!(out2.get_pk(1), 4);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_drain_multi_source_returns_none() {
        let schema = make_schema_i64();
        let b1 = make_batch(&[(1, 0, 1, 10)]);
        let b2 = make_batch(&[(2, 0, 1, 20)]);
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        assert!(cursor.drain_single_source(0).is_none());
    }

    #[test]
    fn test_col_ptr_pk_returns_null() {
        // PK (logical col 0, pk_index=0) must always return null — callers read
        // the PK through current_key_lo / current_key_hi instead.
        let schema = make_schema_i64();
        let batch = make_batch(&[(42, 0, 1, 99)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let cursor = ReadCursor::new(entries, schema);
        assert!(cursor.valid);
        let pk_index = cursor.schema.pk_index as usize; // 0
        let ptr = cursor.col_ptr(pk_index, 16);
        assert!(ptr.is_null(), "col_ptr for PK index must return null");
    }

    #[test]
    fn test_col_ptr_payload_returns_valid_pointer() {
        // Payload col at logical index 1 must return a non-null pointer with
        // the correct value.
        let schema = make_schema_i64();
        let batch = make_batch(&[(7, 0, 1, 1234)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let cursor = ReadCursor::new(entries, schema);
        assert!(cursor.valid);
        let ptr = cursor.col_ptr(1, 8); // logical col 1 = i64 payload
        assert!(!ptr.is_null(), "col_ptr for payload col must not be null");
        let val = i64::from_le_bytes(unsafe { *(ptr as *const [u8; 8]) });
        assert_eq!(val, 1234);
    }

    #[test]
    fn test_col_ptr_invalid_cursor_returns_null() {
        let schema = make_schema_i64();
        let entries: Vec<ReadCursorEntry> = vec![];
        let cursor = ReadCursor::new(entries, schema);
        assert!(!cursor.valid);
        assert!(cursor.col_ptr(1, 8).is_null());
    }

    #[test]
    fn test_estimated_length_reflects_remaining() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);
        assert_eq!(cursor.estimated_length(), 3);
        cursor.advance();
        assert_eq!(cursor.estimated_length(), 2);
        cursor.advance();
        assert_eq!(cursor.estimated_length(), 1);
        cursor.advance();
        assert_eq!(cursor.estimated_length(), 0);
    }

    #[test]
    fn test_current_key_helper() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(0xDEAD, 0xBEEF, 1, 0)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let cursor = ReadCursor::new(entries, schema);
        assert!(cursor.valid);
        let expected = (0xBEEFu128 << 64) | 0xDEADu128;
        assert_eq!(cursor.current_key(), expected);
        assert_eq!(cursor.current_key_lo, 0xDEAD);
        assert_eq!(cursor.current_key_hi, 0xBEEF);
    }
}
