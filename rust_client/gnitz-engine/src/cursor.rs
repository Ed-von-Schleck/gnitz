//! Unified merge cursor over file-backed and buffer-backed shards.
//!
//! Reuses the TournamentTree + ShardCursor + compare_rows infrastructure
//! from compact.rs. Supports both incremental iteration (for query scans)
//! and bulk merge (for memtable consolidation).

use std::ptr;

use crate::compact::{
    SchemaDescriptor, ShardCursor, ShardWriter, TournamentTree,
};
use crate::shard_reader::MappedShard;

/// A shard reference that is either owned (buffer-backed, created by the cursor)
/// or borrowed (file-backed, caller keeps alive).
pub(crate) enum ShardRef {
    Owned(MappedShard),
    Borrowed(*const MappedShard),
}

impl ShardRef {
    #[inline]
    pub(crate) fn as_ref(&self) -> &MappedShard {
        match self {
            ShardRef::Owned(s) => s,
            ShardRef::Borrowed(p) => unsafe { &**p },
        }
    }
}

// ShardRef contains raw pointers but is only used single-threaded.
unsafe impl Send for ShardRef {}

/// N-way merge cursor over a mix of shard sources.
///
/// On each `advance`, yields the next unique (PK, payload) group with
/// accumulated net weight, skipping ghost rows (net_weight == 0).
///
/// The cursor borrows or owns the underlying shard data. For borrowed
/// (file-backed) shards, the caller must keep the shard handles alive.
pub struct UnifiedCursor {
    shard_refs: Vec<ShardRef>,
    cursors: Vec<ShardCursor>,
    tree: TournamentTree,
    advance_buf: Vec<usize>,
    schema: SchemaDescriptor,
    valid: bool,
    current_shard_idx: usize,
    current_row: usize,
    current_weight: i64,
}

impl UnifiedCursor {
    pub(crate) fn new(shard_refs: Vec<ShardRef>, schema: SchemaDescriptor) -> Self {
        let n = shard_refs.len();
        let shards_view = Self::build_shards_vec(&shard_refs);

        let mut cursors: Vec<ShardCursor> = Vec::with_capacity(n);
        for i in 0..n {
            cursors.push(ShardCursor::new(i, shards_view[i]));
        }
        let tree = TournamentTree::build(&cursors, &shards_view);

        let mut c = UnifiedCursor {
            shard_refs,
            cursors,
            tree,
            advance_buf: Vec::with_capacity(n),
            schema,
            valid: false,
            current_shard_idx: 0,
            current_row: 0,
            current_weight: 0,
        };
        c.find_next();
        c
    }

    pub fn seek(&mut self, key_lo: u64, key_hi: u64) {
        let shards = Self::build_shards_vec(&self.shard_refs);
        for i in 0..self.cursors.len() {
            self.cursors[i].seek(shards[i], key_lo, key_hi);
        }
        self.tree = TournamentTree::build(&self.cursors, &shards);
        self.find_next();
    }

    pub fn advance(&mut self) {
        if !self.valid {
            return;
        }
        let shards = Self::build_shards_vec(&self.shard_refs);
        let num_min = self.advance_buf.len();
        for i in 0..num_min {
            let ci = self.advance_buf[i];
            self.tree.advance_cursor(ci, &mut self.cursors, &shards);
        }
        self.find_next();
    }

    fn find_next(&mut self) {
        let shards = Self::build_shards_vec(&self.shard_refs);
        loop {
            if self.tree.is_empty() {
                self.valid = false;
                return;
            }

            let num_min = self.tree.collect_min_indices(&shards, &self.cursors, &self.schema);
            let mut net_weight: i64 = 0;
            for i in 0..num_min {
                let ci = self.tree.min_indices[i];
                net_weight += self.cursors[ci].weight(shards[self.cursors[ci].shard_idx]);
            }

            self.advance_buf.clear();
            self.advance_buf.extend_from_slice(&self.tree.min_indices[..num_min]);

            if net_weight != 0 {
                let exemplar = self.tree.min_indices[0];
                self.current_shard_idx = self.cursors[exemplar].shard_idx;
                self.current_row = self.cursors[exemplar].position;
                self.current_weight = net_weight;
                self.valid = true;
                return;
            }

            for i in 0..num_min {
                let ci = self.advance_buf[i];
                self.tree.advance_cursor(ci, &mut self.cursors, &shards);
            }
        }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    #[inline]
    pub fn key_lo(&self) -> u64 {
        self.shard_refs[self.current_shard_idx].as_ref().get_pk_lo(self.current_row)
    }

    #[inline]
    pub fn key_hi(&self) -> u64 {
        self.shard_refs[self.current_shard_idx].as_ref().get_pk_hi(self.current_row)
    }

    #[inline]
    pub fn weight(&self) -> i64 {
        self.current_weight
    }

    #[inline]
    pub fn null_word(&self) -> u64 {
        self.shard_refs[self.current_shard_idx].as_ref().get_null_word(self.current_row)
    }

    #[inline]
    pub fn col_ptr(&self, col_idx: usize, col_size: usize) -> *const u8 {
        self.shard_refs[self.current_shard_idx]
            .as_ref()
            .col_ptr_by_logical(self.current_row, col_idx, col_size)
    }

    #[inline]
    pub fn blob_ptr(&self) -> *const u8 {
        self.shard_refs[self.current_shard_idx].as_ref().blob_ptr()
    }

    #[inline]
    pub fn blob_len(&self) -> usize {
        self.shard_refs[self.current_shard_idx].as_ref().blob_len()
    }

    fn build_shards_vec(shard_refs: &[ShardRef]) -> Vec<&MappedShard> {
        shard_refs.iter().map(|r| r.as_ref()).collect()
    }
}

/// Merge N shards into a single ShardWriter (in-memory, no file output).
///
/// Reuses the exact merge loop from compact_shards. The caller can then
/// read the ShardWriter's buffer fields directly or call finalize() to
/// write a shard file.
pub(crate) fn merge_to_writer(
    shard_refs: &[ShardRef],
    schema: &SchemaDescriptor,
) -> ShardWriter {
    let shards: Vec<&MappedShard> = shard_refs.iter().map(|r| r.as_ref()).collect();

    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, shards[i]));
    }
    let mut tree = TournamentTree::build(&cursors, &shards);
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
            tree.advance_cursor(ci, &mut cursors, &shards);
        }
    }

    writer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::SchemaColumn;
    use crate::shard_file;

    fn make_schema() -> SchemaDescriptor {
        let mut cols = [SchemaColumn { type_code: 8, size: 8, nullable: 0, _pad: 0 }; 64];
        cols[1] = SchemaColumn { type_code: 9, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns: cols }
    }

    fn make_buffer_shard(
        pk_lo: &[u64], pk_hi: &[u64], weights: &[i64], nulls: &[u64], col1: &[i64],
        schema: &SchemaDescriptor,
    ) -> MappedShard {
        MappedShard::from_buffers(
            pk_lo.as_ptr() as *const u8,
            pk_hi.as_ptr() as *const u8,
            weights.as_ptr() as *const u8,
            nulls.as_ptr() as *const u8,
            &[col1.as_ptr() as *const u8],
            &[col1.len() * 8],
            ptr::null(), 0,
            pk_lo.len(), schema,
        )
    }

    #[test]
    fn single_shard_iteration() {
        let schema = make_schema();
        let pk_lo = vec![1u64, 2, 3];
        let pk_hi = vec![0u64; 3];
        let weights = vec![1i64, 1, 1];
        let nulls = vec![0u64; 3];
        let col1 = vec![10i64, 20, 30];

        let shard = make_buffer_shard(&pk_lo, &pk_hi, &weights, &nulls, &col1, &schema);
        let refs = vec![ShardRef::Owned(shard)];
        let mut cursor = UnifiedCursor::new(refs, schema);

        assert!(cursor.is_valid());
        assert_eq!(cursor.key_lo(), 1);
        assert_eq!(cursor.weight(), 1);
        cursor.advance();
        assert_eq!(cursor.key_lo(), 2);
        cursor.advance();
        assert_eq!(cursor.key_lo(), 3);
        cursor.advance();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn merge_two_shards() {
        let schema = make_schema();
        let pk1 = vec![1u64, 3, 5];
        let pk2 = vec![2u64, 4, 6];
        let hi = vec![0u64; 3];
        let w = vec![1i64; 3];
        let n = vec![0u64; 3];
        let c1 = vec![10i64, 30, 50];
        let c2 = vec![20i64, 40, 60];

        let s1 = make_buffer_shard(&pk1, &hi, &w, &n, &c1, &schema);
        let s2 = make_buffer_shard(&pk2, &hi, &w, &n, &c2, &schema);
        let refs = vec![ShardRef::Owned(s1), ShardRef::Owned(s2)];
        let mut cursor = UnifiedCursor::new(refs, schema);

        let mut keys = Vec::new();
        while cursor.is_valid() {
            keys.push(cursor.key_lo());
            cursor.advance();
        }
        assert_eq!(keys, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn duplicate_keys_sum_weights() {
        let schema = make_schema();
        let pk1 = vec![1u64, 2, 3];
        let pk2 = vec![1u64, 2, 3];
        let hi = vec![0u64; 3];
        let w1 = vec![1i64, 2, 3];
        let w2 = vec![10i64, 20, 30];
        let n = vec![0u64; 3];
        let c = vec![100i64, 200, 300];

        let s1 = make_buffer_shard(&pk1, &hi, &w1, &n, &c, &schema);
        let s2 = make_buffer_shard(&pk2, &hi, &w2, &n, &c, &schema);
        let refs = vec![ShardRef::Owned(s1), ShardRef::Owned(s2)];
        let mut cursor = UnifiedCursor::new(refs, schema);

        assert_eq!(cursor.weight(), 11); // 1 + 10
        cursor.advance();
        assert_eq!(cursor.weight(), 22); // 2 + 20
        cursor.advance();
        assert_eq!(cursor.weight(), 33); // 3 + 30
    }

    #[test]
    fn ghost_elimination() {
        let schema = make_schema();
        let pk = vec![1u64, 2, 3];
        let hi = vec![0u64; 3];
        let w1 = vec![1i64, 1, 1];
        let w2 = vec![-1i64, 0, -1]; // cancels pk=1 and pk=3
        let n = vec![0u64; 3];
        let c = vec![10i64, 20, 30];

        let s1 = make_buffer_shard(&pk, &hi, &w1, &n, &c, &schema);
        let s2 = make_buffer_shard(&pk, &hi, &w2, &n, &c, &schema);
        let refs = vec![ShardRef::Owned(s1), ShardRef::Owned(s2)];
        let mut cursor = UnifiedCursor::new(refs, schema);

        assert!(cursor.is_valid());
        assert_eq!(cursor.key_lo(), 2);
        assert_eq!(cursor.weight(), 1);
        cursor.advance();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn seek_positions_correctly() {
        let schema = make_schema();
        let pk = vec![10u64, 20, 30, 40, 50];
        let hi = vec![0u64; 5];
        let w = vec![1i64; 5];
        let n = vec![0u64; 5];
        let c = vec![100i64, 200, 300, 400, 500];

        let shard = make_buffer_shard(&pk, &hi, &w, &n, &c, &schema);
        let refs = vec![ShardRef::Owned(shard)];
        let mut cursor = UnifiedCursor::new(refs, schema);

        cursor.seek(25, 0);
        assert!(cursor.is_valid());
        assert_eq!(cursor.key_lo(), 30);

        cursor.seek(100, 0);
        assert!(!cursor.is_valid());
    }

    #[test]
    fn empty_cursor() {
        let schema = make_schema();
        let refs: Vec<ShardRef> = vec![];
        let cursor = UnifiedCursor::new(refs, schema);
        assert!(!cursor.is_valid());
    }

    #[test]
    fn merge_to_writer_basic() {
        let schema = make_schema();
        let pk1 = vec![1u64, 3];
        let pk2 = vec![2u64, 4];
        let hi = vec![0u64; 2];
        let w = vec![1i64; 2];
        let n = vec![0u64; 2];
        let c1 = vec![10i64, 30];
        let c2 = vec![20i64, 40];

        let s1 = make_buffer_shard(&pk1, &hi, &w, &n, &c1, &schema);
        let s2 = make_buffer_shard(&pk2, &hi, &w, &n, &c2, &schema);
        let refs = vec![ShardRef::Owned(s1), ShardRef::Owned(s2)];

        let writer = merge_to_writer(&refs, &schema);
        assert_eq!(writer.count, 4);
    }

    #[test]
    fn merge_to_writer_ghost_elimination() {
        let schema = make_schema();
        let pk = vec![1u64, 2, 3];
        let hi = vec![0u64; 3];
        let w1 = vec![1i64, 1, 1];
        let w2 = vec![-1i64, -1i64, -1i64];
        let n = vec![0u64; 3];
        let c = vec![10i64, 20, 30];

        let s1 = make_buffer_shard(&pk, &hi, &w1, &n, &c, &schema);
        let s2 = make_buffer_shard(&pk, &hi, &w2, &n, &c, &schema);
        let refs = vec![ShardRef::Owned(s1), ShardRef::Owned(s2)];

        let writer = merge_to_writer(&refs, &schema);
        assert_eq!(writer.count, 0);
    }

    #[test]
    fn merge_to_writer_then_write_file() {
        let schema = make_schema();
        let pk = vec![5u64, 10, 15];
        let hi = vec![0u64; 3];
        let w = vec![1i64; 3];
        let n = vec![0u64; 3];
        let c = vec![50i64, 100, 150];

        let shard = make_buffer_shard(&pk, &hi, &w, &n, &c, &schema);
        let refs = vec![ShardRef::Owned(shard)];
        let writer = merge_to_writer(&refs, &schema);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("merged.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        // Build regions from writer and call shard_file::write_shard
        let pk_index = schema.pk_index as usize;
        let mut regions: Vec<&[u8]> = Vec::new();
        regions.push(&writer.pk_lo);
        regions.push(&writer.pk_hi);
        regions.push(&writer.weight);
        regions.push(&writer.null_bitmap);
        for ci in 0..schema.num_columns as usize {
            if ci != pk_index { regions.push(&writer.col_bufs[ci]); }
        }
        regions.push(&writer.blob_heap);

        let pk_lo_s = unsafe {
            std::slice::from_raw_parts(writer.pk_lo.as_ptr() as *const u64, writer.count)
        };
        let pk_hi_s = unsafe {
            std::slice::from_raw_parts(writer.pk_hi.as_ptr() as *const u64, writer.count)
        };
        shard_file::write_shard(
            libc::AT_FDCWD, &cpath, 1, writer.count,
            &regions, pk_lo_s, pk_hi_s, false,
        ).unwrap();

        let read_back = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(read_back.count, 3);
        assert_eq!(read_back.get_pk_lo(0), 5);
        assert_eq!(read_back.get_pk_lo(2), 15);
    }
}
