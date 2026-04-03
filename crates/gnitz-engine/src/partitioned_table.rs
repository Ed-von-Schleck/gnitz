//! Partitioned table: hash-routes rows across N child Table handles.
//!
//! User tables have 256 partitions; system tables have 1.  The partition
//! index is `xxh3(pk_lo, pk_hi) & 0xFF`.

use std::sync::Arc;

use crate::compact::SchemaDescriptor;
use crate::memtable::OwnedBatch;
use crate::merge::{self, MemBatch};
use crate::read_cursor::{self, CursorHandle};
use crate::shard_reader::MappedShard;
use crate::table::{self, Table};
use crate::xxh;

// ---------------------------------------------------------------------------
// PartitionedTable
// ---------------------------------------------------------------------------

pub struct PartitionedTable {
    tables: Vec<Table>,
    num_partitions: u32,
    part_offset: u32,
    schema: SchemaDescriptor,
    last_found_partition: Option<usize>,
}

impl PartitionedTable {
    pub fn new(
        dir: &str,
        name: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        num_partitions: u32,
        durable: bool,
        part_start: u32,
        part_end: u32,
        arena_size: u64,
    ) -> Result<Self, i32> {
        table::ensure_dir(dir)?;

        let mut tables = Vec::with_capacity((part_end - part_start) as usize);
        for p in part_start..part_end {
            let part_dir = if num_partitions == 1 {
                format!("{}/part_0", dir)
            } else {
                format!("{}/part_{}", dir, p)
            };
            let t = Table::new(&part_dir, name, schema, table_id, arena_size, durable)?;
            tables.push(t);
        }

        Ok(PartitionedTable {
            tables,
            num_partitions,
            part_offset: part_start,
            schema,
            last_found_partition: None,
        })
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    pub fn ingest_batch_from_regions(
        &mut self,
        ptrs: &[*const u8],
        sizes: &[u32],
        count: u32,
        num_payload_cols: usize,
    ) -> Result<(), i32> {
        if count == 0 || self.tables.is_empty() {
            return Ok(());
        }

        if self.num_partitions == 1 {
            return self.tables[0].ingest_batch_from_regions(ptrs, sizes, count, num_payload_cols);
        }

        let batch = unsafe {
            merge::parse_single_batch_from_regions(ptrs, sizes, count as usize, num_payload_cols)
        };

        let np = self.num_partitions as usize;
        let mut part_indices: Vec<Vec<u32>> = Vec::with_capacity(np);
        for _ in 0..np {
            part_indices.push(Vec::new());
        }

        for i in 0..batch.count {
            let p = partition_for_key(batch.get_pk_lo(i), batch.get_pk_hi(i));
            part_indices[p].push(i as u32);
        }

        let offset = self.part_offset as usize;
        let num_live = self.tables.len();

        for p in 0..np {
            if part_indices[p].is_empty() {
                continue;
            }
            let local = p.wrapping_sub(offset);
            if local >= num_live {
                continue;
            }
            let sub_batch = OwnedBatch::from_indexed_rows(
                &batch, &part_indices[p], &self.schema,
            );
            self.tables[local].ingest_owned_batch(sub_batch)?;
        }

        Ok(())
    }

    pub fn ingest_batch_memonly_from_regions(
        &mut self,
        ptrs: &[*const u8],
        sizes: &[u32],
        count: u32,
        num_payload_cols: usize,
    ) -> Result<(), i32> {
        if count == 0 || self.tables.is_empty() {
            return Ok(());
        }
        self.tables[0].ingest_batch_memonly_from_regions(ptrs, sizes, count, num_payload_cols)
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    pub fn create_cursor(&mut self) -> Result<CursorHandle<'static>, i32> {
        if self.tables.is_empty() {
            return Ok(unsafe {
                read_cursor::create_cursor_from_snapshots(&[], &[], self.schema)
            });
        }

        if self.num_partitions == 1 {
            return self.tables[0].create_cursor();
        }

        let mut all_snapshots: Vec<Arc<OwnedBatch>> = Vec::new();
        let mut all_shard_ptrs: Vec<*const MappedShard> = Vec::new();

        for table in &mut self.tables {
            table.compact_if_needed()?;
            all_snapshots.push(table.get_snapshot());
            all_shard_ptrs.append(&mut table.all_shard_ptrs());
        }

        let handle = unsafe {
            read_cursor::create_cursor_from_snapshots(
                &all_snapshots, &all_shard_ptrs, self.schema,
            )
        };
        Ok(handle)
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    pub fn has_pk(&mut self, key_lo: u64, key_hi: u64) -> bool {
        if self.tables.is_empty() {
            return false;
        }
        if self.num_partitions == 1 {
            return self.tables[0].has_pk(key_lo, key_hi);
        }
        match self.local_index(key_lo, key_hi) {
            Some(local) => self.tables[local].has_pk(key_lo, key_hi),
            None => false,
        }
    }

    pub fn retract_pk(&mut self, key_lo: u64, key_hi: u64) -> (i64, bool) {
        self.last_found_partition = None;
        if self.tables.is_empty() {
            return (0, false);
        }
        let local = if self.num_partitions == 1 {
            0
        } else {
            match self.local_index(key_lo, key_hi) {
                Some(l) => l,
                None => return (0, false),
            }
        };
        let (w, found) = self.tables[local].retract_pk(key_lo, key_hi);
        if found {
            self.last_found_partition = Some(local);
        }
        (w, found)
    }

    pub fn get_weight_for_row(
        &mut self,
        key_lo: u64,
        key_hi: u64,
        ref_batch: &MemBatch,
        ref_row: usize,
    ) -> i64 {
        if self.tables.is_empty() {
            return 0;
        }
        if self.num_partitions == 1 {
            return self.tables[0].get_weight_for_row(key_lo, key_hi, ref_batch, ref_row);
        }
        match self.local_index(key_lo, key_hi) {
            Some(local) => self.tables[local].get_weight_for_row(key_lo, key_hi, ref_batch, ref_row),
            None => 0,
        }
    }

    // ------------------------------------------------------------------
    // Found-row accessors
    // ------------------------------------------------------------------

    pub fn found_null_word(&self) -> u64 {
        match self.last_found_partition {
            Some(local) => self.tables[local].found_null_word(),
            None => 0,
        }
    }

    pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
        match self.last_found_partition {
            Some(local) => self.tables[local].found_col_ptr(payload_col, col_size),
            None => std::ptr::null(),
        }
    }

    pub fn found_blob_ptr(&self) -> *const u8 {
        match self.last_found_partition {
            Some(local) => self.tables[local].found_blob_ptr(),
            None => std::ptr::null(),
        }
    }

    // ------------------------------------------------------------------
    // Broadcast operations
    // ------------------------------------------------------------------

    pub fn flush(&mut self) -> Result<bool, i32> {
        let mut any_wrote = false;
        for table in &mut self.tables {
            if let Ok(true) = table.flush() {
                any_wrote = true;
            }
        }
        Ok(any_wrote)
    }

    pub fn compact_if_needed(&mut self) -> Result<(), i32> {
        for table in &mut self.tables {
            table.compact_if_needed()?;
        }
        Ok(())
    }

    pub fn set_has_wal(&mut self, flag: bool) {
        for table in &mut self.tables {
            table.set_has_wal(flag);
        }
    }

    pub fn current_lsn(&self) -> u64 {
        self.tables.iter().map(|t| t.current_lsn).max().unwrap_or(0)
    }

    pub fn bloom_add(&mut self, key_lo: u64, key_hi: u64) {
        if self.tables.is_empty() {
            return;
        }
        if self.num_partitions == 1 {
            self.tables[0].bloom_add(key_lo, key_hi);
            return;
        }
        if let Some(local) = self.local_index(key_lo, key_hi) {
            self.tables[local].bloom_add(key_lo, key_hi);
        }
    }

    // ------------------------------------------------------------------
    // Partition lifecycle
    // ------------------------------------------------------------------

    pub fn close_partitions_outside(&mut self, start: u32, end: u32) {
        let old_offset = self.part_offset;
        let old_tables = std::mem::take(&mut self.tables);

        for (local, table) in old_tables.into_iter().enumerate() {
            let p = local as u32 + old_offset;
            if p >= start && p < end {
                self.tables.push(table);
            }
            // else: table is dropped here (Table::drop calls close)
        }
        self.part_offset = start;
    }

    pub fn close_all_partitions(&mut self) {
        self.tables.clear();
        self.part_offset = 0;
    }

    pub fn close(&mut self) {
        self.tables.clear();
    }

    /// Get the base directory for child table creation (partition 0's directory).
    pub fn child_base_dir(&self) -> String {
        if self.tables.is_empty() {
            return String::new();
        }
        self.tables[0].directory().to_string()
    }

    pub fn create_child(
        &self,
        child_name: &str,
        child_schema: SchemaDescriptor,
    ) -> Result<Table, i32> {
        if self.tables.is_empty() {
            return Err(-1);
        }
        self.tables[0].create_child(child_name, child_schema)
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    fn local_index(&self, key_lo: u64, key_hi: u64) -> Option<usize> {
        let p = partition_for_key(key_lo, key_hi) as u32;
        let local = p.wrapping_sub(self.part_offset) as usize;
        if local < self.tables.len() {
            Some(local)
        } else {
            None
        }
    }
}

impl Drop for PartitionedTable {
    fn drop(&mut self) {
        self.close();
    }
}

// ---------------------------------------------------------------------------
// Hash routing
// ---------------------------------------------------------------------------

#[inline]
pub fn partition_for_key(pk_lo: u64, pk_hi: u64) -> usize {
    let h = xxh::hash_u128(pk_lo, pk_hi);
    (h & 0xFF) as usize
}

pub fn partition_arena_size(num_partitions: u32) -> u64 {
    if num_partitions <= 1 {
        1024 * 1024
    } else {
        256 * 1024
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor};

    fn make_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: 3, size: 8, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: 7, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_regions(rows: &[(u64, i64, i64)]) -> (Vec<*const u8>, Vec<u32>) {
        let n = rows.len();
        let pk_lo: Vec<u8> = rows.iter().flat_map(|r| r.0.to_le_bytes()).collect();
        let pk_hi: Vec<u8> = vec![0u8; n * 8];
        let weight: Vec<u8> = rows.iter().flat_map(|r| r.1.to_le_bytes()).collect();
        let null_bmp: Vec<u8> = vec![0u8; n * 8];
        let col0: Vec<u8> = rows.iter().flat_map(|r| r.2.to_le_bytes()).collect();
        let blob: Vec<u8> = Vec::new();
        let pk_lo = pk_lo.leak(); let pk_hi = pk_hi.leak();
        let weight = weight.leak(); let null_bmp = null_bmp.leak();
        let col0 = col0.leak(); let blob = blob.leak();
        (vec![pk_lo.as_ptr(), pk_hi.as_ptr(), weight.as_ptr(),
              null_bmp.as_ptr(), col0.as_ptr(), blob.as_ptr()],
         vec![(n*8) as u32, (n*8) as u32, (n*8) as u32,
              (n*8) as u32, (n*8) as u32, 0])
    }

    #[test]
    fn single_partition_lifecycle() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("sp_test");
        let schema = make_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1, false, 0, 1,
            partition_arena_size(1),
        ).unwrap();

        let (ptrs, sizes) = make_regions(&[(10, 1, 100), (20, 1, 200)]);
        pt.ingest_batch_from_regions(&ptrs, &sizes, 2, 1).unwrap();
        assert!(pt.has_pk(10, 0));
        assert!(pt.has_pk(20, 0));
        assert!(!pt.has_pk(99, 0));
        pt.flush().unwrap();
        assert!(pt.has_pk(10, 0));
    }

    #[test]
    fn multi_partition_hash_routing() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("mp_test");
        let schema = make_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 200, 256, false, 0, 256,
            partition_arena_size(256),
        ).unwrap();

        let rows: Vec<(u64, i64, i64)> = (0..100).map(|i| (i * 7 + 13, 1, (i * 100) as i64)).collect();
        let (ptrs, sizes) = make_regions(&rows);
        pt.ingest_batch_from_regions(&ptrs, &sizes, rows.len() as u32, 1).unwrap();

        for &(pk, _, _) in &rows {
            assert!(pt.has_pk(pk, 0), "PK {} not found", pk);
        }
        assert!(!pt.has_pk(999999, 0));
    }

    #[test]
    fn multi_partition_cursor() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("mc_test");
        let schema = make_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 300, 256, false, 0, 256,
            partition_arena_size(256),
        ).unwrap();

        let (ptrs, sizes) = make_regions(&[(30, 1, 300), (10, 1, 100), (20, 1, 200), (40, 1, 400)]);
        pt.ingest_batch_from_regions(&ptrs, &sizes, 4, 1).unwrap();

        let cursor = pt.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key_lo, 10);
    }

    #[test]
    fn retract_pk_routing() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("rt_test");
        let schema = make_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 400, 256, false, 0, 256,
            partition_arena_size(256),
        ).unwrap();

        let (ptrs, sizes) = make_regions(&[(10, 1, 100), (20, 1, 200)]);
        pt.ingest_batch_from_regions(&ptrs, &sizes, 2, 1).unwrap();

        let (w, found) = pt.retract_pk(10, 0);
        assert_eq!(w, 1);
        assert!(found);
        assert!(!pt.found_col_ptr(0, 8).is_null());

        let (_, found) = pt.retract_pk(99, 0);
        assert!(!found);
    }

    #[test]
    fn close_partitions_outside() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cpo_test");
        let schema = make_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 500, 256, false, 0, 256,
            partition_arena_size(256),
        ).unwrap();

        assert_eq!(pt.tables.len(), 256);
        pt.close_partitions_outside(100, 110);
        assert_eq!(pt.tables.len(), 10);
        assert_eq!(pt.part_offset, 100);
    }
}
