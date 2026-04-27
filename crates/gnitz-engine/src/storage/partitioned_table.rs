//! Partitioned table: hash-routes rows across N child Table handles.
//!
//! User tables have 256 partitions; system tables have 1.  The partition
//! index is `xxh3(pk_lo, pk_hi) & 0xFF`.

use std::sync::Arc;

use crate::schema::SchemaDescriptor;
use super::batch::Batch;
use super::error::StorageError;
use super::read_cursor::{self, CursorHandle};
use super::shard_reader::MappedShard;
use super::table::{self, Table};
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
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Self, StorageError> {
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

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch (owned). Moves into tables[0]
    /// directly for the single-partition case, scatters via a borrowed
    /// `MemBatch` view otherwise.
    #[allow(clippy::needless_range_loop)]
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 || self.tables.is_empty() {
            return Ok(());
        }

        if self.num_partitions == 1 {
            return self.tables[0].ingest_owned_batch(batch);
        }

        let mb = batch.as_mem_batch();
        let np = self.num_partitions as usize;
        let mut part_indices: Vec<Vec<u32>> = Vec::with_capacity(np);
        for _ in 0..np {
            part_indices.push(Vec::new());
        }
        for i in 0..mb.count {
            let p = partition_for_key(mb.get_pk(i));
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
            let sub_batch = Batch::from_indexed_rows(
                &mb, &part_indices[p], &self.schema,
            );
            self.tables[local].ingest_owned_batch(sub_batch)?;
        }

        // `batch` drops here; its buffers return to batch_pool.
        Ok(())
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    pub fn create_cursor(&mut self) -> Result<CursorHandle, StorageError> {
        if self.tables.is_empty() {
            return Ok(read_cursor::create_cursor_from_snapshots(&[], &[], self.schema));
        }

        if self.num_partitions == 1 {
            return self.tables[0].create_cursor();
        }

        let mut all_snapshots: Vec<Arc<Batch>> = Vec::new();
        let mut all_shard_arcs: Vec<Arc<MappedShard>> = Vec::new();

        for table in &mut self.tables {
            table.compact_if_needed()?;
            all_snapshots.push(table.get_snapshot());
            all_shard_arcs.append(&mut table.all_shard_arcs());
        }

        let handle = read_cursor::create_cursor_from_snapshots(
            &all_snapshots, &all_shard_arcs, self.schema,
        );
        Ok(handle)
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    pub fn has_pk(&mut self, key: u128) -> bool {
        if self.tables.is_empty() {
            return false;
        }
        if self.num_partitions == 1 {
            return self.tables[0].has_pk(key);
        }
        match self.local_index(key) {
            Some(local) => self.tables[local].has_pk(key),
            None => false,
        }
    }

    pub fn retract_pk(&mut self, key: u128) -> (i64, bool) {
        self.last_found_partition = None;
        if self.tables.is_empty() {
            return (0, false);
        }
        let local = if self.num_partitions == 1 {
            0
        } else {
            match self.local_index(key) {
                Some(l) => l,
                None => return (0, false),
            }
        };
        let (w, found) = self.tables[local].retract_pk(key);
        if found {
            self.last_found_partition = Some(local);
        }
        (w, found)
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

    pub fn flush(&mut self) -> Result<bool, StorageError> {
        let mut any_wrote = false;
        for table in &mut self.tables {
            if table.flush()? {
                any_wrote = true;
            }
        }
        Ok(any_wrote)
    }

    pub fn current_lsn(&self) -> u64 {
        self.tables.iter().map(|t| t.current_lsn).max().unwrap_or(0)
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

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    fn local_index(&self, key: u128) -> Option<usize> {
        let p = partition_for_key(key) as u32;
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
pub fn partition_for_key(pk: u128) -> usize {
    let h = xxh::hash_u128(pk);
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
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

    fn make_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    /// Build an unsorted owned `Batch` of (pk, weight, val_i64) rows
    /// matching `make_schema` (U64 PK + I64 payload).
    fn make_batch(rows: &[(u64, i64, i64)]) -> Batch {
        let schema = make_schema();
        let mut batch = Batch::with_schema(schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            batch.extend_pk(pk as u128);
            batch.extend_weight(&w.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(0, &val.to_le_bytes());
            batch.count += 1;
        }
        if !rows.is_empty() {
            batch.sorted = false;
            batch.consolidated = false;
        }
        batch
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

        pt.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        assert!(pt.has_pk(10));
        assert!(pt.has_pk(20));
        assert!(!pt.has_pk(99));
        pt.flush().unwrap();
        assert!(pt.has_pk(10));
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
        pt.ingest_owned_batch(make_batch(&rows)).unwrap();

        for &(pk, _, _) in &rows {
            assert!(pt.has_pk(pk as u128), "PK {} not found", pk);
        }
        assert!(!pt.has_pk(999999));
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

        pt.ingest_owned_batch(make_batch(&[(30, 1, 300), (10, 1, 100), (20, 1, 200), (40, 1, 400)])).unwrap();

        let cursor = pt.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key_lo(), 10);
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

        pt.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

        let (w, found) = pt.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);
        assert!(!pt.found_col_ptr(0, 8).is_null());

        let (_, found) = pt.retract_pk(99);
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
