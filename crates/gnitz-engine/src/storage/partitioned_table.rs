//! Partitioned table: hash-routes rows across N child Table handles.
//!
//! User tables have 256 partitions; system tables have 1.  The partition
//! index is `xxh3(pk_lo, pk_hi) & 0xFF`.

use std::rc::Rc;

use crate::schema::{NARROW_PK_MAX_BYTES, SchemaDescriptor};
use super::batch::Batch;
use super::error::StorageError;
use super::read_cursor::{self, CursorHandle};
use super::shard_reader::MappedShard;
use super::table::{self, FlushOutcome, FlushWork, Table};

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
        // `mix` is hardcoded to 256 buckets (`(h >> 56) as usize`); any other
        // partition count makes `part_indices[p]` index out of bounds in
        // `ingest_owned_batch`. Programming invariant — must fire in release.
        assert!(
            num_partitions == 1 || num_partitions == 256,
            "PartitionedTable supports only 1 or 256 partitions, got {num_partitions}",
        );
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
        if mb.pk_stride as usize > NARROW_PK_MAX_BYTES {
            for i in 0..mb.count {
                let p = partition_for_pk_bytes(mb.get_pk_bytes(i));
                part_indices[p].push(i as u32);
            }
        } else {
            for i in 0..mb.count {
                let p = partition_for_key(mb.get_pk(i));
                part_indices[p].push(i as u32);
            }
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

    /// Open a read-only cursor over every partition (memtable runs + shards).
    /// Infallible, non-mutating — the recommended default. See
    /// `Table::open_cursor`.
    pub fn open_cursor(&self) -> CursorHandle {
        if self.tables.is_empty() {
            return read_cursor::create_cursor_from_snapshots(&[], &[], self.schema);
        }
        if self.num_partitions == 1 {
            return self.tables[0].open_cursor();
        }
        let mut all_snapshots: Vec<Rc<Batch>> = Vec::new();
        let mut all_shard_arcs: Vec<Rc<MappedShard>> = Vec::new();
        for table in &self.tables {
            all_snapshots.extend(table.snapshot_runs().iter().cloned());
            all_shard_arcs.extend(table.all_shard_arcs());
        }
        read_cursor::create_cursor_from_snapshots(
            &all_snapshots, &all_shard_arcs, self.schema,
        )
    }

    /// Open a cursor after running `compact_if_needed` on each partition.
    /// Maintenance-only — see `Table::create_cursor_compacting` for the
    /// validator hazard this name surfaces. The lint guards external callers,
    /// not this delegation.
    #[allow(clippy::disallowed_methods)]
    pub fn create_cursor_compacting(&mut self) -> Result<CursorHandle, StorageError> {
        if self.tables.is_empty() {
            return Ok(read_cursor::create_cursor_from_snapshots(&[], &[], self.schema));
        }

        if self.num_partitions == 1 {
            return self.tables[0].create_cursor_compacting();
        }

        let mut all_snapshots: Vec<Rc<Batch>> = Vec::new();
        let mut all_shard_arcs: Vec<Rc<MappedShard>> = Vec::new();

        for table in &mut self.tables {
            table.compact_if_needed()?;
            all_snapshots.extend(table.snapshot_runs().iter().cloned());
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

    /// Byte-keyed sibling of [`has_pk`] for wide (`pk_stride > 16`) PKs.
    pub fn has_pk_bytes(&mut self, key: &[u8]) -> bool {
        if self.tables.is_empty() {
            return false;
        }
        if self.num_partitions == 1 {
            return self.tables[0].has_pk_bytes(key);
        }
        match self.local_index_bytes(key) {
            Some(local) => self.tables[local].has_pk_bytes(key),
            None => false,
        }
    }

    /// Byte-keyed sibling of [`retract_pk`] for wide PKs.
    pub fn retract_pk_bytes(&mut self, key: &[u8]) -> (i64, bool) {
        self.last_found_partition = None;
        if self.tables.is_empty() {
            return (0, false);
        }
        let local = if self.num_partitions == 1 {
            0
        } else {
            match self.local_index_bytes(key) {
                Some(l) => l,
                None => return (0, false),
            }
        };
        let (w, found) = self.tables[local].retract_pk_bytes(key);
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

    pub fn found_blob_slice(&self) -> &[u8] {
        match self.last_found_partition {
            Some(local) => self.tables[local].found_blob_slice(),
            None        => &[],
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

    /// Phase 1 across all partitions. Returns one (partition_idx, FlushWork)
    /// pair per partition that produced deferred work. `Empty`/`DoneInline`
    /// outcomes are silently consumed (memtable already reset for those).
    pub fn flush_prepare(&mut self) -> Result<Vec<(usize, FlushWork)>, StorageError> {
        let mut works = Vec::new();
        for (i, table) in self.tables.iter_mut().enumerate() {
            match table.flush_prepare(true)? {
                FlushOutcome::Empty | FlushOutcome::DoneInline => {}
                FlushOutcome::Pending(w) => works.push((i, *w)),
            }
        }
        Ok(works)
    }

    /// Phase 3: dispatch each FlushWork back to its partition's `flush_commit`.
    /// Returns the dirfds to fsync (one per partition that committed).
    pub fn flush_commit_batch(
        &mut self,
        works: Vec<(usize, FlushWork)>,
    ) -> Result<Vec<libc::c_int>, StorageError> {
        let mut dirfds = Vec::with_capacity(works.len());
        for (idx, w) in works {
            if let Some(fd) = self.tables[idx].flush_commit(w)? {
                dirfds.push(fd);
            }
        }
        Ok(dirfds)
    }

    pub fn current_lsn(&self) -> u64 {
        self.tables.iter().map(|t| t.current_lsn).max().unwrap_or(0)
    }

    // ------------------------------------------------------------------
    // Partition lifecycle
    // ------------------------------------------------------------------

    pub fn close_partitions_outside(&mut self, start: u32, end: u32) {
        // The cached index refers to the old `tables` layout; rebuilding the
        // vector below would leave it dangling.
        self.last_found_partition = None;
        assert!(
            start <= end,
            "close_partitions_outside: start ({start}) > end ({end})",
        );
        assert!(
            start >= self.part_offset,
            "close_partitions_outside: left-expansion (start={start} < part_offset={}) \
             not supported — surviving tables would map to wrong indices",
            self.part_offset,
        );
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
        self.last_found_partition = None;
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

    /// Byte-keyed sibling of [`local_index`] for wide PKs. `partition_for_pk_bytes`
    /// is bit-identical to `partition_for_key` for `len <= 16`.
    fn local_index_bytes(&self, key: &[u8]) -> Option<usize> {
        let p = partition_for_pk_bytes(key) as u32;
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

// Multiplicative hash: two Fibonacci multipliers XOR'd together.
// ~4 instructions vs ~20 for XXH3-64; distribution across 256 buckets
// is sufficient for worker routing. XXH3 is reserved for filters
// (xor8, bloom) where collision quality matters.
#[inline(always)]
fn mix(pk: u128) -> usize {
    let lo = pk as u64;
    let hi = (pk >> 64) as u64;
    let h = lo.wrapping_mul(0x9e3779b97f4a7c15_u64)
             ^ hi.wrapping_mul(0x6c62272e07bb0142_u64);
    (h >> 56) as usize
}

#[inline]
pub fn partition_for_key(pk: u128) -> usize {
    mix(pk)
}

/// Route an OPK PK region (any width) to a partition. For `len ≤ 16` the OPK
/// bytes are big-endian, so `widen_pk_be` right-aligns them to recover the
/// native unsigned value (sign-flipped for signed); `mix` of that equals
/// `partition_for_key(widen_pk_be(bytes))`. This is the invariant the join
/// router relies on: `extract_col_key` (both PK and OPK-encoded payload paths)
/// also funnels through `widen_pk_be`, so the two sides of a distributed join
/// agree. For wide regions (`len > 16`) it takes the top 8 bits of xxh3 of the
/// OPK bytes directly (uniformly distributed already).
#[inline]
pub fn partition_for_pk_bytes(bytes: &[u8]) -> usize {
    if bytes.len() <= 16 {
        mix(gnitz_wire::widen_pk_be(bytes, bytes.len()))
    } else {
        (crate::xxh::checksum(bytes) >> 56) as usize
    }
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
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
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

        let cursor = pt.open_cursor();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key as u64, 10);
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

    fn make_wide_schema() -> SchemaDescriptor {
        // 3×U64 compound PK → pk_stride = 24 (> 16, wide).
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        )
    }

    #[test]
    fn partition_for_pk_bytes_narrow_invariance() {
        // For every narrow width, routing the physically-stored OPK
        // (big-endian) bytes must equal partition_for_key of the same value.
        let vals: [u128; 9] = [
            0, 1, 7, 42, 255, 65537, 0x0123_4567_89ab_cdef,
            u64::MAX as u128, u128::MAX,
        ];
        for &v in &vals {
            for &len in &[1usize, 2, 4, 8, 16] {
                // Truncate v to the column width, then OPK-encode (big-endian).
                let value = if len == 16 { v } else { v & ((1u128 << (len * 8)) - 1) };
                let be = value.to_be_bytes();
                let opk = &be[16 - len..];
                assert_eq!(
                    partition_for_pk_bytes(opk),
                    partition_for_key(value),
                    "len={len} v={v:#x}",
                );
            }
        }
    }

    #[test]
    fn partition_for_pk_bytes_wide_determinism_and_spread() {
        for &stride in &[24usize, 64, 80] {
            // Determinism: same bytes → same bucket; always in 0..256.
            let mut seen = [false; 256];
            let mut buckets = 0;
            for k in 0u64..4000 {
                let mut pk = vec![0u8; stride];
                pk[..8].copy_from_slice(&k.to_le_bytes());
                pk[8..16].copy_from_slice(&(k.wrapping_mul(2654435761)).to_le_bytes());
                let p = partition_for_pk_bytes(&pk);
                assert!(p < 256, "stride={stride} k={k} p={p}");
                assert_eq!(p, partition_for_pk_bytes(&pk), "deterministic");
                if !seen[p] {
                    seen[p] = true;
                    buckets += 1;
                }
            }
            assert!(buckets > 64, "stride={stride}: only {buckets}/256 buckets hit");
        }
    }

    #[test]
    fn ingest_owned_batch_wide_routing() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("wide_test");
        let schema = make_wide_schema();
        // Only partition 0 is live. Per-partition Table::ingest (and its
        // memtable upsert, which still calls get_pk and is an out-of-scope
        // boundary for wide PKs) only runs for rows routed to partition 0.
        // We feed only PKs that route elsewhere, so this exercises the new
        // wide *routing* loop end-to-end with no downstream Table ingest.
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 700, 256, false, 0, 1,
            partition_arena_size(256),
        ).unwrap();

        let mut batch = Batch::with_schema(schema, 256);
        let mut n = 0;
        let mut k = 0u64;
        while n < 200 {
            let mut pk = [0u8; 24];
            pk[..8].copy_from_slice(&(k * 7 + 13).to_le_bytes());
            pk[8..16].copy_from_slice(&(k * 31 + 5).to_le_bytes());
            pk[16..24].copy_from_slice(&(k + 1).to_le_bytes());
            k += 1;
            if partition_for_pk_bytes(&pk) == 0 {
                continue; // would hit the out-of-scope memtable boundary
            }
            batch.extend_pk_bytes(&pk);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(0, &(n as i64).to_le_bytes());
            batch.count += 1;
            n += 1;
        }
        batch.sorted = false;
        batch.consolidated = false;
        // The wide-PK routing loop must not panic (get_pk would, for
        // pk_stride > 16). All rows route outside the single live
        // partition, so no Table::ingest is invoked.
        pt.ingest_owned_batch(batch).unwrap();
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

    /// A stale `last_found_partition` (set by a prior `retract_pk` hit) must be
    /// invalidated when `close_partitions_outside` rebuilds the `tables` vector,
    /// or `found_null_word()` would index into the now-smaller vector.
    #[test]
    fn close_partitions_outside_invalidates_found_partition() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cpo_stale_test");
        let schema = make_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(), "test", schema, 600, 256, false, 0, 256,
            partition_arena_size(256),
        ).unwrap();

        // Pick a PK whose partition we can exclude from the surviving range.
        let pk = 12345u128;
        let part = partition_for_key(pk) as u32;
        pt.ingest_owned_batch(make_batch(&[(pk as u64, 1, 999)])).unwrap();

        let (_, found) = pt.retract_pk(pk);
        assert!(found, "row must be found, setting last_found_partition");

        // Close every partition except a small range that excludes `part`.
        let (start, end) = if part >= 200 { (0u32, 10u32) } else { (200u32, 210u32) };
        pt.close_partitions_outside(start, end);

        // Must not panic and must report the cleared-found default.
        assert_eq!(pt.found_null_word(), 0);
        assert!(pt.found_col_ptr(0, 8).is_null());
    }
}
