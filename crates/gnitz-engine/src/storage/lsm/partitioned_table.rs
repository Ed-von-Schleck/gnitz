//! Partitioned table: hash-routes rows across N child Table handles.
//!
//! A relation the catalog placed `Keyed` hash-routes across 256 partitions; one
//! placed `Replicated` or `Local` is unhashed and holds one. The 256-bucket
//! index is the Fibonacci `mix(pk) >> 56` (see `schema::key`), not
//! `xxh3 & 0xFF`.

use std::cell::RefCell;
use std::rc::Rc;

use super::batch::Batch;
use super::child_dir::ChildAddr;
use super::error::StorageError;
use super::read_cursor::{self, ReadCursor};
use super::shard_reader::MappedShard;
use super::table::{self, RecoverySource, Table};
#[cfg(test)]
use super::table::{FlushOutcome, FlushWork};
use crate::foundation::fault::Seam;
use crate::schema::SchemaDescriptor;
#[cfg(test)]
use gnitz_wire::{partition_for_key, partition_for_pk_bytes};

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its partition subdirs, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

thread_local! {
    /// Reused per-partition scatter index buffers for `ingest_owned_batch`.
    /// Clears (retaining capacity) rather than reallocating 256 vecs per
    /// ingest — same hold-across-work, no-cap shape as exchange.rs's pools.
    static PARTITION_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

// ---------------------------------------------------------------------------
// PartitionedTable
// ---------------------------------------------------------------------------

/// How a table distributes its rows across child `Table` handles, and which
/// children this process holds. The child set is part of the routing rather
/// than a separate range pair, so an unhashed store cannot be built with
/// anything but its own one child.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Routing {
    /// One child holding the whole local dataset, its rows not keyed by
    /// `partition_for_pk` at all — a replicated base table's full copy, or a
    /// `Local` view's slice of its own making — homed at the owning worker's rank.
    Unhashed { rank: u32 },
    /// 256-way hash scatter by `mix(pk) >> 56`, over the `[start, end)` slice
    /// of the tiling this process owns.
    Hashed { start: u32, end: u32 },
}

impl Routing {
    /// True for the single rank-homed child, whose rows are not keyed by
    /// `partition_for_pk`. The one spelling of the shape test, so a caller
    /// holding a bare `Routing` need not re-`matches!` it.
    pub(crate) const fn is_unhashed(self) -> bool {
        matches!(self, Routing::Unhashed { .. })
    }

    /// The children this store holds, in `tables` order.
    pub fn children(self) -> impl Iterator<Item = ChildAddr<'static>> {
        let (range, addr): (std::ops::Range<u32>, fn(u32) -> ChildAddr<'static>) = match self {
            Routing::Unhashed { rank } => (rank..rank + 1, ChildAddr::Local),
            Routing::Hashed { start, end } => (start..end, ChildAddr::Partition),
        };
        range.map(addr)
    }

    /// Every child this store has across the whole cluster at `num_workers` —
    /// one per launched rank when unhashed, all 256 partitions when hashed. The
    /// boot resume verdict enumerates manifests through this;
    /// [`children`](Self::children) is this process's slice of the same set.
    pub fn cluster_children(self, num_workers: u32) -> impl Iterator<Item = ChildAddr<'static>> {
        let (range, addr): (std::ops::Range<u32>, fn(u32) -> ChildAddr<'static>) = if self.is_unhashed() {
            (0..num_workers, ChildAddr::Local)
        } else {
            (0..NUM_PARTITIONS, ChildAddr::Partition)
        };
        range.map(addr)
    }
}

/// Scatter bucket count for `Routing::Hashed`; `mix` takes `(h >> 56)` ∈ 0..256.
/// The one partition-layout constant — catalog and runtime derive from it.
pub const NUM_PARTITIONS: u32 = 256;

/// Contiguous partition range `[start, end)` owned by `worker_id` of
/// `num_workers`: equal chunks, with the last worker absorbing the remainder.
/// The one spelling of worker→partition assignment, shared by the fork-time
/// trim, the worker boot, and the boot resume verdict. The `.max(1)` guards a
/// zero-workers call (start 0, full range).
pub fn partition_range(worker_id: u32, num_workers: u32) -> (u32, u32) {
    let chunk = NUM_PARTITIONS / num_workers.max(1);
    let start = worker_id * chunk;
    let end = if worker_id + 1 >= num_workers {
        NUM_PARTITIONS
    } else {
        (worker_id + 1) * chunk
    };
    (start, end)
}

/// Report rows the ingest scatter routed outside this store. Outlined and
/// `#[cold]` so the format arguments do not pin the scatter loop's counters to
/// the stack on the path that never takes it.
#[cold]
#[inline(never)]
fn misrouted_rows(rows: usize, partition: usize, routing: Routing) -> StorageError {
    gnitz_error!("storage: {rows} row(s) routed to partition {partition}, outside this store's {routing:?}");
    StorageError::MisroutedRows
}

pub struct PartitionedTable {
    tables: Vec<Table>,
    routing: Routing,
    schema: SchemaDescriptor,
}

impl PartitionedTable {
    pub fn new(
        dir: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        routing: Routing,
        recovery_source: RecoverySource,
    ) -> Result<Self, StorageError> {
        // Per-child arena: an unhashed store holds the whole dataset in one
        // child (1 MiB); a hashed store spreads it over 256 (256 KiB each).
        let arena_size: u64 = match routing {
            Routing::Unhashed { .. } => 1 << 20,
            Routing::Hashed { .. } => 256 << 10,
        };
        table::ensure_dir(dir)?;

        // Widen the window where the table dir exists but its partition subdirs
        // do not, so a concurrent master remove_dir_all (DROP) deterministically
        // races this create. User tables only.
        if matches!(routing, Routing::Hashed { .. }) {
            if let Some(ms) = TABLE_CREATE_DELAY.count() {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }

        // Workers share the data directory, so an unhashed store's child
        // is stamped with the rank that owns it (`rep_{k}`) — a fixed name would
        // have every worker flushing the same files, and a partition index would
        // move the child when the worker count changes. The master's pre-fork
        // child is rank 0; a worker that inherits it across the fork re-homes it
        // to its own rank before any flush (`rehome_unhashed_stores`).
        let mut tables = Vec::new();
        for child in routing.children() {
            tables.push(Table::new(
                &child.dir(dir),
                schema,
                table_id,
                arena_size,
                recovery_source,
            )?);
        }

        Ok(PartitionedTable {
            tables,
            routing,
            schema,
        })
    }

    /// How this store is routed, and which children it holds. The boot child-dir
    /// reconciliation reads it to decide which grammar is live.
    pub(crate) fn routing(&self) -> Routing {
        self.routing
    }

    /// Replace the comparator schema in place across the store and every child
    /// partition (ALTER … DROP NOT NULL). Routing is unaffected — DROP NOT NULL
    /// never touches PK columns — but the swap must reach every partition
    /// atomically so no child keeps the stale `FixedIntNonnull` comparator.
    pub fn swap_schema(&mut self, schema: SchemaDescriptor) {
        self.schema = schema;
        for t in &mut self.tables {
            t.swap_schema(schema);
        }
    }

    /// True for an unhashed store — one child holding the whole local dataset,
    /// homed at this worker's rank (a replicated base table or replicated-derived
    /// view). The bootstrap trim exempts these so the child is never dropped on a
    /// worker whose partition range excludes the child's index.
    pub(crate) fn is_unhashed(&self) -> bool {
        self.routing.is_unhashed()
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch (owned). Moves into tables[0]
    /// directly for the unhashed case, scatters via a borrowed
    /// `MemBatch` view otherwise.
    #[cfg(test)] // production ingest goes through ingest_returning_effective
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 || self.tables.is_empty() {
            return Ok(());
        }
        if self.is_unhashed() {
            return self.tables[0].ingest_owned_batch(batch);
        }
        self.scatter_ingest(&batch)
        // `batch` drops here; its buffers return to batch_pool.
    }

    /// Ingest a borrowed Batch. The scatter path copies per partition either
    /// way; the unhashed path routes to `Table::ingest_borrowed_batch`, whose
    /// consolidation pass doubles as the single copy for an unconsolidated
    /// batch (see there).
    pub fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        if batch.count == 0 || self.tables.is_empty() {
            return Ok(());
        }
        if self.is_unhashed() {
            return self.tables[0].ingest_borrowed_batch(batch);
        }
        self.scatter_ingest(batch)
    }

    #[allow(clippy::needless_range_loop)]
    fn scatter_ingest(&mut self, batch: &Batch) -> Result<(), StorageError> {
        let mb = batch.as_mem_batch();
        let np = NUM_PARTITIONS as usize;

        // Thread-local per-partition index pool, mirroring exchange.rs's
        // SCATTER_INDICES / WORKER_ROWS: clears (retaining capacity) per call
        // rather than allocating 256 vecs every ingest. The borrow is held
        // across the inner single-`Table` ingests, which never re-enter this
        // function (no nested PARTITION_INDICES borrow).
        PARTITION_INDICES.with(|pool| {
            let mut part_indices = pool.borrow_mut();
            if part_indices.len() < np {
                part_indices.resize_with(np, Vec::new);
            }
            part_indices[..np].iter_mut().for_each(Vec::clear);

            // Route every row by the table's distribution prefix via the shared
            // `partition_for_pk` (the leading OPK bytes; the full PK for the
            // default). Ingest, probe (`slot_for_key`), and the write-side
            // scatter all funnel through it, so a row lands in the same partition
            // wherever it is routed.
            for i in 0..mb.count {
                part_indices[self.schema.partition_for_pk(mb.get_pk_bytes(i))].push(i as u32);
            }

            for p in 0..np {
                if part_indices[p].is_empty() {
                    continue;
                }
                // Every production producer routes to the owning worker before the
                // ingest, so a row outside this store's own partition range means
                // the relation is *placed* by one key and *addressed* by another.
                // Reported rather than dropped: the ingest callers treat a storage
                // error as committed data not applied and fail stop, where a silent
                // drop would let the relation diverge with no error and no log line.
                let Some(local) = self.local_slot(p) else {
                    return Err(misrouted_rows(part_indices[p].len(), p, self.routing));
                };
                let sub_batch = Batch::from_indexed_rows(&mb, &part_indices[p], &self.schema);
                self.tables[local].ingest_owned_batch(sub_batch)?;
            }
            Ok(())
        })
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// Gather every live partition's read sources — memtable `snapshot_runs`,
    /// RAM-tier `in_memory_runs`, and `all_shard_arcs_iter` — into one
    /// (snapshots, shards) pair for `create_read_cursor`. The snapshot
    /// accumulator is pre-seeded with the known memtable-run count; the shard
    /// iterator extends without a throwaway per-partition `Vec`.
    fn gather_runs(tables: &[Table]) -> (Vec<Rc<Batch>>, Vec<Rc<MappedShard>>) {
        let mut snapshots: Vec<Rc<Batch>> = Vec::with_capacity(tables.iter().map(|t| t.snapshot_runs().len()).sum());
        let mut shards: Vec<Rc<MappedShard>> = Vec::with_capacity(tables.len());
        for table in tables {
            snapshots.extend(table.snapshot_runs().iter().cloned());
            snapshots.extend(table.in_memory_runs());
            shards.extend(table.all_shard_arcs_iter());
        }
        (snapshots, shards)
    }

    /// Open a read-only cursor over every partition (memtable runs + shards).
    /// Infallible, non-mutating — the recommended default. See
    /// `Table::open_cursor`.
    ///
    /// Child count, not routing, picks the path: `Table::open_cursor` gathers
    /// the same three run sources `gather_runs` would, so a lone child skips
    /// the two accumulator `Vec`s whether it is unhashed or a hashed store
    /// trimmed to one partition.
    pub fn open_cursor(&self) -> ReadCursor {
        match self.tables.as_slice() {
            [] => read_cursor::create_read_cursor(&[], &[], self.schema),
            [only] => only.open_cursor(),
            many => {
                let (snaps, shards) = Self::gather_runs(many);
                read_cursor::create_read_cursor(&snaps, &shards, self.schema)
            }
        }
    }

    /// A cursor over ONLY the partition that could hold `key` (full OPK bytes),
    /// or `None` when this process holds none. [`open_cursor`](Self::open_cursor)
    /// merges every partition; for an exact key all but one of them contribute
    /// nothing but a loser-tree leaf and a binary search per probe.
    ///
    /// Resolution is [`slot_for_key`](Self::slot_for_key) — the function ingest
    /// routes through — so a probe can never miss a row the merged cursor would
    /// have found.
    pub(crate) fn open_cursor_for_key(&self, key: &[u8]) -> Option<ReadCursor> {
        self.slot_for_key(key).map(|local| self.tables[local].open_cursor())
    }

    /// Raw run and shard rows across every local partition, without building a
    /// cursor. Not equal to `open_cursor().estimated_length()`: `ReadCursor::new`
    /// drives once at open, so the cursor's figure is one group lower — an
    /// irrelevant difference to the ratio the selectivity gate takes.
    pub(crate) fn estimated_rows(&self) -> usize {
        self.tables.iter().map(Table::estimated_rows).sum()
    }

    /// Run `compact_if_needed` on every partition. Maintenance-only; readers
    /// that want an up-to-date L1 call this before `open_cursor`.
    pub fn compact_if_needed(&mut self) -> Result<(), StorageError> {
        for table in &mut self.tables {
            table.compact_if_needed()?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// Native `u128` PK existence check. Routes via the OPK bytes — exactly as
    /// ingestion does (`partition_for_pk_bytes(get_pk_bytes)`) — so a signed or
    /// compound key reaches the same partition it was ingested into. Routing on
    /// the raw native value would `mix(native) != mix(widen_pk_be(opk))` and probe
    /// the wrong partition. The sole caller is the test-only lone-PK FK existence
    /// check (passes a native value); all DML retraction routes through `_bytes`.
    #[cfg(test)]
    pub(crate) fn has_pk(&mut self, key: u128) -> bool {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        self.has_pk_bytes(opk.pk_bytes())
    }

    #[cfg(test)] // no production caller after the §4 DML/UPSERT byte-path fixes
    pub fn retract_pk(&mut self, key: u128) -> (i64, Option<table::RowRef>) {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        self.retract_pk_bytes(opk.pk_bytes())
    }

    /// Byte-keyed sibling of [`has_pk`] for wide (`pk_stride > 16`) PKs.
    pub fn has_pk_bytes(&mut self, key: &[u8]) -> bool {
        self.slot_for_key(key)
            .is_some_and(|local| self.tables[local].has_pk_bytes(key))
    }

    /// Byte-keyed sibling of [`retract_pk`] for wide PKs. Returns the net
    /// weight and, when positive, the live stored row as an owned `RowRef`.
    pub fn retract_pk_bytes(&mut self, key: &[u8]) -> (i64, Option<table::RowRef>) {
        match self.slot_for_key(key) {
            Some(local) => self.tables[local].retract_pk_bytes(key),
            None => (0, None),
        }
    }

    // ------------------------------------------------------------------
    // Broadcast operations
    // ------------------------------------------------------------------

    pub fn flush(&mut self) -> Result<(), StorageError> {
        for table in &mut self.tables {
            table.flush()?;
        }
        Ok(())
    }

    /// Mutable access to every partition's `Table`. The checkpoint flush rounds
    /// collect partitions through this as raw `*mut Table` and drive each
    /// through the per-`Table` two-phase flush. An unhashed store holds
    /// exactly its one worker-owned partition here.
    pub fn partitions_mut(&mut self) -> &mut [Table] {
        &mut self.tables
    }

    pub fn current_lsn(&self) -> u64 {
        self.tables.iter().map(|t| t.current_lsn()).max().unwrap_or(0)
    }

    /// Recovery watermark: the **min** `current_lsn` across partitions, 0 when
    /// empty. `flush` commits partitions sequentially, so a partial family
    /// flush (ENOSPC/EIO on a later partition after an earlier one committed)
    /// advances only the committed partitions' counters. The recovery dedupe
    /// filter must use this floor — skipping by the max would drop committed
    /// SAL zones whose rows the lagging partition never flushed. Under-dedupe
    /// (re-replaying already-flushed zones) is safe: replay is idempotent.
    pub fn min_flushed_lsn(&self) -> u64 {
        self.tables.iter().map(|t| t.current_lsn()).min().unwrap_or(0)
    }

    // ------------------------------------------------------------------
    // Partition lifecycle
    // ------------------------------------------------------------------

    /// Hashed stores only — an unhashed store's child belongs to its rank, not
    /// to a partition range, and the bootstrap trim exempts it.
    pub fn close_partitions_outside(&mut self, start: u32, end: u32) {
        assert!(start <= end, "close_partitions_outside: start ({start}) > end ({end})",);
        let old_offset = match self.routing {
            Routing::Hashed { start: s, .. } => s,
            Routing::Unhashed { .. } => panic!("close_partitions_outside on an unhashed store"),
        };
        assert!(
            start >= old_offset,
            "close_partitions_outside: left-expansion (start={start} < offset={old_offset}) \
             not supported — surviving tables would map to wrong indices",
        );
        let old_tables = std::mem::take(&mut self.tables);

        for (local, table) in old_tables.into_iter().enumerate() {
            let p = local as u32 + old_offset;
            if p >= start && p < end {
                self.tables.push(table);
            }
            // else: table is dropped here (Table::drop calls close)
        }
        self.routing = Routing::Hashed {
            start,
            end: start + self.tables.len() as u32,
        };
    }

    /// Drop every child. The routing *shape* is kept — the post-fork master
    /// holds no children but an unhashed store is still unhashed.
    pub fn close_all_partitions(&mut self) {
        self.tables.clear();
        if let Routing::Hashed { start, end } = &mut self.routing {
            *start = 0;
            *end = 0;
        }
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    /// Maps a global partition id to this worker's local `tables` slot, or
    /// `None` when the partition is not held locally. Single source of the
    /// global→local translation for both the ingest scatter and the PK probe.
    /// A partition below the store's `start` underflows the `wrapping_sub` to a
    /// value past `tables.len()`, which the bound check maps to `None`.
    fn local_slot(&self, p: usize) -> Option<usize> {
        let Routing::Hashed { start, .. } = self.routing else {
            return None;
        };
        let local = p.wrapping_sub(start as usize);
        (local < self.tables.len()).then_some(local)
    }

    /// The `tables` slot that could hold `key` (full OPK bytes), or `None` when
    /// this store holds none — the one dispatch behind every keyed read. An
    /// unhashed store's single child covers the whole local dataset, so it can
    /// hold any key; a hashed store's slot is the key's partition, when this
    /// process owns it. An empty `tables` (the post-fork master) holds nothing.
    ///
    /// `key` is the **full** PK, but partition *selection* goes through
    /// `schema.partition_for_pk` (hashing only the distribution prefix, exactly as
    /// ingest does — see its doc) so a probe lands in the partition the row was
    /// routed to. The in-partition match then keys on the full `key`, so
    /// prefix-twins coexist in one partition, distinguished by their full PK.
    fn slot_for_key(&self, key: &[u8]) -> Option<usize> {
        if self.is_unhashed() {
            return (!self.tables.is_empty()).then_some(0);
        }
        self.local_slot(self.schema.partition_for_pk(key))
    }
}

// ---------------------------------------------------------------------------
// PartitionProbe
// ---------------------------------------------------------------------------

/// Per-partition cursors for a multi-key read, opened on first touch and keyed
/// by local slot. A probe reads only the partition its key hashes into, so the
/// merge width is one partition's runs rather than the whole store's; a read
/// that touches one key ever opens exactly one cursor.
///
/// Probing in globally ascending key order keeps each routed cursor's own sweep
/// monotone, so a gather over ascending keys stays (PK, payload)-ascending for
/// free.
///
/// The store is a **per-call** argument rather than a field: a probe outlives
/// its construction call and is driven while `&mut CatalogEngine` is in use
/// between chunks, so it cannot hold a borrow.
pub(crate) struct PartitionProbe {
    slots: Vec<Option<Box<ReadCursor>>>,
}

impl PartitionProbe {
    /// One (unopened) slot per local partition of `store`.
    pub(crate) fn new(store: &PartitionedTable) -> Self {
        PartitionProbe {
            slots: (0..store.tables.len()).map(|_| None).collect(),
        }
    }

    /// The cursor over the partition holding `key` (full OPK bytes), opening it
    /// on first touch — or `None` when this process holds no such partition,
    /// which is exactly "the merged cursor could not have returned `key`".
    pub(crate) fn probe<'s>(&'s mut self, store: &PartitionedTable, key: &[u8]) -> Option<&'s mut ReadCursor> {
        // `slot_for_key` bounds `local` by the store's current child count, and
        // `slots` was sized from that same list, which never grows.
        let local = store.slot_for_key(key)?;
        let slot = &mut self.slots[local];
        Some(slot.get_or_insert_with(|| Box::new(store.tables[local].open_cursor())))
    }

    /// How many partition cursors have been opened so far.
    #[cfg(test)]
    pub(crate) fn open_count(&self) -> usize {
        self.slots.iter().filter(|s| s.is_some()).count()
    }
}

// ---------------------------------------------------------------------------
// Shared test fixture
// ---------------------------------------------------------------------------

/// A reopened [`PartitionedTable`] left in a **partial-flush** LSN state: a
/// 256-way durable table whose partition 0 flushed a durable shard (so it leads)
/// while partition 1 never flushed (so it reopens at the floor, the ENOSPC-on-B
/// case). This is the only shape where `min_flushed_lsn()` (the recovery
/// watermark) and `current_lsn()` (the LSN-allocator max) diverge, so it is the
/// single source for both the storage-side watermark test and the `dag`-side
/// `StoreHandle::Partitioned` dispatch test — the latter could not tell
/// `recovery_lsn` from `current_lsn` on a `min == max` table.
///
/// Lives here (not in `test_support`) because the partial flush reads the private
/// per-partition `tables` field; hoisting it would force a production widening
/// purely to relocate a test.
#[cfg(test)]
pub(crate) struct PartialFlushLsn {
    /// Keeps the on-disk table alive for `pt`'s lifetime.
    _dir: tempfile::TempDir,
    /// The reopened table; `pt.min_flushed_lsn() < pt.current_lsn()`.
    pub pt: PartitionedTable,
    /// The lagging partition's floor — `pt.min_flushed_lsn()` and the value
    /// `StoreHandle::Partitioned::recovery_lsn` must report.
    pub recovery_lsn: u64,
    /// The leading partition's LSN — `pt.current_lsn()` and the value
    /// `StoreHandle::Partitioned::current_lsn` must report.
    pub current_lsn: u64,
}

#[cfg(test)]
pub(crate) fn partial_flush_lsn_fixture() -> PartialFlushLsn {
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    crate::foundation::posix_io::raise_fd_limit_for_tests();

    let schema = || {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    };
    // One-row (pk, weight=1, payload) batch routed by its narrow PK.
    let row = |pk: u64, val: i64| {
        let mut b = Batch::with_capacity(schema(), 1);
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
        b
    };

    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("pt_partial_flush_lsn");
    let path = tdir.to_str().unwrap().to_owned();

    // Two live partitions (0 and 1) of a 256-way durable table.
    let open = || {
        PartitionedTable::new(
            &path,
            schema(),
            830,
            Routing::Hashed { start: 0, end: 2 },
            RecoverySource::SalReplay,
        )
        .unwrap()
    };
    let mut pt = open();

    // PKs that route to live partitions 0 and 1 (narrow PK ⇒ partition_for_key
    // matches the stored-bytes routing).
    let mut p0 = Vec::new();
    let mut p1 = Vec::new();
    for k in 0u64..200_000 {
        match partition_for_key(k as u128) {
            0 if p0.len() < 3 => p0.push(k),
            1 if p1.is_empty() => p1.push(k),
            _ => {}
        }
        if p0.len() == 3 && !p1.is_empty() {
            break;
        }
    }
    assert_eq!((p0.len(), p1.len()), (3, 1), "need PKs routed to partitions 0 and 1");

    // Each durable Table::ingest bumps that partition's current_lsn by 1.
    // Partition A (0) takes three ingests, B (1) one, so A leads.
    for &k in &p0 {
        pt.ingest_owned_batch(row(k, k as i64)).unwrap();
    }
    pt.ingest_owned_batch(row(p1[0], 0)).unwrap();

    let lsn_a = pt.tables[0].current_lsn();
    let lsn_b = pt.tables[1].current_lsn();
    assert!(
        lsn_a > lsn_b,
        "partition A must lead before the partial flush ({lsn_a} vs {lsn_b})"
    );

    // Partial family flush: only partition A commits a durable shard; B's rows
    // stay in its memtable.
    pt.tables[0].flush().unwrap();
    drop(pt);

    // Next boot: reopen both partitions from disk.
    let pt = open();
    let a_reloaded = pt.tables[0].current_lsn();
    let b_reloaded = pt.tables[1].current_lsn();

    // A's flushed shard stamps lsn_max = current_lsn-1; reopen restores +1.
    // B never flushed, so it reopens at the floor, below A.
    assert_eq!(a_reloaded, lsn_a, "flushed partition keeps its LSN across reopen");
    assert!(
        b_reloaded < a_reloaded,
        "unflushed partition B reopens below A ({b_reloaded} vs {a_reloaded})",
    );

    PartialFlushLsn {
        _dir: dir,
        pt,
        recovery_lsn: b_reloaded,
        current_lsn: a_reloaded,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::posix_io::raise_fd_limit_for_tests;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::{make_batch_raw, make_schema_u64_i64, make_wide_batch, opk_pk, wide_pk_3xu64_schema};
    use std::os::fd::AsRawFd;

    /// Unsorted `Raw` rows for the U64+I64 schema.
    fn make_batch(rows: &[(u64, i64, i64)]) -> Batch {
        make_batch_raw(&make_schema_u64_i64(), rows)
    }

    /// The I64 payload (logical column 1) of a positioned cursor's current row.
    fn cursor_val(c: &ReadCursor) -> i64 {
        let ptr = c.col_ptr(1, 8);
        assert!(!ptr.is_null(), "payload column 1 is present on a positioned row");
        i64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr, 8) }.try_into().unwrap())
    }

    #[test]
    fn single_partition_lifecycle() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("sp_test");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            100,
            Routing::Unhashed { rank: 0 },
            RecoverySource::Rederive,
        )
        .unwrap();

        pt.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)]))
            .unwrap();
        assert!(pt.has_pk(10));
        assert!(pt.has_pk(20));
        assert!(!pt.has_pk(99));
        pt.flush().unwrap();
        assert!(pt.has_pk(10));
    }

    /// Every keyed read routes through `slot_for_key`, so a routed cursor must
    /// answer exactly what the merged whole-store cursor answers — over a fully
    /// hashed store, a hashed store trimmed to a sub-range (a key outside it
    /// yields `None`), an **unhashed** store (whose one child holds the whole
    /// local dataset, and which resolving through `local_slot` alone would
    /// silently report as empty), and a store with no children at all.
    #[test]
    fn open_cursor_for_key_matches_the_merged_cursor() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        // Present keys 0..256 (even payload), absent probes 1000..1064.
        let rows: Vec<(u64, i64, i64)> = (0..256u64).map(|k| (k, 1, (k * 10) as i64)).collect();
        let opk = |k: u64| crate::schema::key::opk_key(&schema, &(k as u128).to_le_bytes());
        // Ingest only the rows this store addresses, through `slot_for_key` — the
        // same dispatch the read below uses. A store handed rows it does not
        // address rejects them (`StorageError::MisroutedRows`), so the trimmed
        // slice is built by construction rather than by throwing rows away.
        let build = |name: &str, routing| {
            let tdir = dir.path().join(name);
            let mut pt =
                PartitionedTable::new(tdir.to_str().unwrap(), schema, 300, routing, RecoverySource::Rederive).unwrap();
            let owned: Vec<(u64, i64, i64)> = rows
                .iter()
                .copied()
                .filter(|&(k, _, _)| pt.slot_for_key(opk(k).pk_bytes()).is_some())
                .collect();
            let ingested = owned.len();
            if ingested > 0 {
                pt.ingest_owned_batch(make_batch(&owned)).unwrap();
            }
            (pt, ingested)
        };
        // The whole-store answer for `key`: the live payload, or `None`.
        let whole = |pt: &PartitionedTable, key: &[u8]| -> Option<i64> {
            let mut c = pt.open_cursor();
            c.seek_exact_live(key).then(|| cursor_val(&c))
        };
        let routed = |pt: &PartitionedTable, key: &[u8]| -> Option<i64> {
            let mut c = pt.open_cursor_for_key(key)?;
            c.seek_exact_live(key).then(|| cursor_val(&c))
        };
        let keys: Vec<_> = (0u128..256)
            .chain(1000..1064)
            .map(|k| crate::schema::key::opk_key(&schema, &k.to_le_bytes()))
            .collect();

        let (trim_start, trim_end) = partition_range(1, 4);
        // `want_owned`: how many of the 256 keys this shape addresses, or `None`
        // for the trimmed slice — its share depends on the hash, but it is always
        // a non-empty proper subset.
        for (name, routing, want_owned) in [
            ("ocfk_hashed", Routing::Hashed { start: 0, end: 256 }, Some(256)),
            (
                "ocfk_trimmed",
                Routing::Hashed {
                    start: trim_start,
                    end: trim_end,
                },
                None,
            ),
            ("ocfk_unhashed", Routing::Unhashed { rank: 7 }, Some(256)),
            ("ocfk_empty", Routing::Hashed { start: 0, end: 0 }, Some(0)),
        ] {
            let (pt, ingested) = build(name, routing);
            match want_owned {
                Some(n) => assert_eq!(ingested, n, "{name}: keys addressed"),
                None => assert!(ingested > 0 && ingested < rows.len(), "{name}: partial slice"),
            }
            let mut hits = 0;
            for k in &keys {
                let want = whole(&pt, k.pk_bytes());
                assert_eq!(routed(&pt, k.pk_bytes()), want, "{name}: key {:?}", k.pk_bytes());
                hits += usize::from(want.is_some());
            }
            assert_eq!(hits, ingested, "{name}: every key this store addresses reads back");
        }
    }

    /// `estimated_rows` counts the raw rows ingested across the local
    /// partitions with all three tiers live at once (memtable runs, RAM tier,
    /// shards). Asserted against the ingest count, NOT against
    /// `open_cursor().estimated_length()`, which is one group lower (the cursor
    /// drives once at open).
    #[test]
    fn estimated_rows_counts_every_tier() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("est_rows");
        let schema = make_schema_u64_i64();
        // One child, so all three tiers stack on the same `Table` and the sum is
        // the whole ingest count.
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            860,
            Routing::Unhashed { rank: 0 },
            RecoverySource::Rederive,
        )
        .unwrap();
        let run = |lo: u64, hi: u64| -> Vec<(u64, i64, i64)> { (lo..hi).map(|k| (k, 1, k as i64)).collect() };

        // Tier 1 — shards: a flush past the (shrunk) RAM ceiling spills to disk.
        pt.partitions_mut()[0].set_inmem_ceiling_for_test(100);
        let shard_rows = run(0, 64);
        pt.ingest_owned_batch(make_batch(&shard_rows)).unwrap();
        pt.flush().unwrap();
        assert!(!pt.tables[0].all_shard_arcs().is_empty(), "the ceiling breach spilled");

        // Tier 2 — RAM tier: at the real ceiling a flush parks its run in heap.
        pt.partitions_mut()[0].set_inmem_ceiling_for_test(1 << 20);
        let inmem_rows = run(100, 164);
        pt.ingest_owned_batch(make_batch(&inmem_rows)).unwrap();
        pt.flush().unwrap();
        assert!(pt.tables[0].in_memory_runs().count() > 0, "the flush parked a RAM run");

        // Tier 3 — memtable runs.
        let mem_rows = run(200, 264);
        pt.ingest_owned_batch(make_batch(&mem_rows)).unwrap();

        let total = shard_rows.len() + inmem_rows.len() + mem_rows.len();
        assert_eq!(
            pt.estimated_rows(),
            total,
            "every tier counted once ({total} distinct PKs ingested)"
        );
        assert_eq!(
            pt.open_cursor().estimated_length(),
            total - 1,
            "the cursor's figure is one group lower — the gate must not read it"
        );
    }

    /// A `PartitionProbe` returns the same rows as the whole-store cursor for
    /// the same key sequence — including one that revisits a partition after
    /// leaving it and one that probes backward — and opens exactly the
    /// partitions its keys reach.
    #[test]
    fn partition_probe_matches_the_merged_cursor_and_opens_only_what_it_touches() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let rows: Vec<(u64, i64, i64)> = (0..256u64).map(|k| (k, 1, (k * 10) as i64)).collect();
        let opk = |k: u64| crate::schema::key::opk_key(&schema, &(k as u128).to_le_bytes());
        let build = |name: &str, routing| {
            let tdir = dir.path().join(name);
            let mut pt =
                PartitionedTable::new(tdir.to_str().unwrap(), schema, 870, routing, RecoverySource::Rederive).unwrap();
            pt.ingest_owned_batch(make_batch(&rows)).unwrap();
            pt
        };
        let pt = build("probe_hashed", Routing::Hashed { start: 0, end: 256 });
        // Ascending, then a revisit-after-leaving and a backward sweep.
        let sequences: [Vec<u64>; 3] = [(0..64).collect(), vec![0, 1, 200, 2, 201, 3], (0..64).rev().collect()];
        for seq in &sequences {
            let mut probe = PartitionProbe::new(&pt);
            let mut whole = pt.open_cursor();
            for &k in seq {
                let key = opk(k);
                let want = whole
                    .seek_exact_live(key.pk_bytes())
                    .then(|| cursor_val(&whole))
                    .expect("every probed key was ingested");
                let c = probe.probe(&pt, key.pk_bytes()).expect("all 256 partitions are local");
                assert!(c.advance_to_exact_live(key.pk_bytes()), "routed cursor finds pk {k}");
                assert_eq!(cursor_val(c), want, "pk {k}");
            }
        }

        // One key → one cursor; k keys spanning k partitions → k cursors.
        let mut probe = PartitionProbe::new(&pt);
        probe.probe(&pt, opk(7).pk_bytes()).unwrap();
        assert_eq!(probe.open_count(), 1, "a single key opens a single partition");
        let mut parts: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut probe = PartitionProbe::new(&pt);
        for k in 0..32u64 {
            let key = opk(k);
            parts.insert(schema.partition_for_pk(key.pk_bytes()));
            probe.probe(&pt, key.pk_bytes()).unwrap();
        }
        assert_eq!(probe.open_count(), parts.len(), "one cursor per distinct partition");

        // A store owning nothing resolves every key to `None` and opens nothing.
        let empty = build("probe_empty", Routing::Hashed { start: 0, end: 0 });
        let mut probe = PartitionProbe::new(&empty);
        for k in 0..32u64 {
            assert!(probe.probe(&empty, opk(k).pk_bytes()).is_none());
        }
        assert_eq!(probe.open_count(), 0);
    }

    #[test]
    fn multi_partition_hash_routing() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("mp_test");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            200,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        let rows: Vec<(u64, i64, i64)> = (0..100).map(|i| (i * 7 + 13, 1, (i * 100) as i64)).collect();
        pt.ingest_owned_batch(make_batch(&rows)).unwrap();

        for &(pk, _, _) in &rows {
            assert!(pt.has_pk(pk as u128), "PK {pk} not found");
        }
        assert!(!pt.has_pk(999999));
    }

    #[test]
    fn multi_partition_cursor() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("mc_test");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            300,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        pt.ingest_owned_batch(make_batch(&[(30, 1, 300), (10, 1, 100), (20, 1, 200), (40, 1, 400)]))
            .unwrap();

        let cursor = pt.open_cursor();
        assert!(cursor.valid);
        assert_eq!(cursor.current_key_narrow() as u64, 10);
    }

    #[test]
    fn retract_pk_routing() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("rt_test");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            400,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        pt.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)]))
            .unwrap();

        let (w, found) = pt.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found.is_some());

        let (_, found) = pt.retract_pk(99);
        assert!(found.is_none());
    }

    /// §4.4 regression: a `CLUSTER BY prefix` table with a compound PK. Two rows
    /// that share the distribution prefix but differ in the PK suffix
    /// ("prefix twins") must co-locate, and each must be independently findable
    /// and retractable. The probe (`slot_for_key`) slices to the
    /// distribution prefix exactly as ingest does — without that slice the
    /// retraction probe would land in the full-key partition and miss the row
    /// (UPSERT would duplicate the PK, DELETE would be dropped). The chosen twins
    /// route their full PK to a *different* partition than their prefix, so the
    /// slice is observably exercised.
    #[test]
    fn prefix_distribution_routes_twins_together_and_retracts() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("prefix_twins");
        // 2×U64 compound PK, CLUSTER BY col0 (k=1, dist_stride=8) + I64 payload.
        let schema = SchemaDescriptor::new_with_placement(
            &[
                SchemaColumn::new(type_code::U64, 0), // col 0 (distribution key)
                SchemaColumn::new(type_code::U64, 0), // col 1
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0, 1],
            crate::schema::Placement::Keyed { prefix_len: 1 },
        );
        assert_eq!(schema.dist_stride(), 8, "CLUSTER BY one U64 column ⇒ 8-byte prefix");
        // All 256 partitions live so any prefix-routed partition exists locally.
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            900,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        // OPK bytes for (col0, col1): each column big-endian, tightly packed.
        let pk_bytes = |c0: u64, c1: u64| {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&c0.to_be_bytes());
            b[8..].copy_from_slice(&c1.to_be_bytes());
            b
        };

        // Find a c0 whose 8-byte prefix routes differently from both full 16-byte
        // twin keys, so the probe's prefix slice is observably load-bearing.
        let (c0, twin_a, twin_b) = (1u64..5000)
            .find_map(|c0| {
                let pp = partition_for_pk_bytes(&c0.to_be_bytes());
                let (a, b) = (pk_bytes(c0, 1), pk_bytes(c0, 2));
                (pp != partition_for_pk_bytes(&a) && pp != partition_for_pk_bytes(&b)).then_some((c0, a, b))
            })
            .expect("a prefix routing differently from its full keys exists");
        // Both twins share the distribution prefix ⇒ same prefix partition.
        assert_eq!(
            partition_for_pk_bytes(&twin_a[..8]),
            partition_for_pk_bytes(&twin_b[..8]),
            "prefix twins co-partition on col0",
        );

        let mut batch = Batch::with_capacity(schema, 2);
        for (pk, val) in [(twin_a, 100i64), (twin_b, 200)] {
            batch.extend_pk_bytes(&pk);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(0, &val.to_le_bytes());
            batch.count += 1;
        }
        pt.ingest_owned_batch(batch).unwrap();

        // Both twins are found via the prefix-sliced probe; an absent suffix in
        // the same (populated) prefix-partition is not — the in-partition match
        // is on the full PK, not the prefix.
        assert!(pt.has_pk_bytes(&twin_a), "twin A found");
        assert!(pt.has_pk_bytes(&twin_b), "twin B found");
        assert!(!pt.has_pk_bytes(&pk_bytes(c0, 999)), "absent suffix not found");

        // `retract_pk_bytes` is the uniqueness/retraction *lookup* (enforce_unique_pk
        // writes the actual -1 from the found row). It must reach the prefix-
        // partition the row lives in and surface the matching twin's weight and
        // payload — not the other twin's. Under the §4.4 bug (probe not sliced to
        // the prefix) it would route by the full key to a different partition and
        // miss the row entirely.
        let read_found_val = |fr: &table::RowRef| {
            i64::from_le_bytes(gnitz_expr::RowSource::get_col_ptr(fr, 0, 0, 8).try_into().unwrap())
        };
        let (wa, fa) = pt.retract_pk_bytes(&twin_a);
        assert_eq!(wa, 1, "twin A found in its prefix-partition");
        let fa = fa.expect("found-row payload available");
        assert_eq!(read_found_val(&fa), 100, "found the correct twin (A), not B");

        let (wb, fb) = pt.retract_pk_bytes(&twin_b);
        assert_eq!(wb, 1, "twin B found in the same prefix-partition");
        let fb = fb.expect("found-row payload available");
        assert_eq!(read_found_val(&fb), 200, "twin B's payload is distinct from A's");

        // An absent suffix routing to the same populated prefix-partition is not
        // found — confirms the in-partition match keys on the full PK.
        let (_, found_absent) = pt.retract_pk_bytes(&pk_bytes(c0, 999));
        assert!(
            found_absent.is_none(),
            "absent suffix in a populated prefix-partition is not found"
        );
    }

    #[test]
    fn partition_for_pk_bytes_narrow_invariance() {
        // For every narrow width, routing the physically-stored OPK
        // (big-endian) bytes must equal partition_for_key of the same value.
        let vals: [u128; 9] = [
            0,
            1,
            7,
            42,
            255,
            65537,
            0x0123_4567_89ab_cdef,
            u64::MAX as u128,
            u128::MAX,
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

    /// A 24-byte compound PK routes and ingests end-to-end: the scatter loop must
    /// hash the OPK bytes (`get_pk` would panic past `pk_stride > 16`) and every
    /// row must reach its partition's `Table::ingest`. All 256 partitions are
    /// live, so `scatter_ingest` addresses every row and the ingest succeeds.
    #[test]
    fn ingest_owned_batch_wide_routing() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("wide_test");
        let schema = wide_pk_3xu64_schema();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            700,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        // `c0` is strictly increasing, so the rows are OPK-sorted as
        // `make_wide_batch` requires.
        let rows: Vec<(u64, u64, u64, i64, i64)> = (0..200u64)
            .map(|k| (k * 7 + 13, k * 31 + 5, k + 1, 1, k as i64))
            .collect();
        let pks: Vec<Vec<u8>> = rows
            .iter()
            .map(|&(c0, c1, c2, _, _)| opk_pk(&schema, &[c0 as u128, c1 as u128, c2 as u128]))
            .collect();
        // The keys spread over many partitions, so the routing loop is the thing
        // under test rather than one bucket's worth of it.
        let distinct = pks
            .iter()
            .map(|pk| partition_for_pk_bytes(pk))
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(distinct > 64, "only {distinct} distinct partitions across 200 keys");

        pt.ingest_owned_batch(make_wide_batch(&schema, &rows)).unwrap();
        for pk in &pks {
            let mut c = pt.open_cursor_for_key(pk).expect("every key routes to a live child");
            assert!(c.seek_exact_live(pk), "wide key {pk:?} must survive the ingest");
        }
    }

    #[test]
    fn close_partitions_outside() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cpo_test");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            500,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        assert_eq!(pt.tables.len(), 256);
        pt.close_partitions_outside(100, 110);
        assert_eq!(pt.tables.len(), 10);
        assert_eq!(pt.routing, Routing::Hashed { start: 100, end: 110 });
    }

    // ── In-memory ephemeral flush across partitions ──────────────────────

    /// Recursively count files under `root` whose name satisfies `pred`.
    fn count_tree(root: &std::path::Path, pred: impl Fn(&str) -> bool + Copy) -> usize {
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(root) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    n += count_tree(&path, pred);
                } else if let Some(name) = e.file_name().to_str() {
                    if pred(name) {
                        n += 1;
                    }
                }
            }
        }
        n
    }

    /// Per-partition two-phase prepare, as the worker's unified flush loop
    /// drives it: `Table::flush_prepare` on every partition, collecting the
    /// `Pending` works with their partition index.
    fn prepare_all(pt: &mut PartitionedTable) -> Vec<(usize, FlushWork)> {
        let mut works = Vec::new();
        for (i, t) in pt.partitions_mut().iter_mut().enumerate() {
            match t.flush_prepare().unwrap() {
                FlushOutcome::Done => {}
                FlushOutcome::Pending(w) => works.push((i, w)),
            }
        }
        works
    }

    /// A durable `PartitionedTable` still routes flushes through the durable
    /// branch: `flush_prepare` returns `Pending` work, writes its folded shard at
    /// its final name, and stages a manifest `.tmp`. Guards against the
    /// durability fix over-broadening to base tables.
    #[test]
    fn flush_prepare_durable_returns_pending() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("pt_durable_flush");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            800,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::SalReplay,
        )
        .unwrap();

        pt.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]))
            .unwrap();
        let works = prepare_all(&mut pt);
        assert!(!works.is_empty(), "durable table must return Pending work");
        assert!(
            count_tree(&tdir, |n| n == "manifest.bin.tmp") > 0,
            "durable flush_prepare must stage a manifest .tmp",
        );
        // Dropping the work unlinks the manifest .tmp (PreparedManifest::drop);
        // prepare already wrote the folded shard at its final name and registered
        // it in the index, so the rows stay readable and no .tmp survives.
        drop(works);
        assert!(
            count_tree(&tdir, |n| n.ends_with(".tmp")) == 0,
            "drop must unlink the staged manifest .tmp",
        );
        assert!(pt.has_pk(10) && pt.has_pk(20) && pt.has_pk(30));
    }

    /// A non-durable `PartitionedTable` flushes every child in-memory:
    /// `flush_prepare` returns no work, writes no shard/manifest file, and the
    /// rows stay readable.
    #[test]
    fn flush_prepare_nondurable_no_work_no_files() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("pt_nondurable_flush");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            810,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        pt.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)]))
            .unwrap();
        let works = prepare_all(&mut pt);
        assert!(works.is_empty(), "Rederive table must return no Pending work");
        assert_eq!(
            count_tree(&tdir, |n| n.starts_with("shard_")),
            0,
            "Rederive checkpoint flush must write no shard files",
        );
        assert_eq!(count_tree(&tdir, |n| n == "manifest.bin"), 0);
        for pk in [10u128, 20, 30, 40] {
            assert!(pt.has_pk(pk), "pk {pk} must be readable after in-memory flush");
        }
    }

    /// The compact-then-open read path (operator-state / `ScanTrace`) must
    /// surface each partition's `in_memory_l0`, not just `open_cursor`.
    /// Fails pre-fix (multi-partition branch dropped `in_memory_runs`).
    #[test]
    fn compacted_open_cursor_gathers_in_memory_runs() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("pt_ccc_inmem");
        let schema = make_schema_u64_i64();
        let mut pt = PartitionedTable::new(
            tdir.to_str().unwrap(),
            schema,
            820,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::Rederive,
        )
        .unwrap();

        let rows: Vec<(u64, i64, i64)> = (0..50).map(|i| (i * 7 + 3, 1, (i * 10) as i64)).collect();
        pt.ingest_owned_batch(make_batch(&rows)).unwrap();

        // Flush all partitions into in_memory_l0; memtables now empty.
        assert!(prepare_all(&mut pt).is_empty());

        pt.compact_if_needed().unwrap();
        let batch = pt.open_cursor().materialize();
        let mut seen = std::collections::HashSet::new();
        for i in 0..batch.count {
            assert_eq!(batch.get_weight(i), 1);
            seen.insert(batch.get_pk(i) as u64);
        }
        for &(pk, _, _) in &rows {
            assert!(seen.contains(&pk), "pk {pk} missing from the compacted cursor");
        }
        assert_eq!(seen.len(), rows.len(), "all rows surfaced via compacting cursor");
    }

    /// Recovery watermark after a *partial* family flush. `flush` commits
    /// partitions sequentially, so ENOSPC/EIO on a later partition leaves an
    /// earlier one's manifest advanced while the lagging partition's rows stay
    /// only in the SAL. The recovery dedupe filter must read the **min**
    /// `current_lsn` (`min_flushed_lsn`), not the max (`current_lsn`): skipping
    /// by the max over-skips committed SAL zones the lagging partition never
    /// flushed, silently dropping its rows on the next boot. Red before the
    /// fix — `min_flushed_lsn` did not exist and recovery used the max.
    #[test]
    fn min_flushed_lsn_floors_recovery_watermark_after_partial_flush() {
        let f = partial_flush_lsn_fixture();
        // The fix: recovery aggregates with min, so the lagging partition's
        // still-in-SAL zones replay. The max (current_lsn) would skip them and
        // drop its rows. (The StoreHandle dispatch over this same min-vs-max
        // split is pinned by query::dag::tests::store_handle_partitioned_lsn_dispatch.)
        assert_eq!(
            f.pt.min_flushed_lsn(),
            f.recovery_lsn,
            "recovery watermark is the lagging partition's floor",
        );
        assert!(f.pt.min_flushed_lsn() < f.current_lsn);
        assert_eq!(
            f.pt.current_lsn(),
            f.current_lsn,
            "current_lsn still reports the max for the allocator",
        );
    }

    // ── Barrier-flush directory-fd hygiene ───────────────────────────────

    /// Count this process's open fds that point at a partition directory
    /// (`.../part_N`) under `root`. At rest the engine holds zero — dir fds are
    /// opened per flush and released on commit — and shard/manifest *file* fds
    /// inside a partition dir have a `shard_`/`manifest` basename, not `part_`,
    /// so they are excluded. Scoped to `root` (each test's unique tempdir)
    /// because `/proc/self/fd` is process-global and cargo runs tests in
    /// parallel threads: without the prefix filter a sibling test's transient
    /// dir fds would be counted here. This is thus a precise, isolated probe of
    /// the dir-fd leak, independent of the data fds a flush legitimately opens.
    fn count_partition_dir_fds(root: &std::path::Path) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .flatten()
            .filter_map(|e| std::fs::read_link(e.path()).ok())
            .filter(|target| {
                target.starts_with(root)
                    && target
                        .file_name()
                        .and_then(|s| s.to_str())
                        .is_some_and(|n| n.starts_with("part_"))
            })
            .count()
    }

    fn durable_hashed(dir: &std::path::Path, name: &str, table_id: u32) -> PartitionedTable {
        PartitionedTable::new(
            dir.join(name).to_str().unwrap(),
            make_schema_u64_i64(),
            table_id,
            Routing::Hashed { start: 0, end: 256 },
            RecoverySource::SalReplay,
        )
        .unwrap()
    }

    /// A partial family commit leaks no directory fd: each committed
    /// partition's dir fd (opened in `flush_commit` for the final fsync) is an
    /// `OwnedFd` released by `Drop`, and the un-committed `works` tail drops
    /// its staged manifest `.tmp`s (`PreparedManifest`'s Drop, no fds). This is
    /// the RAII property the worker's flush loop relies on when a mid-batch
    /// `flush_commit` error unwinds its pending vec.
    #[test]
    fn partial_commit_leaks_no_dirfd() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let mut pt = durable_hashed(dir.path(), "pt_fd_hygiene", 840);

        // Enough distinct PKs that ≥2 partitions receive data, so a partial
        // commit (some works committed, the rest dropped) is reachable.
        let rows: Vec<(u64, i64, i64)> = (0..64).map(|i| (i, 1, i as i64)).collect();
        pt.ingest_owned_batch(make_batch(&rows)).unwrap();

        let root = dir.path();
        assert_eq!(count_partition_dir_fds(root), 0, "no dir fds held at rest");

        let mut works = prepare_all(&mut pt);
        let n = works.len();
        assert!(
            n >= 2,
            "need ≥2 partitions with data to exercise a partial commit (got {n})"
        );
        // flush_prepare writes each shard at its final name and drops the dir fd
        // it opened for the write — no dir fd survives into the FlushWork.
        assert_eq!(
            count_partition_dir_fds(root),
            0,
            "prepare holds no dir fds (shard written at final name)"
        );

        // Commit only the first partition's work, then drop everything — the
        // shape of the worker's error unwind after a mid-batch failure.
        let (idx, w) = works.remove(0);
        let fd = pt.partitions_mut()[idx].flush_commit(w).unwrap();
        drop(fd);
        drop(works);

        assert_eq!(
            count_partition_dir_fds(root),
            0,
            "partial commit leaks no dir fd (n_works={n})",
        );
        assert_eq!(
            count_tree(&dir.path().join("pt_fd_hygiene"), |name| name.ends_with(".tmp")),
            0,
            "dropped works unlink their staged manifest .tmps",
        );
    }

    /// Success-path regression: repeated checkpoints over a partitioned table
    /// leave the partition-dir-fd count flat at zero. The worker fsyncs each
    /// commit's `OwnedFd` then drops it; modeled here by the fsync + drop.
    #[test]
    fn repeated_flush_commit_leaves_dirfds_flat() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut pt = durable_hashed(root, "pt_fd_flat", 850);

        for round in 0..8u64 {
            let rows: Vec<(u64, i64, i64)> = (0..16).map(|i| (round * 100 + i, 1, i as i64)).collect();
            pt.ingest_owned_batch(make_batch(&rows)).unwrap();
            for (idx, w) in prepare_all(&mut pt) {
                let fd = pt.partitions_mut()[idx].flush_commit(w).unwrap();
                let _ = crate::foundation::posix_io::fsync_eintr(fd.as_raw_fd());
                // fd closes here, as it does in the worker sweep.
            }
            assert_eq!(
                count_partition_dir_fds(root),
                0,
                "round {round}: dir fds released after commit"
            );
        }
    }
}
