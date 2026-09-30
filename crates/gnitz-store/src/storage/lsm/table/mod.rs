//! Unified Table: two RAM-tier [`RunSet`]s over a `ShardIndex`.
//!
//! Ingest lands in the `memtable` run set and folds into the `ram_tier` once it
//! passes its byte budget; the RAM tier spills to a shard past its own
//! ([`StoreBudgets`]), and a checkpoint folds it into one durable shard.

use std::cell::Cell;
use std::rc::Rc;

use gnitz_foundation::posix_io::create_dir;
use gnitz_wire::PkKeys;

use super::manifest::Manifest;
use super::read_cursor::{self, PkSetGather, ReadCursor};
use super::run::{Run, StoredRow};
use super::run_set::{RunSet, TrimmedRun};
use super::shard_index::{ShardBudget, ShardIndex};
use crate::schema::key::{probe_key, PkBuf};
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
use crate::storage::error::StorageError;
use crate::storage::repr::batch::Batch;
use crate::storage::repr::merge::ColumnarSource;
use crate::storage::repr::seek::pk_group_end;
#[cfg(test)]
use crate::storage::repr::shard_reader::MappedShard;

/// Ingest runs fold into the RAM tier once they pass this.
const MEMTABLE_BYTES: usize = 192 << 10;

/// The RAM-tier ceiling every store opens with: it bounds that one tier, not the
/// table's heap. The ceiling trades spill writes against RSS; shrinking it is how
/// a test reaches the disk regime on small data.
pub(crate) const DEFAULT_RAM_TIER_BYTES: usize = 32 << 20;

/// What one `Table` opens with: its RAM-tier ceiling, and what bounds its
/// registered on-disk shard bytes.
#[derive(Clone, Copy)]
pub(crate) struct StoreBudgets {
    ram_tier_bytes: usize,
    shard: ShardBudget,
}

impl Default for StoreBudgets {
    fn default() -> Self {
        StoreBudgets::new(DEFAULT_RAM_TIER_BYTES)
    }
}

impl StoreBudgets {
    /// An unbounded store at `ram_tier_bytes`.
    pub(crate) fn new(ram_tier_bytes: usize) -> Self {
        StoreBudgets {
            ram_tier_bytes,
            shard: ShardBudget::Unbounded,
        }
    }

    /// `CREATE VIEW … WITH (capacity = …)` — see [`ShardBudget::Dehydrate`].
    /// `None` is an unbounded view.
    pub(crate) fn bounded(self, capacity_bytes: Option<u64>) -> Self {
        StoreBudgets {
            shard: capacity_bytes.map_or(ShardBudget::Unbounded, ShardBudget::Dehydrate),
            ..self
        }
    }

    /// A fed view's **delta store** — see [`ShardBudget::Drop`].
    pub(crate) fn delta(self, budget: u64) -> Self {
        StoreBudgets { shard: ShardBudget::Drop(budget), ..self }
    }
}

// ---------------------------------------------------------------------------
// RecoverySource
// ---------------------------------------------------------------------------

/// How a relation's tail is recovered across a restart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RecoverySource {
    /// The tail is recovered by replaying the fsynced SAL over the shards loaded
    /// from the manifest at open.
    SalReplay,
    /// Derived state, persisted at each checkpoint under a generation-stamped
    /// manifest.
    Rederive {
        /// The generation a manifest must carry for the open to resume from it
        /// instead of erasing it; `None` erases it.
        resume_at: Option<u64>,
    },
}

/// Index of the first candidate in `pool` whose payload group nets strictly positive.
fn first_live_payload_group<P: PayloadOrder>(
    schema: &SchemaDescriptor,
    pool: &[StoredRow],
    payload: P,
) -> Option<usize> {
    (0..pool.len()).find(|&i| {
        let (run, row) = (&pool[i].run, pool[i].row);
        let others: i64 = pool
            .iter()
            .enumerate()
            .filter(|&(j, c)| j != i && payload.compare(schema, run, row, &c.run, c.row).is_eq())
            .map(|(_, c)| c.weight())
            .sum();
        pool[i].weight() + others > 0
    })
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

pub(crate) struct Table {
    /// Ingest runs, folded into `ram_tier` once they pass its byte budget.
    memtable: RunSet,
    /// Folded memtable runs, spilled to a shard past the RAM-tier ceiling.
    ram_tier: RunSet,
    /// The disk tier, and the one owner of this store's schema and directory.
    shard_index: ShardIndex,

    recovery_source: RecoverySource,

    /// The checkpoint mark of the manifest this open loaded; 0 without one.
    checkpoint_mark: u64,

    /// What the next publish writes as the manifest's caller record.
    caller_record: Vec<u8>,

    /// Set by [`Self::hold_in_ram`]: the RAM tier grows past its budget and never
    /// persists.
    held_in_ram: bool,

    /// `live_row_at`'s candidate pool, kept between calls for its capacity.
    live_row_scratch: Cell<Vec<StoredRow>>,

    /// Last `full_scan` result, held until the row set moves.
    cached_full_scan: Cell<Option<Rc<Batch>>>,

    /// The manifest bytes this process's last barrier made durable here.
    durable_manifest: Option<Vec<u8>>,
}

mod flush;

pub(crate) use flush::flush_barrier;

#[cfg(test)]
mod bench_flush;

#[cfg(test)]
mod bench_ingest;

impl Table {
    /// Open a table at `dir`. `recovery_source` decides what this does with
    /// whatever is already on disk; `budgets` binds for the store's whole life.
    pub(crate) fn new(
        dir: &str,
        schema: SchemaDescriptor,
        recovery_source: RecoverySource,
        budgets: StoreBudgets,
    ) -> Result<Self, StorageError> {
        // First, so an unusable directory fails the open rather than the first
        // flush.
        let created = create_dir(dir)?;

        let rederived = matches!(recovery_source, RecoverySource::Rederive { .. });
        let loaded = match recovery_source {
            _ if created => None,
            // Erased at open, so it reads nothing: an I/O error on the file it is
            // about to unlink must not abort it.
            RecoverySource::Rederive { resume_at: None } => None,
            // Rebuilt from its sources, so damage is erased rather than fatal.
            RecoverySource::Rederive { resume_at: Some(want) } => super::manifest::read_at(dir, want)?,
            // Its shards are its only copy, so damage stops the open.
            RecoverySource::SalReplay => super::manifest::read(dir)?,
        };
        if loaded.is_none() && rederived && !created {
            // So a later re-open cannot reload what this one rejected.
            super::manifest::unlink(dir)?;
        }
        let recovery_source = match recovery_source {
            RecoverySource::Rederive { resume_at } => RecoverySource::Rederive {
                resume_at: resume_at.filter(|_| loaded.is_some()),
            },
            s => s,
        };
        let (checkpoint_mark, caller_record, shards) = match loaded {
            Some(Manifest { checkpoint_mark, caller_record, shards }) => (checkpoint_mark, caller_record, Some(shards)),
            None => (0, Vec::new(), None),
        };
        // Only a `SalReplay` store is point-probed by PK.
        let skip_pk_filter = rederived;
        Ok(Table {
            memtable: RunSet::new(MEMTABLE_BYTES),
            ram_tier: RunSet::new(budgets.ram_tier_bytes),
            shard_index: ShardIndex::open(dir, schema, budgets.shard, skip_pk_filter, shards.as_ref())?,
            recovery_source,
            checkpoint_mark,
            caller_record,
            held_in_ram: false,
            live_row_scratch: Cell::new(Vec::new()),
            cached_full_scan: Cell::new(None),
            durable_manifest: None,
        })
    }

    /// The schema this store's rows are read in.
    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.shard_index.schema
    }

    /// The highest key this store's capacity sweep has dropped.
    /// See [`ShardIndex::dropped_max`].
    pub(crate) fn dropped_max(&self) -> PkBuf {
        self.shard_index.dropped_max()
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// See [`ShardIndex::has_skeleton_shard`].
    pub(crate) fn has_skeleton_rows(&self) -> bool {
        self.shard_index.has_skeleton_shard()
    }

    /// True when this store is rebuilt from its sources at open.
    pub(crate) fn is_rederived(&self) -> bool {
        matches!(self.recovery_source, RecoverySource::Rederive { .. })
    }

    /// The policy this open's on-disk state was accepted under.
    pub(crate) fn recovery_source(&self) -> RecoverySource {
        self.recovery_source
    }

    /// Whether this open reloaded checkpointed state rather than starting empty.
    pub(crate) fn resumed_from_checkpoint(&self) -> bool {
        matches!(self.recovery_source, RecoverySource::Rederive { resume_at: Some(_) })
    }

    /// See [`ShardIndex::append_terminal_run`].
    pub(crate) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        self.cached_full_scan.set(None);
        self.shard_index.append_terminal_run(run)
    }

    /// Verify every shard's body.
    pub(crate) fn verify_shards(&self) -> Result<(), StorageError> {
        self.shard_index.verify_shards()
    }

    /// Publish `schema` across this store (any column ALTER). All-or-nothing:
    /// only the shard index's swap can fail, and it runs first.
    pub(crate) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        self.shard_index.swap_schema(schema)?;
        self.memtable.widen_runs(&schema);
        self.ram_tier.widen_runs(&schema);
        self.cached_full_scan.set(None);
        Ok(())
    }

    /// Never spill again: a process that holds this store as a read replica of
    /// another process's directory must not write into it.
    pub(crate) fn hold_in_ram(&mut self) {
        self.held_in_ram = true;
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch into the memtable.
    /// The relation-store ingest entry point.
    ///
    /// `#[inline]`: it takes a `Batch` by value, and every stateful operator
    /// calls it once per epoch.
    #[inline]
    pub(crate) fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.push_memtable(batch.into_consolidated())
    }

    /// [`Self::ingest_owned_batch`] for a caller that keeps reading `batch`.
    pub(crate) fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        self.push_memtable(batch.to_consolidated())
    }

    /// The tail both entry points share, taking a batch already certified
    /// `Consolidated`.
    fn push_memtable(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        self.cached_full_scan.set(None);
        self.memtable.push(TrimmedRun::new(batch), &self.shard_index.schema);
        if self.memtable.is_full() {
            self.fold_to_ram()?;
        }
        Ok(())
    }

    /// The checkpoint mark of the manifest this open loaded; 0 without one.
    pub(crate) fn checkpoint_mark(&self) -> u64 {
        self.checkpoint_mark
    }

    /// The bytes this store's next published manifest carries for its owner.
    pub(crate) fn set_caller_record(&mut self, record: Vec<u8>) {
        self.caller_record = record;
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// The heap-resident tiers, newest first.
    fn ram_tiers(&self) -> [&RunSet; 2] {
        [&self.memtable, &self.ram_tier]
    }

    /// The heap-resident runs whose PK extent meets the inclusive `bound`; `None`
    /// takes them all.
    fn mem_runs(&self, bound: Option<(PkBuf, PkBuf)>) -> impl Iterator<Item = Run> + '_ {
        self.ram_tiers()
            .into_iter()
            .flat_map(move |set| set.runs_overlapping(bound))
            .cloned()
            .map(Run::Mem)
    }

    /// How many cursor sources [`Self::mem_runs`] can yield.
    fn mem_run_count(&self) -> usize {
        self.ram_tiers().iter().map(|s| s.len()).sum()
    }

    /// Every run this table reads through.
    pub(crate) fn runs(&self) -> impl Iterator<Item = Run> + '_ {
        self.mem_runs(None)
            .chain(self.shard_index.all_shard_arcs_iter().map(Run::Shard))
    }

    /// Open a read-only cursor over every tier.
    pub(crate) fn open_cursor(&self) -> ReadCursor {
        let cap = self.mem_run_count() + self.shard_index.shard_count();
        read_cursor::from_runs(self.runs(), self.shard_index.schema, cap)
    }

    /// A cursor over the keys in `[first, last]`, positioned on the first live row
    /// `>= first`.
    pub(crate) fn open_cursor_in_range(&self, first: &[u8], last: &[u8]) -> ReadCursor {
        let (runs, cap) = self.runs_in_range(first, Some(last));
        read_cursor::from_runs_at(runs, self.shard_index.schema, cap, first)
    }

    /// The runs that can hold a key in `[start, end]` (`None`: the top of the key
    /// space), and a capacity hint for a cursor over them.
    fn runs_in_range<'a>(&'a self, start: &[u8], end: Option<&[u8]>) -> (impl Iterator<Item = Run> + 'a, usize) {
        let stride = self.shard_index.schema.pk_stride();
        debug_assert_eq!(start.len(), stride, "runs_in_range: start is not pk_stride wide");
        debug_assert!(
            end.is_none_or(|e| e.len() == stride),
            "runs_in_range: end is not pk_stride wide",
        );
        let (lo, hi) = (
            PkBuf::from_bytes(start),
            end.map_or_else(|| PkBuf::max(stride), PkBuf::from_bytes),
        );
        let runs = self
            .mem_runs(Some((lo, hi)))
            .chain(self.shard_index.shard_arcs_in_range(lo, hi).map(Run::Shard));
        (runs, self.mem_run_count() + self.shard_index.narrow_range_shards())
    }

    /// A cursor positioned on the OPK key band `[start, end)`; `None` opens empty.
    pub(crate) fn range_cursor(&self, range: Option<(PkBuf, Option<PkBuf>)>) -> ReadCursor {
        let Some((start, end)) = range else {
            return read_cursor::empty_cursor(self.shard_index.schema);
        };
        let end = end.as_ref().map(PkBuf::pk_bytes);
        let (runs, cap) = self.runs_in_range(start.pk_bytes(), end);
        read_cursor::from_runs_in_band(runs, self.shard_index.schema, cap, start.pk_bytes(), end)
    }

    /// Every live row of `keys`, over the runs the span they cover can reach.
    pub(crate) fn gather(&self, keys: PkKeys, extra: Option<Rc<Batch>>) -> PkSetGather {
        let schema = self.shard_index.schema;
        let Some((first, last)) = keys.bounds() else {
            return PkSetGather::over_runs(std::iter::empty(), schema, 0, keys);
        };
        let (runs, cap) = self.runs_in_range(first, Some(last));
        PkSetGather::over_runs(runs.chain(extra.map(Run::Mem)), schema, cap + 1, keys)
    }

    /// The consolidated batch of all live rows, cached until the row set moves.
    pub(crate) fn full_scan(&self) -> Rc<Batch> {
        let rc = self
            .cached_full_scan
            .take()
            .unwrap_or_else(|| self.open_cursor().materialize());
        self.cached_full_scan.set(Some(Rc::clone(&rc)));
        rc
    }

    /// Raw rows across every tier, cross-run duplicates and ghosts included.
    pub(crate) fn estimated_rows(&self) -> usize {
        self.ram_tiers().iter().map(|s| s.row_count()).sum::<usize>() + self.shard_index.total_rows()
    }

    /// `(registered shards, how many carry a PK filter)`.
    #[cfg(test)]
    pub(crate) fn pk_filter_census(&self) -> (usize, usize) {
        self.shard_index.all_shard_arcs_iter().fold((0, 0), |(n, filtered), s| {
            (n + 1, filtered + usize::from(s.has_shard_filter()))
        })
    }

    /// Every registered shard.
    #[cfg(test)]
    pub(crate) fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.shard_index.all_shard_arcs_iter().collect()
    }

    /// `(L0 shard count, per-level guard count)`.
    #[cfg(test)]
    pub(crate) fn level_shape(&self) -> (usize, [usize; super::shard_index::FLSM_LEVELS]) {
        self.shard_index.level_shape()
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// Byte-keyed PK existence check: the net weight across every tier is
    /// positive.
    #[inline]
    pub(crate) fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let mut w: i64 = 0;
        self.for_each_pk_candidate(key, |run, row| w += run.get_weight(row));
        w > 0
    }

    /// Visit every row whose PK equals `key`, the heap tiers' newest first: that
    /// puts `live_row_at`'s live row ahead of the retracted history it would
    /// otherwise scan past.
    fn for_each_pk_candidate(&self, key: &[u8], mut f: impl FnMut(&Run, usize)) {
        let mut visit = |run: Run, start: usize| {
            for row in start..pk_group_end(&run, start) {
                f(&run, row);
            }
        };
        let fingerprint = probe_key(key);
        for set in self.ram_tiers() {
            set.find_pk_bytes(key, fingerprint, |batch, start| {
                visit(Run::Mem(Rc::clone(batch)), start)
            });
        }
        self.shard_index
            .find_pk_bytes(key, fingerprint, &mut |shard, start| visit(Run::Shard(shard), start));
    }

    /// The net weight at OPK `key` and, when it is positive, a live
    /// (PK, payload) row: one whose payload group across every tier nets
    /// positive.
    pub(crate) fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        let mut pool = self.live_row_scratch.take();

        let mut total_w: i64 = 0;
        self.for_each_pk_candidate(key, |run, row| {
            total_w += run.get_weight(row);
            pool.push(StoredRow { run: run.clone(), row });
        });

        let mut row = None;
        if total_w > 0 {
            let schema = &self.shard_index.schema;
            let winner = with_payload_cmp!(schema, first_live_payload_group, schema, &pool);
            debug_assert!(
                winner.is_some(),
                "positive net PK weight implies a positive payload group"
            );
            row = winner.map(|i| pool.swap_remove(i));
        }

        pool.clear();
        self.live_row_scratch.set(pool);
        (total_w, row)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
