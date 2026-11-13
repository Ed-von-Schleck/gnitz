//! Unified Table: three [`RunSet`]s in RAM over a `ShardIndex`.
//!
//! Ingest lands in the `memtable` run set and folds into the `ram_tier` once it
//! passes its byte budget; the RAM tier spills to a shard past its own ceiling,
//! and a barrier folds it into one durable shard.
//!
//! A store's runs are its history in order, so a read may stop short of the
//! newest: a pending ingest lands in the `pending` run set, above the store's
//! [`Cut`], and [`Table::seal`] moves the cut past it. A barrier that comes
//! first writes the pending rows to shards the disk tier holds apart, which a
//! reader at the cut leaves out.
//!
//! Oldest to newest, the runs are: the shard levels, the RAM tier, the
//! memtable, then above the cut the pending shards and the `pending` set. Each
//! run is the consolidated sum of a contiguous stretch of ingests, and every
//! heap run is newer than every shard below the cut: a barrier empties every
//! heap tier into shards, a spill moves the oldest, and an ingest below the cut
//! lands only while nothing is pending.
//!
//! Every reader lists the runs newest first: the heap tiers, then the shards
//! with the pending ones first. That puts the pending shards after the
//! memtable and the RAM tier, which is their age order only because a barrier
//! that writes a pending shard empties both, and nothing lands below the cut
//! while a shard is pending. Readers rely on that order: a cursor is
//! [marked](ReadCursor::mark_newest_first) newest first, and
//! [`Table::unique_live_row_at`] lets the newest run holding a key decide it.
//!
//! A row and its retraction stay stored until a fold merges their runs. An open
//! starts at the store's [`LiveFloor`] rather than walking the cancelled pairs
//! under it.

use std::cell::Cell;
use std::rc::Rc;

use super::manifest::RecoverySource;
use super::run_set::RunSet;
use super::shard_index::ShardIndex;
use super::Cut;
use gnitz_wire::{KeyRange, PkBuf, PkKeys};
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{empty_cursor, from_runs, from_runs_at, from_runs_in_band, PkSetGather, ReadCursor};
use gnitz_zset::repr::{Run, StoredRow};
use gnitz_zset::schema::SchemaDescriptor;
use gnitz_zset::schema::{group_band, probe_key, range_band};

/// Ingest runs fold into the RAM tier once they pass this.
pub(super) const MEMTABLE_BYTES: usize = 192 << 10;

/// The RAM-tier ceiling a registry's stores open with unless its `StoreConfig`
/// overrides it: it bounds that one tier, not the table's heap. The ceiling trades
/// spill writes against RSS; shrinking it is how a test reaches the disk regime on
/// small data.
pub(crate) const DEFAULT_RAM_TIER_BYTES: usize = 32 << 20;

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

/// A key under which no row nets nonzero at either [`Cut`]. Every ingest lowers
/// it, and the first open after the rows move judges whether it repays a seek.
/// No row above the cut sits under it, so the two cuts agree on every key below.
#[derive(Clone, Copy, Default)]
struct LiveFloor {
    /// `None` where unknown, or where it skips too few rows to repay a seek.
    at: Option<PkBuf>,
    judged: bool,
}

pub(crate) struct Table {
    /// Runs ingested above the cut, folded among themselves only: [`Self::seal`]
    /// moves them into `memtable` as one run.
    pending: RunSet,
    /// Ingest runs, folded into `ram_tier` once they pass [`MEMTABLE_BYTES`].
    /// [Cancelling](RunSet::cancelling): a reduce output's tick retracts the
    /// rows the tick before it wrote, and merged at once the pair leaves no
    /// cancelled row for a seek of that group to step over.
    memtable: RunSet,
    /// Folded memtable runs, spilled to a shard past the RAM-tier ceiling.
    ram_tier: RunSet,
    /// The disk tier, and the one owner of this store's schema, directory and
    /// manifest.
    shard_index: ShardIndex,

    /// Set by [`Self::hold_in_ram`]: the RAM tier grows past its budget and never
    /// persists.
    held_in_ram: bool,

    /// Last `full_scan` result, held until the row set moves.
    cached_full_scan: Cell<Option<Rc<Batch>>>,

    live_floor: Cell<LiveFloor>,
}

mod flush;

pub(crate) use flush::flush_barrier;

impl Table {
    /// Open a table at `dir`. `recovery_source` decides what this does with
    /// whatever is already on disk; `capacity`, which bounds the registered
    /// on-disk shard bytes, binds for the store's whole life, and the RAM-tier
    /// ceiling until [`Self::hold_in_ram`].
    pub(crate) fn new(
        dir: &str,
        schema: SchemaDescriptor,
        recovery_source: RecoverySource,
        ram_tier_bytes: usize,
        capacity: Option<u64>,
        compress: bool,
    ) -> Result<Self, StorageError> {
        let shard_index = ShardIndex::open(dir, schema, recovery_source, capacity, compress)?;
        Ok(Table {
            // Never full: the budget only caps its PK filter.
            pending: RunSet::new(MEMTABLE_BYTES),
            memtable: RunSet::cancelling(MEMTABLE_BYTES),
            ram_tier: RunSet::cancelling(ram_tier_bytes),
            shard_index,
            held_in_ram: false,
            cached_full_scan: Cell::new(None),
            live_floor: Default::default(),
        })
    }

    /// The schema this store's rows are read in.
    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.shard_index.schema
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// See [`ShardIndex::has_skeleton_shard`].
    pub(crate) fn has_skeleton_rows(&self) -> bool {
        self.shard_index.has_skeleton_shard()
    }

    /// The checkpoint mark of the manifest this open loaded; `None` without one.
    pub(crate) fn loaded_mark(&self) -> Option<u64> {
        self.shard_index.dir.loaded_mark()
    }

    /// See [`ShardIndex::append_terminal_run`].
    pub(crate) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        debug_assert!(
            self.ram_tiers(Cut::Sealed).all(|set| set.len() == 0) && !self.has_pending(),
            "a terminal shard lands under no heap run"
        );
        self.admit(run);
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
        self.pending.widen_runs(&schema);
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
    ///
    /// `#[inline]`: it takes a `Batch` by value, and every stateful operator
    /// calls it once per epoch.
    #[inline]
    pub(crate) fn ingest(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.ingest_run(Rc::new(batch.into_consolidated().trimmed()))
    }

    /// [`Self::ingest`] of a consolidated run its caller keeps a handle to.
    pub(crate) fn ingest_run(&mut self, run: Rc<Batch>) -> Result<(), StorageError> {
        if run.is_empty() {
            return Ok(());
        }
        debug_assert!(
            !self.has_pending(),
            "an ingest below the cut lands only while nothing is pending"
        );
        self.admit(&run);
        let ingested = run.total_bytes();
        self.memtable.push_run(run, &self.shard_index.schema);
        if self.memtable.is_full() {
            self.fold_to_ram()?;
        }
        self.upkeep(ingested)
    }

    /// Drop every row this store holds, in RAM and on disk. The next barrier
    /// publishes the empty store.
    pub(crate) fn clear(&mut self) {
        self.cached_full_scan.set(None);
        self.live_floor = Default::default();
        self.pending.clear();
        self.memtable.clear();
        self.ram_tier.clear();
        self.shard_index.clear();
    }

    /// [`Self::ingest`] above the cut: every reader sees the rows
    /// but one at [`Cut::Sealed`], until [`Self::seal`].
    pub(crate) fn ingest_pending(&mut self, batch: Batch) {
        let batch = batch.into_consolidated();
        if batch.is_empty() {
            return;
        }
        self.admit(&batch);
        self.pending
            .push_run(Rc::new(batch.trimmed()), &self.shard_index.schema);
    }

    /// Whether any row sits above the cut.
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.len() > 0 || self.shard_index.has_pending()
    }

    /// Move the cut past every pending row and answer them, consolidated: the
    /// store's delta since the last seal. `None` when there is none. The row set
    /// a [`Cut::Now`] reader sees does not move.
    pub(crate) fn seal(&mut self) -> Result<Option<Batch>, StorageError> {
        let schema = self.shard_index.schema;
        let run = self.pending.drain_into(&mut self.memtable, &schema);
        let shards = self.shard_index.seal_pending();
        let delta = match shards.is_empty() {
            true => run,
            // A barrier since the last seal left part of the delta in shards.
            false => {
                let runs = run.into_iter().map(Run::Mem).chain(shards.into_iter().map(Run::Shard));
                Some(from_runs(runs, schema).materialize())
            }
        };
        // The cut now reads every ingest.
        self.rejudge_live_floor();
        if self.memtable.is_full() {
            self.fold_to_ram()?;
        }
        self.upkeep(delta.as_ref().map_or(0, |delta| delta.total_bytes()))?;
        // Copied only while a heap tier still holds the run.
        Ok(delta.map(Rc::unwrap_or_clone).filter(|delta| !delta.is_empty()))
    }

    /// Pay the disk tier's [upkeep](ShardIndex::upkeep) for `ingested` bytes.
    fn upkeep(&mut self, ingested: usize) -> Result<(), StorageError> {
        // A read replica of another process's directory must not write into it.
        if self.held_in_ram {
            return Ok(());
        }
        self.shard_index.upkeep(ingested)
    }

    /// Fold the memtable into the RAM tier, then every fold the disk tier owes,
    /// in one call: for a caller with no ingest to pay for them out of.
    pub(crate) fn settle(&mut self) -> Result<(), StorageError> {
        self.fold_to_ram()?;
        // A read replica of another process's directory must not write into it.
        if self.held_in_ram {
            return Ok(());
        }
        self.shard_index.maintain(u64::MAX).map(drop)
    }

    /// The bytes this store's next published manifest carries for its owner.
    pub(crate) fn set_caller_record(&mut self, record: Vec<u8>) {
        self.shard_index.dir.set_caller_record(record);
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// The heap-resident tiers `cut` reads, newest first.
    fn ram_tiers(&self, cut: Cut) -> impl Iterator<Item = &RunSet> {
        [&self.pending, &self.memtable, &self.ram_tier]
            .into_iter()
            .skip(usize::from(cut == Cut::Sealed))
    }

    /// The runs `cut` reads whose PK extent meets the inclusive `bound`, in the
    /// newest-first order the module doc states; `None` takes them all.
    fn sources<'s, 'b>(
        &'s self,
        bound: Option<(&'b [u8], &'b [u8])>,
        cut: Cut,
    ) -> impl Iterator<Item = Run> + use<'s, 'b> {
        self.ram_tiers(cut)
            .flat_map(move |set| set.runs_overlapping(bound))
            .cloned()
            .map(Run::Mem)
            .chain(self.shard_index.shard_arcs(bound, cut).map(Run::Shard))
    }

    /// Every run the store holds.
    pub(crate) fn runs(&self) -> impl Iterator<Item = Run> + '_ {
        self.sources(None, Cut::Now)
    }

    /// Open a read-only cursor over the rows `cut` reads: every run, unpruned,
    /// positioned at the [`LiveFloor`], which it judges when that is due.
    pub(crate) fn open_cursor(&self, cut: Cut) -> ReadCursor {
        let schema = self.shard_index.schema;
        let LiveFloor { at, judged } = self.live_floor.get();
        let runs = self.sources(None, cut);
        let mut cursor = match at {
            Some(at) => from_runs_at(runs, schema, at.pk_bytes()),
            None => from_runs(runs, schema),
        };
        cursor.mark_newest_first();
        if !judged {
            let first_live = cursor.passed_more_than_a_seek().then(|| match cursor.is_valid() {
                true => PkBuf::from_bytes(cursor.current_pk_bytes()),
                false => PkBuf::max(schema.pk_stride()),
            });
            // Kept at or under every row above the cut, so it holds at both cuts.
            let at = match (first_live, self.pending_least()) {
                (Some(f), Some(p)) if p < f.pk_bytes() => Some(PkBuf::from_bytes(p)),
                (f, _) => f,
            };
            self.live_floor.set(LiveFloor { at, judged: true });
        }
        cursor
    }

    /// The least key a row above the cut holds.
    fn pending_least(&self) -> Option<&[u8]> {
        self.pending
            .runs_overlapping(None)
            .map(|run| run.get_pk_bytes(0))
            .chain(self.shard_index.pending_least())
            .min()
    }

    /// A fold leaves the floor valid, but can cancel the rows it skips.
    fn rejudge_live_floor(&mut self) {
        self.live_floor.get_mut().judged = false;
    }

    /// `rows`, a non-empty run, enter the store.
    fn admit(&mut self, rows: &Batch) {
        debug_assert!(
            !rows.is_empty() && rows.is_consolidated(),
            "a run's least key is its first"
        );
        self.cached_full_scan.set(None);
        let least = rows.get_pk_bytes(0);
        let floor = self.live_floor.get_mut();
        floor.judged = false;
        match &mut floor.at {
            Some(at) if least < at.pk_bytes() => *at = PkBuf::from_bytes(least),
            _ => {}
        }
    }

    /// A cursor for probing at the keys in `[first, last]` — whole PKs, or the
    /// same leading bytes of one — positioned on the band they span, over the
    /// rows `cut` reads.
    pub(crate) fn cursor_between(&self, first: &[u8], last: &[u8], cut: Cut) -> ReadCursor {
        let stride = self.shard_index.schema.pk_stride();
        let band = group_band(first, last, stride)
            .expect("`first <= last`, so the band from one's group to the other's holds a key");
        self.cursor(Some(band), cut)
    }

    /// A cursor positioned on the key band `r` walks, over the rows `cut` reads.
    pub(crate) fn range_cursor(&self, r: &KeyRange, cut: Cut) -> ReadCursor {
        self.cursor(range_band(&self.shard_index.schema, r), cut)
    }

    /// Every live row of `keys` — whole PKs, or the same leading columns of
    /// one — over the runs `cut` reads that the span they cover can reach.
    pub(crate) fn gather(&self, keys: PkKeys, cut: Cut) -> PkSetGather {
        let cursor = match keys.bounds() {
            Some((first, last)) => self.cursor_between(first, last, cut),
            None => empty_cursor(self.shard_index.schema),
        };
        PkSetGather::over(cursor, keys)
    }

    /// A cursor positioned on the OPK key band `[start, end)` of the rows `cut`
    /// reads (`None` for `end`: the top of the key space), over the runs that can
    /// hold a key of it; `None` for the band is the empty one. The band is
    /// clamped to the [`LiveFloor`], and the whole key space is
    /// [`Self::open_cursor`]'s.
    fn cursor(&self, band: Option<(PkBuf, Option<PkBuf>)>, cut: Cut) -> ReadCursor {
        let schema = self.shard_index.schema;
        let Some((mut start, end)) = band else {
            return empty_cursor(schema);
        };
        if end.is_none() && start.pk_bytes().iter().all(|&b| b == 0) {
            return self.open_cursor(cut);
        }
        match self.live_floor.get().at {
            Some(at) if at > start => {
                if end.is_some_and(|end| end <= at) {
                    return empty_cursor(schema);
                }
                start = at;
            }
            _ => {}
        }
        let stride = schema.pk_stride();
        let hi = end.unwrap_or_else(|| PkBuf::max(stride));
        debug_assert!(
            start.pk_bytes().len() == stride && hi.pk_bytes().len() == stride,
            "Table::cursor: a bound is not pk_stride wide",
        );
        let runs = self.sources(Some((start.pk_bytes(), hi.pk_bytes())), cut);
        let mut cursor = from_runs_in_band(runs, schema, start.pk_bytes(), end.as_ref().map(PkBuf::pk_bytes));
        cursor.mark_newest_first();
        cursor
    }

    /// The consolidated batch of all live rows, cached until the row set moves.
    pub(crate) fn full_scan(&self) -> Rc<Batch> {
        let rc = self
            .cached_full_scan
            .take()
            .unwrap_or_else(|| self.open_cursor(Cut::Now).materialize());
        self.cached_full_scan.set(Some(Rc::clone(&rc)));
        rc
    }

    /// Raw rows across every tier, cross-run duplicates and ghosts included.
    pub(crate) fn estimated_rows(&self) -> usize {
        self.ram_tiers(Cut::Now).map(|s| s.row_count()).sum::<usize>() + self.shard_index.total_rows()
    }

    /// `(L0 shard count, L1 and terminal guard counts)`.
    #[cfg(test)]
    pub(crate) fn level_shape(&self) -> (usize, [usize; 2]) {
        self.shard_index.level_shape()
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// The live row at OPK `key`, for a store whose every key nets 0 or 1 — a
    /// base table's under its unique-PK rule, or a system family's.
    ///
    /// By the run order the module doc states, the newest run holding `key`
    /// decides it: what that stretch of ingests left is the key's state now,
    /// and its rows of the key are that state less the one it started from — a
    /// positive row is the live one, and a retraction alone means the stretch
    /// deleted the key.
    #[inline]
    pub(crate) fn unique_live_row_at(&self, key: &[u8]) -> Option<StoredRow> {
        let fp = probe_key(key);
        let heap = self.ram_tiers(Cut::Now).find_map(|set| set.holding(key, fp).next());
        let (run, mut rows) = match heap {
            Some((batch, rows)) => (Run::Mem(Rc::clone(batch)), rows),
            None => {
                let (shard, rows) = self.shard_index.holding(key, fp).next()?;
                (Run::Shard(Rc::clone(shard)), rows)
            }
        };
        let live = (rows.find(|&row| run.get_weight(row) > 0)).map(|row| StoredRow::new(run, row));
        debug_assert!(
            {
                let w = self.net_weight_at(key);
                w == i64::from(live.is_some()) && (0..=1).contains(&w)
            },
            "unique_live_row_at: a key nets 0 or 1, and 1 exactly when it answers a row",
        );
        live
    }

    /// The net weight at OPK `key`, summed over every run of every tier: the
    /// check [`Self::unique_live_row_at`]'s newest-run answer is held to.
    pub(crate) fn net_weight_at(&self, key: &[u8]) -> i64 {
        let fp = probe_key(key);
        let heap = self.ram_tiers(Cut::Now).flat_map(|set| set.holding(key, fp));
        let heap: i64 = heap
            .map(|(batch, rows)| rows.map(|r| batch.get_weight(r)).sum::<i64>())
            .sum();
        let shards = self.shard_index.holding(key, fp);
        heap + shards
            .map(|(shard, rows)| rows.map(|r| shard.get_weight(r)).sum::<i64>())
            .sum::<i64>()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/table.rs"]
mod bench;
