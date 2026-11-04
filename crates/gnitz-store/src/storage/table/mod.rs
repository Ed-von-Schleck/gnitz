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
//! A row and its retraction stay stored until a fold merges their runs. An open
//! starts at the store's [`LiveFloor`] rather than walking the cancelled pairs
//! under it.

use std::cell::Cell;
use std::ops::Range;
use std::rc::Rc;

use super::run_set::RunSet;
use super::shard_index::{RecoverySource, ShardIndex};
use super::Cut;
use gnitz_wire::{KeyRange, PkBuf, PkKeys};
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{empty_cursor, from_runs, from_runs_at, from_runs_in_band, PkSetGather, ReadCursor};
use gnitz_zset::repr::{first_live_payload_group, Run, StoredRow};
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

    /// `live_row_at`'s candidate pool, kept between calls for its capacity.
    live_row_scratch: Cell<Vec<StoredRow>>,

    /// Last `full_scan` result, held until the row set moves.
    cached_full_scan: Cell<Option<Rc<Batch>>>,

    live_floor: Cell<LiveFloor>,
}

mod flush;

pub(crate) use flush::flush_barrier;

impl Table {
    /// Open a table at `dir`. `recovery_source` decides what this does with
    /// whatever is already on disk; the RAM-tier ceiling and `capacity`, which
    /// bounds the registered on-disk shard bytes, bind for the store's whole life.
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
            live_row_scratch: Cell::new(Vec::new()),
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
        self.shard_index.loaded_mark
    }

    /// See [`ShardIndex::append_terminal_run`].
    pub(crate) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
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
        debug_assert_eq!(
            self.pending.len(),
            0,
            "an ingest below the cut lands above every pending run"
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
                let cap = shards.len() + 1;
                let runs = run.into_iter().map(Run::Mem).chain(shards.into_iter().map(Run::Shard));
                Some(from_runs(runs, schema, cap).materialize())
            }
        };
        // The cut now reads every ingest.
        self.rejudge_live_floor();
        if self.memtable.is_full() {
            self.fold_to_ram()?;
        }
        self.upkeep(delta.as_ref().map_or(0, |delta| delta.total_bytes()))?;
        // Copied only while the memtable still holds the run.
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

    /// Every fold the disk tier owes, in one call: for a caller with no ingest
    /// to pay for them out of.
    pub(crate) fn settle(&mut self) -> Result<(), StorageError> {
        if self.held_in_ram {
            return Ok(());
        }
        self.rejudge_live_floor();
        self.shard_index.maintain(u64::MAX).map(drop)
    }

    /// The bytes this store's next published manifest carries for its owner.
    pub(crate) fn set_caller_record(&mut self, record: Vec<u8>) {
        self.shard_index.caller_record = record;
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

    /// The heap-resident runs whose PK extent meets the inclusive `bound`; `None`
    /// takes them all.
    fn mem_runs(&self, bound: Option<(PkBuf, PkBuf)>, cut: Cut) -> impl Iterator<Item = Run> + '_ {
        self.ram_tiers(cut)
            .flat_map(move |set| set.runs_overlapping(bound))
            .cloned()
            .map(Run::Mem)
    }

    /// How many cursor sources [`Self::mem_runs`] can yield.
    fn mem_run_count(&self) -> usize {
        self.ram_tiers(Cut::Now).map(|s| s.len()).sum()
    }

    /// Every run `cut` reads.
    pub(crate) fn runs(&self, cut: Cut) -> impl Iterator<Item = Run> + '_ {
        self.mem_runs(None, cut)
            .chain(self.shard_index.shard_arcs(cut).map(Run::Shard))
    }

    /// Open a read-only cursor over the rows `cut` reads.
    pub(crate) fn open_cursor(&self, cut: Cut) -> ReadCursor {
        let LiveFloor { at, judged } = self.live_floor.get();
        let cursor = self.cursor_from(at, cut);
        if !judged {
            let first_live = cursor.passed_more_than_a_seek().then(|| match cursor.is_valid() {
                true => PkBuf::from_bytes(cursor.current_pk_bytes()),
                false => PkBuf::max(self.shard_index.schema.pk_stride()),
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

    /// A cursor over the rows `cut` reads, on its first live row at or above
    /// `floor`.
    fn cursor_from(&self, floor: Option<PkBuf>, cut: Cut) -> ReadCursor {
        let schema = self.shard_index.schema;
        let cap = self.mem_run_count() + self.shard_index.shard_count();
        match floor {
            Some(at) => from_runs_at(self.runs(cut), schema, cap, at.pk_bytes()),
            None => from_runs(self.runs(cut), schema, cap),
        }
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
        // The zero-width prefix is every key: no run to rule out, no key to seek.
        if first.is_empty() {
            return self.open_cursor(cut);
        }
        let stride = self.shard_index.schema.pk_stride();
        let band = group_band(first, last, stride)
            .expect("`first <= last`, so the band from one's group to the other's holds a key");
        self.band_cursor(band, cut)
    }

    /// A cursor positioned on the key band `r` walks, over the rows `cut` reads.
    pub(crate) fn range_cursor(&self, r: &KeyRange, cut: Cut) -> ReadCursor {
        match range_band(&self.shard_index.schema, r) {
            Some(band) => self.band_cursor(band, cut),
            None => empty_cursor(self.shard_index.schema),
        }
    }

    /// A cursor positioned on the OPK key band `[start, end)` of the rows `cut`
    /// reads (`None` for `end`: the top of the key space), over the runs that can
    /// hold a key of it.
    fn band_cursor(&self, (mut start, end): (PkBuf, Option<PkBuf>), cut: Cut) -> ReadCursor {
        let schema = self.shard_index.schema;
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
            "band_cursor: a bound is not pk_stride wide",
        );
        let runs = self
            .mem_runs(Some((start, hi)), cut)
            .chain(self.shard_index.shard_arcs_in_range(start, hi, cut).map(Run::Shard));
        let cap = self.mem_run_count() + self.shard_index.narrow_range_shards();
        let end = end.as_ref().map(PkBuf::pk_bytes);
        from_runs_in_band(runs, schema, cap, start.pk_bytes(), end)
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

    /// Byte-keyed PK existence check: the net weight across every tier is
    /// positive.
    #[inline]
    pub(crate) fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let mut w: i64 = 0;
        self.for_each_pk_candidate(key, probe_key(key), true, |run, row| w += run.get_weight(row));
        w > 0
    }

    /// Visit every row whose PK equals `key`, whose [`probe_key`] is
    /// `fingerprint`: the heap tiers' when `heap`, newest first, which puts
    /// `live_row_at`'s live row ahead of the retracted history it would
    /// otherwise scan past, then the shards'.
    fn for_each_pk_candidate(&self, key: &[u8], fingerprint: u64, heap: bool, mut f: impl FnMut(&Run, usize)) {
        let mut visit = |run: Run, rows: Range<usize>| {
            for row in rows {
                f(&run, row);
            }
        };
        if heap {
            for set in self.ram_tiers(Cut::Now) {
                set.find_pk_bytes(key, fingerprint, |batch, rows| visit(Run::Mem(Rc::clone(batch)), rows));
            }
        }
        self.shard_index.find_pk_bytes(key, fingerprint, |shard, rows| {
            visit(Run::Shard(Rc::clone(shard)), rows)
        });
    }

    /// [`Self::live_row_at`]'s row, for a store whose every key nets 0 or 1 —
    /// a base table's, under its unique-PK rule.
    ///
    /// There every heap run is the consolidated sum of a contiguous stretch of
    /// ingests, newer than every shard — a barrier empties every heap tier
    /// into shards, and a spill the oldest — and the heap tiers are walked
    /// newest first, the pending tier being newer than the memtable since an
    /// ingest below the cut finds it empty. So the newest heap run holding `key` decides it: what
    /// that stretch left is the key's state now, and its rows of the key are
    /// that state less the one it started from — a positive row is the live
    /// one, and a retraction alone means the stretch deleted the key. A key no
    /// heap run holds is summed over the shards.
    #[inline]
    pub(crate) fn unique_live_row_at(&self, key: &[u8]) -> Option<StoredRow> {
        let fingerprint = probe_key(key);
        let newest = self
            .ram_tiers(Cut::Now)
            .find_map(|set| set.newest_holding(key, fingerprint));
        let live = match newest {
            Some((run, rows)) => {
                let live = rows.into_iter().find(|&row| run.get_weight(row) > 0);
                live.map(|row| StoredRow::new(Run::Mem(Rc::clone(run)), row))
            }
            None => self.live_row_in(key, fingerprint, false).1,
        };
        debug_assert!(
            {
                let (w, _) = self.live_row_at(key);
                w == i64::from(live.is_some()) && (0..=1).contains(&w)
            },
            "unique_live_row_at: a key nets 0 or 1, and 1 exactly when it answers a row",
        );
        live
    }

    /// The net weight at OPK `key` and, when it is positive, a live
    /// (PK, payload) row: one whose payload group across every tier nets
    /// positive.
    pub(crate) fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        self.live_row_in(key, probe_key(key), true)
    }

    /// [`Self::live_row_at`] over the shards, and the heap tiers too when `heap`;
    /// `fingerprint` is `key`'s [`probe_key`].
    fn live_row_in(&self, key: &[u8], fingerprint: u64, heap: bool) -> (i64, Option<StoredRow>) {
        let mut pool = self.live_row_scratch.take();

        let mut total_w: i64 = 0;
        self.for_each_pk_candidate(key, fingerprint, heap, |run, row| {
            total_w += run.get_weight(row);
            pool.push(StoredRow::new(run.clone(), row));
        });

        let mut row = None;
        if total_w > 0 {
            let schema = &self.shard_index.schema;
            let winner = first_live_payload_group(schema, &pool);
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

#[cfg(test)]
#[path = "benches/table.rs"]
mod bench;
