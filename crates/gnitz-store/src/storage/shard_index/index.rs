//! In-memory FLSM index state + the compaction trigger/orchestration for
//! [`ShardIndex`]: the one shard writer, PK probes, the post-spill upkeep
//! (`maintain`), and `run_compact` — the L0→L1 fold, the byte targets every
//! level's guard partition is held at, and the vertical drain into the terminal
//! level.

use std::fs;
use std::ops::Range;
use std::rc::Rc;

use super::super::manifest;
use super::{
    fold_destinations, CompactionKind, FLSMLevel, LevelGuard, ShardBudget, ShardEntry, ShardIndex, CANCEL_PERCENT,
    GUARD_FILE_THRESHOLD, L0, L0_COMPACT_THRESHOLD, L1, MIN_GUARD_BYTES, SWEEP_STEPS, TERMINAL,
};
use gnitz_expr::RowSource;
use gnitz_wire::PkBuf;
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::ShardWriteOpts;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{merge_and_route, MappedShard};
use gnitz_zset::schema::key::pk_ranges_overlap;

impl ShardIndex {
    pub(super) fn all_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.settled_entries().chain(&self.pending)
    }

    /// Every shard at or below the store's cut.
    fn settled_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.levels.iter().flat_map(FLSMLevel::entries)
    }

    /// Mutable twin of [`all_entries`](Self::all_entries), in the same order, so
    /// [`swap_schema`](Self::swap_schema) can assign rebound shards back into
    /// the entries it walked.
    pub(super) fn all_entries_mut(&mut self) -> impl Iterator<Item = &mut ShardEntry> {
        self.levels
            .iter_mut()
            .flat_map(|l| l.guards.iter_mut().flat_map(|g| g.entries.iter_mut()))
            .chain(&mut self.pending)
    }

    /// Write `batch` as an unpublished shard named by a fresh seq. `newest`
    /// defaults to that seq.
    fn write_shard(&mut self, batch: &Batch, skeleton: bool, newest: Option<u64>) -> Result<ShardEntry, StorageError> {
        self.shard_seq += 1;
        let seq = self.shard_seq;
        let path = manifest::shard_path(&self.output_dir, seq);
        batch
            .write_as_shard(
                &path,
                ShardWriteOpts {
                    skeleton,
                    skip_pk_filter: self.skip_pk_filter,
                },
            )
            .and_then(|()| ShardEntry::open(&self.output_dir, seq, &self.schema, newest.unwrap_or(seq)))
            .inspect_err(|_| self.unlink_shard(seq))
    }

    /// Unlink the shard drawn at `seq`, best-effort.
    fn unlink_shard(&self, seq: u64) {
        let _ = fs::remove_file(manifest::shard_path(&self.output_dir, seq));
    }

    /// Append `run` to L0 as one unpublished shard: the spill.
    pub(crate) fn append_l0_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        let entry = self.write_shard(run, false, None)?;
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        self.levels[L0].get_or_create_guard(whole).entries.push(entry);
        Ok(())
    }

    /// Write `run`, rows above the store's cut, as one unpublished shard outside
    /// every level.
    pub(crate) fn append_pending_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        let entry = self.write_shard(run, false, None)?;
        self.pending.push(entry);
        Ok(())
    }

    /// The shards above the cut.
    pub(crate) fn pending_arcs(&self) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.pending.iter().map(|e| Rc::clone(&e.shard))
    }

    /// Move the cut past every pending shard: each enters L0 as it stands.
    /// Answers whether one did, so the caller owes the upkeep a spill does.
    pub(crate) fn seal_pending(&mut self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        let pending = std::mem::take(&mut self.pending);
        self.levels[L0].get_or_create_guard(whole).entries.extend(pending);
        true
    }

    /// The disk tier's upkeep after a spill.
    pub(crate) fn maintain(&mut self) -> Result<(), StorageError> {
        if self.levels[L0].entries().count() > L0_COMPACT_THRESHOLD || self.l0_cancels() {
            self.run_compact()?;
        }
        self.enforce_capacity()
    }

    /// Whether `retractions` are expected to cancel [`CANCEL_PERCENT`] of `rows`.
    fn cancels(&self, retractions: usize, rows: usize) -> bool {
        let cancelled = self.cancel_yield.expect(retractions);
        cancelled > 0 && cancelled * 100 >= rows * CANCEL_PERCENT
    }

    /// Whether L0's retractions are expected to cancel that share of the whole
    /// store. They retract from the levels below, so only the fold down brings
    /// them to the guards whose own folds then cancel them.
    fn l0_cancels(&self) -> bool {
        let retractions: usize = self.levels[L0].entries().map(|e| e.shard.retraction_rows()).sum();
        retractions > 0 && self.cancels(retractions, self.settled_entries().map(|e| e.shard.row_count()).sum())
    }

    /// Write `run` as one unpublished shard at the terminal level, under a new
    /// guard keyed by its first PK.
    pub(crate) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        assert!(
            !run.is_empty() && run.is_consolidated(),
            "a terminal run is consolidated"
        );
        let first = PkBuf::from_bytes(run.get_pk_bytes(0));
        let terminal = &self.levels[TERMINAL];
        assert!(
            terminal.guards.last().is_none_or(|g| g.key_extent().1 < first),
            "a terminal run ascends past every key its level holds"
        );
        let entry = self.write_shard(run, false, None)?;
        self.l0_run_bytes = self.l0_run_bytes.max(entry.shard.file_len());
        self.levels[TERMINAL]
            .guards
            .push(LevelGuard { guard_key: first, entries: vec![entry] });
        Ok(())
    }

    /// Whether the last renamed manifest names `e`.
    fn published(&self, e: &ShardEntry) -> bool {
        e.seq <= self.published_through
    }

    /// Every live shard no published manifest names yet.
    pub(crate) fn unsynced_paths(&self) -> impl Iterator<Item = String> + '_ {
        self.all_entries()
            .filter(|e| !self.published(e))
            .map(|e| manifest::shard_path(&self.output_dir, e.seq))
    }

    /// The manifest built from the current shard set has been renamed into place.
    pub(crate) fn mark_published(&mut self) {
        self.published_through = self.shard_seq;
    }

    /// Every live shard's `Rc`, yielded lazily — callers `extend` without an
    /// intermediate `Vec` (the per-worker cursor gather).
    pub(crate) fn all_shard_arcs_iter(&self) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.all_entries().map(|e| Rc::clone(&e.shard))
    }

    /// Every shard whose PK extent meets `[lo, hi]`; the pending ones iff `pending`.
    pub(crate) fn shard_arcs_in_range(
        &self,
        lo: PkBuf,
        hi: PkBuf,
        pending: bool,
    ) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.levels
            .iter()
            .flat_map(move |level| {
                let run = level.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
                level.guards[run].iter().flat_map(|g| g.entries.iter())
            })
            .chain(self.pending.iter().filter(move |_| pending))
            .filter(move |e| pk_ranges_overlap(e.pk_min.pk_bytes(), e.pk_max.pk_bytes(), lo.pk_bytes(), hi.pk_bytes()))
            .map(|e| Rc::clone(&e.shard))
    }

    /// A capacity hint for a cursor over a range inside one guard per level.
    pub(crate) fn narrow_range_shards(&self) -> usize {
        self.levels[L0].entries().count() + GUARD_FILE_THRESHOLD + 1
    }

    /// Registered shards across every tier — one cursor source each, which is
    /// what an unbounded cursor open sizes its vectors to.
    pub(crate) fn shard_count(&self) -> usize {
        self.all_entries().count()
    }

    /// Raw rows across every live shard, summed without touching an `Rc`. Raw:
    /// cross-shard duplicates and ghosts are counted, so it is an upper bound on
    /// the live rows a walk would emit — the shape the selectivity gate wants.
    pub(crate) fn total_rows(&self) -> usize {
        self.all_entries().map(|e| e.shard.row_count()).sum()
    }

    /// Whether a read of this store can meet a `(PK, coarse weight)` row it has to
    /// hydrate. Only a budgeted view's sweep writes one, and only into the
    /// terminal level.
    pub(crate) fn has_skeleton_shard(&self) -> bool {
        matches!(self.budget, ShardBudget::Dehydrate(_))
            && self.levels[TERMINAL].guards.iter().any(LevelGuard::dehydrated)
    }

    /// Visit each shard holding OPK `key`, with the row its matches start at;
    /// `filter_key` is `key`'s `probe_key`. Each level routes by the whole key
    /// against its guard partition.
    pub(crate) fn find_pk_bytes(&self, key: &[u8], filter_key: u64, mut visitor: impl FnMut(&Rc<MappedShard>, usize)) {
        for level in &self.levels {
            let Some(guard) = level.guards.get(level.slot(key)) else {
                continue;
            };
            for e in &guard.entries {
                if let Some(row) = e.probe_pk_bytes(key, filter_key) {
                    visitor(&e.shard, row);
                }
            }
        }
        for e in &self.pending {
            if let Some(row) = e.probe_pk_bytes(key, filter_key) {
                visitor(&e.shard, row);
            }
        }
    }

    /// The one compaction driver: merge the guards `sources` names — whole
    /// guards, one range per level — into `dest`'s guards `keys`, routed as
    /// [`merge_and_route`] defines, then retire them. Answers how many output
    /// shards it wrote.
    ///
    /// A failure registers nothing and unlinks every output it wrote.
    fn compact(
        &mut self,
        sources: &[(usize, Range<usize>)],
        dest: usize,
        keys: &[PkBuf],
        kind: CompactionKind,
    ) -> Result<usize, StorageError> {
        let entries = sources
            .iter()
            .flat_map(|(level, guards)| &self.levels[*level].guards[guards.clone()])
            .flat_map(|g| &g.entries);
        let shards: Vec<Rc<MappedShard>> = entries.clone().map(|e| Rc::clone(&e.shard)).collect();
        // Every output inherits the newest stamp over the inputs.
        let newest = entries.map(|e| e.newest).max().unwrap_or(0);

        let schema = self.schema;
        let inputs: Vec<&MappedShard> = shards.iter().map(|s| &**s).collect();
        let mut opened: Vec<(PkBuf, ShardEntry)> = Vec::with_capacity(keys.len());
        let dehydrate = kind == CompactionKind::Dehydrate;
        let merged = merge_and_route(&inputs, keys, dehydrate, &schema, &mut |key, skeleton, batch| {
            opened.push((key, self.write_shard(&batch, skeleton, Some(newest))?));
            Ok(())
        });
        if let Err(e) = merged {
            self.retire(opened.into_iter().map(|(_, entry)| entry));
            return Err(e);
        }
        if kind == CompactionKind::L0Fold {
            // A run that lands in one guard is one shard, and can outweigh the
            // shards it was spilled as: a frame spans the whole run where it
            // spanned one spill. Unobserved, every such guard would be over
            // target as written and be rewritten at once to be cut in two.
            let largest = opened.iter().map(|(_, e)| e.shard.file_len()).max();
            self.l0_run_bytes = self.l0_run_bytes.max(largest.unwrap_or(0));
        }
        if kind == CompactionKind::GuardSplit {
            // The one fold that reads a guard's whole history at its level, and so
            // every row its retractions can cancel there.
            let (level, guards) = &sources[0];
            let retractions = self.levels[*level].guards[guards.clone()]
                .iter()
                .map(LevelGuard::retractions)
                .sum();
            let read: usize = shards.iter().map(|s| s.row_count()).sum();
            let wrote: usize = opened.iter().map(|(_, e)| e.shard.row_count()).sum();
            self.cancel_yield.observe(retractions, read.saturating_sub(wrote));
        }
        #[cfg(test)]
        super::cstats::record(
            kind,
            shards.iter().map(|s| s.file_len()).sum(),
            opened.iter().map(|(_, e)| e.shard.file_len()).sum(),
            shards.len(),
        );

        for (level, guards) in sources {
            let superseded: Vec<ShardEntry> = self.levels[*level]
                .guards
                .drain(guards.clone())
                .flat_map(|g| g.entries)
                .collect();
            self.retire(superseded);
        }
        let written = opened.len();
        for (key, entry) in opened {
            let guard = self.levels[dest].get_or_create_guard(key);
            guard.entries.push(entry);
            debug_assert!(
                dest != TERMINAL || guard.entries.len() == 1,
                "a terminal guard holds exactly one shard"
            );
        }
        Ok(written)
    }

    /// Unlink the unpublished `entries`; a published one's file waits for
    /// [`unlink_retired`](Self::unlink_retired).
    fn retire(&mut self, entries: impl IntoIterator<Item = ShardEntry>) {
        for e in entries {
            if self.published(&e) {
                self.retired.push(e.seq);
            } else {
                self.unlink_shard(e.seq);
            }
        }
    }

    /// Retire every shard, leaving a store of no rows.
    pub(crate) fn clear(&mut self) {
        let levels = std::mem::take(&mut self.levels);
        let settled = levels.into_iter().flat_map(|l| l.guards).flat_map(|g| g.entries);
        let pending = std::mem::take(&mut self.pending);
        self.retire(settled.chain(pending));
    }

    /// Fold L0 into L1, rebalance L1 and the terminal level against their byte
    /// targets, then drain L1 down to its own. Called with a shard in L0.
    /// Observing `R` first is what makes those targets reflect the fold this call
    /// is about to perform.
    fn run_compact(&mut self) -> Result<(), StorageError> {
        self.l0_run_bytes = self.l0_run_bytes.max(self.levels[L0].bytes());
        let keys = self.l1_guard_keys();
        self.compact(&[(L0, 0..1)], L1, &keys, CompactionKind::L0Fold)?;

        for level in [L1, TERMINAL] {
            self.rebalance_guards(level)?;
        }

        while self.levels[L1].bytes() > self.l1_target_bytes() {
            let Some(gi) = self.cheapest_l1_guard_to_drain() else {
                break;
            };
            self.vertical_fold(gi)?;
        }
        Ok(())
    }

    /// The largest a guard of `level_idx` is allowed to get: `R`, so no
    /// compaction's input grows with the dataset — every fold reads at most two
    /// guards' worth.
    ///
    /// A budgeted store's terminal level takes one sweep step instead, since that
    /// is the granularity `enforce_capacity` evicts at. The clamp keeps a very
    /// large or very small `capacity` from naming a target outside
    /// `[MIN_GUARD_BYTES, R]`.
    fn guard_target_bytes(&self, level_idx: usize) -> u64 {
        match self.budget.cap() {
            Some(cap) if level_idx == TERMINAL => (cap / SWEEP_STEPS).clamp(MIN_GUARD_BYTES, self.l0_run_bytes),
            _ => self.l0_run_bytes,
        }
    }

    /// Bytes L1 is drained to. Capped at two sweep steps for a budgeted store:
    /// `enforce_capacity` cannot evict from L1, so bytes parked there come out of
    /// what the user asked for.
    pub(super) fn l1_target_bytes(&self) -> u64 {
        let target = Self::balanced_l1_target(self.levels[TERMINAL].bytes(), self.l0_run_bytes);
        match self.budget.cap() {
            Some(cap) => target.min(2 * (cap / SWEEP_STEPS)),
            None => target,
        }
    }

    /// L1's two rewrite terms — one per guard fold, one per vertical — balance at
    /// `2√(|L2|·R)`. The `16 R` floor under it is a chosen minimum, not a derived
    /// one: it keeps a small store from draining L1 on every spill.
    ///
    /// `l2_bytes × r` overflows a `u64` at the design point, hence the `u128`;
    /// the `2` stays outside the root so the extremes saturate rather than
    /// overflow it in turn.
    fn balanced_l1_target(l2_bytes: u64, r: u64) -> u64 {
        let balanced = 2 * (u128::from(l2_bytes) * u128::from(r)).isqrt();
        u64::try_from(balanced).unwrap_or(u64::MAX).max(16u64.saturating_mul(r))
    }

    /// The L1 guard whose fold rewrites the fewest terminal bytes: those of the
    /// terminal guards its key extent meets.
    fn cheapest_l1_guard_to_drain(&self) -> Option<usize> {
        let guards = &self.levels[L1].guards;
        let terminal = &self.levels[TERMINAL];
        let target = self.guard_target_bytes(TERMINAL);
        (0..guards.len()).min_by_key(|&gi| {
            let (lo, hi) = guards[gi].key_extent();
            let run = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
            terminal.guards[run]
                .iter()
                .filter(|g| g.key_extent().1 >= lo)
                // A fold into a dehydrated guard evicts what it folds.
                .map(|g| if g.dehydrated() { target } else { g.bytes() })
                .sum::<u64>()
        })
    }

    /// The keys an L0 fold routes into: L1's guard keys, or L0's shard lower
    /// bounds while L1 is empty. A guard key is a whole OPK key, compared as one
    /// byte string, so the partition is exact at every PK width.
    ///
    /// L0 rows above everything L1 holds get a guard of their own once they are
    /// worth half of one, so an ascending stream lands in fresh guards and not on
    /// the last one, which it would only push over target.
    fn l1_guard_keys(&self) -> Vec<PkBuf> {
        let l0 = &self.levels[L0];
        let Some(last) = self.levels[L1].guards.last() else {
            let mut keys: Vec<PkBuf> = l0.entries().map(|e| e.pk_min).collect();
            keys.sort_unstable();
            keys.dedup();
            return keys;
        };
        let top = last.key_extent().1.max(last.guard_key);
        let (mut above, mut rows, mut fence) = (0, 0, None::<PkBuf>);
        for e in l0.entries() {
            let (n, first) = (e.shard.row_count(), e.first_row_above(&top));
            rows += n;
            if first < n {
                above += n - first;
                let key = PkBuf::from_bytes(e.shard.get_pk_bytes(first));
                fence = Some(fence.map_or(key, |f| f.min(key)));
            }
        }
        // Row share stands in for byte share.
        let above_bytes = u128::from(l0.bytes()) * above as u128 / rows.max(1) as u128;
        let fence = fence.filter(|_| above_bytes >= u128::from(self.guard_target_bytes(L1) / 2));
        self.levels[L1]
            .guards
            .iter()
            .map(|g| g.guard_key)
            .chain(fence)
            .collect()
    }

    /// Hold `level`'s partition at its byte target. The split runs first: a merge
    /// must see the sizes it left.
    fn rebalance_guards(&mut self, level: usize) -> Result<(), StorageError> {
        self.split_overfull_guards(level)?;
        self.merge_underfull_guards(level)
    }

    /// Fold every guard in `level_idx` that is over the file threshold or its
    /// byte target, or whose retractions are expected to cancel
    /// [`CANCEL_PERCENT`] of its rows, cutting the byte-overfull ones at their own
    /// key quantiles — any other folds to one shard in place.
    fn split_overfull_guards(&mut self, level_idx: usize) -> Result<(), StorageError> {
        let target = self.guard_target_bytes(level_idx);
        // Descending, so each fold reshapes only indices above the guards still to go.
        for gi in (0..self.levels[level_idx].guards.len()).rev() {
            let guard = &self.levels[level_idx].guards[gi];
            let keys = fold_destinations(guard.guard_key, guard.entries.iter(), target);
            if keys.len() > 1
                || guard.entries.len() > GUARD_FILE_THRESHOLD
                || self.cancels(guard.retractions(), guard.rows())
            {
                self.compact(&[(level_idx, gi..gi + 1)], level_idx, &keys, CompactionKind::GuardSplit)?;
            }
        }
        Ok(())
    }

    /// Maximal runs of two or more adjacent guards whose combined bytes fit
    /// `bound`. A run also breaks at a change of representation, because folding
    /// a hydrated guard together with a dehydrated one would evict it (see
    /// [`merge_and_route`]).
    fn underfull_runs(&self, level_idx: usize, bound: u64) -> Vec<Range<usize>> {
        let guards = &self.levels[level_idx].guards;
        let mut runs = Vec::new();
        let (mut start, mut acc) = (0usize, 0u64);
        for (i, g) in guards.iter().enumerate() {
            let bytes = g.bytes();
            if i > start && (acc + bytes > bound || g.dehydrated() != guards[start].dehydrated()) {
                if i - start > 1 {
                    runs.push(start..i);
                }
                (start, acc) = (i, 0);
            }
            acc += bytes;
        }
        if guards.len() - start > 1 {
            runs.push(start..guards.len());
        }
        runs
    }

    /// Fold each underfull run into its lowest key, so the guard count follows
    /// the level's bytes down as well as up — the capacity sweep can shrink a
    /// guard by an order of magnitude in one rewrite.
    ///
    /// Half the target is the hysteresis: a merged run no larger than its inputs
    /// is under the split trigger.
    ///
    /// One pass stops once it has read `R`. A rise in `R` leaves a whole level
    /// underfull at once; later passes finish it.
    fn merge_underfull_guards(&mut self, level_idx: usize) -> Result<(), StorageError> {
        let bound = self.guard_target_bytes(level_idx) / 2;
        let mut budget = self.l0_run_bytes;
        // Descending, so each drain shifts only indices above the runs still to go.
        for run in self.underfull_runs(level_idx, bound).into_iter().rev() {
            if budget == 0 {
                break;
            }
            let guards = &self.levels[level_idx].guards[run.clone()];
            budget = budget.saturating_sub(guards.iter().map(LevelGuard::bytes).sum());
            let key = guards[0].guard_key;
            self.compact(&[(level_idx, run)], level_idx, &[key], CompactionKind::GuardMerge)?;
        }
        Ok(())
    }

    /// The sweep's first dehydration of a hydrated terminal guard: one skeleton
    /// shard in place, never split — the output is one row per key, so a part
    /// count derived from the hydrated input would shatter it.
    fn dehydrate_guard(&mut self, guard_idx: usize) -> Result<(), StorageError> {
        let key = self.levels[TERMINAL].guards[guard_idx].guard_key;
        self.compact(
            &[(TERMINAL, guard_idx..guard_idx + 1)],
            TERMINAL,
            &[key],
            CompactionKind::Dehydrate,
        )?;
        Ok(())
    }

    /// Evict a guard by **unlinking** it ([`ShardBudget::Drop`]): no output shard is
    /// written at all, so the sweep costs unlinks where a bounded view's costs a
    /// whole-guard rewrite.
    fn drop_guard(&mut self, guard_idx: usize) {
        let guard = self.levels[TERMINAL].guards.remove(guard_idx);
        let (_, hi) = guard.key_extent();
        self.dropped_max = self.dropped_max.max(hi);
        self.retire(guard.entries);
    }

    /// The keys an L1 guard is cut at before it is folded down: the low end of its
    /// key extent, then every terminal guard key inside the extent, so each band
    /// overlaps one terminal guard.
    fn vertical_band_keys(&self, src: usize) -> Vec<PkBuf> {
        let (lo, hi) = self.levels[L1].guards[src].key_extent();
        let terminal = &self.levels[TERMINAL];
        // `skip(1)`: the run's first guard owns `lo`, and every later key is above it.
        let run = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
        std::iter::once(lo)
            .chain(terminal.guards[run].iter().skip(1).map(|g| g.guard_key))
            .collect()
    }

    /// Fold L1 guard `src` down into the terminal level, banded so no single merge
    /// reads more than one band plus one terminal guard.
    ///
    /// Atomic per band, not per call: a failure in band *k* leaves bands `0..k`
    /// folded, every intermediate state a valid partition, and the next spill
    /// redoes the rest.
    fn vertical_fold(&mut self, src: usize) -> Result<(), StorageError> {
        let keys = self.vertical_band_keys(src);
        let bands = match keys.len() {
            1 => 1,
            _ => self.compact(&[(L1, src..src + 1)], L1, &keys, CompactionKind::BandCut)?,
        };
        // Each fold removes the band at `src`.
        for _ in 0..bands {
            let band = &self.levels[L1].guards[src];
            let (lo, hi) = band.key_extent();
            let terminal = &self.levels[TERMINAL];
            let dest = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
            debug_assert!(dest.len() <= 1, "a band overlaps one terminal guard");
            // A band above every row of the guard that owns its keys, or over an
            // empty level, starts a guard at its own low key.
            let owner = terminal.guards[dest.clone()]
                .first()
                .filter(|g| g.guard_key >= lo || g.key_extent().1 >= lo);
            if owner.is_none() && band.entries.len() == 1 {
                // One shard, already what the merge would write.
                let band = self.levels[L1].guards.remove(src);
                let fresh = self.levels[TERMINAL].get_or_create_guard(lo);
                debug_assert!(fresh.entries.is_empty(), "a fresh guard key names no guard");
                fresh.entries = band.entries;
                continue;
            }
            let key = owner.map_or(lo, |g| g.guard_key);
            // Cut where the output would be split. A skeleton output is one row
            // per key, so a part count from its hydrated inputs would shatter it.
            let keys = match owner {
                Some(g) if g.dehydrated() => vec![key],
                _ => {
                    let entries = band.entries.iter().chain(owner.into_iter().flat_map(|g| &g.entries));
                    fold_destinations(key, entries, self.guard_target_bytes(TERMINAL))
                }
            };
            let dest = if owner.is_some() { dest } else { dest.start..dest.start };
            self.compact(
                &[(L1, src..src + 1), (TERMINAL, dest)],
                TERMINAL,
                &keys,
                CompactionKind::Vertical,
            )?;
        }
        // Once, not per band: the bands were cut against the terminal partition
        // as it stood.
        self.rebalance_guards(TERMINAL)
    }

    /// Sum of every registered shard's file size — the quantity a
    /// capacity-bounded store is held under. The published superseded files
    /// awaiting the barrier's drain are on disk but not counted.
    fn resident_bytes(&self) -> u64 {
        self.all_entries().map(|e| e.shard.file_len()).sum()
    }

    /// Hold this store's registered shard bytes at or under `cap` by evicting
    /// terminal-level guards, oldest-written first — the only recency signal the
    /// tree carries, since nothing records that a row was *read*. An eviction
    /// leaves skeleton rows behind for a capacity-bounded view's output store and
    /// nothing at all for a delta store ([`ShardBudget::Drop`]).
    ///
    /// While over `cap`: evict the oldest hydrated terminal guard if there is
    /// one; otherwise push a level's worth of data down to make one — draining L1
    /// before refilling L0, because a spill has just refilled L0 at every trigger.
    /// The push-down is the expensive half, so it is budgeted to one per call and
    /// a store below its skeleton floor converges across spills instead of running
    /// the whole level down inside one trigger.
    ///
    /// Terminates: an eviction strictly decreases the hydrated terminal guard
    /// count (the guard count itself, when dropping) and nothing here re-hydrates;
    /// the push-down runs at most once. The fixpoint across calls is the skeleton
    /// floor — or, for a dropping store, an empty one, since a drop leaves no
    /// residue to stop at.
    fn enforce_capacity(&mut self) -> Result<(), StorageError> {
        let Some(cap) = self.budget.cap() else {
            return Ok(());
        };
        let mut pushed_down = false;
        while self.resident_bytes() > cap {
            // Step 1: the oldest-written hydrated terminal guard.
            let victim = self.levels[TERMINAL]
                .guards
                .iter()
                .enumerate()
                .filter(|(_, g)| !g.dehydrated())
                .min_by_key(|(_, g)| g.newest())
                .map(|(gi, _)| gi);
            if let Some(gi) = victim {
                match self.budget {
                    ShardBudget::Drop(_) => self.drop_guard(gi),
                    _ => self.dehydrate_guard(gi)?,
                }
                continue;
            }
            // Step 2: nothing left to dehydrate where it sits — push one level's
            // worth of data down, once per call.
            if std::mem::replace(&mut pushed_down, true) {
                break;
            }
            if let Some(gi) = self.cheapest_l1_guard_to_drain() {
                self.vertical_fold(gi)?;
            } else if !self.levels[L0].guards.is_empty() {
                self.run_compact()?;
            } else {
                // Step 3: everything is at the terminal level and dehydrated —
                // the skeleton floor, below which the sweep cannot go.
                break;
            }
        }
        Ok(())
    }

    /// Unlink every retired shard.
    pub(crate) fn unlink_retired(&mut self) {
        for seq in std::mem::take(&mut self.retired) {
            self.unlink_shard(seq);
        }
    }
}

#[cfg(test)]
#[path = "tests/index.rs"]
mod tests;
