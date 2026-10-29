//! What [`ShardIndex`] does with the tree its parent module declares: the one
//! shard writer and the appends through it, the PK probes and shard listings a
//! cursor opens over, publish tracking and retirement, and the upkeep
//! (`maintain`) with the folds it plans and runs — the L0→L1 fold, the byte
//! targets every level's guard partition is held at, and the vertical drain into
//! the terminal level.

use std::fs;
use std::ops::Range;
use std::rc::Rc;

use super::super::{manifest, Cut};
use super::{
    fold_destinations, CompactionKind, FLSMLevel, LevelGuard, ShardEntry, ShardIndex, CANCEL_PERCENT,
    GUARD_FILE_THRESHOLD, L0, L0_COMPACT_THRESHOLD, L1, MIN_GUARD_BYTES, SWEEP_STEPS, TERMINAL, UPKEEP_LEVY,
};
use crate::storage::pk_ranges_overlap;
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::ShardWriteOpts;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{merge_guard, Codebook, MappedShard};

/// One fold the tree owes: the shards it merges and where the merge goes.
struct Fold {
    kind: CompactionKind,
    /// The inputs, by seq, in merge order.
    sources: Vec<u64>,
    dest: usize,
    /// The destination guards, sorted and distinct.
    keys: Vec<PkBuf>,
}

/// A fold under way: its inputs stay registered and read, its outputs are
/// written and registered nowhere, until the last destination is written and
/// the two are exchanged.
pub(super) struct Running {
    fold: Fold,
    shards: Vec<Rc<MappedShard>>,
    /// Every output inherits the newest stamp over the inputs.
    newest: u64,
    /// The destination to merge next, and where its rows start in each input.
    next: usize,
    starts: Vec<usize>,
    opened: Vec<(PkBuf, ShardEntry)>,
}

impl ShardIndex {
    pub(super) fn all_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.settled_entries().chain(self.pending_at(Cut::Now))
    }

    /// The shards above the cut that `cut` reads.
    fn pending_at(&self, cut: Cut) -> &[ShardEntry] {
        match cut {
            Cut::Now => &self.pending,
            Cut::Sealed => &[],
        }
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

    /// Write `run`, a run spilled into the store, as an unpublished shard named
    /// by a fresh seq, its heap coded under the codebook [`Self::codebook_for`]
    /// picks for it, if any.
    fn write_shard(&mut self, run: &Batch) -> Result<ShardEntry, StorageError> {
        let codebook = self.codebook_for(run);
        self.write_with(None, |path, opts| {
            run.write_as_shard(path, ShardWriteOpts { codebook: codebook.as_deref(), ..opts })
        })
    }

    /// Write a shard named by a fresh seq through `write`, handed its path and
    /// the store's options, and open it. `newest` defaults to that seq.
    fn write_with(
        &mut self,
        newest: Option<u64>,
        write: impl FnOnce(&str, ShardWriteOpts<'_>) -> Result<(), StorageError>,
    ) -> Result<ShardEntry, StorageError> {
        self.shard_seq += 1;
        let seq = self.shard_seq;
        let path = manifest::shard_path(&self.output_dir, seq);
        let opts = ShardWriteOpts {
            skip_pk_filter: self.skip_pk_filter,
            ..Default::default()
        };
        write(&path, opts)
            .and_then(|()| ShardEntry::open(&self.output_dir, seq, &self.schema, newest.unwrap_or(seq)))
            .inspect_err(|_| self.unlink_shard(seq))
    }

    /// The codebook `run` is coded under, adopted as the store's own: none in a
    /// store that codes no heap, or for a run too small to code.
    fn codebook_for(&mut self, run: &Batch) -> Option<Rc<Codebook>> {
        if !self.compress {
            return None;
        }
        let codebook = Codebook::for_batch(self.codebook.as_ref(), run)?;
        self.codebook = Some(Rc::clone(&codebook));
        Some(codebook)
    }

    /// Unlink the shard drawn at `seq`, best-effort.
    fn unlink_shard(&self, seq: u64) {
        let _ = fs::remove_file(manifest::shard_path(&self.output_dir, seq));
    }

    /// Append `run` to L0 as one unpublished shard: the spill.
    pub(crate) fn append_l0_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        let entry = self.write_shard(run)?;
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        self.levels[L0].get_or_create_guard(whole).entries.push(entry);
        self.owed = true;
        Ok(())
    }

    /// Write `run`, rows above the store's cut, as one unpublished shard outside
    /// every level.
    pub(crate) fn append_pending_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        let entry = self.write_shard(run)?;
        self.pending.push(entry);
        Ok(())
    }

    /// Whether any shard sits above the cut.
    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// The least key a shard above the cut holds.
    pub(crate) fn pending_least(&self) -> Option<&[u8]> {
        self.pending.iter().map(|e| e.pk_min.pk_bytes()).min()
    }

    /// Move the cut past every pending shard: each enters L0 as it stands.
    /// Answers their shards.
    pub(crate) fn seal_pending(&mut self) -> Vec<Rc<MappedShard>> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let pending = std::mem::take(&mut self.pending);
        let shards = pending.iter().map(|e| Rc::clone(&e.shard)).collect();
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        self.levels[L0].get_or_create_guard(whole).entries.extend(pending);
        self.owed = true;
        shards
    }

    /// Whether the tree may owe a fold.
    #[cfg(test)]
    pub(crate) fn owed(&self) -> bool {
        self.owed
    }

    /// Pay the upkeep for `ingested` bytes: [`UPKEEP_LEVY`] bytes of fold input
    /// for each, less what earlier calls overdrew. A store nothing is ingested
    /// into keeps what it owes.
    pub(crate) fn upkeep(&mut self, ingested: usize) -> Result<(), StorageError> {
        if !self.owed {
            return Ok(());
        }
        let levy = ingested as u64 * UPKEEP_LEVY;
        let read = self.maintain(levy.saturating_sub(self.upkeep_overdraft))?;
        self.upkeep_overdraft = (self.upkeep_overdraft + read).saturating_sub(levy);
        Ok(())
    }

    /// The disk tier's upkeep: the folds the tree owes, one destination at a
    /// time, until none is left or `budget` input bytes are merged. A fold is
    /// resumed where the last call left it, so a call is held for one
    /// destination past its budget at most. Answers the input bytes merged.
    pub(crate) fn maintain(&mut self, budget: u64) -> Result<u64, StorageError> {
        let mut read = 0;
        while self.owed && read < budget {
            if self.running.is_none() {
                match self.plan() {
                    Some(fold) => self.begin(fold)?,
                    None => self.owed = false,
                }
                continue;
            }
            read += self.step()?;
        }
        Ok(read)
    }

    /// Finish the fold under way, if any: a manifest names a tree, and a
    /// half-written fold's outputs are in none.
    pub(crate) fn finish_fold(&mut self) -> Result<(), StorageError> {
        while self.running.is_some() {
            self.step()?;
        }
        Ok(())
    }

    /// Give up the fold under way and what it wrote; its inputs were never
    /// touched.
    pub(super) fn abandon_fold(&mut self) {
        self.bands.clear();
        if let Some(run) = self.running.take() {
            self.retire(run.opened.into_iter().map(|(_, entry)| entry));
        }
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
        debug_assert!(
            self.running.is_none() && self.bands.is_empty(),
            "a terminal run enters a store with no fold under way"
        );
        self.owed = true;
        let entry = self.write_shard(run)?;
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
        debug_assert!(self.running.is_none(), "a fold under way holds seqs no manifest names");
        self.published_through = self.shard_seq;
    }

    /// Every shard `cut` reads: the pending ones only at [`Cut::Now`].
    pub(crate) fn shard_arcs(&self, cut: Cut) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.settled_entries()
            .chain(self.pending_at(cut))
            .map(|e| Rc::clone(&e.shard))
    }

    /// Every shard `cut` reads whose PK extent meets `[lo, hi]`.
    pub(crate) fn shard_arcs_in_range(
        &self,
        lo: PkBuf,
        hi: PkBuf,
        cut: Cut,
    ) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.levels
            .iter()
            .flat_map(move |level| {
                let run = level.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
                level.guards[run].iter().flat_map(|g| g.entries.iter())
            })
            .chain(self.pending_at(cut))
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
        self.capacity.is_some() && self.levels[TERMINAL].guards.iter().any(LevelGuard::dehydrated)
    }

    /// Visit each shard holding OPK `key`, with its rows of that key;
    /// `filter_key` is `key`'s `probe_key`. Each level routes by the whole key
    /// against its guard partition.
    pub(crate) fn find_pk_bytes(
        &self,
        key: &[u8],
        filter_key: u64,
        mut visitor: impl FnMut(&Rc<MappedShard>, Range<usize>),
    ) {
        for level in &self.levels {
            let Some(guard) = level.guards.get(level.slot(key)) else {
                continue;
            };
            for e in &guard.entries {
                if let Some(rows) = e.probe_pk_bytes(key, filter_key) {
                    visitor(&e.shard, rows);
                }
            }
        }
        for e in &self.pending {
            if let Some(rows) = e.probe_pk_bytes(key, filter_key) {
                visitor(&e.shard, rows);
            }
        }
    }

    /// Start `fold`: pin its inputs and verify them.
    fn begin(&mut self, fold: Fold) -> Result<(), StorageError> {
        let shards: Vec<&ShardEntry> = fold
            .sources
            .iter()
            .map(|seq| {
                self.settled_entries()
                    .find(|e| e.seq == *seq)
                    .expect("a fold names registered shards")
            })
            .collect();
        for e in &shards {
            e.shard.verify_body()?;
        }
        let newest = shards.iter().map(|e| e.newest).max().unwrap_or(0);
        let shards: Vec<Rc<MappedShard>> = shards.iter().map(|e| Rc::clone(&e.shard)).collect();
        self.running = Some(Running {
            starts: vec![0; shards.len()],
            shards,
            newest,
            next: 0,
            opened: Vec::with_capacity(fold.keys.len()),
            fold,
        });
        Ok(())
    }

    /// Merge the next destination of the fold under way and write its shard;
    /// after the last, exchange the fold's inputs for its outputs.
    ///
    /// A failure registers nothing and unlinks every output the fold wrote.
    /// Answers the input bytes it merged.
    fn step(&mut self) -> Result<u64, StorageError> {
        let mut run = self.running.take().expect("a fold is under way");
        let before: usize = run.starts.iter().sum();
        let dehydrate = run.fold.kind == CompactionKind::Dehydrate;
        let codebook = self.codebook.clone();
        let merged = merge_guard(
            &run.shards,
            &mut run.starts,
            run.fold.keys.get(run.next + 1).map(PkBuf::pk_bytes),
            dehydrate,
            &self.schema,
            codebook.as_deref(),
        );
        // Row share stands in for byte share.
        let (rows, bytes): (usize, u64) = run
            .shards
            .iter()
            .fold((0, 0), |(r, b), s| (r + s.row_count(), b + s.file_len()));
        let merged_rows = (run.starts.iter().sum::<usize>() - before) as u128;
        let read = (u128::from(bytes) * merged_rows / rows.max(1) as u128) as u64 + 1;
        if let Some(merged) = merged {
            // A store that has coded no heap yet codes the run under one of its own.
            let fresh = merged.plain().and_then(|b| self.codebook_for(b));
            let written = self.write_with(Some(run.newest), |path, opts| {
                merged.write_as_shard(path, ShardWriteOpts { codebook: fresh.as_deref(), ..opts })
            });
            match written {
                Ok(entry) => run.opened.push((run.fold.keys[run.next], entry)),
                Err(e) => {
                    self.running = Some(run);
                    self.abandon_fold();
                    return Err(e);
                }
            }
        }
        run.next += 1;
        if run.next < run.fold.keys.len() {
            self.running = Some(run);
        } else {
            self.install(run);
        }
        Ok(read)
    }

    /// Retire the inputs of a finished fold and register its outputs under
    /// their guards.
    fn install(&mut self, run: Running) {
        let Running { fold, shards, opened, .. } = run;
        if fold.kind == CompactionKind::L0Fold {
            // A run that lands in one guard is one shard whose encodings span
            // the whole run, and can outweigh the spills it was written from.
            // Unobserved, every such guard would be over target as written and
            // be rewritten at once to be cut in two.
            let largest = opened.iter().map(|(_, e)| e.shard.file_len()).max();
            self.l0_run_bytes = self.l0_run_bytes.max(largest.unwrap_or(0));
        }
        if fold.kind == CompactionKind::GuardSplit {
            // The one fold that reads a guard's whole history at its level, and so
            // every row its retractions can cancel there.
            let read: usize = shards.iter().map(|s| s.row_count()).sum();
            let wrote: usize = opened.iter().map(|(_, e)| e.shard.row_count()).sum();
            // Its inputs are the guard's shards in order, and the oldest has no
            // older one there to retract from.
            let retractions = shards.iter().skip(1).map(|s| s.retraction_rows()).sum();
            self.cancel_yield.observe(retractions, read.saturating_sub(wrote));
        }
        #[cfg(test)]
        super::cstats::record(
            fold.kind,
            shards.iter().map(|s| s.file_len()).sum(),
            opened.iter().map(|(_, e)| e.shard.file_len()).sum(),
            shards.len(),
        );

        let mut superseded = Vec::with_capacity(fold.sources.len());
        for level in &mut self.levels {
            for guard in &mut level.guards {
                superseded.extend(guard.entries.extract_if(.., |e| fold.sources.contains(&e.seq)));
            }
            level.guards.retain(|g| !g.entries.is_empty());
        }
        self.retire(superseded);
        if fold.kind == CompactionKind::BandCut {
            // Ascending from the back, so the bands fold down in key order.
            self.bands = opened.iter().rev().map(|(key, _)| *key).collect();
        }
        for (key, entry) in opened {
            let guard = self.levels[fold.dest].get_or_create_guard(key);
            guard.entries.push(entry);
            debug_assert!(
                fold.dest != TERMINAL || guard.entries.len() == 1,
                "a terminal guard holds exactly one shard"
            );
        }
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
        self.abandon_fold();
        let levels = std::mem::take(&mut self.levels);
        let settled = levels.into_iter().flat_map(|l| l.guards).flat_map(|g| g.entries);
        let pending = std::mem::take(&mut self.pending);
        self.retire(settled.chain(pending));
    }

    /// The next fold the tree owes, or `None` when it owes none. What costs no
    /// merge — a band moved down whole — is done here.
    ///
    /// In order: the bands of a cut L1 guard, which were cut against the
    /// terminal partition as it stood; every guard over its byte target or file
    /// threshold, before the L0 fold lands more on it; the L0 fold; the
    /// underfull runs, which must see the sizes a split left; L1 down to its
    /// byte target; and the store down to its capacity.
    fn plan(&mut self) -> Option<Fold> {
        loop {
            while let Some(band) = self.bands.pop() {
                if let Some(fold) = self.plan_band(band) {
                    return Some(fold);
                }
            }
            let split = [L1, TERMINAL].into_iter().find_map(|level| self.plan_split(level));
            if split.is_some() {
                return split;
            }
            if self.levels[L0].entries().count() > L0_COMPACT_THRESHOLD || self.l0_cancels() {
                return Some(self.plan_l0_fold());
            }
            let merge = [L1, TERMINAL].into_iter().find_map(|level| self.plan_merge(level));
            if merge.is_some() {
                return merge;
            }
            if self.levels[L1].bytes() <= self.l1_target_bytes() {
                self.capacity.filter(|&cap| self.resident_bytes() > cap)?;
                // The oldest-written hydrated terminal guard: the only recency
                // signal the tree carries, since nothing records that a row was read.
                let victim = self.levels[TERMINAL]
                    .guards
                    .iter()
                    .enumerate()
                    .filter(|(_, g)| !g.dehydrated())
                    .min_by_key(|(_, g)| g.entries.iter().map(|e| e.newest).max())
                    .map(|(gi, _)| gi);
                if let Some(gi) = victim {
                    return Some(self.plan_dehydration(gi));
                }
                // Nothing left to evict where it sits: push a level's worth of
                // data down to make one, L1 before L0.
            }
            match self.cheapest_l1_guard_to_drain() {
                Some(gi) => {
                    if let Some(fold) = self.plan_drain(gi) {
                        return Some(fold);
                    }
                }
                None if !self.levels[L0].guards.is_empty() => return Some(self.plan_l0_fold()),
                // Everything is at the terminal level and dehydrated: the
                // skeleton floor, below which a sweep cannot go, however far
                // over its capacity the store is.
                None => return None,
            }
        }
    }

    /// Every shard of `guards`, in guard order.
    fn seqs<'a>(guards: impl IntoIterator<Item = &'a LevelGuard>) -> Vec<u64> {
        guards.into_iter().flat_map(|g| &g.entries).map(|e| e.seq).collect()
    }

    /// All of L0 into L1's guards. Observing `R` first is what makes the targets
    /// reflect the fold this plans.
    fn plan_l0_fold(&mut self) -> Fold {
        self.l0_run_bytes = self.l0_run_bytes.max(self.levels[L0].bytes());
        Fold {
            kind: CompactionKind::L0Fold,
            sources: Self::seqs(&self.levels[L0].guards),
            dest: L1,
            keys: self.l1_guard_keys(),
        }
    }

    /// The largest a guard of `level_idx` is allowed to get: `R`, so no
    /// compaction's input grows with the dataset — every fold reads at most two
    /// guards' worth.
    ///
    /// A budgeted store's terminal level takes one sweep step instead, since that
    /// is the granularity the capacity sweep evicts at. The clamp keeps a very
    /// large or very small `capacity` from naming a target outside
    /// `[MIN_GUARD_BYTES, R]`.
    fn guard_target_bytes(&self, level_idx: usize) -> u64 {
        match self.capacity {
            Some(cap) if level_idx == TERMINAL => (cap / SWEEP_STEPS).clamp(MIN_GUARD_BYTES, self.l0_run_bytes),
            _ => self.l0_run_bytes,
        }
    }

    /// Bytes L1 is drained to. Capped at two sweep steps for a budgeted store:
    /// the capacity sweep cannot evict from L1, so bytes parked there come out of
    /// what the user asked for.
    fn l1_target_bytes(&self) -> u64 {
        let target = Self::balanced_l1_target(self.levels[TERMINAL].bytes(), self.l0_run_bytes);
        match self.capacity {
            Some(cap) => target.min(2 * (cap / SWEEP_STEPS)),
            None => target,
        }
    }

    /// `2√(|L2|·R)`, a chosen size and not a derived optimum: a larger L1 saves
    /// vertical rewrites and holds more shards for a scan to merge. The `16 R`
    /// floor under it is chosen too: it keeps a small store from draining L1 on
    /// every spill.
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
            let (n, first) = (e.shard.row_count(), e.shard.pk_rows(top.pk_bytes()).end);
            rows += n;
            if first < n {
                above += n - first;
                let key = e.shard.pk_at(first);
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

    /// The highest guard in `level_idx` that is over its byte target, or whose
    /// retractions are expected to cancel [`CANCEL_PERCENT`] of its rows — cut
    /// at its own key quantiles, or folded to one shard in place — or else over
    /// the file threshold, whose newest shards fold into one: the two newest,
    /// then each older one no larger than the shards already taken. A shard is
    /// rewritten once the ones above it have grown to its size, so a byte is
    /// rewritten a logarithmic number of times over the guard's growth; folding
    /// the whole guard each time would rewrite its oldest shard at every fold.
    fn plan_split(&self, level_idx: usize) -> Option<Fold> {
        self.levels[level_idx]
            .guards
            .iter()
            .rev()
            .find_map(|guard| self.split_of(level_idx, guard))
    }

    /// [`Self::plan_split`] for the one guard.
    fn split_of(&self, level_idx: usize, guard: &LevelGuard) -> Option<Fold> {
        let target = self.guard_target_bytes(level_idx);
        let keys = fold_destinations(guard.guard_key, guard.entries.iter(), target);
        let mut start = 0;
        if keys.len() == 1 && !self.cancels(guard.retractions(), guard.rows()) {
            if guard.entries.len() <= GUARD_FILE_THRESHOLD {
                return None;
            }
            debug_assert!(!guard.dehydrated(), "a skeleton shard is folded with its whole guard");
            let bytes = |e: &ShardEntry| e.shard.file_len();
            start = guard.entries.len() - 2;
            let mut taken: u64 = guard.entries[start..].iter().map(bytes).sum();
            while start > 0 && bytes(&guard.entries[start - 1]) <= taken {
                start -= 1;
                taken += bytes(&guard.entries[start]);
            }
        }
        Some(Fold {
            kind: match start {
                0 => CompactionKind::GuardSplit,
                _ => CompactionKind::TierFold,
            },
            sources: guard.entries[start..].iter().map(|e| e.seq).collect(),
            dest: level_idx,
            keys,
        })
    }

    /// The highest underfull run of `level_idx`, folded into its lowest key, so
    /// the guard count follows the level's bytes down as well as up — the
    /// capacity sweep can shrink a guard by an order of magnitude in one rewrite.
    ///
    /// The runs are the maximal runs of two or more adjacent guards whose
    /// combined bytes fit half the target. A run also breaks at a change of
    /// representation, because folding a hydrated guard together with a
    /// dehydrated one would evict it (see [`merge_guard`]).
    ///
    /// Half the target is the hysteresis: a merged run no larger than its inputs
    /// is under the split trigger.
    fn plan_merge(&self, level_idx: usize) -> Option<Fold> {
        let bound = self.guard_target_bytes(level_idx) / 2;
        let guards = &self.levels[level_idx].guards;
        let (mut start, mut acc, mut highest) = (0usize, 0u64, None);
        for (i, g) in guards.iter().enumerate() {
            let bytes = g.bytes();
            if i > start && (acc + bytes > bound || g.dehydrated() != guards[start].dehydrated()) {
                if i - start > 1 {
                    highest = Some(start..i);
                }
                (start, acc) = (i, 0);
            }
            acc += bytes;
        }
        if guards.len() - start > 1 {
            highest = Some(start..guards.len());
        }
        let guards = &guards[highest?];
        Some(Fold {
            kind: CompactionKind::GuardMerge,
            sources: Self::seqs(guards),
            dest: level_idx,
            keys: vec![guards[0].guard_key],
        })
    }

    /// The sweep's first dehydration of a hydrated terminal guard: one skeleton
    /// shard in place, never split — the output is one row per key, so a part
    /// count derived from the hydrated input would shatter it.
    fn plan_dehydration(&self, guard_idx: usize) -> Fold {
        let guard = &self.levels[TERMINAL].guards[guard_idx];
        Fold {
            kind: CompactionKind::Dehydrate,
            sources: Self::seqs([guard]),
            dest: TERMINAL,
            keys: vec![guard.guard_key],
        }
    }

    /// Start L1 guard `src` on its way down into the terminal level, banded so
    /// no single merge reads more than one band plus one terminal guard: the
    /// fold that cuts it at the low end of its key extent and at every terminal
    /// guard key inside the extent. A guard that overlaps one terminal guard
    /// already is a band, and goes down as [`Self::plan_band`] plans it.
    ///
    /// Each band folds down as a fold of its own, so a failure in band *k*
    /// leaves bands `0..k` folded and every intermediate state a valid
    /// partition.
    fn plan_drain(&mut self, src: usize) -> Option<Fold> {
        let guard = &self.levels[L1].guards[src];
        let (lo, hi) = guard.key_extent();
        let terminal = &self.levels[TERMINAL];
        let run = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
        if run.len() <= 1 {
            return self.plan_band(guard.guard_key);
        }
        Some(Fold {
            kind: CompactionKind::BandCut,
            sources: Self::seqs([guard]),
            dest: L1,
            // `skip(1)`: the run's first guard owns `lo`, and every later key is above it.
            keys: std::iter::once(lo)
                .chain(terminal.guards[run].iter().skip(1).map(|g| g.guard_key))
                .collect(),
        })
    }

    /// Fold the L1 band keyed `band` into the terminal guard it overlaps, or
    /// move it down as it stands and answer `None`.
    fn plan_band(&mut self, band: PkBuf) -> Option<Fold> {
        let l1 = &self.levels[L1].guards;
        let src = l1.binary_search_by(|g| g.guard_key.cmp(&band)).ok()?;
        let (lo, hi) = l1[src].key_extent();
        let terminal = &self.levels[TERMINAL];
        let dest = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
        debug_assert!(dest.len() <= 1, "a band overlaps one terminal guard");
        // A band above every row of the guard that owns its keys, or over an
        // empty level, starts a guard at its own low key.
        let owner = terminal.guards[dest]
            .first()
            .filter(|g| g.guard_key >= lo || g.key_extent().1 >= lo);
        if owner.is_none() && l1[src].entries.len() == 1 {
            // One shard, already what the merge would write.
            let band = self.levels[L1].guards.remove(src);
            let fresh = self.levels[TERMINAL].get_or_create_guard(lo);
            debug_assert!(fresh.entries.is_empty(), "a fresh guard key names no guard");
            fresh.entries = band.entries;
            return None;
        }
        let key = owner.map_or(lo, |g| g.guard_key);
        let inputs = l1[src].entries.iter().chain(owner.into_iter().flat_map(|g| &g.entries));
        Some(Fold {
            kind: CompactionKind::Vertical,
            sources: inputs.clone().map(|e| e.seq).collect(),
            dest: TERMINAL,
            // Cut where the output would be split. A skeleton output is one row
            // per key, so a part count from its hydrated inputs would shatter it.
            keys: match owner {
                Some(g) if g.dehydrated() => vec![key],
                _ => fold_destinations(key, inputs, self.guard_target_bytes(TERMINAL)),
            },
        })
    }

    /// Sum of every registered shard's file size — the quantity a
    /// capacity-bounded store is held under. The published superseded files
    /// awaiting the barrier's drain are on disk but not counted.
    fn resident_bytes(&self) -> u64 {
        self.all_entries().map(|e| e.shard.file_len()).sum()
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

#[cfg(test)]
#[path = "benches/index.rs"]
mod bench;
