//! In-memory FLSM index state + the compaction trigger/orchestration for
//! [`ShardIndex`]: shard insertion, L0 sort, PK probes, the `should_compact`
//! trigger, and `run_compact` — the L0→L1 fold, the byte targets every level's
//! guard partition is held at, and the vertical drain into the terminal level.

use std::cmp::Ordering;
use std::ffi::CStr;
use std::fs;
use std::rc::Rc;

use super::super::compact;
use super::super::error::StorageError;
use super::super::shard_reader::MappedShard;
use super::{
    to_cstrings, CompactionInputs, CompactionKind, FLSMLevel, LevelGuard, ShardEntry, ShardIndex, GUARD_FILE_THRESHOLD,
    L0_COMPACT_THRESHOLD, LMAX_FILE_THRESHOLD, MIN_GUARD_BYTES, SWEEP_STEPS, TERMINAL_LEVEL_IDX,
};

impl ShardIndex {
    pub(super) fn all_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.l0.iter().chain(
            self.levels
                .iter()
                .flat_map(|l| l.guards.iter().flat_map(|g| g.entries.iter())),
        )
    }

    /// Mutable twin of [`all_entries`](Self::all_entries), in the same order, so
    /// the schema swap's commit half can assign re-opened entries back into the
    /// slots its prepare half walked.
    pub(super) fn all_entries_mut(&mut self) -> impl Iterator<Item = &mut ShardEntry> {
        self.l0.iter_mut().chain(
            self.levels
                .iter_mut()
                .flat_map(|l| l.guards.iter_mut().flat_map(|g| g.entries.iter_mut())),
        )
    }

    pub(super) fn level_num(level_idx: usize) -> usize {
        level_idx + 1
    }

    /// Open `path` and insert it into the sorted L0 tier. The entry registers
    /// unswept, so it is in the next barrier's fdatasync sweep by construction.
    pub(crate) fn add_unsynced_shard(&mut self, path: &str, max_lsn: u64) -> Result<(), StorageError> {
        let entry = ShardEntry::open(path, &self.schema, max_lsn, false)?;
        self.l0.push(entry);
        self.sort_l0();
        Ok(())
    }

    pub(super) fn sort_l0(&mut self) {
        // OPK bytes are order-preserving, so a single byte-wise comparison of
        // `pk_min` sorts L0 at every PK width. `false < true` sinks is_empty
        // entries to the end.
        self.l0.sort_by(|a, b| match (a.is_empty(), b.is_empty()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => a.pk_min.pk_bytes().cmp(b.pk_min.pk_bytes()),
        });
    }

    /// Derived, not cached: the L0 tier crossed its compaction threshold.
    pub(crate) fn should_compact(&self) -> bool {
        self.l0.len() > L0_COMPACT_THRESHOLD
    }

    /// The paths `flush_prepare` hands the barrier as its fdatasync sweep list:
    /// every live shard an fdatasync has not yet reached.
    pub(crate) fn unsynced_paths(&self) -> impl Iterator<Item = &str> {
        self.all_entries().filter(|e| !e.synced).map(|e| e.filename.as_str())
    }

    /// Mark every live shard durable, once the barrier has fdatasync'd the sweep
    /// list and renamed the manifest that references them.
    pub(crate) fn clear_unsynced(&mut self) {
        for e in self.all_entries_mut() {
            e.synced = true;
        }
    }

    /// Every live shard's `Rc`, yielded lazily — callers `extend` without an
    /// intermediate `Vec` (the per-worker cursor gather).
    pub(crate) fn all_shard_arcs_iter(&self) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.all_entries().map(|e| Rc::clone(&e.shard))
    }

    #[cfg(test)]
    pub(crate) fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.all_shard_arcs_iter().collect()
    }

    /// Every shard that can hold a key in `[start, end]`, `end` `None` meaning
    /// the top of the key space. Complete because guards partition the key line:
    /// a key is reachable from exactly one guard per level.
    pub(crate) fn shard_arcs_in_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        let lo = crate::schema::key::pack_pk_be(start);
        let hi = end.map_or(u128::MAX, crate::schema::key::pack_pk_be);
        let pack = |k: &crate::schema::key::PkBuf| crate::schema::key::pack_pk_be(k.pk_bytes());
        let l0 = self
            .l0
            .iter()
            .filter(move |e| !e.is_empty() && pack(&e.pk_max) >= lo && pack(&e.pk_min) <= hi)
            .map(|e| Rc::clone(&e.shard));
        let deep = self.levels.iter().flat_map(move |level| {
            let run = level.find_guards_for_range(lo, hi);
            level.guards[run]
                .iter()
                .flat_map(|g| g.entries.iter().map(|e| Rc::clone(&e.shard)))
        });
        l0.chain(deep)
    }

    /// Raw rows across every live shard, summed without touching an `Rc`. Raw:
    /// cross-shard duplicates and ghosts are counted, so it is an upper bound on
    /// the live rows a walk would emit — the shape the selectivity gate wants.
    pub(crate) fn total_rows(&self) -> usize {
        self.all_entries().map(|e| e.shard.count).sum()
    }

    /// Whether any registered shard is a skeleton — i.e. whether a read of this
    /// store can meet a `(PK, coarse weight)` row it has to hydrate.
    ///
    /// A budget is necessary but not sufficient: only `dehydrate_guard` writes a
    /// skeleton and only `enforce_capacity` reaches it, but a bounded view under
    /// its cap has never dehydrated and reads exactly like an unbounded one. So
    /// the answer is still derived from the shards — over the terminal level
    /// alone, the only one a skeleton is ever written into. Every read verb asks
    /// this, so the unbounded store answers it without touching a shard.
    pub(crate) fn has_skeleton_shard(&self) -> bool {
        self.capacity_bytes.is_some()
            && self.levels.get(TERMINAL_LEVEL_IDX).is_some_and(|l| {
                l.guards
                    .iter()
                    .flat_map(|g| g.entries.iter())
                    .any(|e| e.shard.is_skeleton())
            })
    }

    /// Test-only u128 oracle: OPK-encodes a **native** PK value (handling
    /// signed/compound columns) and delegates to [`find_pk_bytes`], the
    /// production path. Wide PKs cannot fit a u128.
    #[cfg(test)]
    pub(crate) fn find_pk(&self, key: u128, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        let filter_key = crate::schema::key::probe_key(opk.pk_bytes());
        self.find_pk_bytes(opk.pk_bytes(), filter_key, visitor);
    }

    /// Point lookup by OPK `key` bytes — universal across all PK widths. L0 is
    /// scanned (range-rejected per entry); each L1+ level routes by the guard
    /// key `pack_pk_be(key)` (the same order-preserving space `l1_guard_keys`
    /// builds), restoring O(log N) routing for wide PKs too.
    pub(crate) fn find_pk_bytes(&self, key: &[u8], filter_key: u64, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        // A pure function of `key`, so the sweep derives it once instead of once
        // per candidate shard.
        let route_key = crate::schema::key::pack_pk_be(key);
        for e in &self.l0 {
            if let Some((arc, idx)) = e.probe_pk_bytes(key, filter_key) {
                visitor(arc, idx);
            }
        }
        for level in &self.levels {
            if let Some(g_idx) = level.find_guard_idx(route_key) {
                for e in &level.guards[g_idx].entries {
                    if let Some((arc, idx)) = e.probe_pk_bytes(key, filter_key) {
                        visitor(arc, idx);
                    }
                }
            }
        }
    }

    pub(crate) fn max_lsn(&self) -> u64 {
        self.all_entries().map(|e| e.max_lsn).max().unwrap_or(0)
    }

    /// Grow `levels` so `level_idx` is addressable. Indexed from 0; the 1-based
    /// level *number* lives only at the two serde boundaries that carry it (the
    /// shard filename and the manifest `level` field).
    pub(super) fn ensure_level(&mut self, level_idx: usize) {
        while self.levels.len() <= level_idx {
            self.levels.push(FLSMLevel::new());
        }
    }

    /// Advance and return the compaction counter. Every output-emitting
    /// compaction draws a fresh value — persisted via the manifest header — so
    /// no two output shards ever share a basename over the table's lifetime,
    /// even across restarts.
    fn next_compact_seq(&mut self) -> u64 {
        self.compact_seq += 1;
        self.compact_seq
    }

    /// Open every just-compacted output shard. On any failure, unlink all
    /// outputs so a failed compaction leaves no orphan on disk for the running
    /// session; callers mutate index state only after every open succeeded.
    fn open_outputs(&self, outputs: &[(u128, String)], max_lsn: u64) -> Result<Vec<(u128, ShardEntry)>, StorageError> {
        let mut opened = Vec::with_capacity(outputs.len());
        for (gk, filename) in outputs {
            // Unswept, like any other new shard: a crash before the sweep leaves
            // the last-published manifest on the still-present inputs, so an
            // unswept output is only ever an orphan.
            match ShardEntry::open(filename, &self.schema, max_lsn, false) {
                Ok(entry) => opened.push((*gk, entry)),
                Err(e) => {
                    for (_, f) in outputs {
                        let _ = fs::remove_file(f);
                    }
                    return Err(e);
                }
            }
        }
        Ok(opened)
    }

    /// One compaction's input set, gathered in a single walk of `entries`: their
    /// paths, their LSN watermark, and their registered bytes.
    ///
    /// The watermark is one derivation because `Table::new` seeds
    /// `current_lsn = max_lsn() + 1`: a watermark below an input's would let a
    /// later spill reuse a live shard's name.
    fn compaction_inputs<'a>(entries: impl IntoIterator<Item = &'a ShardEntry>) -> CompactionInputs {
        let mut inputs = CompactionInputs::default();
        for e in entries {
            inputs.files.push(e.filename.clone());
            inputs.max_lsn = inputs.max_lsn.max(e.max_lsn);
            inputs.bytes += e.shard.file_len();
        }
        inputs
    }

    /// The one compaction driver: merge `inputs` into `dest_idx`, routed across
    /// `guard_keys`, then release the entries they came from via `drop_sources`.
    ///
    /// A destination guard is written skeleton if it is already dehydrated —
    /// ordinary compaction never re-hydrates what the sweep evicted — or if
    /// `force_skeleton` says so, which is how a dehydration starts. Derived per
    /// destination, because a vertical routes into every terminal guard its key
    /// range overlaps while the sweep dehydrates one at a time.
    ///
    /// Nothing is mutated until every output shard has been written *and*
    /// reopened, so a failure leaves the index exactly as it was and the caller
    /// can retry against the untouched source tier.
    fn compact_into(
        &mut self,
        inputs: CompactionInputs,
        guard_keys: &[u128],
        dest_idx: usize,
        force_skeleton: bool,
        kind: CompactionKind,
        drop_sources: impl FnOnce(&mut Self),
    ) -> Result<(), StorageError> {
        let CompactionInputs {
            files,
            max_lsn,
            bytes: in_bytes,
        } = inputs;
        let compact_seq = self.next_compact_seq();
        let cstrings = to_cstrings(&files)?;
        let cstrs: Vec<&CStr> = cstrings.iter().map(|c| c.as_c_str()).collect();

        // Only terminal-level guards are ever dehydrated.
        let dest = (dest_idx == TERMINAL_LEVEL_IDX)
            .then(|| self.levels.get(dest_idx))
            .flatten();
        let guards: Vec<(u128, bool)> = guard_keys
            .iter()
            .map(|&gk| {
                let skeleton = force_skeleton
                    || dest.is_some_and(|l| l.find_exact_guard(gk).is_some_and(|gi| l.guards[gi].dehydrated()));
                (gk, skeleton)
            })
            .collect();

        let outputs = compact::merge_and_route(
            &cstrs,
            &guards,
            &self.schema,
            compact::Output {
                dir: &self.output_dir,
                table_id: self.table_id,
                level_num: Self::level_num(dest_idx) as u32,
                compact_seq,
                skip_pk_filter: self.skip_pk_filter,
            },
        )?;
        let opened = self.open_outputs(&outputs, max_lsn)?;
        super::cstats::record(
            kind,
            in_bytes,
            opened.iter().map(|(_, e)| e.shard.file_len()).sum(),
            files.len(),
        );

        // The superseded inputs are the sole writer of `pending_deletions`. Each
        // leaves the index carrying its own sweep flag, so an unpublished spill
        // among them stops being swept with nothing having to prune it.
        self.pending_deletions.extend(files);
        drop_sources(self);
        self.ensure_level(dest_idx);
        for (gk, entry) in opened {
            self.levels[dest_idx].get_or_create_guard(gk).entries.push(entry);
        }
        self.unlink_superseded_now();
        Ok(())
    }

    /// Drain `pending_deletions` right here, for a store that publishes no
    /// manifest ([`ShardIndex::evict_by_drop`]). A no-op for every other store,
    /// which defers to the checkpoint barrier's post-publish drain.
    ///
    /// Hung off the *store*, not off the eviction step: a delta store runs the
    /// ordinary compactions — L0 folds, overfull-guard folds, verticals — far more
    /// often than it drops, so draining only on a drop would unlink the sweep's
    /// victims and leak everything else. It empties the whole queue rather than
    /// this compaction's share, which is correct here because every entry in such
    /// a store's queue is its own.
    fn unlink_superseded_now(&mut self) {
        if self.evict_by_drop {
            self.try_cleanup();
        }
    }

    /// Fold L0 into L1, rebalance every level's guards against their byte
    /// targets, then drain L1 down to its own. Observing `R` first is what makes
    /// those targets reflect the fold this call is about to perform.
    pub(crate) fn run_compact(&mut self) -> Result<(), StorageError> {
        self.l0_run_bytes = self.l0_run_bytes.max(self.l0.iter().map(|e| e.shard.file_len()).sum());

        let inputs = Self::compaction_inputs(&self.l0);
        let guard_keys = self.l1_guard_keys();
        self.compact_into(inputs, &guard_keys, 0, false, CompactionKind::L0Fold, |s| s.l0.clear())?;

        self.rebalance_guards()?;

        while self.level_bytes(0) > self.l1_target_bytes() {
            let Some(gi) = self.cheapest_l1_guard_to_drain() else {
                break;
            };
            self.vertical_fold(gi)?;
        }
        Ok(())
    }

    /// Registered shard bytes of one level — 0 for a level that does not exist
    /// yet, which is the terminal level's state until the first vertical.
    pub(super) fn level_bytes(&self, level_idx: usize) -> u64 {
        self.levels.get(level_idx).map_or(0, FLSMLevel::bytes)
    }

    /// Registered shard bytes of each guard of `level_idx`, in key order.
    pub(super) fn guard_bytes(&self, level_idx: usize) -> impl Iterator<Item = u64> + '_ {
        self.levels
            .get(level_idx)
            .into_iter()
            .flat_map(|l| l.guards.iter())
            .map(LevelGuard::bytes)
    }

    /// The largest a guard of `level_idx` is allowed to get: `R`, so no
    /// compaction's input grows with the dataset — every fold reads at most two
    /// guards' worth.
    ///
    /// A budgeted store's terminal level takes one sweep step instead, since that
    /// is the granularity `enforce_capacity` evicts at. The clamp keeps a very
    /// large or very small `capacity` from naming a target outside
    /// `[MIN_GUARD_BYTES, R]`.
    pub(super) fn guard_target_bytes(&self, level_idx: usize) -> u64 {
        match self.capacity_bytes {
            Some(cap) if level_idx == TERMINAL_LEVEL_IDX => {
                (cap / SWEEP_STEPS).clamp(MIN_GUARD_BYTES, self.l0_run_bytes)
            }
            _ => self.l0_run_bytes,
        }
    }

    /// Bytes L1 is drained to. Capped at two sweep steps for a budgeted store:
    /// `enforce_capacity` cannot evict from L1, so bytes parked there come out of
    /// what the user asked for.
    pub(super) fn l1_target_bytes(&self) -> u64 {
        let target = Self::balanced_l1_target(self.level_bytes(TERMINAL_LEVEL_IDX), self.l0_run_bytes);
        match self.capacity_bytes {
            Some(cap) => target.min(2 * (cap / SWEEP_STEPS)),
            None => target,
        }
    }

    /// L1's two rewrite terms — one per guard fold, one per vertical — balance at
    /// `2√(|L2|·R)`. The `16 R` floor under it is a chosen minimum, not a derived
    /// one: it keeps a small store from draining L1 on every spill.
    ///
    /// `l2_bytes × r` overflows a `u64` at 100 GiB of terminal bytes, hence the
    /// `u128`; the `2` stays outside the root so the extremes saturate rather
    /// than overflow it in turn.
    pub(super) fn balanced_l1_target(l2_bytes: u64, r: u64) -> u64 {
        let balanced = 2 * (u128::from(l2_bytes) * u128::from(r)).isqrt();
        u64::try_from(balanced).unwrap_or(u64::MAX).max(16u64.saturating_mul(r))
    }

    /// The L1 guard whose fold costs least: the narrowest key span, and so the
    /// fewest terminal guards to merge with. Write recency would be degenerate
    /// here — one L0 fold stamps every destination guard with the same `max_lsn`.
    ///
    /// Nothing starves: a guard skipped for being wide is one absorbing a large
    /// share of every fold, so it crosses its byte target and splits.
    fn cheapest_l1_guard_to_drain(&self) -> Option<usize> {
        let guards = &self.levels.first()?.guards;
        (0..guards.len())
            .filter(|&gi| !guards[gi].entries.is_empty())
            .min_by_key(|&gi| {
                let (lo, hi) = self.src_guard_span(gi);
                hi - lo
            })
    }

    pub(super) fn l1_guard_keys(&self) -> Vec<u128> {
        // `pack_pk_be` (left-aligned OPK MSBs) is order-preserving at every PK
        // width — a wide key's leading-16 prefix included — so one guard space
        // serves all of them and L1+ point lookups stay O(log N).
        if !self.levels.is_empty() && !self.levels[0].guards.is_empty() {
            // Below-first-guard keys saturate to bucket 0 on both routing paths
            // (`find_guard_for_key` write, `find_guard_idx` read), so the raw
            // guard keys need no 0-anchor.
            self.levels[0].guards.iter().map(|g| g.guard_key).collect()
        } else {
            // The guard space is the order-preserving `pack_pk_be` image of
            // the OPK pk_min bytes (a 16-byte prefix for wide PKs, the whole
            // key otherwise), the same key the read
            // router (`find_pk_bytes`) and the compaction merge order use. Skip
            // empty shards; dedup consecutive keys (L0 is sorted by pk_min, so
            // equal OPK keys are adjacent).
            let mut keys: Vec<u128> = Vec::new();
            for e in &self.l0 {
                if e.is_empty() {
                    continue;
                }
                let pk = crate::schema::key::pack_pk_be(e.pk_min.pk_bytes());
                if keys.last().copied() != Some(pk) {
                    keys.push(pk);
                }
            }
            // merge_and_route rejects an empty guard list; an empty table still
            // needs one bucket.
            if keys.is_empty() {
                keys.push(0);
            }
            keys
        }
    }

    /// The deepest guarded level folds each guard to a single file; shallower
    /// ones keep the wider fan-in.
    fn guard_threshold(level_idx: usize) -> usize {
        if level_idx == TERMINAL_LEVEL_IDX {
            LMAX_FILE_THRESHOLD
        } else {
            GUARD_FILE_THRESHOLD
        }
    }

    /// Hold every level's partition at its byte target, in the order the two
    /// passes have to run: a merge must see the sizes a split left.
    pub(super) fn rebalance_guards(&mut self) -> Result<(), StorageError> {
        for li in 0..self.levels.len() {
            self.split_overfull_guards(li)?;
            self.merge_underfull_guards(li)?;
        }
        Ok(())
    }

    /// Fold the guards `range` names into `keys` within their own level — one
    /// output shard per non-empty destination bucket. A split, a merge and the
    /// sweep's dehydration are this same rewrite under different triggers.
    ///
    /// Sources are drained, so a bucket that came out empty leaves no entry-less
    /// guard behind; keys in an emptied band route to the guard below it, which
    /// holds no row for them either.
    ///
    /// A dehydrated source makes every destination skeleton: skeleton rows carry
    /// no payload, so nothing may rewrite them full width.
    fn fold_guards(
        &mut self,
        level_idx: usize,
        range: std::ops::Range<usize>,
        keys: &[u128],
        kind: CompactionKind,
    ) -> Result<(), StorageError> {
        debug_assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "destination keys must be sorted and distinct"
        );
        let guards = &self.levels[level_idx].guards[range.clone()];
        let skeleton = kind == CompactionKind::Dehydrate || guards.iter().any(LevelGuard::dehydrated);
        let inputs = Self::compaction_inputs(guards.iter().flat_map(|g| g.entries.iter()));
        self.compact_into(inputs, keys, level_idx, skeleton, kind, |s| {
            s.levels[level_idx].guards.drain(range);
        })
    }

    /// Fold every guard in `level_idx` that is over its file threshold or its
    /// byte target, cutting the byte-overfull ones at their own key quantiles —
    /// a guard over the file threshold alone folds to one shard in place.
    ///
    /// Walked by key rather than by index because a fold reshapes the level under
    /// it. Each guard present on entry is folded once; what a split creates is
    /// already at the target.
    pub(super) fn split_overfull_guards(&mut self, level_idx: usize) -> Result<(), StorageError> {
        let target = self.guard_target_bytes(level_idx);
        let threshold = Self::guard_threshold(level_idx);
        let present: Vec<u128> = self.levels[level_idx].guards.iter().map(|g| g.guard_key).collect();
        for gk in present {
            let Some(gi) = self.levels[level_idx].find_exact_guard(gk) else {
                continue;
            };
            let guard = &self.levels[level_idx].guards[gi];
            let keys = guard.fold_destinations(target);
            if keys.len() > 1 || guard.entries.len() > threshold {
                self.fold_guards(level_idx, gi..gi + 1, &keys, CompactionKind::GuardSplit)?;
            }
        }
        Ok(())
    }

    /// Maximal runs of two or more adjacent guards whose combined bytes fit
    /// `bound`. A run also breaks at a change of representation, because folding
    /// a hydrated guard together with a dehydrated one would evict it (see
    /// [`Self::fold_guards`]).
    fn underfull_runs(&self, level_idx: usize, bound: u64) -> Vec<std::ops::Range<usize>> {
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
    /// Half the target is the hysteresis: a merged run is under the split trigger
    /// by construction, so the two passes cannot trade the same bytes forever.
    pub(super) fn merge_underfull_guards(&mut self, level_idx: usize) -> Result<(), StorageError> {
        let bound = self.guard_target_bytes(level_idx) / 2;
        // Descending, so each drain shifts only indices above the runs still to go.
        for run in self.underfull_runs(level_idx, bound).into_iter().rev() {
            let key = self.levels[level_idx].guards[run.start].guard_key;
            self.fold_guards(level_idx, run, &[key], CompactionKind::GuardMerge)?;
        }
        Ok(())
    }

    /// The sweep's first dehydration of a hydrated terminal guard: one skeleton
    /// shard in place, never split — the output is one row per key, so a part
    /// count derived from the hydrated input would shatter it.
    pub(super) fn dehydrate_guard(&mut self, guard_idx: usize) -> Result<(), StorageError> {
        let key = self.levels[TERMINAL_LEVEL_IDX].guards[guard_idx].guard_key;
        self.fold_guards(
            TERMINAL_LEVEL_IDX,
            guard_idx..guard_idx + 1,
            &[key],
            CompactionKind::Dehydrate,
        )
    }

    /// Evict a guard by **unlinking** it: no output shard is written at all, so
    /// the sweep costs unlinks where a bounded view's costs a whole-guard rewrite.
    /// A delta store's eviction — its rows are the change itself, not a key's
    /// current value, so there is no summed weight worth keeping a stub of.
    ///
    /// The guard is **removed** rather than left with its entries cleared.
    /// Clearing would leave a permanent entry-less guard behind every drop — one
    /// per drop, forever, in `find_guard_idx`'s binary-search space — because
    /// `_tick` leads a delta store's key and never goes backwards, so each spill
    /// lands above the last and no later `vertical_fold` range ever covers the
    /// emptied guard again. Removal is what `vertical_fold`'s own `drop_sources`
    /// already does to its source guard, and keys below the new first guard
    /// saturate to bucket 0 on both routing paths.
    ///
    /// This is also what raises `dropped_through`, as a running **max**, so the
    /// invariant holds whatever order the victims are chosen in — including a drop
    /// that lands in the middle of the retained range, which merely refuses more
    /// cursors than it had to.
    fn drop_guard(&mut self, level_idx: usize, guard_idx: usize) {
        let guard = &self.levels[level_idx].guards[guard_idx];
        let highest_round = guard.highest_leading_u64().unwrap_or(0);
        let inputs = Self::compaction_inputs(&guard.entries);
        self.dropped_through = self.dropped_through.max(highest_round);
        self.pending_deletions.extend(inputs.files);
        self.levels[level_idx].guards.remove(guard_idx);
        self.unlink_superseded_now();
    }

    /// The `pack_pk_be` span an L1 guard's fold has to cover: its own key on the
    /// low side, since it owns everything below it, and its true key extent on
    /// the high side. The gap to the next L1 guard key would be `u128::MAX` for
    /// the last one and route the fold into the whole terminal level.
    fn src_guard_span(&self, src_guard_idx: usize) -> (u128, u128) {
        let g = &self.levels[0].guards[src_guard_idx];
        let (lo, hi) = g.key_extent().unwrap_or((g.guard_key, g.guard_key));
        (g.guard_key.min(lo), hi)
    }

    /// The keys an L1 guard is cut at before it is folded down: its own key, plus
    /// every terminal guard key its span covers. Each band then overlaps exactly
    /// one destination guard, so one merge reads one band plus one terminal
    /// guard.
    fn vertical_band_keys(&self, src_guard_idx: usize) -> Vec<u128> {
        let (lo, hi) = self.src_guard_span(src_guard_idx);
        let mut keys = vec![self.levels[0].guards[src_guard_idx].guard_key];
        if let Some(dest) = self.levels.get(TERMINAL_LEVEL_IDX) {
            // `skip(1)`: the run's first guard owns everything below its own key,
            // so cutting there would route the source rows below it into a guard
            // that does not own them.
            let run = dest.find_guards_for_range(lo, hi);
            keys.extend(dest.guards[run].iter().skip(1).map(|d| d.guard_key));
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    /// Fold L1 guard `src_guard_idx` down into the terminal level, banded so no
    /// single merge reads more than one band plus one terminal guard.
    ///
    /// Atomic per band, not per call: a failure in band *k* leaves bands `0..k`
    /// folded, every intermediate state a valid partition, and the next spill
    /// redoes the rest.
    pub(super) fn vertical_fold(&mut self, src_guard_idx: usize) -> Result<(), StorageError> {
        self.ensure_level(TERMINAL_LEVEL_IDX);
        let keys = self.vertical_band_keys(src_guard_idx);
        if keys.len() > 1 {
            self.fold_guards(0, src_guard_idx..src_guard_idx + 1, &keys, CompactionKind::Vertical)?;
        }
        for key in keys {
            // A band whose bucket came out empty was never created.
            if let Some(gi) = self.levels[0].find_exact_guard(key) {
                self.fold_band_into_terminal(gi)?;
            }
        }
        // Once, not per band: the bands were cut against the terminal partition
        // as it stood, so reshaping it mid-loop would leave a later band
        // straddling a guard it was cut to overlap singly.
        self.split_overfull_guards(TERMINAL_LEVEL_IDX)?;
        self.merge_underfull_guards(TERMINAL_LEVEL_IDX)
    }

    /// Merge one L1 band with the terminal guards its span overlaps — a run of
    /// exactly one once [`Self::vertical_fold`] has banded the source. The
    /// terminal level is the deepest destination: an L2→L3 fold would serialize a
    /// level `load_manifest` rejects.
    fn fold_band_into_terminal(&mut self, src_guard_idx: usize) -> Result<(), StorageError> {
        const DEST_IDX: usize = TERMINAL_LEVEL_IDX;

        let src_guard_key = self.levels[0].guards[src_guard_idx].guard_key;
        let (range_min, range_max) = self.src_guard_span(src_guard_idx);
        let dest_range = self.levels[DEST_IDX].find_guards_for_range(range_min, range_max);
        // Input order does not affect the merge — it orders by (PK, payload) and
        // sums the weights of equal rows.
        let inputs = Self::compaction_inputs(
            self.levels[0].guards[src_guard_idx].entries.iter().chain(
                self.levels[DEST_IDX].guards[dest_range.clone()]
                    .iter()
                    .flat_map(|g| g.entries.iter()),
            ),
        );

        // An empty terminal level has no guard to route to; the source's own key
        // seeds the first one.
        let guard_keys: Vec<u128> = if dest_range.is_empty() {
            vec![src_guard_key]
        } else {
            self.levels[DEST_IDX].guards[dest_range.clone()]
                .iter()
                .map(|g| g.guard_key)
                .collect()
        };

        self.compact_into(inputs, &guard_keys, DEST_IDX, false, CompactionKind::Vertical, |s| {
            s.levels[0].guards.remove(src_guard_idx);
            s.levels[DEST_IDX].guards.drain(dest_range);
        })
    }

    /// Sum of every **registered** shard's file size — the quantity a
    /// capacity-bounded store is held under. Derived rather than counted, so it
    /// cannot drift from the several install and removal sites that would have to
    /// maintain a counter.
    ///
    /// Registered, not on-disk: a compaction's superseded inputs sit in
    /// `pending_deletions` until the checkpoint barrier's post-publish drain, so
    /// bytes on disk exceed this by the compaction garbage since the last
    /// checkpoint.
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.all_entries().map(|e| e.shard.file_len()).sum()
    }

    /// Hold this store's registered shard bytes at or under `cap` by evicting
    /// terminal-level guards, oldest-written first — the only recency signal the
    /// tree carries, since nothing records that a row was *read*. An eviction
    /// leaves skeleton rows behind for a capacity-bounded view's output store and
    /// nothing at all for a delta store ([`ShardIndex::evict_by_drop`]).
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
    pub(crate) fn enforce_capacity(&mut self) -> Result<(), StorageError> {
        let Some(cap) = self.capacity_bytes else {
            return Ok(());
        };
        let mut pushed_down = false;
        while self.resident_bytes() > cap {
            // Step 1: the oldest-written hydrated terminal guard. `newest_lsn` is
            // `None` exactly for an empty guard, which has nothing to dehydrate.
            let victim = self
                .levels
                .get(TERMINAL_LEVEL_IDX)
                .into_iter()
                .flat_map(|l| l.guards.iter().enumerate())
                .filter(|(_, g)| !g.dehydrated())
                .filter_map(|(gi, g)| g.newest_lsn().map(|lsn| (gi, lsn)))
                .min_by_key(|&(_, lsn)| lsn)
                .map(|(gi, _)| gi);
            if let Some(gi) = victim {
                match self.evict_by_drop {
                    true => self.drop_guard(TERMINAL_LEVEL_IDX, gi),
                    false => self.dehydrate_guard(gi)?,
                }
                continue;
            }
            // Step 2: nothing left to dehydrate where it sits — push one level's
            // worth of data down, once per call.
            if pushed_down {
                return Ok(());
            }
            pushed_down = true;
            if let Some(gi) = self.cheapest_l1_guard_to_drain() {
                self.vertical_fold(gi)?;
            } else if !self.l0.is_empty() {
                self.run_compact()?;
            } else {
                // Step 3: everything is at the terminal level and dehydrated —
                // the skeleton floor, below which the sweep cannot go.
                return Ok(());
            }
        }
        Ok(())
    }

    pub(crate) fn try_cleanup(&mut self) -> usize {
        let mut deleted = 0;
        let mut remaining = Vec::new();

        for path in self.pending_deletions.drain(..) {
            match fs::remove_file(&path) {
                Ok(()) => deleted += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => deleted += 1,
                Err(_) => remaining.push(path),
            }
        }

        self.pending_deletions = remaining;
        deleted
    }
}
