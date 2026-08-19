//! In-memory FLSM index state + the compaction trigger/orchestration for
//! [`ShardIndex`]: shard insertion, L0 sort, PK probes, the `should_compact`
//! trigger, and `run_compact` with its L0→L1 / vertical guard merges.

use std::cmp::Ordering;
use std::ffi::CStr;
use std::fs;
use std::rc::Rc;

use super::super::compact;
use super::super::error::StorageError;
use super::super::shard_reader::MappedShard;
use super::{
    to_cstrings, FLSMLevel, ShardEntry, ShardIndex, GUARD_FILE_THRESHOLD, L0_COMPACT_THRESHOLD, L1_TARGET_FILES,
    LMAX_FILE_THRESHOLD, TERMINAL_LEVEL_IDX,
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
    pub fn add_unsynced_shard(&mut self, path: &str, max_lsn: u64) -> Result<(), StorageError> {
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
    pub fn should_compact(&self) -> bool {
        self.l0.len() > L0_COMPACT_THRESHOLD
    }

    /// The paths `flush_prepare` hands the barrier as its fdatasync sweep list:
    /// every live shard an fdatasync has not yet reached.
    pub fn unsynced_paths(&self) -> impl Iterator<Item = &str> {
        self.all_entries().filter(|e| !e.synced).map(|e| e.filename.as_str())
    }

    /// Mark every live shard durable, once the barrier has fdatasync'd the sweep
    /// list and renamed the manifest that references them.
    pub fn clear_unsynced(&mut self) {
        for e in self.all_entries_mut() {
            e.synced = true;
        }
    }

    /// Every live shard's `Rc`, yielded lazily — callers `extend` without an
    /// intermediate `Vec` (the per-worker cursor gather).
    pub fn all_shard_arcs_iter(&self) -> impl Iterator<Item = Rc<MappedShard>> + '_ {
        self.all_entries().map(|e| Rc::clone(&e.shard))
    }

    #[cfg(test)]
    pub fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.all_shard_arcs_iter().collect()
    }

    /// Raw rows across every live shard, summed without touching an `Rc`. Raw:
    /// cross-shard duplicates and ghosts are counted, so it is an upper bound on
    /// the live rows a walk would emit — the shape the selectivity gate wants.
    pub fn total_rows(&self) -> usize {
        self.all_entries().map(|e| e.shard.count).sum()
    }

    /// Whether any registered shard is a skeleton — i.e. whether a read of this
    /// store can meet a `(PK, coarse weight)` row it has to hydrate.
    ///
    /// Derived from the shards themselves rather than from the store's capacity:
    /// a bounded view under its cap has never dehydrated and reads exactly like an
    /// unbounded one, and this is what lets it keep the ordinary read paths. Only
    /// compaction writes a skeleton, so the memtable and RAM tier never hold one
    /// and the registered set is the whole question.
    pub fn has_skeleton_shard(&self) -> bool {
        self.all_entries().any(|e| e.shard.is_skeleton())
    }

    /// Test-only u128 oracle: OPK-encodes a **native** PK value (handling
    /// signed/compound columns) and delegates to [`find_pk_bytes`], the
    /// production path. Wide PKs cannot fit a u128.
    #[cfg(test)]
    pub(crate) fn find_pk(&self, key: u128, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        let xor8_key = crate::schema::key::probe_key(opk.pk_bytes());
        self.find_pk_bytes(opk.pk_bytes(), xor8_key, visitor);
    }

    /// Point lookup by OPK `key` bytes — universal across all PK widths. L0 is
    /// scanned (range-rejected per entry); each L1+ level routes by the guard
    /// key `pack_pk_be(key)` (the same order-preserving space `l1_guard_keys`
    /// builds), restoring O(log N) routing for wide PKs too.
    pub fn find_pk_bytes(&self, key: &[u8], xor8_key: u64, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        // A pure function of `key`, so the sweep derives it once instead of once
        // per candidate shard.
        let route_key = crate::schema::key::pack_pk_be(key);
        for e in &self.l0 {
            if let Some((arc, idx)) = e.probe_pk_bytes(key, xor8_key) {
                visitor(arc, idx);
            }
        }
        for level in &self.levels {
            if let Some(g_idx) = level.find_guard_idx(route_key) {
                for e in &level.guards[g_idx].entries {
                    if let Some((arc, idx)) = e.probe_pk_bytes(key, xor8_key) {
                        visitor(arc, idx);
                    }
                }
            }
        }
    }

    pub fn max_lsn(&self) -> u64 {
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

    /// Filenames of `entries` plus their LSN watermark. One derivation, because
    /// `Table::new` seeds `current_lsn = max_lsn() + 1`: a watermark below an
    /// input's would let a later spill reuse a live shard's name.
    fn compaction_inputs<'a>(entries: impl IntoIterator<Item = &'a ShardEntry>) -> (Vec<String>, u64) {
        let mut files = Vec::new();
        let mut max_lsn = 0u64;
        for e in entries {
            files.push(e.filename.clone());
            max_lsn = max_lsn.max(e.max_lsn);
        }
        (files, max_lsn)
    }

    /// The one compaction driver: merge `inputs` into `dest_idx`, routed across
    /// `guard_keys`, then release the entries they came from via `drop_sources`.
    ///
    /// Each destination guard is written skeleton iff it is already dehydrated or
    /// `force_skeleton` is set. The two halves answer different questions:
    /// `force_skeleton` performs a guard's *first* dehydration (a hydrated guard
    /// derives `false`, so the derived rule alone could never start), while the
    /// derived rule is the never-re-hydrate rule — a vertical folding hydrated L1
    /// data into an already-dehydrated L2 guard emits skeleton, so ordinary
    /// compaction cannot undo the sweep's work, and data landing in a dehydrated
    /// guard is skeletonized without the sweep running at all. The slice is
    /// per-guard because a vertical's destination set is every L2 guard its key
    /// range overlaps while the sweep dehydrates one guard at a time, so a mixed
    /// destination set is routine.
    ///
    /// Nothing is mutated until every output shard has been written *and*
    /// reopened, so a failure leaves the index exactly as it was and the caller
    /// can retry against the untouched source tier.
    fn compact_into(
        &mut self,
        inputs: Vec<String>,
        max_lsn: u64,
        guard_keys: &[u128],
        dest_idx: usize,
        force_skeleton: bool,
        drop_sources: impl FnOnce(&mut Self),
    ) -> Result<(), StorageError> {
        let compact_seq = self.next_compact_seq();
        let cstrings = to_cstrings(&inputs)?;
        let cstrs: Vec<&CStr> = cstrings.iter().map(|c| c.as_c_str()).collect();

        // Only terminal-level guards are ever dehydrated, so the derived half is
        // asked only there; every shallower destination is hydrated unless the
        // sweep is forcing this guard's first dehydration.
        let dest = (dest_idx == TERMINAL_LEVEL_IDX)
            .then(|| self.levels.get(dest_idx))
            .flatten();
        let guards: Vec<(u128, bool)> = guard_keys
            .iter()
            .map(|&gk| {
                let skeleton = force_skeleton
                    || dest.is_some_and(|l| {
                        l.find_guard_idx(gk)
                            .is_some_and(|gi| l.guards[gi].guard_key == gk && l.guards[gi].dehydrated())
                    });
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

        // The superseded inputs are the sole writer of `pending_deletions`. Each
        // leaves the index carrying its own sweep flag, so an unpublished spill
        // among them stops being swept with nothing having to prune it.
        self.pending_deletions.extend(inputs);
        drop_sources(self);
        self.ensure_level(dest_idx);
        for (gk, entry) in opened {
            self.levels[dest_idx].get_or_create_guard(gk).entries.push(entry);
        }
        Ok(())
    }

    pub fn run_compact(&mut self) -> Result<(), StorageError> {
        let (inputs, max_lsn) = Self::compaction_inputs(&self.l0);
        let guard_keys = self.l1_guard_keys();
        self.compact_into(inputs, max_lsn, &guard_keys, 0, false, |s| s.l0.clear())?;

        self.compact_guards_if_needed()?;

        if self.levels[0].total_file_count() > L1_TARGET_FILES {
            self.compact_guard_vertical()?;
        }
        Ok(())
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

    pub(super) fn compact_guards_if_needed(&mut self) -> Result<(), StorageError> {
        for li in 0..self.levels.len() {
            self.compact_overfull_guards(li, Self::guard_threshold(li))?;
        }
        Ok(())
    }

    /// Fold every guard in `level_idx` whose file count exceeds `threshold` down
    /// to one file via `compact_one_guard`. Index-based walk because
    /// `compact_one_guard` replaces a guard's `entries` in place (guard count is
    /// stable across the loop).
    fn compact_overfull_guards(&mut self, level_idx: usize, threshold: usize) -> Result<(), StorageError> {
        let mut gi = 0;
        while gi < self.levels[level_idx].guards.len() {
            if self.levels[level_idx].guards[gi].entries.len() > threshold {
                self.compact_one_guard(level_idx, gi, false)?;
            }
            gi += 1;
        }
        Ok(())
    }

    /// Fold one guard's files into a single output, in place. A guard whose rows
    /// all cancel is left with no entries rather than an empty shard.
    /// `force_skeleton` dehydrates the rewritten guard (see [`Self::compact_into`]);
    /// the destination is the one guard key it names, so the flag reaches exactly
    /// that guard.
    fn compact_one_guard(
        &mut self,
        level_idx: usize,
        guard_idx: usize,
        force_skeleton: bool,
    ) -> Result<(), StorageError> {
        let guard = &self.levels[level_idx].guards[guard_idx];
        let guard_key = guard.guard_key;
        let (inputs, max_lsn) = Self::compaction_inputs(&guard.entries);

        self.compact_into(inputs, max_lsn, &[guard_key], level_idx, force_skeleton, |s| {
            s.levels[level_idx].guards[guard_idx].entries.clear();
        })
    }

    /// The ordinary vertical trigger: pick the worst (most-filed) L1 guard and
    /// hand it to [`Self::vertical_fold`]. A guard holding one file is left alone —
    /// folding it would rewrite the terminal level to save nothing.
    pub(super) fn compact_guard_vertical(&mut self) -> Result<(), StorageError> {
        const SRC_IDX: usize = 0; // L1

        let worst_idx = {
            let src = &self.levels[SRC_IDX];
            let mut worst = None;
            let mut worst_count = 0;
            for (i, g) in src.guards.iter().enumerate() {
                if g.entries.len() > worst_count {
                    worst_count = g.entries.len();
                    worst = Some(i);
                }
            }
            match worst {
                Some(idx) if worst_count > 1 => idx,
                _ => return Ok(()),
            }
        };

        self.vertical_fold(worst_idx)
    }

    /// Fold L1 guard `src_guard_idx`, together with the L2 guards its key range
    /// overlaps, down into L2 — the deepest vertical destination, since an L2→L3
    /// fold would serialize a level `load_manifest` rejects.
    ///
    /// The destination range comes from the source guard's true key extent, not
    /// from the gap to the next L1 guard key: the latter is `u128::MAX` for the
    /// last (or only) L1 guard, which would route the fold into every L2 guard at
    /// or above its key and rewrite the whole terminal level on every such fold.
    /// That holds for any store, not just a bounded one — a single L1 guard is just
    /// the common case under the capacity sweep.
    ///
    /// Passes `force_skeleton = false`: only the sweep's own dehydration step
    /// overrides a destination guard's derived representation.
    pub(super) fn vertical_fold(&mut self, src_guard_idx: usize) -> Result<(), StorageError> {
        const SRC_IDX: usize = 0; // L1
        const DEST_IDX: usize = TERMINAL_LEVEL_IDX; // L2

        let src_guard = &self.levels[SRC_IDX].guards[src_guard_idx];
        let src_guard_key = src_guard.guard_key;
        // An empty source guard has no extent; its own key bounds the range on
        // both sides, so the fold touches at most the guard it would land in.
        let (extent_min, extent_max) = src_guard.key_extent().unwrap_or((src_guard_key, src_guard_key));
        // Guard 0 also owns everything below `guards[0].guard_key`, so the range
        // start is the lower of the guard's own key and its extent's.
        let range_min = src_guard_key.min(extent_min);

        self.ensure_level(DEST_IDX);

        let dest_range = self.levels[DEST_IDX].find_guards_for_range(range_min, extent_max);
        // The source guard's files followed by every overlapping destination
        // guard's, in one pass. Input order does not affect the merge — it orders
        // by (PK, payload) and sums the weights of equal rows.
        let (all_input_files, vert_max_lsn) = Self::compaction_inputs(
            self.levels[SRC_IDX].guards[src_guard_idx].entries.iter().chain(
                self.levels[DEST_IDX].guards[dest_range.clone()]
                    .iter()
                    .flat_map(|g| g.entries.iter()),
            ),
        );

        // No overlapping destination guard yet: the source guard's own key seeds
        // one, so the folded rows keep a slot to route to.
        let guard_keys: Vec<u128> = if dest_range.is_empty() {
            vec![src_guard_key]
        } else {
            self.levels[DEST_IDX].guards[dest_range.clone()]
                .iter()
                .map(|g| g.guard_key)
                .collect()
        };

        self.compact_into(all_input_files, vert_max_lsn, &guard_keys, DEST_IDX, false, |s| {
            s.levels[SRC_IDX].guards.remove(src_guard_idx);
            s.levels[DEST_IDX].guards.drain(dest_range);
        })?;

        self.compact_overfull_guards(DEST_IDX, Self::guard_threshold(DEST_IDX))
    }

    /// Sum of every **registered** shard's file size — the quantity a
    /// capacity-bounded store is held under. Derived, not cached: it runs only
    /// under a capacity and only once per spill, so recomputing beats maintaining
    /// a counter across the install (`add_unsynced_shard`, `open_outputs`) and
    /// removal (`supersede_files`, every `drop_sources` closure) sites, and cannot
    /// drift from them.
    ///
    /// Registered, not on-disk: a compaction's superseded inputs sit in
    /// `pending_deletions` until the checkpoint barrier's post-publish drain, so
    /// bytes on disk exceed this by the compaction garbage accumulated since the
    /// last checkpoint.
    pub fn resident_bytes(&self) -> u64 {
        self.all_entries().map(|e| e.shard.file_len()).sum()
    }

    /// Hold this store's registered shard bytes at or under `cap` by dehydrating
    /// terminal-level guards, oldest-written first.
    ///
    /// While `resident_bytes() > cap`:
    /// 1. a hydrated terminal-level guard exists → `compact_one_guard` on the one
    ///    whose newest entry has the smallest `max_lsn`, with
    ///    `force_skeleton = true`;
    /// 2. else, if this call has not pushed down yet: an L1 guard exists →
    ///    `vertical_fold` on the oldest one; else L0 non-empty → `run_compact`;
    ///    then loop — a `vertical_fold` gives step 1 a victim, a `run_compact`
    ///    only moves L0 into L1 for the next call's fold;
    /// 3. otherwise stop.
    ///
    /// L1 is drained before L0 is refilled because ordinary compaction cannot be
    /// relied on to do it: the vertical is gated on `L1_TARGET_FILES`, and a
    /// store whose keys form one guard band has its L1 guard folded back to a
    /// single file by `compact_guards_if_needed` on every pass, so that gate
    /// never fires and the terminal level never forms. Preferring L0 here would
    /// leave the sweep with nothing to dehydrate forever, because a spill has
    /// just refilled L0 at every trigger.
    ///
    /// Only the terminal level can hold skeletons and the compaction trigger is a
    /// file count that never fires on byte volume, so pushing data down is the
    /// sweep's second job — but it is the expensive half (a whole guard's worth
    /// of data rewritten), so it is budgeted to one per call. A store held under
    /// its capacity by dehydration alone never reaches step 2; a store whose
    /// capacity is under its **skeleton floor** (the size of a fully dehydrated
    /// store) performs one push-down per spill and then stops, converging across
    /// spills rather than running the whole level down inside one trigger.
    ///
    /// Victim choice is **write recency** (`max_lsn`), the only recency signal the
    /// tree carries — nothing anywhere records that a row was *read*.
    ///
    /// Terminates: step 1 strictly decreases the hydrated-terminal-guard count and
    /// nothing here re-hydrates a guard; step 2 runs at most once. The fixpoint
    /// across calls is the skeleton floor.
    pub fn enforce_capacity(&mut self) -> Result<(), StorageError> {
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
                self.compact_one_guard(TERMINAL_LEVEL_IDX, gi, true)?;
                continue;
            }
            // Step 2: nothing left to dehydrate where it sits — push one level's
            // worth of data down, once per call.
            if pushed_down {
                return Ok(());
            }
            pushed_down = true;
            let oldest_l1 = self.levels.first().and_then(|l| {
                l.guards
                    .iter()
                    .enumerate()
                    // An empty guard was never written, so it sorts last: it is
                    // folded away only once no guard holding data is left.
                    .min_by_key(|(_, g)| g.newest_lsn().unwrap_or(u64::MAX))
                    .map(|(gi, _)| gi)
            });
            if let Some(gi) = oldest_l1 {
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

    pub fn try_cleanup(&mut self) -> usize {
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
