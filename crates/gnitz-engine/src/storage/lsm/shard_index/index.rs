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
    to_cstrings, FLSMLevel, ShardEntry, ShardIndex, FLSM_LEVELS, GUARD_FILE_THRESHOLD, L0_COMPACT_THRESHOLD,
    L1_TARGET_FILES, LMAX_FILE_THRESHOLD,
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

    /// Open `path`, insert it into the sorted L0 tier, and record it for the
    /// barrier's fdatasync sweep.
    pub fn add_unsynced_shard(&mut self, path: &str, max_lsn: u64) -> Result<(), StorageError> {
        let entry = ShardEntry::open(path, &self.schema, max_lsn)?;
        self.l0.push(entry);
        self.sort_l0();
        self.mark_unsynced(path);
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

    /// Hold `path` for the next barrier's fdatasync sweep, which must reach it
    /// before the manifest referencing it is renamed into place.
    fn mark_unsynced(&mut self, path: &str) {
        self.unsynced.push(path.to_string());
    }

    /// The unsynced set, cloned by `flush_prepare` into the barrier's sweep list.
    pub fn unsynced_paths(&self) -> &[String] {
        &self.unsynced
    }

    /// Clear the unsynced set once the barrier has fdatasync'd every path in it
    /// and renamed the manifest that references them.
    pub fn clear_unsynced(&mut self) {
        self.unsynced.clear();
    }

    /// Move compaction-superseded input files to `pending_deletions` for the
    /// (possibly deferred) drain. The sole writer of `pending_deletions`: an
    /// unpublished spill among the inputs no longer needs a barrier sweep, so
    /// it is pruned from `unsynced` in the same step — keeping the invariant
    /// that every `unsynced` path stays openable until swept.
    fn supersede_files(&mut self, inputs: Vec<String>) {
        self.unsynced.retain(|p| !inputs.contains(p));
        self.pending_deletions.extend(inputs);
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

    /// Test-only u128 oracle: OPK-encodes a **native** PK value (handling
    /// signed/compound columns) and delegates to [`find_pk_bytes`], the
    /// production path. Wide PKs cannot fit a u128.
    #[cfg(test)]
    pub(crate) fn find_pk(&self, key: u128, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        let xor8_key = super::super::xor8::probe_key(opk.pk_bytes());
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
            match ShardEntry::open(filename, &self.schema, max_lsn) {
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
    /// Nothing is mutated until every output shard has been written *and*
    /// reopened, so a failure leaves the index exactly as it was and the caller
    /// can retry against the untouched source tier.
    fn compact_into(
        &mut self,
        inputs: Vec<String>,
        max_lsn: u64,
        guard_keys: &[u128],
        dest_idx: usize,
        drop_sources: impl FnOnce(&mut Self),
    ) -> Result<(), StorageError> {
        let compact_seq = self.next_compact_seq();
        let cstrings = to_cstrings(&inputs)?;
        let cstrs: Vec<&CStr> = cstrings.iter().map(|c| c.as_c_str()).collect();

        let outputs = compact::merge_and_route(
            &cstrs,
            &self.output_dir,
            guard_keys,
            &self.schema,
            self.table_id,
            Self::level_num(dest_idx) as u32,
            compact_seq,
        )?;
        let opened = self.open_outputs(&outputs, max_lsn)?;

        self.supersede_files(inputs);
        drop_sources(self);
        self.ensure_level(dest_idx);
        for (gk, entry) in opened {
            // A crash before the sweep leaves the last-published manifest on the
            // still-present inputs, so an unswept output is only ever an orphan.
            self.mark_unsynced(&entry.filename);
            self.levels[dest_idx].get_or_create_guard(gk).entries.push(entry);
        }
        Ok(())
    }

    pub fn run_compact(&mut self) -> Result<(), StorageError> {
        let (inputs, max_lsn) = Self::compaction_inputs(&self.l0);
        let guard_keys = self.l1_guard_keys();
        self.compact_into(inputs, max_lsn, &guard_keys, 0, |s| s.l0.clear())?;

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
        if level_idx + 1 == FLSM_LEVELS {
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
                self.compact_one_guard(level_idx, gi)?;
            }
            gi += 1;
        }
        Ok(())
    }

    /// Fold one guard's files into a single output, in place. A guard whose rows
    /// all cancel is left with no entries rather than an empty shard.
    fn compact_one_guard(&mut self, level_idx: usize, guard_idx: usize) -> Result<(), StorageError> {
        let guard = &self.levels[level_idx].guards[guard_idx];
        let guard_key = guard.guard_key;
        let (inputs, max_lsn) = Self::compaction_inputs(&guard.entries);

        self.compact_into(inputs, max_lsn, &[guard_key], level_idx, |s| {
            s.levels[level_idx].guards[guard_idx].entries.clear();
        })
    }

    /// L1→L2 vertical compaction: fold the worst (most-filed) L1 guard, together
    /// with the L2 guards its key range overlaps, down into L2 — the deepest
    /// vertical destination, since an L2→L3 fold would serialize a level
    /// `load_manifest` rejects.
    pub(super) fn compact_guard_vertical(&mut self) -> Result<(), StorageError> {
        const SRC_IDX: usize = 0; // L1
        const DEST_IDX: usize = 1; // L2

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

        let src_guard_key = self.levels[SRC_IDX].guards[worst_idx].guard_key;
        let src_max_bound = if worst_idx + 1 < self.levels[SRC_IDX].guards.len() {
            self.levels[SRC_IDX].guards[worst_idx + 1].guard_key.saturating_sub(1)
        } else {
            u128::MAX
        };

        self.ensure_level(DEST_IDX);

        let dest_range = self.levels[DEST_IDX].find_guards_for_range(src_guard_key, src_max_bound);
        // The source guard's files followed by every overlapping destination
        // guard's, in one pass. Input order does not affect the merge — it orders
        // by (PK, payload) and sums the weights of equal rows.
        let (all_input_files, vert_max_lsn) = Self::compaction_inputs(
            self.levels[SRC_IDX].guards[worst_idx].entries.iter().chain(
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

        self.compact_into(all_input_files, vert_max_lsn, &guard_keys, DEST_IDX, |s| {
            s.levels[SRC_IDX].guards.remove(worst_idx);
            s.levels[DEST_IDX].guards.drain(dest_range);
        })?;

        self.compact_overfull_guards(DEST_IDX, Self::guard_threshold(DEST_IDX))
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
