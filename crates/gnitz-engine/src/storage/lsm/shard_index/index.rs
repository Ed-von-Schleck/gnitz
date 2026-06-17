//! In-memory FLSM index state + the compaction trigger/orchestration for
//! [`ShardIndex`]: shard insertion, L0 sort, PK probes, the `should_compact`
//! trigger, and `run_compact` with its L0→L1 / vertical guard merges.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::fs;
use std::rc::Rc;

use super::super::compact;
use super::super::error::StorageError;
use super::super::shard_reader::MappedShard;
use super::{ShardIndex, ShardEntry, FLSMLevel, PendingShard, to_cstrings,
            MAX_LEVELS, L0_COMPACT_THRESHOLD, GUARD_FILE_THRESHOLD, LMAX_FILE_THRESHOLD, L1_TARGET_FILES};

impl ShardIndex {
    /// Enable `SHARD_FLAG_PK_UNIQUE` tagging for compacted shards.
    /// Only call this for base tables with a user-defined PK constraint.
    pub fn enable_pk_unique_tagging(&mut self) {
        self.can_tag_pk_unique = true;
    }

    pub(super) fn all_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.l0.iter().chain(
            self.levels
                .iter()
                .flat_map(|l| l.guards.iter().flat_map(|g| g.entries.iter())),
        )
    }

    pub(super) fn level_num(level_idx: usize) -> usize {
        level_idx + 1
    }

    pub fn add_shard(&mut self, path: &str, min_lsn: u64, max_lsn: u64) -> Result<(), StorageError> {
        let entry = ShardEntry::open(path, &self.schema, min_lsn, max_lsn)?;
        self.l0.push(entry);
        self.sort_l0();
        self.update_flags();
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

    pub(super) fn update_flags(&mut self) {
        self.needs_compaction = self.l0.len() > L0_COMPACT_THRESHOLD;
    }

    pub fn should_compact(&self) -> bool {
        self.needs_compaction
    }

    pub fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.all_entries()
            .map(|e| Rc::clone(&e.shard))
            .collect()
    }

    /// Test-only u128 oracle: OPK-encodes a **native** PK value (handling
    /// signed/compound columns) and delegates to [`find_pk_bytes`], the
    /// production path. Wide PKs cannot fit a u128.
    #[cfg(test)]
    pub(crate) fn find_pk(&self, key: u128, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        let (opk, stride) = super::super::columnar::opk_key(&self.schema, key);
        self.find_pk_bytes(&opk[..stride], visitor);
    }

    /// Point lookup by OPK `key` bytes — universal across all PK widths. L0 is
    /// scanned (range-rejected per entry); each L1+ level routes by the guard
    /// key `pack_pk_be(key)` (the same order-preserving space `l1_guard_keys`
    /// builds), restoring O(log N) routing for wide PKs too.
    pub fn find_pk_bytes(&self, key: &[u8], visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        for e in &self.l0 {
            if let Some((arc, idx)) = e.probe_pk_bytes(key) {
                visitor(arc, idx);
            }
        }
        let route_key = super::super::merge::pack_pk_be(key);
        for level in &self.levels {
            if let Some(g_idx) = level.find_guard_idx(route_key) {
                for e in &level.guards[g_idx].entries {
                    if let Some((arc, idx)) = e.probe_pk_bytes(key) {
                        visitor(arc, idx);
                    }
                }
            }
        }
    }

    pub fn max_lsn(&self) -> u64 {
        self.all_entries().map(|e| e.max_lsn).max().unwrap_or(0)
    }

    /// Insert an already-mmap'd PendingShard into L0. Caller must have
    /// renamed the underlying .tmp into `pending.final_path` already; Linux
    /// rename(2) does not invalidate existing mmaps.
    pub fn add_opened_shard(&mut self, pending: PendingShard) -> Result<(), StorageError> {
        let entry = ShardEntry {
            shard: pending.mapped,
            filename: pending.final_path,
            min_lsn: pending.min_lsn,
            max_lsn: pending.max_lsn,
            pk_min: pending.pk_min,
            pk_max: pending.pk_max,
        };
        self.l0.push(entry);
        self.sort_l0();
        self.update_flags();
        Ok(())
    }

    pub(super) fn get_or_create_level(&mut self, level_num: usize) -> &mut FLSMLevel {
        self.ensure_level(level_num);
        &mut self.levels[level_num - 1]
    }

    pub(super) fn ensure_level(&mut self, level_num: usize) {
        let idx = level_num - 1;
        while self.levels.len() <= idx {
            self.levels.push(FLSMLevel::new());
        }
    }

    pub fn run_compact(&mut self) -> Result<(), StorageError> {
        if !self.needs_compaction {
            return Ok(());
        }

        self.compact_seq += 1;
        let l0_filenames: Vec<String> = self.l0.iter().map(|e| e.filename.clone()).collect();
        let l0_max_lsn = self.l0.iter().map(|e| e.max_lsn).max().unwrap_or(0);

        let lsn_tag = if l0_max_lsn > 0 {
            l0_max_lsn
        } else {
            self.compact_seq
        };

        let guard_keys = self.l1_guard_keys();

        let l0_cstrings = to_cstrings(&l0_filenames)?;
        let l0_cstrs: Vec<&CStr> = l0_cstrings.iter().map(|c| c.as_c_str()).collect();

        let out_dir = CString::new(self.output_dir.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let result_cap = guard_keys.len().max(l0_filenames.len());
        let mut results: Vec<compact::GuardResult> =
            (0..result_cap).map(|_| compact::GuardResult::zeroed()).collect();

        let num_results = compact::merge_and_route(
            &l0_cstrs,
            &out_dir,
            &guard_keys,
            &self.schema,
            self.table_id,
            1,
            lsn_tag,
            &mut results,
            self.can_tag_pk_unique,
        )?;

        let guard_outputs: Vec<(u128, String)> = results[..num_results]
            .iter()
            .map(|r| (r.guard_key, r.filename_str().to_string()))
            .collect();

        self.commit_l0_to_l1(&guard_outputs, l0_max_lsn)?;

        self.pending_deletions.extend(l0_filenames);

        self.compact_guards_if_needed()?;

        if !self.levels.is_empty() {
            let l1_count = self.levels[0].total_file_count();
            if l1_count > L1_TARGET_FILES {
                self.compact_guard_vertical(1)?;
            }
        }

        Ok(())
    }

    pub(super) fn l1_guard_keys(&self) -> Vec<u128> {
        // After the OPK-at-rest flip, `pack_pk_be` (left-aligned OPK MSBs) is a
        // universal order-preserving guard key for every PK width — including
        // wide (`pk_stride > 16`), whose leading-16 prefix is order-preserving.
        // This restores O(log N) wide-PK L1+ point lookups that previously
        // collapsed all wide compaction to a single guard 0 (O(N) reads).
        if !self.levels.is_empty() && !self.levels[0].guards.is_empty() {
            // Always anchor the guard space at 0. `find_guard_for_key` routes a
            // key below the first guard to guard[0] via `saturating_sub(1)`, but
            // `find_guard_idx` returns `None` for keys below the first guard
            // key — so a key inserted after L1 established `guard[0] > 0` would
            // be written during compaction yet invisible on read. Mirrors the
            // fix in `compact_guard_vertical`.
            let mut keys: Vec<u128> = self.levels[0]
                .guards
                .iter()
                .map(|g| g.guard_key)
                .collect();
            if keys.first().copied() != Some(0) {
                keys.insert(0, 0);
            }
            keys
        } else {
            // The guard space is the order-preserving `pk_sort_key`
            // (= `pack_pk_be` of the OPK pk_min bytes), the same key the read
            // router (`find_pk_bytes`) and the compaction merge order use. Skip
            // empty shards; dedup consecutive keys (L0 is sorted by pk_min, so
            // equal OPK keys are adjacent).
            let mut keys: Vec<u128> = Vec::new();
            for e in &self.l0 {
                if e.is_empty() {
                    continue;
                }
                let pk = super::super::merge::pk_sort_key(e.pk_min.pk_bytes());
                if keys.last().copied() != Some(pk) {
                    keys.push(pk);
                }
            }
            if keys.is_empty() || keys[0] != 0 {
                keys.insert(0, 0);
            }
            keys
        }
    }

    fn commit_l0_to_l1(
        &mut self,
        guard_outputs: &[(u128, String)],
        max_lsn: u64,
    ) -> Result<(), StorageError> {
        self.ensure_level(1);
        let schema_copy = self.schema;

        // Open all new entries before touching self.l0.
        // If any open fails, l0 is still intact and the caller can retry.
        let mut new_entries: Vec<(u128, ShardEntry)> =
            Vec::with_capacity(guard_outputs.len());
        for (gk, filename) in guard_outputs {
            let entry = ShardEntry::open(filename, &schema_copy, 0, max_lsn)?;
            new_entries.push((*gk, entry));
        }

        // All opens succeeded — safe to mutate state.
        self.l0.clear();
        for (gk, entry) in new_entries {
            self.levels[0]
                .get_or_create_guard(gk)
                .entries
                .push(entry);
        }
        self.update_flags();
        Ok(())
    }

    pub(super) fn compact_guards_if_needed(&mut self) -> Result<(), StorageError> {
        for li in 0..self.levels.len() {
            let threshold = if Self::level_num(li) == MAX_LEVELS - 1 {
                LMAX_FILE_THRESHOLD
            } else {
                GUARD_FILE_THRESHOLD
            };
            let mut gi = 0;
            while gi < self.levels[li].guards.len() {
                if self.levels[li].guards[gi].entries.len() > threshold {
                    self.compact_one_guard(li, gi)?;
                }
                gi += 1;
            }
        }
        Ok(())
    }

    fn compact_one_guard(&mut self, level_idx: usize, guard_idx: usize) -> Result<(), StorageError> {
        self.compact_seq += 1;
        let guard = &self.levels[level_idx].guards[guard_idx];

        let guard_max_lsn = guard.entries.iter().map(|e| e.max_lsn).max().unwrap_or(0);
        let out_path = format!(
            "{}/hcomp_{}_L{}_G{}_{}.db",
            self.output_dir,
            self.table_id,
            Self::level_num(level_idx),
            guard.guard_key,
            self.compact_seq,
        );

        let input_filenames: Vec<String> =
            guard.entries.iter().map(|e| e.filename.clone()).collect();
        let input_cstrings = to_cstrings(&input_filenames)?;
        let input_cstrs: Vec<&CStr> = input_cstrings.iter().map(|c| c.as_c_str()).collect();
        let out_cstr = CString::new(out_path.as_str()).map_err(|_| StorageError::InvalidPath)?;

        if let Err(e) = compact::compact_shards(&input_cstrs, &out_cstr, &self.schema, self.table_id, self.can_tag_pk_unique) {
            let _ = fs::remove_file(&out_path);
            return Err(e);
        }

        let new_entry = ShardEntry::open(&out_path, &self.schema, 0, guard_max_lsn)?;

        self.pending_deletions.extend(input_filenames);

        self.levels[level_idx].guards[guard_idx].entries = vec![new_entry];

        Ok(())
    }

    pub(super) fn compact_guard_vertical(&mut self, src_level_num: usize) -> Result<(), StorageError> {
        if !(1..MAX_LEVELS).contains(&src_level_num) {
            return Ok(());
        }
        let src_idx = src_level_num - 1;

        let worst_idx = {
            let src = &self.levels[src_idx];
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

        let src_guard_key = self.levels[src_idx].guards[worst_idx].guard_key;
        let src_max_bound = if worst_idx + 1 < self.levels[src_idx].guards.len() {
            self.levels[src_idx].guards[worst_idx + 1].guard_key.saturating_sub(1)
        } else {
            u128::MAX
        };

        self.compact_seq += 1;
        let dest_idx = src_level_num;
        self.ensure_level(src_level_num + 1);

        let mut all_input_files: Vec<String> = self.levels[src_idx].guards[worst_idx]
            .entries
            .iter()
            .map(|e| e.filename.clone())
            .collect();

        let dest_guard_indices =
            self.levels[dest_idx].find_guards_for_range(src_guard_key, src_max_bound);
        let mut vert_max_lsn = self.levels[src_idx].guards[worst_idx]
            .entries
            .iter()
            .map(|e| e.max_lsn)
            .max()
            .unwrap_or(0);

        for &di in &dest_guard_indices {
            let dg = &self.levels[dest_idx].guards[di];
            for e in &dg.entries {
                all_input_files.push(e.filename.clone());
                if e.max_lsn > vert_max_lsn {
                    vert_max_lsn = e.max_lsn;
                }
            }
        }

        let lsn_tag = if vert_max_lsn > 0 {
            vert_max_lsn
        } else {
            self.compact_seq
        };

        let mut guard_keys: Vec<u128> = if !dest_guard_indices.is_empty() {
            dest_guard_indices
                .iter()
                .map(|&di| self.levels[dest_idx].guards[di].guard_key)
                .collect()
        } else {
            vec![self.levels[src_idx].guards[worst_idx].guard_key]
        };

        // Ensure guard_keys covers the source range's lower bound.
        // Without this, keys below the lowest destination guard are routed
        // to that guard (via find_guard_for_key → index 0), but find_guard_idx
        // on the read path returns None for keys below the guard key.
        if !guard_keys.is_empty() && guard_keys[0] > src_guard_key {
            guard_keys.insert(0, self.levels[src_idx].guards[worst_idx].guard_key);
        }

        let input_cstrings = to_cstrings(&all_input_files)?;
        let input_cstrs: Vec<&CStr> = input_cstrings.iter().map(|c| c.as_c_str()).collect();
        let out_dir = CString::new(self.output_dir.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let result_cap = guard_keys.len().max(1);
        let mut results: Vec<compact::GuardResult> =
            (0..result_cap).map(|_| compact::GuardResult::zeroed()).collect();

        let num_results = compact::merge_and_route(
            &input_cstrs,
            &out_dir,
            &guard_keys,
            &self.schema,
            self.table_id,
            src_level_num as u32 + 1,
            lsn_tag,
            &mut results,
            self.can_tag_pk_unique,
        )?;

        let guard_outputs: Vec<(u128, String)> = results[..num_results]
            .iter()
            .map(|r| (r.guard_key, r.filename_str().to_string()))
            .collect();

        let schema_copy = self.schema;
        let mut opened: Vec<(u128, ShardEntry)> = Vec::with_capacity(guard_outputs.len());
        for (gk, filename) in &guard_outputs {
            match ShardEntry::open(filename, &schema_copy, 0, vert_max_lsn) {
                Ok(entry) => opened.push((*gk, entry)),
                Err(e) => {
                    for (_, f) in &guard_outputs {
                        let _ = fs::remove_file(f);
                    }
                    return Err(e);
                }
            }
        }

        self.pending_deletions.extend(all_input_files);
        self.levels[src_idx].guards.remove(worst_idx);
        {
            self.levels[dest_idx]
                .guards
                .retain(|g| !guard_keys.contains(&g.guard_key));
        }
        for (gk, entry) in opened {
            self.levels[dest_idx]
                .get_or_create_guard(gk)
                .entries
                .push(entry);
        }

        self.update_flags();

        if Self::level_num(dest_idx) == MAX_LEVELS - 1 {
            let mut gi = 0;
            while gi < self.levels[dest_idx].guards.len() {
                if self.levels[dest_idx].guards[gi].entries.len() > LMAX_FILE_THRESHOLD {
                    self.compact_one_guard(dest_idx, gi)?;
                }
                gi += 1;
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
