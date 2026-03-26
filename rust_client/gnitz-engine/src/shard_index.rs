//! FLSM Shard Index: manages shard lifecycle, compaction, and manifest I/O.
//!
//! Replaces 6 RPython modules: flsm.py, index.py, compactor.py, manifest.py,
//! metadata.py, refcount.py. Exposed as an opaque handle via FFI.

use std::ffi::CStr;
use std::fs;

use crate::compact::{self, SchemaDescriptor};
use crate::manifest::{self, ManifestEntryRaw};
use crate::shard_reader::MappedShard;

// ---------------------------------------------------------------------------
// Constants (from flsm.py)
// ---------------------------------------------------------------------------

const MAX_LEVELS: usize = 3;
const L0_COMPACT_THRESHOLD: usize = 4;
const GUARD_FILE_THRESHOLD: usize = 4;
const LMAX_FILE_THRESHOLD: usize = 1;
const L1_TARGET_FILES: usize = 16;

// ---------------------------------------------------------------------------
// ShardEntry: owns a MappedShard + cached metadata
// ---------------------------------------------------------------------------

struct ShardEntry {
    shard: MappedShard,
    filename: String,
    min_lsn: u64,
    max_lsn: u64,
    pk_min: u128,
    pk_max: u128,
}

impl ShardEntry {
    fn open(
        path: &str,
        schema: &SchemaDescriptor,
        min_lsn: u64,
        max_lsn: u64,
    ) -> Result<Self, i32> {
        let cpath = std::ffi::CString::new(path).map_err(|_| -1)?;
        let shard = MappedShard::open(&cpath, schema, false)?;
        let (pk_min, pk_max) = if shard.count > 0 {
            (shard.get_pk(0), shard.get_pk(shard.count - 1))
        } else {
            (u128::MAX, 0)
        };
        Ok(ShardEntry {
            shard,
            filename: path.to_string(),
            min_lsn,
            max_lsn,
            pk_min,
            pk_max,
        })
    }
}

// ---------------------------------------------------------------------------
// LevelGuard: key-range-bounded group of shard entries
// ---------------------------------------------------------------------------

struct LevelGuard {
    guard_key_lo: u64,
    guard_key_hi: u64,
    entries: Vec<ShardEntry>,
}

impl LevelGuard {
    fn new(gk_lo: u64, gk_hi: u64) -> Self {
        LevelGuard {
            guard_key_lo: gk_lo,
            guard_key_hi: gk_hi,
            entries: Vec::new(),
        }
    }

    #[inline]
    fn guard_key(&self) -> u128 {
        ((self.guard_key_hi as u128) << 64) | (self.guard_key_lo as u128)
    }

    fn filenames(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.filename.clone()).collect()
    }
}

// ---------------------------------------------------------------------------
// FLSMLevel: one level in the LSM hierarchy (L1..L3)
// ---------------------------------------------------------------------------

struct FLSMLevel {
    level_num: usize,
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    fn new(level_num: usize) -> Self {
        FLSMLevel {
            level_num,
            guards: Vec::new(),
        }
    }

    fn find_guard_idx(&self, key: u128) -> Option<usize> {
        let n = self.guards.len();
        if n == 0 {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = n - 1;
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            if self.guards[mid].guard_key() <= key {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        if self.guards[lo].guard_key() <= key {
            Some(lo)
        } else {
            None
        }
    }

    fn find_guards_for_range(&self, range_min: u128, range_max: u128) -> Vec<usize> {
        let mut result = Vec::new();
        for i in 0..self.guards.len() {
            let gk = self.guards[i].guard_key();
            if gk > range_max {
                break;
            }
            let next_gk = if i + 1 < self.guards.len() {
                self.guards[i + 1].guard_key()
            } else {
                u128::MAX
            };
            if next_gk > range_min {
                result.push(i);
            }
        }
        result
    }

    fn insert_guard_sorted(&mut self, guard: LevelGuard) {
        let gk = guard.guard_key();
        let pos = self
            .guards
            .iter()
            .position(|g| g.guard_key() > gk)
            .unwrap_or(self.guards.len());
        self.guards.insert(pos, guard);
    }

    fn total_file_count(&self) -> usize {
        self.guards.iter().map(|g| g.entries.len()).sum()
    }

    fn get_or_create_guard(&mut self, gk_lo: u64, gk_hi: u64) -> &mut LevelGuard {
        let gk = ((gk_hi as u128) << 64) | (gk_lo as u128);
        let needs_new = match self.find_guard_idx(gk) {
            Some(idx) => self.guards[idx].guard_key() != gk,
            None => true,
        };
        if needs_new {
            self.insert_guard_sorted(LevelGuard::new(gk_lo, gk_hi));
        }
        let idx = self.find_guard_idx(gk).unwrap();
        &mut self.guards[idx]
    }
}

// ---------------------------------------------------------------------------
// ShardIndex: the opaque handle
// ---------------------------------------------------------------------------

pub struct ShardIndex {
    table_id: u32,
    output_dir: String,
    schema: SchemaDescriptor,

    l0: Vec<ShardEntry>,
    levels: Vec<FLSMLevel>,

    pub needs_compaction: bool,
    compact_seq: u64,
    pending_deletions: Vec<String>,
}

impl ShardIndex {
    pub fn new(table_id: u32, output_dir: &str, schema: SchemaDescriptor) -> Self {
        ShardIndex {
            table_id,
            output_dir: output_dir.to_string(),
            schema,
            l0: Vec::new(),
            levels: Vec::new(),
            needs_compaction: false,
            compact_seq: 0,
            pending_deletions: Vec::new(),
        }
    }

    // -------------------------------------------------------------------
    // L0 management
    // -------------------------------------------------------------------

    pub fn add_shard(&mut self, path: &str, min_lsn: u64, max_lsn: u64) -> Result<(), i32> {
        let entry = ShardEntry::open(path, &self.schema, min_lsn, max_lsn)?;
        self.l0.push(entry);
        self.sort_l0();
        self.update_flags();
        Ok(())
    }

    fn sort_l0(&mut self) {
        self.l0.sort_by_key(|e| e.pk_min);
    }

    fn update_flags(&mut self) {
        self.needs_compaction = self.l0.len() > L0_COMPACT_THRESHOLD;
    }

    // -------------------------------------------------------------------
    // Query: all shard pointers for cursor
    // -------------------------------------------------------------------

    pub fn all_shard_ptrs(&self) -> Vec<*const MappedShard> {
        let cap = self.l0.len() + self.levels.iter().map(|l| l.total_file_count()).sum::<usize>();
        let mut result = Vec::with_capacity(cap);
        for e in &self.l0 {
            result.push(&e.shard as *const MappedShard);
        }
        for level in &self.levels {
            for guard in &level.guards {
                for e in &guard.entries {
                    result.push(&e.shard as *const MappedShard);
                }
            }
        }
        result
    }

    // -------------------------------------------------------------------
    // Query: point lookup
    // -------------------------------------------------------------------

    pub fn find_pk(&self, key_lo: u64, key_hi: u64) -> Vec<(*const MappedShard, usize)> {
        let key = ((key_hi as u128) << 64) | (key_lo as u128);
        let mut results = Vec::with_capacity(2);

        for e in &self.l0 {
            if e.pk_min <= key && key <= e.pk_max {
                if e.shard.has_xor8() && !e.shard.xor8_may_contain(key_lo, key_hi) {
                    continue;
                }
                let row = e.shard.find_row_index(key_lo, key_hi);
                if row >= 0 {
                    results.push((&e.shard as *const MappedShard, row as usize));
                }
            }
        }

        for level in &self.levels {
            if let Some(g_idx) = level.find_guard_idx(key) {
                let guard = &level.guards[g_idx];
                for e in &guard.entries {
                    if e.pk_min <= key && key <= e.pk_max {
                        if e.shard.has_xor8() && !e.shard.xor8_may_contain(key_lo, key_hi) {
                            continue;
                        }
                        let row = e.shard.find_row_index(key_lo, key_hi);
                        if row >= 0 {
                            results.push((&e.shard as *const MappedShard, row as usize));
                        }
                    }
                }
            }
        }

        results
    }

    // -------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------

    pub fn max_lsn(&self) -> u64 {
        let mut result = 0u64;
        for e in &self.l0 {
            if e.max_lsn > result {
                result = e.max_lsn;
            }
        }
        for level in &self.levels {
            for guard in &level.guards {
                for e in &guard.entries {
                    if e.max_lsn > result {
                        result = e.max_lsn;
                    }
                }
            }
        }
        result
    }

    fn build_manifest_entries(&self) -> Vec<ManifestEntryRaw> {
        let mut entries = Vec::new();
        for e in &self.l0 {
            entries.push(self.entry_to_raw(e, 0, 0, 0));
        }
        for level in &self.levels {
            for guard in &level.guards {
                for e in &guard.entries {
                    entries.push(self.entry_to_raw(
                        e,
                        level.level_num as u64,
                        guard.guard_key_lo,
                        guard.guard_key_hi,
                    ));
                }
            }
        }
        entries
    }

    fn entry_to_raw(
        &self,
        e: &ShardEntry,
        level: u64,
        gk_lo: u64,
        gk_hi: u64,
    ) -> ManifestEntryRaw {
        let mut raw = ManifestEntryRaw::zeroed();
        raw.table_id = self.table_id as u64;
        raw.pk_min_lo = e.pk_min as u64;
        raw.pk_min_hi = (e.pk_min >> 64) as u64;
        raw.pk_max_lo = e.pk_max as u64;
        raw.pk_max_hi = (e.pk_max >> 64) as u64;
        raw.min_lsn = e.min_lsn;
        raw.max_lsn = e.max_lsn;
        raw.level = level;
        raw.guard_key_lo = gk_lo;
        raw.guard_key_hi = gk_hi;
        let name_bytes = e.filename.as_bytes();
        let len = name_bytes.len().min(127);
        raw.filename[..len].copy_from_slice(&name_bytes[..len]);
        raw
    }

    // -------------------------------------------------------------------
    // Manifest I/O
    // -------------------------------------------------------------------

    pub fn load_manifest(&mut self, path: &str) -> Result<(), i32> {
        let cpath = std::ffi::CString::new(path).map_err(|_| -1)?;
        let mut entries = vec![ManifestEntryRaw::zeroed(); 4096];
        let mut global_lsn = 0u64;
        let count = manifest::read_file(&cpath, &mut entries, 4096, &mut global_lsn);
        if count < 0 {
            if count == manifest::MANIFEST_ERR_IO {
                // File doesn't exist yet — empty index is fine
                return Ok(());
            }
            return Err(count);
        }

        for i in 0..count as usize {
            let raw = &entries[i];
            if raw.table_id != self.table_id as u64 {
                continue;
            }
            let filename = raw.filename_str().to_string();
            let entry = ShardEntry::open(
                &filename,
                &self.schema,
                raw.min_lsn,
                raw.max_lsn,
            )?;

            if raw.level == 0 {
                self.l0.push(entry);
            } else {
                let level_num = raw.level as usize;
                let level = self.get_or_create_level(level_num);
                level.get_or_create_guard(raw.guard_key_lo, raw.guard_key_hi).entries.push(entry);
            }
        }
        self.sort_l0();
        self.update_flags();
        Ok(())
    }

    pub fn publish_manifest(&self, path: &str) -> Result<(), i32> {
        let entries = self.build_manifest_entries();
        let global_lsn = self.max_lsn();
        let cpath = std::ffi::CString::new(path).map_err(|_| -1)?;
        let rc = manifest::write_file(&cpath, &entries, global_lsn);
        if rc != 0 {
            return Err(rc);
        }
        Ok(())
    }

    // -------------------------------------------------------------------
    // Level management
    // -------------------------------------------------------------------

    fn get_or_create_level(&mut self, level_num: usize) -> &mut FLSMLevel {
        self.ensure_level(level_num);
        &mut self.levels[level_num - 1]
    }

    fn ensure_level(&mut self, level_num: usize) {
        let idx = level_num - 1;
        while self.levels.len() <= idx {
            self.levels.push(FLSMLevel::new(self.levels.len() + 1));
        }
    }

    // -------------------------------------------------------------------
    // Compaction: L0 → L1
    // -------------------------------------------------------------------

    pub fn run_compact(&mut self) -> Result<(), i32> {
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

        // Determine guard keys for routing
        let guard_keys = self.l1_guard_keys();

        // Collect L0 file paths as CStrings
        let l0_cstrings: Vec<std::ffi::CString> = l0_filenames
            .iter()
            .map(|f| std::ffi::CString::new(f.as_str()).unwrap())
            .collect();
        let l0_cstrs: Vec<&CStr> = l0_cstrings.iter().map(|c| c.as_c_str()).collect();

        let out_dir = std::ffi::CString::new(self.output_dir.as_str()).unwrap();
        let result_cap = guard_keys.len().max(l0_filenames.len());
        let mut results: Vec<compact::GuardResult> = (0..result_cap)
            .map(|_| compact::GuardResult::zeroed())
            .collect();

        let num_results = compact::merge_and_route(
            &l0_cstrs,
            &out_dir,
            &guard_keys,
            &self.schema,
            self.table_id,
            1, // level 1
            lsn_tag,
            &mut results,
        );

        if num_results < 0 {
            return Err(num_results);
        }

        let guard_outputs: Vec<(u64, u64, String)> = results[..num_results as usize]
            .iter()
            .map(|r| {
                let fname = r.filename_str().to_string();
                (r.guard_key_lo, r.guard_key_hi, fname)
            })
            .collect();

        self.commit_l0_to_l1(&l0_filenames, &guard_outputs, l0_max_lsn)?;

        for fn_ in &l0_filenames {
            self.pending_deletions.push(fn_.clone());
        }

        // Phase 3: horizontal compaction
        self.compact_guards_if_needed()?;

        // Phase 4: vertical L1→L2 if overfull
        if !self.levels.is_empty() {
            let l1_count = self.levels[0].total_file_count();
            if l1_count > L1_TARGET_FILES {
                self.compact_guard_vertical(1)?;
            }
        }

        Ok(())
    }

    fn l1_guard_keys(&self) -> Vec<(u64, u64)> {
        if !self.levels.is_empty() && !self.levels[0].guards.is_empty() {
            self.levels[0]
                .guards
                .iter()
                .map(|g| (g.guard_key_lo, g.guard_key_hi))
                .collect()
        } else {
            // No L1 guards: derive from L0 min keys
            let mut keys: Vec<(u64, u64)> = Vec::new();
            for e in &self.l0 {
                let lo = e.pk_min as u64;
                let hi = (e.pk_min >> 64) as u64;
                if keys.is_empty() || keys.last().map(|k| *k != (lo, hi)).unwrap_or(true) {
                    keys.push((lo, hi));
                }
            }
            if keys.is_empty() {
                keys.push((0, 0));
            }
            keys
        }
    }

    fn commit_l0_to_l1(
        &mut self,
        l0_filenames: &[String],
        guard_outputs: &[(u64, u64, String)],
        max_lsn: u64,
    ) -> Result<(), i32> {
        // Remove compacted L0 entries
        self.l0.retain(|e| !l0_filenames.contains(&e.filename));

        // Install output handles into L1 guards
        self.ensure_level(1);
        let schema_copy = self.schema;
        for (gk_lo, gk_hi, filename) in guard_outputs {
            let entry = ShardEntry::open(filename, &schema_copy, 0, max_lsn)?;
            self.levels[0].get_or_create_guard(*gk_lo, *gk_hi).entries.push(entry);
        }

        self.update_flags();
        Ok(())
    }

    // -------------------------------------------------------------------
    // Horizontal compaction (within guard)
    // -------------------------------------------------------------------

    fn compact_guards_if_needed(&mut self) -> Result<(), i32> {
        for li in 0..self.levels.len() {
            let threshold = if self.levels[li].level_num == MAX_LEVELS - 1 {
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

    fn compact_one_guard(&mut self, level_idx: usize, guard_idx: usize) -> Result<(), i32> {
        self.compact_seq += 1;
        let guard = &self.levels[level_idx].guards[guard_idx];

        let guard_max_lsn = guard.entries.iter().map(|e| e.max_lsn).max().unwrap_or(0);
        let out_path = format!(
            "{}/hcomp_{}_L{}_G{}_{}.db",
            self.output_dir,
            self.table_id,
            self.levels[level_idx].level_num,
            guard.guard_key_lo,
            self.compact_seq,
        );

        let input_filenames = guard.filenames();
        let input_cstrings: Vec<std::ffi::CString> = input_filenames
            .iter()
            .map(|f| std::ffi::CString::new(f.as_str()).unwrap())
            .collect();
        let input_cstrs: Vec<&CStr> = input_cstrings.iter().map(|c| c.as_c_str()).collect();
        let out_cstr = std::ffi::CString::new(out_path.as_str()).unwrap();

        let rc = compact::compact_shards(&input_cstrs, &out_cstr, &self.schema, self.table_id);
        if rc != 0 {
            let _ = fs::remove_file(&out_path);
            return Err(rc);
        }

        let new_entry = ShardEntry::open(&out_path, &self.schema, 0, guard_max_lsn)?;

        for fn_ in &input_filenames {
            self.pending_deletions.push(fn_.clone());
        }

        self.levels[level_idx].guards[guard_idx].entries = vec![new_entry];

        Ok(())
    }

    // -------------------------------------------------------------------
    // Vertical compaction (L_n → L_n+1)
    // -------------------------------------------------------------------

    fn compact_guard_vertical(&mut self, src_level_num: usize) -> Result<(), i32> {
        if src_level_num < 1 || src_level_num >= MAX_LEVELS {
            return Ok(());
        }
        let src_idx = src_level_num - 1;

        // Phase A: select worst guard
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

        // Phase B: gather inputs
        let src_guard_key = self.levels[src_idx].guards[worst_idx].guard_key();
        let src_max_bound = if worst_idx + 1 < self.levels[src_idx].guards.len() {
            self.levels[src_idx].guards[worst_idx + 1].guard_key() - 1
        } else {
            u128::MAX
        };

        self.compact_seq += 1;
        let dest_idx = src_level_num; // dest level index in self.levels
        self.ensure_level(src_level_num + 1);

        // Collect all input files (src guard + overlapping dest guards)
        let mut all_input_files: Vec<String> = Vec::new();
        let src_filenames: Vec<String> = self.levels[src_idx].guards[worst_idx].filenames();
        all_input_files.extend(src_filenames.iter().cloned());

        let dest_guard_indices = self.levels[dest_idx].find_guards_for_range(src_guard_key, src_max_bound);
        let mut dest_guard_filenames: Vec<Vec<String>> = Vec::new();
        let mut vert_max_lsn = self.levels[src_idx].guards[worst_idx]
            .entries
            .iter()
            .map(|e| e.max_lsn)
            .max()
            .unwrap_or(0);

        for &di in &dest_guard_indices {
            let dg = &self.levels[dest_idx].guards[di];
            let fns = dg.filenames();
            all_input_files.extend(fns.iter().cloned());
            dest_guard_filenames.push(fns);
            for e in &dg.entries {
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

        // Determine guard keys for routing
        let guard_keys: Vec<(u64, u64)> = if !dest_guard_indices.is_empty() {
            dest_guard_indices
                .iter()
                .map(|&di| {
                    let g = &self.levels[dest_idx].guards[di];
                    (g.guard_key_lo, g.guard_key_hi)
                })
                .collect()
        } else {
            vec![(
                self.levels[src_idx].guards[worst_idx].guard_key_lo,
                self.levels[src_idx].guards[worst_idx].guard_key_hi,
            )]
        };

        // Phase C: compact
        let input_cstrings: Vec<std::ffi::CString> = all_input_files
            .iter()
            .map(|f| std::ffi::CString::new(f.as_str()).unwrap())
            .collect();
        let input_cstrs: Vec<&CStr> = input_cstrings.iter().map(|c| c.as_c_str()).collect();
        let out_dir = std::ffi::CString::new(self.output_dir.as_str()).unwrap();
        let result_cap = guard_keys.len().max(1);
        let mut results: Vec<compact::GuardResult> = (0..result_cap)
            .map(|_| compact::GuardResult::zeroed())
            .collect();

        let num_results = compact::merge_and_route(
            &input_cstrs,
            &out_dir,
            &guard_keys,
            &self.schema,
            self.table_id,
            src_level_num as u32 + 1,
            lsn_tag,
            &mut results,
        );

        if num_results < 0 {
            return Err(num_results);
        }

        let guard_outputs: Vec<(u64, u64, String)> = results[..num_results as usize]
            .iter()
            .map(|r| (r.guard_key_lo, r.guard_key_hi, r.filename_str().to_string()))
            .collect();

        // Phase D: tear down old handles
        // Mark src guard files for deletion
        for fn_ in &src_filenames {
            self.pending_deletions.push(fn_.clone());
        }
        // Mark dest guard files for deletion
        for fns in &dest_guard_filenames {
            for fn_ in fns {
                self.pending_deletions.push(fn_.clone());
            }
        }

        // Remove src guard
        self.levels[src_idx].guards.remove(worst_idx);

        // Remove old dest guards (by key match)
        {
            let old_keys: Vec<u128> = guard_keys.iter().map(|&(lo, hi)| ((hi as u128) << 64) | (lo as u128)).collect();
            self.levels[dest_idx]
                .guards
                .retain(|g| !old_keys.contains(&g.guard_key()));
        }

        // Phase E: install outputs
        let schema_copy = self.schema;
        for (gk_lo, gk_hi, filename) in &guard_outputs {
            let entry = ShardEntry::open(filename, &schema_copy, 0, vert_max_lsn)?;
            self.levels[dest_idx].get_or_create_guard(*gk_lo, *gk_hi).entries.push(entry);
        }

        self.update_flags();

        // Lazy leveling: Z=1 at Lmax
        if self.levels[dest_idx].level_num == MAX_LEVELS - 1 {
            let di = dest_idx;
            let mut gi = 0;
            while gi < self.levels[di].guards.len() {
                if self.levels[di].guards[gi].entries.len() > LMAX_FILE_THRESHOLD {
                    self.compact_one_guard(di, gi)?;
                }
                gi += 1;
            }
        }

        Ok(())
    }

    // -------------------------------------------------------------------
    // Cleanup: delete unreferenced shard files
    // -------------------------------------------------------------------

    pub fn try_cleanup(&mut self) -> usize {
        let mut deleted = 0;
        let mut remaining = Vec::new();

        for path in self.pending_deletions.drain(..) {
            match fs::remove_file(&path) {
                Ok(()) => { deleted += 1; }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => { deleted += 1; }
                Err(_) => { remaining.push(path); }
            }
        }

        self.pending_deletions = remaining;
        deleted
    }
}

