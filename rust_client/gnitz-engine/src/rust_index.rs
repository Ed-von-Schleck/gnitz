//! Shard index with FLSM tiered compaction (replaces index.py + flsm.py).

use std::ffi::CString;

use crate::compact::{self, SchemaDescriptor};
use crate::refcount::RefCounter;
use crate::shard_reader::MappedShard;
use crate::xor8;

const MAX_LEVELS: usize = 3;
const L0_COMPACT_THRESHOLD: usize = 4;
const GUARD_FILE_THRESHOLD: usize = 4;
const LMAX_FILE_THRESHOLD: usize = 1;
const L1_TARGET_FILES: usize = 16;

// ---------------------------------------------------------------------------
// ShardHandleEntry — owns a MappedShard + metadata
// ---------------------------------------------------------------------------

pub(crate) struct ShardHandleEntry {
    pub shard: MappedShard,
    pub filename: String,
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub pk_min: u128,
    pub pk_max: u128,
}

impl ShardHandleEntry {
    pub fn open(
        filename: &str,
        schema: &SchemaDescriptor,
        min_lsn: u64,
        max_lsn: u64,
        _validate_checksums: bool,
    ) -> Result<Self, i32> {
        let cpath = CString::new(filename).map_err(|_| -1)?;
        let shard = MappedShard::open(&cpath, schema, _validate_checksums)?;

        let (pk_min, pk_max) = if shard.count > 0 {
            (shard.get_pk(0), shard.get_pk(shard.count - 1))
        } else {
            (0u128, 0u128)
        };

        Ok(ShardHandleEntry {
            shard,
            filename: filename.to_string(),
            min_lsn,
            max_lsn,
            pk_min,
            pk_max,
        })
    }
}

// ---------------------------------------------------------------------------
// LevelGuard — a guard key boundary with its shard handles
// ---------------------------------------------------------------------------

struct LevelGuard {
    guard_key: u128,
    guard_key_lo: u64,
    guard_key_hi: u64,
    handles: Vec<ShardHandleEntry>,
}

impl LevelGuard {
    fn new(lo: u64, hi: u64) -> Self {
        LevelGuard {
            guard_key: ((hi as u128) << 64) | (lo as u128),
            guard_key_lo: lo,
            guard_key_hi: hi,
            handles: Vec::new(),
        }
    }

    fn needs_horizontal_compact(&self, threshold: usize) -> bool {
        self.handles.len() > threshold
    }
}

// ---------------------------------------------------------------------------
// FLSMLevel
// ---------------------------------------------------------------------------

struct FLSMLevel {
    level_num: usize,
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    fn new(level_num: usize) -> Self {
        FLSMLevel { level_num, guards: Vec::new() }
    }

    fn find_guard_idx(&self, key: u128) -> isize {
        let n = self.guards.len();
        if n == 0 { return -1; }
        let mut lo = 0usize;
        let mut hi = n - 1;
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            if self.guards[mid].guard_key <= key {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        if self.guards[lo].guard_key <= key { lo as isize } else { -1 }
    }

    fn find_guards_for_range(&self, range_min: u128, range_max: u128) -> Vec<usize> {
        let mut result = Vec::new();
        for (i, g) in self.guards.iter().enumerate() {
            if g.guard_key > range_max { break; }
            let next_gk = if i + 1 < self.guards.len() {
                self.guards[i + 1].guard_key
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
        let gk = guard.guard_key;
        let pos = self.guards.iter().position(|g| g.guard_key > gk);
        match pos {
            Some(i) => self.guards.insert(i, guard),
            None => self.guards.push(guard),
        }
    }

    fn total_file_count(&self) -> usize {
        self.guards.iter().map(|g| g.handles.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// RustIndex — L0 flat pool + FLSM levels
// ---------------------------------------------------------------------------

pub struct RustIndex {
    schema: SchemaDescriptor,
    pub(crate) table_id: u32,
    l0_handles: Vec<ShardHandleEntry>,
    levels: Vec<FLSMLevel>,
    output_dir: String,
    validate_checksums: bool,
    pub(crate) needs_compaction: bool,
    compact_seq: usize,
}

impl RustIndex {
    pub fn new(
        table_id: u32,
        schema: SchemaDescriptor,
        output_dir: &str,
        validate_checksums: bool,
    ) -> Self {
        RustIndex {
            schema,
            table_id,
            l0_handles: Vec::new(),
            levels: Vec::new(),
            output_dir: output_dir.to_string(),
            validate_checksums,
            needs_compaction: false,
            compact_seq: 0,
        }
    }

    pub fn add_l0_handle(&mut self, handle: ShardHandleEntry, rc: &mut RefCounter) {
        rc.acquire(&handle.filename).ok();
        self.l0_handles.push(handle);
        self.sort_l0();
        self.update_flags();
    }

    fn sort_l0(&mut self) {
        self.l0_handles.sort_by_key(|h| h.pk_min);
    }

    fn update_flags(&mut self) {
        self.needs_compaction = self.l0_handles.len() > L0_COMPACT_THRESHOLD;
    }

    /// All shard handles for cursor creation (L0 + all levels).
    pub fn all_shard_refs(&self) -> Vec<&MappedShard> {
        let mut result = Vec::new();
        for h in &self.l0_handles {
            result.push(&h.shard);
        }
        for level in &self.levels {
            for guard in &level.guards {
                for h in &guard.handles {
                    result.push(&h.shard);
                }
            }
        }
        result
    }

    /// PK lookup across L0 + all levels. Returns (net_weight, Option<(&MappedShard, row_idx)>).
    pub fn find_pk(&self, key_lo: u64, key_hi: u64) -> (i64, Option<(&MappedShard, usize)>) {
        let key = ((key_hi as u128) << 64) | (key_lo as u128);
        let mut total_w: i64 = 0;
        let mut found: Option<(&MappedShard, usize)> = None;

        for h in &self.l0_handles {
            if h.pk_min <= key && key <= h.pk_max {
                self.search_shard(&h.shard, key_lo, key_hi, &mut total_w, &mut found);
            }
        }

        for level in &self.levels {
            let g_idx = level.find_guard_idx(key);
            if g_idx < 0 { continue; }
            let guard = &level.guards[g_idx as usize];
            for h in &guard.handles {
                if h.pk_min <= key && key <= h.pk_max {
                    self.search_shard(&h.shard, key_lo, key_hi, &mut total_w, &mut found);
                }
            }
        }

        (total_w, found)
    }

    fn search_shard<'a>(
        &self,
        shard: &'a MappedShard,
        key_lo: u64, key_hi: u64,
        total_w: &mut i64,
        found: &mut Option<(&'a MappedShard, usize)>,
    ) {
        let idx = shard.find_lower_bound(key_lo, key_hi);
        let mut i = idx;
        while i < shard.count {
            if shard.get_pk_lo(i) != key_lo || shard.get_pk_hi(i) != key_hi {
                break;
            }
            *total_w += shard.get_weight(i);
            if found.is_none() {
                *found = Some((shard, i));
            }
            i += 1;
        }
    }

    /// Metadata for manifest serialization.
    pub fn get_metadata(&self) -> Vec<ManifestEntryData> {
        let mut result = Vec::new();
        for h in &self.l0_handles {
            result.push(ManifestEntryData {
                table_id: self.table_id,
                filename: h.filename.clone(),
                pk_min: h.pk_min,
                pk_max: h.pk_max,
                min_lsn: h.min_lsn,
                max_lsn: h.max_lsn,
                level: 0,
                guard_key_lo: 0,
                guard_key_hi: 0,
            });
        }
        for level in &self.levels {
            for guard in &level.guards {
                for h in &guard.handles {
                    result.push(ManifestEntryData {
                        table_id: self.table_id,
                        filename: h.filename.clone(),
                        pk_min: h.pk_min,
                        pk_max: h.pk_max,
                        min_lsn: h.min_lsn,
                        max_lsn: h.max_lsn,
                        level: level.level_num as u32,
                        guard_key_lo: guard.guard_key_lo,
                        guard_key_hi: guard.guard_key_hi,
                    });
                }
            }
        }
        result
    }

    pub fn max_lsn(&self) -> u64 {
        let mut result = 0u64;
        for h in &self.l0_handles {
            if h.max_lsn > result { result = h.max_lsn; }
        }
        for level in &self.levels {
            for guard in &level.guards {
                for h in &guard.handles {
                    if h.max_lsn > result { result = h.max_lsn; }
                }
            }
        }
        result
    }

    /// Load shards from parsed manifest entries.
    pub fn populate_from_entries(&mut self, entries: &[ManifestEntryData], rc: &mut RefCounter) {
        for entry in entries {
            if entry.table_id != self.table_id { continue; }
            let handle = match ShardHandleEntry::open(
                &entry.filename, &self.schema, entry.min_lsn, entry.max_lsn,
                self.validate_checksums,
            ) {
                Ok(h) => h,
                Err(_) => continue,
            };

            if entry.level == 0 {
                self.add_l0_handle(handle, rc);
            } else {
                let level = self.get_or_create_level(entry.level as usize);
                let gk = ((entry.guard_key_hi as u128) << 64) | (entry.guard_key_lo as u128);
                let g_idx = level.find_guard_idx(gk);
                if g_idx < 0 || level.guards[g_idx as usize].guard_key != gk {
                    level.insert_guard_sorted(LevelGuard::new(entry.guard_key_lo, entry.guard_key_hi));
                }
                let g_idx = level.find_guard_idx(gk);
                if g_idx >= 0 {
                    rc.acquire(&handle.filename).ok();
                    level.guards[g_idx as usize].handles.push(handle);
                }
            }
        }
    }

    fn get_or_create_level(&mut self, level_num: usize) -> &mut FLSMLevel {
        let idx = level_num - 1;
        while self.levels.len() <= idx {
            self.levels.push(FLSMLevel::new(self.levels.len() + 1));
        }
        &mut self.levels[idx]
    }

    /// Run L0→L1 compaction + cascading guard compaction.
    pub fn run_compact(&mut self, rc: &mut RefCounter) {
        if !self.needs_compaction { return; }

        self.compact_seq += 1;
        let mut l0_max_lsn = 0u64;
        let mut l0_files: Vec<String> = Vec::new();
        for h in &self.l0_handles {
            l0_files.push(h.filename.clone());
            if h.max_lsn > l0_max_lsn { l0_max_lsn = h.max_lsn; }
        }
        let lsn_tag = if l0_max_lsn > 0 { l0_max_lsn } else { self.compact_seq as u64 };

        // Collect existing L1 guard keys
        let l1 = self.get_or_create_level(1);
        let mut guard_keys: Vec<(u64, u64)> = Vec::new();
        for g in &l1.guards {
            guard_keys.push((g.guard_key_lo, g.guard_key_hi));
        }
        if guard_keys.is_empty() {
            guard_keys.push((0, 0));
        }

        // Build input file list as CStrings
        let input_cstrs: Vec<CString> = l0_files.iter()
            .map(|f| CString::new(f.as_str()).unwrap())
            .collect();
        let input_refs: Vec<&std::ffi::CStr> = input_cstrs.iter()
            .map(|c| c.as_c_str())
            .collect();

        let out_dir_c = CString::new(self.output_dir.as_str()).unwrap();

        let mut out_results = vec![compact::GuardResult::zeroed(); guard_keys.len()];
        let n_results = compact::merge_and_route(
            &input_refs, &out_dir_c, &guard_keys, &self.schema,
            self.table_id, 1, lsn_tag, &mut out_results,
        );

        if n_results < 0 {
            return; // compaction failed — don't mutate state
        }

        // Close and release old L0 handles
        for h in self.l0_handles.drain(..) {
            rc.release(&h.filename);
            rc.mark_for_deletion(&h.filename);
        }

        // Open output shard handles (before borrowing levels mutably)
        let schema = self.schema.clone();
        let vc = self.validate_checksums;
        let mut new_handles: Vec<(u64, u64, ShardHandleEntry)> = Vec::new();
        for i in 0..n_results as usize {
            let r = &out_results[i];
            let filename = r.filename_str();
            if let Ok(h) = ShardHandleEntry::open(&filename, &schema, 0, l0_max_lsn, vc) {
                new_handles.push((r.guard_key_lo, r.guard_key_hi, h));
            }
        }

        // Install into L1
        let l1 = self.get_or_create_level(1);
        for (gk_lo, gk_hi, handle) in new_handles {
            let gk = ((gk_hi as u128) << 64) | (gk_lo as u128);
            let g_idx = l1.find_guard_idx(gk);
            if g_idx < 0 || l1.guards[g_idx as usize].guard_key != gk {
                l1.insert_guard_sorted(LevelGuard::new(gk_lo, gk_hi));
            }
            let g_idx = l1.find_guard_idx(gk);
            if g_idx >= 0 {
                rc.acquire(&handle.filename).ok();
                l1.guards[g_idx as usize].handles.push(handle);
            }
        }

        self.update_flags();

        // Horizontal compaction within guards
        self.compact_guards_if_needed(rc);

        // L1→L2 if L1 is overfull
        if !self.levels.is_empty() && self.levels[0].total_file_count() > L1_TARGET_FILES {
            self.compact_guard_vertical(1, rc);
        }
    }

    fn compact_guards_if_needed(&mut self, rc: &mut RefCounter) {
        for li in 0..self.levels.len() {
            let threshold = if self.levels[li].level_num == MAX_LEVELS - 1 {
                LMAX_FILE_THRESHOLD
            } else {
                GUARD_FILE_THRESHOLD
            };
            let mut gi = 0;
            while gi < self.levels[li].guards.len() {
                if self.levels[li].guards[gi].needs_horizontal_compact(threshold) {
                    self.compact_one_guard(li, gi, rc);
                }
                gi += 1;
            }
        }
    }

    fn compact_one_guard(&mut self, level_idx: usize, guard_idx: usize, rc: &mut RefCounter) {
        let guard = &self.levels[level_idx].guards[guard_idx];
        self.compact_seq += 1;

        let mut guard_max_lsn = 0u64;
        let mut input_files: Vec<String> = Vec::new();
        for h in &guard.handles {
            input_files.push(h.filename.clone());
            if h.max_lsn > guard_max_lsn { guard_max_lsn = h.max_lsn; }
        }

        let out_path = format!("{}/hcomp_{}_L{}_{}.db",
            self.output_dir, self.table_id,
            self.levels[level_idx].level_num, self.compact_seq);

        let input_cstrs: Vec<CString> = input_files.iter()
            .map(|f| CString::new(f.as_str()).unwrap())
            .collect();
        let input_refs: Vec<&std::ffi::CStr> = input_cstrs.iter()
            .map(|c| c.as_c_str())
            .collect();
        let out_c = CString::new(out_path.as_str()).unwrap();

        let result = compact::compact_shards(&input_refs, &out_c, &self.schema, self.table_id);
        if result < 0 {
            return;
        }

        let new_handle = match ShardHandleEntry::open(
            &out_path, &self.schema, 0, guard_max_lsn, self.validate_checksums,
        ) {
            Ok(h) => h,
            Err(_) => return,
        };

        rc.acquire(&new_handle.filename).ok();

        // Close old handles
        let guard = &mut self.levels[level_idx].guards[guard_idx];
        for h in guard.handles.drain(..) {
            rc.release(&h.filename);
            rc.mark_for_deletion(&h.filename);
        }
        guard.handles.push(new_handle);
    }

    fn compact_guard_vertical(&mut self, _src_level_num: usize, _rc: &mut RefCounter) {
        // Vertical compaction (L1→L2) is complex. For now, defer to a separate
        // implementation step. The horizontal compaction above handles the most
        // common case.
        // TODO: implement vertical compaction
    }

    pub fn close_all(&mut self, rc: &mut RefCounter) {
        for h in self.l0_handles.drain(..) {
            rc.release(&h.filename);
        }
        for level in &mut self.levels {
            for guard in &mut level.guards {
                for h in guard.handles.drain(..) {
                    rc.release(&h.filename);
                }
            }
        }
        self.levels.clear();
    }
}

pub struct ManifestEntryData {
    pub table_id: u32,
    pub filename: String,
    pub pk_min: u128,
    pub pk_max: u128,
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub level: u32,
    pub guard_key_lo: u64,
    pub guard_key_hi: u64,
}

impl compact::GuardResult {
    fn zeroed() -> Self {
        compact::GuardResult {
            guard_key_lo: 0,
            guard_key_hi: 0,
            filename: [0u8; 256],
        }
    }

    fn filename_str(&self) -> String {
        let end = self.filename.iter().position(|&b| b == 0).unwrap_or(256);
        String::from_utf8_lossy(&self.filename[..end]).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::SchemaColumn;
    use crate::shard_file;

    fn make_schema() -> SchemaDescriptor {
        let mut cols = [SchemaColumn { type_code: 8, size: 8, nullable: 0, _pad: 0 }; 64];
        cols[1] = SchemaColumn { type_code: 9, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns: cols }
    }

    fn write_test_shard(dir: &std::path::Path, name: &str, pks: &[u64], vals: &[i64]) -> String {
        let path = dir.join(name);
        let cpath = CString::new(path.to_str().unwrap()).unwrap();
        let count = pks.len();
        let pk_hi = vec![0u64; count];
        let weights = vec![1i64; count];
        let nulls = vec![0u64; count];

        fn as_bytes<T>(v: &[T]) -> &[u8] {
            unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * std::mem::size_of::<T>()) }
        }
        let regions: Vec<&[u8]> = vec![
            as_bytes(pks), as_bytes(&pk_hi), as_bytes(&weights), as_bytes(&nulls),
            as_bytes(vals), &[],
        ];
        shard_file::write_shard(
            libc::AT_FDCWD, &cpath, 1, count,
            &regions, pks, &pk_hi, false,
        ).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn add_and_find() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema();
        let mut rc = RefCounter::new();
        let mut idx = RustIndex::new(1, schema.clone(), dir.path().to_str().unwrap(), false);

        let f = write_test_shard(dir.path(), "s1.db", &[10, 20, 30], &[100, 200, 300]);
        let handle = ShardHandleEntry::open(&f, &schema, 0, 1, false).unwrap();
        idx.add_l0_handle(handle, &mut rc);

        let (w, found) = idx.find_pk(20, 0);
        assert_eq!(w, 1);
        assert!(found.is_some());

        let (w, found) = idx.find_pk(99, 0);
        assert_eq!(w, 0);
        assert!(found.is_none());

        idx.close_all(&mut rc);
    }
}
