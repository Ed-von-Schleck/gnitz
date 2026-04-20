//! FLSM Shard Index: manages shard lifecycle, compaction, and manifest I/O.

use std::ffi::{CStr, CString};
use std::fs;
use std::sync::Arc;

use super::compact;
use crate::schema::SchemaDescriptor;
use super::manifest::{self, ManifestEntryRaw};
use super::shard_reader::MappedShard;
use crate::util::{make_pk, split_pk};

const MAX_LEVELS: usize = 3;
const L0_COMPACT_THRESHOLD: usize = 4;
const GUARD_FILE_THRESHOLD: usize = 4;
const LMAX_FILE_THRESHOLD: usize = 1;
const L1_TARGET_FILES: usize = 16;

fn to_cstrings(strings: &[String]) -> Result<Vec<CString>, i32> {
    strings
        .iter()
        .map(|f| CString::new(f.as_str()).map_err(|_| -1))
        .collect()
}

struct ShardEntry {
    shard: Arc<MappedShard>,
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
        let cpath = CString::new(path).map_err(|_| -1)?;
        let shard = Arc::new(MappedShard::open(&cpath, schema, false)?);
        // Empty shard: pk_min > pk_max so range checks always fail
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

    fn probe_pk(&self, key: u128) -> Option<(Arc<MappedShard>, usize)> {
        if self.pk_min <= key && key <= self.pk_max {
            if self.shard.has_xor8() && !self.shard.xor8_may_contain(key) {
                return None;
            }
            let row = self.shard.find_row_index(key);
            if row >= 0 {
                return Some((Arc::clone(&self.shard), row as usize));
            }
        }
        None
    }
}

struct LevelGuard {
    guard_key: u128,
    entries: Vec<ShardEntry>,
}

impl LevelGuard {
    fn new(gk: u128) -> Self {
        LevelGuard {
            guard_key: gk,
            entries: Vec::new(),
        }
    }
}

struct FLSMLevel {
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    fn new() -> Self {
        FLSMLevel { guards: Vec::new() }
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
            if self.guards[mid].guard_key <= key {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        if self.guards[lo].guard_key <= key {
            Some(lo)
        } else {
            None
        }
    }

    fn find_guards_for_range(&self, range_min: u128, range_max: u128) -> Vec<usize> {
        let start = match self.guards.partition_point(|g| g.guard_key <= range_min) {
            0 => 0,
            n => n - 1,
        };
        let mut result = Vec::new();
        for i in start..self.guards.len() {
            let gk = self.guards[i].guard_key;
            if gk > range_max {
                break;
            }
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

    fn total_file_count(&self) -> usize {
        self.guards.iter().map(|g| g.entries.len()).sum()
    }

    fn get_or_create_guard(&mut self, gk: u128) -> &mut LevelGuard {
        let pos = self.guards.partition_point(|g| g.guard_key < gk);
        if pos < self.guards.len() && self.guards[pos].guard_key == gk {
            return &mut self.guards[pos];
        }
        self.guards.insert(pos, LevelGuard::new(gk));
        &mut self.guards[pos]
    }
}

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

    fn all_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.l0.iter().chain(
            self.levels
                .iter()
                .flat_map(|l| l.guards.iter().flat_map(|g| g.entries.iter())),
        )
    }

    fn level_num(level_idx: usize) -> usize {
        level_idx + 1
    }

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

    pub fn all_shard_arcs(&self) -> Vec<Arc<MappedShard>> {
        self.all_entries()
            .map(|e| Arc::clone(&e.shard))
            .collect()
    }

    pub fn find_pk(&self, key: u128, visitor: &mut impl FnMut(Arc<MappedShard>, usize)) {
        for e in &self.l0 {
            if let Some((arc, idx)) = e.probe_pk(key) {
                visitor(arc, idx);
            }
        }

        for level in &self.levels {
            if let Some(g_idx) = level.find_guard_idx(key) {
                for e in &level.guards[g_idx].entries {
                    if let Some((arc, idx)) = e.probe_pk(key) {
                        visitor(arc, idx);
                    }
                }
            }
        }
    }

    pub fn max_lsn(&self) -> u64 {
        self.all_entries().map(|e| e.max_lsn).max().unwrap_or(0)
    }

    fn build_manifest_entries(&self) -> Vec<ManifestEntryRaw> {
        let mut entries = Vec::new();
        for e in &self.l0 {
            entries.push(self.entry_to_raw(e, 0, 0));
        }
        for (li, level) in self.levels.iter().enumerate() {
            for guard in &level.guards {
                for e in &guard.entries {
                    entries.push(self.entry_to_raw(
                        e,
                        Self::level_num(li) as u64,
                        guard.guard_key,
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
        gk: u128,
    ) -> ManifestEntryRaw {
        let mut raw = ManifestEntryRaw::zeroed();
        raw.table_id = self.table_id as u64;
        let (min_lo, min_hi) = split_pk(e.pk_min);
        let (max_lo, max_hi) = split_pk(e.pk_max);
        raw.pk_min_lo = min_lo;
        raw.pk_min_hi = min_hi;
        raw.pk_max_lo = max_lo;
        raw.pk_max_hi = max_hi;
        raw.min_lsn = e.min_lsn;
        raw.max_lsn = e.max_lsn;
        raw.level = level;
        let (gk_lo, gk_hi) = split_pk(gk);
        raw.guard_key_lo = gk_lo;
        raw.guard_key_hi = gk_hi;
        let name_bytes = e.filename.as_bytes();
        let len = name_bytes.len().min(127);
        raw.filename[..len].copy_from_slice(&name_bytes[..len]);
        raw
    }

    pub fn load_manifest(&mut self, path: &str) -> Result<(), i32> {
        let cpath = CString::new(path).map_err(|_| -1)?;
        let cap = manifest::entry_count(&cpath);
        if cap < 0 {
            if cap == manifest::MANIFEST_ERR_IO {
                return Ok(());
            }
            return Err(cap);
        }
        let cap = (cap as usize).max(1);
        let mut entries = vec![ManifestEntryRaw::zeroed(); cap];
        let mut global_lsn = 0u64;
        let count = manifest::read_file(&cpath, &mut entries, cap as u32, &mut global_lsn);
        if count < 0 {
            if count == manifest::MANIFEST_ERR_IO {
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
            let entry = ShardEntry::open(&filename, &self.schema, raw.min_lsn, raw.max_lsn)?;

            if raw.level == 0 {
                self.l0.push(entry);
            } else {
                let level_num = raw.level as usize;
                let level = self.get_or_create_level(level_num);
                level
                    .get_or_create_guard(make_pk(raw.guard_key_lo, raw.guard_key_hi))
                    .entries
                    .push(entry);
            }
        }
        self.sort_l0();
        self.update_flags();
        Ok(())
    }

    pub fn publish_manifest(&self, path: &str) -> Result<(), i32> {
        let entries = self.build_manifest_entries();
        let global_lsn = self.max_lsn();
        let cpath = CString::new(path).map_err(|_| -1)?;
        let rc = manifest::write_file(&cpath, &entries, global_lsn);
        if rc != 0 {
            return Err(rc);
        }
        Ok(())
    }

    fn get_or_create_level(&mut self, level_num: usize) -> &mut FLSMLevel {
        self.ensure_level(level_num);
        &mut self.levels[level_num - 1]
    }

    fn ensure_level(&mut self, level_num: usize) {
        let idx = level_num - 1;
        while self.levels.len() <= idx {
            self.levels.push(FLSMLevel::new());
        }
    }

    /// Synchronous compaction: all file writes use pwrite+fdatasync
    /// inside `shard_file::write_shard_streaming`, not io_uring. When
    /// this function returns, no I/O activity related to this run
    /// remains in flight — callers may rmtree the owning directory
    /// immediately without draining anything. This invariant is
    /// load-bearing for the schema-migration swap path, which rmtrees
    /// dropped-table directories right after the Phase-4 ACKs.
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

        let guard_keys = self.l1_guard_keys();

        let l0_cstrings = to_cstrings(&l0_filenames)?;
        let l0_cstrs: Vec<&CStr> = l0_cstrings.iter().map(|c| c.as_c_str()).collect();

        let out_dir = CString::new(self.output_dir.as_str()).map_err(|_| -1)?;
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
        );

        if num_results < 0 {
            return Err(num_results);
        }

        let guard_outputs: Vec<(u128, String)> = results[..num_results as usize]
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

    fn l1_guard_keys(&self) -> Vec<u128> {
        if !self.levels.is_empty() && !self.levels[0].guards.is_empty() {
            self.levels[0]
                .guards
                .iter()
                .map(|g| g.guard_key)
                .collect()
        } else {
            let mut keys: Vec<u128> = Vec::new();
            for e in &self.l0 {
                if keys.is_empty() || keys.last().map(|k| *k != e.pk_min).unwrap_or(true) {
                    keys.push(e.pk_min);
                }
            }
            if keys.is_empty() {
                keys.push(0);
            }
            keys
        }
    }

    fn commit_l0_to_l1(
        &mut self,
        guard_outputs: &[(u128, String)],
        max_lsn: u64,
    ) -> Result<(), i32> {
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

    fn compact_guards_if_needed(&mut self) -> Result<(), i32> {
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

    fn compact_one_guard(&mut self, level_idx: usize, guard_idx: usize) -> Result<(), i32> {
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
        let out_cstr = CString::new(out_path.as_str()).map_err(|_| -1)?;

        let rc = compact::compact_shards(&input_cstrs, &out_cstr, &self.schema, self.table_id);
        if rc != 0 {
            let _ = fs::remove_file(&out_path);
            return Err(rc);
        }

        let new_entry = ShardEntry::open(&out_path, &self.schema, 0, guard_max_lsn)?;

        self.pending_deletions.extend(input_filenames);

        self.levels[level_idx].guards[guard_idx].entries = vec![new_entry];

        Ok(())
    }

    fn compact_guard_vertical(&mut self, src_level_num: usize) -> Result<(), i32> {
        if src_level_num < 1 || src_level_num >= MAX_LEVELS {
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
            self.levels[src_idx].guards[worst_idx + 1].guard_key - 1
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
        let mut dest_file_start_indices: Vec<usize> = Vec::new();
        let mut vert_max_lsn = self.levels[src_idx].guards[worst_idx]
            .entries
            .iter()
            .map(|e| e.max_lsn)
            .max()
            .unwrap_or(0);

        for &di in &dest_guard_indices {
            let dg = &self.levels[dest_idx].guards[di];
            dest_file_start_indices.push(all_input_files.len());
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
        let out_dir = CString::new(self.output_dir.as_str()).map_err(|_| -1)?;
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
        );

        if num_results < 0 {
            return Err(num_results);
        }

        let guard_outputs: Vec<(u128, String)> = results[..num_results as usize]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use super::super::shard_file;

    fn test_schema() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn::new(0, 0); 64],
        };
        // col 0 = PK (U64, size 8)
        sd.columns[0] = SchemaColumn::new(type_code::U64, 0);
        // col 1 = payload (I64, size 8)
        sd.columns[1] = SchemaColumn::new(type_code::I64, 0);
        sd
    }

    /// Build and write a shard file with the given PK/value pairs. All pk_hi = 0.
    fn write_test_shard(
        dir: &std::path::Path,
        name: &str,
        pk_los: &[u64],
        values: &[i64],
    ) -> String {
        let n = pk_los.len();
        let pk_hi = vec![0u64; n];
        let weights = vec![1i64; n];
        let nulls = vec![0u64; n];
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_los.as_ptr() as *const u8, n * 8),
            (pk_hi.as_ptr() as *const u8, n * 8),
            (weights.as_ptr() as *const u8, n * 8),
            (nulls.as_ptr() as *const u8, n * 8),
            (values.as_ptr() as *const u8, n * 8),
            (blob.as_ptr(), 0),
        ];

        let image = shard_file::build_shard_image(42, n as u32, &regions);
        let path = dir.join(name);
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        let rc = shard_file::write_shard_at(libc::AT_FDCWD, &cpath, &image, false);
        assert_eq!(rc, 0);
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn test_add_shard_and_find_pk() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        let path1 = write_test_shard(dir.path(), "s1.db", &[10, 20, 30], &[100, 200, 300]);
        let path2 = write_test_shard(dir.path(), "s2.db", &[25, 35, 40], &[250, 350, 400]);

        idx.add_shard(&path1, 1, 10).unwrap();
        idx.add_shard(&path2, 11, 20).unwrap();

        // Find existing keys
        let mut hits = Vec::new();
        idx.find_pk(10, &mut |ptr, row| hits.push((ptr, row)));
        assert_eq!(hits.len(), 1);

        hits.clear();
        idx.find_pk(25, &mut |ptr, row| hits.push((ptr, row)));
        assert_eq!(hits.len(), 1);

        // Missing key returns nothing
        hits.clear();
        idx.find_pk(99, &mut |ptr, row| hits.push((ptr, row)));
        assert!(hits.is_empty());
    }

    #[test]
    fn test_manifest_roundtrip_with_levels() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Add enough shards to trigger compaction to L1
        for i in 0..5u64 {
            let name = format!("s{}.db", i);
            let pk = i * 10 + 1;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 100]);
            idx.add_shard(&path, i, i + 1).unwrap();
        }
        idx.run_compact().unwrap();

        // Publish manifest
        let manifest_path = dir.path().join("MANIFEST");
        idx.publish_manifest(manifest_path.to_str().unwrap()).unwrap();

        // Load into a fresh index
        let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);
        idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();

        // Verify all keys are findable in the new index
        for i in 0..5u64 {
            let pk = (i * 10 + 1) as u128;
            let mut found = false;
            idx2.find_pk(pk, &mut |_, _| found = true);
            assert!(found, "key {} not found after manifest roundtrip", pk);
        }

        assert_eq!(idx.max_lsn(), idx2.max_lsn());
    }

    #[test]
    fn test_run_compact_l0_to_l1() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Add > L0_COMPACT_THRESHOLD shards
        let mut all_pks = Vec::new();
        for i in 0..5u64 {
            let name = format!("s{}.db", i);
            let pk = (i + 1) * 10;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64]);
            idx.add_shard(&path, i, i + 1).unwrap();
            all_pks.push(pk);
        }

        assert!(idx.needs_compaction);
        idx.run_compact().unwrap();

        // L0 should be empty after compaction
        assert!(idx.l0.is_empty());
        // L1 should have entries
        assert!(!idx.levels.is_empty());
        assert!(idx.levels[0].total_file_count() > 0);

        // All keys still findable
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "key {} lost after compaction", pk);
        }
    }

    #[test]
    fn test_compact_guards_if_needed() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Manually populate L1 with > GUARD_FILE_THRESHOLD entries in one guard
        idx.ensure_level(1);
        let guard = idx.levels[0].get_or_create_guard(0);
        let mut all_pks = Vec::new();
        for i in 0..6u64 {
            let name = format!("guard_s{}.db", i);
            let pk = i + 1;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
            let entry = ShardEntry::open(&path, &schema, 0, 100).unwrap();
            guard.entries.push(entry);
            all_pks.push(pk);
        }
        assert!(idx.levels[0].guards[0].entries.len() > GUARD_FILE_THRESHOLD);

        idx.compact_guards_if_needed().unwrap();

        // After compaction the guard should have 1 file
        assert_eq!(idx.levels[0].guards[0].entries.len(), 1);

        // All keys still findable
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "key {} lost after guard compaction", pk);
        }
    }

    #[test]
    fn test_compact_guard_vertical_failure_leaves_index_unchanged() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);
        idx.ensure_level(1);

        // Build a guard at key 0 with 3 entries (max_lsn=100 → lsn_tag=100)
        for i in 0..3u64 {
            let path = write_test_shard(
                dir.path(),
                &format!("src_{}.db", i),
                &[i + 1],
                &[(i as i64 + 1) * 10],
            );
            let e = ShardEntry::open(&path, &schema, 0, 100).unwrap();
            idx.levels[0].get_or_create_guard(0).entries.push(e);
        }
        // Second guard so worst_count > 1 condition is met in compact_guard_vertical
        {
            let path = write_test_shard(dir.path(), "other.db", &[9999], &[42]);
            let e = ShardEntry::open(&path, &schema, 0, 50).unwrap();
            idx.levels[0].get_or_create_guard(5000).entries.push(e);
        }

        // Block the output path: shard_42_100_L2_G0.db must fail to finalize
        let blocker = dir.path().join("shard_42_100_L2_G0.db");
        std::fs::create_dir_all(&blocker).unwrap();

        let pre_guard_count = idx.levels[0].guards.len();
        let pre_entries = idx.levels[0].guards[0].entries.len();

        let result = idx.compact_guard_vertical(1);
        assert!(result.is_err(), "expected Err when output path is blocked");
        assert_eq!(idx.pending_deletions.len(), 0, "no input files should be queued on failure");
        assert_eq!(
            idx.levels[0].guards.len(),
            pre_guard_count,
            "src guards must be unchanged on failure"
        );
        assert_eq!(
            idx.levels[0].guards[0].entries.len(),
            pre_entries,
            "src entries must be unchanged on failure"
        );
    }

    #[test]
    fn test_find_guards_for_range() {
        let mut level = FLSMLevel::new();
        // Guards at keys 0, 100, 200, 300
        for gk in [0u64, 100, 200, 300] {
            level.guards.push(LevelGuard::new(gk as u128));
        }

        // Range entirely within guard 0
        let r = level.find_guards_for_range(make_pk(10, 0), make_pk(50, 0));
        assert_eq!(r, vec![0]);

        // Range spanning guards 1 and 2
        let r = level.find_guards_for_range(make_pk(100, 0), make_pk(250, 0));
        assert_eq!(r, vec![1, 2]);

        // Range spanning all guards
        let r = level.find_guards_for_range(make_pk(0, 0), make_pk(999, 0));
        assert_eq!(r, vec![0, 1, 2, 3]);

        // Point query at exact guard boundary
        let r = level.find_guards_for_range(make_pk(200, 0), make_pk(200, 0));
        assert_eq!(r, vec![2]);

        // Range below all guards still hits guard 0 (partition_point - 1)
        let r = level.find_guards_for_range(make_pk(0, 0), make_pk(0, 0));
        assert_eq!(r, vec![0]);

        // No guards at all
        let empty = FLSMLevel::new();
        let r = empty.find_guards_for_range(make_pk(0, 0), make_pk(100, 0));
        assert!(r.is_empty());
    }

    #[test]
    fn test_try_cleanup() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Create real files
        let path1 = write_test_shard(dir.path(), "cleanup1.db", &[1], &[10]);
        let path2 = write_test_shard(dir.path(), "cleanup2.db", &[2], &[20]);
        assert!(std::path::Path::new(&path1).exists());
        assert!(std::path::Path::new(&path2).exists());

        // Add real + nonexistent to pending deletions
        idx.pending_deletions.push(path1.clone());
        idx.pending_deletions.push(path2.clone());
        idx.pending_deletions
            .push(dir.path().join("nonexistent.db").to_str().unwrap().to_string());

        let deleted = idx.try_cleanup();
        // All 3 should count as deleted (2 real + 1 NotFound)
        assert_eq!(deleted, 3);
        assert!(idx.pending_deletions.is_empty());
        assert!(!std::path::Path::new(&path1).exists());
        assert!(!std::path::Path::new(&path2).exists());
    }

    #[test]
    fn test_max_lsn() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        assert_eq!(idx.max_lsn(), 0);

        let path1 = write_test_shard(dir.path(), "lsn1.db", &[10], &[100]);
        idx.add_shard(&path1, 5, 50).unwrap();
        assert_eq!(idx.max_lsn(), 50);

        let path2 = write_test_shard(dir.path(), "lsn2.db", &[20], &[200]);
        idx.add_shard(&path2, 100, 200).unwrap();
        assert_eq!(idx.max_lsn(), 200);

        let path3 = write_test_shard(dir.path(), "lsn3.db", &[30], &[300]);
        idx.add_shard(&path3, 10, 75).unwrap();
        // max_lsn should still be 200 (from second shard)
        assert_eq!(idx.max_lsn(), 200);
    }

    #[test]
    fn test_run_compact_fails_on_long_path_l0_intact() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();

        // Build an output dir long enough that output filenames exceed 255 bytes.
        // Filename overhead: "/shard_42_N_L1_GN.db" ≈ 20 bytes, so out_dir >= 236 bytes.
        // A 240-char subdir is within NAME_MAX (255) and guarantees the total overflows.
        let long_subdir = "a".repeat(240);
        let long_out_dir = dir.path().join(&long_subdir);
        std::fs::create_dir_all(&long_out_dir).unwrap();
        let long_out_str = long_out_dir.to_str().unwrap();
        assert!(
            long_out_str.len() >= 236,
            "test setup: out_dir too short ({})",
            long_out_str.len()
        );

        let mut idx = ShardIndex::new(42, long_out_str, schema);

        // Add L0_COMPACT_THRESHOLD + 1 shards (triggers compaction).
        let mut all_pks = Vec::new();
        for i in 0..5u64 {
            let pk = (i + 1) * 10;
            let path = write_test_shard(dir.path(), &format!("s{}.db", i), &[pk], &[pk as i64]);
            idx.add_shard(&path, i, i + 1).unwrap();
            all_pks.push(pk);
        }

        assert!(idx.needs_compaction);
        let l0_before = idx.l0.len();

        let result = idx.run_compact();
        assert!(result.is_err(), "expected Err when output path exceeds 255 bytes");

        // L0 must be unchanged — atomicity fix ensures this.
        assert_eq!(idx.l0.len(), l0_before, "L0 must not be modified on failure");

        // needs_compaction must still be true (update_flags was not called).
        assert!(idx.needs_compaction, "needs_compaction must remain true after failure");

        // All original keys must still be findable via L0.
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "pk {} lost from L0 after failed run_compact", pk);
        }
    }

    /// Bug 1: When L1 guard at key=100 is vertically compacted into L2 that has
    /// a guard at key=200, the routing must cover keys below 200 (the source
    /// range's lower bound). Without the fix, keys 100-199 become unfindable.
    #[test]
    fn test_compact_guard_vertical_routing_gap() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Build L1 (levels[0]) and L2 (levels[1])
        idx.ensure_level(2);

        // L1 guard at key=100: 5 shards (> GUARD_FILE_THRESHOLD=4) with keys in [100, 199]
        let src_pks: Vec<u64> = vec![100, 120, 140, 160, 180];
        for (i, &pk) in src_pks.iter().enumerate() {
            let name = format!("src_{}.db", i);
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
            let entry = ShardEntry::open(&path, &schema, 0, 100).unwrap();
            idx.levels[0].get_or_create_guard(100).entries.push(entry);
        }

        // L1 guard at key=500: 1 shard (so worst_guard picks key=100)
        {
            let path = write_test_shard(dir.path(), "high.db", &[500], &[5000]);
            let entry = ShardEntry::open(&path, &schema, 0, 50).unwrap();
            idx.levels[0].get_or_create_guard(500).entries.push(entry);
        }

        // L2 guard at key=200: 1 shard with key=250
        {
            let path = write_test_shard(dir.path(), "dest.db", &[250], &[2500]);
            let entry = ShardEntry::open(&path, &schema, 0, 80).unwrap();
            idx.levels[1].get_or_create_guard(200).entries.push(entry);
        }

        // Compact L1 → L2 (src_level_num=1)
        idx.compact_guard_vertical(1).unwrap();

        // All source keys (100-180) must be findable — they should not be lost
        // to the routing gap below L2's guard at 200.
        for &pk in &src_pks {
            let mut found = false;
            idx.find_pk(pk as u128, &mut |_, _| found = true);
            assert!(found, "key {} lost after vertical compaction (routing gap bug)", pk);
        }

        // The destination key 250 must also still be present
        let mut found_250 = false;
        idx.find_pk(250, &mut |_, _| found_250 = true);
        assert!(found_250, "destination key 250 lost after vertical compaction");
    }
}
