//! Manifest serialize / load / recover for [`ShardIndex`]: building manifest
//! entries, loading + reopening shards, orphan GC, atomic publish, and the
//! two-phase pending-shard manifest preparation.

use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::rc::Rc;

use super::super::error::StorageError;
use super::super::manifest::{self, ManifestEntryRaw, PreparedManifest};
use super::super::shard_reader::MappedShard;
use super::{ShardEntry, ShardIndex, MAX_LEVELS};

impl ShardIndex {
    fn build_manifest_entries(&self) -> Vec<ManifestEntryRaw> {
        let mut entries = Vec::new();
        for e in &self.l0 {
            entries.push(self.entry_to_raw(e, 0, 0));
        }
        for (li, level) in self.levels.iter().enumerate() {
            for guard in &level.guards {
                for e in &guard.entries {
                    entries.push(self.entry_to_raw(e, Self::level_num(li) as u64, guard.guard_key));
                }
            }
        }
        entries
    }

    fn entry_to_raw(&self, e: &ShardEntry, level: u64, gk: u128) -> ManifestEntryRaw {
        let mut raw = ManifestEntryRaw::zeroed();
        raw.table_id = self.table_id as u64;
        raw.pk_min = e.pk_min;
        raw.pk_max = e.pk_max;
        raw.min_lsn = e.min_lsn;
        raw.max_lsn = e.max_lsn;
        raw.level = level;
        raw.guard_key = gk;
        let name_bytes = e.filename.as_bytes();
        let len = name_bytes.len().min(127);
        raw.filename[..len].copy_from_slice(&name_bytes[..len]);
        raw
    }

    pub fn load_manifest(&mut self, path: &str) -> Result<(), StorageError> {
        let cpath = CString::new(path).map_err(|_| StorageError::InvalidPath)?;
        // Missing manifest file ⇒ first-time table boot, treat as empty.
        // Other read errors propagate.
        let cap = match manifest::entry_count(&cpath)? {
            Some(n) => n.max(1),
            None => return Ok(()),
        };
        let mut entries = vec![ManifestEntryRaw::zeroed(); cap];
        let mut global_lsn = 0u64;
        let count = manifest::read_file(&cpath, &mut entries, cap as u32, &mut global_lsn)?;

        for raw in entries.iter().take(count) {
            if raw.table_id != self.table_id as u64 {
                continue;
            }
            let filename = raw.filename_str().to_string();
            let entry = ShardEntry::open(&filename, &self.schema, raw.min_lsn, raw.max_lsn)?;

            if raw.level == 0 {
                self.l0.push(entry);
            } else {
                // Bound the level before `get_or_create_level` (which calls
                // `ensure_level`): a corrupted manifest with an arbitrary level
                // would otherwise allocate thousands of empty FLSMLevels and
                // crash the engine at startup.
                if raw.level >= MAX_LEVELS as u64 {
                    return Err(StorageError::InvalidVersion);
                }
                let level_num = raw.level as usize;
                let level = self.get_or_create_level(level_num);
                level.get_or_create_guard(raw.guard_key).entries.push(entry);
            }
        }
        self.sort_l0();
        self.update_flags();
        Ok(())
    }

    /// Startup GC: removes orphaned shard/compaction files and stale `.tmp`
    /// artifacts left by crashes.  Must run after a successful load_manifest()
    /// so the live set is populated before files are deleted.
    pub fn gc_orphans(&self) -> usize {
        let shard_prefix = format!("shard_{}_", self.table_id);
        let hcomp_prefix = format!("hcomp_{}_", self.table_id);

        let live: HashSet<&str> = self
            .all_entries()
            .filter_map(|e| std::path::Path::new(&e.filename).file_name().and_then(|n| n.to_str()))
            .collect();

        let mut removed = 0usize;

        // Stray manifest .tmp from a crash mid-publish.
        let manifest_tmp = format!("{}/manifest.bin.tmp", self.output_dir);
        if std::fs::remove_file(&manifest_tmp).is_ok() {
            removed += 1;
        }

        let Ok(rd) = std::fs::read_dir(&self.output_dir) else {
            return removed;
        };
        for entry in rd.flatten() {
            let name_os = entry.file_name();
            let Some(name) = name_os.to_str() else {
                continue;
            };
            if (name.starts_with(&shard_prefix) || name.starts_with(&hcomp_prefix)) && !live.contains(name) {
                let _ = std::fs::remove_file(entry.path());
                removed += 1;
            }
        }
        removed
    }

    pub fn publish_manifest(&self, path: &str) -> Result<(), StorageError> {
        let entries = self.build_manifest_entries();
        let global_lsn = self.max_lsn();
        let cpath = CString::new(path).map_err(|_| StorageError::InvalidPath)?;
        manifest::write_file(&cpath, &entries, global_lsn)
    }

    /// Open a shard mmap from `tmp_path` and return a not-yet-indexed
    /// `ShardEntry` recording the metadata needed to publish it later. The index
    /// is NOT mutated. `final_path` is the full filesystem path the shard will
    /// live at after the rename (recorded as the entry's `filename`).
    pub fn open_shard_for_pending(
        &self,
        tmp_path: &CStr,
        final_path: String,
        min_lsn: u64,
        max_lsn: u64,
    ) -> Result<ShardEntry, StorageError> {
        let mapped = Rc::new(MappedShard::open(tmp_path, &self.schema, false)?);
        Ok(ShardEntry::from_mapped(mapped, final_path, min_lsn, max_lsn))
    }

    /// Serialize all current entries plus one pending (not-yet-indexed)
    /// `ShardEntry` into a manifest `.tmp`. Returns the prepared manifest
    /// (fd + paths) without modifying any index state. `entry_to_raw` reads only
    /// the entry's metadata fields — it never dereferences `shard`.
    pub fn prepare_manifest_with_pending(
        &self,
        manifest_path: &CStr,
        pending: &ShardEntry,
    ) -> Result<PreparedManifest, StorageError> {
        let mut entries = self.build_manifest_entries();
        entries.push(self.entry_to_raw(pending, 0, 0));

        let global_lsn = self.max_lsn().max(pending.max_lsn);
        manifest::prepare_file(manifest_path, &entries, global_lsn)
    }
}
