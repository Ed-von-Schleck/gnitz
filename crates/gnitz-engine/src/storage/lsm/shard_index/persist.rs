//! Manifest serialize / load / recover for [`ShardIndex`]: building manifest
//! entries, loading + reopening shards, orphan GC, and staging the manifest
//! `.tmp` the barrier renames into place.

use std::collections::HashSet;
use std::ffi::CStr;

use super::super::error::StorageError;
use super::super::manifest::{self, ManifestEntryRaw, ManifestHeader, PreparedManifest};
use super::{ShardEntry, ShardIndex, MAX_LEVELS};

/// Basename of a shard's full path — its manifest identity. Shard files always
/// live flat in the table's `output_dir`, which `load_manifest` re-prepends.
fn shard_basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap()
}

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
        // Store the basename, not the full path: a full path could exceed the
        // 128-byte filename field, and a truncated name is an unopenable shard
        // at reload. Basenames are bounded well under the field width — a
        // violation is a naming-scheme bug, so fail loudly instead of truncating.
        let name_bytes = shard_basename(&e.filename).as_bytes();
        assert!(
            name_bytes.len() < 128,
            "shard basename overflows the manifest filename field: {}",
            e.filename,
        );
        let mut filename = [0u8; 128];
        filename[..name_bytes.len()].copy_from_slice(name_bytes);
        ManifestEntryRaw {
            max_lsn: e.max_lsn,
            filename,
            level,
            guard_key: gk,
        }
    }

    pub fn load_manifest(&mut self, path: &str) -> Result<(), StorageError> {
        let cpath = super::super::cstr(path)?;
        // Missing manifest file ⇒ first-time table boot, treat as empty.
        // Other read errors propagate.
        let cap = match manifest::entry_count(&cpath)? {
            Some(n) => n.max(1),
            None => return Ok(()),
        };
        let mut entries = vec![ManifestEntryRaw::default(); cap];
        let (count, header) = manifest::read_file(&cpath, &mut entries)?;
        // Compaction output names must never reuse a value baked into a live,
        // manifest-referenced shard across a restart.
        self.compact_seq = header.compact_seq;

        for raw in entries.iter().take(count) {
            // The manifest stores the basename; the shard lives in this table's
            // directory (`entry_to_raw`). Re-prepend it to recover the path.
            let filename = format!("{}/{}", self.output_dir, raw.filename_str());
            let entry = ShardEntry::open(&filename, &self.schema, raw.max_lsn)?;

            if raw.level == 0 {
                self.l0.push(entry);
            } else {
                // Bound the level before `ensure_level`: a corrupted manifest
                // with an arbitrary level would otherwise allocate thousands of
                // empty FLSMLevels and crash the engine at startup.
                if raw.level >= MAX_LEVELS as u64 {
                    return Err(StorageError::InvalidVersion);
                }
                let level_num = raw.level as usize;
                self.ensure_level(level_num);
                self.levels[level_num - 1]
                    .get_or_create_guard(raw.guard_key)
                    .entries
                    .push(entry);
            }
        }
        self.sort_l0();
        Ok(())
    }

    /// Startup GC: removes orphaned shard/compaction files and stale `.tmp`
    /// artifacts left by crashes.  Must run after a successful load_manifest()
    /// so the live set is populated before files are deleted.
    pub fn gc_orphans(&self) -> usize {
        let live: HashSet<&str> = self.all_entries().map(|e| shard_basename(&e.filename)).collect();

        let mut removed = 0usize;

        // Stray manifest .tmp from a crash mid-publish.
        let manifest_tmp = format!("{}/manifest.bin.tmp", self.output_dir);
        if std::fs::remove_file(&manifest_tmp).is_ok() {
            removed += 1;
        }

        removed + super::super::naming::remove_shard_files(&self.output_dir, self.table_id, &live)
    }

    /// Serialize the current index into a manifest `.tmp`, returning the prepared
    /// manifest (fd + paths) without modifying any index state. The barrier's
    /// one-shot shard write already registered its shard via `add_shard`, so the
    /// current index is authoritative — no pending entry to splice in.
    ///
    /// `generation` is the checkpoint generation the manifest is stamped with —
    /// passed by the publish path (the worker's ephemeral round supplies the
    /// committed generation; base `SalReplay` publishes stamp 0, which is never
    /// read back).
    pub fn prepare_manifest(&self, manifest_path: &CStr, generation: u64) -> Result<PreparedManifest, StorageError> {
        let entries = self.build_manifest_entries();
        let header = ManifestHeader {
            compact_seq: self.compact_seq,
            generation,
        };
        manifest::prepare_file(manifest_path, &entries, header)
    }
}
