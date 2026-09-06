//! Manifest serialize / load / recover for [`ShardIndex`]: building manifest
//! entries, loading + reopening shards, orphan GC, and staging the manifest
//! `.tmp` the barrier renames into place.

use std::collections::HashSet;
use std::ffi::CStr;

use super::super::error::StorageError;
use super::super::manifest::{self, ManifestEntryRaw, ManifestHeader, PreparedManifest};
use super::{LevelGuard, ShardEntry, ShardIndex, MAX_LEVELS};
use crate::schema::key::PkBuf;

/// Basename of a shard's full path — its manifest identity. Shard files always
/// live flat in the table's `output_dir`, which `load_manifest` re-prepends.
///
/// The reduction itself is the shard reader's, so the name recorded here is the
/// name a shard's descriptive digest is seeded with. Splitting UTF-8 at an ASCII
/// `/` leaves UTF-8, so the conversion back cannot fail.
fn shard_basename(path: &str) -> &str {
    std::str::from_utf8(crate::storage::repr::layout::shard_basename(path.as_bytes())).unwrap()
}

impl ShardIndex {
    fn build_manifest_entries(&self) -> Vec<ManifestEntryRaw> {
        let mut entries = Vec::new();
        for e in &self.l0 {
            // An L0 entry has no guard; `load_manifest` reads `guard_key` only
            // for `level > 0`, so the zero key stored here is never consulted.
            entries.push(ManifestEntryRaw::new(
                shard_basename(&e.filename),
                e.max_lsn,
                0,
                PkBuf::zeroed(0),
            ));
        }
        for (li, level) in self.levels.iter().enumerate() {
            for guard in &level.guards {
                for e in &guard.entries {
                    entries.push(ManifestEntryRaw::new(
                        shard_basename(&e.filename),
                        e.max_lsn,
                        Self::level_num(li) as u64,
                        guard.guard_key,
                    ));
                }
            }
        }
        entries
    }

    /// Load the shard set `path` names, returning the header it carried — the
    /// only read of that file, so the caller's own header fields come from here
    /// rather than a second peek. `Ok(None)` when the manifest is absent
    /// (first-time table boot); other read errors propagate.
    pub(crate) fn load_manifest(&mut self, path: &str) -> Result<Option<ManifestHeader>, StorageError> {
        let cpath = super::super::cstr(path)?;
        let Some((entries, header)) = manifest::read_file(&cpath)? else {
            return Ok(None);
        };
        // Compaction output names must never reuse a value baked into a live,
        // manifest-referenced shard across a restart.
        self.compact_seq = header.compact_seq;

        let stride = self.schema.pk_stride();
        for raw in &entries {
            // The manifest stores the basename; the shard lives in this table's
            // directory (`build_manifest_entries`). Re-prepend it to recover the path.
            let filename = format!("{}/{}", self.output_dir, raw.filename_str());
            // Published manifest ⇒ the barrier that renamed it fdatasync'd this
            // file first, so it is durable and owes no sweep.
            let entry = ShardEntry::open(&filename, &self.schema, raw.max_lsn, true)?;

            if raw.level == 0 {
                self.l0.push(entry);
            } else {
                // A corrupt manifest can name any level at all; the tier stack
                // is `MAX_LEVELS` deep and indexing it is unchecked below.
                if raw.level >= MAX_LEVELS as u64 {
                    return Err(StorageError::InvalidVersion);
                }
                // The read side of `ShardIndex::level_num`, which every writer
                // goes through.
                let level_idx = raw.level as usize - 1;
                // Every stored guard key is exactly `pk_stride` wide (a sample
                // key, a shard bound, or a synthetic key minted at that stride),
                // zero-padded into the field — so the schema recovers the width
                // and the manifest carries no length.
                let gk = PkBuf::from_bytes(&raw.guard_key[..stride]);
                self.levels[level_idx].get_or_create_guard(gk).entries.push(entry);
            }
        }
        self.sort_l0();
        // Every guard was held at the `R` of the session that wrote it, so the
        // largest one recovers that unit. Without it a resumed store carries the
        // `MIN_GUARD_BYTES` floor until its first fold and shatters every guard
        // it loaded against a target orders of magnitude too small.
        self.l0_run_bytes = self.l0_run_bytes.max(
            self.levels
                .iter()
                .flat_map(|l| l.guards.iter().map(LevelGuard::bytes))
                .max()
                .unwrap_or(0),
        );
        Ok(Some(header))
    }

    /// Startup GC: removes orphaned shard/compaction files and stale `.tmp`
    /// artifacts left by crashes.  Must run after a successful load_manifest()
    /// so the live set is populated before files are deleted.
    pub(crate) fn gc_orphans(&self) -> usize {
        let live: HashSet<&str> = self.all_entries().map(|e| shard_basename(&e.filename)).collect();

        let mut removed = 0usize;

        // Stray manifest .tmp from a crash mid-publish.
        let manifest_tmp = super::super::manifest::tmp_path(&self.output_dir);
        if std::fs::remove_file(&manifest_tmp).is_ok() {
            removed += 1;
        }

        removed + super::super::naming::remove_shard_files(&self.output_dir, self.table_id, &live)
    }

    /// Serialize the current index into a manifest `.tmp`, returning the prepared
    /// manifest (fd + paths) without modifying any index state. The barrier's
    /// one-shot shard write already registered its shard, so the current index is
    /// authoritative — no pending entry to splice in.
    ///
    /// The header's two sequence fields come from the publisher: the checkpoint
    /// generation from the round, the layout sequence from the table's child set.
    pub(crate) fn prepare_manifest(
        &self,
        manifest_path: &CStr,
        checkpoint_gen: u64,
        layout_seq: u64,
    ) -> Result<PreparedManifest, StorageError> {
        let entries = self.build_manifest_entries();
        let header = ManifestHeader {
            compact_seq: self.compact_seq,
            checkpoint_gen,
            layout_seq,
        };
        manifest::prepare_file(manifest_path, &entries, header)
    }
}
