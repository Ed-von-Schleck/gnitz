//! The manifest a [`ShardIndex`] publishes, and the one it opens from.

use std::collections::HashSet;

use super::super::error::StorageError;
use super::super::manifest::{Manifest, ManifestEntry};
use super::{ShardEntry, ShardIndex};
use crate::schema::key::PkBuf;

/// Basename of a shard's full path — its manifest identity. Shard files always
/// live flat in the table's `output_dir`, which `install` re-prepends.
///
/// The reduction itself is the shard reader's, so the name recorded here is the
/// name a shard's descriptive digest is seeded with. Splitting UTF-8 at an ASCII
/// `/` leaves UTF-8, so the conversion back cannot fail.
fn shard_basename(path: &str) -> &str {
    std::str::from_utf8(crate::storage::repr::layout::shard_basename(path.as_bytes())).unwrap()
}

impl ShardIndex {
    /// The manifest describing the current index, stamped `checkpoint_gen`.
    pub(crate) fn manifest(&self, checkpoint_gen: u64) -> Manifest {
        let entry = |e: &ShardEntry, level: u64, guard_key: PkBuf| ManifestEntry {
            name: shard_basename(&e.filename).to_owned(),
            max_lsn: e.max_lsn,
            level,
            guard_key,
        };
        let mut entries: Vec<ManifestEntry> = self.l0.iter().map(|e| entry(e, 0, PkBuf::zeroed(0))).collect();
        for (li, level) in self.levels.iter().enumerate() {
            for guard in &level.guards {
                for e in &guard.entries {
                    entries.push(entry(e, Self::level_num(li) as u64, guard.guard_key));
                }
            }
        }
        Manifest {
            compact_seq: self.compact_seq,
            checkpoint_gen,
            run_bytes: self.l0_run_bytes,
            entries,
        }
    }

    /// Open the shard set `m` names (none for `None`), then remove every shard
    /// and staging file in the directory it does not name.
    pub(in crate::storage::lsm) fn install(&mut self, m: Option<&Manifest>) -> Result<(), StorageError> {
        if let Some(m) = m {
            // Compaction output names must never reuse a value baked into a live,
            // manifest-referenced shard across a restart.
            self.compact_seq = m.compact_seq;
            self.l0_run_bytes = m.run_bytes;
            for e in &m.entries {
                let filename = format!("{}/{}", self.output_dir, e.name);
                // Published manifest ⇒ the barrier that renamed it fdatasync'd
                // this file first, so it is durable and owes no sweep.
                let entry = ShardEntry::open(&filename, &self.schema, e.max_lsn, true)?;
                if e.level == 0 {
                    self.l0.push(entry);
                } else {
                    let Some(level) = (e.level as usize).checked_sub(1).and_then(|i| self.levels.get_mut(i)) else {
                        return Err(StorageError::Corrupt("manifest level"));
                    };
                    level.get_or_create_guard(e.guard_key).entries.push(entry);
                }
            }
        }
        let live: HashSet<&str> = self.all_entries().map(|e| shard_basename(&e.filename)).collect();
        super::super::naming::remove_stale_files(&self.output_dir, &live);
        Ok(())
    }
}
