//! The manifest a [`ShardIndex`] publishes, and the one it opens from.

use std::collections::HashSet;

use super::super::error::StorageError;
use super::super::manifest::{Manifest, ManifestEntry, ManifestStamp};
use super::super::naming;
use super::{ShardEntry, ShardIndex};
use crate::schema::key::PkBuf;

impl ShardIndex {
    /// The manifest describing the current index.
    pub(crate) fn manifest(&self, stamp: ManifestStamp) -> Manifest {
        let entry = |e: &ShardEntry, level: u64, guard_key: PkBuf| ManifestEntry {
            seq: e.seq,
            newest: e.newest,
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
            stamp,
            run_bytes: self.l0_run_bytes,
            caller_record: Vec::new(),
            entries,
        }
    }

    /// Open the shard set `m` names (none for `None`), then remove every shard
    /// and staging file in the directory it does not name.
    pub(in crate::storage::lsm) fn install(&mut self, m: Option<&Manifest>) -> Result<(), StorageError> {
        if let Some(m) = m {
            self.l0_run_bytes = m.run_bytes;
            for e in &m.entries {
                let path = naming::shard_path(&self.output_dir, e.seq);
                // Published manifest ⇒ the barrier that renamed it fdatasync'd
                // this file first, so it is durable and owes no sweep.
                let entry = ShardEntry::open(&path, &self.schema, e.seq, e.newest, true)?;
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
        self.shard_seq = self.all_entries().map(|e| e.seq).max().unwrap_or(0);
        let live: HashSet<String> = self.all_entries().map(|e| naming::shard_name(e.seq)).collect();
        naming::remove_stale_files(&self.output_dir, &live);
        Ok(())
    }
}
