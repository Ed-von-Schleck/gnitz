//! A shard's basename grammar, shared by its producers and the one cleaner. A
//! store owns its directory, so every shard and staging file in it is the store's.

use std::collections::HashSet;

use super::STAGING_SUFFIX;

/// Basename prefix both grammars share.
pub(super) const SHARD_PREFIX: &str = "shard_";

/// Flat spill/barrier shard basename: `shard_{lsn}.db`.
pub(super) fn spill_shard_name(lsn: u64) -> String {
    format!("{SHARD_PREFIX}{lsn}.db")
}

/// Compaction-output shard basename: `shard_{seq}_P{part}.db`. `compact_seq`
/// never repeats within a store: the manifest carries it across a restart.
pub(super) fn compact_shard_name(compact_seq: u64, part: usize) -> String {
    format!("{SHARD_PREFIX}{compact_seq}_P{part}.db")
}

/// Remove every staging file and every shard not in `keep` from `dir`,
/// best-effort.
pub(super) fn remove_stale_files(dir: &str, keep: &HashSet<&str>) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                let stale = name.ends_with(STAGING_SUFFIX) || (name.starts_with(SHARD_PREFIX) && !keep.contains(name));
                if stale {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}
