//! A shard's basename grammar, shared by its producers and the one cleaner. A
//! store owns its directory, so every shard and staging file in it is the store's.

use std::collections::HashSet;

use super::manifest::STAGING_SUFFIX;

/// Every shard basename's prefix.
pub(super) const SHARD_PREFIX: &str = "shard_";

/// The basename of the shard drawn at `seq`: `shard_{seq}.db`.
pub(super) fn shard_name(seq: u64) -> String {
    format!("{SHARD_PREFIX}{seq}.db")
}

/// The path of the shard drawn at `seq` in the store at `dir`.
pub(super) fn shard_path(dir: &str, seq: u64) -> String {
    format!("{dir}/{}", shard_name(seq))
}

/// Remove every staging file and every shard not in `keep` from `dir`,
/// best-effort.
pub(super) fn remove_stale_files(dir: &str, keep: &HashSet<String>) {
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
