//! The single owner of the table-directory filename grammar. Every producer
//! (flush spill/barrier shards, compaction outputs) and every cleaner
//! (`gc_orphans`, `erase_stale_shards`) routes through here, so writer and
//! cleaner can never disagree about which files belong to a table.

use std::collections::HashSet;

/// Flat spill/barrier shard basename: `shard_{tid}_{lsn}.db`.
pub(super) fn spill_shard_name(table_id: u32, lsn: u64) -> String {
    format!("shard_{table_id}_{lsn}.db")
}

/// Compaction-output shard basename: `shard_{tid}_{seq}_L{level}_G{guard}.db`.
/// `compact_seq` is per-table monotonic and manifest-persisted (globally unique
/// across restarts) and `guard_key` is unique within a call, so no two outputs
/// ever share a basename over the table's lifetime — which is what keeps the
/// finalizing rename from clobbering a live shard. Collision-free against the
/// flat grammar: a spill name has no `_L` segment.
pub(super) fn compact_shard_name(table_id: u32, compact_seq: u64, level_num: usize, guard_key: u128) -> String {
    let name = format!("shard_{table_id}_{compact_seq}_L{level_num}_G{guard_key}.db");
    // The grammar's worst case (~84 bytes) is well inside the manifest's 128-byte
    // filename field, which is the binding limit — a basename that overflows it
    // reloads as an unopenable shard.
    debug_assert!(
        name.len() < 128,
        "compaction basename overflows the manifest field: {name}"
    );
    name
}

/// Basename prefix both grammars share; `.tmp` leftovers match it too. The one
/// way to ask "does this file belong to `table_id`".
pub(super) fn shard_prefix(table_id: u32) -> String {
    format!("shard_{table_id}_")
}

/// Compaction output rather than flat spill? The `_L` level segment separates the
/// two grammars. Only tests ask — production reads each shard's level off the
/// manifest.
#[cfg(test)]
pub(super) fn is_compaction_output(name: &str) -> bool {
    name.contains("_L")
}

/// Remove every one of `table_id`'s shard files in `dir` whose basename is not
/// in `keep`. `keep = ∅` erases them all. Returns the number of matching files
/// attempted, best-effort per file.
pub(super) fn remove_shard_files(dir: &str, table_id: u32, keep: &HashSet<&str>) -> usize {
    let prefix = shard_prefix(table_id);
    let mut removed = 0usize;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&prefix) && !keep.contains(name) {
                    let _ = std::fs::remove_file(entry.path());
                    removed += 1;
                }
            }
        }
    }
    removed
}
