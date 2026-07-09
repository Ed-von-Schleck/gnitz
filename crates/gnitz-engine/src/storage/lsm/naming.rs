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
    format!("shard_{table_id}_{compact_seq}_L{level_num}_G{guard_key}.db")
}

/// Remove every one of `table_id`'s shard files in `dir` whose basename is not
/// in `keep` (both grammars share the `shard_{tid}_` prefix; `.tmp` leftovers
/// of half-written shards match it too). `keep = ∅` erases them all. Returns
/// the number of matching files attempted, best-effort per file.
pub(super) fn remove_shard_files(dir: &str, table_id: u32, keep: &HashSet<&str>) -> usize {
    let prefix = format!("shard_{table_id}_");
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
