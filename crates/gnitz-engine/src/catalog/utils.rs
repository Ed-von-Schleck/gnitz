use super::*;

// ---------------------------------------------------------------------------
// Identifier validation
//
// `FK_INDEX_INFIX` and `validate_user_identifier` live in `gnitz-wire` (shared
// with the SQL planner) and are re-bound in `catalog/mod.rs`.
// ---------------------------------------------------------------------------

/// Only the test-only direct DDL entry points (`ddl.rs`) take qualified-name
/// strings; the wire path ships schema and entity ids separately.
#[cfg(test)]
pub(crate) fn parse_qualified_name<'a>(name: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    if let Some(dot_pos) = name.find('.') {
        (&name[..dot_pos], &name[dot_pos + 1..])
    } else {
        (default_schema, name)
    }
}

pub(crate) fn make_fk_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{schema_name}__{table_name}{FK_INDEX_INFIX}{col_name}")
}

/// Production index names arrive pre-built over the wire; only the test-only
/// `ddl.rs::create_index` path names them engine-side.
#[cfg(test)]
pub(crate) fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{schema_name}__{table_name}__idx_{col_name}")
}

// ---------------------------------------------------------------------------
// On-disk directory naming conventions
//
// Every entity directory is *built* and *parsed back* here, so the creation
// hooks and the boot-time orphan sweep (`gc_orphan_directories`) can never
// disagree on the shape. Each `*_dir` builder has a matching `is_*_dir_name`
// recognizer where the sweep needs to classify an on-disk name.
// ---------------------------------------------------------------------------

/// `<base_dir>/<schema_name>` — a schema's directory. Name-based (no id): a
/// DROP+CREATE of the same schema name reuses the path, which is why recreation
/// must cancel a pending deletion of it.
pub(crate) fn schema_dir(base_dir: &str, schema_name: &str) -> String {
    format!("{base_dir}/{schema_name}")
}

/// `<base_dir>/<schema_name>/<name>_<tid>` — a table's directory.
pub(crate) fn table_dir(base_dir: &str, schema_name: &str, name: &str, tid: i64) -> String {
    format!("{base_dir}/{schema_name}/{name}_{tid}")
}

/// `<base_dir>/<schema_name>/view_<name>_<vid>` — a view's directory.
pub(crate) fn view_dir(base_dir: &str, schema_name: &str, name: &str, vid: i64) -> String {
    format!("{base_dir}/{schema_name}/view_{name}_{vid}")
}

/// `<owner_dir>/idx_<idx_id>` — an index's directory, nested in its owner.
pub(crate) fn index_dir(owner_dir: &str, idx_id: i64) -> String {
    format!("{owner_dir}/idx_{idx_id}")
}

/// True if `name` is shaped like an index directory (`idx_<digits>`).
pub(crate) fn is_index_dir_name(name: &str) -> bool {
    name.strip_prefix("idx_").is_some_and(has_numeric_id)
}

/// The directory holding THIS process's copy of an index table: the index dir
/// itself for master/standalone, `{idx_dir}/w{rank}` for a forked worker
/// (single-writer isolation for spills and compaction, so sibling workers never
/// collide on same-name `.tmp`/compaction files under the shared tree).
/// `Table::new` creates the path recursively, so this is a pure path function.
pub(crate) fn index_table_dir(idx_dir: &str) -> String {
    if crate::foundation::worker_ctx::is_worker() {
        format!("{idx_dir}/w{}", crate::foundation::worker_ctx::worker_rank())
    } else {
        idx_dir.to_string()
    }
}

/// Open this process's copy of an ephemeral secondary-index table under
/// `idx_dir` (homed by `index_table_dir`). The one recipe for the live CREATE
/// INDEX hook and the worker-boot rebuild: the arena size and
/// `RecoverySource::Rederive` (load-bearing — a durable source would
/// double-count its loaded shards on the next open) must never diverge.
pub(crate) fn new_index_table(idx_dir: &str, index_id: i64, idx_schema: SchemaDescriptor) -> Result<Table, String> {
    Table::new(
        &index_table_dir(idx_dir),
        idx_schema,
        index_id as u32,
        SYS_TABLE_ARENA,
        RecoverySource::Rederive,
    )
    .map_err(|e| format!("Failed to create index table {index_id}: error {e}"))
}

/// True if `name` is a per-rank index subdir (`w<digits>`), as written by
/// `index_table_dir` for a forked worker.
pub(crate) fn is_index_rank_dir_name(name: &str) -> bool {
    name.strip_prefix('w').is_some_and(has_numeric_id)
}

/// Remove a live index dir's per-rank `w{k}` subdirs. They must never survive
/// a boot (each worker rebuilds its own slice-local copy afterwards, and a
/// smaller worker count would otherwise strand `w{k}` dirs forever). Runs once
/// per boot on the master, pre-fork — no worker exists yet.
pub(crate) fn remove_stale_index_rank_dirs(idx_dir: &str) {
    for rank_name in subdir_names(idx_dir) {
        if !is_index_rank_dir_name(&rank_name) {
            continue;
        }
        let rank_full = format!("{idx_dir}/{rank_name}");
        match fs::remove_dir_all(&rank_full) {
            Ok(()) => gnitz_debug!("recovery: removed stale index rank dir {}", rank_full),
            Err(e) => gnitz_debug!("recovery: failed to remove index rank dir {}: {}", rank_full, e),
        }
    }
}

/// True if `name` is shaped like a table or view directory — both end in
/// `_<digits>` (`<name>_<tid>` and `view_<name>_<vid>` respectively).
pub(crate) fn is_table_dir_name(name: &str) -> bool {
    name.rsplit_once('_').is_some_and(|(_, id)| has_numeric_id(id))
}

/// A directory-name id component: non-empty and all ASCII digits.
fn has_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Immediate sub-directory names of `path`. Empty if `path` is missing or
/// unreadable — both mean "nothing to scan" for the orphan sweep. Non-directory
/// entries are skipped.
pub(crate) fn subdir_names(path: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(path) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Helper: read column data from cursor
// ---------------------------------------------------------------------------

/// Read a u64 from a cursor column. `logical_col` is the schema column index.
/// `read_i64(col) as u64` is bit-for-bit `u64::from_le_bytes` of the same 8 bytes.
pub(crate) fn cursor_read_u64(cursor: &ReadCursor, logical_col: usize) -> u64 {
    cursor.read_i64(logical_col) as u64
}

/// Read a German string from a cursor column. `logical_col` is the schema column index.
pub(crate) fn cursor_read_string(cursor: &ReadCursor, logical_col: usize) -> String {
    String::from_utf8(cursor.read_german_bytes(logical_col)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Helper: filesystem
// ---------------------------------------------------------------------------

pub(crate) fn ensure_dir(path: &str) -> Result<(), String> {
    // Reject any embedded NUL bytes (should never happen with clean paths)
    if path.contains('\0') {
        return Err(format!("Path contains NUL byte: {path:?}"));
    }
    match fs::create_dir_all(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(format!("Failed to create directory '{path}': {e}")),
    }
}

pub(crate) fn fsync_dir(path: &str) {
    if let Ok(dir) = fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

// ---------------------------------------------------------------------------
// Copy/retract helpers
// ---------------------------------------------------------------------------

/// Seek a system table by PK, copy the matching row with weight=-1.
/// Returns a single-row retraction batch (or empty batch if PK not found).
pub(crate) fn retract_single_row(table: &Table, schema: &SchemaDescriptor, pk: u128) -> Batch {
    let mut batch = Batch::with_schema(*schema, 1);
    let mut cursor = table.open_cursor();
    // OPK-encode the native PK; correct for single-column and compound system PKs.
    let (opk, stride) = crate::storage::opk_key(schema, &pk.to_le_bytes());
    if cursor.seek_exact_live(&opk[..stride]) {
        cursor.copy_current_row_into(&mut batch, -1);
    }
    batch
}

/// Scan `[start, pk_end)` and emit a weight=-1 batch of every positive-weight
/// row in the range. Used for U64-PK system tables where rows belonging to one
/// owner share a packed PK prefix (e.g. `sys_columns` keyed by
/// `pack_column_id(owner, col)`).
pub(crate) fn retract_rows_in_pk_range(table: &Table, schema: &SchemaDescriptor, start: u128, pk_end: u128) -> Batch {
    let mut batch = Batch::with_schema(*schema, 8);
    let mut cursor = table.open_cursor();
    // U64-PK system table: OPK == big-endian; the native-value range
    // comparisons below (`current_key_narrow()`/`get_pk` vs `pk_end`) stay valid.
    cursor.seek_bytes(&(start as u64).to_be_bytes());

    while cursor.valid {
        if cursor.current_key_narrow() >= pk_end {
            break;
        }
        if cursor.current_weight > 0 {
            cursor.copy_current_row_into(&mut batch, -1);
        }
        cursor.advance();
    }
    batch
}

/// Scan all positive-weight rows for `view_id` and emit each as a retraction
/// (weight=-1). The circuit/dep catalog tables use a compound `(view_id, sub)`
/// PK, so all of a view's rows share the `view_id` byte prefix and are
/// contiguous in compound-PK sort order — `compare_pk_bytes` reproduces the
/// `(view_id, sub)` ordering natively (no `(pk_hi, pk_lo)` u128 exploit).
pub(crate) fn retract_rows_by_view(table: &Table, schema: &SchemaDescriptor, view_id: u64) -> Batch {
    let mut batch = Batch::with_schema(*schema, 8);
    // The (view_id, sub) PK is OPK-at-rest; the leading view_id column (U64) is
    // big-endian, so the prefix must be OPK (BE), not native LE.
    let prefix = view_id.to_be_bytes();
    table
        .open_cursor()
        .for_each_positive_with_prefix(&prefix, |cursor| cursor.copy_current_row_into(&mut batch, -1));
    batch
}
