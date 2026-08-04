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

/// `<base_dir>/_system_catalog` — the system catalog root. Not a user schema
/// directory, so the boot orphan sweep (which scans only registered schema
/// names) never reaches it.
pub(crate) fn sys_catalog_dir(base_dir: &str) -> String {
    format!("{base_dir}/{SYS_CATALOG_DIRNAME}")
}

/// `<base_dir>/_system_catalog/<family_name>` — one system family's store
/// directory. The same string reaches the store and the family's `TABLE_TAB`
/// self-description row, so both are built here.
pub(crate) fn sys_family_dir(base_dir: &str, family_name: &str) -> String {
    format!("{}/{family_name}", sys_catalog_dir(base_dir))
}

/// `<base_dir>/<schema_name>/t_<tid>` for a table, `.../v_<vid>` for a view.
/// Id-only (no embedded name), so a RENAME never changes the path and never
/// orphans data.
pub(crate) fn relation_dir(base_dir: &str, schema_name: &str, kind: RelationKind, id: i64) -> String {
    let tag = if kind.is_view() { 'v' } else { 't' };
    format!("{base_dir}/{schema_name}/{tag}_{id}")
}

/// `<base_dir>/<schema_name>/_preflight_<vid>` — the throwaway root the master's
/// CREATE VIEW pre-flight compiles into. Not the view's own directory:
/// `child_scratch_dir` stamps the rank into each scratch child's name and the
/// master is rank 0, so compiling in place would write worker 0's real paths.
/// Sits under a schema dir and ends in `_<digits>` so `is_table_dir_name`
/// classifies it, which is what lets the boot orphan sweep reclaim one left by a
/// crash mid-compile; a root elsewhere would leak a directory per crash.
pub(crate) fn preflight_dir(base_dir: &str, schema_name: &str, vid: i64) -> String {
    format!("{base_dir}/{schema_name}/_preflight_{vid}")
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
/// A pure path function; `new_index_table` creates the path recursively.
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
/// `RecoverySource::Rederive` must never diverge — a durable source would
/// double-count its loaded shards on the next open.
pub(crate) fn new_index_table(idx_dir: &str, index_id: i64, idx_schema: SchemaDescriptor) -> Result<Table, String> {
    let table_dir = index_table_dir(idx_dir);
    // An index dir is a catalog-owned LAYOUT node — staged into
    // `pending_dir_deletions`, enumerated by `gc_orphan_directories`, reaped by
    // name on DROP — so it is materialized at DDL time. (A `Rederive`
    // `Table::new` itself opens dirless and would create the path only on its
    // first spill.)
    std::fs::create_dir_all(&table_dir).map_err(|e| format!("Failed to create index dir {table_dir}: {e}"))?;
    Table::new(
        &table_dir,
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

/// Remove a relation's child directories that this boot's worker count no longer
/// owns — `ChildAddr::is_owned_by` holds the rule. Names in none of the child
/// grammars (an `idx_{id}` dir, say) are left alone.
pub(crate) fn reclaim_retired_children(dir: &str, routing: Routing, num_workers: u32) {
    for name in subdir_names(dir) {
        let Some(child) = ChildAddr::parse(&name) else { continue };
        if !child.is_owned_by(routing, num_workers) {
            let full = format!("{dir}/{name}");
            gnitz_debug!("recovery: removing retired child dir {}", full);
            crate::storage::remove_child(&full);
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
///
/// Materialized rather than streamed: every caller unlinks entries from the
/// directory it is walking, and `readdir` may skip entries when the directory
/// is modified mid-iteration.
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
    // `create_dir_all` already succeeds on an existing directory; the only
    // `AlreadyExists` it reports is a non-directory blocking the path, which is
    // a genuine failure.
    fs::create_dir_all(path).map_err(|e| format!("Failed to create directory '{path}': {e}"))
}

pub(crate) fn fsync_dir(path: &str) {
    if let Ok(dir) = fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

// ---------------------------------------------------------------------------
// Copy/retract helpers
// ---------------------------------------------------------------------------

/// Emit a weight=−1 batch of every live row of `table` in the OPK key range
/// `[start, end)` (`end` `None` = unbounded above). The one retraction
/// primitive: the callers differ only in the bounds they encode — a single PK's
/// point range, one owner's packed-column band, or one view's `(view_id, sub)`
/// prefix — all produced through `schema::key`, so no call site re-derives the
/// OPK layout.
pub(crate) fn retract_key_range(table: &Table, schema: &SchemaDescriptor, start: &[u8], end: Option<&[u8]>) -> Batch {
    let mut batch = Batch::with_capacity(*schema, 8);
    let mut cursor = table.open_cursor();
    cursor.seek_range_bytes(start, end);
    while cursor.valid {
        if cursor.current_weight > 0 {
            cursor.copy_current_row_into(&mut batch, -1);
        }
        cursor.advance();
    }
    batch
}

/// The OPK image of a native system-table PK. `pk` is the packed native value
/// (`u128` covers every system family: a single U64 id, or a compound
/// `(view_id, sub)` written `(vid << 64) | sub`).
pub(crate) fn sys_opk(schema: &SchemaDescriptor, pk: u128) -> crate::schema::key::PkBuf {
    crate::schema::key::opk_key(schema, &pk.to_le_bytes())
}

/// Retract the single live row at `pk`, or return an empty batch when the PK is
/// absent or already retracted.
pub(crate) fn retract_single_row(table: &Table, schema: &SchemaDescriptor, pk: u128) -> Batch {
    let mut batch = Batch::with_capacity(*schema, 1);
    let mut cursor = table.open_cursor();
    if cursor.advance_to_exact_live(sys_opk(schema, pk).pk_bytes()) {
        cursor.copy_current_row_into(&mut batch, -1);
    }
    batch
}
