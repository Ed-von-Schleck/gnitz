use super::*;

/// The name an FK auto-index is created under. `FK_INDEX_INFIX` lives in
/// `gnitz-wire`, shared with the SQL planner.
pub(crate) fn make_fk_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{schema_name}__{table_name}{FK_INDEX_INFIX}{col_name}")
}

// ---------------------------------------------------------------------------
// On-disk directory naming conventions
//
// Every directory the catalog itself names is *built* and *parsed back* here, so
// the creation hooks and the boot-time orphan sweep (`gc_orphan_directories`)
// can never disagree on the shape. Below a relation directory the names are
// storage's `ChildAddr` grammar, and the sweep classifies them through it.
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
/// directory.
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
/// Sits under a schema dir and ends in `_<digits>`, which is what lets the boot
/// orphan sweep reclaim one left by a crash mid-compile; a root elsewhere would
/// leak a directory per crash.
pub(crate) fn preflight_dir(base_dir: &str, schema_name: &str, vid: i64) -> String {
    format!("{base_dir}/{schema_name}/_preflight_{vid}")
}

/// `<owner_dir>/idx_<idx_id>` — an index's directory, nested in its owner.
/// `ChildAddr` owns the name so the boot sweeps that walk a relation directory
/// classify it instead of tripping over it.
pub(crate) fn index_dir(owner_dir: &str, idx_id: i64) -> String {
    ChildAddr::Index { id: idx_id }.dir(owner_dir)
}

/// True if `name` could be a relation directory. The three writers directly
/// under a schema dir are `relation_dir` (`t_<id>` / `v_<id>`) and
/// `preflight_dir` (`_preflight_<id>`), all of which end in `_<digits>`.
///
/// Deliberately looser than those builders: it is the eligibility gate on
/// `gc_orphan_directories`' `remove_dir_all`, and matching the exact shapes
/// would strand a directory written under an older naming scheme instead of
/// reclaiming it. Anything not ending in `_<digits>` is left untouched.
pub(crate) fn is_table_dir_name(name: &str) -> bool {
    name.rsplit_once('_').is_some_and(|(_, id)| has_numeric_id(id))
}

/// A directory-name id component: non-empty and all ASCII digits.
fn has_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
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

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
fn lock_file_path(base_dir: &str) -> String {
    format!("{base_dir}/{DIR_LOCK_FILENAME}")
}

/// How long [`lock_data_dir`] keeps retrying before it reports the directory as
/// held, and how long it sleeps between attempts.
///
/// It is a bounded retry rather than one `LOCK_NB` attempt because of how a
/// server restarts: a worker is a forked child carrying `PR_SET_PDEATHSIG`, so
/// it dies *after* the master, while the master's exit is what a supervisor
/// observes. Between those two instants a worker still holds the inherited
/// lock, and a bare `LOCK_NB` would turn every fast restart into an intermittent
/// "another process holds this data directory". A directory whose owner is
/// genuinely live still fails, because it stays held for the whole window.
const DIR_LOCK_RETRY_FOR: std::time::Duration = std::time::Duration::from_secs(2);
const DIR_LOCK_RETRY_EVERY: std::time::Duration = std::time::Duration::from_millis(20);

/// Take the exclusive `flock` on `base_dir`'s lock file, so exactly one live
/// handle writes it.
///
/// Two writers on one directory silently corrupt shard state: `current_lsn` is
/// per-`Table` and reseeded from `max_lsn + 1` at open, so both would mint
/// identical shard names. Nothing else enforces this — a forked worker inherits
/// the open file description and with it the same lock, which `flock` treats as
/// one holder rather than a conflict, so the server's own children never
/// contend.
///
/// The returned file must outlive every store under `base_dir`: closing it
/// releases the lock.
pub(crate) fn lock_data_dir(base_dir: &str) -> Result<fs::File, String> {
    let path = lock_file_path(base_dir);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| format!("Failed to open data-directory lock '{path}': {e}"))?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
    let deadline = std::time::Instant::now() + DIR_LOCK_RETRY_FOR;
    loop {
        if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(file);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EWOULDBLOCK) {
            return Err(format!("Failed to lock data directory '{base_dir}': {err}"));
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("data directory '{base_dir}' is locked by another live process"));
        }
        std::thread::sleep(DIR_LOCK_RETRY_EVERY);
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
pub(crate) fn retract_key_range(table: &Table, schema: &SchemaDescriptor, start: &[u8], end: &[u8]) -> Batch {
    let mut cursor = table.open_cursor();
    cursor.seek_range_bytes(start, Some(end));
    // Sized off the positioned walk's own upper bound, so the appends never
    // re-grow (each growth re-copies every live byte).
    let mut batch = Batch::with_capacity(*schema, cursor.estimated_length());
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
