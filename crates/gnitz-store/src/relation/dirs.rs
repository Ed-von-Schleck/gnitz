//! Relation directory naming, crash-safe staging of a directory a create makes,
//! and the data-directory lock.
//!
//! Every directory a relation registration itself names is *built* and *parsed
//! back* here, so the creation path and the orphan sweeps can never disagree on
//! the shape. Below a relation directory the names are storage's `ChildAddr`
//! grammar, and the sweeps classify them through it.

use std::fs;

use super::RelationKind;
use crate::storage::StoreError;

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
const DIR_LOCK_FILENAME: &str = "LOCK";

/// `<base_dir>/<schema_name>/t_<tid>` for a table, `.../v_<vid>` for a view.
/// Id-only (no embedded name), so a RENAME never changes the path and never
/// orphans data.
pub fn relation_dir(base_dir: &str, schema_name: &str, kind: RelationKind, id: i64) -> String {
    let tag = if kind.is_view() { 'v' } else { 't' };
    format!("{base_dir}/{schema_name}/{tag}_{id}")
}

/// True if `name` could be a relation directory. The writers directly under a
/// schema dir are [`relation_dir`] (`t_<id>` / `v_<id>`) and the catalog's
/// pre-flight root (`_preflight_<id>`), all of which end in `_<digits>`.
///
/// Deliberately looser than those builders: it is the eligibility gate on the
/// orphan sweeps' `remove_dir_all`, and matching the exact shapes would strand a
/// directory written under an older naming scheme instead of reclaiming it.
/// Anything not ending in `_<digits>` is left untouched.
pub fn is_table_dir_name(name: &str) -> bool {
    name.rsplit_once('_').is_some_and(|(_, id)| has_numeric_id(id))
}

/// A directory-name id component: non-empty and all ASCII digits. Storage's
/// `child_dir::parse_id` is the stricter twin one level down — it also bounds the
/// magnitude, which this must not, being the gate on an orphan `remove_dir_all`.
fn has_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Stage `dir` across `f`: on `Err` remove it, on `Ok` fsync its parent — each
/// only when `f` is what created it. One that was already there holds an
/// existing relation's rows (a boot replay reopens one; so does a compensation
/// restoring what a bundle dropped), and deciding that here is what stops a
/// caller staging live shards.
///
/// "`f` created it" is observed, not declared: the probe runs on both sides, so
/// a caller that creates no directory at all (a storeless relation) neither
/// cleans up nor syncs, without having to say so.
pub fn staged_dir<T, E>(dir: &str, f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    let existed = std::path::Path::new(dir).exists();
    let out = f();
    if existed || !std::path::Path::new(dir).exists() {
        return out;
    }
    if out.is_err() {
        let _ = fs::remove_dir_all(dir);
    } else if let Some(parent) = dir.rsplit_once('/').map(|(p, _)| p) {
        // A new directory entry is metadata in the parent.
        let _ = crate::storage::fsync_dir(parent);
    }
    out
}

pub fn ensure_dir(path: &str) -> Result<(), StoreError> {
    // `create_dir_all` already succeeds on an existing directory; the only
    // `AlreadyExists` it reports is a non-directory blocking the path, which is
    // a genuine failure.
    fs::create_dir_all(path).map_err(|e| StoreError::storage(format!("create directory '{path}'"), e.into()))
}

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
/// contend. A second `open` in the *same* process does contend: it opens a fresh
/// file description, so two mirror handles on one directory are refused here.
///
/// The returned file must outlive every store under `base_dir`: closing it
/// releases the lock.
pub fn lock_data_dir(base_dir: &str) -> Result<fs::File, StoreError> {
    let path = lock_file_path(base_dir);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| StoreError::storage(format!("open data-directory lock '{path}'"), e.into()))?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
    let deadline = std::time::Instant::now() + DIR_LOCK_RETRY_FOR;
    loop {
        if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(file);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EWOULDBLOCK) {
            return Err(StoreError::storage(
                format!("lock data directory '{base_dir}'"),
                err.into(),
            ));
        }
        if std::time::Instant::now() >= deadline {
            return Err(StoreError::rejected(format!(
                "data directory '{base_dir}' is already held"
            )));
        }
        std::thread::sleep(DIR_LOCK_RETRY_EVERY);
    }
}
