//! Relation directory naming, crash-safe staging of a directory a create makes,
//! and the data-directory lock.
//!
//! Every directory a relation registration itself names is *built* and *parsed
//! back* here, so the creation path and the orphan sweeps can never disagree on
//! the shape. Below a relation directory the names are storage's `ChildAddr`
//! grammar, and the sweeps classify them through it.

use std::fs;

use super::{Relation, RelationKind, RelationRegistry};
use crate::storage::{subdir_names, ChildAddr, StoreError};

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

/// True if `name` could be a relation directory. Deliberately looser than
/// [`relation_dir`]: it gates the orphan sweep's `remove_dir_all`, and matching
/// the exact shape would strand a directory written under an older naming scheme
/// rather than reclaim it. Anything not ending in `_<digits>` is left alone.
fn is_table_dir_name(name: &str) -> bool {
    name.rsplit_once('_')
        .is_some_and(|(_, id)| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
}

/// Stage `dir` across `f`: on `Err` remove it, on `Ok` fsync its parent — each
/// only when `f` is what created it, which is observed rather than declared. A
/// directory that was already there holds an existing relation's rows, and a
/// caller that creates none neither cleans up nor syncs.
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

/// How long the server has [`lock_data_dir`] retry before it reports the
/// directory as held.
///
/// Bounded retry rather than one `LOCK_NB` attempt: a forked worker outlives the
/// master whose exit a supervisor observes, and holds the inherited lock in
/// between. A genuinely live owner holds it for the whole window and still fails.
pub const DIR_LOCK_RETRY_FOR: std::time::Duration = std::time::Duration::from_secs(2);
/// How long [`lock_data_dir`] sleeps between attempts.
const DIR_LOCK_RETRY_EVERY: std::time::Duration = std::time::Duration::from_millis(20);

/// Create `base_dir` if absent and take the exclusive `flock` on its lock file,
/// so exactly one live handle writes it — retrying a held lock for `retry`: the
/// server's restart window, zero for a host whose directory no forked child can
/// hold.
///
/// Two writers on one directory silently corrupt shard state. A forked worker
/// inherits the open file description, which `flock` treats as one holder, so the
/// server's own children never contend; a second `open` in the same process does.
///
/// The returned file must outlive every store under `base_dir` — closing it
/// releases the lock.
pub fn lock_data_dir(base_dir: &str, retry: std::time::Duration) -> Result<fs::File, StoreError> {
    // The directory before the lock: the lock file is opened with
    // `create(true)`, which fails if its directory is absent.
    ensure_dir(base_dir)?;
    let path = format!("{base_dir}/{DIR_LOCK_FILENAME}");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| StoreError::storage(format!("open data-directory lock '{path}'"), e.into()))?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
    let deadline = std::time::Instant::now() + retry;
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

impl RelationRegistry {
    /// `<owner's directory>/idx_<index_id>` — the one name this registry gives an
    /// index's directory. `None` for an unregistered owner.
    ///
    /// By id and not by column list, because the directory is named before the
    /// index it belongs to is entered — and because a promoted index outlives the
    /// row that named it, so only the *creating* id spells the path on disk.
    pub fn index_dir(&self, owner: i64, index_id: i64) -> Option<String> {
        self.relation(owner)
            .map(|e| ChildAddr::Index { id: index_id }.dir(e.directory()))
    }

    /// Drop `id`'s entry and erase this process's store directory for it. Its own
    /// child goes through [`remove_child`](crate::storage) first, so a crash
    /// mid-removal leaves no manifest whose shards are gone.
    pub fn unregister_and_erase(&mut self, id: i64) {
        let Some(dir) = self.relation(id).map(|e| e.directory().to_string()) else {
            return;
        };
        self.unregister(id);
        crate::storage::remove_child(&ChildAddr::worker(self.slot).dir(&dir));
        if let Err(e) = fs::remove_dir_all(&dir) {
            gnitz_debug!("relation: failed to erase relation dir {}: {}", dir, e);
        }
    }

    /// Remove every relation directory under `schema_dirs` that no registered
    /// relation owns, and every `idx_<id>` child of a live relation that no
    /// registered index owns. Only `<tag>_<digits>` names are eligible.
    /// Best-effort: a failure to remove one orphan is logged and never aborts
    /// the caller.
    pub fn reclaim_orphan_relation_dirs(&self, schema_dirs: impl IntoIterator<Item = String>) {
        self.assert_origin("reclaim_orphan_relation_dirs");
        let live: rustc_hash::FxHashMap<&str, &Relation> = self.relations().map(|e| (e.directory(), e)).collect();

        for schema_dir in schema_dirs {
            for name in subdir_names(&schema_dir) {
                let full = format!("{schema_dir}/{name}");

                if let Some(entry) = live.get(full.as_str()) {
                    // Live table/view: sweep orphaned `idx_<id>` sub-dirs left by
                    // a standalone DROP INDEX whose gated deletion was lost to a
                    // crash. Matched on the circuit's own `index_id`, since a
                    // promoted circuit outlives the IDX_TAB row that named it.
                    for idx_name in subdir_names(&full) {
                        let Some(ChildAddr::Index { id }) = ChildAddr::parse(&idx_name) else {
                            continue;
                        };
                        if entry.indexes().iter().any(|ix| ix.id() == id) {
                            // Its per-worker children are `reconcile_child_dirs`'
                            // job — the sweep descends into an index dir.
                            continue;
                        }
                        let idx_full = format!("{full}/{idx_name}");
                        match fs::remove_dir_all(&idx_full) {
                            Ok(()) => gnitz_debug!("recovery: removed orphan index dir {}", idx_full),
                            Err(e) => gnitz_debug!("recovery: failed to remove orphan index dir {}: {}", idx_full, e),
                        }
                    }
                    continue;
                }

                // Only `<something>_<digits>` dirs are eligible for removal —
                // never touch an unexpected entry. Every writer directly under a
                // schema dir uses that shape, so a matching name absent from
                // `live` is orphaned either way.
                if !is_table_dir_name(&name) {
                    continue;
                }
                match fs::remove_dir_all(&full) {
                    Ok(()) => gnitz_debug!("recovery: removed orphan table/view dir {}", full),
                    Err(e) => gnitz_debug!("recovery: failed to remove orphan dir {}: {}", full, e),
                }
            }
        }
    }
}
