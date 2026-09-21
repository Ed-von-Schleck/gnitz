//! Relation directory naming, the orphan sweeps, and the data-directory lock.
//!
//! Every directory a relation registration itself names is *built* and *parsed
//! back* here, so the creation path and the orphan sweeps can never disagree on
//! the shape. Below a relation directory the names are storage's `ChildAddr`
//! grammar, and the sweeps classify them through it.

use std::fs;

use super::{Relation, RelationKind, RelationRegistry};
use crate::storage::{create_dir, fsync_dir, remove_child, subdir_names, ChildAddr, ChildKind, StoreError};

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
const DIR_LOCK_FILENAME: &str = "LOCK";

/// `<base_dir>/_relations` — the directory every relation's store directory sits
/// in, system families included. Only this crate's hosts write it.
pub fn relations_dir(base_dir: &str) -> String {
    format!("{base_dir}/_relations")
}

/// `<base_dir>/_relations/v_<id>` for a view, `.../t_<id>` otherwise. Id-only
/// (no embedded name), so a RENAME never changes the path and never orphans data.
pub fn relation_dir(base_dir: &str, kind: RelationKind, id: i64) -> String {
    let tag = if kind.is_view() { 'v' } else { 't' };
    format!("{}/{tag}_{id}", relations_dir(base_dir))
}

pub(crate) fn ensure_dir(path: &str) -> Result<(), StoreError> {
    create_dir(path)
        .map(drop)
        .map_err(|e| StoreError::storage(format!("create directory '{path}'"), e))
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

/// Create `base_dir` and its [`relations_dir`], and take the exclusive `flock` on
/// its lock file, retrying a held lock for `retry`. A forked worker shares its
/// parent's lock; a second `open` in one process contends. The returned file must
/// outlive every store under `base_dir`.
/// Sets NOCOW on `base_dir`, which everything later created under it inherits.
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
            gnitz_foundation::posix_io::try_set_nocow(base_dir);
            let root = relations_dir(base_dir);
            let create_err = |e| StoreError::storage(format!("create '{root}'"), e);
            if create_dir(&root).map_err(create_err)? {
                fsync_dir(base_dir).map_err(create_err)?;
            }
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
    /// Drop `id`'s entry and erase this process's store for it.
    pub fn unregister_and_erase(&mut self, id: i64) -> Result<(), StoreError> {
        let Some(dir) = self.relation(id).map(|e| e.directory().to_string()) else {
            return Ok(());
        };
        self.unregister(id);
        let child = ChildAddr { kind: ChildKind::Rows, slot: self.slot }.dir(&dir);
        remove_child(&child).map_err(|e| StoreError::storage(format!("erase relation {id} (dir={child})"), e))?;
        if let Err(e) = fs::remove_dir_all(&dir) {
            gnitz_debug!("relation: failed to erase relation dir {}: {}", dir, e);
        }
        Ok(())
    }

    /// Best-effort removal of every directory under [`relations_dir`] no registered
    /// relation owns, and every index child no registered index owns. Sound only
    /// where every process sharing the base directory has applied this catalog.
    pub fn reclaim_orphan_relation_dirs(&self) {
        let root = relations_dir(&self.base_dir);
        let live: rustc_hash::FxHashMap<&str, &Relation> = self.relations().map(|e| (e.directory(), e)).collect();

        for name in subdir_names(&root).unwrap_or_default() {
            let full = format!("{root}/{name}");
            let Some(entry) = live.get(full.as_str()) else {
                match fs::remove_dir_all(&full) {
                    Ok(()) => gnitz_debug!("recovery: removed orphan relation dir {}", full),
                    Err(e) => gnitz_debug!("recovery: failed to remove orphan dir {}: {}", full, e),
                }
                continue;
            };
            // Matched on the circuit's own `index_id`, since a promoted circuit
            // outlives the IDX_TAB row that named it.
            for child in subdir_names(&full).unwrap_or_default() {
                let Some(ChildAddr { kind: ChildKind::Index(id), .. }) = ChildAddr::parse(&child) else {
                    continue;
                };
                if entry.indexes().iter().any(|ix| ix.id() == id) {
                    continue;
                }
                let child_full = format!("{full}/{child}");
                match remove_child(&child_full) {
                    Ok(()) => gnitz_debug!("recovery: removed orphan index dir {}", child_full),
                    Err(e) => gnitz_debug!("recovery: failed to remove orphan index dir {}: {}", child_full, e),
                }
            }
        }
    }
}
