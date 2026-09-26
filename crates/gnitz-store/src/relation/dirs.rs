//! Relation directory naming, the orphan sweeps, and the data-directory lock.
//!
//! Every directory a relation registration itself names is *built* and *parsed
//! back* here, so the creation path and the orphan sweeps can never disagree on
//! the shape. Below a relation directory the names are storage's `ChildAddr`
//! grammar, and the sweeps classify them through it.

use std::fs;

use super::{Relation, RelationKind, RelationRegistry};
use crate::storage::{
    caller_record_at, create_dir, fsync_dir, parse_id, remove_child, subdir_names, ChildAddr, ChildKind, StorageError,
    StoreError,
};

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
const DIR_LOCK_FILENAME: &str = "LOCK";

/// `<base_dir>/_relations` — the directory every relation's store directory sits
/// in, system families included. Only this crate's hosts write it.
pub fn relations_dir(base_dir: &str) -> String {
    format!("{base_dir}/_relations")
}

const VIEW_TAG: &str = "v_";
const TABLE_TAG: &str = "t_";

/// `<base_dir>/_relations/v_<id>` for a view, `.../t_<id>` otherwise. Id-only
/// (no embedded name), so a RENAME never changes the path and never orphans data.
pub fn relation_dir(base_dir: &str, kind: RelationKind, id: i64) -> String {
    format!("{}/{}", relations_dir(base_dir), dir_name(kind.is_view(), id))
}

fn dir_name(is_view: bool, id: i64) -> String {
    let tag = if is_view { VIEW_TAG } else { TABLE_TAG };
    format!("{tag}{id}")
}

/// The view id `name` denotes, or `None` unless [`relation_dir`] gives that
/// view exactly this name.
fn parse_view_dir_name(name: &str) -> Option<i64> {
    let id = parse_id(name.strip_prefix(VIEW_TAG)?)?;
    (dir_name(true, id) == name).then_some(id)
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

/// One view directory's caller record, or the I/O error reading it failed with.
type PersistedRecord = (i64, Result<Vec<u8>, StoreError>);

impl RelationRegistry {
    /// Each view directory's caller record for this slot, by id. A directory
    /// whose manifest is absent or damaged holds none; a failed read is that
    /// view's `Err`.
    pub fn persisted_view_records(&self) -> Result<Vec<PersistedRecord>, StoreError> {
        let root = relations_dir(&self.base_dir);
        let names = subdir_names(&root).map_err(|e| StoreError::storage(format!("list '{root}'"), e))?;
        let mut records = Vec::new();
        for name in names {
            let Some(id) = parse_view_dir_name(&name) else { continue };
            let dir = format!("{root}/{name}");
            match caller_record_at(&dir, self.slot) {
                Ok(Some(record)) => records.push((id, Ok(record))),
                Err(e @ StorageError::Io(_)) => records.push((
                    id,
                    Err(StoreError::storage(format!("read the manifest under '{dir}'"), e)),
                )),
                Ok(None) | Err(_) => {}
            }
        }
        Ok(records)
    }

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
