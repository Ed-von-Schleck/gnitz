//! Relation directory naming, the orphan sweep, and the data-directory lock.

use std::fs::{self, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use super::{Relation, RelationRegistry, Store};
use crate::storage::{
    caller_record_at, create_dir, fsync_dir, parse_id, remove_children, subdir_names, ChildKind, StoreError,
};

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
const DIR_LOCK_FILENAME: &str = "LOCK";

/// `<base_dir>/_relations` — the directory every relation's store directory sits
/// in, system families included. Only this crate's hosts write it.
pub fn relations_dir(base_dir: &str) -> String {
    format!("{base_dir}/_relations")
}

/// `<base_dir>/_relations/<id>`.
pub fn relation_dir(base_dir: &str, id: i64) -> String {
    format!("{}/{id}", relations_dir(base_dir))
}

/// The relation id `name` denotes, or `None` unless [`relation_dir`] gives that
/// id exactly this name.
fn parse_relation_dir_name(name: &str) -> Option<i64> {
    parse_id(name).filter(|id: &i64| id.to_string() == name)
}

pub(crate) fn ensure_dir(path: &str) -> Result<(), StoreError> {
    create_dir(path)
        .map(drop)
        .map_err(|e| StoreError::storage(format!("create directory '{path}'"), e))
}

/// How long [`lock_data_dir`] waits out a held lock: a forked child holds its
/// parent's lock until it execs or exits, so a free directory can read as held.
const DIR_LOCK_RETRY_FOR: Duration = Duration::from_secs(2);

/// How long [`lock_data_dir`] sleeps between attempts.
const DIR_LOCK_RETRY_EVERY: Duration = Duration::from_millis(20);

/// The exclusive `flock` on a data directory, held until dropped.
#[must_use = "the data directory is unlocked once this is dropped"]
pub struct DirLock {
    _file: fs::File,
}

/// Create and lock `base_dir`, retrying a held lock for [`DIR_LOCK_RETRY_FOR`],
/// then set NOCOW on it and create its [`relations_dir`].
pub fn lock_data_dir(base_dir: &str) -> Result<DirLock, StoreError> {
    ensure_dir(base_dir)?;
    let path = format!("{base_dir}/{DIR_LOCK_FILENAME}");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| StoreError::storage(format!("open data-directory lock '{path}'"), e.into()))?;
    let deadline = Instant::now() + DIR_LOCK_RETRY_FOR;
    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => thread::sleep(DIR_LOCK_RETRY_EVERY),
            Err(TryLockError::WouldBlock) => {
                return Err(StoreError::rejected(format!(
                    "data directory '{base_dir}' is already held"
                )))
            }
            Err(TryLockError::Error(e)) => {
                return Err(StoreError::storage(
                    format!("lock data directory '{base_dir}'"),
                    e.into(),
                ))
            }
        }
    }
    gnitz_foundation::posix_io::try_set_nocow(base_dir);
    let root = relations_dir(base_dir);
    let create_err = |e| StoreError::storage(format!("create '{root}'"), e);
    if create_dir(&root).map_err(create_err)? {
        fsync_dir(base_dir).map_err(create_err)?;
    }
    Ok(DirLock { _file: file })
}

/// One relation directory's caller record, or the I/O error reading it failed with.
type PersistedRecord = (i64, Result<Vec<u8>, StoreError>);

impl RelationRegistry {
    /// Each relation directory's caller record for this slot, by id. A directory
    /// whose manifest is absent or damaged holds none; a failed read is that
    /// relation's `Err`.
    pub fn persisted_records(&self) -> Result<Vec<PersistedRecord>, StoreError> {
        let root = relations_dir(&self.base_dir);
        let names = subdir_names(&root).map_err(|e| StoreError::storage(format!("list '{root}'"), e))?;
        let mut records = Vec::new();
        for name in names {
            let Some(id) = parse_relation_dir_name(&name) else {
                continue;
            };
            let dir = format!("{root}/{name}");
            match caller_record_at(&dir, self.slot) {
                Ok(Some(record)) => records.push((id, Ok(record))),
                Err(e) => records.push((
                    id,
                    Err(StoreError::storage(format!("read the manifest under '{dir}'"), e)),
                )),
                Ok(None) => {}
            }
        }
        Ok(records)
    }

    /// Drop `id`'s entry and erase its directory, every rank's children included.
    pub fn unregister_and_erase(&mut self, id: i64) -> Result<(), StoreError> {
        assert_eq!(self.slot.of, 1, "erasing a directory other ranks' stores live in");
        let Some(Relation { store, .. }) = self.tables.remove(&id) else {
            return Ok(());
        };
        let dir = relation_dir(&self.base_dir, id);
        // Durably first: the manifest carries the record a reopen would revive.
        if let Store::Held(table) = store {
            table
                .unlink_manifest()
                .map_err(|e| StoreError::storage(format!("erase relation {id} (dir={dir})"), e))?;
        }
        if let Err(e) = fs::remove_dir_all(&dir) {
            gnitz_debug!("relation: failed to erase relation dir {}: {}", dir, e);
        }
        Ok(())
    }

    /// Remove every relation directory and child this registry does not own. Sound
    /// only once every process sharing `base_dir` has applied this catalog.
    pub fn reclaim_orphan_relation_dirs(&self) -> Result<(), StoreError> {
        let root = relations_dir(&self.base_dir);
        let names = subdir_names(&root).map_err(|e| StoreError::storage(format!("list '{root}'"), e))?;
        for name in names {
            let dir = format!("{root}/{name}");
            let Some(entry) = parse_relation_dir_name(&name).and_then(|id| self.tables.get(&id)) else {
                // Unowned: no manifest in it is ever read again, so no fsync.
                if let Err(e) = fs::remove_dir_all(&dir) {
                    gnitz_debug!("relation: failed to remove orphan dir {}: {}", dir, e);
                }
                continue;
            };
            let of = self.slot.of;
            remove_children(&dir, |c| {
                c.slot.of != of || matches!(c.kind, ChildKind::Index(cols) if entry.index_on(cols.as_slice()).is_none())
            })
            .map_err(|e| StoreError::storage(format!("reclaim children of '{dir}'"), e))?;
        }
        Ok(())
    }
}
