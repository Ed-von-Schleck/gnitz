//! The relation directory tree: relation directory naming, the names of the
//! child directories under one and the sweeps over them, the orphan sweep, and
//! the data-directory lock. A child directory is one store's directory, whose
//! file names storage owns.

use std::fs::{self, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use gnitz_foundation::posix_io::{create_dir, fsync_dir};
use gnitz_wire::PkColList;

use super::{Relation, RelationRegistry, Store};
use crate::schema::Slot;
use crate::storage::{manifest_path, published, retire_store, StorageError, StoreError};

/// What a child directory holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChildKind<'a> {
    /// The relation's own rows: a table's partition, a view's output.
    Rows,
    /// Operator-state table `child` of a compiled view.
    Scratch(&'a str),
    /// A fed view's retained deltas.
    Delta,
    /// Secondary index over column list `cols` of the relation.
    Index(PkColList),
}

/// One child directory of a relation: `kind`'s store for worker `slot`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ChildAddr<'a> {
    pub kind: ChildKind<'a>,
    pub slot: Slot,
}

impl<'a> ChildAddr<'a> {
    /// The directory name, relative to the relation's directory.
    fn name(&self) -> String {
        let Slot { rank, of } = self.slot;
        match self.kind {
            ChildKind::Rows => format!("w{rank}of{of}"),
            ChildKind::Scratch(child) => format!("scratch_{child}_w{rank}of{of}"),
            ChildKind::Delta => format!("delta_w{rank}of{of}"),
            ChildKind::Index(cols) => {
                let list: Vec<String> = cols.as_slice().iter().map(u32::to_string).collect();
                format!("idx_{}_w{rank}of{of}", list.join("-"))
            }
        }
    }

    /// The child's directory under `rel_dir`.
    pub fn dir(&self, rel_dir: &str) -> String {
        format!("{rel_dir}/{}", self.name())
    }

    /// The child's manifest path — what the boot relayout's completeness test
    /// looks for.
    pub fn manifest(&self, rel_dir: &str) -> String {
        manifest_path(&self.dir(rel_dir))
    }

    /// The child `name` denotes, or `None` if it is not exactly the name a builder
    /// produces.
    pub(super) fn parse(name: &'a str) -> Option<Self> {
        Self::parse_grammar(name).filter(|addr| addr.name() == name)
    }

    fn parse_grammar(name: &'a str) -> Option<Self> {
        let (prefix, slot) = match name.rsplit_once("_w") {
            Some((prefix, slot)) => (Some(prefix), slot),
            None => (None, name.strip_prefix('w')?),
        };
        let (rank, of) = slot.split_once("of")?;
        let (rank, of) = (parse_id(rank)?, parse_id(of)?);
        let slot = (rank < of).then_some(Slot { rank, of })?;
        let kind = match prefix {
            None => ChildKind::Rows,
            Some("delta") => ChildKind::Delta,
            Some(p) => match p.strip_prefix("idx_") {
                Some(list) => {
                    let cols: Vec<u32> = list.split('-').map(parse_id).collect::<Option<_>>()?;
                    gnitz_wire::validate_pk_col_list(&cols, gnitz_wire::PK_LIST_COL_LIMIT).ok()?;
                    ChildKind::Index(PkColList::from_slice(&cols))
                }
                None => ChildKind::Scratch(p.strip_prefix("scratch_")?),
            },
        };
        Some(ChildAddr { kind, slot })
    }
}

/// Every child a relation has across the whole cluster at `num_workers` — one
/// per launched rank.
pub(super) fn cluster_children(num_workers: u32) -> impl Iterator<Item = ChildAddr<'static>> {
    (0..num_workers).map(move |rank| ChildAddr {
        kind: ChildKind::Rows,
        slot: Slot::new(rank, num_workers),
    })
}

/// Whether each launched rank's rows and every scratch child under `rel_dir`
/// carry a manifest at `generation`; `false` on any I/O failure.
pub(super) fn children_at_generation(rel_dir: &str, num_workers: u32, generation: u64) -> bool {
    let Ok(names) = subdir_names(rel_dir) else { return false };
    let scratch = names
        .iter()
        .filter(|n| matches!(ChildAddr::parse(n), Some(ChildAddr { kind: ChildKind::Scratch(_), .. })));
    cluster_children(num_workers)
        .map(|c| c.dir(rel_dir))
        .chain(scratch.map(|n| format!("{rel_dir}/{n}")))
        .all(|d| matches!(published(&d), Ok(Some(p)) if p.checkpoint_mark == generation))
}

/// The caller record of `slot`'s rows child under `rel_dir`; `Ok(None)` without
/// an intact manifest.
fn caller_record_at(rel_dir: &str, slot: Slot) -> Result<Option<Vec<u8>>, StorageError> {
    let dir = ChildAddr { kind: ChildKind::Rows, slot }.dir(rel_dir);
    Ok(published(&dir)?.map(|p| p.caller_record))
}

/// Immediate sub-directory names of `path`, none if it is missing. Collected
/// before return, since callers remove entries from the directory they walk.
pub(super) fn subdir_names(path: &str) -> Result<Vec<String>, StorageError> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(names)
}

/// A directory-name id component: ASCII digits only.
fn parse_id<T: std::str::FromStr>(s: &str) -> Option<T> {
    s.bytes().all(|b| b.is_ascii_digit()).then(|| s.parse().ok())?
}

/// Retire every child of `dir` that `dead` picks; names in no child grammar are
/// left alone.
pub(super) fn remove_children(dir: &str, dead: impl Fn(&ChildAddr) -> bool) -> Result<(), StorageError> {
    for name in subdir_names(dir)? {
        if ChildAddr::parse(&name).is_some_and(|c| dead(&c)) {
            gnitz_debug!("relation: removing child dir {}/{}", dir, name);
            retire_store(&format!("{dir}/{name}"))?;
        }
    }
    Ok(())
}

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
const DIR_LOCK_FILENAME: &str = "LOCK";

/// `<base_dir>/_relations` — the directory every relation's store directory sits
/// in, system families included. Only this crate's hosts write it.
pub fn relations_dir(base_dir: &str) -> String {
    format!("{base_dir}/_relations")
}

/// `<base_dir>/_relations/<id>`.
pub fn relation_dir(base_dir: &str, id: u64) -> String {
    format!("{}/{id}", relations_dir(base_dir))
}

/// The relation id `name` denotes, or `None` unless [`relation_dir`] gives that
/// id exactly this name.
fn parse_relation_dir_name(name: &str) -> Option<u64> {
    parse_id(name).filter(|id: &u64| id.to_string() == name)
}

pub(crate) fn ensure_dir(path: &str) -> Result<(), StoreError> {
    create_dir(path)
        .map(drop)
        .map_err(|e| StoreError::storage(format!("create directory '{path}'"), e.into()))
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
    let create_err = |e: std::io::Error| StoreError::storage(format!("create '{root}'"), e.into());
    if create_dir(&root).map_err(create_err)? {
        fsync_dir(base_dir).map_err(create_err)?;
    }
    Ok(DirLock { _file: file })
}

/// One relation directory's caller record, or the I/O error reading it failed with.
type PersistedRecord = (u64, Result<Vec<u8>, StoreError>);

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
    pub fn unregister_and_erase(&mut self, id: u64) -> Result<(), StoreError> {
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

#[cfg(test)]
#[path = "tests/dirs.rs"]
mod tests;
