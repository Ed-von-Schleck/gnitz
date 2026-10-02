//! The relation directory tree: relation directory naming, the names of the
//! child directories under one and the sweeps over them, the orphan sweep, and
//! the data-directory lock. A child directory is one store's directory, whose
//! file names storage owns.

use std::fs::{self, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use gnitz_foundation::posix_io::{create_dir, fsync_dir};
use gnitz_wire::PkColList;

use super::RelationRegistry;
use crate::storage::{manifest_path, read_at, read_intact, retire_store};
use gnitz_zset::repr::StorageError;
use gnitz_zset::schema::Slot;

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

impl ChildKind<'_> {
    /// What a child directory's name carries ahead of its slot; none for the
    /// relation's own rows.
    fn prefix(&self) -> Option<String> {
        match self {
            ChildKind::Rows => None,
            ChildKind::Scratch(child) => Some(format!("scratch_{child}")),
            ChildKind::Delta => Some("delta".to_string()),
            ChildKind::Index(cols) => {
                let list: Vec<String> = cols.as_slice().iter().map(u32::to_string).collect();
                Some(format!("idx_{}", list.join("-")))
            }
        }
    }

    /// The kind's name in a report that sums a store over its slots.
    pub(super) fn label(&self) -> String {
        self.prefix().unwrap_or_else(|| "rows".to_string())
    }
}

impl<'a> ChildAddr<'a> {
    /// The directory name, relative to the relation's directory.
    fn name(&self) -> String {
        let Slot { rank, of } = self.slot;
        match self.kind.prefix() {
            None => format!("w{rank}of{of}"),
            Some(prefix) => format!("{prefix}_w{rank}of{of}"),
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
        let (rank, of) = (rank.parse().ok()?, of.parse().ok()?);
        let slot = (rank < of).then_some(Slot { rank, of })?;
        let kind = match prefix {
            None => ChildKind::Rows,
            Some("delta") => ChildKind::Delta,
            Some(p) => match p.strip_prefix("idx_") {
                Some(list) => {
                    let cols: Vec<u32> = list.split('-').map(|c| c.parse().ok()).collect::<Option<_>>()?;
                    ChildKind::Index(PkColList::checked(&cols, gnitz_wire::MAX_COLUMNS).ok()?)
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
        .all(|d| matches!(read_at(&d, generation), Ok(Some(_))))
}

/// The caller record of `slot`'s rows child under `rel_dir`; `Ok(None)` without
/// an intact manifest.
fn caller_record_at(rel_dir: &str, slot: Slot) -> Result<Option<Vec<u8>>, StorageError> {
    let dir = ChildAddr { kind: ChildKind::Rows, slot }.dir(rel_dir);
    Ok(read_intact(&dir)?.map(|m| m.caller_record))
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
pub(super) fn parse_relation_dir_name(name: &str) -> Option<u64> {
    name.parse().ok().filter(|id: &u64| id.to_string() == name)
}

pub(crate) fn ensure_dir(path: &str) -> Result<(), String> {
    create_dir(path)
        .map(drop)
        .map_err(|e| format!("create directory '{path}': {e}"))
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
pub fn lock_data_dir(base_dir: &str) -> Result<DirLock, String> {
    ensure_dir(base_dir)?;
    let path = format!("{base_dir}/{DIR_LOCK_FILENAME}");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| format!("open data-directory lock '{path}': {e}"))?;
    let deadline = Instant::now() + DIR_LOCK_RETRY_FOR;
    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => thread::sleep(DIR_LOCK_RETRY_EVERY),
            Err(TryLockError::WouldBlock) => return Err(format!("data directory '{base_dir}' is already held")),
            Err(TryLockError::Error(e)) => return Err(format!("lock data directory '{base_dir}': {e}")),
        }
    }
    gnitz_foundation::posix_io::try_set_nocow(base_dir);
    let root = relations_dir(base_dir);
    let create_err = |e: std::io::Error| format!("create '{root}': {e}");
    if create_dir(&root).map_err(create_err)? {
        fsync_dir(base_dir).map_err(create_err)?;
    }
    Ok(DirLock { _file: file })
}

/// One relation directory's caller record, or the I/O error reading it failed with.
type PersistedRecord = (u64, Result<Vec<u8>, String>);

impl RelationRegistry {
    /// Each relation directory's caller record for this slot, by id. A directory
    /// whose manifest is absent or damaged holds none; a failed read is that
    /// relation's `Err`.
    pub fn persisted_records(&self) -> Result<Vec<PersistedRecord>, String> {
        let root = relations_dir(&self.base_dir);
        let names = subdir_names(&root).map_err(|e| format!("list '{root}': {e}"))?;
        let mut records = Vec::new();
        for name in names {
            let Some(id) = parse_relation_dir_name(&name) else {
                continue;
            };
            let dir = format!("{root}/{name}");
            match caller_record_at(&dir, self.slot) {
                Ok(Some(record)) => records.push((id, Ok(record))),
                Err(e) => records.push((id, Err(format!("read the manifest under '{dir}': {e}")))),
                Ok(None) => {}
            }
        }
        Ok(records)
    }

    /// Drop `id`'s entry and erase its directory, every rank's children included.
    pub fn unregister_and_erase(&mut self, id: u64) -> Result<(), String> {
        assert_eq!(self.slot.of, 1, "erasing a directory other ranks' stores live in");
        if self.tables.remove(&id).is_none() {
            return Ok(());
        }
        let dir = relation_dir(&self.base_dir, id);
        // Durably first: a child's manifest carries the record a reopen would revive.
        remove_children(&dir, |_| true).map_err(|e| format!("erase relation {id} (dir={dir}): {e}"))?;
        if let Err(e) = fs::remove_dir_all(&dir) {
            gnitz_debug!("relation: failed to erase relation dir {}: {}", dir, e);
        }
        Ok(())
    }

    /// Remove every relation directory and child this registry does not own. Sound
    /// only once every process sharing `base_dir` has applied this catalog.
    pub fn reclaim_orphan_relation_dirs(&self) -> Result<(), String> {
        let root = relations_dir(&self.base_dir);
        let names = subdir_names(&root).map_err(|e| format!("list '{root}': {e}"))?;
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
            .map_err(|e| format!("reclaim children of '{dir}': {e}"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/dirs.rs"]
mod tests;
