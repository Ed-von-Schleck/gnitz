//! The relation directory tree: relation directory naming, the names of the
//! child directories under one and the sweeps over them — the boot relay, the
//! resume verdict over a view's children, the orphan sweep — and the
//! data-directory lock. A child directory is one store's directory, whose file
//! names storage owns.

use std::fs::{self, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use gnitz_foundation::posix_io::{create_dir_all_durable, fsync_dir};
use gnitz_wire::PkColList;

use super::{RelationKind, RelationRegistry, Residency, SecondaryIndex, StateLayout};
use crate::storage::{link_stores, manifest_path, read_at, read_intact, retire_stores};
use gnitz_zset::algebra::Slot;

/// What a child directory holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChildKind<'a> {
    /// The relation's own rows: a table's partition, a view's output.
    Rows,
    /// Operator-state table `child` of a compiled view.
    Scratch(&'a str),
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
    /// The kind's name in a report that sums a store over its slots, and what a
    /// child directory's name carries ahead of its slot for every kind but the
    /// relation's own rows.
    pub(super) fn label(&self) -> String {
        match self {
            ChildKind::Rows => "rows".to_string(),
            ChildKind::Scratch(child) => format!("scratch_{child}"),
            ChildKind::Index(cols) => {
                let list: Vec<String> = cols.as_slice().iter().map(u32::to_string).collect();
                format!("idx_{}", list.join("-"))
            }
        }
    }
}

impl<'a> ChildAddr<'a> {
    /// The directory name, relative to the relation's directory.
    fn name(&self) -> String {
        let Slot { rank, of } = self.slot;
        match self.kind {
            ChildKind::Rows => format!("w{rank}of{of}"),
            kind => format!("{}_w{rank}of{of}", kind.label()),
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
        let (prefix, slot) = match name.rsplit_once("_w") {
            Some((prefix, slot)) => (Some(prefix), slot),
            None => (None, name.strip_prefix('w')?),
        };
        let (rank, of) = slot.split_once("of")?;
        let (rank, of) = (rank.parse().ok()?, of.parse().ok()?);
        let slot = (rank < of).then_some(Slot { rank, of })?;
        let kind = match prefix {
            None => ChildKind::Rows,
            Some(p) => match p.strip_prefix("idx_") {
                Some(list) => {
                    let cols: Vec<u32> = list.split('-').map(|c| c.parse().ok()).collect::<Option<_>>()?;
                    ChildKind::Index(PkColList::checked(&cols, gnitz_wire::MAX_COLUMNS).ok()?)
                }
                None => ChildKind::Scratch(p.strip_prefix("scratch_")?),
            },
        };
        // The grammar's numbers also read `+1` and `01`; the builder writes neither.
        let addr = ChildAddr { kind, slot };
        (addr.name() == name).then_some(addr)
    }
}

/// Every `kind` child a relation has across the whole cluster at `num_workers`
/// — one per launched rank.
pub(super) fn cluster_children(kind: ChildKind<'_>, num_workers: u32) -> impl Iterator<Item = ChildAddr<'_>> {
    (0..num_workers).map(move |rank| ChildAddr { kind, slot: Slot::new(rank, num_workers) })
}

/// Whether, on each of `num_workers` ranks under `rel_dir`, the rows child and
/// the scratch child of every trace `layout` declares carry a manifest at
/// `generation`; `false` on any I/O failure. A trace with no directory is a
/// mismatch: a crash part-way through retiring a view's children can take it
/// and leave its siblings.
fn children_at_generation(rel_dir: &str, num_workers: u32, generation: u64, layout: &StateLayout) -> bool {
    let kinds = std::iter::once(ChildKind::Rows).chain(layout.names().map(ChildKind::Scratch));
    kinds
        .flat_map(|kind| cluster_children(kind, num_workers))
        .all(|c| matches!(read_at(&c.dir(rel_dir), generation), Ok(Some(_))))
}

/// Immediate sub-directory names of `path`, none if it is missing. Collected
/// before return, since callers remove entries from the directory they walk.
pub(super) fn subdir_names(path: &str) -> Result<Vec<String>, String> {
    let fail = |e: std::io::Error| format!("list '{path}': {e}");
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(fail(e)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(fail)?;
        if entry.file_type().map_err(fail)?.is_dir() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(names)
}

/// The name of every child of `dir` that `pick` selects; a name in no child
/// grammar is never selected.
fn children(dir: &str, pick: impl Fn(&ChildAddr) -> bool) -> Result<Vec<String>, String> {
    let mut names = subdir_names(dir)?;
    names.retain(|n| ChildAddr::parse(n).is_some_and(|c| pick(&c)));
    Ok(names)
}

/// `<base_dir>/LOCK` — the file whose `flock` makes a data directory
/// single-writer.
const DIR_LOCK_FILENAME: &str = "LOCK";

/// `<base_dir>/_relations` — the directory every relation's store directory sits
/// in, system families included. Only this crate's hosts write it.
pub(super) fn relations_dir(base_dir: &str) -> String {
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

pub(super) fn ensure_dir(path: &str) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|e| format!("create directory '{path}': {e}"))
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
/// then create its [`relations_dir`].
pub fn lock_data_dir(base_dir: &str) -> Result<DirLock, String> {
    create_dir_all_durable(base_dir).map_err(|e| format!("create directory '{base_dir}': {e}"))?;
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
    let root = relations_dir(base_dir);
    ensure_dir(&root)?;
    // `LOCK` and `_relations` are entries of `base_dir`, synced every boot: a
    // boot killed before this sync left them created and unsynced.
    fsync_dir(base_dir).map_err(|e| format!("fsync '{base_dir}': {e}"))?;
    Ok(DirLock { _file: file })
}

/// One relation directory's id, with the generation and caller record its
/// manifest carries or the I/O error reading it failed with.
type PersistedRecord = (u64, Result<(u64, Vec<u8>), String>);

impl RelationRegistry {
    /// Each relation directory's generation and caller record for this slot, by
    /// id. A directory whose manifest is absent or damaged holds none; a failed
    /// read is that relation's `Err`.
    pub fn persisted_records(&self) -> Result<Vec<PersistedRecord>, String> {
        let names = subdir_names(&relations_dir(&self.base_dir))?;
        let mut records = Vec::new();
        for id in names.iter().filter_map(|n| parse_relation_dir_name(n)) {
            let dir = self.child_dir(id, ChildKind::Rows);
            match read_intact(&dir) {
                Ok(Some(m)) => records.push((id, Ok((m.checkpoint_mark, m.caller_record)))),
                Err(e) => records.push((id, Err(format!("read the manifest under '{dir}': {e}")))),
                Ok(None) => {}
            }
        }
        Ok(records)
    }

    /// Drop `id`'s entry and erase its directory, every rank's children included.
    pub fn unregister_and_erase(&mut self, id: u64) -> Result<(), String> {
        assert_eq!(
            self.residency,
            Residency::Origin,
            "erasing a directory other processes' stores live in"
        );
        if self.relations.remove(&id).is_none() {
            return Ok(());
        }
        let dir = relation_dir(&self.base_dir, id);
        // Durably first: a child's manifest carries the record a reopen would revive.
        let fail = |e: String| format!("erase relation {id} (dir={dir}): {e}");
        let stores: Vec<String> = children(&dir, |_| true)
            .map_err(fail)?
            .iter()
            .map(|n| format!("{dir}/{n}"))
            .collect();
        retire_stores(&stores).map_err(|e| fail(e.to_string()))?;
        if let Err(e) = fs::remove_dir_all(&dir) {
            gnitz_debug!("relation: failed to erase relation dir {}: {}", dir, e);
        }
        Ok(())
    }

    /// Relay each base table's children onto this boot's worker count, then
    /// [`Self::reclaim_orphan_relation_dirs`]. Idempotent.
    pub fn reconcile_child_dirs<'l>(&self, layout_of: impl Fn(u64) -> Option<&'l StateLayout>) -> Result<(), String> {
        // A relay removes the set it read, and the reclaim deletes directories.
        assert_eq!(
            self.residency,
            Residency::Master,
            "reconcile_child_dirs runs on a process holding no user store"
        );
        // Only a base table carries rows across a worker-count change.
        for (&id, entry) in self.relations.iter().filter(|(_, e)| e.kind().is_base_table()) {
            super::repartition::repartition_relation(
                &relation_dir(&self.base_dir, id),
                &entry.schema(),
                entry.placement(),
                self.slot.of,
                &self.config,
            )?;
        }
        self.reclaim_orphan_relation_dirs(layout_of)
    }

    /// Whether the rows child and every trace child `layout`, its compiled
    /// operator state, declares of view `view_id` carry a manifest at
    /// `generation` on **every launched rank** — the store half of the resume
    /// verdict.
    pub fn view_children_resumable(&self, view_id: u64, generation: u64, layout: &StateLayout) -> bool {
        // A worker cannot speak for its peers.
        assert!(
            matches!(self.residency, Residency::Master | Residency::Origin),
            "view_children_resumable reads every rank's children"
        );
        children_at_generation(&relation_dir(&self.base_dir, view_id), self.slot.of, generation, layout)
    }

    /// Remove every relation directory and child this registry does not own — a
    /// child of another worker count, an index nothing declares or one that
    /// reads its owner's PK, a trace that `layout_of(id)`, view `id`'s compiled
    /// operator state, does not declare — the children retired in one batch. Sound only once every process sharing
    /// `base_dir` has applied this catalog.
    pub fn reclaim_orphan_relation_dirs<'l>(
        &self,
        layout_of: impl Fn(u64) -> Option<&'l StateLayout>,
    ) -> Result<(), String> {
        assert!(
            matches!(self.residency, Residency::Master | Residency::Origin),
            "reclaim_orphan_relation_dirs removes other ranks' children"
        );
        let root = relations_dir(&self.base_dir);
        let mut dead = Vec::new();
        for name in subdir_names(&root)? {
            let dir = format!("{root}/{name}");
            let owner = parse_relation_dir_name(&name).and_then(|id| self.relations.get(&id));
            let Some(entry) = owner else {
                // Unowned: no manifest in it is ever read again, so no fsync.
                if let Err(e) = fs::remove_dir_all(&dir) {
                    gnitz_debug!("relation: failed to remove orphan dir {}: {}", dir, e);
                }
                continue;
            };
            let of = self.slot.of;
            let picked = children(&dir, |c| {
                c.slot.of != of
                    || match c.kind {
                        ChildKind::Index(cols) => entry
                            .index_on(cols.as_slice())
                            .is_none_or(SecondaryIndex::reads_owner_pk),
                        ChildKind::Scratch(name) => {
                            !layout_of(entry.id()).is_some_and(|l| l.names().any(|t| t == name))
                        }
                        ChildKind::Rows => false,
                    }
            })?;
            dead.extend(picked.iter().map(|n| format!("{dir}/{n}")));
        }
        gnitz_debug!("relation: reclaiming {} child dir(s) under {}", dead.len(), root);
        retire_stores(&dead).map_err(|e| format!("reclaim the children no relation owns under '{root}': {e}"))
    }

    /// Put every store this process publishes into the data directory `dest`, at
    /// the path it holds under this one: hard-linked where `dest` shares this
    /// directory's filesystem, copied where it does not. A system family's store
    /// is its relation directory; any other relation's are the children of this
    /// process's slot. A store's manifest is what was published, so a store with
    /// none is left for the boot over `dest` to rebuild.
    pub fn link_published_into(&self, dest: &str) -> Result<(), String> {
        let mut pairs = Vec::new();
        for (&id, entry) in &self.relations {
            if !self.residency.publishes(entry.kind()) {
                continue;
            }
            let (src_rel, dst_rel) = (relation_dir(&self.base_dir, id), relation_dir(dest, id));
            if entry.kind() == RelationKind::SystemCatalog {
                pairs.push((src_rel, dst_rel));
                continue;
            }
            let own = children(&src_rel, |c| c.slot == self.slot)?;
            pairs.extend(own.iter().map(|n| (format!("{src_rel}/{n}"), format!("{dst_rel}/{n}"))));
        }
        link_stores(&pairs).map_err(|e| format!("put the published stores into '{dest}': {e}"))
    }
}

#[cfg(test)]
#[path = "tests/dirs.rs"]
mod tests;
