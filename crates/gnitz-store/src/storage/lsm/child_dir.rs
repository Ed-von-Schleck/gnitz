//! The names of the child directories under a relation's directory, and the
//! sweeps over them. `naming` owns the file names inside one.

use std::fs;

use gnitz_wire::PkColList;

use super::error::StorageError;
use super::manifest;

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

/// Which worker this process is, of how many: the one input that decides which
/// `w{k}of{n}` child every store of this process opens.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Slot {
    pub rank: u32,
    pub of: u32,
}

impl Slot {
    /// A one-worker process: the mirror, and every unit test.
    pub const SOLO: Slot = Slot { rank: 0, of: 1 };

    /// Panics unless `rank < of`.
    pub fn new(rank: u32, of: u32) -> Slot {
        assert!(rank < of, "slot {rank} of {of}");
        Slot { rank, of }
    }
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
        manifest::path(&self.dir(rel_dir))
    }

    /// The child `name` denotes, or `None` if it is not exactly the name a builder
    /// produces.
    pub(crate) fn parse(name: &'a str) -> Option<Self> {
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
pub(crate) fn children_at_generation(rel_dir: &str, num_workers: u32, generation: u64) -> bool {
    let Ok(names) = subdir_names(rel_dir) else { return false };
    let scratch = names
        .iter()
        .filter(|n| matches!(ChildAddr::parse(n), Some(ChildAddr { kind: ChildKind::Scratch(_), .. })));
    cluster_children(num_workers)
        .map(|c| c.dir(rel_dir))
        .chain(scratch.map(|n| format!("{rel_dir}/{n}")))
        .all(|d| matches!(manifest::read(&d), Ok(Some(m)) if m.stamp.checkpoint_gen == generation))
}

/// The caller record of `slot`'s rows child under `rel_dir`; `Ok(None)` without
/// a manifest.
pub(crate) fn caller_record_at(rel_dir: &str, slot: Slot) -> Result<Option<Vec<u8>>, StorageError> {
    let dir = ChildAddr { kind: ChildKind::Rows, slot }.dir(rel_dir);
    Ok(manifest::read(&dir)?.map(|m| m.caller_record))
}

/// Immediate sub-directory names of `path`, none if it is missing. Collected
/// before return, since callers remove entries from the directory they walk.
pub(crate) fn subdir_names(path: &str) -> Result<Vec<String>, StorageError> {
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
pub(crate) fn parse_id<T: std::str::FromStr>(s: &str) -> Option<T> {
    s.bytes().all(|b| b.is_ascii_digit()).then(|| s.parse().ok())?
}

/// Retire a child: once this returns `Ok`, no crash brings its manifest back.
pub(crate) fn remove_child(dir: &str) -> Result<(), StorageError> {
    manifest::unlink(dir)?;
    // Without its manifest the directory holds no reachable rows.
    match fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            gnitz_warn!("storage: failed to remove retired child dir {}: {}", dir, e)
        }
        _ => {}
    }
    Ok(())
}

/// Remove every child of `dir` laid out for a different worker count than
/// `num_workers`; names in no child grammar are left alone.
pub(crate) fn reclaim_retired_children(dir: &str, num_workers: u32) -> Result<(), StorageError> {
    for name in subdir_names(dir)? {
        if ChildAddr::parse(&name).is_some_and(|c| c.slot.of != num_workers) {
            gnitz_debug!("recovery: removing retired child dir {}/{}", dir, name);
            remove_child(&format!("{dir}/{name}"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/child_dir.rs"]
mod tests;
