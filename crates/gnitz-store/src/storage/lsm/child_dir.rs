//! The child directories under a relation's directory: how they are named, and
//! which of them a launched worker count still owns. Sibling of `naming`, which
//! owns the file grammar *inside* one of these. Every builder sits next to its
//! parser, so the paths that create children and the sweeps that reclaim them
//! cannot drift.

use std::fs;

use super::error::StorageError;
use super::manifest;

/// One child directory of a relation. The grammars are disjoint and together
/// cover every name a relation directory can legitimately hold, so a name on
/// disk identifies its kind without knowing how the store was built — which is
/// what lets a boot sweep reclaim children of a shape the relation no longer
/// has, and lets an unparseable name be treated as foreign data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChildAddr<'a> {
    /// `w{k}of{n}` — worker `k`'s store of a relation laid out for `n` workers.
    /// The worker count is in the name so the boot repartition can write the new
    /// layout beside the old one: the two sets are disjoint, so the source stays
    /// intact and readable until the target is durable.
    Worker { rank: u32, of: u32 },
    /// `scratch_{child}_w{k}` — worker `k`'s operator-state table for a view.
    Scratch { child: &'a str, rank: u32 },
    /// `delta_w{k}` — worker `k`'s delta store of a fed view: the recent deltas
    /// `WITH (delta = …)` retains, stamped with the tick round that produced
    /// them. Owned at `rank < num_workers`, the rule `Scratch` uses, because it
    /// is one store per launched worker and nothing repartitions it.
    ///
    /// A grammar of its own rather than a `Scratch` child, even though that would
    /// give the reclaim sweep and the ownership rule for free: a view reset
    /// blanket-removes **every** `Scratch` child at this rank, *after* its stores
    /// have been reopened — so a delta store's directory would be unlinked out
    /// from under a live `Table`. It also keeps a relation's own store out of the
    /// grammar a compiled circuit's operator state owns.
    Delta { rank: u32 },
    /// `idx_{id}` — a secondary index's own directory. Not a store of the owner
    /// relation: never repartitioned, and owned at every worker count (each
    /// worker rebuilds its slice inside it). Named here so one parser covers the
    /// whole namespace and no caller has to pre-filter.
    Index { id: i64 },
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

    /// Panics on `rank >= of` or `of == 0`: a slot outside its own layout names a
    /// child the boot sweep deletes as unowned.
    pub fn new(rank: u32, of: u32) -> Slot {
        assert!(of >= 1 && rank < of, "slot {rank} of {of}");
        Slot { rank, of }
    }
}

impl<'a> ChildAddr<'a> {
    /// `slot`'s store of a relation laid out for `slot.of` workers.
    pub fn worker(slot: Slot) -> Self {
        ChildAddr::Worker { rank: slot.rank, of: slot.of }
    }

    /// `slot`'s delta store of a fed view.
    pub(crate) fn delta(slot: Slot) -> Self {
        ChildAddr::Delta { rank: slot.rank }
    }

    /// The directory name, relative to the relation's directory.
    pub(crate) fn name(&self) -> String {
        match *self {
            ChildAddr::Worker { rank, of } => format!("w{rank}of{of}"),
            ChildAddr::Scratch { child, rank } => format!("scratch_{child}_w{rank}"),
            ChildAddr::Delta { rank } => format!("delta_w{rank}"),
            ChildAddr::Index { id } => format!("idx_{id}"),
        }
    }

    /// The child's directory under `rel_dir`.
    pub fn dir(&self, rel_dir: &str) -> String {
        format!("{rel_dir}/{}", self.name())
    }

    /// The child's manifest — what the boot resume verdict peeks and a rebuild
    /// unlinks.
    pub fn manifest(&self, rel_dir: &str) -> String {
        manifest::path(&self.dir(rel_dir))
    }

    /// The child `name` denotes, or `None` if it is in neither grammar. The
    /// scratch split is right-anchored because child names contain `_`
    /// themselves (`_reduce_{vid}_{nid}`).
    pub fn parse(name: &'a str) -> Option<Self> {
        if let Some(rest) = name.strip_prefix('w') {
            if let Some((rank, of)) = rest.split_once("of") {
                return Some(ChildAddr::Worker { rank: parse_id(rank)?, of: parse_id(of)? });
            }
        }
        if let Some(id) = name.strip_prefix("idx_") {
            return Some(ChildAddr::Index { id: parse_id(id)? });
        }
        if let Some(rank) = name.strip_prefix("delta_w") {
            return Some(ChildAddr::Delta { rank: parse_id(rank)? });
        }
        let (child, rank) = name.strip_prefix("scratch_")?.rsplit_once("_w")?;
        Some(ChildAddr::Scratch { child, rank: parse_id(rank)? })
    }

    /// True when a relation launched at `num_workers` still owns this child. A
    /// worker child laid out for a different count is the previous layout, which
    /// the boot repartition has already consumed; one above the launched count is
    /// stale either way. Scratch is judged by rank alone — the next compile
    /// recreates what it needs. An index directory is owned at every count; its
    /// own contents are swept by the catalog.
    pub(crate) fn is_owned_by(&self, num_workers: u32) -> bool {
        match *self {
            ChildAddr::Worker { rank, of } => rank < num_workers && of == num_workers,
            ChildAddr::Scratch { rank, .. } | ChildAddr::Delta { rank } => rank < num_workers,
            ChildAddr::Index { .. } => true,
        }
    }
}

/// Every child a relation has across the whole cluster at `num_workers` — one
/// per launched rank. The checkpoint's ephemeral round stamps exactly this set
/// unconditionally, and [`state_child_manifests`] reads it back, so the two must
/// be read together when either changes.
pub(super) fn cluster_children(num_workers: u32) -> impl Iterator<Item = ChildAddr<'static>> {
    (0..num_workers).map(move |rank| ChildAddr::Worker { rank, of: num_workers })
}

/// The manifest of every child under `rel_dir` that carries the relation's
/// checkpointed state at `num_workers`: each launched rank's output store, and
/// every operator-scratch child. Those are what the ephemeral checkpoint round
/// publishes, and the boot resume verdict accepts a view only when all of them
/// stand at one generation.
///
/// The other two grammars carry no generation to compare against: a `Delta`
/// child is in no checkpoint round (it is erased at open), and an `Index` child
/// cannot appear here at all — `CREATE INDEX` is gated on a base table, and only
/// views reach the resume verdict.
pub(crate) fn state_child_manifests(rel_dir: &str, num_workers: u32) -> Vec<String> {
    let scratch = subdir_names(rel_dir);
    let scratch = scratch
        .iter()
        .filter(|n| matches!(ChildAddr::parse(n), Some(ChildAddr::Scratch { .. })))
        .map(|n| manifest::path(&format!("{rel_dir}/{n}")));
    cluster_children(num_workers)
        .map(|c| c.manifest(rel_dir))
        .chain(scratch)
        .collect()
}

/// Whether every state child of `rel_dir` carries a manifest at `generation`.
/// A failed read answers `false`, which is safe here because the verdict's only
/// consequence is a rebuild — `manifest::at_generation` keeps the `Err` for
/// `Table::new`, whose consequence is erasing shards.
pub(crate) fn children_at_generation(rel_dir: &str, num_workers: u32, generation: u64) -> bool {
    state_child_manifests(rel_dir, num_workers)
        .iter()
        .all(|m| manifest::at_generation(m, generation).unwrap_or(false))
}

/// Immediate sub-directory names of `path`. Empty if `path` is missing or
/// unreadable — both mean "nothing to scan" for a boot sweep. Non-directory
/// entries are skipped.
///
/// Materialized rather than streamed: callers unlink entries from the directory
/// they are walking, and `readdir` may skip entries when the directory is
/// modified mid-iteration.
pub(crate) fn subdir_names(path: &str) -> Vec<String> {
    let Ok(entries) = fs::read_dir(path) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

/// A directory-name id component: non-empty and all ASCII digits, so no sign,
/// whitespace or `+` slips through `FromStr`.
fn parse_id<T: std::str::FromStr>(s: &str) -> Option<T> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Hard-link the shards `entries` names from `source` into `target`, then
/// publish `target`'s own manifest. Only sound where the two children are copies
/// of one another: a replicated base table's are, a key-routed table's are not.
///
/// Links the manifest's shard set, not whatever the directory holds, so orphans
/// and half-written `.tmp` files are never carried over. A published shard is
/// never rewritten in place, so the link is byte-equivalent at O(shards).
///
/// The manifest is rewritten under `layout_seq`; the rest of `source_header`
/// carries over.
pub(super) fn link_child(
    source: &str,
    target: &str,
    entries: &[manifest::ManifestEntryRaw],
    source_header: manifest::ManifestHeader,
    layout_seq: u64,
) -> Result<(), StorageError> {
    fs::create_dir_all(target)?;
    for e in entries {
        fs::hard_link(
            format!("{source}/{}", e.filename_str()),
            format!("{target}/{}", e.filename_str()),
        )?;
    }
    manifest::publish_sync(
        target,
        entries,
        manifest::ManifestHeader { layout_seq, ..source_header },
    )
}

/// Create a child directory, answering whether **this call** created it. A child
/// that was already there carries checkpointed state, whose loss
/// [`state_child_manifests`] cannot see: it enumerates the scratch children, so
/// the survivors alone answer the resume verdict.
#[must_use = "only a child this call created may be removed again"]
pub(crate) fn create_child(dir: &str) -> Result<bool, StorageError> {
    match fs::create_dir(dir) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        // A missing ancestor — the compile's throwaway pre-flight root. Retried
        // whole so a non-directory in the path still reports its own ENOTDIR.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(dir)?;
            Ok(true)
        }
        Err(e) => Err(e.into()),
    }
}

/// Unlink a child's manifest, make that durable, then remove the directory.
/// `remove_dir_all` deletes in readdir order, so unlinking the manifest first
/// is what keeps a crash from leaving a manifest whose shards are gone — a
/// state a `SalReplay` open cannot recover from. Best-effort: failing to
/// reclaim costs disk space, not correctness.
pub(crate) fn remove_child(dir: &str) {
    let _ = fs::remove_file(manifest::path(dir));
    let _ = fsync_dir(dir);
    let _ = fs::remove_dir_all(dir);
}

/// `fsync` a directory so its entries are durable.
///
/// Every catalog caller discards the result: a relation's own row is in the
/// fsynced SAL and boot replay re-creates whatever directory it names, so a lost
/// directory entry there costs a re-`mkdir` and never a row.
pub fn fsync_dir(dir: &str) -> Result<(), StorageError> {
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(StorageError::from)
}

/// Remove `dir`'s child directories that a cluster of `num_workers` no longer
/// owns — [`ChildAddr::is_owned_by`] holds the rule. Names in neither child
/// grammar are left alone. An index dir is owned at every count, but its own
/// children are per-worker stores under the same grammar, so the sweep descends
/// into it. Runs after the boot repartition, which has already consumed any
/// previous-layout set it needed.
pub(crate) fn reclaim_retired_children(dir: &str, num_workers: u32) {
    for name in subdir_names(dir) {
        let Some(child) = ChildAddr::parse(&name) else { continue };
        let full = format!("{dir}/{name}");
        if !child.is_owned_by(num_workers) {
            gnitz_debug!("recovery: removing retired child dir {}", full);
            remove_child(&full);
        } else if matches!(child, ChildAddr::Index { .. }) {
            reclaim_retired_children(&full, num_workers);
        }
    }
}

#[cfg(test)]
#[path = "tests/child_dir.rs"]
mod tests;
