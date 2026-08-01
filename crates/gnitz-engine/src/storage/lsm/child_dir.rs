//! The child directories under a relation's directory: how they are named, and
//! how a boot gives a launched rank the copy it is missing. Sibling of
//! `naming`, which owns the file grammar *inside* one of these. Every builder
//! sits next to its parser, so the paths that create children and the sweeps
//! that reclaim them cannot drift.

use std::fs;

use super::manifest;
use super::partitioned_table::Routing;
use super::{cstr, error::StorageError};

/// One child directory of a relation. The three grammars are disjoint, so a
/// name on disk identifies its kind without knowing how the store was built —
/// which is what lets a boot sweep reclaim children of a shape the relation no
/// longer has, and what makes a shape flip find no manifest (and rebuild)
/// rather than resume the wrong layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChildAddr<'a> {
    /// `part_{p}` — one bucket of a hashed store's 256-way tiling.
    Partition(u32),
    /// `rep_{k}` — worker `k`'s single child of an unhashed store: a
    /// replicated base table's full copy, or a replicated-derived view's local
    /// slice. Addressed by rank rather than partition index, so it stays put
    /// when the worker count changes.
    Local(u32),
    /// `scratch_{child}_w{k}` — worker `k`'s operator-state table for a view.
    Scratch { child: &'a str, rank: u32 },
}

impl<'a> ChildAddr<'a> {
    /// The directory name, relative to the relation's directory.
    pub fn name(&self) -> String {
        match *self {
            ChildAddr::Partition(p) => format!("part_{p}"),
            ChildAddr::Local(k) => format!("rep_{k}"),
            ChildAddr::Scratch { child, rank } => format!("scratch_{child}_w{rank}"),
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

    /// The child `name` denotes, or `None` if it is in none of the grammars.
    /// The scratch split is right-anchored because child names contain `_`
    /// themselves (`_reduce_in_{vid}_{nid}`).
    pub fn parse(name: &'a str) -> Option<Self> {
        if let Some(p) = numeric_suffix(name, "part_") {
            return Some(ChildAddr::Partition(p));
        }
        if let Some(k) = numeric_suffix(name, "rep_") {
            return Some(ChildAddr::Local(k));
        }
        let (child, rank) = name.strip_prefix("scratch_")?.rsplit_once("_w")?;
        Some(ChildAddr::Scratch {
            child,
            rank: parse_u32(rank)?,
        })
    }

    /// True when a store routed by `routing`, launched at `num_workers`, still
    /// owns this child. A hashed store's per-worker ranges tile 0..256 exactly
    /// at every count, so a partition is never stale; a rank-stamped child
    /// above the launched count is. A child in the grammar the store does not
    /// use is residue left by a shape flip. Scratch is judged by rank either
    /// way — the next compile recreates what it needs.
    pub fn is_owned_by(&self, routing: Routing, num_workers: u32) -> bool {
        let unhashed = matches!(routing, Routing::Unhashed { .. });
        match *self {
            ChildAddr::Partition(_) => !unhashed,
            ChildAddr::Local(k) => unhashed && k < num_workers,
            ChildAddr::Scratch { rank, .. } => rank < num_workers,
        }
    }
}

fn parse_u32(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn numeric_suffix(name: &str, prefix: &str) -> Option<u32> {
    parse_u32(name.strip_prefix(prefix)?)
}

/// Give every launched rank a copy of an unhashed **base table**, by
/// hard-linking rank 0's shards — worker 0's child, which is current at every
/// worker count. A rank whose child already has a manifest is left alone, so a
/// shrink or an unchanged restart does no work at all.
///
/// Only sound where the children are copies of one another. A replicated-derived
/// view's child is the slice *that worker* produced, not a copy of anyone else's,
/// so a missing one must be re-derived instead.
///
/// Links the shard set the manifest names, not whatever the directory holds, so
/// orphans and half-written `.tmp` files are never carried over — a `.tmp` link
/// would be truncated out from under rank 0 by a same-name reopen. A published
/// shard is never rewritten in place, so a link is byte-equivalent and costs
/// O(shards) rather than O(bytes).
///
/// The manifest is linked last, under the staging name, and renamed only after
/// the directory is durable: a crash leaves a manifest-less directory that the
/// next boot redoes, never a manifest whose shards are missing. Runs on the
/// master pre-fork, so no worker can be writing either directory.
pub fn seed_missing_locals(rel_dir: &str, num_workers: u32) -> Result<(), StorageError> {
    let source = ChildAddr::Local(0).dir(rel_dir);
    let Some((entries, _)) = manifest::read_file(&cstr(manifest::path(&source))?)? else {
        return Ok(()); // never checkpointed — nothing to copy
    };
    for k in 1..num_workers {
        let target = ChildAddr::Local(k).dir(rel_dir);
        if fs::metadata(manifest::path(&target)).is_ok() {
            continue;
        }
        remove_child(&target); // a torn earlier attempt
        super::table::ensure_dir(&target)?;
        for e in &entries {
            fs::hard_link(
                format!("{source}/{}", e.filename_str()),
                format!("{target}/{}", e.filename_str()),
            )
            .map_err(|_| StorageError::Io)?;
        }
        let staged = manifest::tmp_path(&target);
        fs::hard_link(manifest::path(&source), &staged).map_err(|_| StorageError::Io)?;
        fsync_dir(&target)?;
        fs::rename(&staged, manifest::path(&target)).map_err(|_| StorageError::Io)?;
        fsync_dir(&target)?;
    }
    Ok(())
}

/// Unlink a child's manifest, make that durable, then remove the directory.
/// `remove_dir_all` deletes in readdir order, so unlinking the manifest first
/// is what keeps a crash from leaving a manifest whose shards are gone — a
/// state a `SalReplay` open cannot recover from. Best-effort: failing to
/// reclaim costs disk space, not correctness.
pub fn remove_child(dir: &str) {
    let _ = fs::remove_file(manifest::path(dir));
    let _ = fsync_dir(dir);
    let _ = fs::remove_dir_all(dir);
}

/// `fsync` a directory so its entries are durable.
fn fsync_dir(dir: &str) -> Result<(), StorageError> {
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|_| StorageError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_inverts_name_for_every_grammar() {
        for addr in [
            ChildAddr::Partition(0),
            ChildAddr::Partition(255),
            ChildAddr::Local(3),
            ChildAddr::Scratch {
                child: "_reduce_in_9_3",
                rank: 2,
            },
        ] {
            let name = addr.name();
            assert_eq!(ChildAddr::parse(&name), Some(addr), "round-trip of {name}");
            assert_eq!(addr.manifest("/d"), format!("/d/{name}/manifest.bin"));
        }
    }

    #[test]
    fn parse_rejects_names_in_no_grammar() {
        for name in ["part_", "rep_x", "scratch_x_wq", "idx_7", "w3", "manifest.bin"] {
            assert_eq!(ChildAddr::parse(name), None, "{name} is not a child dir");
        }
    }

    #[test]
    fn ownership_follows_the_live_grammar() {
        let local = Routing::Unhashed { rank: 0 };
        let hashed = Routing::Hashed { start: 0, end: 256 };
        // A rank-stamped child survives iff its rank is still launched.
        assert!(ChildAddr::Local(2).is_owned_by(local, 3));
        assert!(!ChildAddr::Local(3).is_owned_by(local, 3));
        // Partitions tile the whole space at every count, so none is ever stale.
        assert!(ChildAddr::Partition(255).is_owned_by(hashed, 1));
        // Residue in the other grammar, both directions.
        assert!(!ChildAddr::Local(0).is_owned_by(hashed, 4));
        assert!(!ChildAddr::Partition(0).is_owned_by(local, 4));
        // Scratch is judged by rank whatever the store shape is.
        let scratch = |rank| ChildAddr::Scratch { child: "agg", rank };
        assert!(scratch(1).is_owned_by(hashed, 2) && scratch(1).is_owned_by(local, 2));
        assert!(!scratch(7).is_owned_by(hashed, 2) && !scratch(7).is_owned_by(local, 2));
    }
}
