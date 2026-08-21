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
pub(crate) enum ChildAddr<'a> {
    /// `w{k}of{n}` — worker `k`'s store of a relation laid out for `n` workers.
    /// The worker count is in the name so the boot repartition can write the new
    /// layout beside the old one: the two sets are disjoint, so the source stays
    /// intact and readable until the target is durable.
    Worker { rank: u32, of: u32 },
    /// `scratch_{child}_w{k}` — worker `k`'s operator-state table for a view.
    Scratch { child: &'a str, rank: u32 },
    /// `idx_{id}` — a secondary index's own directory. Not a store of the owner
    /// relation: never repartitioned, and owned at every worker count (each
    /// worker rebuilds its slice inside it). Named here so one parser covers the
    /// whole namespace and no caller has to pre-filter.
    Index { id: i64 },
}

impl<'a> ChildAddr<'a> {
    /// This worker's store of a relation laid out for `of` workers. The master
    /// is rank 0 pre-fork, so it opens the child worker 0 will inherit.
    pub(crate) fn this_worker(of: u32) -> Self {
        ChildAddr::Worker {
            rank: crate::foundation::worker_ctx::worker_rank(),
            of,
        }
    }

    /// The directory name, relative to the relation's directory.
    pub(crate) fn name(&self) -> String {
        match *self {
            ChildAddr::Worker { rank, of } => format!("w{rank}of{of}"),
            ChildAddr::Scratch { child, rank } => format!("scratch_{child}_w{rank}"),
            ChildAddr::Index { id } => format!("idx_{id}"),
        }
    }

    /// The child's directory under `rel_dir`.
    pub(crate) fn dir(&self, rel_dir: &str) -> String {
        format!("{rel_dir}/{}", self.name())
    }

    /// The child's manifest — what the boot resume verdict peeks and a rebuild
    /// unlinks.
    pub(crate) fn manifest(&self, rel_dir: &str) -> String {
        manifest::path(&self.dir(rel_dir))
    }

    /// The child `name` denotes, or `None` if it is in neither grammar. The
    /// scratch split is right-anchored because child names contain `_`
    /// themselves (`_reduce_in_{vid}_{nid}`).
    pub(crate) fn parse(name: &'a str) -> Option<Self> {
        if let Some(rest) = name.strip_prefix('w') {
            if let Some((rank, of)) = rest.split_once("of") {
                return Some(ChildAddr::Worker {
                    rank: parse_id(rank)?,
                    of: parse_id(of)?,
                });
            }
        }
        if let Some(id) = name.strip_prefix("idx_") {
            return Some(ChildAddr::Index { id: parse_id(id)? });
        }
        let (child, rank) = name.strip_prefix("scratch_")?.rsplit_once("_w")?;
        Some(ChildAddr::Scratch {
            child,
            rank: parse_id(rank)?,
        })
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
            ChildAddr::Scratch { rank, .. } => rank < num_workers,
            ChildAddr::Index { .. } => true,
        }
    }
}

/// Every child a relation has across the whole cluster at `num_workers` — one
/// per launched rank. The boot resume verdict enumerates manifests through this,
/// and the checkpoint's ephemeral round stamps exactly this set, so the two must
/// be read together when either changes.
pub(crate) fn cluster_children(num_workers: u32) -> impl Iterator<Item = ChildAddr<'static>> {
    (0..num_workers).map(move |rank| ChildAddr::Worker { rank, of: num_workers })
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
/// The manifest is rewritten rather than linked, because `layout_seq` differs;
/// `compact_seq` carries over verbatim, so a later compaction cannot name an
/// output after a value already baked into a shard just linked in.
pub(super) fn link_child(
    source: &str,
    target: &str,
    entries: &[manifest::ManifestEntryRaw],
    compact_seq: u64,
    layout_seq: u64,
) -> Result<(), StorageError> {
    super::table::ensure_dir(target)?;
    for e in entries {
        fs::hard_link(
            format!("{source}/{}", e.filename_str()),
            format!("{target}/{}", e.filename_str()),
        )?;
    }
    manifest::publish_sync(
        target,
        entries,
        manifest::ManifestHeader {
            compact_seq,
            checkpoint_gen: 0,
            layout_seq,
        },
    )
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
pub(crate) fn fsync_dir(dir: &str) -> Result<(), StorageError> {
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
mod tests {
    use super::super::cstr;
    use super::*;

    #[test]
    fn parse_inverts_name_for_every_grammar() {
        for addr in [
            ChildAddr::Worker { rank: 0, of: 1 },
            ChildAddr::Worker { rank: 3, of: 64 },
            ChildAddr::Scratch {
                child: "_reduce_in_9_3",
                rank: 2,
            },
            ChildAddr::Index { id: 7 },
        ] {
            let name = addr.name();
            assert_eq!(ChildAddr::parse(&name), Some(addr), "round-trip of {name}");
            assert_eq!(addr.manifest("/d"), format!("/d/{name}/manifest.bin"));
        }
    }

    #[test]
    fn parse_rejects_names_in_no_grammar() {
        for name in [
            "part_0",
            "part_255",
            "rep_3",
            "w3",
            "wof",
            "w3of",
            "wxof4",
            "w3ofx",
            "scratch_x_wq",
            "idx_",
            "idx_x",
            "manifest.bin",
        ] {
            assert_eq!(ChildAddr::parse(name), None, "{name} is not a child dir");
        }
    }

    #[test]
    fn ownership_follows_the_launched_count() {
        // A worker child survives iff its rank is launched AND it was laid out
        // for exactly this count.
        assert!(ChildAddr::Worker { rank: 2, of: 3 }.is_owned_by(3));
        assert!(!ChildAddr::Worker { rank: 3, of: 3 }.is_owned_by(3));
        assert!(!ChildAddr::Worker { rank: 0, of: 2 }.is_owned_by(4));
        assert!(!ChildAddr::Worker { rank: 0, of: 8 }.is_owned_by(4));
        // Scratch is judged by rank alone.
        let scratch = |rank| ChildAddr::Scratch { child: "agg", rank };
        assert!(scratch(1).is_owned_by(2));
        assert!(!scratch(7).is_owned_by(2));
    }

    #[test]
    fn cluster_children_is_the_launched_set() {
        let got: Vec<String> = cluster_children(3).map(|c| c.name()).collect();
        assert_eq!(got, ["w0of3", "w1of3", "w2of3"]);
        assert!(cluster_children(3).all(|c| c.is_owned_by(3)));
    }

    /// `link_child` hard-links the source's shards into a sibling child under
    /// the same basename, which is what keeps their descriptive digests valid
    /// where they land.
    #[test]
    fn linked_child_shard_opens_under_its_linked_name() {
        use crate::storage::repr::shard_reader::MappedShard;
        use crate::test_support::{make_batch, make_schema_u64_i64};

        let tmp = tempfile::tempdir().unwrap();
        let rel_dir = tmp.path().to_str().unwrap().to_string();
        let schema = make_schema_u64_i64();
        let source = ChildAddr::Worker { rank: 0, of: 2 }.dir(&rel_dir);
        super::super::table::ensure_dir(&source).unwrap();

        let name = "shard_42_1.db";
        let rows: Vec<(u64, i64, i64)> = (1..=4).map(|i| (i, 1, i as i64 * 10)).collect();
        make_batch(&schema, &rows)
            .write_as_shard(
                &cstr(format!("{source}/{name}")).unwrap(),
                &schema,
                super::super::shard_file::ShardWriteOpts::default(),
            )
            .unwrap();

        let entries = [manifest::ManifestEntryRaw::new(name, 1, 0, 0)];

        let target = ChildAddr::Worker { rank: 1, of: 2 }.dir(&rel_dir);
        link_child(&source, &target, &entries, 0, 1).unwrap();

        let shard = MappedShard::open(&cstr(format!("{target}/{name}")).unwrap(), &schema, true)
            .expect("a hard-linked shard keeps its basename, so its digest still validates");
        assert_eq!(shard.count, 4);
        assert_eq!(
            manifest::peek_header(&cstr(manifest::path(&target)).unwrap())
                .unwrap()
                .map(|h| h.layout_seq),
            Some(1),
            "the linked child carries the target layout sequence",
        );
    }
}
