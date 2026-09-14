//! Boot-time relayout of a base relation's children onto the launched worker
//! count.
//!
//! A child's *contents* depend on the worker count — `w{k}of{n}` holds exactly
//! the rows `worker_for_pk(pk, n) == k` — so a restart at a different count has
//! to move data. This module is the whole of that move: decide which set on disk
//! is live, write the launched-count set beside it, and only then remove the
//! source. The two sets are disjoint names, so a crash at any point leaves the
//! older set intact and the next boot redoes the work.
//!
//! Runs on the master, pre-fork, before the relation's own store is
//! constructed — so no handle is open on either set and no worker exists to race
//! it.

use std::collections::HashMap;
use std::ffi::{CStr, CString};

use super::batch::Batch;
use super::child_dir::{link_child, remove_child, ChildAddr};
use super::error::{StorageError, StoreError};
use super::flush_barrier::LazyRing;
use super::manifest::{self, ManifestEntryRaw, ManifestHeader};
use super::read_cursor::{self, ReadCursor};
use super::shard_file::ShardWriteOpts;
use super::shard_index::{ShardIndex, TERMINAL_LEVEL_IDX};
use super::table::{RecoverySource, StoreBudgets, Table};
use crate::schema::SchemaDescriptor;

/// Bytes of rewritten rows a target child buffers before it writes a shard.
/// Peak RAM for a rewrite is `launched × REWRITE_SHARD_BYTES` — 16 MiB at W=4,
/// 256 MiB at W=64 — plus the source cursor's loser tree, which is `O(shards)`
/// and not `O(rows)`. Relations are rewritten one at a time, so that peak does
/// not accumulate across them.
const REWRITE_SHARD_BYTES: usize = 4 * 1024 * 1024;

/// Rows the source cursor yields per drain. Only the scatter's working set —
/// each drained chunk is split across the targets and appended, then dropped.
const REWRITE_DRAIN_ROWS: usize = 64 * 1024;

/// One child's shard set, kept from the survey's own read, so the replicated
/// relayout can hard-link it into each target without re-opening the manifest it
/// came from.
struct SourceManifest {
    /// Which rank it came from — the lowest one in the set that published a
    /// manifest.
    rank: u32,
    entries: Vec<ManifestEntryRaw>,
    compact_seq: u64,
}

/// What one `w*of{of}` child set's surviving children say about it.
struct SetSurvey {
    /// `(layout sequence, max lsn)` per rank that published a manifest. A rank
    /// absent here has none, so the set is incomplete.
    ranks: HashMap<u32, (u64, u64)>,
    /// The manifest a relay would link from, if this set is the source.
    source: SourceManifest,
}

impl SetSurvey {
    /// Every rank in `0..of` published a manifest.
    fn is_complete(&self, of: u32) -> bool {
        (0..of).all(|k| self.ranks.contains_key(&k))
    }

    /// The highest layout sequence its members carry.
    fn max_seq(&self) -> u64 {
        self.ranks.values().map(|&(seq, _)| seq).max().unwrap_or(0)
    }

    /// The recovery watermark the set presents: the **minimum** `max_lsn` across
    /// its children, stamped on every rewritten shard so the target set presents
    /// the same floor. The maximum would skip a committed SAL zone a lagging
    /// child never flushed; under-dedupe only costs a replay, since re-applying a
    /// base delta nets zero (`enforce_unique_pk` retracts before re-inserting).
    fn floor(&self) -> u64 {
        self.ranks.values().map(|&(_, lsn)| lsn).min().unwrap_or(0)
    }
}

/// The child set a relayout reads from.
struct SourceSet {
    of: u32,
    seq: u64,
    floor: u64,
    source: SourceManifest,
}

/// What `rel_dir`'s children say this boot must do with them.
enum Layout {
    /// Nothing to move. Either a complete set already sits at the launched
    /// count, or the only children there are a previous boot's partial set at
    /// it — a child with no manifest holds no reachable rows, and the
    /// un-checkpointed SAL tail covers them.
    Current,
    /// Relay this set onto the launched count.
    Relay(SourceSet),
    /// Durable children laid out for these counts, none of them relayable.
    /// Their rows cannot be placed, so the boot refuses rather than come up
    /// without them. Only a key-routed relation reaches this: a replicated set
    /// with any manifest at all is a usable copy.
    Unplaceable(Vec<u32>),
}

/// Classify `rel_dir`'s children against the `launched` count.
///
/// `Err` names a directory in no child grammar: a relation whose layout this
/// build cannot read must refuse to boot rather than come up empty with its rows
/// still on disk and unreferenced.
fn classify(rel_dir: &str, launched: u32, replicated: bool) -> Result<Layout, StoreError> {
    let names = super::child_dir::subdir_names(rel_dir);
    let mut workers: Vec<(u32, u32)> = Vec::new();
    for name in &names {
        match ChildAddr::parse(name) {
            // Scratch is view operator state and an index dir is its own
            // relation; neither is repartitioned.
            Some(ChildAddr::Worker { rank, of }) => workers.push((rank, of)),
            Some(_) => {}
            None => {
                return Err(StoreError::rejected(format!(
                    "{rel_dir}/{name} is in no child-directory grammar this build knows; \
                     it may hold rows written by an incompatible layout. Refusing to boot — \
                     delete the relation directory deliberately if the data is expendable."
                )))
            }
        }
    }
    // Every restart after a successful boot lands here: the relayout removed the
    // source set and the child-dir sweep reclaimed any residue, so no manifest
    // needs reading to know there is nothing to move.
    if workers.iter().all(|&(_, of)| of == launched) {
        return Ok(Layout::Current);
    }

    // By `(rank, of)`, so each set's lowest surviving rank is the first read and
    // the `or_insert_with` below keeps it. `subdir_names` is readdir order, which
    // would pick a different source child across boots.
    workers.sort_unstable();
    let mut sets: HashMap<u32, SetSurvey> = HashMap::new();
    for &(rank, of) in &workers {
        let context = || format!("repartition {rel_dir}: manifest of w{rank}of{of}");
        let cpath = super::cstr(ChildAddr::Worker { rank, of }.manifest(rel_dir))
            .map_err(|e| StoreError::storage(context(), e))?;
        let Some((entries, header)) = manifest::read_file(&cpath).map_err(|e| StoreError::storage(context(), e))?
        else {
            continue;
        };
        let max_lsn = entries.iter().map(|e| e.max_lsn).max().unwrap_or(0);
        let survey = sets.entry(of).or_insert_with(|| SetSurvey {
            ranks: HashMap::new(),
            source: SourceManifest {
                rank,
                entries,
                compact_seq: header.compact_seq,
            },
        });
        survey.ranks.insert(rank, (header.layout_seq, max_lsn));
    }

    // Kept before the pool consumes `sets`, for the refusal message below.
    let counts: Vec<u32> = sets.keys().copied().collect();
    let live = sets
        .into_iter()
        .filter(|&(of, ref s)| match of {
            0 => false,
            // Nothing to move: the set must be whole, or a relayout that crashed
            // part-way through writing it would be accepted as the live one.
            of if of == launched => s.is_complete(of),
            // A relay source. A replicated set needs one survivor, not all:
            // `link_targets` reads one child and copies it, and a survivor a
            // checkpoint behind is still whole — a published manifest plus the
            // SAL tail is, and linking from the laggard only replays more.
            _ => s.is_complete(of) || replicated,
        })
        // A tie on the sequence cannot arise: a relay-eligible partial set
        // becomes the next relayout's source, which mints `seq + 1` above it,
        // and a partial set at the launched count is cleared by `remove_set`
        // before its sequence is ever reused.
        .max_by_key(|(_, s)| s.max_seq());

    Ok(match live {
        Some((of, _)) if of == launched => Layout::Current,
        Some((of, s)) => Layout::Relay(SourceSet {
            of,
            seq: s.max_seq(),
            floor: s.floor(),
            source: s.source,
        }),
        None => {
            let mut foreign: Vec<u32> = counts.into_iter().filter(|&of| of != launched).collect();
            foreign.sort_unstable();
            if foreign.is_empty() {
                Layout::Current
            } else {
                Layout::Unplaceable(foreign)
            }
        }
    })
}

/// Bring `rel_dir`'s children onto `launched` workers — see [`Layout`] for the
/// three states its children can be in.
pub(crate) fn repartition_relation(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    table_id: u32,
    launched: u32,
) -> Result<(), StoreError> {
    let replicated = schema.placement().is_replicated();
    let source = match classify(rel_dir, launched, replicated)? {
        Layout::Current => return Ok(()),
        Layout::Unplaceable(counts) => {
            return Err(StoreError::rejected(format!(
                "{rel_dir} holds checkpointed children laid out for {counts:?} worker(s) but no complete \
                 set at any of them; launching {launched}, its rows cannot be placed. Refusing to boot."
            )))
        }
        Layout::Relay(source) => source,
    };

    // A retried relayout must not ingest into a surviving partial target:
    // attempt 2's shards would merge with attempt 1's and the weights would sum,
    // putting every base row at weight 2.
    remove_set(rel_dir, launched);

    let seq = source.seq + 1;
    gnitz_info!(
        "repartition: {} from {} to {} worker(s), layout seq {}, lsn floor {}",
        rel_dir,
        source.of,
        launched,
        seq,
        source.floor
    );

    // Replicated children are copies of one another, key-routed ones are hash
    // slices, so only the latter has to move rows.
    let result = if replicated {
        link_targets(rel_dir, &source, launched, seq)
    } else {
        rewrite_targets(rel_dir, schema, table_id, &source, launched, seq)
    };
    let context = || format!("repartition {rel_dir} to {launched} worker(s)");
    result.map_err(|e| StoreError::storage(context(), e))?;

    // Makes the target children's directory entries durable — nothing else
    // does, and the source unlinks below are. A swallowed failure here is the
    // whole relation, so it propagates.
    super::child_dir::fsync_dir(rel_dir).map_err(|e| StoreError::storage(context(), e))?;

    // Only now that every target child carries a durable manifest.
    remove_set(rel_dir, source.of);
    Ok(())
}

/// Remove every child of the `of`-worker set. Best-effort: a leftover child is
/// superseded by its layout sequence and reclaimed by the boot child-dir sweep.
fn remove_set(rel_dir: &str, of: u32) {
    for child in super::child_dir::cluster_children(of) {
        remove_child(&child.dir(rel_dir));
    }
}

/// Replicated relations: every child is a copy of every other, so the target set
/// is one surviving source child — the lowest-ranked one that published a
/// manifest — hard-linked `launched` times. No other source child is ever
/// consulted.
fn link_targets(rel_dir: &str, source: &SourceSet, launched: u32, seq: u64) -> Result<(), StorageError> {
    let source_dir = ChildAddr::Worker { rank: source.source.rank, of: source.of }.dir(rel_dir);
    for target in super::child_dir::cluster_children(launched) {
        link_child(
            &source_dir,
            &target.dir(rel_dir),
            &source.source.entries,
            source.source.compact_seq,
            seq,
        )?;
    }
    Ok(())
}

/// One target child under construction: buffered rows plus the shards already
/// written for it.
struct TargetChild {
    dir: String,
    buffer: Batch,
    entries: Vec<ManifestEntryRaw>,
    next_seq: u64,
}

impl TargetChild {
    pub(crate) fn new(dir: String, schema: &SchemaDescriptor) -> Self {
        TargetChild {
            dir,
            buffer: Batch::empty_with_schema(schema),
            entries: Vec::new(),
            next_seq: 0,
        }
    }

    /// Write the buffer as one shard and record its manifest entry. The output
    /// goes through the **compaction** grammar, whose uniqueness is anchored on a
    /// per-shard sequence rather than on the LSN counter: the spill grammar names
    /// a shard `shard_{tid}_{current_lsn}.db` and `Table::new` re-derives that
    /// counter as `max_lsn() + 1`, so a shard named from a stamped-down floor
    /// would be clobbered by the next spill's finalizing rename. With no filename
    /// depending on the counter, the floor is a property of the manifest alone.
    ///
    /// Registered at the **terminal** guarded level: the output already satisfies
    /// what a guarded level requires — globally ascending, non-overlapping,
    /// consolidated, one guard key per shard. At L0 it would instead be an
    /// unbounded run the next spill folds in one go, permanently raising that
    /// store's guard target to the size of the whole child.
    fn flush_shard(&mut self, table_id: u32, floor: u64) -> Result<(), StorageError> {
        if self.buffer.count == 0 {
            return Ok(());
        }
        // The guard key is this shard's first PK: the rewrite emits globally
        // ascending rows, so each shard owns a distinct key range.
        let guard_key = crate::schema::key::PkBuf::from_bytes(self.buffer.get_pk_bytes(0));
        let level = ShardIndex::level_num(TERMINAL_LEVEL_IDX);
        // Each shard draws its own `next_seq`, so the part index is always 0.
        let name = super::naming::compact_shard_name(table_id, self.next_seq, level, 0);
        self.next_seq += 1;
        self.buffer.write_as_shard(
            &super::cstr(format!("{}/{name}", self.dir))?,
            ShardWriteOpts::COMPACTION,
        )?;
        self.entries
            .push(ManifestEntryRaw::new(&name, floor, level as u64, guard_key));
        // Keeps the buffer's capacity, so each shard is filled into the
        // allocation the previous one grew rather than re-growing from zero.
        self.buffer.clear();
        Ok(())
    }

    /// fdatasync every shard, then publish the manifest.
    fn publish(&self, ring: &mut LazyRing, seq: u64) -> Result<(), StorageError> {
        let paths = super::to_cstrings(
            self.entries
                .iter()
                .map(|e| format!("{}/{}", self.dir, e.filename_str())),
        )?;
        let refs: Vec<&CStr> = paths.iter().map(CString::as_c_str).collect();
        super::flush_barrier::sync_by_path(ring, &refs)?;
        manifest::publish_sync(
            &self.dir,
            &self.entries,
            ManifestHeader {
                compact_seq: self.next_seq,
                checkpoint_gen: 0,
                layout_seq: seq,
            },
        )
    }
}

/// Key-routed relations: merge the source set into one ascending cursor and
/// scatter every row into its new owner's child.
fn rewrite_targets(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    table_id: u32,
    source: &SourceSet,
    launched: u32,
    seq: u64,
) -> Result<(), StorageError> {
    let floor = source.floor;
    let sources: Vec<Table> = super::child_dir::cluster_children(source.of)
        .map(|c| {
            Table::new(
                &c.dir(rel_dir),
                *schema,
                table_id,
                RecoverySource::SalReplay,
                StoreBudgets::default(),
            )
        })
        .collect::<Result<_, _>>()?;
    // Unsized: one boot-time walk over every source child, where the source
    // vectors' realloc ladder is noise against the rewrite it feeds.
    let mut cursor: ReadCursor = read_cursor::from_runs(sources.iter().flat_map(Table::runs), *schema, 0);

    let mut targets: Vec<TargetChild> = super::child_dir::cluster_children(launched)
        .map(|c| {
            let dir = c.dir(rel_dir);
            // The one directory opener, the same one `Table::new` takes: it
            // applies the platform's no-CoW hint, and the shards below are
            // written into this directory by path. Where the flag is inherited at
            // file creation (btrfs) `ensure_dir` would leave every one of them
            // copy-on-write, since nothing flags the directory until the next boot.
            super::table::open_table_dirfd(&dir)?;
            Ok(TargetChild::new(dir, schema))
        })
        .collect::<Result<_, StorageError>>()?;

    let mut rows: Vec<Vec<u32>> = vec![Vec::new(); launched as usize];
    while let Some(chunk) = cursor.drain_chunk(REWRITE_DRAIN_ROWS) {
        rows.iter_mut().for_each(Vec::clear);
        super::super::scatter::route_rows_by_pk(&chunk.as_mem_batch(), schema, &mut rows);
        for (w, idx) in rows.iter().enumerate() {
            if idx.is_empty() {
                continue;
            }
            let slice = chunk.ascending_subset(idx);
            targets[w].buffer.append_batch(&slice, 0, slice.count);
            if targets[w].buffer.total_bytes() >= REWRITE_SHARD_BYTES {
                targets[w].flush_shard(table_id, floor)?;
            }
        }
    }
    let mut ring = LazyRing::default();
    for t in targets.iter_mut() {
        t.flush_shard(table_id, floor)?;
        t.publish(&mut ring, seq)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/repartition.rs"]
mod tests;
