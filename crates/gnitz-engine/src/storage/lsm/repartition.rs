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
use super::error::StorageError;
use super::flush_barrier::LazyRing;
use super::manifest::{self, ManifestEntryRaw, ManifestHeader};
use super::read_cursor::{self, ReadCursor};
use super::shard_file::ShardWriteOpts;
use super::table::{RecoverySource, Table};
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

/// Rank 0's shard set, kept from the survey's own read. Rank 0 is a member of
/// every complete set, and the replicated relayout hard-links exactly this into
/// each target, so nothing has to re-open the manifest it came from.
struct Rank0Manifest {
    entries: Vec<ManifestEntryRaw>,
    compact_seq: u64,
}

/// A complete child set on disk: `w{k}of{of}` exists with a manifest for every
/// `k` in `0..of`.
struct CompleteSet {
    of: u32,
    /// The highest layout sequence its members carry.
    seq: u64,
    /// The recovery watermark the set presents: the **minimum** `max_lsn` across
    /// its children, stamped on every rewritten shard so the target set presents
    /// the same floor. The maximum would skip a committed SAL zone a lagging
    /// child never flushed; under-dedupe only costs a replay, since re-applying a
    /// base delta nets zero (`enforce_unique_pk` retracts before re-inserting).
    floor: u64,
    rank0: Rank0Manifest,
}

/// What `rel_dir`'s children say this boot must do with them.
enum Layout {
    /// Nothing to move. Either a complete set already sits at the launched
    /// count, or the only children there are a previous boot's partial set at
    /// it — a child with no manifest holds no reachable rows, and the
    /// un-checkpointed SAL tail covers them.
    Current,
    /// Relay this set onto the launched count.
    Relay(CompleteSet),
    /// Durable children laid out for these counts, with no complete set at any
    /// of them. Their rows cannot be placed, so the boot refuses rather than
    /// come up without them.
    Unplaceable(Vec<u32>),
}

/// Classify `rel_dir`'s children against the `launched` count.
///
/// `Err` names a directory in no child grammar: a relation whose layout this
/// build cannot read must refuse to boot rather than come up empty with its rows
/// still on disk and unreferenced.
fn classify(rel_dir: &str, launched: u32) -> Result<Layout, String> {
    let names = super::child_dir::subdir_names(rel_dir);
    let mut workers: Vec<(u32, u32)> = Vec::new();
    for name in &names {
        match ChildAddr::parse(name) {
            // Scratch is view operator state and an index dir is its own
            // relation; neither is repartitioned.
            Some(ChildAddr::Worker { rank, of }) => workers.push((rank, of)),
            Some(_) => {}
            None => {
                return Err(format!(
                    "{rel_dir}/{name} is in no child-directory grammar this build knows; \
                     it may hold rows written by an incompatible layout. Refusing to boot — \
                     delete the relation directory deliberately if the data is expendable."
                ))
            }
        }
    }
    // Every restart after a successful boot lands here: the relayout removed the
    // source set and the child-dir sweep reclaimed any residue, so no manifest
    // needs reading to know there is nothing to move.
    if workers.iter().all(|&(_, of)| of == launched) {
        return Ok(Layout::Current);
    }

    // `of` -> rank -> (layout sequence, max lsn); a rank absent from the inner
    // map has no manifest, so its set is incomplete.
    let mut sets: HashMap<u32, HashMap<u32, (u64, u64)>> = HashMap::new();
    let mut rank0: HashMap<u32, Rank0Manifest> = HashMap::new();
    for &(rank, of) in &workers {
        let cpath = super::cstr(ChildAddr::Worker { rank, of }.manifest(rel_dir)).map_err(|e| e.to_string())?;
        let Some((entries, header)) = manifest::read_file(&cpath)
            .map_err(|e| format!("repartition {rel_dir}: unreadable manifest in w{rank}of{of}: error {e}"))?
        else {
            continue;
        };
        let max_lsn = entries.iter().map(|e| e.max_lsn).max().unwrap_or(0);
        if rank == 0 {
            rank0.insert(
                of,
                Rank0Manifest {
                    entries,
                    compact_seq: header.compact_seq,
                },
            );
        }
        sets.entry(of).or_default().insert(rank, (header.layout_seq, max_lsn));
    }

    let live = sets
        .iter()
        .filter(|(&of, ranks)| of > 0 && (0..of).all(|k| ranks.contains_key(&k)))
        .map(|(&of, ranks)| {
            (
                of,
                ranks.values().map(|&(seq, _)| seq).max().unwrap_or(0),
                ranks.values().map(|&(_, lsn)| lsn).min().unwrap_or(0),
            )
        })
        // A tie on the sequence is impossible for two *complete* sets — a rewrite
        // stamps one above the source's highest — so `max` is total here.
        .max_by_key(|&(_, seq, _)| seq);

    Ok(match live {
        Some((of, _, _)) if of == launched => Layout::Current,
        Some((of, seq, floor)) => Layout::Relay(CompleteSet {
            of,
            seq,
            floor,
            rank0: rank0.remove(&of).expect("a complete set has a rank-0 manifest"),
        }),
        None => {
            let mut foreign: Vec<u32> = sets.into_keys().filter(|&of| of != launched).collect();
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
) -> Result<(), String> {
    let source = match classify(rel_dir, launched)? {
        Layout::Current => return Ok(()),
        Layout::Unplaceable(counts) => {
            return Err(format!(
                "{rel_dir} holds checkpointed children laid out for {counts:?} worker(s) but no complete \
                 set for any count; launching {launched}, its rows cannot be placed. Refusing to boot."
            ))
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
    let result = if schema.placement().is_replicated() {
        link_targets(rel_dir, &source, launched, seq)
    } else {
        rewrite_targets(rel_dir, schema, table_id, &source, launched, seq)
    };
    result.map_err(|e| format!("repartition {rel_dir} to {launched} worker(s): error {e}"))?;

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
/// is the source's rank 0 hard-linked `launched` times. No other source child is
/// ever consulted.
fn link_targets(rel_dir: &str, source: &CompleteSet, launched: u32, seq: u64) -> Result<(), StorageError> {
    let source_dir = ChildAddr::Worker { rank: 0, of: source.of }.dir(rel_dir);
    for target in super::child_dir::cluster_children(launched) {
        link_child(
            &source_dir,
            &target.dir(rel_dir),
            &source.rank0.entries,
            source.rank0.compact_seq,
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
    fn new(dir: String, schema: &SchemaDescriptor) -> Self {
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
    fn flush_shard(&mut self, schema: &SchemaDescriptor, table_id: u32, floor: u64) -> Result<(), StorageError> {
        if self.buffer.count == 0 {
            return Ok(());
        }
        // The guard key is this shard's first PK: the rewrite emits globally
        // ascending rows, so each shard owns a distinct key range.
        let guard_key = crate::schema::key::pack_pk_be(self.buffer.get_pk_bytes(0));
        let name = super::naming::compact_shard_name(table_id, self.next_seq, 0, guard_key);
        self.next_seq += 1;
        self.buffer.write_as_shard(
            &super::cstr(format!("{}/{name}", self.dir))?,
            schema,
            ShardWriteOpts::COMPACTION,
        )?;
        self.entries.push(ManifestEntryRaw::new(&name, floor, 0, guard_key));
        // Keeps the buffer's capacity, so each shard is filled into the
        // allocation the previous one grew rather than re-growing from zero.
        self.buffer.clear();
        Ok(())
    }

    /// fdatasync every shard, then publish the manifest.
    fn publish(&self, ring: &mut LazyRing, seq: u64) -> Result<(), StorageError> {
        let paths: Vec<CString> = self
            .entries
            .iter()
            .map(|e| super::cstr(format!("{}/{}", self.dir, e.filename_str())))
            .collect::<Result<_, _>>()?;
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
    source: &CompleteSet,
    launched: u32,
    seq: u64,
) -> Result<(), StorageError> {
    let floor = source.floor;
    let sources: Vec<Table> = super::child_dir::cluster_children(source.of)
        .map(|c| Table::new(&c.dir(rel_dir), *schema, table_id, RecoverySource::SalReplay))
        .collect::<Result<_, _>>()?;
    let mut cursor: ReadCursor = read_cursor::from_runs(sources.iter().flat_map(Table::runs), *schema);

    let mut targets: Vec<TargetChild> = super::child_dir::cluster_children(launched)
        .map(|c| {
            let dir = c.dir(rel_dir);
            super::table::ensure_dir(&dir)?;
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
            let slice = chunk.ascending_subset(idx, schema);
            targets[w].buffer.append_batch(&slice, 0, slice.count);
            if targets[w].buffer.total_bytes() >= REWRITE_SHARD_BYTES {
                targets[w].flush_shard(schema, table_id, floor)?;
            }
        }
    }
    let mut ring = LazyRing::default();
    for t in targets.iter_mut() {
        t.flush_shard(schema, table_id, floor)?;
        t.publish(&mut ring, seq)?;
    }
    Ok(())
}
