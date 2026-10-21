//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`merge_guard`] merges the rows one guard owns → column-first scatter → one
//! output batch, handed to the caller to write, one guard of a fold per call.
//! The merge kernel itself is the shared
//! [`run_merge`](crate::repr::merge::run_merge), which owns the
//! (PK, payload) total order; this module only drives it and materializes
//! survivors.

use super::batch::Batch;
use super::codebook::Codebook;
use super::merge::run_merge;
use super::scatter::UnifiedSet;
use super::shard_file::ShardWriteOpts;
use super::shard_reader::MappedShard;
use super::string_heap::row_long_bytes;
use super::StorageError;
use crate::schema::key::compare_pk_ordering;
use crate::schema::SchemaDescriptor;
use gnitz_wire::RowSource;
use gnitz_wire::{CodedHeap, Heap, PkBuf};
use std::ops::Range;
use std::rc::Rc;

/// Slot owning `key` in a sorted guard list: the last guard `≤ key`, saturating
/// to slot 0 for keys below the first guard.
pub fn guard_slot<T>(guards: &[T], key: &[u8], gk: impl Fn(&T) -> &[u8]) -> usize {
    guards
        .partition_point(|g| compare_pk_ordering(gk(g), key).is_le())
        .saturating_sub(1)
}

/// The rows of one guard a merge kept, to be written as a shard: their long
/// values verbatim, or coded under the codebook the merge was given.
pub struct GuardRun<'a> {
    batch: Batch,
    codebook: Option<&'a Codebook>,
    /// The bytes the run's long values decode to.
    plain_len: usize,
}

impl<'a> GuardRun<'a> {
    /// The run's rows, where its heap holds its values verbatim.
    pub fn plain(&self) -> Option<&Batch> {
        self.codebook.is_none().then_some(&self.batch)
    }

    /// Write the run as a new, unsynced shard at `path`; `Err` if `path`
    /// exists. A coded run's heap is written under the merge's codebook, a
    /// plain one's under `opts`' — each where that pays.
    pub fn write_as_shard(&self, path: &str, opts: ShardWriteOpts<'_>) -> Result<(), StorageError> {
        let codebook = self.codebook.or(opts.codebook);
        self.batch
            .write_shard_over(self, path, ShardWriteOpts { codebook, ..opts })
    }
}

impl RowSource for GuardRun<'_> {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        self.batch.get_pk_bytes(row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.batch.get_null_word(row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        self.batch.get_col_ptr(row, payload_col, col_size)
    }
    #[inline(always)]
    fn heap(&self) -> Heap<'_> {
        match self.codebook {
            None => Heap::Plain(self.batch.blob()),
            Some(book) => Heap::Coded(CodedHeap {
                table: book.table(),
                codes: self.batch.blob(),
                plain_len: self.plain_len,
            }),
        }
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        self.batch.len()
    }
}

/// Merge the rows of `shards` that guard `g` of `guards` owns into one batch,
/// with whether it is a skeleton batch — one PK-only `(PK, Σweight)` row per
/// key. `None` when every one of them cancelled. Every batch is a skeleton one
/// when `dehydrate` is set or any input is a skeleton shard: a skeleton row
/// carries no payload to write back full width. A skeleton merge runs under the
/// PK-only schema, where a key's rows are one element; every other merge is by
/// (PK, payload).
///
/// Each shard is sorted, so the rows a guard owns are one window of it: from
/// `starts`, up to where the next guard's key starts. A window cuts at a key,
/// never inside one, and guard 0 owns every key below its own. `starts` is
/// zeroed for guard 0 and left at guard `g + 1`'s windows, so the guards are
/// merged in order, each as a step of its own.
///
/// The run's long values are coded under `codebook` where one is given: a
/// value of a shard of the same table keeps its codes, every other is coded.
///
/// `guards` is sorted and distinct, and the shards' bodies are verified by the
/// caller. Out of line, so the merge is compiled once, here: inlined into a
/// caller, the payload comparator drops out of the merge loop.
#[inline(never)]
pub fn merge_guard<'a>(
    shards: &[Rc<MappedShard>],
    guards: &[PkBuf],
    g: usize,
    starts: &mut [usize],
    dehydrate: bool,
    schema: &SchemaDescriptor,
    codebook: Option<&'a Codebook>,
) -> Option<(bool, GuardRun<'a>)> {
    debug_assert!(guards.is_sorted_by(|a, b| a < b), "guards must be sorted and distinct");
    let skeleton = dehydrate || shards.iter().any(|s| s.is_skeleton());
    let out_schema = if skeleton { schema.pk_only() } else { *schema };
    let windows: Vec<Range<usize>> = shards
        .iter()
        .zip(starts)
        .map(|(shard, start)| {
            let end = match guards.get(g + 1) {
                Some(next) => shard.seek(next.pk_bytes(), Some(*start)),
                None => shard.row_count(),
            };
            std::mem::replace(start, end)..end
        })
        .collect();
    // The merge compares every key of a window and the scatter copies it.
    let shards: Vec<_> = shards.iter().zip(&windows).map(|(s, w)| s.window(w.clone())).collect();
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(windows.iter().map(Range::len).sum());
    run_merge(&shards, &out_schema, windows.iter().cloned(), |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });
    if survivors.is_empty() {
        return None;
    }
    let set = UnifiedSet::of(&shards, &out_schema, windows);
    // A skeleton run holds no string.
    let codebook = codebook.filter(|_| !skeleton);
    let batch = set.materialize_under(&survivors, set.src_rows(), codebook);
    let mask = out_schema.string_payload_slots();
    let plain_len = match codebook {
        None => batch.blob().len(),
        Some(_) => (0..batch.len()).map(|row| row_long_bytes(&batch, mask, row)).sum(),
    };
    Some((skeleton, GuardRun { batch, codebook, plain_len }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/compact.rs"]
mod bench;
