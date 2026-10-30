//! Shard compaction: the N-way [`run_merge`] of sorted shard files, by (PK,
//! payload) or, for a skeleton merge, by PK alone, one output guard at a time.

use super::codebook::Codebook;
use super::file::ShardWriteOpts;
use super::reader::MappedShard;
use crate::repr::merge::run_merge;
use crate::repr::scatter::UnifiedSet;
use crate::repr::string_heap::row_long_bytes;
use crate::repr::{Batch, StorageError};
use crate::schema::{compare_pk_ordering, SchemaDescriptor};
use gnitz_wire::RowSource;
use gnitz_wire::{CodedHeap, Heap};
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
    /// A coded run's codebook, and the number of bytes its long values decode to.
    coded: Option<(&'a Codebook, usize)>,
    /// Stamp `SHARD_FLAG_SKELETON`: the run is the PK-only `(PK, Σweight)` projection.
    skeleton: bool,
}

impl<'a> GuardRun<'a> {
    /// The run's rows, where its heap holds its values verbatim.
    pub fn plain(&self) -> Option<&Batch> {
        self.coded.is_none().then_some(&self.batch)
    }

    /// Write the run as a new, unsynced shard at `path`; `Err` if `path`
    /// exists. A coded run's heap is written under the merge's codebook, a
    /// plain one's under `opts`' — each where that pays.
    pub fn write_as_shard(&self, path: &str, opts: ShardWriteOpts<'_>) -> Result<(), StorageError> {
        let codebook = self.coded.map(|(book, _)| book).or(opts.codebook);
        self.batch
            .write_shard_over(self, path, ShardWriteOpts { codebook, ..opts }, self.skeleton)
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
        match self.coded {
            None => Heap::Plain(self.batch.blob()),
            Some((book, plain_len)) => Heap::Coded(CodedHeap {
                table: book.table(),
                codes: self.batch.blob(),
                plain_len,
            }),
        }
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        self.batch.len()
    }
}

/// A fold's merge over its input shards, one guard at a time: each guard's
/// window of every shard starts where the previous guard's ended.
pub struct GuardMerge {
    shards: Vec<Rc<MappedShard>>,
    /// Where the next window starts in each shard.
    starts: Vec<usize>,
    /// Whether every run is a skeleton one.
    skeleton: bool,
    /// The schema every run is merged under.
    out_schema: SchemaDescriptor,
}

impl GuardMerge {
    /// A merge over `shards` from their first rows, once each body matches its
    /// checksum. Every run is a skeleton one when `dehydrate` is set or any input
    /// is a skeleton shard, whose rows carry no payload to write back.
    pub fn begin(shards: Vec<Rc<MappedShard>>, dehydrate: bool) -> Result<Self, StorageError> {
        let schema = *shards.first().expect("a guard merge has an input shard").schema();
        debug_assert!(
            shards.iter().all(|s| *s.schema() == schema),
            "a guard merge's shards share one schema"
        );
        for shard in &shards {
            shard.advise_sequential();
            shard.verify_body()?;
        }
        let skeleton = dehydrate || shards.iter().any(|s| s.is_skeleton());
        Ok(GuardMerge {
            starts: vec![0; shards.len()],
            shards,
            skeleton,
            out_schema: if skeleton { schema.pk_only() } else { schema },
        })
    }

    /// The input shards.
    pub fn shards(&self) -> &[Rc<MappedShard>] {
        &self.shards
    }

    /// The input rows merged so far.
    pub fn rows_merged(&self) -> usize {
        self.starts.iter().sum()
    }

    /// Merge the rows below `until` (all, when `None`) that earlier calls left
    /// into one run; `None` when every row cancelled. Long values are coded under
    /// `codebook` where given; those of a shard of the same table keep their codes.
    ///
    /// Out of line: inlined into a caller, the payload comparator drops out of
    /// the merge loop.
    #[inline(never)]
    pub fn next<'a>(&mut self, until: Option<&[u8]>, codebook: Option<&'a Codebook>) -> Option<GuardRun<'a>> {
        let (skeleton, out_schema) = (self.skeleton, &self.out_schema);
        let windows: Vec<Range<usize>> = self
            .shards
            .iter()
            .zip(&mut self.starts)
            .map(|(shard, start)| {
                let end = match until {
                    Some(key) => shard.seek(key, Some(*start)),
                    None => shard.row_count(),
                };
                assert!(end >= *start, "guard keys ascend");
                std::mem::replace(start, end)..end
            })
            .collect();
        // The merge compares every key of a window and the scatter copies it.
        let shards: Vec<_> = self
            .shards
            .iter()
            .zip(&windows)
            .map(|(s, w)| s.window(w.clone()))
            .collect();
        let survivors = run_merge(&shards, out_schema, windows.iter().cloned());
        if survivors.is_empty() {
            return None;
        }
        let set = UnifiedSet::of(&shards, out_schema, windows);
        // A skeleton run holds no string.
        let codebook = codebook.filter(|_| !skeleton);
        let batch = set.materialize(&survivors, codebook);
        let coded = codebook.map(|book| {
            let mask = out_schema.string_payload_slots();
            (
                book,
                (0..batch.len()).map(|row| row_long_bytes(&batch, mask, row)).sum(),
            )
        });
        Some(GuardRun { batch, coded, skeleton })
    }
}

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/compact.rs"]
mod bench;
