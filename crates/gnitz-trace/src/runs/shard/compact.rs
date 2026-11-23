//! Shard compaction: the N-way [`run_merge`] of sorted shard files, by (PK,
//! payload) or, for a skeleton merge, by PK alone, one output guard at a time;
//! windows that ascend without overlap are taken in key order unmerged, each
//! decoded straight into the run.

use super::codebook::Codebook;
use super::file::ShardWriteOpts;
use super::reader::MappedShard;
use crate::runs::StorageError;
use gnitz_wire::RowSource;
use gnitz_wire::{CodedHeap, Heap, PkBuf};
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::UnifiedSet;
use gnitz_zset::repr::{BlobCache, HeapCoder, StringDest, long_bytes, prorated_blob_cap};
use gnitz_zset::repr::{disjoint_order, run_merge};
use gnitz_zset::schema::{SchemaDescriptor, compare_pk_ordering};
use smallvec::SmallVec;
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
    /// The run of `batch`, whose rows are `schema`'s, its long values coded under
    /// `codebook` where given.
    pub(super) fn new(batch: Batch, schema: &SchemaDescriptor, codebook: Option<&'a Codebook>, skeleton: bool) -> Self {
        let coded = codebook.map(|book| {
            let mask = schema.string_payload_slots();
            (book, long_bytes(&batch.as_mem_batch(), mask, 0..batch.len()))
        });
        GuardRun { batch, coded, skeleton }
    }

    /// The run's rows, where its heap holds its values verbatim.
    pub fn plain(&self) -> Option<&Batch> {
        self.coded.is_none().then_some(&self.batch)
    }

    /// Write the run as a new, unsynced shard at `path`; `Err` if `path`
    /// exists. A coded run's heap is written under the merge's codebook, a
    /// plain one's under `opts`' — each where that pays.
    pub fn write_as_shard(&self, path: &str, opts: ShardWriteOpts<'_>) -> Result<(), StorageError> {
        let codebook = self.coded.map(|(book, _)| book).or(opts.codebook);
        super::write_shard_over(
            &self.batch,
            self,
            path,
            ShardWriteOpts { codebook, ..opts },
            self.skeleton,
        )
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

/// A fold's merge over its input shards into its destinations, one at a time:
/// the first receives the rows below the first fence, each fence starts the
/// next, and the last runs to the end — the rows [`guard_slot`] routes to each
/// destination among guards keyed at the fences.
pub struct GuardMerge {
    shards: Vec<Rc<MappedShard>>,
    /// Where the next window starts in each shard.
    starts: Vec<usize>,
    /// The keys each destination after the first starts at, ascending.
    fences: Vec<PkBuf>,
    /// The destination to merge next.
    next: usize,
    /// Whether every run is a skeleton one.
    skeleton: bool,
    /// Whether each shard holds each element of the merge's order once: false
    /// when a hydrated shard is read under a skeleton merge's PK-only order,
    /// where its rows repeat PKs that fold.
    distinct: bool,
    /// The schema every run is merged under.
    out_schema: SchemaDescriptor,
}

impl GuardMerge {
    /// A merge of `shards` from their first rows into the destinations
    /// `fences` cuts them into, once each body matches its checksum. Every run is a skeleton one when
    /// `dehydrate` is set or any input is a skeleton shard, whose rows carry no
    /// payload to write back.
    pub fn begin(shards: Vec<Rc<MappedShard>>, fences: Vec<PkBuf>, dehydrate: bool) -> Result<Self, StorageError> {
        let schema = *shards.first().expect("a guard merge has an input shard").schema();
        assert!(fences.is_sorted_by(|a, b| a < b), "a guard merge's fences ascend");
        debug_assert!(
            shards.iter().all(|s| *s.schema() == schema),
            "a guard merge's shards share one schema"
        );
        for shard in &shards {
            shard.advise_sequential();
            shard.verify_body()?;
        }
        let skeleton = dehydrate || shards.iter().any(|s| s.is_skeleton());
        let distinct = !skeleton || shards.iter().all(|s| s.is_skeleton());
        Ok(GuardMerge {
            starts: vec![0; shards.len()],
            fences,
            next: 0,
            shards,
            skeleton,
            distinct,
            out_schema: if skeleton { schema.pk_only() } else { schema },
        })
    }

    /// Whether every destination is merged.
    pub fn done(&self) -> bool {
        self.next > self.fences.len()
    }

    /// The input bytes merged so far: each shard's file share of the rows of it
    /// merged.
    pub fn bytes_merged(&self) -> u64 {
        (self.shards.iter().zip(&self.starts))
            .map(|(s, &at)| (u128::from(s.file_len()) * at as u128 / s.row_count().max(1) as u128) as u64)
            .sum()
    }

    /// The window of every shard the next destination merges.
    fn windows(&self) -> Vec<Range<usize>> {
        let until = self.fences.get(self.next).map(PkBuf::pk_bytes);
        (self.shards.iter().zip(&self.starts))
            .map(|(shard, &start)| match until {
                Some(key) => start..shard.seek(key, Some(start)),
                None => start..shard.row_count(),
            })
            .collect()
    }

    /// Merge the next destination's rows into one run; `None` when every row
    /// cancelled. Long values are coded under `codebook` where it pays; those of
    /// a shard of the same table keep their codes.
    ///
    /// Out of line: inlined into a caller, the payload comparator drops out of
    /// the merge loop.
    #[inline(never)]
    pub fn next<'a>(&mut self, codebook: Option<&'a Codebook>) -> Option<GuardRun<'a>> {
        let windows = self.windows();
        for (start, w) in self.starts.iter_mut().zip(&windows) {
            *start = w.end;
        }
        self.next += 1;
        // A skeleton run holds no string; a table that does not pay codes none.
        let codebook = codebook.filter(|b| !self.skeleton && b.pays());
        let batch = match self.in_order(&windows) {
            Some(order) => self.concat(&windows, &order, codebook),
            None => self.merged(windows, codebook),
        }?;
        Some(GuardRun::new(batch, &self.out_schema, codebook, self.skeleton))
    }

    /// The run of `windows`, one per shard, merged: every key compared and
    /// each survivor copied, its long values coded under `codebook` where
    /// given. `None` when nothing survives.
    fn merged(&self, windows: Vec<Range<usize>>, codebook: Option<&Codebook>) -> Option<Batch> {
        let shards: Vec<_> = (self.shards.iter().zip(&windows))
            .map(|(s, w)| s.window(w.clone()))
            .collect();
        let (survivors, spans) = run_merge(&shards, &self.out_schema, windows.iter().cloned(), self.distinct);
        if survivors.is_empty() {
            return None;
        }
        Some(UnifiedSet::with(&shards, &self.out_schema, windows, 0, spans, |set| {
            set.materialize(&survivors, codebook.map(|b| b as &dyn HeapCoder))
        }))
    }

    /// The shards holding rows in `windows`, in the [`disjoint_order`] of their
    /// windows, when the windows need no merge. Windows that are not
    /// [`distinct`](Self::distinct) fold within themselves, so always merge.
    fn in_order(&self, windows: &[Range<usize>]) -> Option<SmallVec<[usize; 8]>> {
        if !self.distinct {
            return None;
        }
        disjoint_order(
            (self.shards.iter().zip(windows).enumerate())
                .filter(|(_, (_, w))| !w.is_empty())
                .map(|(i, (s, w))| (s.pk_at(w.start), s.pk_at(w.end - 1), i)),
        )
    }

    /// The run of the windows of the shards [`Self::in_order`] put in `order`:
    /// each window's rows decoded straight into it, one after the other, its
    /// long values coded under `codebook` where given. A window's heap is
    /// carried whole where [`MappedShard::carry_for`] carries it, and its
    /// values are relocated under one dedup cache otherwise. `None` when every
    /// window is empty.
    fn concat(&self, windows: &[Range<usize>], order: &[usize], codebook: Option<&Codebook>) -> Option<Batch> {
        let rows: usize = order.iter().map(|&i| windows[i].len()).sum();
        if rows == 0 {
            return None;
        }
        let out_schema = &self.out_schema;
        let strings = out_schema.string_payload_slots();
        let shards: SmallVec<[_; 8]> = (order.iter())
            .map(|&i| {
                let (s, w) = (&*self.shards[i], windows[i].clone());
                (s, w.clone(), s.carry_for(w, strings, codebook))
            })
            .collect();
        let mut batch = Batch::with_capacity(out_schema, rows);
        if out_schema.has_german_string() {
            batch.reserve_blob(
                (shards.iter())
                    .map(|(s, w, carry)| match carry {
                        Some((heap, _)) => heap.len(),
                        None => prorated_blob_cap(s.heap().plain_len(), s.row_count(), w.len()),
                    })
                    .sum(),
            );
        }
        let mut cache = BlobCache::for_rows(out_schema, rows);
        for (s, w, carry) in shards {
            let dest = match carry {
                Some((heap, dead)) => StringDest::Carried(batch.adopt_heap(heap, dead, 0)),
                None => StringDest::Placed(codebook.map(|b| b as &dyn HeapCoder)),
            };
            batch.write_at_most(w.len(), &mut cache, |dw| {
                s.decode_rows_into(w.clone(), dw, dest, 0);
                (w.len(), ())
            });
        }
        // A coded run's heap holds codes, which compare as content only through
        // the codebook, so it is left unflagged, as the merge path leaves its run.
        if codebook.is_none() {
            batch.certify_consolidated();
        }
        Some(batch)
    }
}

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/compact.rs"]
mod bench;
