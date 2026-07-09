//! Sorted-run management for [`MemTable`]: appending consolidated runs, N-way
//! fold/consolidation, flush preparation, the capacity / flush triggers, and
//! the inline-consolidation policy.

use std::rc::Rc;

use super::super::batch::{write_to_batch, Batch, Layout};
use super::super::merge::{self, pack_pk_be, SortedMemBatch};
use super::{MemTable, INLINE_CONSOLIDATE_THRESHOLD};
use crate::schema::SchemaDescriptor;

/// Merge N sorted MemBatch views into a single consolidated Batch.
pub(super) fn consolidate_batches(batches: &[SortedMemBatch], schema: &SchemaDescriptor) -> Batch {
    if batches.is_empty() {
        return Batch::empty_with_schema(schema);
    }

    let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();

    // Phase 1: consolidate and count survivors first, so the output arena — whose
    // zero-fill and allocation dominate the flush provision cost — is sized to the
    // post-cancellation row count, not the Σ-input upper bound. Aggregation trace
    // folds cancel heavily (retract + insert per re-aggregated group), so
    // `survivors.len()` is routinely a fraction of the input row count. The blob
    // upper bound stays `total_blob` (survivors' blobs are a subset; blob is
    // reserved, not zeroed, so an over-estimate costs nothing).
    let survivors = merge::merge_survivors(batches, schema);
    if survivors.is_empty() {
        return Batch::empty_with_schema(schema); // empty or fully cancelled fold
    }
    let mut result = write_to_batch(schema, survivors.len(), total_blob, |writer| {
        merge::scatter_survivors(batches, schema, &survivors, writer);
    });
    result.certify_layout(Layout::Consolidated, schema);
    result
}

/// Fold flushed runs (each individually (PK,payload)-sorted) into one run.
/// Single-run input is returned by clone (no merge). Used by `Table` to
/// consolidate `in_memory_l0` (the in-heap non-durable flush run set).
pub(crate) fn consolidate_runs(runs: &[Rc<Batch>], schema: &SchemaDescriptor) -> Rc<Batch> {
    if runs.len() == 1 {
        return Rc::clone(&runs[0]);
    }
    let sorted: Vec<SortedMemBatch> = runs
        .iter()
        .map(|r| r.as_sorted_mem_batch(schema).expect("flushed runs are sorted"))
        .collect();
    Rc::new(consolidate_batches(&sorted, schema))
}

/// Insert every row's PK into `bloom`, keyed by its leading ≤16 OPK bytes via
/// `pack_pk_be` — the same derivation the probe side (`may_contain_pk`,
/// `InMemRun::may_contain`) packs with, consistent for signed PKs (whose
/// sign-flipped `get_pk` value would not be). `pack_pk_be` truncates to the
/// leading 16 OPK bytes, so wide PKs (`pk_stride > 16`) hash their prefix: add
/// and probe pack identically, so there is no false negative; two wide PKs
/// sharing a 16-byte prefix collide to one slot, a tolerated false positive
/// resolved by the run scan. The single owner of the PK bloom keying, shared by
/// the memtable bloom and the per-run `in_memory_l0` blooms.
/// Construct an empty bloom for `expected_n` keys — the one constructor the
/// memtable's lazy build uses.
pub(super) fn new_bloom(expected_n: u32) -> super::super::bloom::BloomFilter {
    super::super::bloom::BloomFilter::new(expected_n)
}

pub(crate) fn bloom_add_batch(bloom: &mut super::super::bloom::BloomFilter, batch: &Batch) {
    for i in 0..batch.count {
        bloom.add(pack_pk_be(batch.get_pk_bytes(i)));
    }
}

impl MemTable {
    /// Append a consolidated batch as a new run. The batch must be consolidated
    /// (debug-verified at entry); producers certify it via `into_consolidated`.
    pub fn upsert_sorted_batch(&mut self, batch: Batch) {
        debug_assert!(
            batch.consolidated_verified(&self.schema),
            "upsert_sorted_batch requires a consolidated batch",
        );
        // The sole production caller pre-flushes at 75% of `max_bytes`
        // (`Table::ingest_owned_batch`), so the arena can never
        // overfill here.
        debug_assert!(
            self.runs_bytes <= self.max_bytes,
            "memtable overfilled: caller skipped the pre-flush"
        );
        if batch.count == 0 {
            return;
        }
        // Maintain the bloom only once built (first probe); an unprobed
        // memtable pays nothing.
        if let Some(bloom) = self.bloom.get_mut() {
            bloom_add_batch(bloom, &batch);
        }
        self.runs_bytes += batch.total_bytes();
        self.runs.push(Rc::new(batch));
        self.maybe_inline_consolidate();
    }
    pub fn should_flush(&self) -> bool {
        self.runs_bytes > self.max_bytes * 3 / 4
    }

    pub fn is_empty(&self) -> bool {
        // Empty batches are never pushed, so no runs ⇔ no rows.
        self.runs.is_empty()
    }

    fn runs_as_sorted(&self) -> Vec<SortedMemBatch<'_>> {
        self.runs
            .iter()
            .map(|r| {
                r.as_sorted_mem_batch(&self.schema)
                    .expect("MemTable runs are always sorted")
            })
            .collect()
    }

    /// Borrow the current sorted runs. Each run is independently
    /// sorted+consolidated; cross-run merging is deferred to the consumer
    /// (typically a `ReadCursor`).
    pub fn snapshot_runs(&self) -> &[Rc<Batch>] {
        &self.runs
    }

    /// Collapse runs into one merged run in-place.
    /// `found` is dropped because run/row indices change.
    fn force_consolidate(&mut self) {
        if self.runs.len() <= 1 {
            return;
        }
        let batches = self.runs_as_sorted();
        let merged = consolidate_batches(&batches, &self.schema);
        // batches borrow from self.runs; release before clear() reborrows mut.
        drop(batches);
        self.runs.clear();
        self.runs_bytes = merged.total_bytes();
        // Rebuild the bloom (iff built) from surviving rows only: stale hashes
        // from weight-cancelled rows are cleared here, reducing the
        // false-positive rate. Re-adding into the live filter instead would
        // ratchet the FP rate up across folds.
        if let Some(bloom) = self.bloom.get_mut() {
            bloom.reset();
            if merged.count > 0 {
                bloom_add_batch(bloom, &merged);
            }
        }
        if merged.count > 0 {
            self.runs.push(Rc::new(merged));
        }
    }

    /// Consolidate runs and return an `Rc<Batch>` of the merged result.
    /// The memtable retains the merged run until `reset()`, so a failed
    /// flush IO leaves the data intact for retry.
    pub fn consolidate_for_flush(&mut self) -> Rc<Batch> {
        self.force_consolidate();
        if self.runs.is_empty() {
            Rc::new(Batch::empty_with_schema(&self.schema))
        } else {
            Rc::clone(&self.runs[0])
        }
    }
    /// Clear all runs and the bloom filter.  Ready for reuse.
    pub fn reset(&mut self) {
        self.runs.clear();
        self.runs_bytes = 0;
        self.bloom.take();
    }

    /// If the run count has reached the threshold, merge all runs into a
    /// single consolidated run.  Bounds the cost of cursor builds and
    /// `lookup_pk()`, and eliminates weight-cancelled rows early.
    fn maybe_inline_consolidate(&mut self) {
        if self.runs.len() >= INLINE_CONSOLIDATE_THRESHOLD {
            self.force_consolidate();
        }
    }
}
