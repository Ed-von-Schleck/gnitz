//! Sorted-run management for [`MemTable`]: appending consolidated runs, N-way
//! fold/consolidation, flush preparation, the capacity / flush triggers, and
//! the inline-consolidation policy.

use std::rc::Rc;

use super::super::batch::{write_to_batch, Batch, ConsolidatedBatch};
use super::super::error::StorageError;
use super::super::merge::{self, pack_pk_be, SortedMemBatch};
use super::{MemTable, INLINE_CONSOLIDATE_THRESHOLD};
use crate::schema::SchemaDescriptor;

/// Merge N sorted MemBatch views into a single consolidated Batch.
pub(super) fn consolidate_batches(batches: &[SortedMemBatch], schema: &SchemaDescriptor) -> Batch {
    if batches.is_empty() {
        return Batch::empty_with_schema(schema);
    }

    let total_rows: usize = batches.iter().map(|b| b.count).sum();
    let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();
    if total_rows == 0 {
        return Batch::empty_with_schema(schema);
    }

    let mut result = write_to_batch(schema, total_rows, total_blob, |writer| {
        merge::merge_batches(batches, schema, writer);
    });
    result.sorted = true;
    result.consolidated = true;
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

impl MemTable {
    /// Insert every row's PK into the bloom, keyed by its leading ≤16 OPK bytes
    /// via `pack_pk_be` — the same derivation `may_contain_pk` probes with,
    /// consistent for signed PKs (whose sign-flipped `get_pk` value would not
    /// be). `pack_pk_be` truncates to the leading 16 OPK bytes, so wide PKs
    /// (`pk_stride > 16`) hash their prefix: add and probe pack identically, so
    /// there is no false negative; two wide PKs sharing a 16-byte prefix collide
    /// to one slot, a tolerated false positive resolved by the run scan.
    fn bloom_add_batch(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            self.bloom.add(pack_pk_be(batch.get_pk_bytes(i)));
        }
    }

    /// Append a consolidated batch as a new run.
    pub fn upsert_sorted_batch(&mut self, batch: ConsolidatedBatch) -> Result<(), StorageError> {
        let batch = batch.into_inner();
        if batch.count == 0 {
            return Ok(());
        }
        self.check_capacity()?;
        self.bloom_add_batch(&batch);
        self.total_row_count += batch.count;
        self.runs_bytes += batch.total_bytes();
        self.runs.push(Rc::new(batch));
        self.maybe_inline_consolidate();
        Ok(())
    }
    pub fn should_flush(&self) -> bool {
        self.runs_bytes > self.max_bytes * 3 / 4
    }

    pub fn is_empty(&self) -> bool {
        self.total_row_count == 0
    }

    #[cfg(test)]
    pub(crate) fn total_row_count(&self) -> usize {
        self.total_row_count
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
        self.total_row_count = merged.count;
        // Rebuild bloom from surviving rows only: stale hashes from
        // weight-cancelled rows are cleared here, reducing false-positive rate.
        self.bloom.reset();
        if merged.count > 0 {
            self.bloom_add_batch(&merged);
            self.runs.push(Rc::new(merged));
        }
        self.found = None;
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
    /// Clear all runs and bloom filter.  Ready for reuse.
    pub fn reset(&mut self) {
        self.runs.clear();
        self.runs_bytes = 0;
        self.total_row_count = 0;
        self.found = None;
        self.bloom.reset();
    }

    /// If the run count has reached the threshold, merge all runs into a
    /// single consolidated run.  Bounds the cost of cursor builds and
    /// `lookup_pk()`, and eliminates weight-cancelled rows early.
    fn maybe_inline_consolidate(&mut self) {
        if self.runs.len() >= INLINE_CONSOLIDATE_THRESHOLD {
            self.force_consolidate();
        }
    }

    fn check_capacity(&self) -> Result<(), StorageError> {
        if self.runs_bytes > self.max_bytes {
            return Err(StorageError::Capacity);
        }
        Ok(())
    }
}
