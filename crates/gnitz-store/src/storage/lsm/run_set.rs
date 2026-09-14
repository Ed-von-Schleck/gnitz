//! `RunSet` — a set of in-heap sorted runs with a PK bloom and a fold trigger.
//!
//! Both RAM tiers of a [`Table`](super::table::Table) are this: the memtable
//! accepts ingest batches and folds them into one run past its byte budget; the
//! RAM tier accepts those folded runs and spills to a shard past its ceiling.
//! The two are separate sets, not one, because the memtable's small budget is
//! what keeps the RAM tier's big fold off the per-push path: the memtable
//! absorbs many pushes per drain, so the tier below it re-merges its whole
//! window far less often than a single set folding every [`FOLD_THRESHOLD`]
//! pushes would.

use std::cell::OnceCell;
use std::rc::Rc;

use super::batch::{write_to_batch, Batch, Layout};
use super::bloom::BloomFilter;
use super::merge::{self, MemBatch};
use super::scatter::scatter_unified_sources;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;

/// Runs to accumulate before folding them into one. Bounds the cost of cursor
/// builds and PK probes, and cancels weight-cancelled rows early.
///
/// Measured by e2e ingest sweep: 125k rows/s at 4, 143k at 16, 133k at 32 — a
/// smaller value folds too eagerly, a larger one leaves too many runs for the
/// merge and the probe to walk.
pub(super) const FOLD_THRESHOLD: usize = 16;

/// Rough bytes per row, used to size the bloom from a byte budget.
const EST_BYTES_PER_ROW: usize = 40;

pub(super) struct RunSet {
    runs: Vec<Rc<Batch>>,
    /// PK bloom over every live run, built **lazily on the first probe** and
    /// maintained on later pushes. A set is written far more often than it is
    /// point-probed — view and operator-trace tables never probe at all — so
    /// hashing every ingested row up front would be pure overhead for the bulk
    /// of ingest volume. Base tables probe once per DML row, so they build once
    /// per fold window and amortize.
    ///
    /// A fold that cancelled rows drops it, so the next probe rebuilds without
    /// their hashes; one that cancelled none keeps it (see [`Self::fold`]).
    ///
    /// A worker owns its partition single-threaded, so `OnceCell` needs no
    /// synchronization.
    bloom: OnceCell<BloomFilter>,
    /// Heap budget: [`is_full`](Self::is_full) reports crossing it, and the
    /// bloom's key capacity is derived from it. What crossing it *means* — fold
    /// into the next tier, or spill to a shard — is `Table`'s policy.
    budget: usize,
    bytes: usize,
}

impl RunSet {
    pub(crate) fn new(budget: usize) -> Self {
        RunSet {
            runs: Vec::with_capacity(FOLD_THRESHOLD),
            bloom: OnceCell::new(),
            budget,
            bytes: 0,
        }
    }

    /// Append a run, folding the set when it gets crowded. Empty runs are never
    /// stored, so `is_empty()` is exactly "no rows".
    ///
    /// The run must be consolidated — every consumer (the fold's N-way merge,
    /// the PK probe's binary search) reads it as sorted and ghost-free. Producers
    /// certify it via `into_consolidated`; the flag is re-checked against the
    /// data here in debug builds, so a run that lies about its layout is caught
    /// at the boundary rather than silently mis-merged.
    pub(crate) fn push(&mut self, run: Rc<Batch>, schema: &SchemaDescriptor) {
        debug_assert!(
            run.consolidated_verified(schema),
            "RunSet::push requires a consolidated run",
        );
        if run.count == 0 {
            return;
        }
        // Maintain the bloom only once built (first probe); an unprobed set pays
        // nothing.
        if let Some(bloom) = self.bloom.get_mut() {
            bloom_add_batch(bloom, &run);
        }
        self.bytes += run.total_bytes();
        self.runs.push(run);
        if self.runs.len() >= FOLD_THRESHOLD {
            self.fold(schema);
        }
    }

    pub(super) fn runs(&self) -> &[Rc<Batch>] {
        &self.runs
    }

    /// How many runs this set holds — one cursor source each.
    pub(super) fn len(&self) -> usize {
        self.runs.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// The set has outgrown its heap budget and must be drained by its owner.
    pub(super) fn is_full(&self) -> bool {
        self.bytes > self.budget
    }

    /// Shrink the budget so a test can drive the drain path without ingesting
    /// megabytes. A production store's budgets are fixed at its open.
    #[cfg(test)]
    pub(super) fn set_budget(&mut self, budget: usize) {
        self.budget = budget;
        self.bloom.take(); // its key capacity was sized from the old budget
    }

    pub(super) fn row_count(&self) -> usize {
        self.runs.iter().map(|r| r.count).sum()
    }

    pub(super) fn clear(&mut self) {
        self.runs.clear();
        self.bytes = 0;
        self.bloom.take();
    }

    /// Widen every run narrower than `schema` to it, filling the appended
    /// trailing columns with NULL (`ALTER TABLE … ADD COLUMN`).
    ///
    /// Each widened run is a **new** `Rc`, never a mutation through the existing
    /// one — a live `ReadCursor` may still hold it. Deliberately not `push`:
    /// that folds at `FOLD_THRESHOLD`, which would collapse the set's runs as a
    /// side effect of a schema swap.
    pub(super) fn widen_runs(&mut self, schema: &SchemaDescriptor) {
        let npc = schema.num_payload_cols();
        let mut bytes = 0;
        for run in &mut self.runs {
            if run.num_payload_cols() < npc {
                let widened = run.widened_with_null_tail(schema);
                *run = Rc::new(widened);
            }
            bytes += run.total_bytes();
        }
        self.bytes = bytes;
        // The bloom hashes PK bytes alone (`bloom_add_batch`), which the widen
        // copies verbatim, so it stays valid.
    }

    /// Fold every run into one consolidated run, dropping net-zero
    /// (PK, payload) rows.
    ///
    /// An unchanged row count means nothing cancelled, so the bloom already
    /// holds exactly the survivors' keys and is kept — rebuilding on every fold
    /// is quadratic between clears, since fold *j* re-hashes what folds
    /// 1..*j*−1 hashed.
    pub(super) fn fold(&mut self, schema: &SchemaDescriptor) {
        if self.runs.len() <= 1 {
            return;
        }
        let sorted: Vec<MemBatch> = self.runs.iter().map(|r| r.as_mem_batch()).collect();
        let input_rows: usize = sorted.iter().map(|b| b.count).sum();
        let merged = consolidate_batches(&sorted, schema);
        drop(sorted); // borrows self.runs; release before the mutable reborrow
        self.runs.clear();
        if merged.count != input_rows {
            self.bloom.take();
        }
        if merged.count > 0 {
            self.bytes = merged.total_bytes();
            self.runs.push(Rc::new(merged));
        } else {
            self.bytes = 0;
        }
    }

    /// Fold to a single run and return it, **retained** — a consumer whose write
    /// fails leaves the data intact for retry, and clears the set itself once the
    /// run is safely elsewhere. `None` when the set is empty or fully cancelled.
    pub(super) fn fold_to_single(&mut self, schema: &SchemaDescriptor) -> Option<Rc<Batch>> {
        self.fold(schema);
        self.runs.first().map(Rc::clone)
    }

    /// Bloom probe for a PK by its [`probe_key`] — derived by the caller,
    /// which probes both RAM tiers and every shard with the same key. The first
    /// probe builds the filter from all live runs.
    pub(super) fn may_contain(&self, probe_key: u64) -> bool {
        // Before `get_or_init`, which would otherwise size a filter from the
        // *budget* — a megabyte for the RAM tier — to answer a question an empty
        // set already answers. An empty RAM tier is the normal state: it receives
        // a run only once the memtable has drained.
        if self.runs.is_empty() {
            return false;
        }
        let bloom = self.bloom.get_or_init(|| {
            // Sized to the budget's row capacity, NOT to the row count at first
            // probe — a filter sized to first-probe contents would over-saturate
            // as later pushes add incrementally.
            let mut bloom = BloomFilter::new((self.budget / EST_BYTES_PER_ROW).max(16) as u32);
            for run in &self.runs {
                bloom_add_batch(&mut bloom, run);
            }
            bloom
        });
        bloom.may_contain(probe_key)
    }
}

/// Insert every row's PK into `bloom`, keyed by [`probe_key`] — the same
/// derivation the shard filter and this set's probe side use, so one PK maps to
/// one key everywhere and no width or signedness produces a false negative.
fn bloom_add_batch(bloom: &mut BloomFilter, batch: &Batch) {
    for i in 0..batch.count {
        bloom.add(probe_key(batch.get_pk_bytes(i)));
    }
}

/// Merge N sorted MemBatch views into a single consolidated Batch.
fn consolidate_batches(batches: &[MemBatch], schema: &SchemaDescriptor) -> Batch {
    if batches.is_empty() {
        return Batch::empty_with_schema(schema);
    }

    let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();

    // Consolidate and count survivors first, so the output arena — whose
    // allocation dominates the flush provision cost — is sized to the
    // post-cancellation row count, not the Σ-input upper bound. Aggregation
    // trace folds cancel heavily (retract + insert per re-aggregated group), so
    // `survivors.len()` is routinely a fraction of the input row count. The blob
    // bound stays `total_blob` (survivors' blobs are a subset; blob is reserved,
    // not zeroed, so an over-estimate costs nothing).
    // `src`/`row` originate as `u32` fields of `HeapNode`, so the casts are lossless.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(batches.iter().map(|b| b.count).sum());
    merge::run_merge(batches, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w))
    });
    if survivors.is_empty() {
        return Batch::empty_with_schema(schema);
    }
    let mut cols = Vec::with_capacity(batches.len() * schema.num_payload_cols());
    let unified: Vec<_> = batches
        .iter()
        .map(|b| merge::mem_batch_to_unified(b, schema, &mut cols))
        .collect();
    let mut result = write_to_batch(schema, survivors.len(), total_blob, |writer| {
        scatter_unified_sources(&unified, &cols, &survivors, writer);
    });
    result.certify_layout(Layout::Consolidated);
    result
}

#[cfg(test)]
#[path = "tests/run_set.rs"]
mod tests;
