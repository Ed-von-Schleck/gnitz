//! `RunSet` — a set of in-heap sorted runs with a PK bloom and a fold trigger.
//!
//! Both RAM tiers of a [`Table`](super::table::Table) are this: the memtable
//! accepts ingest batches and folds them into one run past its byte budget; the
//! RAM tier accepts those folded runs and spills to a shard past its ceiling.
//! The two are separate sets, not one, because the memtable's small budget is
//! what keeps the RAM tier's big fold off the per-push path: the memtable
//! absorbs many pushes per drain, so the tier below it folds its whole
//! window far less often than a single set folding every [`FOLD_THRESHOLD`]
//! pushes would.

use std::cell::OnceCell;
use std::rc::Rc;

use super::bloom::BloomFilter;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;
use crate::storage::repr::batch::{Batch, Layout};
use crate::storage::repr::merge::{self, MemBatch};
use crate::storage::repr::scatter::UnifiedSet;

/// Runs to accumulate before folding them into one: bounds how many runs a
/// cursor merges and a PK probe walks.
pub(super) const FOLD_THRESHOLD: usize = 16;

/// Rough bytes per row, used to size the bloom from a byte budget.
const EST_BYTES_PER_ROW: usize = 40;

pub(super) struct RunSet {
    runs: Vec<Rc<Batch>>,
    /// PK bloom over every live run, built on the first probe: a set that is
    /// never probed never hashes a row.
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

    /// Append a consolidated run, folding the set when it gets crowded. Empty
    /// runs are never stored, so `is_empty()` is exactly "no rows".
    pub(crate) fn push(&mut self, run: Rc<Batch>, schema: &SchemaDescriptor) {
        debug_assert!(
            run.consolidated_verified(schema),
            "RunSet::push requires a consolidated run",
        );
        if run.count == 0 {
            return;
        }
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

    /// Rebind the ceiling a drain is judged against.
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
    pub(super) fn widen_runs(&mut self, schema: &SchemaDescriptor) {
        let npc = schema.num_payload_cols();
        let mut bytes = 0;
        for run in &mut self.runs {
            if run.num_payload_cols() < npc {
                let widened = run.widened_with_nulls(schema, false);
                debug_assert!(
                    widened.consolidated_verified(schema),
                    "widen_runs: the widened run must still be consolidated",
                );
                *run = Rc::new(widened);
            }
            bytes += run.total_bytes();
        }
        self.bytes = bytes;
        // The widen leaves PK bytes untouched, so the bloom stays valid.
    }

    /// Fold every run into one consolidated run, dropping net-zero
    /// (PK, payload) rows.
    pub(super) fn fold(&mut self, schema: &SchemaDescriptor) {
        if self.runs.len() <= 1 {
            return;
        }
        let input_rows = self.row_count();
        // A dominant run — usually the previous fold's output — is galloped
        // against the fold of the rest, so its stretches are bulk-copied.
        let big = (0..self.runs.len()).max_by_key(|&i| self.runs[i].count).unwrap();
        let merged = if self.runs[big].count * 2 >= input_rows {
            let dominant = self.runs.swap_remove(big);
            match &self.runs[..] {
                [one] => dominant.merged_consolidated(one, schema),
                _ => dominant.merged_consolidated(&self.consolidate_all(schema), schema),
            }
        } else {
            self.consolidate_all(schema)
        };
        self.runs.clear();
        // Keep the bloom only while it holds exactly the survivors' keys.
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

    /// Every run folded N-way into one consolidated batch.
    fn consolidate_all(&self, schema: &SchemaDescriptor) -> Batch {
        let views: Vec<MemBatch> = self.runs.iter().map(|r| r.as_mem_batch()).collect();
        let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(self.row_count());
        merge::run_merge(&views, schema, |src, row, w| {
            survivors.push((src as u32, row as u32, w))
        });
        let mut result = UnifiedSet::whole(&views, schema).materialize(&survivors, survivors.len());
        result.certify_layout(Layout::Consolidated);
        result
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
        // Answered without building a budget-sized filter.
        if self.runs.is_empty() {
            return false;
        }
        let bloom = self.bloom.get_or_init(|| {
            // Sized to the budget, not the current rows: later pushes add to it.
            let mut bloom = BloomFilter::new((self.budget / EST_BYTES_PER_ROW).max(16) as u32);
            for run in &self.runs {
                bloom_add_batch(&mut bloom, run);
            }
            bloom
        });
        bloom.may_contain(probe_key)
    }
}

/// Insert every row's PK into `bloom`, keyed by [`probe_key`].
fn bloom_add_batch(bloom: &mut BloomFilter, batch: &Batch) {
    for i in 0..batch.count {
        bloom.add(probe_key(batch.get_pk_bytes(i)));
    }
}

#[cfg(test)]
#[path = "tests/run_set.rs"]
mod tests;
