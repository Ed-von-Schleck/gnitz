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
use gnitz_zset::repr::{merge_consolidated, Batch, MemBatch};
use gnitz_zset::schema::key::{pk_bytes_eq, pk_in_range, pk_ranges_overlap, probe_key, PkBuf};
use gnitz_zset::schema::SchemaDescriptor;

/// Runs to accumulate before folding them into one: bounds how many runs a
/// cursor merges and a PK probe walks.
pub(super) const FOLD_THRESHOLD: usize = 16;

/// Rough bytes per row, used to size the bloom from a byte budget.
const EST_BYTES_PER_ROW: usize = 40;

fn pk_min(run: &Batch) -> &[u8] {
    run.get_pk_bytes(0)
}

fn pk_max(run: &Batch) -> &[u8] {
    run.get_pk_bytes(run.len() - 1)
}

/// A batch as a [`RunSet`] holds it: [`Batch::trimmed`], since the set charges
/// only its rows' bytes.
#[derive(Clone)]
pub(super) struct TrimmedRun(Rc<Batch>);

impl TrimmedRun {
    pub(super) fn new(batch: Batch) -> Self {
        TrimmedRun(Rc::new(batch.trimmed()))
    }

    /// The run itself, shared.
    pub(super) fn rc(&self) -> Rc<Batch> {
        Rc::clone(&self.0)
    }

    /// The run's rows, copied only while a set still holds the run.
    pub(super) fn into_batch(self) -> Batch {
        Rc::try_unwrap(self.0).unwrap_or_else(|rc| Batch::clone(&rc))
    }
}

impl std::ops::Deref for TrimmedRun {
    type Target = Batch;
    fn deref(&self) -> &Batch {
        &self.0
    }
}

pub(super) struct RunSet {
    runs: Vec<Rc<Batch>>,
    /// PK bloom over every key pushed since it was built — a superset of the
    /// live runs' keys — built on the first probe.
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
    /// runs are never stored.
    pub(crate) fn push(&mut self, TrimmedRun(run): TrimmedRun, schema: &SchemaDescriptor) {
        debug_assert!(run.consolidated_verified(), "RunSet::push requires a consolidated run",);
        if run.is_empty() {
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

    /// The runs whose PK extent meets the inclusive `bound`; `None` takes them all.
    pub(super) fn runs_overlapping(&self, bound: Option<(PkBuf, PkBuf)>) -> impl Iterator<Item = &Rc<Batch>> {
        self.runs.iter().filter(move |run| {
            bound.is_none_or(|(lo, hi)| pk_ranges_overlap(pk_min(run), pk_max(run), lo.pk_bytes(), hi.pk_bytes()))
        })
    }

    /// Visit each run holding `key`, newest first, with the row its matches start
    /// at; `fingerprint` is `key`'s [`probe_key`].
    pub(super) fn find_pk_bytes(&self, key: &[u8], fingerprint: u64, mut visitor: impl FnMut(&Rc<Batch>, usize)) {
        if !self.may_contain(fingerprint) {
            return;
        }
        for run in self.runs.iter().rev() {
            if !pk_in_range(pk_min(run), pk_max(run), key) {
                continue;
            }
            let start = run.find_lower_bound_bytes(key);
            if start < run.len() && pk_bytes_eq(run.get_pk_bytes(start), key) {
                visitor(run, start);
            }
        }
    }

    /// How many runs this set holds — one cursor source each.
    pub(super) fn len(&self) -> usize {
        self.runs.len()
    }

    /// The set has outgrown its heap budget and must be drained by its owner.
    pub(super) fn is_full(&self) -> bool {
        self.bytes > self.budget
    }

    pub(super) fn row_count(&self) -> usize {
        self.runs.iter().map(|r| r.len()).sum()
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
                    widened.consolidated_verified(),
                    "widen_runs: the widened run must still be consolidated",
                );
                *run = TrimmedRun::new(widened).0;
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
        let big = (0..self.runs.len()).max_by_key(|&i| self.runs[i].len()).unwrap();
        let merged = if self.runs[big].len() * 2 >= input_rows {
            // `remove`, not `swap_remove`: the rest stay in push order, which
            // is what lets an ascending load's runs fold by concatenation.
            let dominant = self.runs.remove(big);
            match &self.runs[..] {
                [one] => dominant.merged_consolidated(one, schema),
                _ => dominant.merged_consolidated(&self.consolidate_all(schema), schema),
            }
        } else {
            self.consolidate_all(schema)
        };
        self.runs.clear();
        // A cancelled row's key stays in the filter as a false positive.
        if self.bloom.get().is_some_and(|b| b.stale(merged.len())) {
            self.bloom.take();
        }
        self.bytes = 0;
        if !merged.is_empty() {
            let run = TrimmedRun::new(merged).0;
            self.bytes = run.total_bytes();
            self.runs.push(run);
        }
    }

    /// Every run folded N-way into one consolidated batch.
    fn consolidate_all(&self, schema: &SchemaDescriptor) -> Batch {
        // Runs that each end below where the next begins share no row and hold
        // none out of order across them: as they stand they are the merge,
        // copied whole. An ascending load folds this way.
        if self.runs.windows(2).all(|w| pk_max(&w[0]) < pk_min(&w[1])) {
            let mut out = Batch::concat(schema, self.runs.iter().map(|r| r.as_mem_batch()));
            out.certify_consolidated();
            return out;
        }
        let views: Vec<MemBatch> = self.runs.iter().map(|r| r.as_mem_batch()).collect();
        merge_consolidated(&views, schema)
    }

    /// Fold to a single run and return it, **retained** — a consumer whose write
    /// fails leaves the data intact for retry, and clears the set itself once the
    /// run is safely elsewhere. `None` when the set is empty or fully cancelled.
    pub(super) fn fold_to_single(&mut self, schema: &SchemaDescriptor) -> Option<TrimmedRun> {
        self.fold(schema);
        self.runs.first().map(|run| TrimmedRun(Rc::clone(run)))
    }

    /// Bloom probe for a PK by its [`probe_key`]. The first probe builds the
    /// filter from all live runs.
    fn may_contain(&self, probe_key: u64) -> bool {
        // Answered without building a budget-sized filter.
        if self.runs.is_empty() {
            return false;
        }
        let bloom = self.bloom.get_or_init(|| {
            // Sized to the budget, not the current rows: later pushes add to it.
            let mut bloom = BloomFilter::new((self.budget / EST_BYTES_PER_ROW).max(16));
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
    for i in 0..batch.len() {
        bloom.add(probe_key(batch.get_pk_bytes(i)));
    }
}

#[cfg(test)]
#[path = "tests/run_set.rs"]
mod tests;
