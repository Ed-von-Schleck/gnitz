//! `RunSet` — a set of in-heap sorted runs with a PK bloom and a fold trigger.
//!
//! Both RAM tiers of a [`Table`](super::table::Table) are this: the memtable
//! accepts ingest batches and folds them into one run at 3/4 of its arena; the
//! RAM tier accepts those folded runs and spills to a shard past its ceiling.
//! Same runs, same fold, same probe — only the byte trigger and where the folded
//! run goes differ, and those live in `Table` as policy.

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
    /// per fold window and amortize. Dropped on every fold and rebuilt on the
    /// next probe, which also clears the stale hashes of weight-cancelled rows.
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
    pub(super) fn new(budget: usize) -> Self {
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
    pub(super) fn push(&mut self, run: Rc<Batch>, schema: &SchemaDescriptor) {
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

    #[cfg(test)]
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

    /// Shrink the budget so tests can drive the drain path without ingesting
    /// megabytes.
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

    /// Widen every run narrower than `out_schema` to it, filling the appended
    /// trailing columns with NULL (`ALTER TABLE … ADD COLUMN`). `in_schema` is
    /// this set's still-live pre-ALTER descriptor, which a `Batch` cannot supply
    /// on its own (it carries only region strides).
    ///
    /// Each widened run is a **new** `Rc`, never a mutation through the existing
    /// one — a live `ReadCursor` may still hold it. Deliberately not `push`:
    /// that folds at `FOLD_THRESHOLD`, which would collapse the set's runs as a
    /// side effect of a schema swap.
    pub(super) fn widen_runs(&mut self, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) {
        let out_npc = out_schema.num_payload_cols();
        if in_schema.num_payload_cols() == out_npc {
            return;
        }
        let mut bytes = 0;
        for run in &mut self.runs {
            if run.num_payload_cols() < out_npc {
                let widened = run.widened_with_null_tail(in_schema, out_schema);
                *run = Rc::new(widened);
            }
            bytes += run.total_bytes();
        }
        self.bytes = bytes;
        // The bloom hashes PK bytes alone (`bloom_add_batch`), which the widen
        // copies verbatim, so it stays valid.
    }

    /// Fold every run into one consolidated run, dropping net-zero
    /// (PK, payload) rows. The bloom is dropped rather than rebuilt: the next
    /// probe rebuilds it from the survivors alone, so a set that is folded and
    /// then flushed without being probed never pays for the rebuild at all.
    pub(super) fn fold(&mut self, schema: &SchemaDescriptor) {
        if self.runs.len() <= 1 {
            return;
        }
        let sorted: Vec<MemBatch> = self.runs.iter().map(|r| r.as_mem_batch()).collect();
        let merged = consolidate_batches(&sorted, schema);
        drop(sorted); // borrows self.runs; release before the mutable reborrow
        self.runs.clear();
        self.bloom.take();
        self.bytes = merged.total_bytes();
        if merged.count > 0 {
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
    // zero-fill and allocation dominate the flush provision cost — is sized to
    // the post-cancellation row count, not the Σ-input upper bound. Aggregation
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
    let mut cols = Vec::new();
    let unified: Vec<_> = batches
        .iter()
        .map(|b| merge::mem_batch_to_unified(b, schema, &mut cols))
        .collect();
    let mut result = write_to_batch(schema, survivors.len(), total_blob, |writer| {
        scatter_unified_sources(&unified, &cols, &survivors, writer);
    });
    result.certify_layout(Layout::Consolidated, schema);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{make_batch, make_schema_u64_i64};

    fn push(set: &mut RunSet, schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) {
        set.push(Rc::new(make_batch(schema, rows)), schema);
    }

    /// Probe by a narrow PK, deriving the filter key the way the production walk
    /// does so no assertion spells a second version of it.
    fn probes(set: &RunSet, pk: u64) -> bool {
        set.may_contain(probe_key(&pk.to_be_bytes()))
    }

    #[test]
    fn fold_sums_weights_and_drops_ghosts() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        assert!(set.is_empty());

        push(&mut set, &schema, &[(10, 1, 100), (30, 1, 300)]);
        push(&mut set, &schema, &[(20, 1, 200), (30, -1, 300)]);
        assert_eq!(set.len(), 2);

        let folded = set.fold_to_single(&schema).expect("survivors remain");
        assert_eq!(folded.count, 2, "PK 30 cancels to a ghost");
        assert_eq!(folded.get_pk(0), 10);
        assert_eq!(folded.get_pk(1), 20);
    }

    /// A single run folds by handing back the run itself — no rewrite.
    #[test]
    fn fold_to_single_is_identity_for_one_run() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        push(&mut set, &schema, &[(10, 1, 100), (20, 1, 200)]);
        let original = Rc::clone(&set.runs()[0]);

        let folded = set.fold_to_single(&schema).expect("one run");
        assert!(Rc::ptr_eq(&folded, &original), "singleton fold must not rewrite");
    }

    #[test]
    fn empty_and_fully_cancelled_sets_fold_to_none() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        assert!(set.fold_to_single(&schema).is_none(), "empty set");

        push(&mut set, &schema, &[(1, 1, 10)]);
        push(&mut set, &schema, &[(1, -1, 10)]);
        assert!(set.fold_to_single(&schema).is_none(), "fully cancelled set");
        assert!(set.is_empty());
        assert_eq!(set.bytes(), 0, "byte total tracks the fold");
    }

    /// Pushing past the threshold folds inline, so the run count never exceeds it.
    #[test]
    fn push_folds_at_the_threshold() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        for i in 0..FOLD_THRESHOLD as u64 - 1 {
            push(&mut set, &schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
        }
        assert_eq!(set.len(), FOLD_THRESHOLD - 1, "below the threshold: no fold");

        push(&mut set, &schema, &[(FOLD_THRESHOLD as u64, 1, 1600)]);
        assert_eq!(set.len(), 1, "the threshold push folds");
        assert_eq!(set.row_count(), FOLD_THRESHOLD);
    }

    #[test]
    fn empty_runs_are_never_stored() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        set.push(Rc::new(make_batch(&schema, &[])), &schema);
        assert!(set.is_empty());
    }

    /// The bloom answers for every live PK, is maintained across pushes once
    /// built, and survives a fold (rebuilt lazily from the survivors).
    #[test]
    fn bloom_covers_live_rows_across_push_and_fold() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        push(&mut set, &schema, &[(10, 1, 100), (20, 1, 200)]);

        assert!(probes(&set, 10), "first probe builds the filter");
        assert!(probes(&set, 20));

        // Maintained incrementally once built.
        push(&mut set, &schema, &[(30, 1, 300)]);
        assert!(probes(&set, 30));

        set.fold(&schema);
        for pk in [10u64, 20, 30] {
            assert!(probes(&set, pk), "PK {pk} after fold");
        }
    }

    /// A run handed out by `runs()` stays readable after the set is cleared —
    /// the cursor-lifetime contract.
    #[test]
    fn handed_out_runs_survive_clear() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        push(&mut set, &schema, &[(10, 1, 100)]);
        let held = Rc::clone(&set.runs()[0]);

        set.clear();
        assert!(set.is_empty());
        assert_eq!(held.count, 1);
        assert_eq!(held.get_pk(0), 10);
    }

    /// A 2-row batch with descending PKs — not `(PK, payload)`-sorted.
    fn desc_two_row_batch(schema: &SchemaDescriptor) -> Batch {
        let mut b = Batch::with_capacity(*schema, 2);
        for &(pk, val) in &[(20u128, 200i64), (10, 100)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    /// A run that lies about being consolidated is rejected on the way in.
    /// `set_layout_unchecked` stamps the tag without inspecting the data, so the
    /// descending batch is built without complaint — but `push`, which skips a
    /// re-fold on the strength of that tag, verifies it and panics. In production
    /// the ingress strip clears the layout, so such a run never reaches a set.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "flagged consolidated")]
    fn lying_consolidated_run_is_rejected() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);

        let mut bad = desc_two_row_batch(&schema);
        bad.set_layout_unchecked(Layout::Consolidated);
        set.push(Rc::new(bad), &schema);
    }

    /// The same unsorted rows with the flags stripped (as the ingress strip
    /// leaves every client batch) sort+consolidate and push without complaint.
    #[test]
    fn cleared_flags_unsorted_run_consolidates_ok() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        push(&mut set, &schema, &[(5, 1, 50)]);

        let clean = desc_two_row_batch(&schema);
        set.push(Rc::new(clean.into_consolidated(&schema)), &schema);

        let folded = set.fold_to_single(&schema).expect("three rows survive");
        assert_eq!(folded.count, 3);
        assert_eq!(folded.get_pk(0), 5);
        assert_eq!(folded.get_pk(1), 10);
        assert_eq!(folded.get_pk(2), 20);
    }

    /// Reduce-output shape: insertion + retraction across ticks, where each tick
    /// retracts the previous aggregate and inserts the new one. Only the last
    /// tick's aggregate may survive the fold.
    #[test]
    fn reduce_output_folds_to_the_latest_aggregate() {
        use crate::schema::{type_code, SchemaColumn};

        // U128 PK + I64 group_val + I64 agg_val.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let make = |rows: &[(u128, i64, i64, i64)]| {
            let mut b = Batch::with_capacity(schema, rows.len().max(1));
            for &(pk, w, gv, av) in rows {
                b.extend_pk(pk);
                b.extend_weight(&w.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &gv.to_le_bytes());
                b.extend_col(1, &av.to_le_bytes());
                b.count += 1;
            }
            b.certify_layout(Layout::Sorted, &schema);
            Rc::new(b.into_consolidated(&schema))
        };

        let mut set = RunSet::new(1 << 20);
        set.push(make(&[(0, 1, 0, 5000)]), &schema);
        set.push(make(&[(0, -1, 0, 5000), (0, 1, 0, 10000)]), &schema);
        set.push(make(&[(0, -1, 0, 10000), (0, 1, 0, 15000)]), &schema);

        let folded = set.fold_to_single(&schema).expect("the latest aggregate survives");
        assert_eq!(folded.count, 1, "only the latest aggregate remains");
        assert_eq!(folded.get_pk(0), 0);
        assert_eq!(folded.get_weight(0), 1);
        let agg = i64::from_le_bytes(folded.get_col_ptr(0, 1, 8).try_into().unwrap());
        assert_eq!(agg, 15000);
    }

    #[test]
    fn byte_total_tracks_pushes_and_folds() {
        let schema = make_schema_u64_i64();
        let mut set = RunSet::new(1 << 20);
        assert_eq!(set.bytes(), 0);
        push(&mut set, &schema, &[(1, 1, 10)]);
        let one = set.bytes();
        assert!(one > 0);
        push(&mut set, &schema, &[(2, 1, 20)]);
        assert_eq!(set.bytes(), 2 * one, "pushes accumulate");

        set.fold(&schema);
        assert_eq!(
            set.bytes(),
            set.runs()[0].total_bytes(),
            "fold re-derives from the merged run"
        );
    }
}
