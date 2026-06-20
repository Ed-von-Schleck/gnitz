//! Point lookup + Bloom probe for [`MemTable`]: the per-run exact-match scan,
//! PK net-weight lookups, and the positive-payload row locator used by the
//! retract path.

use std::cmp::Ordering;

use super::super::batch::Batch;
use super::super::columnar;
use super::super::merge::pack_pk_be;
use super::MemTable;

/// Start index of the first exact-match candidate for OPK `key` in `run`.
/// Returns `run.count` when `key` falls outside the run's `[min, max]` range
/// (so the caller's `get_pk_bytes(lo) == key` loop runs zero times). After the
/// OPK-at-rest flip this is a raw `memcmp` binary search at every PK width —
/// no schema, no pk_is_fast dispatch. `key` is exactly `pk_stride` OPK bytes.
///
/// Named `exact_match_start` rather than `lower_bound` because it returns
/// `run.count` (past-the-end) for out-of-range keys, which is not standard
/// lower-bound behaviour; callers always use it as the start of an exact-match
/// scan, never for range queries.
fn run_exact_match_start_bytes(run: &Batch, key: &[u8]) -> usize {
    if run.count == 0 {
        return 0;
    }
    // O(1) range reject before the binary search (cache-miss avoidance).
    if key < run.get_pk_bytes(0) || key > run.get_pk_bytes(run.count - 1) {
        return run.count;
    }
    run.find_lower_bound_bytes(key)
}

/// Iterator over the run-row indices whose PK equals `key`. Runs are PK-sorted,
/// so exact matches form one contiguous range beginning at the lower bound;
/// iteration is lazy and stops at the first non-matching row, visiting only the
/// matching rows. Factors out the per-run exact-match scan shared by every PK
/// lookup so callers never re-derive the bound check or the index step.
pub(crate) fn run_pk_match_rows<'a>(run: &'a Batch, key: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
    let start = run_exact_match_start_bytes(run, key);
    (start..run.count).take_while(move |&lo| run.get_pk_bytes(lo) == key)
}

impl MemTable {
    /// Bloom probe for a PK by its OPK bytes. Keyed identically to the
    /// insert side (`pack_pk_be` of the OPK region), so it never produces a
    /// false negative for any PK type. `opk_key` is exactly `pk_stride` bytes.
    pub fn may_contain_pk(&self, opk_key: &[u8]) -> bool {
        self.bloom.may_contain(pack_pk_be(opk_key))
    }
    /// Look up a PK across all sorted runs.
    ///
    /// Returns `(net_weight, found, row_count)` where `row_count` is the
    /// number of memtable rows matching `key` (across all runs).
    /// Look up a PK across all sorted runs by its OPK `key` bytes. Returns
    /// `(net_weight, found, row_count)`. The universal lookup at every PK
    /// width — `find_lower_bound_bytes` is a raw memcmp search on the OPK
    /// regions. `key` is exactly `pk_stride` OPK bytes.
    pub fn lookup_pk_bytes(&mut self, key: &[u8]) -> (i64, bool, usize) {
        let mut total_w: i64 = 0;
        let mut row_count: usize = 0;
        self.found = None;

        for (ri, run) in self.runs.iter().enumerate() {
            for lo in run_pk_match_rows(run, key) {
                total_w += run.get_weight(lo);
                if self.found.is_none() {
                    self.found = Some((ri, lo));
                }
                row_count += 1;
            }
        }

        (total_w, self.found.is_some(), row_count)
    }
    /// Find the net weight for rows matching both PK (OPK `key` bytes) and
    /// full payload. `key` is exactly `pk_stride` OPK bytes.
    pub fn find_weight_for_row_bytes<S: super::super::columnar::ColumnarSource>(
        &self,
        key: &[u8],
        ref_source: &S,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        for run in &self.runs {
            for lo in run_pk_match_rows(run, key) {
                let ord = columnar::compare_rows(&self.schema, run.as_ref(), lo, ref_source, ref_row);
                if ord == Ordering::Equal {
                    total_w += run.get_weight(lo);
                }
            }
        }

        total_w
    }

    /// Find the first memtable row whose (PK, payload) has positive net weight
    /// across all runs.  Sets `found` on success.
    /// Returns true if such a row was found, false otherwise.
    ///
    /// Used by `Table::retract_pk` to locate the live row for an UPDATE+DELETE
    /// sequence where the old payload has been cancelled but the new payload
    /// is still positive.
    /// Find the first memtable row whose (PK, payload) has positive net weight
    /// across all runs, keyed by OPK `key` bytes. Sets
    /// `found` on success.
    pub fn find_positive_payload_row_bytes(&mut self, key: &[u8]) -> bool {
        self.found = None;

        // Pass 1 — one binary search per run; collect every (run, row, weight)
        // whose PK matches into `cand_scratch`. This is the only scan of the
        // runs. The buffer is reused across calls (cleared, capacity retained),
        // so this path allocates only while warming up to its high-water mark.
        //
        // Candidate count C is structurally bounded: `retract_pk_bytes` reaches
        // this only when a PK has multiple un-consolidated memtable rows, runs
        // are capped at `INLINE_CONSOLIDATE_THRESHOLD - 1` before a merge, and
        // each run is `(PK, payload)`-consolidated, so a PK adds at most a
        // retract+insert pair per run — hence `C ≤ 2R < 2·INLINE_CONSOLIDATE_-
        // THRESHOLD` (~30). C only approaches that bound for a key re-updated on
        // nearly every un-consolidated run; absent that it stays small. A
        // `ConsolidatedBatch` folds only by (PK, payload), so the synthetic
        // multi-payload case exceeds even 2R — the growable buffer absorbs any C.
        self.cand_scratch.clear();
        for (ri, run) in self.runs.iter().enumerate() {
            for lo in run_pk_match_rows(run, key) {
                self.cand_scratch.push((ri, lo, run.get_weight(lo)));
            }
        }

        // Pass 2 — group equal payloads via `== Equal` and return the first group
        // (in run order) whose weights net strictly positive. Only equality is
        // needed, matching the replaced code; the result never depends on
        // `compare_rows` being a total order. Skipping an already-grouped payload
        // (the `already_grouped` check) is load-bearing for correctness, not just
        // speed: a later candidate whose own weight is positive but whose full
        // group nets ≤ 0 must not be returned. Entries are `Copy`, so indexing
        // copies each tuple out and never holds a borrow on `cand_scratch`.
        for i in 0..self.cand_scratch.len() {
            let (ri, row, w0) = self.cand_scratch[i];
            // Skip a payload already evaluated by an earlier candidate.
            let already_grouped = self.cand_scratch[..i].iter().any(|&(rj, rowj, _)| {
                columnar::compare_rows(&self.schema, self.runs[ri].as_ref(), row, self.runs[rj].as_ref(), rowj)
                    == Ordering::Equal
            });
            if already_grouped {
                continue;
            }
            let mut net = w0;
            for k in i + 1..self.cand_scratch.len() {
                let (rk, rowk, wk) = self.cand_scratch[k];
                if columnar::compare_rows(&self.schema, self.runs[ri].as_ref(), row, self.runs[rk].as_ref(), rowk)
                    == Ordering::Equal
                {
                    net += wk;
                }
            }
            if net > 0 {
                self.found = Some((ri, row));
                return true;
            }
        }
        false
    }

    /// Pre-rewrite per-candidate re-scan, retained only as the micro-benchmark
    /// baseline (see `memtable_find_positive_payload_row_bench`). Each
    /// PK-matching candidate re-scans every run via `find_weight_for_row_bytes`;
    /// the production function above replaced this with a single grouping pass.
    /// Selects the same row, so the bench can assert the two agree before timing.
    #[cfg(test)]
    pub(super) fn find_positive_payload_row_rescan_baseline(&mut self, key: &[u8]) -> bool {
        for (ri, run) in self.runs.iter().enumerate() {
            for lo in run_pk_match_rows(run, key) {
                let net_w = self.find_weight_for_row_bytes(key, run.as_ref(), lo);
                if net_w > 0 {
                    self.found = Some((ri, lo));
                    return true;
                }
            }
        }
        self.found = None;
        false
    }
}
