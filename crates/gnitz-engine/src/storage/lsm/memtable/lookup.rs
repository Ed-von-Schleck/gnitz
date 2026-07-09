//! Point lookup + Bloom probe for [`MemTable`]: the per-run exact-match scan,
//! PK net-weight lookups, and the positive-payload row locator used by the
//! retract path.

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
    (start..run.count).take_while(move |&lo| columnar::pk_bytes_eq(run.get_pk_bytes(lo), key))
}

impl MemTable {
    /// Bloom probe for a PK by its OPK bytes. Keyed identically to the
    /// insert side (`pack_pk_be` of the OPK region), so it never produces a
    /// false negative for any PK type. `opk_key` is exactly `pk_stride` bytes.
    /// The first probe builds the filter from all live runs (see the field
    /// doc); later upserts maintain it incrementally.
    pub fn may_contain_pk(&self, opk_key: &[u8]) -> bool {
        let bloom = self.bloom.get_or_init(|| {
            let mut bloom = super::runs::new_bloom(self.bloom_capacity());
            for run in &self.runs {
                super::bloom_add_batch(&mut bloom, run);
            }
            bloom
        });
        bloom.may_contain(pack_pk_be(opk_key))
    }
    /// Look up a PK across all sorted runs by its OPK `key` bytes. Returns
    /// `(net_weight, first_match, row_count)` where `first_match` is the
    /// `(run, row)` location of the first matching row (in run order) and
    /// `row_count` the number of matching rows across all runs. The universal
    /// lookup at every PK width — `find_lower_bound_bytes` is a raw memcmp
    /// search on the OPK regions. `key` is exactly `pk_stride` OPK bytes.
    pub fn lookup_pk_bytes(&self, key: &[u8]) -> (i64, Option<(usize, usize)>, usize) {
        let mut total_w: i64 = 0;
        let mut row_count: usize = 0;
        let mut first: Option<(usize, usize)> = None;

        for (ri, run) in self.runs.iter().enumerate() {
            for lo in run_pk_match_rows(run, key) {
                total_w += run.get_weight(lo);
                if first.is_none() {
                    first = Some((ri, lo));
                }
                row_count += 1;
            }
        }

        (total_w, first, row_count)
    }
}
