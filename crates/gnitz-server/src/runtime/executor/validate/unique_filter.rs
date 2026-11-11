//! Unique-index filters: for each `(table_id, col_indices)`, the spans that
//! unique index holds, so a write probes only the spans it cannot prove absent.

use rustc_hash::FxHashSet;

use super::*;
use gnitz_wire::PkKeys;
use gnitz_zset::algebra::KeySpec;
use gnitz_zset::schema::probe_key;

/// Spans a filter tracks before it disables itself.
const UNIQUE_FILTER_CAP: usize = 7 << 20;

pub(crate) struct UniqueFilter {
    /// `probe_key` fingerprints of every span the index holds, or `None` once
    /// the filter capped.
    values: Option<FxHashSet<u64>>,
    /// [`UNIQUE_FILTER_CAP`], held per filter so a test can reach it in a few spans.
    cap: usize,
}

impl UniqueFilter {
    pub(super) fn new() -> Self {
        UniqueFilter {
            values: Some(FxHashSet::default()),
            cap: UNIQUE_FILTER_CAP,
        }
    }

    /// Records `span`; false once the cap has dropped the set and disabled the
    /// filter.
    pub(super) fn insert(&mut self, span: &[u8]) -> bool {
        let Some(values) = self.values.as_mut() else {
            return false;
        };
        let key = probe_key(span);
        // Growing past the cap would copy the whole table for a set this call drops.
        if values.len() >= self.cap && !values.contains(&key) {
            self.values = None;
            return false;
        }
        values.insert(key);
        true
    }

    /// Records every span of `spans` up to the first the cap refuses; false
    /// once the filter has capped.
    pub(super) fn insert_all<'s>(&mut self, spans: impl IntoIterator<Item = &'s [u8]>) -> bool {
        spans.into_iter().all(|span| self.insert(span))
    }

    /// A fingerprint probe: a collision, and every span once the filter has
    /// capped, answers true.
    pub(super) fn may_contain(&self, span: &[u8]) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.contains(&probe_key(span)))
    }
}

/// The spans a write's surviving rows hold in the unique index on `cols` of
/// `tid` that its filter proved absent: what the write adds to the filter if
/// it is admitted.
pub(super) struct SpanClaim {
    pub(super) tid: u64,
    pub(super) cols: PkColList,
    pub(super) absent: PkKeys,
}

impl Checks {
    /// Record the spans of a write its validation admitted. Per PK, the row
    /// validation finds surviving is the row the store keeps, so a filter holds
    /// every span a committed row holds; it holds more after a delete, and
    /// after a write its committer fails.
    ///
    /// No filter is built or dropped between a write's validation and its
    /// commit: a warm scan runs under the table's write guard, which the write
    /// holds until it is laid out, and a DDL seeds or removes a filter under
    /// the catalog write guard, which the write's read guard keeps out. So the
    /// filter each claim was taken against is still there.
    pub(super) fn unique_filters_admit(&self, claims: &[SpanClaim]) {
        let mut filters = self.unique_filters.borrow_mut();
        for c in claims {
            let filter = filters.get_mut(&(c.tid, c.cols)).expect("a claim's filter stays");
            filter.insert_all(c.absent.iter());
        }
    }

    /// Keep in `keys` the spans the filter for `(table_id, cols)` cannot prove
    /// absent, all of them where it has none, and answer the ones it dropped
    /// as the write's claim on the filter; `None` where it dropped none.
    pub(super) fn unique_filter_retain_possible(
        &self,
        table_id: u64,
        cols: PkColList,
        keys: &mut PkKeys,
    ) -> Option<SpanClaim> {
        let filters = self.unique_filters.borrow();
        let f = filters.get(&(table_id, cols))?;
        let mut absent = Vec::new();
        keys.retain(|k| {
            let possible = f.may_contain(k);
            if !possible {
                absent.extend_from_slice(k);
            }
            possible
        });
        (!absent.is_empty()).then(|| SpanClaim {
            tid: table_id,
            cols,
            absent: PkKeys::from_sorted(keys.stride(), absent),
        })
    }

    /// Drop the filter for one index, leaving the table's other filters.
    pub(in crate::runtime::executor) fn unique_filter_remove(&self, owner_id: u64, cols: PkColList) {
        self.unique_filters.borrow_mut().remove(&(owner_id, cols));
    }

    /// Publish a filter holding every span of the index on `cols`.
    pub(in crate::runtime::executor) fn unique_filter_seed(
        &self,
        table_id: u64,
        cols: PkColList,
        filter: UniqueFilter,
    ) {
        self.unique_filters.borrow_mut().insert((table_id, cols), filter);
    }

    /// Build the filter of each index of `uniques` that has none, from the
    /// spans the workers' index stores hold.
    pub(super) async fn ensure_unique_filters_warm(
        &self,
        disp: &MasterDispatcher,
        table_id: u64,
        uniques: &[(PkColList, KeySpec)],
    ) -> Result<(), WireFault> {
        for &(cols, ref spec) in uniques {
            if self.unique_filters.borrow().contains_key(&(table_id, cols)) {
                continue;
            }
            let lease = disp.scan(Read::KeySpans { tid: table_id, cols, claim: false }).await?;
            let (span_schema, mut filter) = (spec.span_schema(), UniqueFilter::new());
            // A capped filter reads no further; the lease drop discards the rest.
            while let Some(frame) = lease.next().await? {
                let spans = frame.rows(&span_schema).pk().chunks_exact(span_schema.pk_stride());
                if !filter.insert_all(spans) {
                    break;
                }
            }
            self.unique_filter_seed(table_id, cols, filter);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/unique_filter.rs"]
mod tests;
