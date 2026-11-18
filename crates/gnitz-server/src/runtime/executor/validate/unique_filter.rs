//! Unique-index filters: for each `(table_id, col_indices)`, the spans that
//! unique index holds, so a write probes only the spans it cannot prove absent.

use rustc_hash::FxHashSet;

use super::*;
use gnitz_wire::PkKeys;
use gnitz_zset::algebra::KeySpec;
use gnitz_zset::schema::probe_key;

/// Spans a filter tracks before it disables itself; a filter that churned past
/// it is rebuilt from the live index once, under half of it.
pub(super) const UNIQUE_FILTER_CAP: usize = 7 << 20;

pub(crate) enum UniqueFilter {
    /// `probe_key` fingerprints of every span the index holds.
    Live(FxHashSet<u64>),
    /// An admitted write capped it: it proves nothing until the next write to
    /// its index rebuilds it from the spans the index holds.
    Churned,
    /// It capped while it was built: it proves nothing for the boot.
    Capped,
}

impl UniqueFilter {
    pub(super) fn new() -> Self {
        UniqueFilter::Live(FxHashSet::default())
    }

    /// Records `span`; false once a set of `cap` spans has been dropped,
    /// leaving the filter [`Self::Capped`].
    pub(super) fn insert(&mut self, span: &[u8], cap: usize) -> bool {
        let UniqueFilter::Live(values) = self else {
            return false;
        };
        let key = probe_key(span);
        // Growing past the cap would copy the whole table for a set this call drops.
        if values.len() >= cap && !values.contains(&key) {
            *self = UniqueFilter::Capped;
            return false;
        }
        values.insert(key);
        true
    }

    /// Records every span of `spans` up to the first `cap` refuses; false once
    /// the filter has capped.
    pub(super) fn insert_all<'s>(&mut self, spans: impl IntoIterator<Item = &'s [u8]>, cap: usize) -> bool {
        spans.into_iter().all(|span| self.insert(span, cap))
    }

    /// A fingerprint probe: a collision, and every span once the filter has
    /// capped, answers true.
    pub(super) fn may_contain(&self, span: &[u8]) -> bool {
        match self {
            UniqueFilter::Live(values) => values.contains(&probe_key(span)),
            UniqueFilter::Churned | UniqueFilter::Capped => true,
        }
    }
}

/// The spans a write's surviving rows hold in the unique index on `cols` of
/// `tid`, taken where its filter proved one of them absent: what the filter
/// holds once the write is admitted.
pub(super) struct SpanClaim {
    pub(super) tid: u64,
    pub(super) cols: PkColList,
    pub(super) key_size: usize,
    /// The spans back to back, `key_size` bytes each, in row order.
    pub(super) spans: Vec<u8>,
}

impl Checks {
    /// Record the spans of a write its validation admitted. Per PK, the row
    /// validation finds surviving is the row the store keeps, so a filter holds
    /// every span a committed row holds; it holds more after a delete, and
    /// after a write its committer fails.
    ///
    /// A claim that caps its filter marks it churned; the filter stays, proving
    /// nothing, until the next write to its index rebuilds it.
    ///
    /// No filter is built or dropped between a write's validation and its
    /// commit: a warm scan runs under the table's write guard, which the write
    /// holds until it is laid out, and a DDL removes a filter under the catalog
    /// write guard, which the write's read guard keeps out. So the filter each
    /// claim was taken against is still there.
    pub(super) fn unique_filters_admit(&self, claims: &[SpanClaim]) {
        let mut filters = self.unique_filters.borrow_mut();
        for c in claims {
            let filter = filters.get_mut(&(c.tid, c.cols)).expect("a claim's filter stays");
            if !filter.insert_all(c.spans.chunks_exact(c.key_size), self.unique_filter_cap) {
                *filter = UniqueFilter::Churned;
            }
        }
    }

    /// Keep in `keys` the spans the filter for `(table_id, cols)` cannot prove
    /// absent, all of them where it has none; true where it dropped one, so the
    /// write holds a span its filter does not.
    pub(super) fn unique_filter_retain_possible(&self, table_id: u64, cols: PkColList, keys: &mut PkKeys) -> bool {
        let filters = self.unique_filters.borrow();
        let Some(f) = filters.get(&(table_id, cols)) else {
            return false;
        };
        let before = keys.len();
        keys.retain(|k| f.may_contain(k));
        keys.len() != before
    }

    /// Drop the filter for one index, leaving the table's other filters.
    pub(in crate::runtime::executor) fn unique_filter_remove(&self, owner_id: u64, cols: PkColList) {
        self.unique_filters.borrow_mut().remove(&(owner_id, cols));
    }

    /// Publish a filter holding every span of the index on `cols`.
    pub(super) fn unique_filter_seed(&self, table_id: u64, cols: PkColList, filter: UniqueFilter) {
        self.unique_filters.borrow_mut().insert((table_id, cols), filter);
    }

    /// Build the filter of each index of `uniques` that has none or whose
    /// filter churned, from the spans the workers' index stores hold.
    ///
    /// A churned filter is rebuilt under half the cap, so the scan of at most
    /// half the cap is paid once per at least half the cap of spans admitted
    /// after it. A filter that caps while it is built stays capped for the boot.
    pub(super) async fn ensure_unique_filters_warm(
        &self,
        disp: &MasterDispatcher,
        table_id: u64,
        uniques: &[(PkColList, KeySpec)],
    ) -> Result<(), WireFault> {
        for &(cols, ref spec) in uniques {
            let rewarm = match self.unique_filters.borrow().get(&(table_id, cols)) {
                None => false,
                Some(UniqueFilter::Churned) => true,
                Some(_) => continue,
            };
            let mut lease = disp.scan(Read::KeySpans { tid: table_id, cols, claim: false }).await?;
            let train = lease.next_train().await;
            let cap = if rewarm {
                self.unique_filter_cap / 2
            } else {
                self.unique_filter_cap
            };
            let (span_schema, mut filter) = (spec.span_schema(), UniqueFilter::new());
            // A capped filter reads no further; the lease drop discards the rest.
            while let Some(frame) = train.next().await? {
                let spans = frame.rows(&span_schema).pk().chunks_exact(span_schema.pk_stride());
                if !filter.insert_all(spans, cap) {
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
