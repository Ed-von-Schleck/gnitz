//! Master-side unique-index filters: for each `(table_id, col_indices)`, the
//! spans that unique index holds, so a write probes only the spans it cannot
//! prove absent.

use rustc_hash::FxHashSet;

use super::*;
use gnitz_zset::schema::key::{probe_key, PkBuf};
use gnitz_zset::schema::KeySpec;

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

    /// A fingerprint probe: a collision, and every span once the filter has
    /// capped, answers true.
    pub(super) fn may_contain(&self, span: &[u8]) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.contains(&probe_key(span)))
    }
}

/// Insert the indexed span of every live row of `batch` into `filter`; a NULL
/// in an indexed column means no span (`key_bytes` → false) and no occupancy.
pub(super) fn extract_into_filter(filter: &mut UniqueFilter, batch: &gnitz_zset::repr::MemBatch<'_>, spec: &KeySpec) {
    let mut keybuf = PkBuf::zeroed(0);
    for row in 0..batch.len() {
        if batch.get_weight(row) <= 0 {
            continue;
        }
        if !spec.key_bytes(batch, row, &mut keybuf) {
            continue;
        }
        if !filter.insert(keybuf.pk_bytes()) {
            return;
        }
    }
}

impl MasterDispatcher {
    /// Keep in `order` the entries whose `span` the filter for
    /// `(table_id, cols)` cannot prove absent: all of them where it has none.
    pub(super) fn unique_filter_retain_possible<'k>(
        &self,
        table_id: u64,
        cols: PkColList,
        order: &mut Vec<u32>,
        span: impl Fn(u32) -> &'k [u8],
    ) {
        if let Some(f) = self.unique_filters.borrow().get(&(table_id, cols)) {
            order.retain(|&x| f.may_contain(span(x)));
        }
    }

    /// Record every indexed span of a durable `batch` on `table_id`.
    pub(crate) fn unique_filter_ingest_batch(&self, table_id: u64, batch: &Batch) {
        let Some(relation) = self.cat().registry.relation(table_id) else {
            return;
        };
        let mb = batch.as_mem_batch();
        let mut filters = self.unique_filters.borrow_mut();
        for ic in relation.unique_indexes_to_check() {
            if let Some(filter) = filters.get_mut(&(table_id, ic.cols())) {
                extract_into_filter(filter, &mb, &ic.key_spec());
            }
        }
    }

    /// Drop every filter of `table_id`.
    pub(crate) fn unique_filter_invalidate_table(&self, table_id: u64) {
        self.unique_filters.borrow_mut().retain(|&(t, _), _| t != table_id);
    }

    /// Drop the filter for one index, leaving the table's other filters.
    pub(crate) fn unique_filter_remove(&self, owner_id: u64, cols: PkColList) {
        self.unique_filters.borrow_mut().remove(&(owner_id, cols));
    }

    /// Publish a filter holding every span of the index on `cols`.
    pub(crate) fn unique_filter_seed(&self, table_id: u64, cols: PkColList, filter: UniqueFilter) {
        self.unique_filters.borrow_mut().insert((table_id, cols), filter);
    }

    /// Build the filter of each index of `uniques` that has none, from the
    /// spans the workers' index stores hold.
    pub(super) async fn ensure_unique_filters_warm(
        &self,
        table_id: u64,
        uniques: &[(PkColList, SchemaDescriptor, KeySpec)],
    ) -> Result<(), WireFault> {
        for &(cols, _, spec) in uniques {
            if self.unique_filters.borrow().contains_key(&(table_id, cols)) {
                continue;
            }
            let lease = self.scan(Read::KeySpans { tid: table_id, cols }).await?;
            let (span_schema, mut filter) = (spec.span_schema(), UniqueFilter::new());
            // A capped filter reads no further; the lease drop discards the rest.
            'train: while let Some(frame) = lease.next().await? {
                let spans = frame.rows(&span_schema);
                for i in 0..spans.len() {
                    if !filter.insert(spans.get_pk_bytes(i)) {
                        break 'train;
                    }
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
