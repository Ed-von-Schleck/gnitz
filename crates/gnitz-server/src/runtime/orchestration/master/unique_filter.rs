//! Master-side unique-index filter cache: for each `(table_id, col_indices)`,
//! the OPK spans known to exist in that unique index. `plan_unique_checks`
//! consults it to skip an occupancy broadcast whose every span is provably
//! absent.
//!
//! The filter must never prove a present span absent; every other inaccuracy
//! costs one spurious broadcast.

use rustc_hash::FxHashSet;

use super::train::drain_rows;
use super::*;
use gnitz_store::schema::key::{probe_key, PkBuf};
use gnitz_store::schema::KeySpec;

/// Spans tracked per filter before it disables itself: `FxHashSet<u64>`'s
/// 2^23-bucket table at its 7/8 load factor, the largest count that never grows
/// to the next power of two.
pub(super) const UNIQUE_FILTER_CAP: usize = (1 << 23) * 7 / 8;

pub(crate) struct UniqueFilter {
    /// `probe_key` fingerprints of the spans known present, or `None` once the
    /// filter capped.
    values: Option<FxHashSet<u64>>,
    cap: usize,
    /// Whether `values` holds every committed span, and so may prove absence.
    warm: bool,
}

impl UniqueFilter {
    pub(super) fn new() -> Self {
        Self::with_cap(UNIQUE_FILTER_CAP)
    }

    pub(super) fn with_cap(cap: usize) -> Self {
        UniqueFilter {
            values: Some(FxHashSet::default()),
            cap,
            warm: false,
        }
    }

    #[cfg(test)]
    pub(super) fn capped(&self) -> bool {
        self.values.is_none()
    }

    pub(super) fn is_warm(&self) -> bool {
        self.warm
    }

    /// Declares the span set complete, so the filter may now prove absence.
    pub(super) fn mark_warm(&mut self) {
        self.warm = true;
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

    /// True when every span in `spans` is definitely absent.
    pub(super) fn proves_all_absent<'k>(&self, mut spans: impl Iterator<Item = &'k [u8]>) -> bool {
        self.warm && spans.all(|s| !self.may_contain(s))
    }

    /// A fingerprint probe: a collision, and every span once the filter has
    /// capped, answers true.
    fn may_contain(&self, span: &[u8]) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.contains(&probe_key(span)))
    }

    /// Distinct spans tracked — zero once capped.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.values.as_ref().map_or(0, |values| values.len())
    }
}

/// Insert the indexed span of every live row of `batch` into `filter`; a NULL
/// in an indexed column means no span (`key_bytes` → false) and no occupancy.
pub(super) fn extract_into_filter(
    filter: &mut UniqueFilter,
    batch: &gnitz_store::storage::MemBatch<'_>,
    spec: &KeySpec,
) {
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
    // -----------------------------------------------------------------------
    // Unique-index filter
    // -----------------------------------------------------------------------

    /// True if every span in `spans` is definitely absent from the filter for
    /// `(table_id, cols)`.
    pub(super) fn unique_filter_all_absent<'k>(
        &self,
        table_id: u64,
        cols: PkColList,
        spans: impl Iterator<Item = &'k [u8]>,
    ) -> bool {
        self.unique_filters
            .borrow()
            .get(&(table_id, cols))
            .is_some_and(|f| f.proves_all_absent(spans))
    }

    /// Record every indexed span of a successfully-flushed `batch` on
    /// `table_id`. A filter that is not yet warm is ingested into too, so a span
    /// committed during a warm-up window is not lost.
    pub(crate) fn unique_filter_ingest_batch(&self, table_id: u64, batch: &Batch) {
        let Some(relation) = self.cat().registry.relation(table_id) else {
            return;
        };
        let mb = batch.as_mem_batch();
        let mut filters = self.unique_filters.borrow_mut();
        for ic in relation.unique_indexes_to_check() {
            let Some(filter) = filters.get_mut(&(table_id, ic.cols())) else {
                continue; // no entry yet; warmup builds it
            };
            extract_into_filter(filter, &mb, &ic.key_spec());
        }
    }

    /// Drop every filter entry for `table_id`; lazy warm-up rebuilds them.
    pub(crate) fn unique_filter_invalidate_table(&self, table_id: u64) {
        self.unique_filters.borrow_mut().retain(|&(t, _), _| t != table_id);
    }

    /// Drop the filter for one index, leaving the table's other filters.
    pub(crate) fn unique_filter_remove(&self, owner_id: u64, cols: PkColList) {
        self.unique_filters.borrow_mut().remove(&(owner_id, cols));
    }

    /// Publish the filter the CREATE-time pre-flight built: it scanned every
    /// worker under the catalog write lock, so the set is complete.
    pub(crate) fn unique_filter_seed(&self, table_id: u64, cols: PkColList, mut filter: UniqueFilter) {
        filter.mark_warm();
        self.unique_filters.borrow_mut().insert((table_id, cols), filter);
    }

    /// Warm every not-yet-warm filter of `uniques`, the unique indexes a write
    /// to `table_id` checks, from a scan of the table, one reply frame at a time.
    pub(super) async fn ensure_unique_filters_warm(
        &self,
        table_id: u64,
        uniques: &[(PkColList, SchemaDescriptor, KeySpec)],
    ) -> Result<(), WireFault> {
        let missing: Vec<(PkColList, KeySpec)> = {
            let mut filters = self.unique_filters.borrow_mut();
            let missing: Vec<(PkColList, KeySpec)> = uniques
                .iter()
                .filter(|(cols, ..)| !filters.get(&(table_id, *cols)).is_some_and(UniqueFilter::is_warm))
                .map(|&(cols, _, spec)| (cols, spec))
                .collect();
            for &(cols, _) in &missing {
                filters.entry((table_id, cols)).or_insert_with(UniqueFilter::new);
            }
            missing
        };
        if missing.is_empty() {
            return Ok(());
        }
        let schema = self.schema_desc_for(table_id);

        let lease = self
            .scan(DirectGroup {
                template: wire::WireMsg {
                    target_id: table_id,
                    ..Default::default()
                },
                ..DirectGroup::new(SalMessageKind::Scan)
            })
            .await?;

        drain_rows(&lease, &schema, |mb| {
            let mut filters = self.unique_filters.borrow_mut();
            for (cols, spec) in &missing {
                if let Some(filter) = filters.get_mut(&(table_id, *cols)) {
                    extract_into_filter(filter, mb, spec);
                }
            }
            Ok(())
        })
        .await?;

        let mut filters = self.unique_filters.borrow_mut();
        for &(cols, _) in &missing {
            if let Some(f) = filters.get_mut(&(table_id, cols)) {
                f.mark_warm();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/unique_filter.rs"]
mod tests;
