//! Master-side unique-index filter cache: the `UniqueFilter` / `WarmupGuard` /
//! `UniqueIndexDesc` types, `extract_into_filter`, and the verbs that warm,
//! query, seed, and invalidate the per-`(table, col_indices)` filters (the
//! preflight seed path shares the types).

use rustc_hash::FxHashSet;

use super::train::drain_index_scan;
use super::*;
use gnitz_store::relation::Relation;
use gnitz_store::schema::key::{probe_key, PkBuf};
use gnitz_store::schema::IndexKeySpec;

// For each `(table_id, col_indices)` we keep a set of the OPK spans known to
// exist in that unique index. The U-SEC rule (`plan_unique_checks`)
// consults it before building a broadcast: if every new span is definitely
// absent, that index's broadcast is skipped entirely.
//
// Correctness invariants:
//   1. A span is added only AFTER fsync confirms the owning batch is durable,
//      so an in-flight insert never creates a false "present" entry.
//   2. On any flush error the affected table's filters are dropped from the map
//      and lazily re-warmed from authoritative worker state.
//   3. Warmup is lazy: the first query for an unwarmed key scans the table
//      (`ensure_unique_filters_warm`) and extracts the indexed spans.
//   4. Deletes are NOT removed — they leave stale "possibly present" entries
//      that cause harmless fall-through broadcasts. Accumulation is bounded by
//      `UNIQUE_FILTER_CAP`: past it the filter caps and disables itself until
//      invalidated.
//   5. Query, warmup and ingest may interleave freely. A cold filter still
//      accepts ingestion, so a span committed during the warmup scan is never
//      lost, and a second warmup of the same key finds the entry already
//      present and returns without building a rival filter. The one forbidden
//      state is a span missing from a filter marked warm.

/// Maximum number of spans tracked per `(table_id, col_indices)` filter; a
/// filter that would exceed it disables itself. This is the largest count that
/// fits `FxHashSet<u64>`'s 2^23-bucket table at its 7/8 load factor, so the set
/// never grows to the next power of two: ≈72 MiB per maxed filter, against
/// ≈144 MiB (and a transient 216 MiB across the rehash) one entry higher.
/// Lowering it shrinks the broadcast-skip reach — every unique index past the
/// cap reverts to always-broadcast on the insert hot path.
pub(super) const UNIQUE_FILTER_CAP: usize = 7_340_032;

pub(crate) struct UniqueFilter {
    /// `probe_key` fingerprints of the OPK leading-key spans known present in
    /// the index, or `None` once the filter exceeded `cap` — dropped whole,
    /// never truncated, after which every query falls through to the
    /// broadcast.
    pub(super) values: Option<FxHashSet<u64>>,
    /// Maximum distinct values tracked: `UNIQUE_FILTER_CAP` in production,
    /// parameterizable so tests exercise the cap discipline cheaply.
    pub(super) cap: usize,
    /// False until the warmup scan has fully populated `values`. While
    /// false the filter still accepts ingestion (so keys committed during
    /// the scan window are not lost) but `unique_filter_all_absent`
    /// refuses the broadcast-skip shortcut — an empty/partial filter must
    /// never be trusted to prove absence.
    pub(super) warm: bool,
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

    /// True once the filter has exceeded `cap` and disabled itself.
    pub(super) fn capped(&self) -> bool {
        self.values.is_none()
    }

    /// On overflow the set is dropped WHOLE, never truncated: a partial set
    /// would prove "absent" for a present key — a uniqueness hole.
    pub(super) fn insert(&mut self, span: &[u8]) {
        let Some(values) = self.values.as_mut() else {
            return;
        };
        values.insert(probe_key(span));
        if values.len() > self.cap {
            self.values = None;
        }
    }

    /// False iff `span` is definitely absent — the only answer this filter
    /// owes. A fingerprint collision, and every span once the filter caps,
    /// reports true: one spurious broadcast, exactly what a true hit costs.
    /// There is no false negative, so a present span is never proven absent.
    pub(super) fn may_contain(&self, span: &[u8]) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.contains(&probe_key(span)))
    }

    /// Distinct spans tracked — zero once capped.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.values.as_ref().map_or(0, |values| values.len())
    }
}

/// RAII guard that removes cold UniqueFilter entries if the warmup future is
/// dropped before it completes.
pub(super) struct WarmupGuard<'d> {
    pub(super) disp: &'d MasterDispatcher,
    pub(super) table_id: i64,
    /// The cold filters this warmup created, by the `unique_filters` map key, so
    /// the drop handler removes exactly those.
    pub(super) missing: Vec<UniqueIndexDesc>,
    pub(super) disarmed: bool,
}

impl Drop for WarmupGuard<'_> {
    fn drop(&mut self) {
        if !self.disarmed {
            for d in &self.missing {
                self.disp.unique_filter_remove(self.table_id, d.cols);
            }
        }
    }
}

/// Column-extraction descriptor for one unique index, `Copy` so the warmup scan
/// can carry it across `await` points the circuit-list borrow cannot cross.
/// `cols` keys the `unique_filters` map; `spec` is the span encode plan.
#[derive(Clone, Copy)]
pub(super) struct UniqueIndexDesc {
    pub(super) cols: PkColList,
    pub(super) spec: IndexKeySpec,
}

/// Walk every positive-weight, non-null row of `batch` and insert the indexed
/// columns' OPK leading-key span into `filter`. A row with a NULL in any
/// indexed column is skipped (`key_bytes` → false), sharing the NULL-distinct
/// key contract with the CREATE-time validator and the projection.
///
/// Owns the capped check for every caller: a capped filter takes no rows, so
/// the walk neither starts nor continues past the row that caps it.
pub(super) fn extract_into_filter(
    filter: &mut UniqueFilter,
    batch: &gnitz_store::storage::MemBatch<'_>,
    spec: &IndexKeySpec,
) {
    if filter.capped() {
        return;
    }
    let mut keybuf = PkBuf::zeroed(0);
    for row in 0..batch.len() {
        if batch.get_weight(row) <= 0 {
            continue;
        }
        if !spec.key_bytes(batch, row, &mut keybuf) {
            continue;
        }
        filter.insert(keybuf.pk_bytes());
        if filter.capped() {
            return;
        }
    }
}

impl MasterDispatcher {
    // -----------------------------------------------------------------------
    // Unique-index filter
    // -----------------------------------------------------------------------

    /// True if every span in `spans` is definitely absent from the filter for
    /// `(table_id, cols)`. Returns false if the filter is not warm (caller is
    /// expected to warm it first) or may contain any span — which a capped
    /// filter always does. On false, caller must fall through to the occupancy
    /// broadcast.
    pub(super) fn unique_filter_all_absent<'k>(
        &self,
        table_id: i64,
        cols: PkColList,
        mut spans: impl Iterator<Item = &'k [u8]>,
    ) -> bool {
        let filters = self.unique_filters.borrow();
        let filter = match filters.get(&(table_id, cols)) {
            Some(f) => f,
            None => return false,
        };
        filter.warm && spans.all(|s| !filter.may_contain(s))
    }

    /// Record every unique-index value from a successfully-flushed `batch` on
    /// `table_id` into the corresponding filters. No-op for filters that are not
    /// yet warm (warmup will pick them up), and for non-unique circuits. Walks
    /// the circuit list in place: this runs per live group per commit.
    pub(crate) fn unique_filter_ingest_batch(&self, table_id: i64, batch: &Batch) {
        let Some(relation) = self
            .cat()
            .registry()
            .relation(table_id)
            .filter(|r| r.has_unique_index())
        else {
            return;
        };
        let mb = batch.as_mem_batch();
        let mut filters = self.unique_filters.borrow_mut();
        for ic in relation.indexes().iter().filter(|ic| ic.is_unique()) {
            let Some(filter) = filters.get_mut(&(table_id, ic.cols())) else {
                continue; // not warm — warmup will pick this up
            };
            extract_into_filter(filter, &mb, &ic.key_spec());
        }
    }

    /// Drop every filter entry for `table_id`. Called on flush errors
    /// (where filter state may be out of sync with workers) and on DDL
    /// changes (DROP TABLE, DROP/CREATE INDEX).
    pub(crate) fn unique_filter_invalidate_table(&self, table_id: i64) {
        self.unique_filters.borrow_mut().retain(|&(t, _), _| t != table_id);
    }

    /// Remove the unique-filter entry for a single `(owner_table_id, cols)`
    /// pair. Called on DROP INDEX so subsequent INSERTs re-trigger warmup for
    /// the now-absent index while leaving unrelated filters on the same table; a
    /// non-existent key (e.g. a non-unique FK index) is a harmless no-op.
    pub(crate) fn unique_filter_remove(&self, owner_id: i64, cols: PkColList) {
        self.unique_filters.borrow_mut().remove(&(owner_id, cols));
    }

    /// Publish the `(table_id, cols)` filter the CREATE-time pre-flight
    /// built under the catalog write lock, marking it warm so the first INSERT
    /// skips `ensure_unique_filters_warm`. A pre-flight that overflowed hands
    /// over an already-capped filter, so `unique_filter_all_absent` always
    /// falls through to the broadcast — the same steady state the lazy warmup
    /// converges to, without a redundant full-cluster scan on the first
    /// INSERT. Symmetric counterpart of `unique_filter_remove`.
    pub(crate) fn unique_filter_seed(&self, table_id: i64, cols: PkColList, mut filter: UniqueFilter) {
        filter.warm = true; // pre-flight scanned every worker under the write lock
        self.unique_filters.borrow_mut().insert((table_id, cols), filter);
    }

    /// Warm every cold unique filter on `table_id` from a scan of the table, one
    /// reply frame at a time.
    pub(super) async fn ensure_unique_filters_warm(&self, table_id: i64) -> Result<(), WireFault> {
        let mut guard = {
            let mut filters = self.unique_filters.borrow_mut();
            // Tested before it is built: the steady state is that every filter is
            // already warm, and that answer costs no allocation.
            let cold = |ic: &gnitz_store::relation::SecondaryIndex| {
                ic.is_unique() && !filters.contains_key(&(table_id, ic.cols()))
            };
            let circuits = self
                .cat()
                .registry()
                .relation(table_id)
                .map_or(&[][..], Relation::indexes);
            if !circuits.iter().any(&cold) {
                return Ok(());
            }
            let missing: Vec<UniqueIndexDesc> = circuits
                .iter()
                .filter(|ic| cold(ic))
                .map(|ic| UniqueIndexDesc { cols: ic.cols(), spec: ic.key_spec() })
                .collect();
            for d in &missing {
                filters.insert((table_id, d.cols), UniqueFilter::new());
            }
            WarmupGuard {
                disp: self,
                table_id,
                missing,
                disarmed: false,
            }
        };
        let schema = self.schema_desc_for(table_id);
        // The reader holds the table's current schema, so no worker ships a block.
        let schema_version = self.cat().get_schema_version(table_id);

        let lease = self
            .scan(DirectGroup {
                template: wire::WireMsg {
                    target_id: table_id as u64,
                    flags: WireFlags { schema_version, ..Default::default() },
                    ..Default::default()
                },
                ..DirectGroup::new(SalMessageKind::Scan)
            })
            .await?;

        drain_index_scan(&lease, "scan", &schema, |mb| {
            let mut filters = self.unique_filters.borrow_mut();
            for d in &guard.missing {
                if let Some(filter) = filters.get_mut(&(table_id, d.cols)) {
                    extract_into_filter(filter, mb, &d.spec);
                }
            }
            Ok(())
        })
        .await?;

        // Fully populated → mark warm so the broadcast-skip shortcut may trust
        // them, and disarm the guard so its Drop leaves them in place.
        let mut filters = self.unique_filters.borrow_mut();
        for d in &guard.missing {
            if let Some(f) = filters.get_mut(&(table_id, d.cols)) {
                f.warm = true;
            }
        }
        guard.disarmed = true;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/unique_filter.rs"]
mod tests;
