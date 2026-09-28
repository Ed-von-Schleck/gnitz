//! The DDL-time pre-flight for `CREATE UNIQUE INDEX`: the fan-out
//! (`validate_unique_index_create`), the per-worker sorted-span streams
//! (`PreflightKeyStream`), their k-way merge (`merge_index_scan`) and the
//! per-key accounting it feeds (`PreflightAccumulator`).
//!
//! Nothing here is on the write path — `preflight.rs` holds that — and the only
//! state shared with it is the `UniqueFilter` this pre-flight seeds.

use super::*;

use std::ops::Range;

use super::unique_filter::UNIQUE_FILTER_CAP;
use crate::runtime::reactor::TrainFrame;
use gnitz_store::relation::Relation;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::storage::MAX_BATCH_REGIONS;

/// One worker's sorted spans in `merge_index_scan`: the frame being read, which
/// pins its ring bytes, and the byte range of its unread keys inside them.
struct PreflightKeyStream<'l> {
    lease: &'l TrainLease,
    w: usize,
    frame: Option<TrainFrame>,
    keys: Range<usize>,
}

impl PreflightKeyStream<'_> {
    /// Yield this worker's next span, pulling frames on demand. Returns
    /// `Ok(None)` once the train has ended.
    async fn next_key(&mut self, frame_schema: &SchemaDescriptor) -> Result<Option<PkBuf>, WireFault> {
        let stride = frame_schema.pk_stride();
        loop {
            if let Some(f) = &self.frame {
                if !self.keys.is_empty() {
                    let s = self.keys.start;
                    self.keys.start += stride;
                    return Ok(Some(PkBuf::from_bytes(&f.slot.bytes()[s..s + stride])));
                }
            }
            self.frame = None; // release the ring slot before waiting on the next
            let Some(f) = self.lease.next_of(self.w).await? else {
                return Ok(None);
            };
            let mut offsets = [0usize; MAX_BATCH_REGIONS];
            let mb = f.rows(frame_schema, &mut offsets);
            let start = mb.pk().as_ptr() as usize - f.slot.bytes().as_ptr() as usize;
            self.keys = start..start + mb.len() * stride;
            self.frame = Some(f);
        }
    }
}

/// Per-key accounting for the pre-flight merge: duplicate verdict + inline
/// seed collection, fed keys in globally-sorted merge order. Split from the
/// frame-pulling loop so the verdict and the all-or-nothing seed rule are
/// directly testable with a small cap.
pub(super) struct PreflightAccumulator {
    prev: Option<PkBuf>,
    pub(super) duplicate: bool,
    /// The filter this pre-flight will publish.
    filter: UniqueFilter,
}

impl PreflightAccumulator {
    pub(super) fn new(cap: usize) -> Self {
        PreflightAccumulator {
            prev: None,
            duplicate: false,
            filter: UniqueFilter::with_cap(cap),
        }
    }

    /// Offer the next span in globally-sorted merge order. Returns `false`
    /// once a duplicate is found — the verdict is monotonic, so the caller
    /// stops merging useful spans (but still drains every worker's train).
    /// Spans are byte-equal iff value-equal, so equality is a plain compare.
    pub(super) fn offer(&mut self, key: PkBuf) -> bool {
        if self.duplicate {
            return false;
        }
        if self.prev == Some(key) {
            self.duplicate = true;
            return false;
        }
        self.prev = Some(key);
        let _ = self.filter.insert(key.pk_bytes());
        true
    }

    /// The filter holding every distinct span this pre-flight saw, ready to
    /// publish.
    pub(super) fn into_seed(self) -> UniqueFilter {
        self.filter
    }
}

/// Streaming k-way merge over the workers' sorted span trains, holding one frame
/// per worker, stopping at the first error or duplicate. Equal spans pop
/// adjacently whether one worker or two hold them.
async fn merge_index_scan(
    scan: &TrainLease,
    frame_schema: &SchemaDescriptor,
) -> Result<PreflightAccumulator, WireFault> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut streams: Vec<PreflightKeyStream> = scan
        .workers()
        .iter()
        .map(|w| PreflightKeyStream { lease: scan, w, frame: None, keys: 0..0 })
        .collect();

    let mut heap: BinaryHeap<Reverse<(PkBuf, usize)>> = BinaryHeap::with_capacity(streams.len());
    for (i, s) in streams.iter_mut().enumerate() {
        if let Some(key) = s.next_key(frame_schema).await? {
            heap.push(Reverse((key, i)));
        }
    }

    let mut acc = PreflightAccumulator::new(UNIQUE_FILTER_CAP);
    while let Some(Reverse((key, i))) = heap.pop() {
        if !acc.offer(key) {
            break;
        } // first duplicate is conclusive
        if let Some(next) = streams[i].next_key(frame_schema).await? {
            heap.push(Reverse((next, i)));
        }
    }
    Ok(acc)
}

impl MasterDispatcher {
    /// Pre-flight global uniqueness check for CREATE UNIQUE INDEX, distributed:
    /// each worker streams its committed partition's indexed-column OPK spans
    /// back sorted, and the master's streaming k-way merge (`merge_index_scan`)
    /// finds any adjacent equal pair, within or across partitions — which no
    /// worker's own slice can show. Master memory is `O(num_workers)` plus the (≤ cap) filter seed.
    ///
    /// MUST run inside the DDL critical section (committer barrier drained,
    /// catalog write lock held) and BEFORE the IDX_TAB +1 is appended, so the
    /// scanned snapshot is exactly what each worker fills the index from.
    ///
    /// `Some(filter)` seeds the master's unique-filter cache; `None` when the
    /// index covers the owner's PK and so can never collide.
    pub async fn validate_unique_index_create(
        &self,
        owner_id: u64,
        col_indices: &[u32],
    ) -> Result<Option<UniqueFilter>, WireFault> {
        let (idx_schema, packed) = {
            let cat = self.cat();
            let Some(owner_schema) = cat.registry.relation(owner_id).map(Relation::schema) else {
                // Created in this same bundle, so empty: seed it and spare the
                // first INSERT a warm-up scan.
                return Ok(Some(UniqueFilter::new()));
            };
            // What the IDX_TAB precheck refuses is refused before any scan.
            let idx_schema = cat.validate_index_create(owner_id, col_indices)?;
            // A PK-covering index skips the scan, and every check it would ever plan.
            if owner_schema.covers_pk(col_indices) {
                return Ok(None);
            }
            (idx_schema, gnitz_wire::pack_pk_cols(col_indices))
        };
        let frame_schema = wire::unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // The read routes a REPLICATED owner to one worker: under a fan-out each
        // distinct value would arrive `nw` times and pop adjacently in the merge
        // — `PreflightAccumulator::offer` reads that as a duplicate and fails the
        // CREATE on a genuinely unique table. The count-agnostic merge still
        // catches real within-copy duplicates via the same adjacent-equal
        // check, and the seed reflects true cardinality. Hashed owners keep the
        // full fan-out (genuine cross-partition duplicates surface as equal
        // spans from different workers).
        //
        // The worker's `UniquePreflight` arm resolves the owner's schema from its
        // own catalog.
        let lease = self
            .scan(DirectGroup {
                template: wire::WireMsg {
                    target_id: owner_id,
                    arg1: packed,
                    ..Default::default()
                },
                ..DirectGroup::new(SalMessageKind::UniquePreflight)
            })
            .await?;

        let merged = merge_index_scan(&lease, &frame_schema).await?;
        if merged.duplicate {
            let cat = self.cat();
            return Err(WireFault {
                status: gnitz_wire::WireStatus::IntegrityViolation,
                text: format!(
                    "cannot create unique index on '{}' column '{}': column contains duplicate values",
                    cat.qualified_name(owner_id),
                    cat.column_names(owner_id, col_indices),
                ),
            });
        }
        Ok(Some(merged.into_seed()))
    }
}

#[cfg(test)]
#[path = "tests/unique_preflight.rs"]
mod tests;
