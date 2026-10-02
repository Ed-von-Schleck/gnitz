//! The DDL-time pre-flight for `CREATE UNIQUE INDEX`: the fan-out
//! (`validate_unique_index_create`), the per-worker sorted-span streams
//! (`PreflightKeyStream`) and their k-way merge (`merge_index_scan`).
//!
//! Nothing here is on the write path — `preflight.rs` holds that — and the only
//! state shared with it is the `UniqueFilter` this pre-flight seeds.

use super::*;

use std::ops::Range;

use crate::runtime::reactor::TrainFrame;
use gnitz_store::relation::Relation;
use gnitz_zset::schema::key::PkBuf;

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
            let mb = f.rows(frame_schema);
            let start = mb.pk().as_ptr() as usize - f.slot.bytes().as_ptr() as usize;
            self.keys = start..start + mb.len() * stride;
            self.frame = Some(f);
        }
    }
}

/// Streaming k-way merge over the workers' sorted span trains, holding one frame
/// per worker. Equal spans pop adjacently whether one worker or two hold them, so
/// the first adjacent equal pair is a duplicate: `None`, the rest of the trains
/// left for the lease drop. Otherwise the filter of every distinct span.
async fn merge_index_scan(
    scan: &TrainLease,
    frame_schema: &SchemaDescriptor,
) -> Result<Option<UniqueFilter>, WireFault> {
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

    let (mut seed, mut prev) = (UniqueFilter::new(), None);
    while let Some(Reverse((key, i))) = heap.pop() {
        // Spans are byte-equal iff value-equal.
        if prev == Some(key) {
            return Ok(None);
        }
        prev = Some(key);
        seed.insert(key.pk_bytes());
        if let Some(next) = streams[i].next_key(frame_schema).await? {
            heap.push(Reverse((next, i)));
        }
    }
    Ok(Some(seed))
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
        cols: PkColList,
    ) -> Result<Option<UniqueFilter>, WireFault> {
        let spec = {
            let cat = self.cat();
            let Some(owner_schema) = cat.registry.relation(owner_id).map(Relation::schema) else {
                // Created in this same bundle, so empty.
                return Ok(Some(UniqueFilter::new()));
            };
            // What the IDX_TAB precheck refuses is refused before any scan.
            let spec = cat.validate_index_create(owner_id, cols.as_slice())?;
            // A PK-covering index skips the scan, and every check it would ever plan.
            if owner_schema.covers_pk(cols.as_slice()) {
                return Ok(None);
            }
            spec
        };
        let frame_schema = spec.span_schema();

        // A replicated owner is read on one worker: every worker's copy would
        // pop each value `nw` times.
        let lease = self.scan(Read::KeySpans { tid: owner_id, cols }).await?;

        let Some(seed) = merge_index_scan(&lease, &frame_schema).await? else {
            let cat = self.cat();
            return Err(WireFault {
                status: gnitz_wire::WireStatus::IntegrityViolation,
                text: format!(
                    "cannot create unique index on '{}' column '{}': column contains duplicate values",
                    cat.qualified_name(owner_id),
                    cat.column_names(owner_id, cols.as_slice()),
                ),
            });
        };
        Ok(Some(seed))
    }
}

#[cfg(test)]
#[path = "tests/unique_preflight.rs"]
mod tests;
