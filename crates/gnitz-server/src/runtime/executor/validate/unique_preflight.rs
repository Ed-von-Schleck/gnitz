//! The DDL-time pre-flight for `CREATE UNIQUE INDEX`: the fan-out
//! (`validate_unique_index_create`), the per-worker sorted-span streams
//! (`PreflightKeyStream`) and their k-way merge (`merge_index_scan`).

use super::*;

use gnitz_store::relation::Relation;
use gnitz_wire::PkBuf;
use gnitz_zset::schema::SchemaDescriptor;

/// One worker's sorted spans in `merge_index_scan`: the spans of the frame being
/// read and how far the merge has read them. Copied out of the frame, so no ring
/// slot is held while the merge waits on the other workers' trains.
struct PreflightKeyStream<'l> {
    train: Train<'l>,
    w: usize,
    keys: Vec<u8>,
    at: usize,
}

impl PreflightKeyStream<'_> {
    /// Yield this worker's next span, pulling frames on demand. Returns
    /// `Ok(None)` once the train has ended.
    async fn next_key(&mut self, frame_schema: &SchemaDescriptor) -> Result<Option<PkBuf>, WireFault> {
        let stride = frame_schema.pk_stride();
        while self.at == self.keys.len() {
            let Some(f) = self.train.next_of(self.w).await? else {
                return Ok(None);
            };
            self.keys.clear();
            self.keys.extend_from_slice(f.rows(frame_schema).pk());
            self.at = 0;
        }
        let key = PkBuf::from_bytes(&self.keys[self.at..self.at + stride]);
        self.at += stride;
        Ok(Some(key))
    }
}

/// Streaming k-way merge over the workers' sorted span trains, holding one frame's
/// spans per worker. Equal spans pop adjacently whether one worker or two hold
/// them, so the first adjacent equal pair is a duplicate: `None`, the rest of the
/// trains left for the lease drop. Otherwise the filter of every distinct span.
async fn merge_index_scan(scan: Train<'_>, frame_schema: &SchemaDescriptor) -> Result<Option<UniqueFilter>, WireFault> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut streams: Vec<PreflightKeyStream> = scan
        .workers()
        .iter()
        .map(|w| PreflightKeyStream { train: scan, w, keys: Vec::new(), at: 0 })
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

/// Pre-flight global uniqueness check for CREATE UNIQUE INDEX, distributed:
/// each worker enters the index on `cols` under the pre-flight's claim,
/// filled from its committed partition, and streams its spans back off it,
/// sorted; the master's streaming k-way merge (`merge_index_scan`) finds any
/// adjacent equal pair, within or across partitions — which no worker's own
/// slice can show. Master memory is `O(num_workers)` plus the (≤ cap) filter seed.
///
/// Runs under the DDL's catalog write, before the zone is laid out, so the index
/// each worker filled is the one its catalog claim adopts; the caller releases
/// every claim it leaves. The DDL's hold of the apply lock keeps a checkpoint's
/// sweep off the claimed directory meanwhile.
///
/// `Some(filter)` seeds the index's unique filter; `None` when the
/// index covers the owner's PK and so can never collide.
pub(in crate::runtime::executor) async fn validate_unique_index_create(
    disp: &MasterDispatcher,
    owner_id: u64,
    cols: PkColList,
) -> Result<Option<UniqueFilter>, WireFault> {
    let spec = {
        let cat = disp.cat();
        let Some(owner_schema) = cat.registry.relation(owner_id).map(Relation::schema) else {
            // Created in this same bundle, so empty.
            return Ok(Some(UniqueFilter::new()));
        };
        // What the IDX_TAB precheck refuses is refused before any scan.
        let spec = cat.validate_index_create(owner_id, cols.as_slice(), true)?;
        // A PK-covering index skips the scan, and every check it would ever plan.
        if owner_schema.covers_pk(cols.as_slice()) {
            return Ok(None);
        }
        spec
    };
    let frame_schema = spec.span_schema();

    // A replicated owner is read on one worker: every worker's copy would
    // pop each value `nw` times.
    let mut lease = disp.scan(Read::KeySpans { tid: owner_id, cols, claim: true }).await?;

    let Some(seed) = merge_index_scan(lease.next_train().await, &frame_schema).await? else {
        let cat = disp.cat();
        return Err(integrity_violation(format!(
            "cannot create unique index on '{}' column '{}': column contains duplicate values",
            cat.qualified_name(owner_id),
            cat.column_names(owner_id, cols.as_slice()),
        )));
    };
    Ok(Some(seed))
}

#[cfg(test)]
#[path = "tests/unique_preflight.rs"]
mod tests;
