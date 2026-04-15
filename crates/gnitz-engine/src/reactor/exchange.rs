use std::collections::HashMap;

use crate::ipc::DecodedWire;
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;

/// Per-view accumulator for FLAG_EXCHANGE replies. When the reactor's
/// `route_reply` sees a FLAG_EXCHANGE wire on worker `w`, it calls
/// `process` instead of consuming the waker; once every worker has
/// reported for a given view_id, `process` returns a `PendingRelay` that
/// the relay task picks up and writes back to SAL.
///
/// Keyed by `decoded.control.target_id` (the view_id), NOT by req_id or
/// source tid: a single tick on one source tid can drive exchanges for
/// multiple views in DAG order; their relays demux independently.
///
/// Failure mode: if a worker dies mid-exchange, its view entries stay
/// in the accumulator and the tick's `join_all` parks forever. This is
/// fine because `worker_watcher` (executor.rs) triggers reactor
/// shutdown on any worker crash, tearing down all parked tasks.
pub struct ExchangeAccumulator {
    payloads:   HashMap<i64, Vec<Option<Batch>>>,
    counts:     HashMap<i64, usize>,
    schemas:    HashMap<i64, SchemaDescriptor>,
    source_ids: HashMap<i64, i64>,
    nw: usize,
}

/// One completed exchange ready for relay.  The relay task owns this:
/// it acquires the catalog read lock + SAL-writer mutex, calls
/// `MasterDispatcher::relay_exchange`, then releases both.
pub struct PendingRelay {
    pub view_id:   i64,
    pub payloads:  Vec<Option<Batch>>,
    pub schema:    SchemaDescriptor,
    pub source_id: i64,
}

impl ExchangeAccumulator {
    pub fn new(nw: usize) -> Self {
        ExchangeAccumulator {
            payloads:   HashMap::new(),
            counts:     HashMap::new(),
            schemas:    HashMap::new(),
            source_ids: HashMap::new(),
            nw,
        }
    }

    /// Accept one FLAG_EXCHANGE reply.  Returns `Some(PendingRelay)` once
    /// every worker has reported for the same view_id; `None` while the
    /// view is still accumulating.  Logs (and drops) an exchange wire
    /// missing its schema instead of producing a malformed relay.
    pub fn process(&mut self, w: usize, decoded: DecodedWire) -> Option<PendingRelay> {
        let vid = decoded.control.target_id as i64;
        let ex_source_id = decoded.control.seek_pk_lo as i64;
        let nw = self.nw;

        let payloads = self.payloads
            .entry(vid)
            .or_insert_with(|| vec![None; nw]);
        let count = self.counts.entry(vid).or_insert(0);

        payloads[w] = decoded.data_batch;
        if let Some(schema) = decoded.schema {
            self.schemas.insert(vid, schema);
        }
        if ex_source_id > 0 {
            self.source_ids.insert(vid, ex_source_id);
        }
        *count += 1;

        if *count == nw {
            let schema = match self.schemas.remove(&vid) {
                Some(s) => s,
                None => {
                    crate::gnitz_warn!("exchange: no schema received for view_id={}", vid);
                    self.payloads.remove(&vid);
                    self.counts.remove(&vid);
                    self.source_ids.remove(&vid);
                    return None;
                }
            };
            let source_id = self.source_ids.remove(&vid).unwrap_or(0);
            let payloads_vec = self.payloads.remove(&vid).unwrap();
            self.counts.remove(&vid);
            Some(PendingRelay { view_id: vid, payloads: payloads_vec, schema, source_id })
        } else {
            None
        }
    }
}
