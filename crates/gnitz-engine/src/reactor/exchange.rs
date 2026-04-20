use std::collections::HashMap;

use crate::ipc::DecodedWire;
use crate::reactor::oneshot;
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;

/// Per-view accumulator for FLAG_EXCHANGE replies. When the reactor's
/// `route_reply` sees a FLAG_EXCHANGE wire on worker `w`, it calls
/// `process` instead of consuming the waker; once every worker has
/// reported for a given `(view_id, source_id)` pair, `process` returns a
/// `PendingRelay` that the relay task picks up and writes back to SAL.
///
/// Keyed by `(view_id, source_id)`. A view with multiple sources (e.g.
/// `v_joined` on `v_filtered` + `dim`) opens two independent exchange
/// rounds per tick, one per source — tupling the key keeps the payloads
/// separated so each relay carries its matching shard columns.
///
/// Failure mode: if a worker dies mid-exchange, its entries stay in the
/// accumulator and the tick's `join_all` parks forever. This is fine
/// because `worker_watcher` (executor.rs) triggers reactor shutdown on
/// any worker crash, tearing down all parked tasks.
pub struct ExchangeAccumulator {
    payloads: HashMap<(i64, i64), Vec<Option<Batch>>>,
    counts:   HashMap<(i64, i64), usize>,
    schemas:  HashMap<(i64, i64), SchemaDescriptor>,
    nw: usize,
}

/// Message carried on the relay channel.
///
/// - `Relay`: one completed exchange ready for relay.  The relay task
///   owns this: it acquires the catalog read lock + SAL-writer mutex,
///   calls `MasterDispatcher::relay_exchange`, then releases both.
/// - `Fence`: a synchronization barrier sent by `apply_migration`
///   between Phase 2 (universal stage) and Phase 3 (swap).  When the
///   loop pops a `Fence`, it has already drained all earlier `Relay`s
///   and the read lock has been released; signalling `done` proves no
///   stale relay message can fire against the post-swap catalog.
pub enum PendingRelay {
    Relay {
        view_id:   i64,
        payloads:  Vec<Option<Batch>>,
        schema:    SchemaDescriptor,
        source_id: i64,
    },
    Fence {
        done: oneshot::Sender<()>,
    },
}

impl ExchangeAccumulator {
    pub fn new(nw: usize) -> Self {
        ExchangeAccumulator {
            payloads: HashMap::new(),
            counts:   HashMap::new(),
            schemas:  HashMap::new(),
            nw,
        }
    }

    /// Accept one FLAG_EXCHANGE reply.  Returns `Some(PendingRelay)` once
    /// every worker has reported for the same `(view_id, source_id)`
    /// pair; `None` while the round is still accumulating.  Logs (and
    /// drops) an exchange wire missing its schema instead of producing a
    /// malformed relay.
    pub fn process(&mut self, w: usize, decoded: DecodedWire) -> Option<PendingRelay> {
        let vid = decoded.control.target_id as i64;
        let source_id = decoded.control.seek_pk_lo as i64;
        let key = (vid, source_id);
        let nw = self.nw;

        let payloads = self.payloads
            .entry(key)
            .or_insert_with(|| vec![None; nw]);
        let count = self.counts.entry(key).or_insert(0);

        payloads[w] = decoded.data_batch;
        if let Some(schema) = decoded.schema {
            self.schemas.insert(key, schema);
        }
        *count += 1;

        if *count == nw {
            let schema = match self.schemas.remove(&key) {
                Some(s) => s,
                None => {
                    crate::gnitz_warn!("exchange: no schema received for (view_id={}, source_id={})",
                        vid, source_id);
                    self.payloads.remove(&key);
                    self.counts.remove(&key);
                    return None;
                }
            };
            let payloads_vec = self.payloads.remove(&key).unwrap();
            self.counts.remove(&key);
            Some(PendingRelay::Relay {
                view_id: vid, payloads: payloads_vec, schema, source_id,
            })
        } else {
            None
        }
    }
}
