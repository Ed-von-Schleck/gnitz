use std::collections::HashMap;

use crate::runtime::wire::DecodedWire;
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
    rounds: HashMap<(i64, i64), ExchangeRound>,
    nw: usize,
}

struct ExchangeRound {
    payloads: Vec<Option<Batch>>,
    count:    usize,
    schema:   Option<SchemaDescriptor>,
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
        ExchangeAccumulator { rounds: HashMap::new(), nw }
    }

    /// Accept one FLAG_EXCHANGE reply.  Returns `Some(PendingRelay)` once
    /// every worker has reported for the same `(view_id, source_id)`
    /// pair; `None` while the round is still accumulating.  Logs (and
    /// drops) an exchange wire missing its schema instead of producing a
    /// malformed relay.
    pub fn process(&mut self, w: usize, decoded: DecodedWire) -> Option<PendingRelay> {
        let vid = decoded.control.target_id as i64;
        let source_id = decoded.control.seek_pk as i64;
        let key = (vid, source_id);
        let nw = self.nw;

        let round = self.rounds.entry(key).or_insert_with(|| ExchangeRound {
            payloads: vec![None; nw],
            count:    0,
            schema:   None,
        });

        round.payloads[w] = decoded.data_batch;
        if let Some(schema) = decoded.schema {
            round.schema = Some(schema);
        }
        round.count += 1;

        if round.count == nw {
            let round = self.rounds.remove(&key).unwrap();
            let schema = match round.schema {
                Some(s) => s,
                None => {
                    crate::gnitz_warn!("exchange: no schema received for (view_id={}, source_id={})",
                        vid, source_id);
                    return None;
                }
            };
            Some(PendingRelay { view_id: vid, payloads: round.payloads, schema, source_id })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::wire::{DecodedWire, DecodedControl};
    use crate::runtime::sal::FLAG_EXCHANGE;
    use crate::schema::SchemaDescriptor;

    fn make_wire(view_id: i64, source_id: i64, with_schema: bool) -> DecodedWire {
        DecodedWire {
            control: DecodedControl {
                status: 0,
                client_id: 0,
                target_id: view_id as u64,
                flags: FLAG_EXCHANGE as u64,
                seek_pk: source_id as u128,
                seek_col_idx: 0,
                request_id: 0,
                error_msg: Vec::new(),
            },
            schema: if with_schema { Some(SchemaDescriptor::minimal_u64()) } else { None },
            data_batch: None,
            batch_backing: None,
        }
    }

    #[test]
    fn partial_round_returns_none() {
        let mut acc = ExchangeAccumulator::new(3);
        // First two of three workers report — round must stay pending.
        assert!(acc.process(0, make_wire(10, 0, true)).is_none());
        assert!(acc.process(1, make_wire(10, 0, false)).is_none());
        assert!(!acc.rounds.is_empty(), "partial round must remain in map");
    }

    #[test]
    fn complete_round_returns_relay_with_correct_ids() {
        let mut acc = ExchangeAccumulator::new(2);
        assert!(acc.process(0, make_wire(7, 3, true)).is_none());
        let relay = acc.process(1, make_wire(7, 3, false))
            .expect("complete round must return PendingRelay");
        assert_eq!(relay.view_id, 7);
        assert_eq!(relay.source_id, 3);
        assert_eq!(relay.payloads.len(), 2);
        assert!(acc.rounds.is_empty(), "completed round must be removed from map");
    }

    #[test]
    fn schema_less_round_returns_none_and_cleans_up() {
        let mut acc = ExchangeAccumulator::new(2);
        assert!(acc.process(0, make_wire(5, 0, false)).is_none());
        let result = acc.process(1, make_wire(5, 0, false));
        assert!(result.is_none(), "schema-less round must return None");
        assert!(acc.rounds.is_empty(), "completed schema-less round must not leak");
    }
}
