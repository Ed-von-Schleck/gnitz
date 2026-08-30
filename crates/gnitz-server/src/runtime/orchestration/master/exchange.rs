//! Exchange rounds: turning per-worker `FLAG_EXCHANGE` frames into the relays
//! the master writes back.
//!
//! A **round** is one `(view_id, source_id)` pair's frames from every worker —
//! keyed by the pair because a two-source view opens one round per source and a
//! relay carries its own source's shard columns. It **completes** when every
//! worker has reported into it, yielding a [`PendingRelay`].
//!
//! Two drivers accumulate rounds, which is why this sits with them rather than
//! in the reactor that delivers the frames: `relay_loop` for the steady state,
//! and `MasterDispatcher::collect_acks_and_relay` for a boot backfill, which
//! runs before a reactor exists.

use rustc_hash::FxHashMap;

use crate::runtime::w2m::worker_mask;
use crate::runtime::wire::{DecodedWire, BACKFILL_PAD_BIT};
use gnitz_engine::schema::SchemaDescriptor;
use gnitz_engine::storage::Batch;
use gnitz_wire::MAX_WORKERS;

/// Per-view accumulator for `FLAG_EXCHANGE` replies, keyed by
/// `(view_id, source_id)`.
///
/// A worker dying mid-round leaves its entries here and the tick's `join_all`
/// parked forever — survivable only because `watchdog` shuts the reactor down
/// on any worker crash.
pub struct ExchangeAccumulator {
    rounds: FxHashMap<(i64, i64), ExchangeRound>,
    nw: usize,
}

struct ExchangeRound {
    /// One slot per live worker — sized `nw`, not `MAX_WORKERS`: `Batch` is
    /// ~1 KB, so a fixed 64-slot array would build and move ~70 KB per round to
    /// use a handful of slots.
    payloads: Vec<Option<Batch>>,
    /// Bit `w` set once worker `w` has reported. A bitmask rather than a
    /// counter (`nw <= MAX_WORKERS == 64`) so a worker reporting twice cannot
    /// complete the round while another worker's slot is still empty.
    reported: u64,
    schema: Option<SchemaDescriptor>,
    /// AND of every worker's per-chunk backfill pad bit (`seek_col_idx &
    /// BACKFILL_PAD_BIT`). Starts `true`; a single non-pad worker clears it.
    /// True once the round completes ⇒ every worker is exhausted and this is the
    /// final (all-pad) round. Always `false` for steady-state exchanges (their
    /// `seek_col_idx` is 0); the steady-state relay path ignores it.
    all_pad: bool,
}

/// One completed exchange ready for relay. The relay driver owns this: it builds
/// the group under the catalog read lock (`prepare_relay`), then emits it under
/// `sal_writer_excl` (`emit_relay_with_decision`) — two separate holds, never
/// one.
pub struct PendingRelay {
    pub view_id: i64,
    pub payloads: Vec<Option<Batch>>,
    pub schema: SchemaDescriptor,
    pub source_id: i64,
    /// True iff every worker reported a backfill pad for this round (the final,
    /// all-pad round). The boot backfill relay (`collect_acks_and_relay`) reads
    /// this to decide the stop signal; the steady-state relay path ignores it.
    pub all_pad: bool,
}

impl ExchangeAccumulator {
    pub fn new(nw: usize) -> Self {
        debug_assert!(
            nw <= MAX_WORKERS,
            "ExchangeAccumulator: nw={nw} exceeds MAX_WORKERS={MAX_WORKERS}"
        );
        ExchangeAccumulator {
            rounds: FxHashMap::default(),
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
        let source_id = decoded.control.seek_pk as i64;
        let key = (vid, source_id);
        let nw = self.nw;

        let round = self.rounds.entry(key).or_insert_with(|| ExchangeRound {
            payloads: (0..nw).map(|_| None).collect(),
            reported: 0,
            schema: None,
            all_pad: true,
        });

        round.payloads[w] = decoded.data_batch;
        if let Some(schema) = decoded.schema {
            round.schema = Some(schema);
        }
        // AND this worker's per-chunk backfill pad bit. 0 for steady-state
        // exchanges, which clears all_pad harmlessly (the relay path ignores it).
        round.all_pad &= (decoded.control.seek_col_idx & BACKFILL_PAD_BIT) != 0;
        round.reported |= 1 << w;

        if round.reported != worker_mask(nw) {
            return None;
        }
        let round = self.rounds.remove(&key).unwrap();
        let schema = match round.schema {
            Some(s) => s,
            None => {
                gnitz_warn!(
                    "exchange: no schema received for (view_id={}, source_id={})",
                    vid,
                    source_id
                );
                return None;
            }
        };
        Some(PendingRelay {
            view_id: vid,
            payloads: round.payloads,
            schema,
            source_id,
            all_pad: round.all_pad,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_wire::control::DecodedControl;
    use gnitz_wire::FLAG_EXCHANGE;

    fn make_wire(view_id: i64, source_id: i64, with_schema: bool) -> DecodedWire {
        DecodedWire {
            control: DecodedControl {
                target_id: view_id as u64,
                flags: FLAG_EXCHANGE,
                seek_pk: source_id as u128,
                ..Default::default()
            },
            schema: if with_schema {
                Some(SchemaDescriptor::minimal_u64())
            } else {
                None
            },
            data_batch: None,
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
        let relay = acc
            .process(1, make_wire(7, 3, false))
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

    fn make_wire_pad(view_id: i64, source_id: i64, pad: bool, with_schema: bool) -> DecodedWire {
        let mut w = make_wire(view_id, source_id, with_schema);
        w.control.seek_col_idx = if pad { BACKFILL_PAD_BIT } else { 0 };
        w
    }

    #[test]
    fn all_pad_is_and_of_worker_pad_bits() {
        // Every worker padded ⇒ the round is the final all-pad round.
        let mut acc = ExchangeAccumulator::new(2);
        assert!(acc.process(0, make_wire_pad(1, 0, true, true)).is_none());
        let relay = acc
            .process(1, make_wire_pad(1, 0, true, false))
            .expect("round completes");
        assert!(relay.all_pad, "all workers padded ⇒ all_pad");

        // A single non-pad worker clears all_pad (backfill must continue).
        let mut acc = ExchangeAccumulator::new(2);
        assert!(acc.process(0, make_wire_pad(2, 0, true, true)).is_none());
        let relay = acc
            .process(1, make_wire_pad(2, 0, false, false))
            .expect("round completes");
        assert!(!relay.all_pad, "a non-pad worker clears all_pad");

        // Steady-state exchanges pass seek_col_idx == 0 ⇒ all_pad false.
        let mut acc = ExchangeAccumulator::new(2);
        assert!(acc.process(0, make_wire(3, 0, true)).is_none());
        let relay = acc.process(1, make_wire(3, 0, false)).expect("round completes");
        assert!(!relay.all_pad, "steady-state (seek_col_idx==0) ⇒ all_pad false");
    }

    fn make_wire_src(view_id: i64, source_id: i64, req_id: u64) -> DecodedWire {
        let mut w = make_wire(view_id, source_id, true);
        w.control.request_id = req_id;
        w
    }

    /// A view with two sources opens one round per source: worker 0 reporting
    /// for source A and worker 1 for source B completes neither.
    #[test]
    fn accumulator_distinguishes_source_ids() {
        let mut acc = ExchangeAccumulator::new(2);
        assert!(
            acc.process(0, make_wire_src(42, 100, 7)).is_none(),
            "source 100 has heard from worker 0 only"
        );
        assert!(
            acc.process(1, make_wire_src(42, 200, 8)).is_none(),
            "source 200 has heard from worker 1 only — the rounds must not merge"
        );
        let relay = acc
            .process(1, make_wire_src(42, 100, 9))
            .expect("source 100's round completes on its second worker");
        assert_eq!(relay.source_id, 100);
        assert_eq!(acc.rounds.len(), 1, "source 200's round is still open");
    }
}
