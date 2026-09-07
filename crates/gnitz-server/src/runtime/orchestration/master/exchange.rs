//! Exchange rounds: turning per-worker `FLAG_EXCHANGE` frames into the relays
//! the master writes back.
//!
//! A **round** is one `(view_id, source_id)` pair's frames from every worker —
//! keyed by the pair because a two-source view opens one round per source and a
//! relay carries its own source's shard columns. It **completes** when every
//! worker has sent its TERMINAL frame into it, yielding a [`PendingRelay`].
//!
//! A worker publishes its partition as a `FRAME_CAP`-bounded train (see
//! `worker/exchange.rs`), so a slot accumulates over several frames and only the
//! terminal one — [`train_has_more`], the engine's one train-end rule — reports
//! the worker.
//!
//! Two drivers accumulate rounds, which is why this sits with them rather than
//! in the reactor that delivers the frames: `relay_loop` for the steady state,
//! and `MasterDispatcher::collect_acks_and_relay` for a boot backfill, which
//! runs before a reactor exists.

use rustc_hash::FxHashMap;

use super::train::train_has_more;
use crate::runtime::wire::{DecodedWire, BACKFILL_PAD_BIT};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::{Batch, Layout};
use gnitz_wire::{low_bits_mask, MAX_WORKERS};

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
    /// use a handful of slots. A worker's slot accumulates its train in frame
    /// order, which is source-row order.
    payloads: Vec<Option<Batch>>,
    /// AND of the layout claims of worker `w`'s frames, re-installed on the
    /// completed slot: `append_batch` downgrades what it appends into, so a
    /// multi-frame slot would otherwise lose its source's claim and drop the
    /// round onto the re-sorting `op_repartition_batches`.
    consolidated: Vec<bool>,
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
        ExchangeAccumulator { rounds: FxHashMap::default(), nw }
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
            consolidated: vec![true; nw],
            reported: 0,
            schema: None,
            all_pad: true,
        });

        // Every exchange frame carries its own schema block (the ring decode
        // takes no hint), so this is last-wins over identical values.
        if let Some(schema) = decoded.schema {
            round.schema = Some(schema);
        }
        if let Some(b) = decoded.data_batch {
            round.consolidated[w] &= b.layout() == Layout::Consolidated;
            match &mut round.payloads[w] {
                // Frames are consecutive ascending row ranges of one partition,
                // so arrival order reproduces it. The first is moved, not copied.
                Some(acc) => acc.append_batch(&b, 0, b.len()),
                slot @ None => *slot = Some(b),
            }
        }
        // Bookkeeping rides the terminal frame alone, so a partial train cannot
        // complete the round.
        if train_has_more(decoded.control.flags) {
            return None;
        }
        // AND this worker's per-chunk backfill pad bit. 0 for steady-state
        // exchanges, which clears all_pad harmlessly (the relay path ignores it).
        round.all_pad &= (decoded.control.seek_col_idx & BACKFILL_PAD_BIT) != 0;
        round.reported |= 1 << w;

        if round.reported != low_bits_mask(nw) {
            return None;
        }
        let mut round = self.rounds.remove(&key).unwrap();
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
        // Debug-verified by `certify_layout`, so a concatenation that is not in
        // fact consolidated fails here rather than silently costing the scatter.
        for (payload, ok) in round.payloads.iter_mut().zip(&round.consolidated) {
            if let (Some(b), true) = (payload.as_mut(), *ok) {
                b.certify_layout(Layout::Consolidated, &schema);
            }
        }
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
#[path = "tests/exchange.rs"]
mod tests;
