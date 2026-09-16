//! The worker's half of an exchange round: publishing its partition
//! (`publish_exchange`), waiting out the master's relay (`do_exchange_wait`),
//! and the defer-then-replay machinery that wait needs — `dispatch_deferred`
//! here, `replay_deferred` at top level.

use super::*;
use crate::runtime::w2m::W2M_EXCHANGE_RING_ID;

impl WorkerProcess {
    /// Apply what the exchange wait deferred to this point — a catalog mutation
    /// — now that the DAG has returned and before the ACK that implies it.
    pub(super) fn dispatch_deferred(&mut self) {
        for req in std::mem::take(&mut self.exchange.deferred) {
            self.handle_request(req);
        }
    }

    /// Publish an exchange frame train to the master and block until its ExchangeRelay
    /// for `view_id` comes back on the SAL, returning it with the backfill
    /// decision the master stamped onto it. Messages arriving mid-wait are
    /// dispatched per [`in_eval`]; a relay for another `(view_id, source_id)`
    /// parks in `pending_relays`.
    ///
    /// `pad` marks this worker's partition exhausted; a steady tick passes `false`.
    pub(super) fn do_exchange_wait(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
        pad: bool,
    ) -> (Batch, BackfillDecision) {
        self.publish_exchange(view_id, batch, source_id, pad);

        let want_key = (view_id, source_id);

        // A relay parked by an earlier, differently-keyed wait satisfies this one
        // without touching the SAL. Only checked here: once the drain loop below
        // starts, a matching relay short-circuits out of `dispatch_in_eval`.
        if let Some(hit) = self.exchange.pending_relays.remove(&want_key) {
            return hit;
        }

        loop {
            self.w2m_writer.sal_park().park(|| self.sal_reader.is_empty());
            while let Some((msg, wire)) = self.next_sal_message() {
                if let Some(hit) = self.dispatch_in_eval(want_key, &msg, wire) {
                    return (hit.batch, hit.decision);
                }
            }
        }
    }

    /// Publish this worker's exchange partition as a train of frames inside
    /// [`ipc::FRAME_CAP`] — the bound every other producer already holds itself
    /// to. What a frame carries is [`exchange_frame`]'s; this only cuts rows.
    fn publish_exchange(&self, view_id: i64, batch: &Batch, source_id: i64, pad: bool) {
        let block = crate::catalog::encode_schema_block(batch.schema(), view_id as u32);
        let frame = |last| exchange_frame(view_id, source_id, &block, last, pad);

        // Whole and unsplit off the source batch — no sub-batch, no per-row
        // German-string walk — which is the path a real payload takes: a
        // 65,536-row chunk at 100 B/row is 6.5 MB against a 64 MiB frame.
        let whole = ipc::WireMsg {
            data: ipc::WireData::Whole(batch),
            ..frame(true)
        };
        if whole.size() <= ipc::FRAME_CAP {
            self.w2m_writer.send_msg(W2M_EXCHANGE_RING_ID, &whole);
            return;
        }

        // An empty partition fits above, so this loop has rows, and
        // `wire_chunk_within` yields at least one — so it terminates.
        let overhead = frame(false).size();
        let mut next_row = 0;
        while next_row < batch.len() {
            let (chunk, size) = batch.wire_chunk_within(next_row, overhead, ipc::FRAME_CAP);
            if size > ipc::FRAME_CAP {
                // One row too wide to frame. A reply faults its client with
                // `oversized_reply`; an exchange has no client to fault.
                gnitz_fatal_abort!(
                    "worker: exchange row {} of view_id={} encodes to {} bytes, past the {} byte frame cap",
                    next_row,
                    view_id,
                    size,
                    ipc::FRAME_CAP,
                );
            }
            let start = next_row;
            next_row += chunk.rows();
            self.w2m_writer.send_msg(
                W2M_EXCHANGE_RING_ID,
                &ipc::WireMsg {
                    data: ipc::WireData::of_chunk(batch, start, &chunk),
                    ..frame(next_row == batch.len())
                },
            );
        }
    }
}

/// One frame of a worker's exchange train, but for its payload. Every frame
/// carries the schema block: the master decodes a ring slot with no hint.
fn exchange_frame<'a>(view_id: i64, source_id: i64, schema_block: &'a [u8], last: bool, pad: bool) -> ipc::WireMsg<'a> {
    ipc::WireMsg {
        target_id: view_id as u64,
        // The backfill pad bit the master ANDs across workers. A pad round is
        // empty, hence a single terminal frame, so it still rides the frame that
        // counts.
        flags: WireFlags {
            backfill_pad: last && pad,
            ..WireFlags::train_frame(0, last)
        },
        arg0: source_id as u64,
        schema_block: Some(schema_block),
        ..Default::default()
    }
}
