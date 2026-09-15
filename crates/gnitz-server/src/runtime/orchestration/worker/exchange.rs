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
    /// `pad` is this chunk's backfill pad bit; a steady-state tick passes
    /// `false` and ignores the (then always CONTINUE == 0) decision.
    pub(super) fn do_exchange_wait(&mut self, view_id: i64, batch: &Batch, source_id: i64, pad: bool) -> (Batch, u64) {
        self.publish_exchange(view_id, batch, source_id, pad);

        let want_key = (view_id, source_id);

        // A relay parked by an earlier, differently-keyed wait satisfies this one
        // without touching the SAL. Only checked here: once the drain loop below
        // starts, a matching relay short-circuits out of `dispatch_in_eval`.
        if let Some(hit) = self.exchange.pending_relays.remove(&want_key) {
            return hit;
        }

        loop {
            // `_exit`, not `self.shutdown()`: the DAG evaluation up the stack
            // holds a live `&mut` to the engine that its flush would alias.
            // Nothing is lost — the master's death aborts the cluster, and
            // recovery replays the SAL tail.
            if m2w::eventfd_wait(self.m2w_efd, 30_000) == Wake::Idle && self.master_is_gone() {
                unsafe { libc::_exit(0) }
            }

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
        let block = crate::catalog::encode_schema_block_ipc(batch.schema(), view_id as u32);
        let frame = |last| exchange_frame(view_id, source_id, &block, last, pad);

        // Whole and unsplit off the source batch — no sub-batch, no per-row
        // German-string walk — which is the path a real payload takes: a
        // 65,536-row chunk at 100 B/row is 6.5 MB against a 64 MiB frame.
        let whole = ipc::WireMsg {
            data: ipc::WireData::Whole(Some(batch)),
            ..frame(true)
        };
        if whole.size() <= ipc::FRAME_CAP {
            self.w2m_writer.send_msg(W2M_EXCHANGE_RING_ID as u64, &whole);
            return;
        }

        // An empty partition fits above, so this loop has rows, and
        // `wire_chunk_within` yields at least one — so it terminates.
        let overhead = frame(false).size();
        let mut next_row = 0;
        while next_row < batch.len() {
            let chunk = batch.wire_chunk_within(next_row, overhead, ipc::FRAME_CAP);
            let size = overhead + chunk.wire_byte_size();
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
            next_row += chunk.len();
            self.w2m_writer.send_msg(
                W2M_EXCHANGE_RING_ID as u64,
                &ipc::WireMsg {
                    data: ipc::WireData::Whole(Some(&chunk)),
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
        flags: WireFlags::train_frame(0, last),
        seek_pk: source_id as u128,
        // The backfill pad bit the master ANDs across workers. A pad round is
        // empty, hence a single terminal frame, so it still rides the frame that
        // counts.
        seek_col_idx: if last && pad { BACKFILL_PAD_BIT } else { 0 },
        schema_block: Some(schema_block),
        ..Default::default()
    }
}
