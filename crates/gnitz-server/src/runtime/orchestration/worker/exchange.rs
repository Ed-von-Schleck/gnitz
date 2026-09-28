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

    /// Publish `batch` as an exchange frame train to the master and block until
    /// its ExchangeRelay for `view_id` comes back on the SAL, returning it with
    /// the drained bit the master stamped onto it. Messages arriving mid-wait are
    /// dispatched per [`in_eval`].
    ///
    /// `pad` marks this worker's partition exhausted; a steady tick passes `false`.
    pub(super) fn do_exchange_wait(&mut self, view_id: i64, batch: Batch, source_id: i64, pad: bool) -> RelayHit {
        self.publish_exchange(view_id, &batch, source_id, pad);
        // Before the park, so this worker never holds its own partition and the
        // relayed one at once.
        drop(batch);

        let want_key = (view_id, source_id);

        loop {
            self.w2m_writer.sal_park().park(|| self.sal_reader.is_empty());
            while let Some((msg, wire)) = self.sal_reader.next() {
                if let Some(hit) = self.dispatch_in_eval(want_key, &msg, wire) {
                    return hit;
                }
            }
        }
    }

    /// Publish this worker's exchange partition as a train of frames.
    pub(super) fn publish_exchange(&self, view_id: i64, batch: &Batch, source_id: i64, pad: bool) {
        let block = gnitz_store::schema::encode_schema_block(batch.schema());
        let sent = reply::send_train(
            &self.w2m_writer,
            W2M_EXCHANGE_RING_ID,
            batch,
            self.reply_frame_budget,
            |last| exchange_frame(view_id, source_id, &block, last, pad),
        );
        // An exchange has no client to fault.
        if let Err(fault) = sent {
            gnitz_fatal_abort!("worker: exchange partition of view_id={view_id}: {fault}");
        }
    }
}

/// One frame of a worker's exchange train, but for its payload. Every frame
/// carries the schema block: the master decodes a ring slot with no hint.
fn exchange_frame<'a>(view_id: i64, source_id: i64, schema_block: &'a [u8], last: bool, pad: bool) -> ipc::WireMsg<'a> {
    ipc::WireMsg {
        target_id: view_id as u64,
        flags: WireFlags {
            scan_last: last,
            drained: pad,
            ..Default::default()
        },
        arg0: source_id as u64,
        schema_block: Some(schema_block),
        ..Default::default()
    }
}
