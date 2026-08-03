//! Worker exchange-wait re-entry: the defer-then-replay machinery
//! (`do_exchange_wait` inline dispatch loop + `dispatch_deferred`;
//! deferred ticks replay in `replay_deferred_ticks`).

use super::*;

impl WorkerProcess {
    pub(super) fn dispatch_deferred(&mut self) {
        for ddl in std::mem::take(&mut self.exchange.deferred) {
            if let Err(e) = self.cat().ddl_sync(ddl.target_id, ddl.batch) {
                // A failed deferred DDL permanently diverges this worker's
                // catalog from the master — silently wrong results. Fail-stop,
                // same as the main-dispatch DdlSync path and the deferred-decode
                // failure branch.
                self.fatal_shutdown(&format!(
                    "deferred DdlSync application failed for tid={}: {}",
                    ddl.target_id, e
                ));
            }
        }
        // See the DdlSync dispatch arm: the master owns physical directory
        // removal for the shared tree; the worker only discards its queue.
        self.cat().discard_pending_dir_deletions();
    }

    /// Send FLAG_EXCHANGE to the master and block until its FLAG_EXCHANGE_RELAY
    /// for `view_id` comes back on the SAL. Messages that arrive mid-wait are
    /// dispatched inline — handle_push, handle_tick — so ACKs flow back through
    /// the master reactor in their natural arrival order, routed by req_id.
    /// Relays whose view_id does not match the innermost wait are parked in
    /// `pending_relays`; the next nested wait to ask for them will pick them
    /// up without re-reading the SAL.
    pub(super) fn do_exchange_wait(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
        tick_request_id: u64,
    ) -> Batch {
        let schema = batch.schema;
        // During a backfill, stamp this chunk's pad bit onto the FLAG_EXCHANGE so
        // the master can AND it across workers and decide termination. Outside a
        // backfill (backfill_pad == None) the field stays 0.
        let pad_bit = if self.exchange.backfill_pad == Some(true) {
            BACKFILL_PAD_BIT
        } else {
            0
        };
        let msg = ipc::WireMsg {
            target_id: view_id as u64,
            flags: FLAG_EXCHANGE as u64,
            seek_pk: source_id as u128,
            seek_col_idx: pad_bit,
            request_id: tick_request_id,
            schema: schema.as_ref(),
            data: ipc::WireData::Whole(Some(batch)),
            ..Default::default()
        };
        self.w2m_writer.send_msg(tick_request_id, &msg);

        let want_key = (view_id, source_id);
        let ctx = DispatchContext::InEval { relay_wait: want_key };

        // A relay parked by an earlier, differently-keyed wait satisfies this one
        // without touching the SAL. Only checked here: once the drain loop below
        // starts, a matching relay short-circuits out of `dispatch` instead.
        if let Some((b, decision)) = self.exchange.pending_relays.remove(&want_key) {
            self.consume_backfill_decision(decision);
            return b;
        }

        loop {
            self.sal_reader.wait(30000);

            // The main run loop flushes before exiting; this path cannot — the
            // DAG evaluation up the stack holds a live `&mut` to the engine that
            // `handle_flush_all` would alias. Nothing is lost: the master's death
            // aborts the cluster, and recovery replays the SAL tail.
            if self.master_is_gone() {
                unsafe { libc::_exit(0) }
            }

            while let Some((kind, target_id, wire)) = self.next_sal_message() {
                if let Some(batch) = self.dispatch(ctx, kind, target_id, wire) {
                    return batch;
                }
            }
        }
    }
}
