//! Worker exchange-wait re-entry: the defer-then-replay machinery
//! (`do_exchange_wait` inline dispatch loop + `dispatch_deferred`;
//! deferred control groups replay in `replay_deferred_control`).

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
        // backfill (backfill_pad == None) the field stays 0, exactly as before.
        let pad_bit = if self.exchange.backfill_pad == Some(true) {
            BACKFILL_PAD_BIT
        } else {
            0
        };
        let sz = ipc::wire_size(STATUS_OK, &[], schema.as_ref(), None, Some(batch), None, &[]);
        self.w2m_writer.send_encoded(sz, tick_request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf,
                0,
                view_id as u64,
                0,
                FLAG_EXCHANGE as u64,
                source_id as u128,
                pad_bit,
                tick_request_id,
                STATUS_OK,
                &[],
                schema.as_ref(),
                None,
                Some(batch),
                None,
                &[],
            );
        });

        let master_pid = self.master_pid;
        let want_key = (view_id, source_id);
        let ctx = DispatchContext::InEval {
            relay_wait: Some(want_key),
            schema,
        };

        loop {
            if let Some((b, decision)) = self.exchange.pending_relays.remove(&want_key) {
                self.consume_backfill_decision(decision);
                return b;
            }

            self.sal_reader.wait(30000);

            // If the master died (killed or gnitz_fatal_abort) while we
            // were waiting, exit like the main run loop does.
            if master_pid != 0 && unsafe { libc::getppid() } != master_pid {
                unsafe {
                    libc::_exit(0);
                }
            }

            loop {
                if let Some((b, decision)) = self.exchange.pending_relays.remove(&want_key) {
                    self.consume_backfill_decision(decision);
                    return b;
                }

                let (kind, target_id, wire) = match self.next_sal_message() {
                    Some(v) => v,
                    None => break, // no more entries — back to outer wait
                };

                match self.dispatch(ctx, kind, target_id, wire) {
                    DispatchOutcome::Continue => {}
                    DispatchOutcome::RelayMatched(batch) => return batch,
                }
            }
        }
    }
}
