//! Worker W2M framing: the `WorkerProcess` reply helpers, the `PendingScan`
//! train they queue, and the frame cutter every train is sent through.

use super::*;

use std::borrow::Borrow;

use ipc::WireMsg;

// ---------------------------------------------------------------------------
// PendingScan
// ---------------------------------------------------------------------------

/// One reply train queued in `pending_streams`, emitted one frame per
/// `drain_sal` pass in strict FIFO order.
pub(super) struct PendingScan {
    pub(super) batch: Rc<Batch>,
    pub(super) route: ReplyRoute,
    /// Rows already emitted.
    pub(super) next_row: usize,
}

/// One frame of a worker reply, but for its payload.
fn reply_frame(route: ReplyRoute, last: bool) -> WireMsg<'static> {
    WireMsg {
        target_id: route.target_id,
        flags: WireFlags::train_frame(last),
        ..Default::default()
    }
}

impl PendingScan {
    /// Emit this train's next frame, returning whether more remain.
    fn emit_next(&mut self, w2m: &W2mWriter, budget: usize) -> Result<bool, gnitz_wire::WireFault> {
        let end = send_train_frame(w2m, self.route, &self.batch, self.next_row, budget, true)?;
        self.next_row = end;
        Ok(end < self.batch.len())
    }
}

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, request_id: u32) {
        self.w2m_writer.send_status(request_id, WireStatus::Ok, &[]);
    }

    /// A control-only frame carrying the fault's own status.
    pub(super) fn send_fault(&self, fault: &gnitz_wire::WireFault, request_id: u32) {
        self.w2m_writer
            .send_status(request_id, fault.status, fault.text.as_bytes());
    }

    /// Reply with `batch`: one frame now when it fits and `route.fifo` is clear,
    /// else a train queued behind the others.
    pub(super) fn send_reply(&mut self, route: ReplyRoute, batch: impl Borrow<Batch> + Into<Rc<Batch>>) {
        if !route.fifo {
            let b: &Batch = batch.borrow();
            let msg = WireMsg {
                data: b.wire_whole(),
                ..reply_frame(route, true)
            };
            if msg.data.is_none() || msg.size() <= self.reply_frame_budget {
                self.w2m_writer.send_msg(route.request_id, &msg);
                return;
            }
        }
        self.pending_streams
            .push_back(PendingScan { batch: batch.into(), route, next_row: 0 });
    }

    /// Emit the front train's next frame. A train that ends or faults is popped;
    /// a fault answers its request instead.
    pub(super) fn emit_pending_scan_chunk(&mut self) {
        let Some(train) = self.pending_streams.front_mut() else {
            return;
        };
        let request_id = train.route.request_id;
        match train.emit_next(&self.w2m_writer, self.reply_frame_budget) {
            Ok(true) => {}
            Ok(false) => {
                self.pending_streams.pop_front();
            }
            Err(fault) => {
                self.pending_streams.pop_front();
                self.send_fault(&fault, request_id);
            }
        }
    }
}

/// Send the frame of `batch` that starts at row `start`, flagged last when it
/// reaches the end and `ends_train`; `Ok` is the row after it. `Err` is a row
/// too wide for any frame.
fn send_train_frame(
    w2m: &W2mWriter,
    route: ReplyRoute,
    batch: &Batch,
    start: usize,
    budget: usize,
    ends_train: bool,
) -> Result<usize, gnitz_wire::WireFault> {
    let head = reply_frame(route, false).size();
    let rows = batch.wire_rows_within(start, budget.saturating_sub(head));
    let end = start + rows.map_or(0, |r| r.rows());
    let msg = WireMsg {
        data: rows,
        ..reply_frame(route, ends_train && end == batch.len())
    };
    if msg.size() > gnitz_wire::MAX_FRAME_PAYLOAD {
        return Err(crate::runtime::wire::oversized_frame_message(msg.size()).into());
    }
    w2m.send_msg(route.request_id, &msg);
    Ok(end)
}

/// Send `keys` to the master as one train over `frame_schema`, a span per row's
/// PK region, `chunk_rows` spans in RAM at a time.
pub(crate) fn send_unique_preflight_keys(
    w2m_writer: &W2mWriter,
    target_id: u64,
    frame_schema: &SchemaDescriptor,
    request_id: u32,
    budget: usize,
    chunk_rows: usize,
    keys: &mut gnitz_zset::repr::KeyProducer,
) {
    let route = ReplyRoute { target_id, request_id, fifo: false };
    let mut chunk = Batch::with_capacity(frame_schema, keys.remaining().min(chunk_rows));
    loop {
        keys.fill(&mut chunk, chunk_rows);
        let drained = keys.remaining() == 0;
        // Every chunk sends at least one frame, so an empty key set still ends
        // its train.
        let mut start = 0;
        loop {
            start =
                send_train_frame(w2m_writer, route, &chunk, start, budget, drained).expect("a key span fits a frame");
            if start == chunk.len() {
                break;
            }
        }
        if drained {
            break;
        }
    }
}

#[cfg(test)]
#[path = "tests/reply.rs"]
mod tests;
