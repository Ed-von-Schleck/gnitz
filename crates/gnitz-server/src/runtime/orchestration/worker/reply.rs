//! Worker W2M framing: the `WorkerProcess` reply helpers, the `PendingScan`
//! train they queue, and the frame cutter every train is sent through.

use super::*;

use std::borrow::Borrow;

use ipc::{WireData, WireMsg};

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
        flags: WireFlags::train_frame(route.schema_version, last),
        ..Default::default()
    }
}

impl PendingScan {
    /// Emit this train's next frame, returning whether more remain.
    fn emit_next(&mut self, w2m: &W2mWriter, budget: usize) -> Result<bool, gnitz_wire::WireFault> {
        let route = self.route;
        let end = send_train_frame(w2m, route.request_id, &self.batch, self.next_row, budget, |last| {
            reply_frame(route, last)
        })?;
        self.next_row = end;
        Ok(end < self.batch.len())
    }
}

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, target_id: u64, request_id: u32) {
        self.w2m_writer.send_status(target_id, request_id, WireStatus::Ok, &[]);
    }

    /// A control-only frame carrying the fault's own status.
    pub(super) fn send_fault(&self, fault: &gnitz_wire::WireFault, request_id: u32) {
        self.w2m_writer
            .send_status(0, request_id, fault.status, fault.text.as_bytes());
    }

    /// Reply with `batch`: one frame now when it fits and `route.fifo` is clear,
    /// else a train queued behind the others.
    pub(super) fn send_reply(&mut self, route: ReplyRoute, batch: impl Borrow<Batch> + Into<Rc<Batch>>) {
        if !route.fifo
            && emit_whole_if_fits(
                &self.w2m_writer,
                route.request_id,
                batch.borrow(),
                self.reply_frame_budget,
                |last| reply_frame(route, last),
            )
        {
            return;
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

/// Emit `batch` whole as one terminal frame if it fits `budget` — heap and all,
/// over the source batch, so no sub-batch is built. `frame(last)` builds all but
/// the payload.
fn emit_whole_if_fits<'a>(
    w2m: &W2mWriter,
    req: u32,
    batch: &'a Batch,
    budget: usize,
    frame: impl Fn(bool) -> WireMsg<'a>,
) -> bool {
    let msg = WireMsg {
        data: WireData::Whole(batch),
        ..frame(true)
    };
    if !batch.is_empty() && msg.size() > budget {
        return false;
    }
    w2m.send_msg(req, &msg);
    true
}

/// Send the frame of `batch` that starts at row `start`, flagged last when it
/// reaches the end; `Ok` is the row after it. `Err` is a row too wide for any
/// frame.
fn send_train_frame<'a>(
    w2m: &W2mWriter,
    req: u32,
    batch: &'a Batch,
    start: usize,
    budget: usize,
    frame: impl Fn(bool) -> WireMsg<'a>,
) -> Result<usize, gnitz_wire::WireFault> {
    if start == 0 && emit_whole_if_fits(w2m, req, batch, budget, &frame) {
        return Ok(batch.len());
    }
    let (chunk, size) = batch.wire_chunk_within(start, frame(false).size(), budget);
    if size > gnitz_wire::MAX_FRAME_PAYLOAD {
        return Err(crate::runtime::wire::oversized_frame_message(size).into());
    }
    let end = start + chunk.rows();
    w2m.send_msg(
        req,
        &WireMsg {
            data: WireData::of_chunk(batch, start, &chunk),
            ..frame(end == batch.len())
        },
    );
    Ok(end)
}

/// Send all of `batch` now, as frames within `budget`.
pub(super) fn send_train<'a>(
    w2m: &W2mWriter,
    req: u32,
    batch: &'a Batch,
    budget: usize,
    frame: impl Fn(bool) -> WireMsg<'a>,
) -> Result<(), gnitz_wire::WireFault> {
    let mut start = 0;
    loop {
        start = send_train_frame(w2m, req, batch, start, budget, &frame)?;
        if start == batch.len() {
            return Ok(());
        }
    }
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
    keys: &mut gnitz_store::storage::KeyProducer,
) {
    let route = ReplyRoute {
        target_id,
        request_id,
        fifo: false,
        // The synthetic schema has no version.
        schema_version: 0,
    };
    let mut chunk = Batch::with_capacity(frame_schema, keys.remaining().min(chunk_rows));
    loop {
        chunk.clear();
        for _ in 0..keys.remaining().min(chunk_rows) {
            chunk.push_key_row(keys.next().expect("producer lends `remaining` spans"), 1);
        }
        let drained = keys.remaining() == 0;
        send_train(w2m_writer, request_id, &chunk, budget, |last| {
            reply_frame(route, drained && last)
        })
        .expect("a key span fits a frame");
        if drained {
            break;
        }
    }
}
