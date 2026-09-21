//! Worker W2M response framing: the `WorkerProcess` reply helpers, and the
//! `PendingScan` train they queue when a reply outgrows one frame.

use super::*;

use ipc::{WireData, WireMsg, FRAME_CAP};

// ---------------------------------------------------------------------------
// PendingScan
// ---------------------------------------------------------------------------

/// One reply train queued in `pending_streams`, emitted one frame per
/// `drain_sal` pass in strict FIFO order.
pub(super) struct PendingScan {
    pub(super) batch: Rc<Batch>,
    pub(super) route: ReplyRoute,
    /// The block the first frame carries; `None` emits no schema at all.
    pub(super) prebuilt_schema: Option<Rc<Vec<u8>>>,
    pub(super) server_version: u16,
    /// Rows already emitted. `0` ⇒ the next frame still owes the schema block.
    pub(super) next_row: usize,
}

/// One frame of a worker reply, but for its payload.
fn reply_frame<'a>(route: ReplyRoute, block: Option<&'a [u8]>, server_version: u16, last: bool) -> WireMsg<'a> {
    WireMsg {
        target_id: route.target_id,
        flags: WireFlags::train_frame(server_version, last),
        schema_block: block,
        ..Default::default()
    }
}

impl PendingScan {
    /// This train's next frame, but for its payload and its terminal flag —
    /// which `emit_next` can only set once it has sized the chunk.
    fn base_frame(&self) -> WireMsg<'_> {
        // The schema block rides the first frame only.
        let block = (self.next_row == 0)
            .then(|| self.prebuilt_schema.as_deref().map(Vec::as_slice))
            .flatten();
        reply_frame(self.route, block, self.server_version, false)
    }

    /// Emit this train's next frame, returning whether more remain.
    fn emit_next(&mut self, w2m: &W2mWriter, budget: usize) -> Result<bool, gnitz_wire::WireFault> {
        if self.next_row == 0
            && emit_whole_if_fits(
                w2m,
                self.route,
                self.prebuilt_schema.as_deref().map(Vec::as_slice),
                self.server_version,
                &self.batch,
                budget,
            )
        {
            self.next_row = self.batch.len();
            return Ok(false);
        }
        let base = self.base_frame();
        let overhead = base.size();
        let start = self.next_row;
        let (chunk, size) = self.batch.wire_chunk_within(start, overhead, budget);
        // Over `budget` only at one row, since `budget <= FRAME_CAP`; over
        // FRAME_CAP there is nothing left to narrow.
        if size > FRAME_CAP {
            return Err(oversized_reply(size));
        }
        let has_more = start + chunk.rows() < self.batch.len();
        w2m.send_msg(
            self.route.request_id,
            &WireMsg {
                flags: WireFlags::train_frame(self.server_version, !has_more),
                data: WireData::of_chunk(&self.batch, start, &chunk),
                ..base
            },
        );
        self.next_row = start + chunk.rows();
        Ok(has_more)
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

    /// Reply with an **owned** `batch`: one frame when it fits, otherwise queued
    /// and split, a frame per `drain_sal` pass. A STRING-column result splits
    /// like any other.
    ///
    /// The fit is tested by reference, so the single-frame case never reaches
    /// `Rc::new`. A `route.fifo` reply is queued even when it fits.
    pub(super) fn send_scan_response(
        &mut self,
        route: ReplyRoute,
        batch: Batch,
        block: Option<Rc<Vec<u8>>>,
        version: u16,
    ) {
        if !route.fifo
            && emit_whole_if_fits(
                &self.w2m_writer,
                route,
                block.as_deref().map(Vec::as_slice),
                version,
                &batch,
                self.reply_frame_budget,
            )
        {
            return;
        }
        self.queue_train(route, Rc::new(batch), block, version);
    }

    /// [`Self::send_scan_response`] for a batch already behind an `Rc` — a cached
    /// full-scan snapshot, or a `ReadSpec` reply that may be one.
    pub(super) fn send_shared_scan_response(
        &mut self,
        route: ReplyRoute,
        batch: Rc<Batch>,
        block: Option<Rc<Vec<u8>>>,
        version: u16,
    ) {
        if !route.fifo
            && emit_whole_if_fits(
                &self.w2m_writer,
                route,
                block.as_deref().map(Vec::as_slice),
                version,
                &batch,
                self.reply_frame_budget,
            )
        {
            return;
        }
        self.queue_train(route, batch, block, version);
    }

    /// Queue a reply that did not go out whole, for `emit_pending_scan_chunk` to
    /// split one frame per `drain_sal` pass.
    fn queue_train(
        &mut self,
        route: ReplyRoute,
        batch: Rc<Batch>,
        prebuilt_schema: Option<Rc<Vec<u8>>>,
        server_version: u16,
    ) {
        self.pending_streams.push_back(PendingScan {
            batch,
            route,
            prebuilt_schema,
            server_version,
            next_row: 0,
        });
    }

    /// Emit one frame of the FRONT pending train, popping it once its terminal
    /// frame is sent or it faults. Called at the top of every `drain_sal` pass
    /// (see the `pending_streams` field doc for why emission is FIFO and
    /// confined there). A mid-train fault is deliverable: `parse_train_header`
    /// errors on any non-zero status, so every drain aborts the train.
    pub(super) fn emit_pending_scan_chunk(&mut self) {
        let budget = self.reply_frame_budget;
        // Disjoint field borrows: the train stays in place across the send.
        let Some(train) = self.pending_streams.front_mut() else {
            return;
        };
        let outcome = train.emit_next(&self.w2m_writer, budget);
        let request_id = train.route.request_id;
        if !matches!(outcome, Ok(true)) {
            self.pending_streams.pop_front();
        }
        if let Err(fault) = outcome {
            self.send_fault(&fault, request_id);
        }
    }
}

/// Emit `batch` whole as one terminal frame if it fits `budget` — heap and all,
/// over the source batch, so no sub-batch is built. `false` means it must be
/// split into a train. By reference, so an owned reply can be tested for fit
/// before anything decides whether it needs an `Rc`.
fn emit_whole_if_fits(
    w2m: &W2mWriter,
    route: ReplyRoute,
    block: Option<&[u8]>,
    server_version: u16,
    batch: &Batch,
    budget: usize,
) -> bool {
    let msg = WireMsg {
        data: WireData::Whole(batch),
        ..reply_frame(route, block, server_version, true)
    };
    // A row-less train has no data block to shrink, so it goes at any budget.
    if !batch.is_empty() && msg.size() > budget {
        return false;
    }
    w2m.send_msg(route.request_id, &msg);
    true
}

/// A reply frame one row wide that still exceeds what a client can read. The
/// row count is already at its floor, so there is nothing left to narrow.
fn oversized_reply(sz: usize) -> gnitz_wire::WireFault {
    gnitz_wire::WireFault::from(crate::runtime::wire::oversized_frame_message(sz))
}

/// Stream `keys` to the master as a train over `frame_schema`, one span per
/// row's PK region, `chunk_rows` spans in RAM at a time. An empty producer
/// still sends the one terminal frame the master's drain waits for. Sends
/// synchronously, so the caller must hold no queued train.
pub(crate) fn send_unique_preflight_keys(
    w2m_writer: &W2mWriter,
    target_id: u64,
    frame_schema: &SchemaDescriptor,
    request_id: u32,
    budget: usize,
    chunk_rows: usize,
    keys: &mut gnitz_store::storage::KeyProducer,
) {
    let route = ReplyRoute { target_id, request_id, fifo: false };
    // The synthetic schema has no version.
    let frame = |last| reply_frame(route, None, 0, last);
    let overhead = frame(false).size();
    let mut chunk = Batch::with_capacity(frame_schema, keys.remaining().min(chunk_rows));
    loop {
        chunk.clear();
        for _ in 0..keys.remaining().min(chunk_rows) {
            chunk.push_key_row(keys.next().expect("producer lends `remaining` spans"), 1);
        }
        let drained = keys.remaining() == 0;
        let mut start = 0;
        loop {
            let (cut, _) = chunk.wire_chunk_within(start, overhead, budget);
            let end = start + cut.rows();
            w2m_writer.send_msg(
                request_id,
                &WireMsg {
                    data: WireData::of_chunk(&chunk, start, &cut),
                    ..frame(drained && end == chunk.len())
                },
            );
            start = end;
            if start == chunk.len() {
                break;
            }
        }
        if drained {
            break;
        }
    }
}
