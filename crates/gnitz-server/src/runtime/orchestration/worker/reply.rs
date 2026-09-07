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

/// The wire flags every frame of a train carries — the convention
/// `train_has_more` reads back. FLAG_CONTINUATION is always set so the client's
/// "stop on no FLAG_CONTINUATION" loop still terminates on the frame after the
/// last one; FLAG_SCAN_LAST marks this worker's terminal frame.
pub(super) fn train_flags(server_version: u16, is_last: bool) -> u64 {
    let last = if is_last { FLAG_SCAN_LAST } else { 0 };
    gnitz_wire::wire_flags_set_schema_version(FLAG_CONTINUATION | last, server_version)
}

/// One frame of a worker reply, but for its payload.
fn reply_frame<'a>(route: ReplyRoute, block: Option<&'a [u8]>, server_version: u16, last: bool) -> WireMsg<'a> {
    WireMsg {
        target_id: route.target_id,
        client_id: route.client_id,
        flags: train_flags(server_version, last),
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
        let chunk = self.batch.wire_chunk_within(start, overhead, budget);
        let size = overhead + chunk.wire_byte_size();
        // Over `budget` only at one row, since `budget <= FRAME_CAP`; over
        // FRAME_CAP there is nothing left to narrow.
        if size > FRAME_CAP {
            return Err(oversized_reply(size));
        }
        let has_more = start + chunk.len() < self.batch.len();
        w2m.send_msg(
            self.route.request_id,
            &WireMsg {
                flags: train_flags(self.server_version, !has_more),
                data: WireData::Whole(Some(&chunk)),
                ..base
            },
        );
        self.next_row = start + chunk.len();
        Ok(has_more)
    }
}

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, target_id: u64, request_id: u64) {
        self.w2m_writer.send_status(target_id, request_id, STATUS_OK, &[]);
    }

    /// A rejection that carries no status of its own, and so is `STATUS_ERROR`
    /// — [`Self::send_fault`] with the status every ordinary refusal carries.
    pub(super) fn send_error(&self, error_msg: &str, request_id: u64) {
        self.w2m_writer
            .send_status(0, request_id, gnitz_wire::STATUS_ERROR, error_msg.as_bytes());
    }

    /// One control-only failure frame carrying the fault's **own** status, so a
    /// refusal a worker mints (`STATUS_DELTA_EXPIRED`) reaches the client as a
    /// code rather than as a string. `target_id` `0`: the reactor routes a reply
    /// by its ring slot, and `worker_error` reads status and text alone.
    pub(super) fn send_fault(&self, fault: &gnitz_wire::WireFault, request_id: u64) {
        self.w2m_writer
            .send_status(0, request_id, fault.status, fault.text.as_bytes());
    }

    /// The block a reply of this [`ReplySchema`] carries, and the schema version
    /// its flags report — which the caller stamps even when the block is
    /// suppressed, so the version cannot ride inside the `Option`.
    fn reply_schema_block(
        &mut self,
        tid_key: i64,
        schema: ReplySchema<'_>,
        client_version: u16,
    ) -> (Option<Rc<Vec<u8>>>, u16) {
        match schema {
            // Version 0: the table's version does not describe a projected
            // schema, and reporting it would let the suppression below drop a
            // block the reader still needs. At 0 nothing is ever suppressed.
            ReplySchema::OneOff(s) => (
                Some(Rc::new(crate::catalog::encode_schema_block_ipc(s, tid_key as u32))),
                0,
            ),
            // The dispatch arm already resolved the descriptor, so the
            // negotiation never has to look one up and cannot miss.
            ReplySchema::Table(s) => self
                .cat()
                .negotiated_schema_block(tid_key, client_version, |_| Some(*s)),
            ReplySchema::ClientAuthored => (None, 0),
        }
    }

    /// Emit `result` as one frame carrying no train flags — the shape whose
    /// consumer (`expect_single_frame`, for Seek) rejects a train rather than
    /// drain one.
    ///
    /// So it cannot split: a seek's result is unbounded (a view key names its
    /// whole PK group) and past [`FRAME_CAP`] no client can read the frame, so
    /// this refuses instead of emitting it.
    pub(super) fn send_response(
        &mut self,
        route: ReplyRoute,
        result: Option<&Batch>,
        schema: ReplySchema<'_>,
        seek_pk: u128,
        client_version: u16,
    ) -> Result<(), gnitz_wire::WireFault> {
        let (block, server_version) = self.reply_schema_block(route.target_id as i64, schema, client_version);
        let msg = WireMsg {
            target_id: route.target_id,
            client_id: route.client_id,
            flags: gnitz_wire::wire_flags_set_schema_version(0, server_version),
            seek_pk,
            request_id: route.request_id,
            data: WireData::Whole(result),
            schema_block: block.as_deref().map(Vec::as_slice),
            ..Default::default()
        };
        let sz = msg.size();
        if sz > FRAME_CAP {
            return Err(oversized_reply(sz));
        }
        self.w2m_writer.send_msg(route.request_id, &msg);
        Ok(())
    }

    /// Reply with an **owned** `batch`: one frame when it fits, otherwise queued
    /// and split, a frame per `drain_sal` pass. A STRING-column result splits
    /// like any other.
    ///
    /// The fit is tested by reference, so the single-frame case never reaches
    /// `Rc::new`; [`Self::send_shared_scan_response`] is for a caller that
    /// genuinely shares. `force_fifo` queues even a fitting reply, so this
    /// request reaches the ring in request order — see
    /// `dispatch_scan_multi_fanout`, which owns that flag.
    pub(super) fn send_scan_response(
        &mut self,
        route: ReplyRoute,
        batch: Batch,
        schema: ReplySchema<'_>,
        client_version: u16,
    ) {
        let (block, version) = self.reply_schema_block(route.target_id as i64, schema, client_version);
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

    /// [`Self::send_scan_response`] for a batch this worker keeps a handle to —
    /// a cached full-scan snapshot.
    ///
    /// A `route` marked `fifo` queues even a fitting reply so this relation
    /// reaches the ring in request order (the multi-scan FIFO contract).
    pub(super) fn send_shared_scan_response(
        &mut self,
        route: ReplyRoute,
        batch: Rc<Batch>,
        schema: ReplySchema<'_>,
        client_version: u16,
    ) {
        let (block, version) = self.reply_schema_block(route.target_id as i64, schema, client_version);
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
        data: WireData::Whole(Some(batch)),
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

/// What one pre-flight frame spends on everything that is not a key: the control
/// block, the schema block, and the data block's header at zero rows. Charged
/// against the budget before it is divided into keys, or the frame runs a
/// kilobyte or two over. The first frame is the widest, so it bounds them all.
pub(crate) fn preflight_frame_overhead(frame_schema: &SchemaDescriptor, schema_block: &[u8]) -> usize {
    let framing = reply_frame(ReplyRoute::default(), Some(schema_block), 0, true).size();
    framing + gnitz_store::storage::wire_block_size(frame_schema, 0, 0)
}

/// Keys one pre-flight frame may carry: [`unique_preflight_keys_per_frame`]
/// clamped so the whole frame — `overhead` included — stays inside `budget`.
/// Unclamped, that test-only override builds a frame the W2M ring cannot hold,
/// which `w2m::try_reserve` asserts against and which aborts in release.
///
/// Charging per key over-counts the region alignment `block_size_from` does
/// once, so the count is conservative; `send_unique_preflight_keys` asserts the
/// frame it actually builds.
pub(crate) fn preflight_keys_per_frame(frame_schema: &SchemaDescriptor, budget: usize, overhead: usize) -> usize {
    let per_key = gnitz_store::storage::wire_block_size(frame_schema, 1, 0)
        - gnitz_store::storage::wire_block_size(frame_schema, 0, 0);
    unique_preflight_keys_per_frame()
        .min(budget.saturating_sub(overhead) / per_key.max(1))
        .max(1)
}

/// Stream the sorted OPK leading-key spans `keys` lends to the master as a train
/// over `frame_schema` (`unique_preflight_wire_schema`, whose PK region is
/// exactly one span). An empty producer emits one empty terminal frame so the
/// master's drain still sees the train end.
///
/// Deliberately NOT `send_scan_response`: that path materializes the reply as
/// one `Batch`, which is what `keys` exists to avoid — this refills one chunk
/// batch per frame.
///
/// It also emits the whole train synchronously, which `pending_streams` forbids
/// inside an exchange wait. Safe here alone: this runs under the catalog WRITE
/// lock, which excludes `handle_scan` — the only thing that could be holding the
/// ring slots a blocked `send_msg` would wait behind.
pub(crate) fn send_unique_preflight_keys(
    w2m_writer: &W2mWriter,
    target_id: u64,
    frame_schema: &SchemaDescriptor,
    request_id: u64,
    budget: usize,
    keys: &mut gnitz_store::storage::KeyProducer,
) {
    // The master's merge decodes every reply block through `decode_wire_ipc`,
    // which verifies no checksum.
    let schema_block = crate::catalog::encode_schema_block_ipc(frame_schema, target_id as u32);
    // Measured off the block this train actually ships, so what is charged and
    // what is emitted cannot drift; the assertion in the loop is what says so.
    let keys_per_frame = preflight_keys_per_frame(
        frame_schema,
        budget,
        preflight_frame_overhead(frame_schema, &schema_block),
    );

    // Reusable chunk batch: filled, encoded, and cleared per frame, sized up
    // front to exactly one frame's fill.
    let mut chunk = Batch::with_capacity(frame_schema, keys.remaining().min(keys_per_frame));
    let mut is_first = true;
    loop {
        chunk.clear();
        let n = keys.remaining().min(keys_per_frame);
        for _ in 0..n {
            let k = keys.next().expect("producer lends `remaining` spans");
            // The span is already OPK and `len == pk_stride`, so it lands in
            // the PK region unchanged; `merge_index_scan` states what that
            // buys.
            chunk.push_key_row(k, 1);
        }
        let is_last = keys.remaining() == 0;
        // Schema block only on the first frame; continuations decode against the
        // master's saved schema hint. The synthetic schema has no version.
        let msg = WireMsg {
            target_id,
            flags: train_flags(0, is_last),
            data: WireData::Whole(Some(&chunk)),
            schema_block: is_first.then_some(schema_block.as_slice()),
            ..Default::default()
        };
        // Over `budget` only at the one-key floor, where there is nothing left
        // to narrow — the same rule `PendingScan::emit_next` states.
        debug_assert!(
            msg.size() <= budget || n <= 1,
            "a pre-flight frame of {n} keys is {} bytes over its {budget}-byte budget",
            msg.size().saturating_sub(budget),
        );
        w2m_writer.send_msg(request_id, &msg);
        is_first = false;
        if is_last {
            break;
        }
    }
}
