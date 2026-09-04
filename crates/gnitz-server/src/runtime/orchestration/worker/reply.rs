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

/// The wire flags every frame of a reply train carries. FLAG_CONTINUATION is
/// always set so the client's "stop on no FLAG_CONTINUATION" loop still
/// terminates on the frame after the last one; FLAG_SCAN_LAST marks this
/// worker's terminal frame.
fn train_flags(server_version: u16, is_last: bool) -> u64 {
    let last = if is_last { FLAG_SCAN_LAST } else { 0 };
    gnitz_wire::wire_flags_set_schema_version(FLAG_CONTINUATION | last, server_version)
}

impl PendingScan {
    /// The routing and schema fields every frame of this train carries.
    fn base_frame(&self) -> WireMsg<'_> {
        WireMsg {
            target_id: self.route.target_id,
            client_id: self.route.client_id,
            // The schema block rides the first frame only.
            schema_block: (self.next_row == 0)
                .then(|| self.prebuilt_schema.as_deref().map(Vec::as_slice))
                .flatten(),
            ..Default::default()
        }
    }

    /// Emit the whole batch as one terminal frame if it fits `budget` — heap and
    /// all, over the source batch, so no sub-batch is built. `false` leaves the
    /// train untouched for [`Self::emit_next`] to split.
    fn emit_if_whole_fits(&mut self, w2m: &W2mWriter, budget: usize) -> bool {
        let msg = WireMsg {
            flags: train_flags(self.server_version, true),
            data: WireData::Whole(Some(&self.batch)),
            ..self.base_frame()
        };
        // A row-less train has no data block to shrink, so it goes at any budget.
        if !self.batch.is_empty() && msg.size() > budget {
            return false;
        }
        w2m.send_msg(self.route.request_id, &msg);
        self.next_row = self.batch.len();
        true
    }

    /// Emit this train's next frame, returning whether more remain.
    fn emit_next(&mut self, w2m: &W2mWriter, budget: usize) -> Result<bool, gnitz_wire::WireFault> {
        if self.next_row == 0 && self.emit_if_whole_fits(w2m, budget) {
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
    ///
    /// A `Table` block is fetched only when the client's copy is stale: reading
    /// the version first is safe because `clear_col_cache_no_bump` drops the
    /// cache entry *before* the bump, so a surviving entry always matches.
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
            ReplySchema::Table(s) => {
                let server_version = self.cat().get_schema_version(tid_key);
                let block = gnitz_wire::wire_should_include_schema(client_version, server_version)
                    .then(|| self.cat().schema_wire_entry(tid_key, s).block);
                (block, server_version)
            }
            ReplySchema::ClientAuthored => (None, 0),
        }
    }

    /// Emit `result` as one frame carrying no train flags — the shape whose
    /// consumers (`expect_single_frame` for Seek, `execute_pipeline` for HasPk)
    /// reject a train rather than drain one.
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

    /// Reply with `batch` as a train: one frame when it fits, otherwise queued
    /// and split, a frame per `drain_sal` pass. The one scan-shaped reply path —
    /// a STRING-column result splits like any other.
    ///
    /// `force_fifo` queues even a fitting reply so this relation reaches the
    /// ring in request order (the multi-scan FIFO contract).
    pub(super) fn send_scan_response(
        &mut self,
        route: ReplyRoute,
        batch: Rc<Batch>,
        schema: ReplySchema<'_>,
        client_version: u16,
        force_fifo: bool,
    ) {
        let (prebuilt_schema, server_version) = self.reply_schema_block(route.target_id as i64, schema, client_version);
        let mut train = PendingScan {
            batch,
            route,
            prebuilt_schema,
            server_version,
            next_row: 0,
        };
        if force_fifo || !train.emit_if_whole_fits(&self.w2m_writer, self.reply_frame_budget) {
            self.pending_streams.push_back(train);
        }
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

/// A reply frame one row wide that still exceeds what a client can read. The
/// row count is already at its floor, so there is nothing left to narrow.
fn oversized_reply(sz: usize) -> gnitz_wire::WireFault {
    gnitz_wire::WireFault::from(crate::runtime::wire::oversized_frame_message(sz))
}

/// Keys one pre-flight frame may carry: [`unique_preflight_keys_per_frame`]
/// clamped so the frame's data block stays inside `budget`. Unclamped, that
/// test-only override reaches `w2m::try_reserve`'s `MAX_W2M_MSG` assertion,
/// which aborts in release.
pub(crate) fn preflight_keys_per_frame(frame_schema: &SchemaDescriptor, budget: usize) -> usize {
    let per_key = gnitz_store::storage::wire_block_size(frame_schema, 1, 0)
        - gnitz_store::storage::wire_block_size(frame_schema, 0, 0);
    unique_preflight_keys_per_frame().min(budget / per_key.max(1)).max(1)
}

/// Stream the sorted OPK leading-key spans `keys` lends to the master as a train
/// over `frame_schema` (`unique_preflight_wire_schema`, whose PK region is
/// exactly one span). An empty producer emits one empty terminal frame so the
/// master's drain still sees the train end.
///
/// Deliberately NOT `send_scan_response`: that path materializes the reply as
/// one `Batch`, which is what `keys` exists to avoid — this refills one chunk
/// batch per frame. It also runs synchronously inside a DDL window, where
/// blocking on a full ring is wanted rather than the cooperative
/// one-frame-per-`drain_sal` discipline.
pub(crate) fn send_unique_preflight_keys(
    w2m_writer: &W2mWriter,
    target_id: u64,
    frame_schema: &SchemaDescriptor,
    request_id: u64,
    keys_per_frame: usize,
    keys: &mut gnitz_store::storage::KeyProducer,
) {
    debug_assert!(keys_per_frame > 0, "keys_per_frame must be positive");
    // The master's merge decodes every reply block through `decode_wire_ipc`,
    // which verifies no checksum.
    let schema_block = crate::catalog::encode_schema_block_ipc(frame_schema, target_id as u32);

    // Reusable chunk batch: filled, encoded, and cleared per frame, sized up
    // front to exactly one frame's fill.
    let mut chunk = Batch::with_capacity(*frame_schema, keys.remaining().min(keys_per_frame));
    let mut is_first = true;
    loop {
        chunk.clear();
        let n = keys.remaining().min(keys_per_frame);
        for _ in 0..n {
            let k = keys.next().expect("producer lends `remaining` spans");
            // The span is already OPK; the raw bytes go into the PK region
            // (len == pk_stride). The master reads them back verbatim via
            // `mb.get_pk_bytes(row)` → `PkBuf` — the wire is byte-transparent.
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
        w2m_writer.send_msg(request_id, &msg);
        is_first = false;
        if is_last {
            break;
        }
    }
}
