//! Worker W2M response framing: the `WorkerProcess` reply helpers
//! (ack / schema-block / response / scan-response / streamed-batch / error).

use super::*;

use ipc::{WireData, WireMsg, FRAME_CAP};

// ---------------------------------------------------------------------------
// PendingScan
// ---------------------------------------------------------------------------

/// One reply train queued in `pending_streams`, emitted one frame per
/// `drain_sal` pass in strict FIFO order. The common fields carry the reply's
/// routing/schema identity; `kind` selects the emission shape. `prebuilt_schema`
/// present ⇒ the first frame carries that block; absent ⇒ no schema is written
/// (the `SchemaDescriptor` is deliberately not stored — a prebuilt block
/// supersedes it, and with it omitted no schema is written at all).
pub(super) struct PendingScan {
    pub(super) batch: Rc<Batch>,
    pub(super) request_id: u64,
    pub(super) client_id: u64,
    pub(super) target_id: u64,
    pub(super) prebuilt_schema: Option<Rc<Vec<u8>>>,
    pub(super) server_version: u16,
    pub(super) kind: PendingScanKind,
}

/// The two emission shapes of a `PendingScan`:
///
/// * `WireSafe` — a fixed-width columnar train (empty blob region) emitted as a
///   [`WireData::Range`], chunked across frames when it exceeds
///   `reply_frame_budget`. `next_row` tracks emission progress (`0` ⇒ the first
///   chunk still owes the schema block; non-zero ⇒ a pure-data continuation).
///   This is the only shape a plain scan or an oversized seek-by-index / gather
///   reply produces.
/// * `NonWireSafe` — a STRING/German-string (blob-bearing) result that cannot
///   chunk: exactly one whole-batch frame. Reached only on the multi-scan FIFO
///   path (`force_fifo`), where even an immediate-emit-eligible reply must queue
///   so ring order equals request order; the plain scan path still emits such a
///   reply inline.
pub(super) enum PendingScanKind {
    WireSafe { next_row: usize },
    NonWireSafe,
}

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, target_id: u64, request_id: u64) {
        self.w2m_writer.send_status(target_id, request_id, STATUS_OK, &[]);
    }

    pub(super) fn send_error(&self, error_msg: &str, request_id: u64) {
        self.send_fault(&error_msg.into(), request_id);
    }

    /// One control-only failure frame carrying the fault's **own** status, so a
    /// refusal a worker mints (`STATUS_DELTA_EXPIRED`) reaches the client as a
    /// code rather than as a string. [`Self::send_error`] is this with the status
    /// every ordinary rejection carries.
    ///
    /// `target_id` is `0` for both, and that is not a loss: no master-side reader
    /// of a reply consumes it — the reactor routes by the ring slot, and
    /// `worker_error` reads the status and the text alone.
    pub(super) fn send_fault(&self, fault: &gnitz_wire::WireFault, request_id: u64) {
        self.w2m_writer
            .send_status(0, request_id, fault.status, fault.text.as_bytes());
    }

    /// Reply schema wire block: the table's cached block for `Table`, a
    /// one-off (never cached) block for `OneOff`, and none at all for
    /// `ClientAuthored` — the client wrote that schema and decodes against it.
    /// Returns the block, the schema version, and the schema's wire-safety —
    /// `Table` reads the cached wire-safe bit instead of recomputing it per
    /// reply. This is the only reader of the descriptor: a `WireMsg` carries
    /// schema *bytes*, so a variant that yields no block emits no schema.
    fn reply_schema_block(&mut self, tid_key: i64, schema: ReplySchema<'_>) -> (Option<Rc<Vec<u8>>>, u16, bool) {
        match schema {
            // Version 0: the block is a projected/synthetic schema, so the
            // table's version does not describe it — reporting the table's would
            // let a version-suppression path drop a block the reader still needs.
            ReplySchema::OneOff(s) => {
                let block = Rc::new(gnitz_engine::catalog::encode_schema_block(s, tid_key as u32));
                (Some(block), 0, schema_wire_safe(s))
            }
            ReplySchema::Table(s) => {
                let e = self.cat().schema_wire_entry(tid_key, s);
                (Some(e.block), e.version, e.wire_safe)
            }
            ReplySchema::ClientAuthored(s) => (None, 0, schema_wire_safe(s)),
        }
    }

    /// The single-frame reply message shape shared by `send_response` and
    /// `stream_batch_response`: the whole batch, the resolved schema block, and
    /// the schema version echoed in the wire flags.
    fn whole_batch_msg<'a>(
        target_id: u64,
        result: Option<&'a Batch>,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
        prebuilt: Option<&'a [u8]>,
        server_version: u16,
    ) -> WireMsg<'a> {
        WireMsg {
            target_id,
            client_id,
            flags: gnitz_wire::wire_flags_set_schema_version(0, server_version),
            seek_pk,
            request_id,
            data: WireData::Whole(result),
            schema_block: prebuilt,
            ..Default::default()
        }
    }

    /// Emit `result` as one frame, or fail if that frame would exceed
    /// [`FRAME_CAP`] — a reply that big is unreadable, since it reaches the
    /// client verbatim and `Connection` reads exactly one frame. Checking here
    /// rather than at a call site keeps the bound on every single-frame reply:
    /// `result` is unbounded on the seek path, where a view key names its whole
    /// PK group. The error takes the worker-error path — a `STATUS_ERROR` reply,
    /// connection intact.
    pub(super) fn send_response(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: ReplySchema<'_>,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
    ) -> Result<(), gnitz_wire::WireFault> {
        let (prebuilt_rc, server_version, _) = self.reply_schema_block(target_id as i64, schema);
        let msg = Self::whole_batch_msg(
            target_id,
            result,
            request_id,
            client_id,
            seek_pk,
            prebuilt_rc.as_deref().map(Vec::as_slice),
            server_version,
        );
        let sz = msg.size();
        if sz > FRAME_CAP {
            return Err(format!("reply wire_size={sz} exceeds the maximum frame payload {FRAME_CAP}").into());
        }
        self.w2m_writer.send_msg_sized(request_id, &msg, sz);
        Ok(())
    }

    /// Reply with `result`, chunking through `pending_streams` when it exceeds
    /// one W2M frame. Single-frame replies are byte-identical to
    /// `send_response` (no train flags, `seek_pk` + `request_id` echoed in the
    /// payload), so slot-forwarding consumers see no wire change; only a real
    /// multi-frame train carries FLAG_CONTINUATION / FLAG_SCAN_LAST. The
    /// schema is always embedded on the train's first frame (internal
    /// consumers decode with it; there is no client cache on these paths).
    pub(super) fn stream_batch_response(
        &mut self,
        target_id: u64,
        result: Option<Batch>,
        schema: ReplySchema<'_>,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
    ) -> Result<(), gnitz_wire::WireFault> {
        let Some(batch) = result.filter(|b| b.count > 0) else {
            return self.send_response(target_id, None, schema, request_id, client_id, seek_pk);
        };
        let (prebuilt_rc, server_version, is_wire_safe) = self.reply_schema_block(target_id as i64, schema);
        let msg = Self::whole_batch_msg(
            target_id,
            Some(&batch),
            request_id,
            client_id,
            seek_pk,
            prebuilt_rc.as_deref().map(Vec::as_slice),
            server_version,
        );
        let sz = msg.size();
        // Non-wire-safe replies cannot chunk, so they single-frame up to the
        // hard frame cap; wire-safe replies chunk past the (overridable, and
        // never larger) frame budget.
        let frame_cap = if is_wire_safe {
            self.reply_frame_budget
        } else {
            FRAME_CAP
        };
        if sz <= frame_cap {
            self.w2m_writer.send_msg_sized(request_id, &msg, sz);
            return Ok(());
        }
        if !is_wire_safe {
            return Err(oversized_string_reply(sz));
        }
        self.enqueue_stream(
            Rc::new(batch),
            request_id,
            client_id,
            target_id,
            prebuilt_rc,
            server_version,
        );
        Ok(())
    }

    /// Send a SCAN response for `batch`. For wire-safe schemas, large batches
    /// are split across multiple frames via `pending_streams`; the first chunk
    /// is emitted at the top of the next `drain_sal` pass. For non-wire-safe
    /// (STRING-column) schemas, a single frame is sent; returns an error message
    /// if the batch exceeds [`FRAME_CAP`].
    #[allow(clippy::too_many_arguments)]
    pub(super) fn send_scan_response(
        &mut self,
        target_id: u64,
        batch: Rc<Batch>,
        schema: ReplySchema<'_>,
        request_id: u64,
        client_id: u64,
        client_version: u16,
        force_fifo: bool,
    ) -> Result<(), gnitz_wire::WireFault> {
        // `include_schema` controls whether the first frame carries a schema
        // block; `server_version` is always embedded in the wire flags so the
        // client can cache/verify.
        let (block_rc, server_version, is_wire_safe) = self.reply_schema_block(target_id as i64, schema);
        let prebuilt_rc = block_rc.filter(|_| gnitz_wire::wire_should_include_schema(client_version, server_version));

        if !is_wire_safe {
            // STRING-column tables: no chunking. Check size; error if too big.
            let msg = Self::non_wire_safe_msg(
                target_id,
                &batch,
                client_id,
                prebuilt_rc.as_deref().map(Vec::as_slice),
                server_version,
            );
            let wire_sz = msg.size();
            if wire_sz > FRAME_CAP {
                return Err(oversized_string_reply(wire_sz));
            }
            if force_fifo {
                // Multi-scan: queue the single blob frame so this relation
                // reaches the ring in request order (the FIFO reply contract).
                // The oversize reject above stays at enqueue time —
                // `emit_pending_scan_chunk` is infallible, so only the encode is
                // deferred; the emit rebuilds an identical message.
                self.pending_streams.push_back(PendingScan {
                    batch,
                    request_id,
                    client_id,
                    target_id,
                    prebuilt_schema: prebuilt_rc,
                    server_version,
                    kind: PendingScanKind::NonWireSafe,
                });
                return Ok(());
            }
            self.w2m_writer.send_msg_sized(request_id, &msg, wire_sz);
            return Ok(());
        }

        // Wire-safe path: the range encoder supports chunking.
        // FLAG_CONTINUATION keeps the client reading (a terminal frame signals
        // scan end); FLAG_SCAN_LAST tells the master this worker's train is done.
        let msg = Self::chunk_msg(
            target_id,
            &batch,
            client_id,
            0,
            batch.count,
            prebuilt_rc.as_deref().map(Vec::as_slice),
            server_version,
            true,
        );
        // `force_fifo` queues even a one-frame reply so ring order equals
        // request order; its lone chunk is the same ≤budget frame this branch
        // would have sent.
        let sz = msg.size();
        if !force_fifo && sz <= self.reply_frame_budget {
            self.w2m_writer.send_msg_sized(request_id, &msg, sz);
        } else {
            self.enqueue_stream(batch, request_id, client_id, target_id, prebuilt_rc, server_version);
        }
        Ok(())
    }

    /// Enqueue a multi-frame reply train; its first chunk is emitted at the
    /// top of the next `drain_sal` pass, after any earlier queued train
    /// fully drains (see the `pending_streams` field doc for the FIFO
    /// deadlock-freedom argument).
    fn enqueue_stream(
        &mut self,
        batch: Rc<Batch>,
        request_id: u64,
        client_id: u64,
        target_id: u64,
        prebuilt_schema: Option<Rc<Vec<u8>>>,
        server_version: u16,
    ) {
        self.pending_streams.push_back(PendingScan {
            batch,
            request_id,
            client_id,
            target_id,
            prebuilt_schema,
            server_version,
            kind: PendingScanKind::WireSafe { next_row: 0 },
        });
    }

    /// Emit one frame of the FRONT pending train; pops it off the queue when
    /// its terminal chunk is sent. Called at the top of every `drain_sal` pass
    /// while `pending_streams` is non-empty (see the field doc for why
    /// emission is FIFO and confined to `drain_sal` / `run`). Unit tests set
    /// a small `reply_frame_budget` to force multi-frame trains from small
    /// batches. Dispatches on the front train's shape: a `WireSafe` train emits
    /// its next columnar chunk; a `NonWireSafe` train emits its one blob frame
    /// and always pops.
    pub(super) fn emit_pending_scan_chunk(&mut self) {
        match self.pending_streams.front().map(|p| &p.kind) {
            None => (),
            Some(PendingScanKind::WireSafe { .. }) => self.emit_wire_safe_chunk(),
            Some(PendingScanKind::NonWireSafe) => self.emit_non_wire_safe_frame(),
        }
    }

    /// The one-frame message a non-wire-safe (STRING/blob) scan reply produces:
    /// the whole batch, terminal train flags, and the schema block iff
    /// `prebuilt` is `Some`. Shared by the immediate branch of
    /// `send_scan_response` and the queued `emit_non_wire_safe_frame`
    /// (multi-scan FIFO) so the two produce byte-identical frames.
    fn non_wire_safe_msg<'a>(
        target_id: u64,
        batch: &'a Batch,
        client_id: u64,
        prebuilt: Option<&'a [u8]>,
        server_version: u16,
    ) -> WireMsg<'a> {
        WireMsg {
            target_id,
            client_id,
            flags: gnitz_wire::wire_flags_set_schema_version(FLAG_CONTINUATION | FLAG_SCAN_LAST, server_version),
            data: WireData::Whole(Some(batch)),
            schema_block: prebuilt,
            ..Default::default()
        }
    }

    /// One columnar chunk of a wire-safe reply train: rows
    /// `[start_row, start_row + count)`, the schema block iff `prebuilt` is
    /// `Some`, and FLAG_SCAN_LAST iff `is_last`. FLAG_CONTINUATION is always set
    /// so the client's "stop on no FLAG_CONTINUATION" loop still terminates on
    /// the frame after the last one. The payload `request_id` stays 0: reply
    /// routing rides the W2M slot's ring prefix.
    #[allow(clippy::too_many_arguments)]
    fn chunk_msg<'a>(
        target_id: u64,
        batch: &'a Batch,
        client_id: u64,
        start_row: usize,
        count: usize,
        prebuilt: Option<&'a [u8]>,
        server_version: u16,
        is_last: bool,
    ) -> WireMsg<'a> {
        WireMsg {
            target_id,
            client_id,
            flags: gnitz_wire::wire_flags_set_schema_version(
                FLAG_CONTINUATION | if is_last { FLAG_SCAN_LAST } else { 0 },
                server_version,
            ),
            data: WireData::Range {
                batch,
                start_row,
                count,
            },
            schema_block: prebuilt,
            ..Default::default()
        }
    }

    /// Emit the single blob frame of a `NonWireSafe` front train and pop it. The
    /// oversize reject already fired at enqueue.
    fn emit_non_wire_safe_frame(&mut self) {
        // This train always emits exactly one frame and always pops, so pop it
        // up front and borrow its fields straight into the emit — no clone needed.
        let Some(p) = self.pending_streams.pop_front() else {
            return;
        };
        self.w2m_writer.send_msg(
            p.request_id,
            &Self::non_wire_safe_msg(
                p.target_id,
                &p.batch,
                p.client_id,
                p.prebuilt_schema.as_deref().map(Vec::as_slice),
                p.server_version,
            ),
        );
    }

    /// Emit the next columnar chunk of a `WireSafe` front train, updating its
    /// `next_row` or popping it on the terminal chunk.
    fn emit_wire_safe_chunk(&mut self) {
        let budget = self.reply_frame_budget;
        let (batch, next_row, request_id, client_id, target_id, prebuilt_schema, server_version) = {
            let Some(p) = self.pending_streams.front() else {
                return;
            };
            let PendingScanKind::WireSafe { next_row } = &p.kind else {
                // `emit_pending_scan_chunk` only routes a WireSafe front here, and
                // nothing mutates the queue in between — same invariant the
                // has-more branch below asserts with `unreachable!`.
                unreachable!("emit_wire_safe_chunk: front train is not WireSafe");
            };
            (
                Rc::clone(&p.batch),
                *next_row,
                p.request_id,
                p.client_id,
                p.target_id,
                p.prebuilt_schema.clone(),
                p.server_version,
            )
        };

        // The schema block rides the first chunk only; continuations are pure data.
        let prebuilt: Option<&[u8]> = if next_row == 0 {
            prebuilt_schema.as_deref().map(Vec::as_slice)
        } else {
            None
        };

        let remaining = batch.count - next_row;
        // Rows per chunk, exactly: a chunk costs `base` (ctrl block, plus the
        // schema block on the first chunk) + the data block, and for a wire-safe
        // schema the data block is affine in the row count — `hdr` (its own
        // header and region directory) plus `per_row` per row, with no alignment
        // padding. `base` is sized with a zero-row range, which carries no data
        // block at all, so `hdr` must be added back explicitly; differencing two
        // whole-message sizes across that discontinuity would fold `hdr` into
        // the slope and charge it once per row.
        let base = Self::chunk_msg(
            target_id,
            &batch,
            client_id,
            next_row,
            0,
            prebuilt,
            server_version,
            false,
        )
        .size();
        let hdr = batch.wire_byte_size_range(0);
        // The weight and null-bitmap regions alone are 8 bytes per row each, so
        // this is never zero.
        let per_row = batch.wire_byte_size_range(1) - hdr;
        // `.max(1)` before the clamp to `remaining`: a chunk always carries at
        // least one row (a budget too small for even one would otherwise stall
        // the train), and a zero-row batch — which `force_fifo` can queue —
        // still emits its one terminal frame rather than a row it does not have.
        let max_rows = (budget.saturating_sub(base + hdr) / per_row).max(1).min(remaining);
        let has_more = next_row + max_rows < batch.count;
        self.w2m_writer.send_msg(
            request_id,
            &Self::chunk_msg(
                target_id,
                &batch,
                client_id,
                next_row,
                max_rows,
                prebuilt,
                server_version,
                !has_more,
            ),
        );

        if has_more {
            match self.pending_streams.front_mut().map(|p| &mut p.kind) {
                Some(PendingScanKind::WireSafe { next_row: nr, .. }) => *nr = next_row + max_rows,
                _ => unreachable!("emit_wire_safe_chunk: front train changed shape mid-emit"),
            }
        } else {
            self.pending_streams.pop_front();
        }
    }
}

/// A blob-bearing (STRING/German-string) reply cannot be split across frames, so
/// one too large for a single frame has no way out.
///
/// The remedy names a projection first because narrowing the row count is not
/// always one: a join builds a row out of two that each fit, and no predicate or
/// `LIMIT` makes that single row returnable.
fn oversized_string_reply(sz: usize) -> gnitz_wire::WireFault {
    gnitz_wire::WireFault::from(format!(
        "reply wire_size={sz} exceeds the maximum frame payload {FRAME_CAP}; a STRING-column \
         result cannot be chunked — project fewer STRING columns, or match fewer rows. \
         One row wider than the cap cannot be returned at all."
    ))
}

/// Stream the sorted OPK leading-key spans lent by `keys` to the master as a
/// train of continuation frames carrying the synthetic pre-flight frame schema
/// (`frame_schema` = `unique_preflight_wire_schema`, whose PK region is exactly
/// the span; each span is written into the PK region verbatim). Every frame is
/// tagged `FLAG_CONTINUATION`; the terminal frame additionally `FLAG_SCAN_LAST`.
/// An empty producer emits one empty terminal frame so the master's drain still
/// sees the train end.
///
/// `keys` lends one span at a time so the whole set never has to exist in RAM
/// at once, and its exact `remaining` count sizes each frame and marks the
/// terminal one without lookahead. It is infallible: the fast path reads its
/// own buffer, and the merge reads mapped spill memory (all fallible spill I/O
/// ran in `SpillSort::finish` before the first frame), so there is no
/// mid-stream read that could truncate the train under an I/O error.
///
/// Deliberately NOT `send_scan_response`: that path attaches the owner
/// table's *cached* schema wire block, which would make the master decode
/// these frames with the table's row stride, and its `pending_streams`
/// chunking would require materialising all keys as one 32 B/row `Batch`. The
/// synthetic schema's wire block is built one-off (the `ReplySchema::OneOff`
/// pattern) and never written to the table-keyed schema-block cache, so the
/// table's cached block is never poisoned. `send_encoded` blocks on a full ring
/// until the master's merge drains it — acceptable backpressure: the worker has
/// nothing else to do during the DDL window.
pub(crate) fn send_unique_preflight_keys(
    w2m_writer: &W2mWriter,
    target_id: u64,
    frame_schema: &SchemaDescriptor,
    request_id: u64,
    keys_per_frame: usize,
    keys: &mut gnitz_engine::storage::KeyProducer,
) {
    debug_assert!(keys_per_frame > 0, "keys_per_frame must be positive");
    let schema_block = gnitz_engine::catalog::encode_schema_block(frame_schema, target_id as u32);

    // Reusable chunk batch: filled, encoded, and cleared per frame, sized up
    // front to exactly one frame's fill.
    let mut chunk = Batch::with_capacity(*frame_schema, keys.remaining().min(keys_per_frame));
    let mut is_first = true;
    loop {
        chunk.clear();
        let n = keys.remaining().min(keys_per_frame);
        for _ in 0..n {
            let k = keys.next().expect("producer lends `remaining` spans");
            chunk.ensure_row_capacity();
            // The span is already OPK; write the raw bytes into the PK region
            // (len == pk_stride). The master reads them back verbatim via
            // `mb.get_pk_bytes(row)` → `PkBuf` — the wire is byte-transparent.
            chunk.extend_pk_bytes(k);
            chunk.extend_weight(&1i64.to_le_bytes());
            chunk.extend_null_bmp(&0u64.to_le_bytes());
            chunk.count += 1;
        }
        let is_last = keys.remaining() == 0;
        // Schema block only on the first frame; continuations decode against
        // the master's saved schema hint (the synthetic schema's version is 0,
        // so no version needs embedding in the flags).
        let msg = WireMsg {
            target_id,
            flags: FLAG_CONTINUATION | if is_last { FLAG_SCAN_LAST } else { 0 },
            data: WireData::Range {
                batch: &chunk,
                start_row: 0,
                count: chunk.count,
            },
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
