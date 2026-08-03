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
///   chunk still owes the schema block; non-zero ⇒ a pure-data continuation);
///   `wire_row_stride` is the constant per-row wire size, computed once at
///   enqueue so each chunk recomputes only the frame base. This is the only
///   shape a plain scan or an oversized seek-by-index / gather reply produces.
/// * `NonWireSafe` — a STRING/German-string (blob-bearing) result that cannot
///   chunk: exactly one whole-batch frame. Reached only on the multi-scan FIFO
///   path (`force_fifo`), where even an immediate-emit-eligible reply must queue
///   so ring order equals request order; the plain scan path still emits such a
///   reply inline.
pub(super) enum PendingScanKind {
    WireSafe { next_row: usize, wire_row_stride: usize },
    NonWireSafe,
}

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, target_id: u64, request_id: u64) {
        self.w2m_writer.send_status(target_id, request_id, STATUS_OK, &[]);
    }

    pub(super) fn send_error(&self, error_msg: &str, request_id: u64) {
        self.w2m_writer
            .send_status(0, request_id, STATUS_ERROR, error_msg.as_bytes());
    }

    /// Reply schema wire block: the table's cached block for `Table`, a
    /// one-off (never cached) block for `OneOff`, and none at all for
    /// `ClientAuthored` — the client wrote that schema and decodes against it.
    /// Returns the block, the schema version, and the schema's wire-safety
    /// (`(None, 0, true)` for `ReplySchema::None`) — `Table` reads the cached
    /// wire-safe bit instead of recomputing it per reply. This is the only
    /// reader of the descriptor: the encoders take the block, never the
    /// descriptor, so a variant that emits no block emits no schema.
    fn reply_schema_block(&mut self, tid_key: i64, schema: ReplySchema<'_>) -> (Option<Rc<Vec<u8>>>, u16, bool) {
        match schema {
            ReplySchema::None => (None, 0, true),
            // Version 0: the block is a projected/synthetic schema, so the
            // table's version does not describe it — reporting the table's would
            // let a version-suppression path drop a block the reader still needs.
            ReplySchema::OneOff(s) => {
                let block = Rc::new(ipc::build_schema_wire_block(s, &[], 0, tid_key as u32));
                (Some(block), 0, schema_wire_safe(s))
            }
            ReplySchema::Table(s) => {
                let e = ipc::get_or_build_schema_wire_block(self.cat(), tid_key, s);
                (Some(e.entry.block), e.version, e.entry.wire_safe)
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
            prebuilt_schema_block: prebuilt,
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
    ) -> Result<(), String> {
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
            return Err(format!(
                "reply wire_size={sz} exceeds the maximum frame payload {FRAME_CAP}"
            ));
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
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
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
        // Per-row wire stride, computed once for the train (wire-safe schemas
        // only reach here, so the stride is constant across chunks).
        let row_span = |rows| Self::chunk_msg(target_id, &batch, client_id, 0, rows, None, server_version, true).size();
        let wire_row_stride = row_span(1) - row_span(0);
        self.pending_streams.push_back(PendingScan {
            batch,
            request_id,
            client_id,
            target_id,
            prebuilt_schema,
            server_version,
            kind: PendingScanKind::WireSafe {
                next_row: 0,
                wire_row_stride,
            },
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
            prebuilt_schema_block: prebuilt,
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
            prebuilt_schema_block: prebuilt,
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
        let (batch, next_row, request_id, client_id, target_id, prebuilt_schema, server_version, per_row) = {
            let Some(p) = self.pending_streams.front() else {
                return;
            };
            let PendingScanKind::WireSafe {
                next_row,
                wire_row_stride,
            } = &p.kind
            else {
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
                *wire_row_stride,
            )
        };

        // The schema block rides the first chunk only; continuations are pure data.
        let prebuilt: Option<&[u8]> = if next_row == 0 {
            prebuilt_schema.as_deref().map(Vec::as_slice)
        } else {
            None
        };

        let remaining = batch.count - next_row;
        // Rows per chunk via linear interpolation: wire-safe schemas have a
        // constant per-row stride (stored at enqueue), so wire size is linear
        // in count and only the frame base (schema block on the first chunk)
        // needs recomputing per chunk.
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
        let max_rows = match budget.saturating_sub(base).checked_div(per_row) {
            Some(rows) => rows.max(1).min(remaining),
            None => remaining.max(1), // per_row == 0: constant wire size, send all
        };
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
fn oversized_string_reply(sz: usize) -> String {
    format!(
        "reply wire_size={sz} exceeds the maximum frame payload {FRAME_CAP}; a STRING-column \
         result cannot be chunked — add a tighter predicate or a LIMIT"
    )
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
    keys: &mut crate::storage::KeyProducer,
) {
    debug_assert!(keys_per_frame > 0, "keys_per_frame must be positive");
    let schema_block = ipc::build_schema_wire_block(frame_schema, &[], 0, target_id as u32);

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
            prebuilt_schema_block: is_first.then_some(schema_block.as_slice()),
            ..Default::default()
        };
        w2m_writer.send_msg(request_id, &msg);
        is_first = false;
        if is_last {
            break;
        }
    }
}
