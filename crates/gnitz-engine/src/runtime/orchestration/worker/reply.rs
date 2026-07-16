//! Worker W2M response framing: the `WorkerProcess` reply helpers
//! (ack / schema-block / response / scan-response / streamed-batch / error).

use super::*;

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
/// * `WireSafe` — a fixed-width columnar train (empty blob region) emitted via
///   `encode_wire_into_range`, chunked across frames when it exceeds
///   `reply_frame_budget`. `next_row` tracks emission progress (`0` ⇒ the first
///   chunk still owes the schema block; non-zero ⇒ a pure-data continuation);
///   `wire_row_stride` is the constant per-row wire size, computed once at
///   enqueue so each chunk recomputes only the frame base. This is the only
///   shape a plain scan or an oversized seek/gather reply produces.
/// * `NonWireSafe` — a STRING/German-string (blob-bearing) result that cannot
///   chunk: exactly one frame via the blob-capable `encode_wire_into`. Reached
///   only on the multi-scan FIFO path (`force_fifo`), where even an
///   immediate-emit-eligible reply must queue so ring order equals request
///   order; the plain scan path still emits such a reply inline.
pub(super) enum PendingScanKind {
    WireSafe { next_row: usize, wire_row_stride: usize },
    NonWireSafe,
}

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, target_id: u64, request_id: u64) {
        let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf,
                0,
                target_id,
                0,
                0,
                0u128,
                0,
                request_id,
                STATUS_OK,
                &[],
                None,
                None,
                None,
                None,
                &[],
            );
        });
    }

    /// Reply schema wire block: the table's cached block for `Table`, a
    /// one-off (never cached) block for `OneOff`. Returns the block, the
    /// table's schema version, and the schema's wire-safety
    /// (`(None, 0, true)` for `ReplySchema::None`) — `Table` reads the
    /// cached wire-safe bit instead of recomputing it per reply.
    fn reply_schema_block(&mut self, tid_key: i64, schema: ReplySchema<'_>) -> (Option<Rc<Vec<u8>>>, u16, bool) {
        match schema {
            ReplySchema::None => (None, 0, true),
            ReplySchema::OneOff(s) => {
                let block = Rc::new(ipc::build_schema_wire_block(s, &[], 0, tid_key as u32));
                (Some(block), self.cat().get_schema_version(tid_key), schema_wire_safe(s))
            }
            ReplySchema::Table(s) => {
                let e = ipc::get_or_build_schema_wire_block(self.cat(), tid_key, s);
                (Some(e.entry.block), e.version, e.entry.wire_safe)
            }
        }
    }

    pub(super) fn send_response(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: ReplySchema<'_>,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
    ) {
        let (prebuilt_rc, server_version, _) = self.reply_schema_block(target_id as i64, schema);
        let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
        let sz = ipc::wire_size(STATUS_OK, &[], schema.descriptor(), None, result, prebuilt, &[]);
        self.send_response_prebuilt(
            target_id,
            result,
            schema.descriptor(),
            request_id,
            client_id,
            seek_pk,
            prebuilt,
            server_version,
            sz,
        );
    }

    /// Encode tail of `send_response`: emit one frame with an already-resolved
    /// schema block. `prebuilt`/`server_version` must come from
    /// `reply_schema_block` for this `schema`, and `sz` must be the frame's
    /// wire size for these exact arguments — split out so a caller that
    /// already resolved the block and size (the `stream_batch_response`
    /// single-frame fast path) does not compute either twice.
    #[allow(clippy::too_many_arguments)]
    fn send_response_prebuilt(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
        prebuilt: Option<&[u8]>,
        server_version: u16,
        sz: usize,
    ) {
        let flags = gnitz_wire::wire_flags_set_schema_version(0, server_version);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into(
                buf,
                0,
                target_id,
                client_id,
                flags,
                seek_pk,
                0,
                request_id,
                STATUS_OK,
                &[],
                schema,
                None,
                result,
                prebuilt,
                &[],
            );
        });
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
            self.send_response(target_id, None, schema, request_id, client_id, seek_pk);
            return Ok(());
        };
        let tid_key = target_id as i64;
        let (prebuilt_rc, server_version, is_wire_safe) = self.reply_schema_block(tid_key, schema);
        let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
        // For a full-range wire-safe batch the blob region is empty and every
        // region is `count · stride`, so wire_size_range(count) equals the
        // wire_size the single-frame path below would compute.
        let sz = if is_wire_safe {
            ipc::wire_size_range(STATUS_OK, &[], schema.descriptor(), None, &batch, batch.count, prebuilt)
        } else {
            ipc::wire_size(STATUS_OK, &[], schema.descriptor(), None, Some(&batch), prebuilt, &[])
        };
        // Non-wire-safe replies cannot chunk, so they single-frame up to the
        // hard ring limit; wire-safe replies chunk past the (overridable)
        // frame budget.
        let frame_cap = if is_wire_safe {
            self.reply_frame_budget
        } else {
            w2m_ring::MAX_W2M_MSG as usize
        };
        if sz <= frame_cap {
            self.send_response_prebuilt(
                target_id,
                Some(&batch),
                schema.descriptor(),
                request_id,
                client_id,
                seek_pk,
                prebuilt,
                server_version,
                sz,
            );
            return Ok(());
        }
        if !is_wire_safe {
            return Err(format!(
                "result wire_size={sz} > MAX_W2M_MSG={}; STRING-column chunking not \
                 yet implemented — add a tighter predicate or LIMIT",
                w2m_ring::MAX_W2M_MSG
            ));
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
    /// if the batch exceeds `MAX_W2M_MSG`.
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
        let tid_key = target_id as i64;
        // Obtain prebuilt schema block + server version. include_schema controls
        // whether the first frame carries a schema block; server_version is always
        // embedded in wire_flags so the client can cache/verify.
        let (block_rc, server_version, is_wire_safe) = self.reply_schema_block(tid_key, schema);
        let prebuilt_rc = block_rc.filter(|_| gnitz_wire::wire_should_include_schema(client_version, server_version));
        let schema_version_flags = gnitz_wire::wire_flags_set_schema_version(0, server_version);

        // When schema omission is in effect (prebuilt_rc=None), pass schema=None to
        // the encode functions so has_schema stays false. Passing schema=Some with
        // prebuilt=None would cause encode_wire_into_range to emit a schema block
        // with empty column names, corrupting the client's schema cache.
        let schema_for_encode = if prebuilt_rc.is_some() {
            schema.descriptor()
        } else {
            None
        };

        if !is_wire_safe {
            // STRING-column tables: no chunking. Check size; error if too big.
            // Sized with descriptor=None to match `emit_non_wire_safe` exactly (a
            // prebuilt block, present whenever a schema is emitted, supersedes the
            // descriptor, so this equals sizing with `schema_for_encode`).
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            let wire_sz = ipc::wire_size(STATUS_OK, &[], None, None, Some(&*batch), prebuilt, &[]);
            if wire_sz > w2m_ring::MAX_W2M_MSG as usize {
                return Err(format!(
                    "scan: batch wire_size={wire_sz} > MAX_W2M_MSG={}; \
                     STRING-column chunking not yet implemented",
                    w2m_ring::MAX_W2M_MSG
                ));
            }
            if force_fifo {
                // Multi-scan: queue the single blob frame so this relation
                // reaches the ring in request order (the FIFO reply contract).
                // The oversize reject above stays at enqueue time —
                // `emit_pending_scan_chunk` is infallible, so only the encode is
                // deferred; the emit recomputes `wire_sz` identically.
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
            self.emit_non_wire_safe(target_id, &batch, request_id, client_id, prebuilt, server_version);
            return Ok(());
        }

        // Wire-safe path: range encoder supports chunking.
        let total_rows = batch.count;
        let total_sz = {
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            ipc::wire_size_range(STATUS_OK, &[], schema_for_encode, None, &batch, total_rows, prebuilt)
        };

        if !force_fifo && total_sz <= self.reply_frame_budget {
            // Single-frame response: FLAG_CONTINUATION keeps the client reading
            // (terminal frame signals scan end); FLAG_SCAN_LAST tells master this
            // worker's chunk train is done. Skipped under `force_fifo`: a
            // multi-scan queues even a one-frame reply so ring order equals
            // request order.
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            let flags = schema_version_flags | FLAG_CONTINUATION | FLAG_SCAN_LAST;
            self.w2m_writer.send_encoded(total_sz, request_id as u32, |buf| {
                ipc::encode_wire_into_range(
                    buf,
                    0,
                    target_id,
                    client_id,
                    flags,
                    STATUS_OK,
                    schema_for_encode,
                    &batch,
                    0,
                    total_rows,
                    prebuilt,
                );
            });
        } else {
            // Enqueue the train; its first chunk is emitted on the next
            // drain_sal pass, after any earlier queued train fully drains. This
            // covers both a genuinely multi-chunk reply and the `force_fifo`
            // single-frame case — a one-chunk train whose lone chunk is the same
            // ≤budget frame the immediate path would send.
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
        let wire_row_stride = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 1, None)
            - ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 0, None);
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

    /// Emit one non-wire-safe (STRING/blob) scan reply as a single terminal
    /// frame (`FLAG_CONTINUATION | FLAG_SCAN_LAST`, schema block iff `prebuilt`
    /// is `Some`) via the blob-capable `encode_wire_into`. The descriptor is
    /// always `None`: a prebuilt block, present whenever a schema is emitted,
    /// supersedes it. Shared by the immediate branch of `send_scan_response` and
    /// the queued `emit_non_wire_safe_frame` (multi-scan FIFO) so the two produce
    /// byte-identical frames. Callers enforce the `MAX_W2M_MSG` limit first.
    fn emit_non_wire_safe(
        &mut self,
        target_id: u64,
        batch: &Batch,
        request_id: u64,
        client_id: u64,
        prebuilt: Option<&[u8]>,
        server_version: u16,
    ) {
        let flags = gnitz_wire::wire_flags_set_schema_version(FLAG_CONTINUATION | FLAG_SCAN_LAST, server_version);
        let wire_sz = ipc::wire_size(STATUS_OK, &[], None, None, Some(batch), prebuilt, &[]);
        self.w2m_writer.send_encoded(wire_sz, request_id as u32, |buf| {
            ipc::encode_wire_into(
                buf,
                0,
                target_id,
                client_id,
                flags,
                0u128,
                0,
                0,
                STATUS_OK,
                &[],
                None,
                None,
                Some(batch),
                prebuilt,
                &[],
            );
        });
    }

    /// Emit the single blob frame of a `NonWireSafe` front train and pop it. The
    /// oversize reject already fired at enqueue, so `emit_non_wire_safe`'s
    /// internal sizing call here is pure.
    fn emit_non_wire_safe_frame(&mut self) {
        // This train always emits exactly one frame and always pops, so pop it
        // up front and move its fields straight into the emit — no clone needed.
        let Some(p) = self.pending_streams.pop_front() else {
            return;
        };
        self.emit_non_wire_safe(
            p.target_id,
            &p.batch,
            p.request_id,
            p.client_id,
            p.prebuilt_schema.as_deref().map(Vec::as_slice),
            p.server_version,
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

        let is_first = next_row == 0;
        // prebuilt_opt drives the schema block: Some on the first chunk when the
        // client needs the schema, None on continuations and schema-suppressed frames.
        // encode_wire_into_range uses the prebuilt bytes directly; no schema arg needed.
        let prebuilt_opt: Option<&[u8]> = if is_first {
            prebuilt_schema.as_deref().map(Vec::as_slice)
        } else {
            None
        };

        let remaining = batch.count - next_row;
        // Rows per chunk via linear interpolation: wire-safe schemas have a
        // constant per-row stride (stored at enqueue), so wire size is linear
        // in count and only the frame base (schema block on the first chunk)
        // needs recomputing per chunk.
        let sz_0 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 0, prebuilt_opt);
        let usable = budget.saturating_sub(sz_0);
        let max_rows = match usable.checked_div(per_row) {
            Some(rows) => rows.max(1).min(remaining),
            None => remaining.max(1), // per_row == 0: constant wire size, send all
        };
        let has_more = next_row + max_rows < batch.count;
        // FLAG_CONTINUATION is always set on worker scan frames so the client's
        // loop termination ("stop on no FLAG_CONTINUATION") still works.
        // FLAG_SCAN_LAST is the W2M-internal signal that this is the last chunk.
        // server_version is always embedded so the master decode path can verify.
        let flags: u64 = gnitz_wire::wire_flags_set_schema_version(
            FLAG_CONTINUATION | if !has_more { FLAG_SCAN_LAST } else { 0 },
            server_version,
        );
        // wire_size_range is linear in count for wire-safe schemas.
        let sz = sz_0 + per_row * max_rows;
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_range(
                buf,
                0,
                target_id,
                client_id,
                flags,
                STATUS_OK,
                None,
                &batch,
                next_row,
                max_rows,
                prebuilt_opt,
            );
        });

        if has_more {
            match self.pending_streams.front_mut().map(|p| &mut p.kind) {
                Some(PendingScanKind::WireSafe { next_row: nr, .. }) => *nr = next_row + max_rows,
                _ => unreachable!("emit_wire_safe_chunk: front train changed shape mid-emit"),
            }
        } else {
            self.pending_streams.pop_front();
        }
    }

    pub(super) fn send_error(&self, error_msg: &str, request_id: u64) {
        let msg = error_msg.as_bytes();
        let sz = ipc::wire_size(STATUS_ERROR, msg, None, None, None, None, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf,
                0,
                0,
                0,
                0,
                0u128,
                0,
                request_id,
                STATUS_ERROR,
                msg,
                None,
                None,
                None,
                None,
                &[],
            );
        });
    }
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
    let mut chunk = Batch::with_schema(*frame_schema, keys.remaining().min(keys_per_frame));
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
        // the master's saved schema hint (synthetic schema version is 0, so
        // no wire_flags_set_schema_version is needed).
        let prebuilt: Option<&[u8]> = if is_first { Some(&schema_block) } else { None };
        let schema_for_encode = if is_first { Some(frame_schema) } else { None };
        let flags = FLAG_CONTINUATION | if is_last { FLAG_SCAN_LAST } else { 0 };
        let sz = ipc::wire_size_range(STATUS_OK, &[], schema_for_encode, None, &chunk, chunk.count, prebuilt);
        w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_range(
                buf,
                0,
                target_id,
                0,
                flags,
                STATUS_OK,
                schema_for_encode,
                &chunk,
                0,
                chunk.count,
                prebuilt,
            );
        });
        is_first = false;
        if is_last {
            break;
        }
    }
}
