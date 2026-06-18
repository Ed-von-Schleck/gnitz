//! Worker W2M response framing: the `WorkerProcess` reply helpers
//! (ack / schema-block / response / scan-response / streamed-batch / error).

use super::*;

impl WorkerProcess {
    // ── W2M response helpers ───────────────────────────────────────────

    pub(super) fn send_ack(&self, target_id: u64, flags: u64, request_id: u64) {
        let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, target_id, 0, flags,
                0u128, 0, request_id, STATUS_OK, &[], None, None, None, None, &[],
            );
        });
    }

    /// Reply schema wire block: the table's cached block for `Table`, a
    /// one-off (never cached) block for `OneOff`. Returns the block and the
    /// table's schema version (`(None, 0)` for `ReplySchema::None`).
    fn reply_schema_block(&mut self, tid_key: i64, schema: ReplySchema<'_>)
        -> (Option<Rc<Vec<u8>>>, u16)
    {
        let block = match schema {
            ReplySchema::None => return (None, 0),
            ReplySchema::OneOff(s) => {
                Rc::new(ipc::build_schema_wire_block(s, &[], tid_key as u32))
            }
            ReplySchema::Table(s) => {
                if let Some(cached) = self.cat().get_cached_schema_wire_block(tid_key) {
                    return (Some(cached.block), cached.version);
                }
                let col_names = self.cat().get_col_names_bytes(tid_key);
                let (name_refs, n) = ipc::col_names_as_refs(&col_names);
                let block = Rc::new(ipc::build_schema_wire_block(s, &name_refs[..n], tid_key as u32));
                let (wire_safe, wire_row_stride) = crate::runtime::sal::compute_wire_props(s);
                self.cat().set_schema_wire_block(tid_key, block.clone(), wire_safe, wire_row_stride);
                block
            }
        };
        (Some(block), self.cat().get_schema_version(tid_key))
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
        let (prebuilt_rc, server_version) = self.reply_schema_block(target_id as i64, schema);
        let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
        let sz = ipc::wire_size(STATUS_OK, &[], schema.descriptor(), None, result, prebuilt, &[]);
        self.send_response_prebuilt(
            target_id, result, schema.descriptor(), request_id, client_id, seek_pk,
            prebuilt, server_version, sz,
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
                buf, 0, target_id, client_id, flags,
                seek_pk, 0, request_id, STATUS_OK, &[],
                schema, None, result, prebuilt, &[],
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
    ) -> Option<String> {
        let Some(batch) = result.filter(|b| b.count > 0) else {
            self.send_response(target_id, None, schema, request_id, client_id, seek_pk);
            return None;
        };
        let tid_key = target_id as i64;
        let is_wire_safe = schema.descriptor().map(schema_wire_safe).unwrap_or(true);
        let (prebuilt_rc, server_version) = self.reply_schema_block(tid_key, schema);
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
                target_id, Some(&batch), schema.descriptor(), request_id, client_id,
                seek_pk, prebuilt, server_version, sz);
            return None;
        }
        if !is_wire_safe {
            return Some(format!(
                "result wire_size={sz} > MAX_W2M_MSG={}; STRING-column chunking not \
                 yet implemented — add a tighter predicate or LIMIT",
                w2m_ring::MAX_W2M_MSG));
        }
        self.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch), next_row: 0, request_id, client_id, target_id,
            prebuilt_schema: prebuilt_rc, server_version,
        });
        None
    }

    /// Send a SCAN response for `batch`. For wire-safe schemas, large batches
    /// are split across multiple frames via `pending_streams`; the first chunk
    /// is emitted at the top of the next `drain_sal` pass. For non-wire-safe
    /// (STRING-column) schemas, a single frame is sent; returns an error message
    /// if the batch exceeds `MAX_W2M_MSG`.
    pub(super) fn send_scan_response(
        &mut self,
        target_id: u64,
        batch: Rc<Batch>,
        schema: ReplySchema<'_>,
        request_id: u64,
        client_id: u64,
        client_version: u16,
    ) -> Option<String> {
        let tid_key = target_id as i64;
        // Obtain prebuilt schema block + server version. include_schema controls
        // whether the first frame carries a schema block; server_version is always
        // embedded in wire_flags so the client can cache/verify.
        let (block_rc, server_version) = self.reply_schema_block(tid_key, schema);
        let prebuilt_rc = block_rc.filter(|_| {
            gnitz_wire::wire_should_include_schema(client_version, server_version)
        });
        let schema_version_flags = gnitz_wire::wire_flags_set_schema_version(0, server_version);

        // When schema omission is in effect (prebuilt_rc=None), pass schema=None to
        // the encode functions so has_schema stays false. Passing schema=Some with
        // prebuilt=None would cause encode_wire_into_range to emit a schema block
        // with empty column names, corrupting the client's schema cache.
        let schema_for_encode = if prebuilt_rc.is_some() { schema.descriptor() } else { None };

        let is_wire_safe = schema.descriptor().map(schema_wire_safe).unwrap_or(true);

        if !is_wire_safe {
            // STRING-column tables: no chunking. Check size; error if too big.
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            let wire_sz = ipc::wire_size(STATUS_OK, &[], schema_for_encode, None, Some(&*batch), prebuilt, &[]);
            if wire_sz > w2m_ring::MAX_W2M_MSG as usize {
                return Some(format!(
                    "scan: batch wire_size={wire_sz} > MAX_W2M_MSG={}; \
                     STRING-column chunking not yet implemented",
                    w2m_ring::MAX_W2M_MSG
                ));
            }
            let flags = schema_version_flags | FLAG_CONTINUATION | FLAG_SCAN_LAST;
            self.w2m_writer.send_encoded(wire_sz, request_id as u32, |buf| {
                ipc::encode_wire_into(
                    buf, 0, target_id, client_id, flags,
                    0u128, 0, 0, STATUS_OK, &[],
                    schema_for_encode, None, Some(&*batch), prebuilt, &[],
                );
            });
            return None;
        }

        // Wire-safe path: range encoder supports chunking.
        let total_rows = batch.count;
        let total_sz = {
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            ipc::wire_size_range(STATUS_OK, &[], schema_for_encode, None, &batch, total_rows, prebuilt)
        };

        if total_sz <= self.reply_frame_budget {
            // Single-frame response: FLAG_CONTINUATION keeps the client reading
            // (terminal frame signals scan end); FLAG_SCAN_LAST tells master this
            // worker's chunk train is done.
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            let flags = schema_version_flags | FLAG_CONTINUATION | FLAG_SCAN_LAST;
            self.w2m_writer.send_encoded(total_sz, request_id as u32, |buf| {
                ipc::encode_wire_into_range(
                    buf, 0, target_id, client_id, flags,
                    0, STATUS_OK,
                    schema_for_encode, &batch, 0, total_rows, prebuilt,
                );
            });
        } else {
            // Multi-chunk: enqueue the train; its first chunk is emitted on the
            // next drain_sal pass, after any earlier queued train fully drains.
            self.pending_streams.push_back(PendingScan {
                batch,
                next_row: 0,
                request_id,
                client_id,
                target_id,
                prebuilt_schema: prebuilt_rc,
                server_version,
            });
        }

        None
    }

    pub(super) fn send_error(&self, error_msg: &str, request_id: u64) {
        let msg = error_msg.as_bytes();
        let sz = ipc::wire_size(STATUS_ERROR, msg, None, None, None, None, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 0, 0, 0,
                0u128, 0, request_id, STATUS_ERROR, msg, None, None, None, None, &[],
            );
        });
    }

}
