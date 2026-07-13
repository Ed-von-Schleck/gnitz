use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::ClientError;
use crate::protocol::message::{encode_message_noschema_parts, encode_message_parts, MessageParts};
use crate::protocol::{
    encode_ddl_txn, encode_push_txn, hello_handshake, recv_message, send_message, send_message_with_extra,
    wire_flags_get_index_version, wire_flags_get_schema_version, wire_flags_set_conflict_mode,
    wire_flags_set_index_version, wire_flags_set_schema_version, ClientTransport, Message, PkTuple, ProtocolError,
    Schema, WireConflictMode, ZSetBatch, FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE,
    FLAG_ALLOCATE_TABLE_ID, FLAG_CONTINUATION, FLAG_GET_INDICES, FLAG_PUSH, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_SEEK_BY_INDEX_RANGE, STATUS_ERROR, STATUS_NO_INDEX, STATUS_SCHEMA_MISMATCH,
};
use lru::LruCache;

pub use gnitz_wire::{
    COL_TAB, DEP_TAB, FIRST_USER_SCHEMA_ID, FIRST_USER_TABLE_ID, IDX_TAB, SCHEMA_TAB, SEQ_TAB, TABLE_TAB, VIEW_TAB,
};

/// Per-connection schema LRU capacity. Sized to comfortably hold a session's
/// working set of tables/views without unbounded growth.
const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();

/// `(schema, data_batch, lsn)` returned by a `scan`/`seek`/`seek_by_index`:
/// the (cached) `Schema`, the materialised `ZSetBatch` if any rows came back,
/// and the server LSN at which the read was served.
pub type ScanResult = Result<(Option<Arc<Schema>>, Option<ZSetBatch>, u64), ClientError>;

/// Generate a session-unique client ID.
///
/// Combines PID (top 32 bits) with a per-process monotonic sequence (bottom 32 bits).
/// This guarantees uniqueness across all connections from the same process, and makes
/// cross-process collisions practically impossible even with PID reuse.
fn new_client_id() -> u64 {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed) as u64;
    (std::process::id() as u64) << 32 | seq
}

fn check_response(msg: Message) -> Result<Message, ClientError> {
    if msg.status == STATUS_SCHEMA_MISMATCH {
        return Err(ClientError::SchemaMismatch);
    }
    if msg.status == STATUS_NO_INDEX {
        return Err(ClientError::NoIndex);
    }
    if msg.status == STATUS_ERROR {
        // Fall back to the default text on an empty string, not only on None:
        // a STATUS_ERROR with Some("") would otherwise surface as a blank
        // ServerError. This matters because the warm-push guard converts
        // silent corruption into a surfaced error, which must be legible.
        let text = msg
            .error_text
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown server error".into());
        return Err(ClientError::ServerError(text));
    }
    Ok(msg)
}

/// A protocol session: the transport plus all per-connection protocol state
/// (client id, negotiated frame ceiling, the schema LRU, and the warm/cold
/// packing, continuation reassembly, cache absorption, and status→error
/// policy that read/write it). Exactly one owner of that state — the sync
/// [`crate::GnitzClient`] holds one; the gnitz-py async I/O thread holds its
/// own. Because the session owns the cache, no `LruCache` is threaded as a
/// parameter and no cache lock is shared across threads.
pub struct Session {
    transport: ClientTransport,
    pub client_id: u64,
    /// Server-negotiated per-connection frame payload ceiling. Set during
    /// `connect()` from the HELLO ACK; subsequent `recv_message` calls
    /// pass this value through so a compromised server cannot force the
    /// client to allocate up to the historical 256 MB ceiling.
    max_payload_len: usize,
    schema_cache: LruCache<u64, (Arc<Schema>, u16)>,
}

impl Session {
    /// `target` is an AF_UNIX socket path or a `tls://HOST:PORT[?PARAM]`
    /// address (see `ClientTransport::connect`).
    pub fn connect(target: &str) -> Result<Self, ClientError> {
        let mut transport = ClientTransport::connect(target)?;
        // Run the HELLO handshake before any data flows. The server
        // accepts the first frame at an 8-byte limit, so this must
        // happen before `send_message` would emit a control block.
        let limit = hello_handshake(&mut transport)?;
        Ok(Session::from_transport(transport, new_client_id(), limit as usize))
    }

    /// Wrap an already-connected, already-handshaken transport. Used by the
    /// gnitz-py async I/O thread, which connects + handshakes on the calling
    /// thread (to capture the negotiated limit and dup the waker fd) and then
    /// moves the raw transport onto its I/O thread to build the session there.
    pub fn from_transport(transport: ClientTransport, client_id: u64, max_payload_len: usize) -> Self {
        Session {
            transport,
            client_id,
            max_payload_len,
            schema_cache: LruCache::new(SCHEMA_CACHE_CAP),
        }
    }

    pub fn close(self) {
        // The transport is dropped here, which closes the connection.
    }

    /// Handle that unblocks a blocking recv parked in another thread (the
    /// async I/O loop's teardown wake). Delegates to the transport.
    pub fn waker(&self) -> Result<crate::protocol::TransportWaker, ProtocolError> {
        self.transport.waker()
    }

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_TABLE_ID, 0, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_SCHEMA_ID, 0, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_INDEX_ID, 0, None, None)?;
        Ok(msg.target_id)
    }

    /// Reserve a contiguous range of `count` SERIAL ids for the sequence keyed
    /// by `seq_table_id`. Returns the range base; the caller owns
    /// `[base, base + count)`. The range `count` rides in `seek_col_idx`, and
    /// `target_id = seq_table_id ≠ 0` steers the master to the durable
    /// range-advance branch.
    pub fn alloc_serial_range(&mut self, seq_table_id: u64, count: u64) -> Result<u64, ClientError> {
        let msg = self.roundtrip(seq_table_id, FLAG_ALLOCATE_SERIAL_RANGE, count, None, None)?;
        Ok(msg.target_id) // base of [base, base + count)
    }

    /// Default push: silent-upsert (`WireConflictMode::Update`).
    /// Callers that need SQL-standard rejection use `push_with_mode`.
    pub fn push(&mut self, target_id: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, ClientError> {
        self.push_with_mode(target_id, schema, batch, WireConflictMode::Update)
    }

    pub fn push_with_mode(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        batch.validate(schema).map_err(ClientError::ServerError)?;
        let msg = self.roundtrip_push(target_id, schema, batch, mode)?;
        Ok(msg.seek_pk as u64)
    }

    /// Send an atomic DDL transaction: a bundle of system-table family batches
    /// (`FLAG_DDL_TXN`) that the server ingests under one durable SAL zone. Used
    /// by every catalog write — a `CREATE`'s N families or a
    /// `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single family. Returns the zone
    /// LSN (echoed in the ACK's `seek_pk`, as `push` does).
    ///
    /// No schema cache interaction: system-table schemas are compile-time-fixed
    /// and known to both sides, so the frame carries a schema block per family
    /// (via `encode_wal_block`) and the server resolves each family's schema from
    /// its own catalog.
    pub fn push_ddl_txn(&mut self, families: &[(u64, &Schema, ZSetBatch)]) -> Result<u64, ClientError> {
        for (_, schema, batch) in families {
            batch.validate(schema).map_err(ClientError::ServerError)?;
        }
        let payload = encode_ddl_txn(self.client_id, families);
        self.send_txn_frame(&payload)
    }

    /// Send a pre-encoded transaction frame and receive its uncorrelated
    /// zone-LSN ACK (`seek_pk`). Shared by `push_ddl_txn` and `push_txn`.
    fn send_txn_frame(&mut self, payload: &[u8]) -> Result<u64, ClientError> {
        self.transport.send_framed(payload)?;
        let msg = check_response(recv_message(&mut self.transport, None, self.max_payload_len)?)?;
        Ok(msg.seek_pk as u64)
    }

    /// Send an atomic **user-table** push transaction (`FLAG_PUSH_TXN`): a bundle
    /// of user-table families — each carrying its conflict mode and its schema
    /// block — that the server validates as a unit under the union of the
    /// involved table locks and commits under one durable SAL zone. Returns the
    /// zone LSN (echoed in the ACK's `seek_pk`, as `push` does).
    ///
    /// The reply is received uncorrelated, exactly as `push_ddl_txn` does. Each
    /// family's batch is validated client-side before encoding, and the encoded
    /// frame is bounds-checked against the server ingress cap so an oversized
    /// bundle fails locally rather than being truncated on the wire.
    pub fn push_txn(&mut self, families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)]) -> Result<u64, ClientError> {
        for (_, schema, batch, _) in families {
            batch.validate(schema).map_err(ClientError::ServerError)?;
        }
        let payload = encode_push_txn(self.client_id, families);
        if payload.len() > gnitz_wire::MAX_FRAME_PAYLOAD_SERVER {
            return Err(ClientError::ServerError(format!(
                "transaction frame is {} bytes, exceeding the {}-byte server ingress cap; split the transaction",
                payload.len(),
                gnitz_wire::MAX_FRAME_PAYLOAD_SERVER
            )));
        }
        self.send_txn_frame(&payload)
    }

    pub fn scan(&mut self, target_id: u64) -> ScanResult {
        let parts = self.pack_scan(target_id);
        self.transport.send_framed_iov(&parts.segments())?;
        self.recv_scan(target_id)
    }

    pub fn seek(&mut self, target_id: u64, pk: &PkTuple) -> ScanResult {
        let flags = self.versioned_flags(target_id, FLAG_SEEK);
        send_message(&mut self.transport, target_id, self.client_id, flags, pk, 0, None, None)?;
        let msg = self.recv_checked(target_id)?;
        self.recover_schema(target_id, msg)
    }

    pub fn seek_by_index(&mut self, table_id: u64, col_indices: &[u32], key_vals: &[u128]) -> ScanResult {
        // Embed the cached schema version so the server can omit the schema
        // block on a warm-cache hit (matching push/scan).
        let flags = self.versioned_flags(table_id, FLAG_SEEK_BY_INDEX);
        // Pack the K = key_vals.len() native values as 16-byte LE slots into a
        // PkTuple (stride K×16 ≤ 64 ≤ MAX_PK_BYTES); send_message's split_wire
        // routes slot 0 → seek_pk and slots 1..K → seek_pk_extra. K=1 is
        // byte-identical to the legacy single-value frame. seek_col_idx carries
        // pack_pk_cols(col_indices). Arity is validated upstream in
        // GnitzClient::seek_by_index (the one choke point for every binding).
        let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
        for (i, &v) in key_vals.iter().enumerate() {
            buf[i * 16..i * 16 + 16].copy_from_slice(&v.to_le_bytes());
        }
        let pk = PkTuple::from_bytes(&buf[..key_vals.len() * 16]);
        let seek_col_idx = gnitz_wire::pack_pk_cols(col_indices);
        send_message(
            &mut self.transport,
            table_id,
            self.client_id,
            flags,
            &pk,
            seek_col_idx,
            None,
            None,
        )?;
        let msg = self.recv_checked(table_id)?;
        self.recover_schema(table_id, msg)
    }

    /// Ordered range scan over a secondary index, described by `desc` (the
    /// equality-pinned leading values plus the half-open cut interval on the
    /// next index column). Arity is validated upstream in
    /// `GnitzClient::seek_by_index_range`, the single choke point.
    ///
    /// The encoded `RangeDescriptor` rides the **explicit** `seek_pk_extra`
    /// blob (up to 82 bytes at max index arity — over the 64-byte `PkTuple`
    /// cap), so it goes through `send_message_with_extra`, not `send_message`.
    pub fn seek_by_index_range(
        &mut self,
        table_id: u64,
        col_indices: &[u32],
        desc: &gnitz_wire::RangeDescriptor,
    ) -> ScanResult {
        let flags = self.versioned_flags(table_id, FLAG_SEEK_BY_INDEX_RANGE);
        let seek_col_idx = gnitz_wire::pack_pk_cols(col_indices);
        send_message_with_extra(
            &mut self.transport,
            table_id,
            self.client_id,
            flags,
            seek_col_idx,
            &desc.encode(),
        )?;
        let msg = self.recv_checked(table_id)?;
        self.recover_schema(table_id, msg)
    }

    /// Pure transport for GET_INDICES: send the cached index epoch and receive
    /// the server's reply on a dedicated path — `recv_message(fd, None, ..)`,
    /// never the cache-aware recv — so the per-table `schema_cache` is
    /// untouched. The reply is schema-bearing only when the list changed; the
    /// "unchanged" reply carries no data and needs no schema. Returns the raw
    /// `(data_batch, server_epoch)` and leaves the `IndexMeta` decode to
    /// `GnitzClient`, where the `col_u64` helper lives.
    pub(crate) fn fetch_indices(
        &mut self,
        table_id: u64,
        cached_epoch: u8,
    ) -> Result<(Option<ZSetBatch>, u8), ClientError> {
        let flags = wire_flags_set_index_version(FLAG_GET_INDICES, cached_epoch);
        send_message(
            &mut self.transport,
            table_id,
            self.client_id,
            flags,
            &PkTuple::EMPTY,
            0,
            None,
            None,
        )?;
        let msg = check_response(recv_message(&mut self.transport, None, self.max_payload_len)?)?;
        Ok((msg.data_batch, wire_flags_get_index_version(msg.flags)))
    }

    // ── Async-shared protocol surface ──────────────────────────────────────
    //
    // Build/receive helpers the gnitz-py async I/O loop drives directly: it
    // packs a batch of requests, ships them with one `send_batch`, then reads
    // the responses back through these same continuation-reassembly and
    // cache-absorption paths the sync methods use.

    /// Pack a scan request (control-only) with the cached schema version, so
    /// the server may omit the schema block on a warm hit. The matching
    /// [`Self::recv_scan`] resolves the schema from the cache the session
    /// owns, so no hint is threaded back.
    pub fn pack_scan(&self, target_id: u64) -> MessageParts {
        let flags = self.versioned_flags(target_id, 0);
        encode_message_parts(target_id, self.client_id, flags, &PkTuple::EMPTY, 0, None, None)
    }

    /// Pack a point-seek request with the cached schema version.
    pub fn pack_seek(&self, target_id: u64, pk: &PkTuple) -> MessageParts {
        let flags = self.versioned_flags(target_id, FLAG_SEEK);
        encode_message_parts(target_id, self.client_id, flags, pk, 0, None, None)
    }

    /// Ship many pre-encoded frames as one vectored write sequence.
    pub fn send_batch(&mut self, parts: &[MessageParts]) -> Result<(), ProtocolError> {
        self.transport.send_framed_batch(parts)
    }

    /// Receive a streaming scan/seek response: reassemble continuation frames,
    /// absorb any schema block into the cache, and recover the schema from the
    /// cache if the response was schema-less. Same body as the sync `scan`.
    pub fn recv_scan(&mut self, target_id: u64) -> ScanResult {
        let mut schema: Option<Arc<Schema>> = None;
        let mut data: Option<ZSetBatch> = None;
        let lsn: u64 = loop {
            let msg = check_response(self.recv_cached(target_id)?)?;
            let is_continuation = (msg.flags & FLAG_CONTINUATION) != 0;
            schema = schema.or(msg.schema);
            if let Some(batch) = msg.data_batch {
                match data.as_mut() {
                    Some(acc) => acc.extend_from_owned(batch),
                    None => data = Some(batch),
                }
            }
            if !is_continuation {
                break msg.seek_pk as u64;
            }
        };
        // Warm-cache responses omit the schema block. Recover once from the LRU.
        if schema.is_none() {
            schema = self.schema_cache.get(&target_id).map(|(s, _)| Arc::clone(s));
        }
        Ok((schema, data, lsn))
    }

    /// Receive a single push ACK, absorbing any schema block it carries into
    /// the cache (matching the sync push path). The caller inspects the status.
    pub fn recv_push_ack(&mut self, target_id: u64) -> Result<Message, ClientError> {
        self.recv_cached(target_id)
    }

    /// The cached schema version for `target_id` OR'd into the flag word, so a
    /// warm-cache request lets the server omit the schema block.
    fn versioned_flags(&self, target_id: u64, base: u64) -> u64 {
        let cached_version = self.schema_cache.peek(&target_id).map(|(_, v)| *v).unwrap_or(0);
        wire_flags_set_schema_version(base, cached_version)
    }

    /// Recover the schema for a single-frame seek response and assemble the
    /// `ScanResult`: prefer the in-frame schema, else fall back to the cache.
    /// Shared tail of `seek` / `seek_by_index` / `seek_by_index_range`.
    fn recover_schema(&mut self, target_id: u64, msg: Message) -> ScanResult {
        let schema = msg
            .schema
            .or_else(|| self.schema_cache.get(&target_id).map(|(s, _)| Arc::clone(s)));
        Ok((schema, msg.data_batch, msg.seek_pk as u64))
    }

    /// Receive one framed message, using the LRU cache to decode continuation
    /// frames that arrive without a schema block, and caching any schema block
    /// the frame does carry.
    fn recv_cached(&mut self, target_id: u64) -> Result<Message, ClientError> {
        let msg = {
            // `get` (not `peek`) so a frequently-accessed schema refreshes its
            // LRU recency and isn't evicted under memory pressure.
            let hint = self.schema_cache.get(&target_id).map(|(s, v)| (s.as_ref(), *v));
            recv_message(&mut self.transport, hint, self.max_payload_len)?
        };
        // `msg.schema` is `Some` exactly when the schema block was physically
        // in the frame. Absorb it as an `Arc` clone (refcount bump, no deep
        // copy) — this is the authoritative schema with the server's real
        // column names.
        if let Some(s) = msg.schema.as_ref() {
            let version = wire_flags_get_schema_version(msg.flags);
            self.schema_cache.put(target_id, (Arc::clone(s), version));
        }
        Ok(msg)
    }

    /// Shared tail of the single-frame seek roundtrips: cache-aware recv then
    /// status→error mapping.
    fn recv_checked(&mut self, target_id: u64) -> Result<Message, ClientError> {
        let msg = self.recv_cached(target_id)?;
        check_response(msg)
    }

    fn roundtrip(
        &mut self,
        target_id: u64,
        flags: u64,
        seek_col_idx: u64,
        schema: Option<&Schema>,
        data: Option<&ZSetBatch>,
    ) -> Result<Message, ClientError> {
        send_message(
            &mut self.transport,
            target_id,
            self.client_id,
            flags,
            &PkTuple::EMPTY,
            seek_col_idx,
            schema,
            data,
        )?;
        // Alloc roundtrips carry no schema blocks; recv_message without a hint is sufficient.
        let msg = recv_message(&mut self.transport, None, self.max_payload_len)?;
        check_response(msg)
    }

    /// Push path: packs `WireConflictMode` into bits 16-23 of `wire_flags`.
    /// When the schema cache holds a valid version for `target_id` *and* the
    /// caller's `schema` type-matches the cached one, omits the schema block
    /// from the wire frame (warm path). On STATUS_SCHEMA_MISMATCH the cache
    /// entry is evicted and the push is retried with the full schema.
    ///
    /// The schema *version* alone is not a sufficient identity check for the
    /// data encoding: it proves the catalog has not changed, not that the
    /// caller encoded under the same column types. A version match with a
    /// type mismatch on the warm (schema-less) path ships bytes the server
    /// silently reinterprets under the catalog schema (e.g. a U64-encoded PK
    /// decoded as I64), corrupting at rest. So the warm path is gated on
    /// `types_match`; on a mismatch we fall through to the cold path so the
    /// full schema block is sent and the server's `validate_schema_match`
    /// returns the same deterministic error the cold path already gives.
    fn roundtrip_push(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<Message, ClientError> {
        // FLAG_PUSH marks the frame as a push independent of data presence, so
        // an empty batch (a legitimate empty Z-set delta) is ACKed as a no-op
        // push instead of being mistaken for a scan request.
        let base_flags = wire_flags_set_conflict_mode(FLAG_PUSH, mode);
        let warm_version: Option<u16> = match self.schema_cache.peek(&target_id) {
            Some((cached_schema, v)) if *v != 0 && schema.types_match(cached_schema.as_ref()) => Some(*v),
            _ => None,
        };
        let parts = match warm_version {
            // Warm path: omit schema block, embed cached version.
            Some(cached_version) => {
                let flags = wire_flags_set_schema_version(base_flags, cached_version);
                encode_message_noschema_parts(target_id, self.client_id, flags, schema, batch)
            }
            // Cold path: include schema block, version = 0.
            None => encode_message_parts(
                target_id,
                self.client_id,
                base_flags,
                &PkTuple::EMPTY,
                0,
                Some(schema),
                Some(batch),
            ),
        };
        self.transport.send_framed_iov(&parts.segments())?;
        let ack = match self.recv_checked(target_id) {
            Err(ClientError::SchemaMismatch) => {
                // Stale cache: evict and retry with full schema.
                self.schema_cache.pop(&target_id);
                let parts = encode_message_parts(
                    target_id,
                    self.client_id,
                    base_flags,
                    &PkTuple::EMPTY,
                    0,
                    Some(schema),
                    Some(batch),
                );
                self.transport.send_framed_iov(&parts.segments())?;
                self.recv_checked(target_id)?
            }
            Ok(msg) => msg,
            Err(e) => return Err(e),
        };
        // No manual cache write here. Whenever the server changed the schema
        // version it also included the schema block in the ACK
        // (wire_should_include_schema), and recv_cached already cached that
        // authoritative schema (with the server's real column names). A
        // schema.clone() here would clobber it with the caller's copy —
        // dropping the server's column names, and on schema evolution pairing
        // the OLD schema with the NEW version. On the pure warm path the
        // version is unchanged: nothing to do.
        Ok(ack)
    }
}

#[cfg(test)]
mod cache_tests {
    use crate::protocol::{ColumnDef, Schema, TypeCode};
    use lru::LruCache;
    use std::sync::Arc;

    #[test]
    fn schema_cache_arc_not_clone() {
        let mut cache: LruCache<u64, (Arc<Schema>, u16)> = LruCache::new(std::num::NonZeroUsize::new(4).unwrap());
        let schema = Arc::new(Schema {
            columns: vec![ColumnDef::new("id", TypeCode::I64, false)],
            pk_cols: vec![0],
        });
        cache.put(42, (Arc::clone(&schema), 1));

        // Warm hit must bump the refcount, not deep-copy.
        let warm = cache.get(&42).map(|(s, _)| Arc::clone(s)).unwrap();
        assert_eq!(Arc::strong_count(&schema), 3); // original + cache + warm
        drop(warm);
        assert_eq!(Arc::strong_count(&schema), 2); // original + cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_msg(error_text: Option<String>) -> Message {
        Message {
            status: STATUS_ERROR,
            target_id: 0,
            flags: 0,
            seek_pk: 0,
            seek_col_idx: 0,
            schema: None,
            data_batch: None,
            error_text,
        }
    }

    // `Message` does not implement Debug, so match the Result rather than
    // calling unwrap_err (which would require the Ok variant to be Debug).
    fn server_error_text(msg: Message) -> String {
        match check_response(msg) {
            Err(ClientError::ServerError(s)) => s,
            Err(other) => panic!("expected ServerError, got {other:?}"),
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn check_response_empty_error_text_falls_back_to_default() {
        // A STATUS_ERROR with Some("") must surface the default text, not a
        // blank ServerError — the warm-push guard's rejection must be legible.
        assert_eq!(
            server_error_text(error_msg(Some(String::new()))),
            "unknown server error"
        );
    }

    #[test]
    fn check_response_none_error_text_falls_back_to_default() {
        assert_eq!(server_error_text(error_msg(None)), "unknown server error");
    }

    #[test]
    fn check_response_nonempty_error_text_preserved() {
        assert_eq!(server_error_text(error_msg(Some("real error".into()))), "real error");
    }
}
