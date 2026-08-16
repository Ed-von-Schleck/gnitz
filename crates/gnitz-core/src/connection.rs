use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::ClientError;
use crate::protocol::message::{
    encode_message_noschema_parts, encode_message_parts, encode_schema_block, MessageParts,
};
use crate::protocol::{
    encode_ddl_txn, encode_push_txn, encode_scan_multi, hello_handshake, recv_message, send_message,
    send_message_with_extra, wire_flags_get_schema_version, wire_flags_set_conflict_mode,
    wire_flags_set_schema_version, ClientTransport, Message, PkTuple, ProtocolError, Schema, WireConflictMode,
    ZSetBatch, FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE, FLAG_ALLOCATE_TABLE_ID,
    FLAG_CONTINUATION, FLAG_PUSH, FLAG_RESOLVE, FLAG_SCAN_SPEC, FLAG_SEEK, FLAG_SEEK_BY_INDEX, STATUS_ERROR,
    STATUS_NO_INDEX, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};
use gnitz_wire::RelDescriptorBlob;
use lru::LruCache;

pub use gnitz_wire::{
    COL_TAB, FIRST_USER_SCHEMA_ID, FIRST_USER_TABLE_ID, IDX_TAB, SCHEMA_TAB, SEQ_TAB, TABLE_TAB, VIEW_TAB,
};

/// Per-connection schema LRU capacity. Sized to comfortably hold a session's
/// working set of tables/views without unbounded growth.
const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();

/// `(schema, data_batch, lsn)` returned by a `scan`/`seek`/`seek_by_index`:
/// the (cached) `Schema`, the materialised `ZSetBatch` if any rows came back,
/// and the server LSN at which the read was served.
pub type ScanResult = Result<(Option<Arc<Schema>>, Option<ZSetBatch>, u64), ClientError>;

/// The N per-relation results of a `scan_multi`, in request order. Each tuple is
/// shaped exactly like a single [`ScanResult`]'s inner triple `(schema,
/// data_batch, lsn)`. Every relation was snapshotted at the same server-side SAL
/// cut, so an atomic multi-table commit is never torn across the result set.
pub type MultiScanResult = Result<Vec<(Option<Arc<Schema>>, Option<ZSetBatch>, u64)>, ClientError>;

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
        return Err(ClientError::ServerError("no index on requested column".into()));
    }
    if msg.status == STATUS_TXN_CONFLICT {
        // Control-only frame: the fresh basis rides in `seek_pk`. Left as a
        // structured error so the SQL layer can retry (autocommit) or surface
        // it (BEGIN/COMMIT); the human-readable text is synthesized upstream.
        return Err(ClientError::TxnConflict {
            fresh_basis: msg.seek_pk as u64,
        });
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

/// Which relation a RESOLVE request describes. The wire carries an id field and
/// a name blob and lets the name win, but exactly one is ever meaningful — this
/// says which, so no caller has to encode that as a `0` / `""` sentinel pair.
#[derive(Copy, Clone, Debug)]
pub(crate) enum RelTarget<'a> {
    /// The canonical `"schema_name.relation_name"`.
    Name(&'a str),
    Id(u64),
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
    /// address (see `ClientTransport::connect`). Returns the session paired with
    /// the server durability watermark from the HELLO ACK, which
    /// `GnitzClient::connect` adopts as the seed for its OCC basis.
    pub fn connect(target: &str) -> Result<(Self, u64), ClientError> {
        let mut transport = ClientTransport::connect(target)?;
        // Run the HELLO handshake before any data flows. The server
        // accepts the first frame at an 8-byte limit, so this must
        // happen before `send_message` would emit a control block.
        let (limit, published_lsn) = hello_handshake(&mut transport)?;
        let session = Session {
            transport,
            client_id: new_client_id(),
            max_payload_len: limit as usize,
            schema_cache: LruCache::new(SCHEMA_CACHE_CAP),
        };
        Ok((session, published_lsn))
    }

    /// Request frames this session has written since it connected. Counted in
    /// the transport, so it covers every verb without a per-path bump. Tests
    /// assert on it to pin the per-statement round-trip count.
    pub fn requests_sent(&self) -> u64 {
        self.transport.frames_sent()
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
    ///
    /// `preconditions` carries the OCC `(tid, basis)` assertions ("`tid` not
    /// written since `basis`"); the server rejects the whole transaction with
    /// `ClientError::TxnConflict` if any fails. Every precondition tid must be a
    /// family tid (the engine rejects otherwise). Pass an empty slice for none.
    pub fn push_txn(
        &mut self,
        families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)],
        preconditions: &[(u64, u64)],
    ) -> Result<u64, ClientError> {
        for (_, schema, batch, _) in families {
            batch.validate(schema).map_err(ClientError::ServerError)?;
        }
        let payload = encode_push_txn(self.client_id, families, preconditions);
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

    /// Consistent multi-relation scan: snapshot every relation in `tids` at one
    /// server-side SAL cut and return their results in request order. An atomic
    /// multi-table commit (a `push_txn`) is either visible in every result or in
    /// none — never torn across the set. `scan_multi(&[t]) == scan(t)`.
    ///
    /// Sends one SCAN_MULTI frame stamped with each relation's cached schema
    /// version (so warm relations omit their schema block), then reads the N
    /// reply trains positionally — the same continuation-reassembly and
    /// cache-absorption `recv_scan` uses, run once per relation in request
    /// order. Like `scan`, it does not advance any commit watermark. A duplicate
    /// tid or a list outside `1..=SCAN_MULTI_MAX_RELATIONS` is rejected locally
    /// by the shared frame encoder (the same check the server runs) before the
    /// frame is sent; other shape/tid errors surface from the server as
    /// `ClientError::ServerError`.
    pub fn scan_multi(&mut self, tids: &[u64]) -> MultiScanResult {
        let payload = self.encode_scan_multi_frame(tids)?;
        self.transport.send_framed(&payload)?;
        // Read the N trains positionally: train i == relation i in request order.
        // `recv_scan` absorbs each relation's schema block into the cache and
        // returns its (schema, batch, lsn) — reused verbatim, N times.
        let mut results = Vec::with_capacity(tids.len());
        for &tid in tids {
            results.push(self.recv_scan(tid)?);
        }
        Ok(results)
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
        // `send_message`'s `split_wire` routes slot 0 → seek_pk and slots 1..K →
        // seek_pk_extra, where the worker reassembles them with
        // `unpack_index_key_slots`. Arity is validated upstream in
        // `GnitzClient::seek_by_index` (the one choke point for every binding).
        let (buf, len) = gnitz_wire::pack_index_key_slots(key_vals);
        let pk = PkTuple::from_bytes(&buf[..len]);
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

    /// Describe one relation in a single round trip: `(live tid, schema,
    /// descriptor)`, or `None` when no such relation exists — a successful
    /// answer the caller renders in its own wording.
    ///
    /// The descriptor's foreign keys are merged into the schema here, while the
    /// `Arc` is still unique, so the block installed in `schema_cache` is the
    /// same FK-complete schema the caller gets rather than a second copy of it.
    ///
    /// The reply is received uncorrelated (`recv_message(.., None, ..)`): it
    /// never carries data, and the cache-aware `recv_cached` would key the block
    /// under the *requested* tid, which for a by-name resolve is 0. The block is
    /// instead installed under the **live** tid the reply carries, which is what
    /// keeps a following `scan`/`push` on its warm path.
    pub(crate) fn resolve(
        &mut self,
        target: RelTarget<'_>,
    ) -> Result<Option<(u64, Arc<Schema>, RelDescriptorBlob)>, ClientError> {
        // The one place the request's "name wins, else id" encoding is spelled.
        let (target_id, qname) = match target {
            RelTarget::Name(q) => (0, q),
            RelTarget::Id(tid) => (tid, ""),
        };
        // The name rides an explicit extra blob: `send_message` would derive one
        // from `PkTuple::split_wire` and silently truncate past `MAX_PK_BYTES`.
        send_message_with_extra(
            &mut self.transport,
            target_id,
            self.client_id,
            FLAG_RESOLVE,
            0,
            qname.as_bytes(),
        )?;
        let msg = check_response(recv_message(&mut self.transport, None, self.max_payload_len)?)?;
        let ncols = msg.schema.as_ref().map_or(0, |s| s.columns.len());
        let Some(desc) = RelDescriptorBlob::decode(&msg.seek_pk_extra, ncols).map_err(ClientError::ServerError)? else {
            return Ok(None);
        };
        let mut schema = msg
            .schema
            .ok_or_else(|| ClientError::ServerError("resolve reply carried no schema block".to_string()))?;
        // `batch_to_schema` rebuilds every column-layout fact but leaves the FK
        // fields at 0 — a reference to *another* relation rides the descriptor.
        // `decode` bounded every `col_idx` against this schema's column count.
        if !desc.fks.is_empty() {
            let cols = &mut Arc::make_mut(&mut schema).columns;
            for fk in &desc.fks {
                cols[fk.col_idx as usize].fk_table_id = fk.fk_table_id;
                cols[fk.col_idx as usize].fk_col_idx = fk.fk_col_idx as u64;
            }
        }
        self.schema_cache.put(
            msg.target_id,
            (Arc::clone(&schema), wire_flags_get_schema_version(msg.flags)),
        );
        Ok(Some((msg.target_id, schema, desc)))
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

    /// Pack a SCAN_MULTI request (control-only), stamping each relation with its
    /// cached schema version. The whole self-contained frame body rides the
    /// `ctrl` segment; the matching receiver reads N `recv_scan` trains in
    /// request order. Rejects a list outside `1..=SCAN_MULTI_MAX_RELATIONS` or
    /// with a duplicate tid through the shared `encode_scan_multi_frame`, so an
    /// async driver needs no pre-check of its own.
    pub fn pack_scan_multi(&self, tids: &[u64]) -> Result<MessageParts, ClientError> {
        Ok(MessageParts {
            ctrl: self.encode_scan_multi_frame(tids)?,
            schema: None,
            data: Vec::new(),
        })
    }

    /// Ship many pre-encoded frames as one vectored write sequence.
    pub fn send_batch(&mut self, parts: &[MessageParts]) -> Result<(), ProtocolError> {
        self.transport.send_framed_batch(parts)
    }

    /// Reassemble one reply train: receive frames via `recv_one` until the
    /// terminal (non-continuation) frame, keeping the first in-frame schema and
    /// concatenating data batches. Status is checked on **every** frame — a
    /// `STATUS_ERROR` fault frame has flags 0, structurally identical to the
    /// master's terminal frame, so a flags-only check would silently drop it.
    /// Returns `(schema, data, terminal seek_pk)` — the scan paths read the
    /// terminal frame's `seek_pk` as the last-committed LSN.
    #[allow(clippy::type_complexity)] // the (schema, data, terminal seek_pk) reply tuple
    fn drain_reply_train(
        &mut self,
        mut recv_one: impl FnMut(&mut Self) -> Result<Message, ClientError>,
    ) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>, u64), ClientError> {
        let mut schema: Option<Arc<Schema>> = None;
        let mut data: Option<ZSetBatch> = None;
        let lsn: u64 = loop {
            let msg = check_response(recv_one(self)?)?;
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
        Ok((schema, data, lsn))
    }

    /// Receive a streaming scan/seek response: reassemble continuation frames,
    /// absorb any schema block into the cache, and recover the schema from the
    /// cache if the response was schema-less. Same body as the sync `scan`.
    pub fn recv_scan(&mut self, target_id: u64) -> ScanResult {
        let (mut schema, data, lsn) = self.drain_reply_train(|s| s.recv_cached(target_id))?;
        // Warm-cache responses omit the schema block. Recover once from the LRU.
        if schema.is_none() {
            schema = self.schema_cache.get(&target_id).map(|(s, _)| Arc::clone(s));
        }
        Ok((schema, data, lsn))
    }

    /// Ship a parameterized bounded read (`ReadSpec`) and reassemble its result.
    /// `spec` is the encoded `ReadSpec`; `reply_schema` is the schema the caller
    /// built for the result — it is encoded into the request blob (bundled with
    /// `spec` by [`gnitz_wire::pack_scan_spec_extra`], which the master forwards
    /// verbatim) and is the decode hint for every reply frame, since the server
    /// sends no schema block back. Returns one concatenated batch. Like `scan`, it
    /// advances no commit watermark and — critically — never touches the schema
    /// cache: a per-query projected schema keyed under the table id would corrupt
    /// a later plain scan of the same relation.
    pub fn scan_spec(
        &mut self,
        target_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<Option<ZSetBatch>, ClientError> {
        let block = encode_schema_block(reply_schema, target_id as u32);
        let extra = gnitz_wire::pack_scan_spec_extra(spec, &block);
        send_message_with_extra(
            &mut self.transport,
            target_id,
            self.client_id,
            FLAG_SCAN_SPEC,
            0,
            &extra,
        )?;
        let (_, data, _) = self.drain_reply_train(|s| {
            Ok(recv_message(
                &mut s.transport,
                Some((reply_schema, 0)),
                s.max_payload_len,
            )?)
        })?;
        Ok(data)
    }

    /// Receive a single push ACK and return its ingest LSN, absorbing any schema
    /// block it carries into the cache (matching the sync push path). The status
    /// runs through the session's own `check_response`, so a pipelined push
    /// classifies server failures exactly as the sync push does rather than
    /// leaving each caller to re-derive the policy.
    pub fn recv_push_ack(&mut self, target_id: u64) -> Result<u64, ClientError> {
        let msg = self.recv_cached(target_id)?;
        check_response(msg).map(|m| m.seek_pk as u64)
    }

    /// The client's cached schema version for `tid` (`0` = no cached schema, so
    /// the server sends the block). `peek` leaves LRU recency untouched — a
    /// version probe is not an access.
    fn cached_schema_version(&self, tid: u64) -> u16 {
        self.schema_cache.peek(&tid).map(|(_, v)| *v).unwrap_or(0)
    }

    /// Build a SCAN_MULTI request frame, stamping each tid with its cached schema
    /// version. The one choke point both encode routes funnel through — the sync
    /// `scan_multi` (which sends it) and the async `pack_scan_multi` (which wraps
    /// it as the control segment) — so the wire-shape contract is enforced here,
    /// in every build profile, for every caller. The case that matters is the
    /// empty list: it would encode a count=0 frame whose lone server error frame
    /// the N=0 read loop never consumes, permanently shifting every later read on
    /// this connection by one frame.
    fn encode_scan_multi_frame(&self, tids: &[u64]) -> Result<Vec<u8>, ClientError> {
        gnitz_wire::validate_scan_multi_tids(tids).map_err(ClientError::ServerError)?;
        let relations: Vec<(u64, u16)> = tids.iter().map(|&tid| (tid, self.cached_schema_version(tid))).collect();
        Ok(encode_scan_multi(self.client_id, &relations))
    }

    /// The cached schema version for `target_id` OR'd into the flag word, so a
    /// warm-cache request lets the server omit the schema block.
    fn versioned_flags(&self, target_id: u64, base: u64) -> u64 {
        wire_flags_set_schema_version(base, self.cached_schema_version(target_id))
    }

    /// Recover the schema for a single-frame seek response and assemble the
    /// `ScanResult`: prefer the in-frame schema, else fall back to the cache.
    /// Shared tail of `seek` / `seek_by_index`.
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
            seek_pk_extra: Vec::new(),
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
