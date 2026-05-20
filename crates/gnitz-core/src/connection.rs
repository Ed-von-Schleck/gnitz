use std::os::fd::{OwnedFd, FromRawFd, AsRawFd};
use std::sync::atomic::{AtomicU32, Ordering};

use lru::LruCache;
use crate::protocol::{
    Message, Schema, ZSetBatch, PkTuple,
    STATUS_ERROR, STATUS_SCHEMA_MISMATCH, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_INDEX_ID,
    FLAG_CONTINUATION, WireConflictMode,
    wire_flags_set_conflict_mode, wire_flags_set_schema_version, wire_flags_get_schema_version,
    send_message, send_message_noschema, recv_message,
    connect as proto_connect,
    hello_handshake,
};
use crate::error::ClientError;

pub use gnitz_wire::{
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, IDX_TAB, DEP_TAB, SEQ_TAB,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID,
};

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
    if msg.status == STATUS_ERROR {
        return Err(ClientError::ServerError(
            msg.error_text.unwrap_or_else(|| "unknown server error".into())
        ));
    }
    Ok(msg)
}

pub struct Connection {
    sock:          OwnedFd,
    pub client_id: u64,
    /// Server-negotiated per-connection frame payload ceiling. Set during
    /// `connect()` from the HELLO ACK; subsequent `recv_message` calls
    /// pass this value through so a compromised server cannot force the
    /// client to allocate up to the historical 256 MB ceiling.
    max_payload_len: usize,
}

impl Connection {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError> {
        let fd = proto_connect(socket_path)?;
        // SAFETY: proto_connect returns a valid, exclusively-owned file descriptor.
        let sock = unsafe { OwnedFd::from_raw_fd(fd) };
        // Run the HELLO handshake before any data flows. The server
        // accepts the first frame at an 8-byte limit, so this must
        // happen before `send_message` would emit a control block.
        let limit = hello_handshake(sock.as_raw_fd())?;
        Ok(Connection {
            sock,
            client_id: new_client_id(),
            max_payload_len: limit as usize,
        })
    }

    pub fn close(self) {
        // OwnedFd is dropped here, which closes the fd.
    }

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_TABLE_ID, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_SCHEMA_ID, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_INDEX_ID, None, None)?;
        Ok(msg.target_id)
    }

    /// Default push: silent-upsert (`WireConflictMode::Update`).
    /// Callers that need SQL-standard rejection use `push_with_mode`.
    pub fn push(
        &mut self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<u64, ClientError> {
        self.push_with_mode(target_id, schema, batch, WireConflictMode::Update, cache)
    }

    pub fn push_with_mode(
        &mut self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<u64, ClientError> {
        batch.validate(schema).map_err(ClientError::ServerError)?;
        let msg = self.roundtrip_push(target_id, schema, batch, mode, cache)?;
        Ok(msg.seek_pk as u64)
    }

    pub fn scan(
        &mut self,
        target_id: u64,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let cached_version = cache.peek(&target_id).map(|(_, v)| *v).unwrap_or(0);
        let flags = wire_flags_set_schema_version(0, cached_version);
        // Send the scan request then collect streaming worker frames
        // (each tagged FLAG_CONTINUATION) until the terminal frame arrives.
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, 0u128, &[], 0, None, None)?;
        let mut schema: Option<Schema> = None;
        let mut data:   Option<ZSetBatch> = None;
        let lsn: u64 = loop {
            let msg = check_response(self.recv_message_cached_inner(target_id, cache)?)?;
            let is_continuation = (msg.flags & FLAG_CONTINUATION) != 0;
            schema = schema.or(msg.schema);
            if let Some(batch) = msg.data_batch {
                match data.as_mut() {
                    Some(acc) => acc.extend_from(&batch),
                    None => data = Some(batch),
                }
            }
            if !is_continuation {
                break msg.seek_pk as u64;
            }
        };
        Ok((schema, data, lsn))
    }

    pub fn seek(
        &mut self,
        target_id: u64,
        pk:        &PkTuple,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip_seek(target_id, pk, cache)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk as u64))
    }

    pub fn seek_by_index(
        &mut self,
        table_id: u64,
        col_idx:  u64,
        key:      u128,
        cache:    &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip_seek_by_index(table_id, col_idx, key, cache)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk as u64))
    }

    /// Receive one framed message, using the LRU cache to decode continuation
    /// frames that arrive without a schema block.
    fn recv_message_cached_inner(
        &mut self,
        target_id: u64,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<Message, ClientError> {
        let msg = {
            let hint = cache.peek(&target_id).map(|(s, v)| (s, *v));
            recv_message(self.sock.as_raw_fd(), hint, self.max_payload_len)?
        };
        // schema_batch is Some only when the schema block was physically in the frame;
        // schema is always Some when schema_batch is (invariant in parse_response).
        if msg.schema_batch.is_some() {
            let version = wire_flags_get_schema_version(msg.flags);
            let s = msg.schema.as_ref().unwrap();
            cache.put(target_id, (s.clone(), version));
        }
        Ok(msg)
    }

    fn roundtrip(
        &mut self,
        target_id: u64,
        flags:     u64,
        schema:    Option<&Schema>,
        data:      Option<&ZSetBatch>,
    ) -> Result<Message, ClientError> {
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, 0u128, &[], 0, schema, data)?;
        // Alloc roundtrips carry no schema blocks; recv_message without a hint is sufficient.
        let msg = recv_message(self.sock.as_raw_fd(), None, self.max_payload_len)?;
        check_response(msg)
    }

    /// Push path: packs `WireConflictMode` into bits 16-23 of `wire_flags`.
    /// When the schema cache holds a valid version for `target_id`, omits the
    /// schema block from the wire frame (warm path). On STATUS_SCHEMA_MISMATCH
    /// the cache entry is evicted and the push is retried with the full schema.
    fn roundtrip_push(
        &mut self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<Message, ClientError> {
        let cached_version = cache.peek(&target_id).map(|(_, v)| *v).unwrap_or(0);
        let base_flags = wire_flags_set_conflict_mode(0, mode);
        if cached_version != 0 {
            // Warm path: omit schema block, embed cached version.
            let flags = wire_flags_set_schema_version(base_flags, cached_version);
            send_message_noschema(self.sock.as_raw_fd(), target_id, self.client_id, flags, schema, batch)?;
        } else {
            // Cold path: include schema block, version = 0.
            send_message(self.sock.as_raw_fd(), target_id, self.client_id, base_flags, 0u128, &[], 0, Some(schema), Some(batch))?;
        }
        let ack = match check_response(self.recv_message_cached_inner(target_id, cache)?) {
            Err(ClientError::SchemaMismatch) => {
                // Stale cache: evict and retry with full schema.
                cache.pop(&target_id);
                send_message(self.sock.as_raw_fd(), target_id, self.client_id, base_flags, 0u128, &[], 0, Some(schema), Some(batch))?;
                check_response(self.recv_message_cached_inner(target_id, cache)?)?
            }
            Ok(msg) => msg,
            Err(e)  => return Err(e),
        };
        // Warm the cache from the ACK's schema version (no schema block in ACK).
        let ack_version = wire_flags_get_schema_version(ack.flags);
        if ack_version != 0 && ack_version != cached_version {
            cache.put(target_id, (schema.clone(), ack_version));
        }
        Ok(ack)
    }

    fn roundtrip_seek_by_index(
        &mut self,
        table_id: u64,
        col_idx:  u64,
        key:      u128,
        cache:    &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<Message, ClientError> {
        send_message(self.sock.as_raw_fd(), table_id, self.client_id, FLAG_SEEK_BY_INDEX, key, &[], col_idx, None, None)?;
        let msg = self.recv_message_cached_inner(table_id, cache)?;
        check_response(msg)
    }

    fn roundtrip_seek(
        &mut self,
        target_id: u64,
        pk:        &PkTuple,
        cache:     &mut LruCache<u64, (Schema, u16)>,
    ) -> Result<Message, ClientError> {
        let (low_16, extra) = pk.split_wire();
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, FLAG_SEEK, low_16, extra, 0, None, None)?;
        let msg = self.recv_message_cached_inner(target_id, cache)?;
        check_response(msg)
    }
}
