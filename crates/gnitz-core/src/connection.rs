use std::os::fd::{OwnedFd, FromRawFd, AsRawFd};

const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();
use std::sync::{atomic::{AtomicU32, Ordering}, Mutex};
use lru::LruCache;
use crate::protocol::{
    Message, Schema, ZSetBatch,
    STATUS_ERROR, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_INDEX_ID,
    FLAG_CONTINUATION, WireConflictMode,
    wire_flags_set_conflict_mode, wire_flags_set_schema_version, wire_flags_get_schema_version,
    send_message, recv_message,
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
    /// Per-table schema cache: target_id → (Schema, server_version).
    /// Mutex for interior mutability; Connection is used in Send+Sync pyclasses.
    schema_cache: Mutex<LruCache<u64, (Schema, u16)>>,
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
            schema_cache: Mutex::new(LruCache::new(SCHEMA_CACHE_CAP)),
        })
    }

    pub fn close(self) {
        // OwnedFd is dropped here, which closes the fd.
    }

    pub fn alloc_table_id(&self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_TABLE_ID, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_schema_id(&self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_SCHEMA_ID, None, None)?;
        Ok(msg.target_id)
    }

    pub fn alloc_index_id(&self) -> Result<u64, ClientError> {
        let msg = self.roundtrip(0, FLAG_ALLOCATE_INDEX_ID, None, None)?;
        Ok(msg.target_id)
    }

    /// Default push: silent-upsert (`WireConflictMode::Update`).
    /// Callers that need SQL-standard rejection use `push_with_mode`.
    pub fn push(
        &self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
    ) -> Result<u64, ClientError> {
        self.push_with_mode(target_id, schema, batch, WireConflictMode::Update)
    }

    pub fn push_with_mode(
        &self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
    ) -> Result<u64, ClientError> {
        batch.validate(schema).map_err(ClientError::ServerError)?;
        let msg = self.roundtrip_push(target_id, schema, batch, mode)?;
        Ok(msg.seek_pk as u64)
    }

    pub fn scan(
        &self,
        target_id: u64,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let cached_version = {
            let cache = self.schema_cache.lock().unwrap();
            cache.peek(&target_id).map(|(_, v)| *v).unwrap_or(0)
        };
        let flags = wire_flags_set_schema_version(0, cached_version);
        // Send the scan request then collect streaming worker frames
        // (each tagged FLAG_CONTINUATION) until the terminal frame arrives.
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, 0u128, 0, None, None)?;
        let mut schema: Option<Schema> = None;
        let mut data:   Option<ZSetBatch> = None;
        let lsn: u64 = loop {
            let msg = check_response(self.recv_message_cached(target_id)?)?;
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
        &self,
        target_id: u64,
        pk:        u128,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip_seek(target_id, pk)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk as u64))
    }

    pub fn seek_by_index(
        &self,
        table_id: u64,
        col_idx:  u64,
        key:      u128,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        let msg = self.roundtrip_seek_by_index(table_id, col_idx, key)?;
        Ok((msg.schema, msg.data_batch, msg.seek_pk as u64))
    }

    /// Receive one framed message, using the LRU cache to decode continuation
    /// frames that arrive without a schema block.
    fn recv_message_cached(&self, target_id: u64) -> Result<Message, ClientError> {
        let hint = {
            let mut cache = self.schema_cache.lock().unwrap();
            cache.get(&target_id).map(|(s, v)| (s.clone(), *v))
        };
        let msg = recv_message(
            self.sock.as_raw_fd(),
            hint.as_ref().map(|(s, v)| (s, *v)),
            self.max_payload_len,
        )?;
        // schema_batch is Some only when the schema block was physically in the frame;
        // schema is always Some when schema_batch is (invariant in parse_response).
        if msg.schema_batch.is_some() {
            let version = wire_flags_get_schema_version(msg.flags);
            let s = msg.schema.as_ref().unwrap();
            self.schema_cache.lock().unwrap().put(target_id, (s.clone(), version));
        }
        Ok(msg)
    }

    fn roundtrip(
        &self,
        target_id: u64,
        flags:     u64,
        schema:    Option<&Schema>,
        data:      Option<&ZSetBatch>,
    ) -> Result<Message, ClientError> {
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, 0u128, 0, schema, data)?;
        let msg = self.recv_message_cached(target_id)?;
        check_response(msg)
    }

    /// Push path: packs `WireConflictMode` into bits 16-23 of `wire_flags`.
    fn roundtrip_push(
        &self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
    ) -> Result<Message, ClientError> {
        let flags = wire_flags_set_conflict_mode(0, mode);
        send_message(
            self.sock.as_raw_fd(), target_id, self.client_id, flags,
            0u128, 0,
            Some(schema), Some(batch),
        )?;
        let msg = self.recv_message_cached(target_id)?;
        check_response(msg)
    }

    fn roundtrip_seek_by_index(
        &self,
        table_id: u64,
        col_idx:  u64,
        key:      u128,
    ) -> Result<Message, ClientError> {
        send_message(self.sock.as_raw_fd(), table_id, self.client_id, FLAG_SEEK_BY_INDEX, key, col_idx, None, None)?;
        let msg = self.recv_message_cached(table_id)?;
        check_response(msg)
    }

    fn roundtrip_seek(
        &self,
        target_id: u64,
        pk:        u128,
    ) -> Result<Message, ClientError> {
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, FLAG_SEEK, pk, 0, None, None)?;
        let msg = self.recv_message_cached(target_id)?;
        check_response(msg)
    }
}
