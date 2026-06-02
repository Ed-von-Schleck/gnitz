use std::os::fd::{OwnedFd, FromRawFd, AsRawFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

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
    if msg.status == STATUS_ERROR {
        // Fall back to the default text on an empty string, not only on None:
        // a STATUS_ERROR with Some("") would otherwise surface as a blank
        // ServerError. This matters because the warm-push guard converts
        // silent corruption into a surfaced error, which must be legible.
        let text = msg.error_text
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown server error".into());
        return Err(ClientError::ServerError(text));
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
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> Result<u64, ClientError> {
        self.push_with_mode(target_id, schema, batch, WireConflictMode::Update, cache)
    }

    pub fn push_with_mode(
        &mut self,
        target_id: u64,
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> Result<u64, ClientError> {
        batch.validate(schema).map_err(ClientError::ServerError)?;
        let msg = self.roundtrip_push(target_id, schema, batch, mode, cache)?;
        Ok(msg.seek_pk as u64)
    }

    pub fn scan(
        &mut self,
        target_id: u64,
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> ScanResult {
        let cached_version = cache.peek(&target_id).map(|(_, v)| *v).unwrap_or(0);
        let flags = wire_flags_set_schema_version(0, cached_version);
        // Send the scan request then collect streaming worker frames
        // (each tagged FLAG_CONTINUATION) until the terminal frame arrives.
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, &PkTuple::EMPTY, 0, None, None)?;
        let mut schema: Option<Arc<Schema>> = None;
        let mut data:   Option<ZSetBatch> = None;
        let lsn: u64 = loop {
            let msg = check_response(self.recv_message_cached_inner(target_id, cache)?)?;
            let is_continuation = (msg.flags & FLAG_CONTINUATION) != 0;
            schema = schema.or(msg.schema.map(Arc::new));
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
        // Warm-cache responses omit the schema block. Recover once from the LRU cache.
        if schema.is_none() {
            schema = cache.get(&target_id).map(|(s, _)| Arc::clone(s));
        }
        Ok((schema, data, lsn))
    }

    pub fn seek(
        &mut self,
        target_id: u64,
        pk:        &PkTuple,
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> ScanResult {
        let msg = self.roundtrip_seek(target_id, pk, cache)?;
        let schema = msg.schema.map(Arc::new).or_else(|| cache.get(&target_id).map(|(s, _)| Arc::clone(s)));
        Ok((schema, msg.data_batch, msg.seek_pk as u64))
    }

    pub fn seek_by_index(
        &mut self,
        table_id: u64,
        col_idx:  u64,
        key:      u128,
        cache:    &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> ScanResult {
        let msg = self.roundtrip_seek_by_index(table_id, col_idx, key, cache)?;
        let schema = msg.schema.map(Arc::new).or_else(|| cache.get(&table_id).map(|(s, _)| Arc::clone(s)));
        Ok((schema, msg.data_batch, msg.seek_pk as u64))
    }

    /// Receive one framed message, using the LRU cache to decode continuation
    /// frames that arrive without a schema block.
    fn recv_message_cached_inner(
        &mut self,
        target_id: u64,
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> Result<Message, ClientError> {
        let msg = {
            // `get` (not `peek`) so a frequently-accessed schema refreshes its
            // LRU recency and isn't evicted under memory pressure.
            let hint = cache.get(&target_id).map(|(s, v)| (s.as_ref(), *v));
            recv_message(self.sock.as_raw_fd(), hint, self.max_payload_len)?
        };
        // schema_batch is Some only when the schema block was physically in the frame;
        // schema is always Some when schema_batch is (invariant in parse_response).
        if msg.schema_batch.is_some() {
            let version = wire_flags_get_schema_version(msg.flags);
            let s = msg.schema.as_ref().unwrap();
            cache.put(target_id, (Arc::new(s.clone()), version));
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
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, &PkTuple::EMPTY, 0, schema, data)?;
        // Alloc roundtrips carry no schema blocks; recv_message without a hint is sufficient.
        let msg = recv_message(self.sock.as_raw_fd(), None, self.max_payload_len)?;
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
        schema:    &Schema,
        batch:     &ZSetBatch,
        mode:      WireConflictMode,
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> Result<Message, ClientError> {
        let base_flags = wire_flags_set_conflict_mode(0, mode);
        let warm_version: Option<u16> = match cache.peek(&target_id) {
            Some((cached_schema, v)) if *v != 0 && schema.types_match(cached_schema.as_ref()) => Some(*v),
            _ => None,
        };
        if let Some(cached_version) = warm_version {
            // Warm path: omit schema block, embed cached version.
            let flags = wire_flags_set_schema_version(base_flags, cached_version);
            send_message_noschema(self.sock.as_raw_fd(), target_id, self.client_id, flags, schema, batch)?;
        } else {
            // Cold path: include schema block, version = 0.
            send_message(self.sock.as_raw_fd(), target_id, self.client_id, base_flags, &PkTuple::EMPTY, 0, Some(schema), Some(batch))?;
        }
        let ack = match check_response(self.recv_message_cached_inner(target_id, cache)?) {
            Err(ClientError::SchemaMismatch) => {
                // Stale cache: evict and retry with full schema.
                cache.pop(&target_id);
                send_message(self.sock.as_raw_fd(), target_id, self.client_id, base_flags, &PkTuple::EMPTY, 0, Some(schema), Some(batch))?;
                check_response(self.recv_message_cached_inner(target_id, cache)?)?
            }
            Ok(msg) => msg,
            Err(e)  => return Err(e),
        };
        // No manual cache write here. Whenever the server changed the schema
        // version it also included the schema block in the ACK
        // (wire_should_include_schema), and recv_message_cached_inner already
        // cached that authoritative schema (with the server's real column
        // names). A schema.clone() here would clobber it with the caller's
        // copy — dropping the server's column names, and on schema evolution
        // pairing the OLD schema with the NEW version. On the pure warm path
        // the version is unchanged: nothing to do.
        Ok(ack)
    }

    fn roundtrip_seek_by_index(
        &mut self,
        table_id: u64,
        col_idx:  u64,
        key:      u128,
        cache:    &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> Result<Message, ClientError> {
        // Embed the cached schema version so the server can omit the schema
        // block on a warm-cache hit (matching roundtrip_push/scan).
        let cached_version = cache.peek(&table_id).map(|(_, v)| *v).unwrap_or(0);
        let flags = wire_flags_set_schema_version(FLAG_SEEK_BY_INDEX, cached_version);
        send_message(self.sock.as_raw_fd(), table_id, self.client_id, flags, &PkTuple::from_u128_narrow(key), col_idx, None, None)?;
        let msg = self.recv_message_cached_inner(table_id, cache)?;
        check_response(msg)
    }

    fn roundtrip_seek(
        &mut self,
        target_id: u64,
        pk:        &PkTuple,
        cache:     &mut LruCache<u64, (Arc<Schema>, u16)>,
    ) -> Result<Message, ClientError> {
        let cached_version = cache.peek(&target_id).map(|(_, v)| *v).unwrap_or(0);
        let flags = wire_flags_set_schema_version(FLAG_SEEK, cached_version);
        send_message(self.sock.as_raw_fd(), target_id, self.client_id, flags, pk, 0, None, None)?;
        let msg = self.recv_message_cached_inner(target_id, cache)?;
        check_response(msg)
    }
}

#[cfg(test)]
mod cache_tests {
    use std::sync::Arc;
    use lru::LruCache;
    use crate::protocol::{Schema, ColumnDef, TypeCode};

    #[test]
    fn schema_cache_arc_not_clone() {
        let mut cache: LruCache<u64, (Arc<Schema>, u16)> =
            LruCache::new(std::num::NonZeroUsize::new(4).unwrap());
        let schema = Arc::new(Schema {
            columns: vec![
                ColumnDef { name: "id".into(), type_code: TypeCode::I64,
                             is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
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
            target_id: 0, client_id: 0, flags: 0, seek_pk: 0,
            seek_col_idx: 0, request_id: 0,
            schema: None, schema_batch: None, data_batch: None,
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
        assert_eq!(server_error_text(error_msg(Some(String::new()))), "unknown server error");
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
