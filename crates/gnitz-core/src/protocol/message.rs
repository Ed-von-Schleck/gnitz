use super::codec::{batch_to_schema, schema_to_batch};
use super::error::ProtocolError;
use super::header::WAL_BLOCK_HEADER_SIZE;
use super::header::{
    wire_flags_get_schema_version, Header, FLAG_DDL_TXN, FLAG_HAS_DATA, FLAG_HAS_SCHEMA, STATUS_ERROR, STATUS_NO_INDEX,
    STATUS_OK, STATUS_SCHEMA_MISMATCH,
};
use super::transport::ClientTransport;
use super::types::{meta_schema, PkTuple, Schema, ZSetBatch};
use super::wal_block::{decode_wal_block, encode_wal_block};
use xxhash_rust::xxh3::xxh3_64;

pub struct Message {
    pub status: u32,
    pub target_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    pub seek_col_idx: u64,
    /// The schema, `Some` iff a schema block was physically in the frame (an
    /// `Arc` so a cache absorb is a refcount bump, not a deep copy). On a
    /// hint-only continuation frame it is `None` — the caller supplied the
    /// schema out of band.
    pub schema: Option<std::sync::Arc<Schema>>,
    pub data_batch: Option<ZSetBatch>,
    pub error_text: Option<String>, // Some(_) when status == STATUS_ERROR
}

// ── Control block ─────────────────────────────────────────────────────────────
//
// The control-block wire codec (layout, template encoder, directory-driven
// decoder) lives in `gnitz_wire::control` — the one implementation both the
// client and the engine run. The wrappers here adapt it to the client types:
// `Header` in/out, UTF-8 validated error text, and the checksum stamp every
// client TCP frame carries.

/// Encode a `Header` + optional error message + optional wide-PK extra bytes
/// into a control WAL block. When `error_msg` is empty the error_msg column
/// is NULL; when `seek_pk_extra` is empty the seek_pk_extra column is NULL.
pub fn encode_control_block(header: &Header, error_msg: &str, seek_pk_extra: &[u8]) -> Vec<u8> {
    let total = gnitz_wire::control::ctrl_block_size(error_msg.len(), seek_pk_extra.len());
    let mut buf = vec![0u8; total];
    gnitz_wire::control::encode_ctrl_block(
        &mut buf,
        0,
        header.target_id,
        header.client_id,
        header.flags,
        header.seek_pk,
        header.seek_col_idx,
        header.request_id,
        header.status,
        error_msg.as_bytes(),
        seek_pk_extra,
    );
    // Client frames carry a body checksum, matching `encode_wal_block`.
    let checksum = xxh3_64(&buf[WAL_BLOCK_HEADER_SIZE..]);
    buf[gnitz_wire::WAL_OFF_CHECKSUM..gnitz_wire::WAL_OFF_CHECKSUM + 8].copy_from_slice(&checksum.to_le_bytes());
    buf
}

/// Decode a control WAL block, returning `(Header, error_msg, seek_pk_extra)`.
/// `error_msg` is empty when the null bit for error_msg is set;
/// `seek_pk_extra` is empty when the null bit for seek_pk_extra is set.
pub fn decode_control_block(data: &[u8]) -> Result<(Header, String, Vec<u8>), ProtocolError> {
    let dc = gnitz_wire::control::peek_control_block(data).map_err(|e| ProtocolError::DecodeError(e.into()))?;
    let error_msg =
        String::from_utf8(dc.error_msg).map_err(|e| ProtocolError::DecodeError(format!("utf8 in error_msg: {e}")))?;
    let header = Header {
        status: dc.status,
        target_id: dc.target_id,
        client_id: dc.client_id,
        flags: dc.flags,
        seek_pk: dc.seek_pk,
        seek_col_idx: dc.seek_col_idx,
        request_id: dc.request_id,
    };
    Ok((header, error_msg, dc.seek_pk_extra))
}

/// An encoded wire message as its constituent WAL blocks — control, optional
/// schema, data (empty when no data block) — kept separate so send paths can
/// hand them to a vectored write (one length prefix over the concatenation)
/// instead of flattening into one contiguous buffer.
pub struct MessageParts {
    pub ctrl: Vec<u8>,
    pub schema: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

impl MessageParts {
    /// The blocks in wire order, for a vectored send. Empty segments are
    /// skipped by the transport.
    pub fn segments(&self) -> [&[u8]; 3] {
        [&self.ctrl, self.schema.as_deref().unwrap_or(&[]), &self.data]
    }

    /// Flatten into one contiguous payload, for callers that need owned bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        let [a, b, c] = self.segments();
        let mut out = Vec::with_capacity(a.len() + b.len() + c.len());
        out.extend_from_slice(a);
        out.extend_from_slice(b);
        out.extend_from_slice(c);
        out
    }
}

impl super::transport::FrameSegments for MessageParts {
    fn segments(&self) -> [&[u8]; 3] {
        MessageParts::segments(self)
    }
}

/// Encode a request/response into its wire blocks (without the 4-byte frame
/// header): control + optional schema + optional data. Pure function — pass
/// the parts to `send_framed_iov` / `send_framed_batch` for framing.
///
/// `seek_pk` carries the seek key for `FLAG_SEEK` / `FLAG_SEEK_BY_INDEX` frames;
/// pass `&PkTuple::EMPTY` for non-seek frames. The wire-level
/// `(seek_pk: u128, seek_pk_extra: BLOB)` split is performed here via
/// `PkTuple::split_wire`, so callers never handle it.
pub fn encode_message_parts(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: &PkTuple,
    seek_col_idx: u64,
    schema: Option<&Schema>,
    data_batch: Option<&ZSetBatch>,
) -> MessageParts {
    let has_data = data_batch.map(|b| !b.is_empty()).unwrap_or(false);
    let has_schema = schema.is_some();

    let mut flags_out = flags;
    if has_schema {
        flags_out |= FLAG_HAS_SCHEMA;
    }
    if has_data {
        flags_out |= FLAG_HAS_DATA;
    }

    let (seek_pk_lo, seek_pk_extra) = seek_pk.split_wire();
    let ctrl_hdr = Header {
        status: STATUS_OK,
        target_id,
        client_id,
        flags: flags_out,
        seek_pk: seek_pk_lo,
        seek_col_idx,
        request_id: 0,
    };
    let ctrl = encode_control_block(&ctrl_hdr, "", seek_pk_extra);
    let schema_block = schema.map(|s| encode_wal_block(meta_schema(), target_id as u32, &schema_to_batch(s)));
    let data = if has_data {
        encode_wal_block(schema.unwrap(), target_id as u32, data_batch.unwrap())
    } else {
        Vec::new()
    };
    MessageParts {
        ctrl,
        schema: schema_block,
        data,
    }
}

/// Like [`encode_message_parts`] but omits the schema block from the frame.
/// The data block is still encoded using `data_schema`; the server
/// reconstructs the schema from its catalog (guided by the schema version in
/// `flags`). Used by warm-cache PUSH paths where the server already knows
/// the schema (version embedded in `flags` bits 24-39).
pub fn encode_message_noschema_parts(
    target_id: u64,
    client_id: u64,
    flags: u64,
    data_schema: &Schema,
    data_batch: &ZSetBatch,
) -> MessageParts {
    let has_data = !data_batch.is_empty();
    let mut flags_out = flags;
    if has_data {
        flags_out |= FLAG_HAS_DATA;
    }
    let ctrl_hdr = Header {
        status: STATUS_OK,
        target_id,
        client_id,
        flags: flags_out,
        seek_pk: 0,
        seek_col_idx: 0,
        request_id: 0,
    };
    let ctrl = encode_control_block(&ctrl_hdr, "", &[]);
    let data = if has_data {
        encode_wal_block(data_schema, target_id as u32, data_batch)
    } else {
        Vec::new()
    };
    MessageParts {
        ctrl,
        schema: None,
        data,
    }
}

/// Flattened form of [`encode_message_parts`], for callers that need one
/// contiguous payload buffer.
#[allow(clippy::too_many_arguments)]
pub fn encode_message(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: &PkTuple,
    seek_col_idx: u64,
    schema: Option<&Schema>,
    data_batch: Option<&ZSetBatch>,
) -> Vec<u8> {
    encode_message_parts(target_id, client_id, flags, seek_pk, seek_col_idx, schema, data_batch).to_vec()
}

/// Encode an atomic DDL transaction frame (`FLAG_DDL_TXN`) into wire bytes
/// (without the 4-byte frame header). Every system-table write — a `CREATE`'s N
/// family batches, a `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single batch — is
/// carried by one such frame so the server ingests the whole bundle under one
/// durable SAL zone.
///
/// Layout: the reused control block (`target_id = 0`, `flags = FLAG_DDL_TXN`),
/// then `u32` family count, then each family's `encode_wal_block`. Each data
/// block embeds its own `table_id` (WAL offset 8) and total size (WAL offset
/// 16), so the server decoder walks the list by header alone with no schema in
/// hand and defers schema resolution to the catalog layer.
pub fn encode_ddl_txn(client_id: u64, families: &[(u64, &Schema, ZSetBatch)]) -> Result<Vec<u8>, ProtocolError> {
    let ctrl_hdr = Header {
        status: STATUS_OK,
        target_id: 0,
        client_id,
        flags: FLAG_DDL_TXN,
        seek_pk: 0,
        seek_col_idx: 0,
        request_id: 0,
    };
    let mut out = encode_control_block(&ctrl_hdr, "", &[]);
    out.extend_from_slice(&(families.len() as u32).to_le_bytes());
    for (tid, schema, batch) in families {
        let block = encode_wal_block(schema, *tid as u32, batch);
        out.extend_from_slice(&block);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
pub fn send_message(
    t: &mut ClientTransport,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: &PkTuple,
    seek_col_idx: u64,
    schema: Option<&Schema>,
    data_batch: Option<&ZSetBatch>,
) -> Result<(), ProtocolError> {
    let parts = encode_message_parts(target_id, client_id, flags, seek_pk, seek_col_idx, schema, data_batch);
    t.send_framed_iov(&parts.segments())
}

/// Control-only frame carrying an **explicit, arbitrary-length** `seek_pk_extra`
/// blob, the channel for the SEEK_BY_INDEX_RANGE `RangeDescriptor`.
/// `encode_message` derives
/// `seek_pk_extra` from `seek_pk.split_wire()`, which a `PkTuple` caps at 64
/// bytes (`MAX_PK_BYTES - 16`); a max-arity range descriptor is up to 82 bytes,
/// so it cannot ride a `PkTuple`. This passes the blob straight to
/// `encode_control_block`, whose BLOB column is already arbitrary-length.
/// `seek_pk` is fixed at 0 (unused by the range seek). No schema/data block.
pub fn send_message_with_extra(
    t: &mut ClientTransport,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_col_idx: u64,
    seek_pk_extra: &[u8],
) -> Result<(), ProtocolError> {
    let ctrl_hdr = Header {
        status: STATUS_OK,
        target_id,
        client_id,
        flags,
        seek_pk: 0,
        seek_col_idx,
        request_id: 0,
    };
    let payload = encode_control_block(&ctrl_hdr, "", seek_pk_extra);
    t.send_framed(&payload)
}

/// Like `send_message` but omits the schema block from the wire frame.
/// The data block is still encoded using `data_schema`; the server must
/// reconstruct the schema from its catalog (guided by the schema version in
/// `flags`). Used by warm-cache PUSH paths where the server already knows the
/// schema (version embedded in `flags` bits 24-39).
pub fn send_message_noschema(
    t: &mut ClientTransport,
    target_id: u64,
    client_id: u64,
    flags: u64,
    data_schema: &Schema,
    data_batch: &ZSetBatch,
) -> Result<(), ProtocolError> {
    let parts = encode_message_noschema_parts(target_id, client_id, flags, data_schema, data_batch);
    t.send_framed_iov(&parts.segments())
}

/// Parse a wire payload (without 4-byte frame header) into a `Message`.
/// The payload is what `recv_framed()` returns.
///
/// `schema_hint` is `Some((schema, cached_version))` when the client has a
/// cached schema for the responding table. On continuation frames (no schema
/// block), the hint is used to decode the data block; a version mismatch is a
/// hard protocol error. Pass `None` for the initial frame or when no cache
/// entry exists.
pub fn parse_response(buf: &[u8], schema_hint: Option<(&Schema, u16)>) -> Result<Message, ProtocolError> {
    if buf.len() < WAL_BLOCK_HEADER_SIZE {
        return Err(ProtocolError::DecodeError("message too small".into()));
    }

    let ctrl_size = u32::from_le_bytes(
        buf[gnitz_wire::WAL_OFF_SIZE..gnitz_wire::WAL_OFF_SIZE + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    if ctrl_size > buf.len() {
        return Err(ProtocolError::DecodeError("control block truncated".into()));
    }

    let (ctrl_header, error_msg, _seek_pk_extra) = decode_control_block(&buf[..ctrl_size])?;

    let flags = ctrl_header.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    let mut off = ctrl_size;
    let mut wire_schema: Option<Schema> = None;

    if has_schema {
        if off + WAL_BLOCK_HEADER_SIZE > buf.len() {
            return Err(ProtocolError::DecodeError("schema block header truncated".into()));
        }
        let sz = u32::from_le_bytes(
            buf[off + gnitz_wire::WAL_OFF_SIZE..off + gnitz_wire::WAL_OFF_SIZE + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if off + sz > buf.len() {
            return Err(ProtocolError::DecodeError("schema block truncated".into()));
        }
        let ms = meta_schema();
        let (sbatch, _) = decode_wal_block(&buf[off..off + sz], ms)?;
        wire_schema = Some(batch_to_schema(&sbatch)?);
        off += sz;
    } else if has_data {
        match schema_hint {
            None => {
                return Err(ProtocolError::DecodeError(
                    "FLAG_HAS_DATA without FLAG_HAS_SCHEMA and no cached schema".into(),
                ))
            }
            Some((_hint_schema, cached_version)) => {
                let server_version = wire_flags_get_schema_version(flags);
                if server_version != cached_version {
                    return Err(ProtocolError::DecodeError(format!(
                        "schema version mismatch: cached={cached_version} server={server_version}"
                    )));
                }
                // hint-only frame: decode against borrowed hint below; no schema carried in Message
            }
        }
    }

    let data_batch = if has_data {
        let eff: &Schema = match wire_schema.as_ref() {
            Some(s) => s,
            None => schema_hint
                .map(|(s, _)| s)
                .ok_or_else(|| ProtocolError::DecodeError("no schema for data block".into()))?,
        };
        if off + WAL_BLOCK_HEADER_SIZE > buf.len() {
            return Err(ProtocolError::DecodeError("data block header truncated".into()));
        }
        let sz = u32::from_le_bytes(
            buf[off + gnitz_wire::WAL_OFF_SIZE..off + gnitz_wire::WAL_OFF_SIZE + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if off + sz > buf.len() {
            return Err(ProtocolError::DecodeError("data block truncated".into()));
        }
        let (batch, _) = decode_wal_block(&buf[off..off + sz], eff)?;
        Some(batch)
    } else {
        None
    };

    let (schema, data_batch, error_text) = if ctrl_header.status == STATUS_ERROR {
        (None, None, Some(error_msg))
    } else if ctrl_header.status == STATUS_SCHEMA_MISMATCH || ctrl_header.status == STATUS_NO_INDEX {
        // Control-only frame: no schema, no data, no error text.
        (None, None, None)
    } else {
        (wire_schema.map(std::sync::Arc::new), data_batch, None)
    };

    Ok(Message {
        status: ctrl_header.status,
        target_id: ctrl_header.target_id,
        flags: ctrl_header.flags,
        seek_pk: ctrl_header.seek_pk,
        seek_col_idx: ctrl_header.seek_col_idx,
        schema,
        data_batch,
        error_text,
    })
}

pub fn recv_message(
    t: &mut ClientTransport,
    schema_hint: Option<(&Schema, u16)>,
    max_payload_len: usize,
) -> Result<Message, ProtocolError> {
    let buf = t.recv_framed(max_payload_len)?;
    parse_response(&buf, schema_hint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::header::wire_flags_set_schema_version;
    use crate::protocol::header::{Header, FLAG_PUSH, FLAG_SEEK, STATUS_ERROR};
    use crate::protocol::types::{ColData, ColumnDef, PkColumn, PkTuple, Schema, TypeCode, ZSetBatch};
    use std::os::unix::io::RawFd;

    fn make_socketpair() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        unsafe {
            libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
        }
        (fds[0], fds[1])
    }

    /// Both ends of a socketpair as transports; drop closes the fds.
    fn make_transport_pair() -> (ClientTransport, ClientTransport) {
        let (a, b) = make_socketpair();
        (ClientTransport::from_unix_fd(a), ClientTransport::from_unix_fd(b))
    }

    // ── control block roundtrip ─────────────────────────────────────────────

    #[test]
    fn test_message_control_schema_roundtrip() {
        let h = Header {
            status: 0,
            target_id: 0x1234_5678_9ABC_DEF0,
            client_id: 0xDEAD_BEEF_0000_0001,
            flags: FLAG_PUSH | FLAG_HAS_SCHEMA,
            seek_pk: 42u128 | (99u128 << 64),
            seek_col_idx: 7,
            request_id: 0xCAFE_BABE_DEAD_F00D,
        };

        let encoded = encode_control_block(&h, "test error", &[]);
        let (decoded, err, _) = decode_control_block(&encoded).unwrap();

        assert_eq!(decoded.status, h.status);
        assert_eq!(decoded.target_id, h.target_id);
        assert_eq!(decoded.client_id, h.client_id);
        assert_eq!(decoded.flags, h.flags);
        assert_eq!(decoded.seek_pk, h.seek_pk);
        assert_eq!(decoded.seek_col_idx, h.seek_col_idx);
        assert_eq!(decoded.request_id, h.request_id);
        assert_eq!(err, "test error");
    }

    #[test]
    fn test_message_control_null_error_msg() {
        let h = Header::default();
        let encoded = encode_control_block(&h, "", &[]);
        let (_, err, _) = decode_control_block(&encoded).unwrap();
        assert_eq!(err, "");
    }

    /// Round-trips a wide `seek_pk_extra` (32 bytes) through encode + decode
    /// and back. Mirrors the server-side `ctrl_block_seek_pk_extra_roundtrip`
    /// test in `runtime/tests/wire.rs`.
    #[test]
    fn ctrl_block_seek_pk_extra_roundtrip() {
        let h = Header::default();
        let extra: Vec<u8> = (0..32u8).collect();
        let encoded = encode_control_block(&h, "", &extra);
        let (_, err, decoded_extra) = decode_control_block(&encoded).unwrap();
        assert_eq!(err, "");
        assert_eq!(decoded_extra, extra);

        // Empty seek_pk_extra → identical to today's frame (null bit set).
        let encoded2 = encode_control_block(&h, "", &[]);
        let (_, _, decoded_extra2) = decode_control_block(&encoded2).unwrap();
        assert!(decoded_extra2.is_empty());
    }

    /// `request_id` round-trips for the reserved sentinel values: 0 (untagged),
    /// u64::MAX (broadcast), and an arbitrary mid-range value. Catches both
    /// sign-extension bugs and the new-column-index drift between client and
    /// server.
    #[test]
    fn test_request_id_roundtrip_reserved_values() {
        for &req_id in &[0u64, u64::MAX, 0x1234_5678_DEAD_BEEFu64] {
            let h = Header {
                request_id: req_id,
                ..Header::default()
            };
            let encoded = encode_control_block(&h, "", &[]);
            let (decoded, _, _) = decode_control_block(&encoded).unwrap();
            assert_eq!(decoded.request_id, req_id);
        }
    }

    // ── send/recv message roundtrips ────────────────────────────────────────

    #[test]
    fn test_message_roundtrip_empty() {
        // Empty batch → FLAG_HAS_SCHEMA but not FLAG_HAS_DATA
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };

        let empty_batch = ZSetBatch::new(&schema);
        let (mut a, mut b) = make_transport_pair();
        send_message(
            &mut a,
            0,
            0,
            FLAG_PUSH,
            &PkTuple::EMPTY,
            0,
            Some(&schema),
            Some(&empty_batch),
        )
        .unwrap();
        let msg = recv_message(&mut b, None, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();

        // Schema was sent, data was not (empty batch)
        assert!(msg.schema.is_some());
        assert!(msg.data_batch.is_none());
    }

    #[test]
    fn test_message_roundtrip_data() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("i64", TypeCode::I64, false),
                ColumnDef::new("f64", TypeCode::F64, false),
            ],
            pk_cols: vec![0],
        };

        let n = 100usize;
        let pks: Vec<u128> = (0..n as u128).collect();
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64> = vec![0; n];

        let i64_vals: Vec<i64> = (0..n as i64).map(|x| x * -7).collect();
        let f64_vals: Vec<f64> = (0..n).map(|x| x as f64 * 1.5).collect();

        let mut i64_bytes = Vec::with_capacity(n * 8);
        for &v in &i64_vals {
            i64_bytes.extend_from_slice(&v.to_le_bytes());
        }
        let mut f64_bytes = Vec::with_capacity(n * 8);
        for &v in &f64_vals {
            f64_bytes.extend_from_slice(&v.to_le_bytes());
        }

        let batch = ZSetBatch {
            pks: PkColumn::U64s(pks.iter().map(|&x| x as u64).collect()),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(i64_bytes.clone()),
                ColData::Fixed(f64_bytes.clone()),
            ],
        };

        let (mut a, mut b) = make_transport_pair();
        send_message(&mut a, 0, 0, FLAG_PUSH, &PkTuple::EMPTY, 0, Some(&schema), Some(&batch)).unwrap();
        let msg = recv_message(&mut b, None, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();

        let data = msg.data_batch.unwrap();
        assert_eq!(data.pks.to_vec_u128(), pks);
        assert_eq!(data.weights, weights);

        match &data.columns[1] {
            ColData::Fixed(got) => assert_eq!(got, &i64_bytes),
            _ => panic!("expected Fixed at col 1"),
        }
        match &data.columns[2] {
            ColData::Fixed(got) => assert_eq!(got, &f64_bytes),
            _ => panic!("expected Fixed at col 2"),
        }
    }

    #[test]
    fn test_message_roundtrip_strings() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s1", TypeCode::String, true),
                ColumnDef::new("s2", TypeCode::String, false),
            ],
            pk_cols: vec![0],
        };

        let n = 50usize;
        let pks: Vec<u128> = (0..n as u128).collect();
        let weights: Vec<i64> = vec![1; n];
        // Every 3rd row: s1 is null → payload bit 0 set
        let nulls: Vec<u64> = (0..n).map(|i| if i % 3 == 0 { 1u64 } else { 0u64 }).collect();

        let col1: Vec<Option<String>> = (0..n)
            .map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Some(format!("nullable_{i}"))
                }
            })
            .collect();
        let col2: Vec<Option<String>> = (0..n).map(|i| Some(format!("nonnull_{i}"))).collect();

        let batch = ZSetBatch {
            pks: PkColumn::U64s(pks.iter().map(|&x| x as u64).collect()),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Strings(col1.clone()),
                ColData::Strings(col2.clone()),
            ],
        };

        let (mut a, mut b) = make_transport_pair();
        send_message(&mut a, 0, 0, FLAG_PUSH, &PkTuple::EMPTY, 0, Some(&schema), Some(&batch)).unwrap();
        let msg = recv_message(&mut b, None, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();

        let data = msg.data_batch.unwrap();
        assert_eq!(data.nulls, nulls);

        match &data.columns[1] {
            ColData::Strings(got) => assert_eq!(got, &col1),
            _ => panic!("expected Strings at col 1"),
        }
        match &data.columns[2] {
            ColData::Strings(got) => assert_eq!(got, &col2),
            _ => panic!("expected Strings at col 2"),
        }
    }

    #[test]
    fn test_message_no_schema_no_data() {
        // Control-only message (scan/alloc style)
        let (mut a, mut b) = make_transport_pair();
        send_message(&mut a, 0, 0, FLAG_PUSH, &PkTuple::EMPTY, 0, None, None).unwrap();
        let msg = recv_message(&mut b, None, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();
        assert!(msg.schema.is_none());
        assert!(msg.data_batch.is_none());
    }

    #[test]
    fn test_message_recv_control_fields() {
        // Verify that send_message/recv_message correctly propagates all control
        // fields into the flat Message struct: target_id, client_id, seek_pk,
        // seek_col_idx.
        let seek_pk = 0xAAAA_BBBB_CCCC_DDDD_u128 | (0x1111_2222_3333_4444_u128 << 64);
        let (mut a, mut b) = make_transport_pair();
        send_message(
            &mut a,
            0xDEAD_BEEF_1234_5678, // target_id
            0xCAFE_BABE_0000_0001, // client_id
            FLAG_PUSH,             // flags
            &PkTuple::from_u128_narrow(seek_pk),
            7, // seek_col_idx
            None,
            None,
        )
        .unwrap();
        let msg = recv_message(&mut b, None, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();
        assert_eq!(msg.target_id, 0xDEAD_BEEF_1234_5678);
        assert_eq!(msg.seek_pk, seek_pk);
        assert_eq!(msg.seek_col_idx, 7);
    }

    #[test]
    fn test_message_error_response() {
        // STATUS_ERROR response: schema and data should be None; error_text populated
        let (mut a, mut b) = make_transport_pair();
        let err_hdr = Header {
            status: STATUS_ERROR,
            ..Header::default()
        };
        let encoded = encode_control_block(&err_hdr, "something broke", &[]);
        a.send_framed(&encoded).unwrap();
        let msg = recv_message(&mut b, None, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();
        assert_eq!(msg.status, STATUS_ERROR);
        assert!(msg.error_text.is_some());
    }

    // ── encode_message + parse_response roundtrips (no sockets) ─────────

    #[test]
    fn test_encode_parse_control_only() {
        let seek_pk = 42u128 | (99u128 << 64);
        let payload = encode_message(
            0xDEAD,
            0xBEEF,
            FLAG_PUSH,
            &PkTuple::from_u128_narrow(seek_pk),
            7,
            None,
            None,
        );
        let msg = parse_response(&payload, None).unwrap();
        assert_eq!(msg.target_id, 0xDEAD);
        assert_eq!(msg.seek_pk, seek_pk);
        assert_eq!(msg.seek_col_idx, 7);
        assert!(msg.schema.is_none());
        assert!(msg.data_batch.is_none());
    }

    #[test]
    fn test_encode_parse_with_data() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };

        let mut val_bytes = Vec::new();
        for &v in &[100i64, 200, 300] {
            val_bytes.extend_from_slice(&v.to_le_bytes());
        }
        let batch = ZSetBatch {
            pks: PkColumn::U64s(vec![1u64, 2u64, 3u64]),
            weights: vec![1, 1, 1],
            nulls: vec![0, 0, 0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(val_bytes)],
        };

        let payload = encode_message(42, 1, 0, &PkTuple::EMPTY, 0, Some(&schema), Some(&batch));
        let msg = parse_response(&payload, None).unwrap();
        assert_eq!(msg.target_id, 42);
        assert!(msg.schema.is_some());
        let data = msg.data_batch.unwrap();
        assert_eq!(data.pks.to_vec_u128(), vec![1u128, 2u128, 3u128]);
        assert_eq!(data.weights, vec![1, 1, 1]);
    }

    #[test]
    fn test_encode_parse_empty_batch() {
        let schema = Schema {
            columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let empty = ZSetBatch::new(&schema);

        let payload = encode_message(10, 1, 0, &PkTuple::EMPTY, 0, Some(&schema), Some(&empty));
        let msg = parse_response(&payload, None).unwrap();
        // Schema sent, but no data (empty batch)
        assert!(msg.schema.is_some());
        assert!(msg.data_batch.is_none());
    }

    /// A wide (stride 24) PK seek must split inside `encode_message`: low 16
    /// bytes land in `seek_pk`, bytes 16..24 in `seek_pk_extra`. `parse_response`
    /// only surfaces the low-16 `seek_pk`, so decode the control block directly
    /// to inspect the extra region.
    #[test]
    fn encode_message_wide_pk_seek_emits_extra() {
        let pk = PkTuple::from_bytes(&(0..24u8).collect::<Vec<_>>());
        let payload = encode_message(7, 1, FLAG_SEEK, &pk, 0, None, None);

        let ctrl_size = u32::from_le_bytes(
            payload[gnitz_wire::WAL_OFF_SIZE..gnitz_wire::WAL_OFF_SIZE + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let (hdr, _err, extra) = decode_control_block(&payload[..ctrl_size]).unwrap();

        let (want_lo, want_extra) = pk.split_wire();
        assert_eq!(hdr.seek_pk, want_lo);
        assert_eq!(extra, want_extra); // bytes 16..24
        assert_eq!(hdr.flags & FLAG_SEEK, FLAG_SEEK);
    }

    /// A hint-only frame (FLAG_HAS_DATA set, FLAG_HAS_SCHEMA clear, matching schema
    /// version) must decode data_batch correctly but leave schema == None.
    /// Before Fix 1, parse_response cloned the hint into wire_schema, so msg.schema
    /// would be Some — this test catches that regression.
    #[test]
    fn hint_only_frame_returns_data_schema_none() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let mut val_bytes = Vec::new();
        val_bytes.extend_from_slice(&42i64.to_le_bytes());
        let batch = ZSetBatch {
            pks: PkColumn::U64s(vec![1u64]),
            weights: vec![1],
            nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(val_bytes)],
        };

        let (mut a, mut b) = make_transport_pair();
        // Embed version=1 in flags; no schema block in the frame.
        let flags = wire_flags_set_schema_version(0, 1);
        send_message_noschema(&mut a, 42, 0, flags, &schema, &batch).unwrap();

        // Parse with a matching hint (same schema, version 1).
        let msg = recv_message(&mut b, Some((&schema, 1)), gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();

        // Fix 1: schema must be None — the hint was not physically in the frame.
        assert!(msg.schema.is_none(), "schema must be None for hint-only frame");
        // Data must still decode correctly.
        let data = msg.data_batch.expect("data_batch must be Some");
        assert_eq!(data.pks.to_vec_u128(), vec![1u128]);
        assert_eq!(data.weights, vec![1i64]);
    }
}
