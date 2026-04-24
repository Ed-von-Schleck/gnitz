use std::os::unix::io::RawFd;
use std::sync::OnceLock;

use super::error::ProtocolError;
use super::header::{Header, STATUS_ERROR, STATUS_OK, FLAG_HAS_SCHEMA, FLAG_HAS_DATA};
use super::types::{ColData, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch, meta_schema};
use super::codec::{schema_to_batch, batch_to_schema};
use super::header::{IPC_CONTROL_TID, WAL_BLOCK_HEADER_SIZE};
use super::wal_block::{encode_wal_block, decode_wal_block};
use super::transport::{send_framed, recv_framed};

pub struct Message {
    pub status:       u32,
    pub target_id:    u64,
    pub client_id:    u64,
    pub flags:        u64,
    pub seek_pk:      u128,
    pub seek_col_idx: u64,
    pub request_id:   u64,
    pub schema:       Option<Schema>,     // derived from schema_batch (avoids re-deriving)
    pub schema_batch: Option<ZSetBatch>,
    pub data_batch:   Option<ZSetBatch>,
    pub error_text:   Option<String>,     // Some(_) when status == STATUS_ERROR
}

// ── CONTROL_SCHEMA ────────────────────────────────────────────────────────────
//
// Schema layout (column indices, payload indices, region indices, null-bit
// positions) lives in `gnitz_wire::control`. This module builds the schema
// using those indices so the client and the server can never disagree on
// the wire format.

fn control_schema() -> &'static Schema {
    use gnitz_wire::control as ctrl;
    const _: () = assert!(ctrl::NUM_COLUMNS == 9,
        "control_schema column list out of sync with gnitz_wire::control::NUM_COLUMNS");
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "msg_idx".into(),      type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "status".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "client_id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "target_id".into(),    type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "flags".into(),        type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "seek_pk".into(),      type_code: TypeCode::U128,   is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "seek_col_idx".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "request_id".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "error_msg".into(),    type_code: TypeCode::String, is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

/// Encode a `Header` + optional error message into a control WAL block.
/// When `error_msg` is empty the error_msg column is NULL.
pub fn encode_control_block(header: &Header, error_msg: &str) -> Result<Vec<u8>, ProtocolError> {
    use gnitz_wire::control as ctrl;
    let cs = control_schema();

    let has_error = !error_msg.is_empty();
    let nulls_val: u64 = if has_error { 0 } else { ctrl::NULL_BIT_ERROR_MSG };

    let mut columns: Vec<ColData> = Vec::with_capacity(ctrl::NUM_COLUMNS);
    columns.push(ColData::Fixed(vec![]));   // col 0: pk placeholder
    columns.push(ColData::Fixed((header.status as u64).to_le_bytes().to_vec()));
    columns.push(ColData::Fixed(header.client_id.to_le_bytes().to_vec()));
    columns.push(ColData::Fixed(header.target_id.to_le_bytes().to_vec()));
    columns.push(ColData::Fixed(header.flags.to_le_bytes().to_vec()));
    columns.push(ColData::U128s(vec![header.seek_pk]));
    columns.push(ColData::Fixed(header.seek_col_idx.to_le_bytes().to_vec()));
    columns.push(ColData::Fixed(header.request_id.to_le_bytes().to_vec()));
    columns.push(ColData::Strings(vec![
        if has_error { Some(error_msg.to_string()) } else { None }
    ]));

    let batch = ZSetBatch {
        pks:     PkColumn::U64s(vec![0u64]),
        weights: vec![1i64],
        nulls:   vec![nulls_val],
        columns,
    };

    Ok(encode_wal_block(cs, IPC_CONTROL_TID, &batch))
}

/// Decode a control WAL block, returning `(Header, error_msg)`.
/// `error_msg` is empty when the null bit for error_msg is set.
pub fn decode_control_block(data: &[u8]) -> Result<(Header, String), ProtocolError> {
    use gnitz_wire::control as ctrl;
    let cs = control_schema();
    let (batch, tid, _lsn) = decode_wal_block(data, cs)?;

    if tid != IPC_CONTROL_TID {
        return Err(ProtocolError::DecodeError(format!(
            "expected control TID 0x{:08X}, got 0x{:08X}", IPC_CONTROL_TID, tid
        )));
    }
    if batch.len() != 1 {
        return Err(ProtocolError::DecodeError(format!(
            "control block must have 1 row, got {}", batch.len()
        )));
    }

    fn read_u64_col(batch: &ZSetBatch, ci: usize) -> Result<u64, ProtocolError> {
        match &batch.columns[ci] {
            ColData::Fixed(v) if v.len() == 8 => {
                Ok(u64::from_le_bytes(v[0..8].try_into().unwrap()))
            }
            ColData::Fixed(v) => Err(ProtocolError::DecodeError(format!(
                "control block col {} Fixed len {} != 8", ci, v.len()
            ))),
            _ => Err(ProtocolError::DecodeError(format!(
                "control block col {} is not Fixed", ci
            ))),
        }
    }

    fn read_u128_col(batch: &ZSetBatch, ci: usize) -> Result<u128, ProtocolError> {
        match &batch.columns[ci] {
            ColData::U128s(v) if v.len() == 1 => Ok(v[0]),
            ColData::U128s(v) => Err(ProtocolError::DecodeError(format!(
                "control block col {} U128s len {} != 1", ci, v.len()
            ))),
            _ => Err(ProtocolError::DecodeError(format!(
                "control block col {} is not U128s", ci
            ))),
        }
    }

    let status       = read_u64_col(&batch, ctrl::COL_STATUS)? as u32;
    let client_id    = read_u64_col(&batch, ctrl::COL_CLIENT_ID)?;
    let target_id    = read_u64_col(&batch, ctrl::COL_TARGET_ID)?;
    let flags        = read_u64_col(&batch, ctrl::COL_FLAGS)?;
    let seek_pk      = read_u128_col(&batch, ctrl::COL_SEEK_PK)?;
    let seek_col_idx = read_u64_col(&batch, ctrl::COL_SEEK_COL_IDX)?;
    let request_id   = read_u64_col(&batch, ctrl::COL_REQUEST_ID)?;

    let is_null = (batch.nulls[0] & ctrl::NULL_BIT_ERROR_MSG) != 0;
    let error_msg = if is_null {
        String::new()
    } else {
        match &batch.columns[ctrl::COL_ERROR_MSG] {
            ColData::Strings(v) => match &v[0] {
                Some(s) => s.clone(),
                None    => String::new(),
            },
            _ => return Err(ProtocolError::DecodeError(
                "control block error_msg column is not Strings".into()
            )),
        }
    };

    let header = Header {
        status, target_id, client_id, flags,
        seek_pk, seek_col_idx, request_id,
    };
    Ok((header, error_msg))
}

/// Encode a request/response into wire bytes (without the 4-byte frame header).
/// The returned `Vec<u8>` contains concatenated WAL blocks: control + optional
/// schema + optional data.  Pass to `send_framed()` to add framing and send.
pub fn encode_message(
    target_id:    u64,
    client_id:    u64,
    flags:        u64,
    seek_pk:      u128,
    seek_col_idx: u64,
    schema:       Option<&Schema>,
    data_batch:   Option<&ZSetBatch>,
) -> Result<Vec<u8>, ProtocolError> {
    let has_data   = data_batch.map(|b| !b.is_empty()).unwrap_or(false);
    let has_schema = has_data || schema.is_some();

    let mut flags_out = flags;
    if has_schema { flags_out |= FLAG_HAS_SCHEMA; }
    if has_data   { flags_out |= FLAG_HAS_DATA;   }

    let ctrl_hdr = Header {
        status: STATUS_OK, target_id, client_id, flags: flags_out,
        seek_pk, seek_col_idx,
        request_id: 0,
    };
    let ctrl_block = encode_control_block(&ctrl_hdr, "")?;
    let schema_block = if has_schema {
        let ms = meta_schema();
        let owned = schema_to_batch(schema.unwrap());
        Some(encode_wal_block(ms, target_id as u32, &owned))
    } else {
        None
    };
    let data_block = if has_data {
        Some(encode_wal_block(
            schema.unwrap(), target_id as u32, data_batch.unwrap()
        ))
    } else {
        None
    };

    let mut out = ctrl_block;
    if let Some(sb) = schema_block { out.extend_from_slice(&sb); }
    if let Some(db) = data_block { out.extend_from_slice(&db); }
    Ok(out)
}

pub fn send_message(
    sock_fd:      RawFd,
    target_id:    u64,
    client_id:    u64,
    flags:        u64,
    seek_pk:      u128,
    seek_col_idx: u64,
    schema:       Option<&Schema>,
    data_batch:   Option<&ZSetBatch>,
) -> Result<(), ProtocolError> {
    let payload = encode_message(
        target_id, client_id, flags,
        seek_pk, seek_col_idx,
        schema, data_batch,
    )?;
    send_framed(sock_fd, &payload)
}

/// Parse a wire payload (without 4-byte frame header) into a `Message`.
/// The payload is what `recv_framed()` returns.
pub fn parse_response(buf: &[u8]) -> Result<Message, ProtocolError> {
    if buf.len() < WAL_BLOCK_HEADER_SIZE {
        return Err(ProtocolError::DecodeError("message too small".into()));
    }

    let ctrl_size = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
    if ctrl_size > buf.len() {
        return Err(ProtocolError::DecodeError("control block truncated".into()));
    }

    let (ctrl_header, error_msg) = decode_control_block(&buf[..ctrl_size])?;

    let flags      = ctrl_header.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data   = (flags & FLAG_HAS_DATA)   != 0;

    if has_data && !has_schema {
        return Err(ProtocolError::DecodeError(
            "FLAG_HAS_DATA without FLAG_HAS_SCHEMA".into()
        ));
    }

    let mut off = ctrl_size;
    let mut wire_schema: Option<Schema>   = None;
    let mut schema_batch: Option<ZSetBatch> = None;

    if has_schema {
        if off + WAL_BLOCK_HEADER_SIZE > buf.len() {
            return Err(ProtocolError::DecodeError("schema block header truncated".into()));
        }
        let sz = u32::from_le_bytes(buf[off + 16..off + 20].try_into().unwrap()) as usize;
        if off + sz > buf.len() {
            return Err(ProtocolError::DecodeError("schema block truncated".into()));
        }
        let ms = meta_schema();
        let (sbatch, _, _) = decode_wal_block(&buf[off..off + sz], ms)?;
        wire_schema  = Some(batch_to_schema(&sbatch)?);
        schema_batch = Some(sbatch);
        off += sz;
    }

    let data_batch = if has_data {
        let eff = wire_schema.as_ref()
            .ok_or_else(|| ProtocolError::DecodeError("no schema for data block".into()))?;
        if off + WAL_BLOCK_HEADER_SIZE > buf.len() {
            return Err(ProtocolError::DecodeError("data block header truncated".into()));
        }
        let sz = u32::from_le_bytes(buf[off + 16..off + 20].try_into().unwrap()) as usize;
        if off + sz > buf.len() {
            return Err(ProtocolError::DecodeError("data block truncated".into()));
        }
        let (batch, _, _) = decode_wal_block(&buf[off..off + sz], eff)?;
        Some(batch)
    } else {
        None
    };

    let (schema, schema_batch, data_batch, error_text) = if ctrl_header.status == STATUS_ERROR {
        (None, None, None, Some(error_msg))
    } else {
        (wire_schema, schema_batch, data_batch, None)
    };

    Ok(Message {
        status:       ctrl_header.status,
        target_id:    ctrl_header.target_id,
        client_id:    ctrl_header.client_id,
        flags:        ctrl_header.flags,
        seek_pk:      ctrl_header.seek_pk,
        seek_col_idx: ctrl_header.seek_col_idx,
        request_id:   ctrl_header.request_id,
        schema,
        schema_batch,
        data_batch,
        error_text,
    })
}

pub fn recv_message(
    sock_fd:     RawFd,
    data_schema: Option<&Schema>,
) -> Result<Message, ProtocolError> {
    let buf = recv_framed(sock_fd)?;
    // data_schema fallback: only needed when server sends data without schema.
    // The server always sends FLAG_HAS_SCHEMA with FLAG_HAS_DATA, so
    // parse_response() works without it.  Keep the parameter for API compat.
    let _ = data_schema;
    parse_response(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::RawFd;
    use crate::protocol::types::{ColData, PkColumn, ColumnDef, Schema, TypeCode, ZSetBatch};
    use crate::protocol::header::{Header, FLAG_PUSH, STATUS_ERROR};

    fn make_socketpair() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()); }
        (fds[0], fds[1])
    }

    // ── control block roundtrip ─────────────────────────────────────────────

    #[test]
    fn test_message_control_schema_roundtrip() {
        let h = Header {
            status:     0,
            target_id:  0x1234_5678_9ABC_DEF0,
            client_id:  0xDEAD_BEEF_0000_0001,
            flags:      FLAG_PUSH | FLAG_HAS_SCHEMA,
            seek_pk:    42u128 | (99u128 << 64),
            seek_col_idx: 7,
            request_id: 0xCAFE_BABE_DEAD_F00D,
        };

        let encoded = encode_control_block(&h, "test error").unwrap();
        let (decoded, err) = decode_control_block(&encoded).unwrap();

        assert_eq!(decoded.status,     h.status);
        assert_eq!(decoded.target_id,  h.target_id);
        assert_eq!(decoded.client_id,  h.client_id);
        assert_eq!(decoded.flags,      h.flags);
        assert_eq!(decoded.seek_pk,    h.seek_pk);
        assert_eq!(decoded.seek_col_idx, h.seek_col_idx);
        assert_eq!(decoded.request_id, h.request_id);
        assert_eq!(err, "test error");
    }

    #[test]
    fn test_message_control_null_error_msg() {
        let h = Header::default();
        let encoded = encode_control_block(&h, "").unwrap();
        let (_, err) = decode_control_block(&encoded).unwrap();
        assert_eq!(err, "");
    }

    /// `request_id` round-trips for the reserved sentinel values: 0 (untagged),
    /// u64::MAX (broadcast), and an arbitrary mid-range value. Catches both
    /// sign-extension bugs and the new-column-index drift between client and
    /// server.
    #[test]
    fn test_request_id_roundtrip_reserved_values() {
        for &req_id in &[0u64, u64::MAX, 0x1234_5678_DEAD_BEEFu64] {
            let h = Header { request_id: req_id, ..Header::default() };
            let encoded = encode_control_block(&h, "").unwrap();
            let (decoded, _) = decode_control_block(&encoded).unwrap();
            assert_eq!(decoded.request_id, req_id);
        }
    }

    // ── send/recv message roundtrips ────────────────────────────────────────

    #[test]
    fn test_message_roundtrip_empty() {
        // Empty batch → FLAG_HAS_SCHEMA but not FLAG_HAS_DATA
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };

        let empty_batch = ZSetBatch::new(&schema);
        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0u128, 0, Some(&schema), Some(&empty_batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

        // Schema was sent, data was not (empty batch)
        assert!(msg.schema_batch.is_some());
        assert!(msg.data_batch.is_none());
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_roundtrip_data() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "i64".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "f64".into(), type_code: TypeCode::F64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };

        let n = 100usize;
        let pks: Vec<u128>    = (0..n as u128).collect();
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];

        let i64_vals: Vec<i64> = (0..n as i64).map(|x| x * -7).collect();
        let f64_vals: Vec<f64> = (0..n).map(|x| x as f64 * 1.5).collect();

        let mut i64_bytes = Vec::with_capacity(n * 8);
        for &v in &i64_vals { i64_bytes.extend_from_slice(&v.to_le_bytes()); }
        let mut f64_bytes = Vec::with_capacity(n * 8);
        for &v in &f64_vals { f64_bytes.extend_from_slice(&v.to_le_bytes()); }

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

        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0u128, 0, Some(&schema), Some(&batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

        let data = msg.data_batch.unwrap();
        assert_eq!(data.pks,     pks);
        assert_eq!(data.weights, weights);

        match &data.columns[1] {
            ColData::Fixed(got) => assert_eq!(got, &i64_bytes),
            _ => panic!("expected Fixed at col 1"),
        }
        match &data.columns[2] {
            ColData::Fixed(got) => assert_eq!(got, &f64_bytes),
            _ => panic!("expected Fixed at col 2"),
        }

        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_roundtrip_strings() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s1".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s2".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };

        let n = 50usize;
        let pks: Vec<u128>    = (0..n as u128).collect();
        let weights: Vec<i64> = vec![1; n];
        // Every 3rd row: s1 is null → payload bit 0 set
        let nulls: Vec<u64>   = (0..n).map(|i| if i % 3 == 0 { 1u64 } else { 0u64 }).collect();

        let col1: Vec<Option<String>> = (0..n).map(|i| {
            if i % 3 == 0 { None } else { Some(format!("nullable_{}", i)) }
        }).collect();
        let col2: Vec<Option<String>> = (0..n).map(|i| Some(format!("nonnull_{}", i))).collect();

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

        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0u128, 0, Some(&schema), Some(&batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

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

        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_no_schema_no_data() {
        // Control-only message (scan/alloc style)
        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0u128, 0, None, None).unwrap();
        let msg = recv_message(b, None).unwrap();
        assert!(msg.schema_batch.is_none());
        assert!(msg.data_batch.is_none());
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_recv_control_fields() {
        // Verify that send_message/recv_message correctly propagates all control
        // fields into the flat Message struct: target_id, client_id, seek_pk,
        // seek_col_idx.
        let seek_pk = 0xAAAA_BBBB_CCCC_DDDD_u128 | (0x1111_2222_3333_4444_u128 << 64);
        let (a, b) = make_socketpair();
        send_message(
            a,
            0xDEAD_BEEF_1234_5678,  // target_id
            0xCAFE_BABE_0000_0001,  // client_id
            FLAG_PUSH,              // flags
            seek_pk,
            7,                      // seek_col_idx
            None,
            None,
        ).unwrap();
        let msg = recv_message(b, None).unwrap();
        assert_eq!(msg.target_id,    0xDEAD_BEEF_1234_5678);
        assert_eq!(msg.client_id,    0xCAFE_BABE_0000_0001);
        assert_eq!(msg.seek_pk,      seek_pk);
        assert_eq!(msg.seek_col_idx, 7);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_error_response() {
        // STATUS_ERROR response: schema and data should be None; error_text populated
        let (a, b) = make_socketpair();
        let err_hdr = Header { status: STATUS_ERROR, ..Header::default() };
        let encoded = encode_control_block(&err_hdr, "something broke").unwrap();
        crate::protocol::transport::send_framed(a, &encoded).unwrap();
        let msg = recv_message(b, None).unwrap();
        assert_eq!(msg.status, STATUS_ERROR);
        assert!(msg.error_text.is_some());
        unsafe { libc::close(a); libc::close(b); }
    }

    // ── encode_message + parse_response roundtrips (no sockets) ─────────

    #[test]
    fn test_encode_parse_control_only() {
        let seek_pk = 42u128 | (99u128 << 64);
        let payload = encode_message(
            0xDEAD, 0xBEEF, FLAG_PUSH,
            seek_pk, 7,
            None, None,
        ).unwrap();
        let msg = parse_response(&payload).unwrap();
        assert_eq!(msg.target_id,    0xDEAD);
        assert_eq!(msg.client_id,    0xBEEF);
        assert_eq!(msg.seek_pk,      seek_pk);
        assert_eq!(msg.seek_col_idx, 7);
        assert!(msg.schema.is_none());
        assert!(msg.data_batch.is_none());
    }

    #[test]
    fn test_encode_parse_with_data() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };

        let mut val_bytes = Vec::new();
        for &v in &[100i64, 200, 300] {
            val_bytes.extend_from_slice(&v.to_le_bytes());
        }
        let batch = ZSetBatch {
            pks:     PkColumn::U64s(vec![1u64, 2u64, 3u64]),
            weights: vec![1, 1, 1],
            nulls:   vec![0, 0, 0],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(val_bytes),
            ],
        };

        let payload = encode_message(
            42, 1, 0, 0u128, 0,
            Some(&schema), Some(&batch),
        ).unwrap();
        let msg = parse_response(&payload).unwrap();
        assert_eq!(msg.target_id, 42);
        assert!(msg.schema.is_some());
        let data = msg.data_batch.unwrap();
        assert_eq!(data.pks, vec![1u128, 2u128, 3u128]);
        assert_eq!(data.weights, vec![1, 1, 1]);
    }

    #[test]
    fn test_encode_parse_empty_batch() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        };
        let empty = ZSetBatch::new(&schema);

        let payload = encode_message(
            10, 1, 0, 0u128, 0,
            Some(&schema), Some(&empty),
        ).unwrap();
        let msg = parse_response(&payload).unwrap();
        // Schema sent, but no data (empty batch)
        assert!(msg.schema.is_some());
        assert!(msg.data_batch.is_none());
    }
}
