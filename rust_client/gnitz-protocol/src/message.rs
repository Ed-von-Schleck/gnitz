use std::os::unix::io::RawFd;
use std::sync::OnceLock;

use crate::error::ProtocolError;
use crate::header::{Header, STATUS_ERROR, STATUS_OK, FLAG_HAS_SCHEMA, FLAG_HAS_DATA};
use crate::types::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch, meta_schema};
use crate::codec::{schema_to_batch, batch_to_schema};
use crate::wal_block::{encode_wal_block, decode_wal_block, IPC_CONTROL_TID, WAL_BLOCK_HEADER_SIZE};
use crate::transport::{send_framed_iov, recv_framed};

pub struct Message {
    pub status:       u32,
    pub target_id:    u64,
    pub client_id:    u64,
    pub flags:        u64,
    pub seek_pk_lo:   u64,
    pub seek_pk_hi:   u64,
    pub seek_col_idx: u64,
    pub schema:       Option<Schema>,     // derived from schema_batch (avoids re-deriving)
    pub schema_batch: Option<ZSetBatch>,
    pub data_batch:   Option<ZSetBatch>,
    pub error_text:   Option<String>,     // Some(_) when status == STATUS_ERROR
}

// ── CONTROL_SCHEMA ────────────────────────────────────────────────────────────
//
// 9 columns (pk_index = 0):
//   col 0: msg_idx     (U64, PK)
//   col 1: status      (U64)
//   col 2: client_id   (U64)
//   col 3: target_id   (U64)
//   col 4: flags       (U64)
//   col 5: seek_pk_lo  (U64)
//   col 6: seek_pk_hi  (U64)
//   col 7: seek_col_idx(U64)
//   col 8: error_msg   (String, nullable)
//
// payload_idx for col i (pk_index=0): i - 1.
// So col 8 has payload_idx = 7; null bit 7 set means no error.

const CTRL_COL_STATUS:     usize = 1;
const CTRL_COL_CLIENT_ID:  usize = 2;
const CTRL_COL_TARGET_ID:  usize = 3;
const CTRL_COL_FLAGS:      usize = 4;
const CTRL_COL_SEEK_PK_LO: usize = 5;
const CTRL_COL_SEEK_PK_HI: usize = 6;
const CTRL_COL_SEEK_COL:   usize = 7;
const CTRL_COL_ERROR_MSG:  usize = 8;

fn control_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "msg_idx".into(),     type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "status".into(),      type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "client_id".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "target_id".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "flags".into(),       type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "seek_pk_lo".into(),  type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "seek_pk_hi".into(),  type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "seek_col_idx".into(),type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "error_msg".into(),   type_code: TypeCode::String, is_nullable: true  },
        ],
        pk_index: 0,
    })
}

/// Encode a `Header` + optional error message into a control WAL block.
/// When `error_msg` is empty the error_msg column is NULL (null bit 7 set).
pub fn encode_control_block(header: &Header, error_msg: &str) -> Result<Vec<u8>, ProtocolError> {
    let cs = control_schema();

    // payload_idx for col 8 = 8 - 1 = 7
    let has_error = !error_msg.is_empty();
    let nulls_val: u64 = if has_error { 0 } else { 1u64 << 7 };

    let u64_vals: [u64; 7] = [
        header.status as u64,
        header.client_id,
        header.target_id,
        header.flags,
        header.seek_pk_lo,
        header.seek_pk_hi,
        header.seek_col_idx,
    ];

    let mut ctrl_bytes = [0u8; 56];
    for (i, &v) in u64_vals.iter().enumerate() {
        ctrl_bytes[i * 8..(i + 1) * 8].copy_from_slice(&v.to_le_bytes());
    }

    let mut columns: Vec<ColData> = Vec::with_capacity(9);
    columns.push(ColData::Fixed(vec![]));   // col 0: pk placeholder
    for i in 0..7 {
        columns.push(ColData::Fixed(ctrl_bytes[i * 8..(i + 1) * 8].to_vec()));
    }
    columns.push(ColData::Strings(vec![
        if has_error { Some(error_msg.to_string()) } else { None }
    ]));

    let batch = ZSetBatch {
        pk_lo:   vec![0u64],
        pk_hi:   vec![0u64],
        weights: vec![1i64],
        nulls:   vec![nulls_val],
        columns,
    };

    Ok(encode_wal_block(cs, IPC_CONTROL_TID, &batch))
}

/// Decode a control WAL block, returning `(Header, error_msg)`.
/// `error_msg` is empty when the null bit for error_msg is set.
pub fn decode_control_block(data: &[u8]) -> Result<(Header, String), ProtocolError> {
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

    let status     = read_u64_col(&batch, CTRL_COL_STATUS)?    as u32;
    let client_id  = read_u64_col(&batch, CTRL_COL_CLIENT_ID)?;
    let target_id  = read_u64_col(&batch, CTRL_COL_TARGET_ID)?;
    let flags      = read_u64_col(&batch, CTRL_COL_FLAGS)?;
    let seek_pk_lo = read_u64_col(&batch, CTRL_COL_SEEK_PK_LO)?;
    let seek_pk_hi = read_u64_col(&batch, CTRL_COL_SEEK_PK_HI)?;
    let seek_col_idx = read_u64_col(&batch, CTRL_COL_SEEK_COL)?;

    // payload_idx for col 8 = 7; null bit set → no error string
    let is_null = (batch.nulls[0] & (1u64 << 7)) != 0;
    let error_msg = if is_null {
        String::new()
    } else {
        match &batch.columns[CTRL_COL_ERROR_MSG] {
            ColData::Strings(v) => match &v[0] {
                Some(s) => s.clone(),
                None    => String::new(),
            },
            _ => return Err(ProtocolError::DecodeError(
                "control block col 8 is not Strings".into()
            )),
        }
    };

    let header = Header { status, target_id, client_id, flags, seek_pk_lo, seek_pk_hi, seek_col_idx };
    Ok((header, error_msg))
}

pub fn send_message(
    sock_fd:      RawFd,
    target_id:    u64,
    client_id:    u64,
    flags:        u64,
    seek_pk_lo:   u64,
    seek_pk_hi:   u64,
    seek_col_idx: u64,
    schema:       Option<&Schema>,
    data_batch:   Option<&ZSetBatch>,
) -> Result<(), ProtocolError> {
    let has_data   = data_batch.map(|b| !b.is_empty()).unwrap_or(false);
    let has_schema = has_data || schema.is_some();

    let mut flags_out = flags;
    if has_schema { flags_out |= FLAG_HAS_SCHEMA; }
    if has_data   { flags_out |= FLAG_HAS_DATA;   }

    let ctrl_hdr = Header {
        status: STATUS_OK, target_id, client_id, flags: flags_out,
        seek_pk_lo, seek_pk_hi, seek_col_idx,
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

    let mut bufs: Vec<&[u8]> = Vec::with_capacity(3);
    bufs.push(&ctrl_block);
    if let Some(ref sb) = schema_block { bufs.push(sb); }
    if let Some(ref db) = data_block { bufs.push(db); }

    send_framed_iov(sock_fd, &bufs)
}

pub fn recv_message(
    sock_fd:     RawFd,
    data_schema: Option<&Schema>,
) -> Result<Message, ProtocolError> {
    let buf = recv_framed(sock_fd)?;

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
        let eff = wire_schema.as_ref().or(data_schema)
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
        seek_pk_lo:   ctrl_header.seek_pk_lo,
        seek_pk_hi:   ctrl_header.seek_pk_hi,
        seek_col_idx: ctrl_header.seek_col_idx,
        schema,
        schema_batch,
        data_batch,
        error_text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::RawFd;
    use crate::types::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
    use crate::header::{Header, FLAG_PUSH, STATUS_ERROR};

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
            seek_pk_lo: 42,
            seek_pk_hi: 99,
            seek_col_idx: 7,
        };

        let encoded = encode_control_block(&h, "test error").unwrap();
        let (decoded, err) = decode_control_block(&encoded).unwrap();

        assert_eq!(decoded.status,     h.status);
        assert_eq!(decoded.target_id,  h.target_id);
        assert_eq!(decoded.client_id,  h.client_id);
        assert_eq!(decoded.flags,      h.flags);
        assert_eq!(decoded.seek_pk_lo, h.seek_pk_lo);
        assert_eq!(decoded.seek_pk_hi, h.seek_pk_hi);
        assert_eq!(decoded.seek_col_idx, h.seek_col_idx);
        assert_eq!(err, "test error");
    }

    #[test]
    fn test_message_control_null_error_msg() {
        let h = Header::default();
        let encoded = encode_control_block(&h, "").unwrap();
        let (_, err) = decode_control_block(&encoded).unwrap();
        assert_eq!(err, "");
    }

    // ── send/recv message roundtrips ────────────────────────────────────────

    #[test]
    fn test_message_roundtrip_empty() {
        // Empty batch → FLAG_HAS_SCHEMA but not FLAG_HAS_DATA
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false },
            ],
            pk_index: 0,
        };

        let empty_batch = ZSetBatch::new(&schema);
        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0, 0, 0, Some(&schema), Some(&empty_batch)).unwrap();
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
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "i64".into(), type_code: TypeCode::I64, is_nullable: false },
                ColumnDef { name: "f64".into(), type_code: TypeCode::F64, is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 100usize;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];

        let i64_vals: Vec<i64> = (0..n as i64).map(|x| x * -7).collect();
        let f64_vals: Vec<f64> = (0..n).map(|x| x as f64 * 1.5).collect();

        let mut i64_bytes = Vec::with_capacity(n * 8);
        for &v in &i64_vals { i64_bytes.extend_from_slice(&v.to_le_bytes()); }
        let mut f64_bytes = Vec::with_capacity(n * 8);
        for &v in &f64_vals { f64_bytes.extend_from_slice(&v.to_le_bytes()); }

        let batch = ZSetBatch {
            pk_lo: pk_lo.clone(),
            pk_hi: pk_hi.clone(),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(i64_bytes.clone()),
                ColData::Fixed(f64_bytes.clone()),
            ],
        };

        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0, 0, 0, Some(&schema), Some(&batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

        let data = msg.data_batch.unwrap();
        assert_eq!(data.pk_lo,   pk_lo);
        assert_eq!(data.pk_hi,   pk_hi);
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
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "s1".into(), type_code: TypeCode::String, is_nullable: true  },
                ColumnDef { name: "s2".into(), type_code: TypeCode::String, is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 50usize;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];
        // Every 3rd row: s1 is null → payload bit 0 set
        let nulls: Vec<u64>   = (0..n).map(|i| if i % 3 == 0 { 1u64 } else { 0u64 }).collect();

        let col1: Vec<Option<String>> = (0..n).map(|i| {
            if i % 3 == 0 { None } else { Some(format!("nullable_{}", i)) }
        }).collect();
        let col2: Vec<Option<String>> = (0..n).map(|i| Some(format!("nonnull_{}", i))).collect();

        let batch = ZSetBatch {
            pk_lo: pk_lo.clone(),
            pk_hi: pk_hi.clone(),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Strings(col1.clone()),
                ColData::Strings(col2.clone()),
            ],
        };

        let (a, b) = make_socketpair();
        send_message(a, 0, 0, FLAG_PUSH, 0, 0, 0, Some(&schema), Some(&batch)).unwrap();
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
        send_message(a, 0, 0, FLAG_PUSH, 0, 0, 0, None, None).unwrap();
        let msg = recv_message(b, None).unwrap();
        assert!(msg.schema_batch.is_none());
        assert!(msg.data_batch.is_none());
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_recv_control_fields() {
        // Verify that send_message/recv_message correctly propagates all control
        // fields into the flat Message struct: target_id, client_id, seek_pk_lo,
        // seek_pk_hi, seek_col_idx.  Regression for Step 5b rename (p4→seek_col_idx).
        let (a, b) = make_socketpair();
        send_message(
            a,
            0xDEAD_BEEF_1234_5678,  // target_id
            0xCAFE_BABE_0000_0001,  // client_id
            FLAG_PUSH,              // flags
            0xAAAA_BBBB_CCCC_DDDD,  // seek_pk_lo
            0x1111_2222_3333_4444,  // seek_pk_hi
            7,                      // seek_col_idx
            None,
            None,
        ).unwrap();
        let msg = recv_message(b, None).unwrap();
        assert_eq!(msg.target_id,    0xDEAD_BEEF_1234_5678);
        assert_eq!(msg.client_id,    0xCAFE_BABE_0000_0001);
        assert_eq!(msg.seek_pk_lo,   0xAAAA_BBBB_CCCC_DDDD);
        assert_eq!(msg.seek_pk_hi,   0x1111_2222_3333_4444);
        assert_eq!(msg.seek_col_idx, 7);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_error_response() {
        // STATUS_ERROR response: schema and data should be None; error_text populated
        let (a, b) = make_socketpair();
        let err_hdr = Header { status: STATUS_ERROR, ..Header::default() };
        let encoded = encode_control_block(&err_hdr, "something broke").unwrap();
        crate::transport::send_framed(a, &encoded).unwrap();
        let msg = recv_message(b, None).unwrap();
        assert_eq!(msg.status, STATUS_ERROR);
        assert!(msg.error_text.is_some());
        unsafe { libc::close(a); libc::close(b); }
    }
}
