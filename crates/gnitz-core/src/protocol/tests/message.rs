use super::*;
use crate::protocol::types::{BatchAppender, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch};
use crate::protocol::wal_block::decode_wal_block;
use crate::protocol::wire_flags_set_schema_version;
use crate::protocol::WireConflictMode;
use crate::protocol::{ClientTransport, Header, FLAG_PUSH, FLAG_SEEK, STATUS_ERROR};
use crate::test_support::{make_transport_pair, payload_of};

// ── FLAG_PUSH_TXN family assembly ──────────────────────────────────────

/// A multi-family `FLAG_PUSH_TXN` frame (both modes, a repeated tid, a
/// delete family): each family's schema block and data block must be built
/// from *that* family's schema, batch and tid.
#[test]
fn push_txn_families_carry_their_own_schema_and_batch() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("val", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let mut b0 = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b0, &schema);
        a.add_row(1, 1).i64_val(10);
        a.add_row(2, 1).i64_val(20);
    }
    let mut b1 = ZSetBatch::new(&schema);
    BatchAppender::new(&mut b1, &schema).add_row(3, 1).i64_val(30);
    let b2 = ZSetBatch {
        pks: PkColumn::from_natives(&schema, [4]),
        weights: vec![-1],
        nulls: vec![0],
        payload: ZSetBatch::filler_columns(&schema, 1),
        blob: vec![],
    };

    let families: Vec<(u64, &Schema, &ZSetBatch, WireConflictMode)> = vec![
        (16, &schema, &b0, WireConflictMode::Update),
        (17, &schema, &b1, WireConflictMode::Error),
        (16, &schema, &b2, WireConflictMode::Update),
    ];
    let payload = encode_push_txn(0xABCD, &families, &[(16, 42), (17, 42)]);

    // The frame layout — count, mode byte, precondition section — is
    // `gnitz_wire::txn_frame`'s and is tested there; what this pins is the
    // adapter above it: each family's two blocks are built from *that*
    // family's schema, batch and tid, in that order.
    let decoded = gnitz_wire::txn_frame::decode_push_txn(&payload).unwrap().0;
    let expected = [
        (16u32, WireConflictMode::Update, 2usize),
        (17, WireConflictMode::Error, 1),
        (16, WireConflictMode::Update, 1),
    ];
    assert_eq!(decoded.len(), expected.len());
    for (fam, (exp_tid, exp_mode, exp_rows)) in decoded.iter().zip(expected) {
        assert_eq!(fam.tid, exp_tid);
        assert_eq!(WireConflictMode::from_wire(fam.mode), Some(exp_mode));
        // The schema block is this family's, keyed under this family's tid.
        assert_eq!(
            gnitz_wire::read_u32_le(fam.schema_block, gnitz_wire::WAL_OFF_TID),
            exp_tid
        );
        let block_schema = schema_from_block(fam.schema_block).unwrap();
        assert_eq!(block_schema, schema);
        // ... and the data block decodes against it, with this family's rows.
        let (batch, _) = decode_wal_block(fam.wal_block, &block_schema).unwrap();
        assert_eq!(batch.len(), exp_rows);
    }
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
/// test in `runtime/suites/wire.rs`.
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
        let h = Header { request_id: req_id, ..Header::default() };
        let encoded = encode_control_block(&h, "", &[]);
        let (decoded, _, _) = decode_control_block(&encoded).unwrap();
        assert_eq!(decoded.request_id, req_id);
    }
}

// ── send/recv message roundtrips ────────────────────────────────────────

/// Ship one cold PUSH frame, the shape `Session::roundtrip_push` builds.
fn send_push(t: &mut ClientTransport, schema: &Schema, batch: &ZSetBatch) {
    let parts = encode_message_parts(0, 0, FLAG_PUSH, 0, &[], 0, Some((schema, batch)));
    t.send_parts(parts, None).unwrap();
}

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
    send_push(&mut a, &schema, &empty_batch);
    let (msg, data) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();

    // Schema was sent, data was not (empty batch)
    assert!(msg.schema.is_some());
    assert!(data.is_none());
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
        pks: PkColumn::from_natives(&schema, pks.iter().copied()),
        weights: weights.clone(),
        nulls: nulls.clone(),
        payload: payload_of(&schema, vec![i64_bytes.clone(), f64_bytes.clone()]),
        blob: vec![],
    };

    let (mut a, mut b) = make_transport_pair();
    send_push(&mut a, &schema, &batch);
    let (_, data) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();

    let data = data.unwrap();
    assert_eq!(data.pks.to_vec_u128(&schema), pks);
    assert_eq!(data.weights, weights);

    {
        let got = &data.payload[0].bytes;
        assert_eq!(got, &i64_bytes);
    }
    {
        let got = &data.payload[1].bytes;
        assert_eq!(got, &f64_bytes);
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

    let mut blob = Vec::new();
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, pks.iter().copied()),
        weights: weights.clone(),
        nulls: nulls.clone(),
        payload: payload_of(
            &schema,
            vec![german_col(&col1, &mut blob), german_col(&col2, &mut blob)],
        ),
        blob,
    };

    let (mut a, mut b) = make_transport_pair();
    send_push(&mut a, &schema, &batch);
    let (_, data) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();

    let data = data.unwrap();
    assert_eq!(data.nulls, nulls);
    assert_eq!(german_vals(&data, 0), col1);
    assert_eq!(german_vals(&data, 1), col2);
}

/// A STRING column region from its values, spilling into `blob`; `None` is a
/// NULL cell, which the region zero-fills.
fn german_col(vals: &[Option<String>], blob: &mut Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 16);
    for v in vals {
        let bytes = v.as_deref().unwrap_or("").as_bytes();
        out.extend_from_slice(&gnitz_wire::encode_german_string(bytes, blob));
    }
    out
}

/// Read a STRING column region back out, `None` where null bit `pi` is set.
fn german_vals(batch: &ZSetBatch, pi: usize) -> Vec<Option<String>> {
    (0..batch.len())
        .map(|row| {
            (!gnitz_wire::null_word_get(batch.nulls[row], pi)).then(|| {
                let cell = &batch.payload[pi].bytes[row * 16..(row + 1) * 16];
                String::from_utf8(gnitz_wire::german_string_content(cell, &batch.blob).to_vec()).unwrap()
            })
        })
        .collect()
}

#[test]
fn test_message_no_schema_no_data() {
    // Control-only message (scan/alloc style)
    let (mut a, mut b) = make_transport_pair();
    a.send_parts(encode_control_frame(0, 0, FLAG_PUSH, 0, 0, &[]), None)
        .unwrap();
    let (msg, data) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();
    assert!(msg.schema.is_none());
    assert!(data.is_none());
}

/// The control scalars a reply is read for must survive the socket.
#[test]
fn test_message_recv_control_fields() {
    let seek_pk = 0xAAAA_BBBB_CCCC_DDDD_u128 | (0x1111_2222_3333_4444_u128 << 64);
    let (mut a, mut b) = make_transport_pair();
    a.send_parts(
        encode_control_frame(0xDEAD_BEEF_1234_5678, 0xCAFE_BABE_0000_0001, FLAG_PUSH, seek_pk, 7, &[]),
        None,
    )
    .unwrap();
    let (msg, _) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();
    assert_eq!(msg.target_id, 0xDEAD_BEEF_1234_5678);
    assert_eq!(msg.seek_pk, seek_pk);
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
    a.send_framed(&encoded, None).unwrap();
    let (msg, _) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();
    assert_eq!(msg.status, STATUS_ERROR);
    assert!(msg.error_text.is_some());
}

// ── encode_message_parts + parse_response roundtrips (no sockets) ────

#[test]
fn test_encode_parse_control_only() {
    let seek_pk = 42u128 | (99u128 << 64);
    let payload = encode_message_parts(0xDEAD, 0xBEEF, FLAG_PUSH, seek_pk, &[], 7, None).to_vec();
    let (msg, data) = parse_response(&payload, None).unwrap();
    assert_eq!(msg.target_id, 0xDEAD);
    assert_eq!(msg.seek_pk, seek_pk);
    assert!(msg.schema.is_none());
    assert!(data.is_none());
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
        pks: PkColumn::from_natives(&schema, [1, 2, 3]),
        weights: vec![1, 1, 1],
        nulls: vec![0, 0, 0],
        payload: payload_of(&schema, vec![val_bytes]),
        blob: vec![],
    };

    let payload = encode_message_parts(42, 1, 0, 0, &[], 0, Some((&schema, &batch))).to_vec();
    let (msg, data) = parse_response(&payload, None).unwrap();
    assert_eq!(msg.target_id, 42);
    assert!(msg.schema.is_some());
    let data = data.unwrap();
    assert_eq!(data.pks.to_vec_u128(&schema), vec![1u128, 2u128, 3u128]);
    assert_eq!(data.weights, vec![1, 1, 1]);
}

#[test]
fn test_encode_parse_empty_batch() {
    let schema = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let empty = ZSetBatch::new(&schema);

    let payload = encode_message_parts(10, 1, 0, 0, &[], 0, Some((&schema, &empty))).to_vec();
    let (msg, data) = parse_response(&payload, None).unwrap();
    // Schema sent, but no data (empty batch)
    assert!(msg.schema.is_some());
    assert!(data.is_none());
}

/// A wide (stride 24) PK seek must split inside `encode_message_parts`: low
/// 16 bytes land in `seek_pk`, bytes 16..24 in `seek_pk_extra`.
/// `parse_response` only surfaces the low-16 `seek_pk`, so decode the
/// control block directly to inspect the extra region.
#[test]
fn encode_message_wide_pk_seek_emits_extra() {
    let pk: Vec<u8> = (0..24u8).collect();
    let (lo, tail) = gnitz_wire::control::split_ctrl_key(&pk);
    let payload = encode_message_parts(7, 1, FLAG_SEEK, lo, tail, 0, None).to_vec();

    let ctrl = gnitz_wire::wal::block_slice_at(&payload, 0).unwrap();
    let (hdr, _err, extra) = decode_control_block(ctrl).unwrap();

    let (want_lo, want_extra) = (lo, tail);
    assert_eq!(hdr.seek_pk, want_lo);
    assert_eq!(extra, want_extra); // bytes 16..24
    assert_eq!(hdr.flags & FLAG_SEEK, FLAG_SEEK);
}

/// A hint-only frame (FLAG_HAS_DATA set, FLAG_HAS_SCHEMA clear, matching schema
/// version) decodes its data under the hint but reports `schema == None`:
/// `Message::schema` means "the block was physically in the frame", which is
/// what the cache absorb keys on.
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
        pks: PkColumn::from_natives(&schema, [1]),
        weights: vec![1],
        nulls: vec![0],
        payload: payload_of(&schema, vec![val_bytes]),
        blob: vec![],
    };

    let (mut a, mut b) = make_transport_pair();
    // Embed version=1 in flags; no schema block in the frame.
    let flags = wire_flags_set_schema_version(0, 1);
    let parts = encode_message_noschema_parts(42, 0, flags, &schema, &batch);
    a.send_parts(parts, None).unwrap();

    // Parse with a matching hint (same schema, version 1).
    let (msg, data) = parse_response(&b.recv_framed(None).unwrap(), Some((&schema, 1))).unwrap();

    // The hint was not physically in the frame.
    assert!(msg.schema.is_none(), "schema must be None for hint-only frame");
    // Data must still decode correctly.
    let data = data.expect("the frame's data block must decode against the hint");
    assert_eq!(data.pks.to_vec_u128(&schema), vec![1u128]);
    assert_eq!(data.weights, vec![1i64]);
}
