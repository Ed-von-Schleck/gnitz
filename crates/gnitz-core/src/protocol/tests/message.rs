use super::*;
use crate::protocol::types::{BatchAppender, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch};
use crate::protocol::wal_block::decode_wal_block;
use crate::protocol::{ClientTransport, ClientVerb, WireConflictMode};

/// A plain push() request's flags.
fn push() -> WireFlags {
    WireFlags {
        verb: ClientVerb::Push,
        ..Default::default()
    }
}
use crate::test_support::{make_transport_pair, payload_of};

// ── PUSH_TXN family assembly ──────────────────────────────────────

/// A multi-family `PUSH_TXN` frame (both modes, a repeated tid, a
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
    let payload = encode_push_txn(&families, &[(16, 42), (17, 42)]);

    // The frame layout is `gnitz_wire::txn_frame`'s and is tested there; what
    // this pins is the adapter above it: each family's two blocks are built from
    // *that* family's schema, batch and tid, in that order.
    let ctrl = peek_control_block(&payload).unwrap();
    let decoded = gnitz_wire::txn_frame::decode_push_txn(&payload, &ctrl).unwrap().0;
    let expected = [
        (16u32, WireConflictMode::Update, 2usize),
        (17, WireConflictMode::Error, 1),
        (16, WireConflictMode::Update, 1),
    ];
    assert_eq!(decoded.len(), expected.len());
    for (fam, (exp_tid, exp_mode, exp_rows)) in decoded.iter().zip(expected) {
        assert_eq!(fam.tid, exp_tid);
        assert_eq!(fam.mode, exp_mode);
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

// ── send/recv message roundtrips ────────────────────────────────────────

fn header(target_id: u64, flags: WireFlags, arg0: u64) -> ControlHeader {
    ControlHeader {
        target_id,
        flags,
        arg0,
        ..Default::default()
    }
}

/// Ship one cold push() frame, the shape `Session::submit` builds.
fn send_push(t: &mut ClientTransport, schema: &Schema, batch: &ZSetBatch) {
    let parts = encode_frame(header(0, push(), 0), &[], Some(schema), Some(batch));
    t.send_parts(parts, None).unwrap();
}

#[test]
fn test_message_roundtrip_empty() {
    // Empty batch → has_schema but not has_data
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
    a.send_parts(encode_frame(header(0, push(), 0), &[], None, None), None)
        .unwrap();
    let (msg, data) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();
    assert!(msg.schema.is_none());
    assert!(data.is_none());
}

/// The control scalars and blob a reply is read for must survive the socket.
#[test]
fn test_message_recv_control_fields() {
    let (mut a, mut b) = make_transport_pair();
    let hdr = ControlHeader {
        arg1: 7,
        ..header(0xDEAD_BEEF_1234_5678, push(), 0xAAAA_BBBB_CCCC_DDDD)
    };
    a.send_parts(encode_frame(hdr, b"a blob", None, None), None).unwrap();
    let (msg, _) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();
    assert_eq!(msg.hdr, hdr);
    assert_eq!(msg.blob, b"a blob");
    assert!(msg.error_text.is_none());
}

/// Under a non-`Ok` status the blob is the error text, and nothing else.
#[test]
fn test_message_error_response() {
    let (mut a, mut b) = make_transport_pair();
    let err_hdr = ControlHeader {
        status: WireStatus::Error,
        ..Default::default()
    };
    a.send_parts(encode_frame(err_hdr, b"something broke", None, None), None)
        .unwrap();
    let (msg, _) = parse_response(&b.recv_framed(None).unwrap(), None).unwrap();
    assert_eq!(msg.hdr.status, WireStatus::Error);
    assert_eq!(msg.error_text.as_deref(), Some("something broke"));
    assert!(msg.blob.is_empty());
}

// ── encode_frame + parse_response roundtrips (no sockets) ────

#[test]
fn test_encode_parse_control_only() {
    let payload = encode_frame(header(0xDEAD, push(), 42), &[], None, None).to_vec();
    let (msg, data) = parse_response(&payload, None).unwrap();
    assert_eq!(msg.hdr.target_id, 0xDEAD);
    assert_eq!(msg.hdr.arg0, 42);
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

    let payload = encode_frame(header(42, WireFlags::default(), 0), &[], Some(&schema), Some(&batch)).to_vec();
    let (msg, data) = parse_response(&payload, None).unwrap();
    assert_eq!(msg.hdr.target_id, 42);
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

    let payload = encode_frame(header(10, WireFlags::default(), 0), &[], Some(&schema), Some(&empty)).to_vec();
    let (msg, data) = parse_response(&payload, None).unwrap();
    // Schema sent, but no data (empty batch)
    assert!(msg.schema.is_some());
    assert!(data.is_none());
}

/// A hint-only frame (`has_data` set, `has_schema` clear, matching schema
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
    let flags = WireFlags { schema_version: 1, ..Default::default() };
    let parts = encode_frame(header(42, flags, 0), &[], None, Some(&batch));
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
