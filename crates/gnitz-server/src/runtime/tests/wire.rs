use crate::runtime::wire::{
    build_schema_wire_block, decode_schema_block, decode_wire, decode_wire_ipc, decode_wire_ipc_zero_copy_with_ctrl,
    encode_ctrl_block_direct, peek_client_control, peek_control_block, peek_control_block_ipc,
    wire_flags_set_schema_version, DecodedWireZeroCopy, SchemaWithVersion, WireData, WireMsg, FLAG_CONTINUATION,
    STATUS_ERROR, STATUS_OK,
};
use gnitz_engine::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_engine::storage::{Batch, MAX_BATCH_REGIONS};
use gnitz_engine_testkit::{make_batch, make_batch_raw, u64_pk_schema};
use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;
use gnitz_wire::type_code;
use gnitz_wire::{encode_german_string, try_decode_german_string};

/// The narrow frame schema every fixture here uses: `(u64 pk, u64 val)`.
fn simple_schema() -> SchemaDescriptor {
    u64_pk_schema(type_code::U64)
}

/// U64 pk, a U64 payload and a STRING payload.
fn string_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    )
}

/// A one-row consolidated batch over [`simple_schema`].
fn make_simple_batch(pk: u64, val: u64) -> Batch {
    make_batch(&simple_schema(), &[(pk, 1, val as i64)])
}

/// `n` rows over [`simple_schema`], each at a distinct weight and payload, so a
/// region the encoder mis-slices reads back as a wrong row rather than a right one.
fn make_wire_safe_batch(n: usize) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|i| (i as u64, i as i64 + 1, i as i64 * 10)).collect();
    make_batch_raw(&simple_schema(), &rows)
}

#[test]
fn encode_decode_roundtrip_no_data() {
    let wire = WireMsg {
        target_id: 42,
        client_id: 7,
        flags: 0x100,
        seek_pk: 10u128 | (20u128 << 64),
        seek_col_idx: 3,
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 42);
    assert_eq!(decoded.control.client_id, 7);
    assert_eq!(decoded.control.flags & 0xFFFF, 0x100);
    assert_eq!(decoded.control.seek_pk, 10u128 | (20u128 << 64));
    assert_eq!(decoded.control.seek_col_idx, 3);
    assert_eq!(decoded.control.request_id, 0);
    assert_eq!(decoded.control.status, STATUS_OK);
    assert!(decoded.control.error_msg.is_empty());
    assert!(decoded.schema.is_none());
    assert!(decoded.data_batch.is_none());
}

#[test]
fn encode_decode_roundtrip_with_schema() {
    let sd = simple_schema();
    let wire = WireMsg {
        target_id: 1,
        schema: Some(&sd),
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    let s = decoded.schema.unwrap();
    assert_eq!(s.num_columns(), 2);
    assert_eq!(s.pk_indices(), &[0]);
    assert_eq!(s.columns[0].type_code, type_code::U64);
    assert_eq!(s.columns[1].type_code, type_code::U64);
    assert!(decoded.data_batch.is_none());
}

#[test]
fn encode_decode_roundtrip_with_data() {
    let sd = simple_schema();
    let batch = make_simple_batch(100, 999);
    let wire = WireMsg {
        target_id: 5,
        schema: Some(&sd),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    assert!(decoded.data_batch.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 1);
    let pk = db.get_pk(0) as u64;
    assert_eq!(pk, 100);
    let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val, 999);
    assert_eq!(db.get_weight(0), 1, "the row's weight must survive the round-trip");
    assert!(db.is_sorted());
    assert!(db.is_consolidated());
}

/// Compound-PK order (including a non-identity `pk_indices` permutation) must
/// survive the schema-block wire round-trip. The
/// catalog-restart peer of this — `schema_roundtrip_catalog_preserves_pk_order`
/// in `catalog/tests/compound_pk_smoke.rs` — exercises the same property through
/// the catalog API.
#[test]
fn schema_roundtrip_wire_preserves_pk_order() {
    let u64c = SchemaColumn::new(type_code::U64, 0);
    let u32c = SchemaColumn::new(type_code::U32, 0);
    let cases: &[(&[SchemaColumn], &[u32])] = &[
        (&[u64c, u64c], &[0, 1]),
        (&[u64c, u64c], &[1, 0]),
        (&[u32c, u32c, u32c, u32c], &[0, 1, 2, 3]),
    ];
    for &(cols, pk_indices) in cases {
        let original = SchemaDescriptor::new(cols, pk_indices);
        let block = build_schema_wire_block(&original, 0);
        let decoded = decode_schema_block(&block, true).unwrap();
        assert!(
            original == decoded,
            "pk_indices {pk_indices:?} did not survive wire round-trip",
        );
    }
}

#[test]
fn encode_decode_string_column() {
    let sd = string_schema();
    let mut batch = Batch::with_capacity(sd, 2);

    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &42u64.to_le_bytes());
    let st1 = encode_german_string(b"hello", &mut batch.blob);
    batch.extend_col(1, &st1);
    batch.count += 1;

    batch.extend_pk(2u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &99u64.to_le_bytes());
    let long_str = b"this is a long string that exceeds twelve bytes";
    let st2 = encode_german_string(long_str, &mut batch.blob);
    batch.extend_col(1, &st2);
    batch.count += 1;

    let wire = WireMsg {
        target_id: 10,
        schema: Some(&sd),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 2);

    let mut s1 = [0u8; 16];
    s1.copy_from_slice(&db.col_data(1)[0..16]);
    let str1 = try_decode_german_string(&s1, &db.blob).unwrap();
    assert_eq!(str1, b"hello");

    let mut s2 = [0u8; 16];
    s2.copy_from_slice(&db.col_data(1)[16..32]);
    let str2 = try_decode_german_string(&s2, &db.blob).unwrap();
    assert_eq!(str2, long_str);
}

#[test]
fn encode_into_buffer_roundtrip() {
    let sd = simple_schema();
    let batch = make_simple_batch(100, 999);

    let sz = WireMsg {
        schema: Some(&sd),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .size();
    let mut buf = vec![0u8; sz];
    let written = WireMsg {
        target_id: 5,
        schema: Some(&sd),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode(&mut buf, 0);
    assert_eq!(written, sz);

    let decoded = decode_wire(&buf).unwrap();
    assert_eq!(decoded.control.target_id, 5);
    assert!(decoded.schema.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 1);
    let pk = db.get_pk(0) as u64;
    assert_eq!(pk, 100);
    let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val, 999);
    assert_eq!(db.get_weight(0), 1);
}

/// The three frame shapes a slot can take, for the sweeps below.
fn every_frame_shape() -> Vec<Vec<u8>> {
    let sd = simple_schema();
    let batch = make_simple_batch(1, 42);
    vec![
        WireMsg {
            target_id: 1,
            ..Default::default()
        }
        .encode_to_vec(),
        WireMsg {
            target_id: 1,
            schema: Some(&sd),
            ..Default::default()
        }
        .encode_to_vec(),
        WireMsg {
            target_id: 1,
            schema: Some(&sd),
            data: WireData::Whole(Some(&batch)),
            ..Default::default()
        }
        .encode_to_vec(),
    ]
}

/// A frame is framed by its own length fields, so **every** proper prefix must
/// be rejected — a control block cut short, a schema block that never arrives, a
/// data block truncated mid-region. Sweeping every cut rather than a few chosen
/// ones is what makes a new block kind fail here instead of framing through.
#[test]
fn every_truncation_of_a_frame_is_rejected() {
    for (shape, wire) in every_frame_shape().iter().enumerate() {
        for cut in 1..wire.len() {
            assert!(
                decode_wire(&wire[..cut]).is_err(),
                "shape {shape}: prefix of {cut}/{} bytes must not decode",
                wire.len()
            );
        }
    }
}

/// `encode` must write exactly the byte count `size` predicted, for every shape
/// a slot can take — the relation every caller sizing a SAL slot depends on.
/// Comparing `size()` against a `Vec` that `size()` itself allocated would
/// compare it to itself, so the observable here is `encode`'s own return value.
#[test]
fn encode_writes_exactly_the_predicted_size() {
    let sd = simple_schema();
    let batch = make_simple_batch(100, 999);
    let msgs = [
        WireMsg::default(),
        WireMsg {
            schema: Some(&sd),
            ..Default::default()
        },
        WireMsg {
            schema: Some(&sd),
            data: WireData::Whole(Some(&batch)),
            ..Default::default()
        },
        WireMsg {
            status: STATUS_ERROR,
            error_msg: b"something went wrong",
            ..Default::default()
        },
    ];
    for (i, msg) in msgs.iter().enumerate() {
        let sz = msg.size();
        let mut buf = vec![0u8; sz];
        assert_eq!(msg.encode(&mut buf, 0), sz, "shape {i}: encode wrote != size()");
    }
}

/// Verify that prebuilt schema bytes produce bit-identical output to the inline path.
/// This is the core invariant of the schema wire block cache.
#[test]
fn prebuilt_schema_block_matches_inline_encode() {
    let sd = simple_schema();
    let batch = make_simple_batch(7, 42);
    let target_id: u64 = 99;

    // Inline path (no prebuilt).
    let inline = WireMsg {
        target_id,
        schema: Some(&sd),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();

    // Prebuilt path.
    let prebuilt = build_schema_wire_block(&sd, target_id as u32);
    let sz = WireMsg {
        schema: Some(&sd),
        prebuilt_schema_block: Some(&prebuilt),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .size();
    let mut buf = vec![0u8; sz];
    WireMsg {
        target_id,
        schema: Some(&sd),
        prebuilt_schema_block: Some(&prebuilt),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode(&mut buf, 0);

    assert_eq!(buf, inline, "prebuilt schema block must produce identical wire bytes");
}

/// A cached schema block with no column defs — every name empty — is valid and
/// round-trips correctly.
#[test]
fn prebuilt_schema_block_no_col_names_roundtrips() {
    let sd = simple_schema();
    let prebuilt = build_schema_wire_block(&sd, 5);
    let sz = WireMsg {
        schema: Some(&sd),
        prebuilt_schema_block: Some(&prebuilt),
        ..Default::default()
    }
    .size();
    let mut buf = vec![0u8; sz];
    WireMsg {
        target_id: 5,
        schema: Some(&sd),
        prebuilt_schema_block: Some(&prebuilt),
        ..Default::default()
    }
    .encode(&mut buf, 0);
    let decoded = decode_wire(&buf).unwrap();
    assert!(decoded.schema.is_some(), "schema block must be present");
    assert_eq!(decoded.schema.unwrap().num_columns(), sd.num_columns());
}

/// An `error_msg` on either side of the 12-byte German-string threshold: the
/// short one stays inline in the control block, the long one spills to the blob
/// region and is read back through the directory.
#[test]
fn decode_wire_round_trips_an_error_msg_inline_and_spilled() {
    for error_msg in [
        b"boom".as_slice(),
        b"this error message is definitely longer than twelve bytes",
    ] {
        let wire = WireMsg {
            target_id: 7,
            client_id: 3,
            request_id: 0xABCD,
            status: STATUS_ERROR,
            error_msg,
            ..Default::default()
        }
        .encode_to_vec();
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.target_id, 7);
        assert_eq!(decoded.control.client_id, 3);
        assert_eq!(decoded.control.request_id, 0xABCD);
        assert_eq!(decoded.control.status, STATUS_ERROR);
        assert_eq!(decoded.control.error_msg, error_msg);
    }
}

/// Every control field round-trips through `decode_wire`, each at a distinct
/// value so a transposed pair fails rather than reading back unchanged.
/// `request_id` is at `u64::MAX`, the widest value the reply-routing key takes.
#[test]
fn decode_wire_round_trips_every_control_field() {
    let wire = WireMsg {
        target_id: 0xDEAD,
        client_id: 0xBEEF,
        flags: 0xCAFE,
        seek_pk: 0x1111u128 | (0x2222u128 << 64),
        seek_col_idx: 0x3333,
        request_id: u64::MAX,
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 0xDEAD);
    assert_eq!(decoded.control.client_id, 0xBEEF);
    assert_eq!(decoded.control.flags & 0xFFFF, 0xCAFE);
    assert_eq!(decoded.control.seek_pk, 0x1111u128 | (0x2222u128 << 64));
    assert_eq!(decoded.control.seek_col_idx, 0x3333);
    assert_eq!(decoded.control.request_id, u64::MAX);
    assert!(decoded.control.error_msg.is_empty(), "no message means no message");
}

#[test]
fn peek_client_control_on_valid_wire() {
    let wire = WireMsg {
        target_id: 99,
        client_id: 0xCAFE_BABE,
        ..Default::default()
    }
    .encode_to_vec();
    let ctrl = peek_client_control(&wire).unwrap();
    assert_eq!(ctrl.target_id, 99);
    assert_eq!(ctrl.client_id, 0xCAFE_BABE);
}

#[test]
fn peek_client_control_rejects_short_data() {
    // Any slice shorter than WAL_HEADER_SIZE must fail.
    let result = peek_client_control(&[0u8; 10]);
    assert!(
        result.is_err(),
        "peek_client_control should reject slices < WAL_HEADER_SIZE"
    );
}

/// The ctrl-block round-trip through the shared `gnitz_wire::control` codec:
/// every variable field distinct and non-zero so a swapped offset corrupts the
/// bytes detectably; `offset = 64` so writes accidentally indexing through
/// `out[offset + OFF_X..]` rather than the sub-slice are also caught. Runs
/// with and without the engine's checksum stamp, decoded at the matching
/// verification setting, so both frames must decode identically.
#[test]
fn encode_ctrl_block_direct_roundtrips() {
    const OFFSET: usize = 64;
    let target_id: u64 = 0x1111_2222_3333_4444;
    let client_id: u64 = 0x5555_6666_7777_8888;
    let wire_flags: u64 = 0x9999_AAAA_BBBB_CCCC;
    let seek_pk: u128 = (0xDDDD_EEEE_FFFF_0011u128 << 64) | 0x2233_4455_6677_8899u128;
    let seek_col_idx: u64 = 0xAA_BB_CC_DD_EE_FF_00_11;
    let request_id: u64 = 0x1234_5678_9ABC_DEF0;
    let status: u32 = 0xDEAD_BEEF;

    for &checksum in &[false, true] {
        let mut buf = vec![0u8; OFFSET + CTRL_BLOCK_SIZE_NO_BLOB];
        let n = encode_ctrl_block_direct(
            &mut buf,
            OFFSET,
            &gnitz_wire::control::ControlHeader {
                status,
                target_id,
                client_id,
                flags: wire_flags,
                seek_pk,
                seek_col_idx,
                request_id,
            },
            b"",
            &[],
            checksum,
        );
        assert_eq!(
            n, CTRL_BLOCK_SIZE_NO_BLOB,
            "encoder size mismatch (checksum={checksum})"
        );
        let block = &buf[OFFSET..OFFSET + n];
        let dec = if checksum {
            peek_control_block(block)
        } else {
            peek_control_block_ipc(block)
        }
        .expect("decode");
        assert_eq!(dec.target_id, target_id);
        assert_eq!(dec.client_id, client_id);
        assert_eq!(dec.flags, wire_flags);
        assert_eq!(dec.seek_pk, seek_pk);
        assert_eq!(dec.seek_col_idx, seek_col_idx);
        assert_eq!(dec.request_id, request_id);
        assert_eq!(dec.status, status);
        assert_eq!(dec.block_size, n);
    }
}

// ---------------------------------------------------------------------------
// Scan chunking tests
// ---------------------------------------------------------------------------

/// Decode a continuation frame (data, no schema block) against `schema` at
/// `version`, the way the master's reply-train reader does: the zero-copy
/// decoder, fed the frame's own control block and a caller-held region-offset
/// array.
fn decode_continuation<'a>(
    bytes: &'a [u8],
    schema: &SchemaDescriptor,
    version: u16,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<DecodedWireZeroCopy<'a>, &'static str> {
    let ctrl = peek_control_block_ipc(bytes)?;
    let hint = SchemaWithVersion {
        descriptor: schema,
        version,
    };
    decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl, Some(hint), offsets)
}

/// A `WireData::Range` message's `size` must match its encoded byte count.
#[test]
fn wire_size_range_matches_encoded_size() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(8);

    for count in [0usize, 1, 4, 8] {
        let sz = WireMsg {
            schema: Some(&sd),
            data: WireData::Range {
                batch: &batch,
                start_row: 0,
                count,
            },
            ..Default::default()
        }
        .size();
        let mut buf = vec![0u8; sz];
        let written = WireMsg {
            target_id: 1,
            schema: Some(&sd),
            data: WireData::Range {
                batch: &batch,
                start_row: 0,
                count,
            },
            ..Default::default()
        }
        .encode_ipc(&mut buf, 0);
        assert_eq!(written, sz, "WireData::Range size mismatch for count={count}");
    }
}

/// A `WireData::Range` message's `size` at count=1 minus count=0 gives a
/// positive per-row delta.
#[test]
fn wire_size_range_positive_per_row_delta() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(1);
    let sz0 = WireMsg {
        schema: Some(&sd),
        data: WireData::Range {
            batch: &batch,
            start_row: 0,
            count: 0,
        },
        ..Default::default()
    }
    .size();
    let sz1 = WireMsg {
        schema: Some(&sd),
        data: WireData::Range {
            batch: &batch,
            start_row: 0,
            count: 1,
        },
        ..Default::default()
    }
    .size();
    assert!(sz1 > sz0, "adding 1 row must increase wire size");
}

/// A `WireData::Range` message round-trips a sub-range of a batch correctly.
#[test]
fn encode_range_roundtrip() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(8);

    // Encode rows [2, 5) into a wire frame with the schema block.
    let sz = WireMsg {
        schema: Some(&sd),
        data: WireData::Range {
            batch: &batch,
            start_row: 0,
            count: 3,
        },
        ..Default::default()
    }
    .size();
    let mut buf = vec![0u8; sz];
    WireMsg {
        target_id: 1,
        schema: Some(&sd),
        data: WireData::Range {
            batch: &batch,
            start_row: 2,
            count: 3,
        },
        ..Default::default()
    }
    .encode_ipc(&mut buf, 0);

    // Every region must be sliced by the same range: PK, weight and payload are
    // distinct per row, so a range applied to one region and not another shows up.
    let decoded = decode_wire_ipc(&buf).expect("decode_wire_ipc");
    let b = decoded.data_batch.expect("data_batch");
    assert_eq!(b.count, 3);
    for i in 0..3usize {
        let src = i + 2;
        assert_eq!(b.get_pk(i), src as u128, "row {i} pk");
        assert_eq!(b.get_weight(i), src as i64 + 1, "row {i} weight");
        assert_eq!(
            u64::from_le_bytes(b.col_data(0)[i * 8..i * 8 + 8].try_into().unwrap()),
            src as u64 * 10,
            "row {i} payload"
        );
    }
}

/// A continuation frame (FLAG_HAS_DATA, no FLAG_HAS_SCHEMA) decodes against a
/// versioned schema hint, and only against a matching version.
#[test]
fn continuation_frame_decoded_with_schema_hint() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(4);

    // Encode a continuation frame: no schema, FLAG_CONTINUATION set.
    // Embed server_version=7 in wire_flags bits 24-39.
    let server_version: u16 = 7;
    let frame_flags = wire_flags_set_schema_version(FLAG_CONTINUATION, server_version);
    let sz = WireMsg {
        data: WireData::Range {
            batch: &batch,
            start_row: 0,
            count: 4,
        },
        ..Default::default()
    }
    .size();
    let mut buf = vec![0u8; sz];
    WireMsg {
        target_id: 1,
        flags: frame_flags,
        data: WireData::Range {
            batch: &batch,
            start_row: 0,
            count: 4,
        },
        ..Default::default()
    }
    .encode_ipc(&mut buf, 0);

    // decode_wire_ipc must fail (no schema in frame, no hint).
    assert!(
        decode_wire_ipc(&buf).is_err(),
        "decode_wire_ipc should fail for continuation frame without schema"
    );

    // A hint at the matching version must succeed.
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let decoded = decode_continuation(&buf, &sd, server_version, &mut offsets).expect("decode with schema hint");
    let b = decoded.data_batch.as_ref().expect("data_batch");
    assert_eq!(b.count, 4);
    for i in 0..4usize {
        assert_eq!(
            gnitz_wire::widen_pk_be(b.get_pk_bytes(i), b.pk_stride as usize),
            i as u128
        );
        assert_eq!(b.get_weight(i), i as i64 + 1, "row {i} weight");
    }
    drop(decoded);

    // A hint at a different version must fail.
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let err = decode_continuation(&buf, &sd, server_version + 1, &mut offsets);
    assert!(err.is_err(), "version mismatch must return Err");
}

/// The blob fallback path (an error message present) round-trips through the
/// shared codec at a non-zero offset. Both German strings spill, so the decoder
/// has to resolve one shared blob region and hand the right slice to each — and
/// they do it under the checksum stamp, which is what this wrapper adds over
/// `gnitz_wire::control::encode_ctrl_block`.
#[test]
fn encode_ctrl_block_direct_error_path_roundtrips() {
    const OFFSET: usize = 32;
    let err = b"something went wrong, well past the twelve-byte threshold";
    let extra = b"and so is this wide-pk-extra blob payload past 12B";
    let mut buf = vec![0u8; OFFSET + 1024];
    let n = encode_ctrl_block_direct(
        &mut buf,
        OFFSET,
        &gnitz_wire::control::ControlHeader {
            status: STATUS_ERROR,
            target_id: 7,
            client_id: 11,
            flags: 13,
            seek_pk: 17,
            seek_col_idx: 19,
            request_id: 23,
        },
        err,
        extra,
        true,
    );
    let dec = peek_control_block(&buf[OFFSET..OFFSET + n]).expect("decode");
    assert_eq!(dec.status, STATUS_ERROR);
    assert_eq!(dec.error_msg, err);
    assert_eq!(dec.seek_pk_extra, extra);
    assert_eq!(dec.seek_pk, 17u128);
    assert_eq!(dec.block_size, n);
}

#[test]
fn schemaless_command_slot_is_a_bare_control_block() {
    // Every command verb's SAL slot is written with no schema block and no data,
    // and `CHECKPOINT_RESERVE` is sized from that. Both the size prediction and
    // the encode must agree it is exactly one control block.
    assert_eq!(WireMsg::default().size(), CTRL_BLOCK_SIZE_NO_BLOB);
    let buf = WireMsg {
        target_id: 7,
        request_id: 42,
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(buf.len(), CTRL_BLOCK_SIZE_NO_BLOB);
}
