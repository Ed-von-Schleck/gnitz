use crate::catalog::encode_schema_block;
use crate::runtime::wire::{
    decode_wire, decode_wire_ipc, decode_wire_ipc_zero_copy_with_ctrl, validate_schema_match, WireData, WireMsg,
};
use crate::test_support::{make_batch, make_batch_raw, u64_pk_schema};
use gnitz_store::schema::{decode_schema_block, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder, Layout, MAX_BATCH_REGIONS};
use gnitz_wire::control::{peek_control_block_ipc, CTRL_BLOCK_SIZE_NO_BLOB};
use gnitz_wire::try_decode_german_string;
use gnitz_wire::type_code;
use gnitz_wire::{ClientVerb, WireFlags, WireStatus};

/// The anonymous schema block a frame for `sd` under `target_id` carries — what
/// every producer in the tree hands `WireMsg::schema_block`.
fn sblock(sd: &SchemaDescriptor, target_id: u64) -> Vec<u8> {
    encode_schema_block(sd, target_id as u32)
}

/// The narrow frame schema every fixture here uses: `(u64 pk, u64 val)`.
fn simple_schema() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(type_code::U64, 0))
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
fn make_blobless_batch(n: usize) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|i| (i as u64, i as i64 + 1, i as i64 * 10)).collect();
    make_batch_raw(&simple_schema(), &rows)
}

#[test]
fn encode_decode_roundtrip_with_schema() {
    let sd = simple_schema();
    let blk = sblock(&sd, 1);
    let wire = WireMsg {
        target_id: 1,
        schema_block: Some(&blk),
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
    let blk = sblock(&sd, 5);
    let wire = WireMsg {
        target_id: 5,
        schema_block: Some(&blk),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    assert!(decoded.data_batch.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.len(), 1);
    let pk = db.get_pk(0) as u64;
    assert_eq!(pk, 100);
    let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val, 999);
    assert_eq!(db.get_weight(0), 1, "the row's weight must survive the round-trip");
    assert_eq!(db.layout(), Layout::Consolidated, "the layout claim survives the frame");
}

/// Compound-PK order (including a non-identity `pk_indices` permutation) must
/// survive the schema-block wire round-trip. The
/// catalog-restart peer of this — `schema_roundtrip_catalog_preserves_pk_order`
/// in `catalog/suites/compound_pk_smoke.rs` — exercises the same property through
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
        let block = crate::catalog::encode_schema_block(&original, 0);
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
    let mut bb = BatchBuilder::new(sd);

    bb.begin_row(1u128, 1);
    bb.put_int(42);
    bb.put_string("hello");
    bb.end_row();

    bb.begin_row(2u128, 1);
    bb.put_int(99);
    let long_str = "this is a long string that exceeds twelve bytes";
    bb.put_string(long_str);
    bb.end_row();
    let batch = bb.finish();

    let blk = sblock(&sd, 10);
    let wire = WireMsg {
        target_id: 10,
        schema_block: Some(&blk),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.len(), 2);

    let mut s1 = [0u8; 16];
    s1.copy_from_slice(&db.col_data(1)[0..16]);
    let str1 = try_decode_german_string(&s1, &db.blob).unwrap();
    assert_eq!(str1, b"hello");

    let mut s2 = [0u8; 16];
    s2.copy_from_slice(&db.col_data(1)[16..32]);
    let str2 = try_decode_german_string(&s2, &db.blob).unwrap();
    assert_eq!(str2, long_str.as_bytes());
}

fn every_frame_shape() -> Vec<Vec<u8>> {
    let sd = simple_schema();
    let batch = make_simple_batch(1, 42);
    let blk = sblock(&sd, 1);
    vec![
        WireMsg { target_id: 1, ..Default::default() }.encode_to_vec(),
        WireMsg {
            target_id: 1,
            schema_block: Some(&blk),
            ..Default::default()
        }
        .encode_to_vec(),
        WireMsg {
            target_id: 1,
            schema_block: Some(&blk),
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
    let empty = make_blobless_batch(0);
    let few = make_blobless_batch(4);
    let blk = sblock(&sd, 0);
    let msgs = [
        WireMsg::default(),
        WireMsg {
            schema_block: Some(&blk),
            ..Default::default()
        },
        WireMsg {
            schema_block: Some(&blk),
            data: WireData::Whole(Some(&batch)),
            ..Default::default()
        },
        WireMsg {
            status: WireStatus::Error,
            error_msg: b"something went wrong",
            ..Default::default()
        },
        WireMsg {
            schema_block: Some(&blk),
            data: WireData::Whole(Some(&empty)),
            ..Default::default()
        },
        WireMsg {
            schema_block: Some(&blk),
            data: WireData::Whole(Some(&few)),
            ..Default::default()
        },
    ];
    for (i, msg) in msgs.iter().enumerate() {
        let sz = msg.size();
        let mut buf = vec![0u8; sz];
        assert_eq!(msg.encode(&mut buf, 0), sz, "shape {i}: encode wrote != size()");
    }
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
            status: WireStatus::Error,
            error_msg,
            ..Default::default()
        }
        .encode_to_vec();
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.target_id, 7);
        assert_eq!(decoded.control.client_id, 3);
        assert_eq!(decoded.control.request_id, 0xABCD);
        assert_eq!(decoded.control.status, WireStatus::Error);
        assert_eq!(decoded.control.error_msg, error_msg);
    }
}

/// Every control field round-trips through `decode_wire`, each at a distinct
/// value so a transposed pair fails rather than reading back unchanged.
/// `request_id` is at `u64::MAX`, the widest value the reply-routing key takes.
#[test]
fn decode_wire_round_trips_every_control_field() {
    let flags = WireFlags {
        verb: ClientVerb::ScanSpec,
        schema_version: 0xCAFE,
        continuation: true,
        ..Default::default()
    };
    let wire = WireMsg {
        target_id: 0xDEAD,
        client_id: 0xBEEF,
        flags,
        seek_pk: 0x1111u128 | (0x2222u128 << 64),
        seek_col_idx: 0x3333,
        request_id: u64::MAX,
        ..Default::default()
    }
    .encode_to_vec();
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 0xDEAD);
    assert_eq!(decoded.control.client_id, 0xBEEF);
    assert_eq!(decoded.control.flags, flags);
    assert_eq!(decoded.control.seek_pk, 0x1111u128 | (0x2222u128 << 64));
    assert_eq!(decoded.control.seek_col_idx, 0x3333);
    assert_eq!(decoded.control.request_id, u64::MAX);
    assert!(decoded.control.error_msg.is_empty(), "no message means no message");
    assert_eq!(
        decoded.control.status,
        WireStatus::Ok,
        "a frame that says nothing says OK"
    );
    assert!(decoded.schema.is_none());
    assert!(decoded.data_batch.is_none());
}

// ---------------------------------------------------------------------------
// Scan chunking tests
// ---------------------------------------------------------------------------

/// One chunk of a reply train — rows [2, 5) of an 8-row batch — round-trips
/// through the frame codec at its predicted size.
#[test]
fn encode_chunk_roundtrip() {
    let sd = simple_schema();
    let batch = make_blobless_batch(8);
    let blk = sblock(&sd, 1);

    // A budget sized for exactly three rows of this schema.
    let chunk = batch.wire_chunk_within(2, 0, make_blobless_batch(3).wire_byte_size());
    assert_eq!(chunk.len(), 3);
    let msg = WireMsg {
        target_id: 1,
        schema_block: Some(&blk),
        data: WireData::Whole(Some(&chunk)),
        ..Default::default()
    };
    let sz = msg.size();
    let mut buf = vec![0u8; sz];
    assert_eq!(msg.encode_ipc(&mut buf, 0), sz);

    // Every region must be sliced by the same range: PK, weight and payload are
    // distinct per row, so a range applied to one region and not another shows up.
    let decoded = decode_wire_ipc(&buf).expect("decode_wire_ipc");
    let b = decoded.data_batch.expect("data_batch");
    assert_eq!(b.len(), 3);
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

/// A continuation frame (`has_data`, no `has_schema`) decodes against a
/// schema hint, and not without one.
#[test]
fn continuation_frame_decoded_with_schema_hint() {
    let sd = simple_schema();
    let batch = make_blobless_batch(4);

    // Encode a continuation frame: no schema, `continuation` set.
    let msg = WireMsg {
        target_id: 1,
        flags: WireFlags { continuation: true, ..Default::default() },
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    };
    let mut buf = vec![0u8; msg.size()];
    msg.encode_ipc(&mut buf, 0);

    // decode_wire_ipc must fail (no schema in frame, no hint).
    assert!(
        decode_wire_ipc(&buf).is_err(),
        "decode_wire_ipc should fail for continuation frame without schema"
    );

    // With a hint it decodes, every region sliced by the same rows.
    let ctrl = peek_control_block_ipc(&buf).expect("control block");
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let decoded =
        decode_wire_ipc_zero_copy_with_ctrl(&buf, ctrl, Some(&sd), &mut offsets).expect("decode with schema hint");
    let b = decoded.data_batch.as_ref().expect("data_batch");
    assert_eq!(b.len(), 4);
    for i in 0..4usize {
        assert_eq!(gnitz_wire::widen_pk_be(b.get_pk_bytes(i)), i as u128);
        assert_eq!(b.get_weight(i), i as i64 + 1, "row {i} weight");
    }
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

/// The shared decoder applies the header's `batch_consolidated` bit onto
/// the decoded batch. Encode a frame whose data batch is flagged consolidated
/// (the encoder mirrors the batch's own claim into the header), decode it, and
/// confirm the claim arrives — read as the tag, since the one-row batch would
/// answer `is_consolidated()` structurally either way.
#[test]
fn decode_applies_batch_flags() {
    let schema = two_col_schema(0);
    let batch = make_batch(&schema, &[(1, 1, 42)]);

    let blk = sblock(&schema, 7);
    let wire = WireMsg {
        target_id: 7,
        schema_block: Some(&blk),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();

    let decoded = decode_wire(&wire).expect("decode");
    let b = decoded.data_batch.as_ref().expect("data batch present");
    assert_eq!(b.layout(), Layout::Consolidated, "decoder applies batch_consolidated");
}

fn two_col_schema(col1_nullable: u8) -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(type_code::I64, col1_nullable))
}

/// Each mismatch family is rejected, and each names itself distinctly.
///
/// `wire == expected` is the verdict; the message only names the first
/// differing column, and the executor and reply-train decoder surface it.
/// Four `is_err()` assertions would still pass if the message collapsed to
/// one constant string — pairwise distinctness is what tests it, without
/// pinning the prose.
#[test]
fn validate_schema_match_names_each_mismatch_distinctly() {
    let col = |tc, n| SchemaColumn::new(tc, n);
    let expected = two_col_schema(0);
    assert!(
        validate_schema_match(&expected, &expected).is_ok(),
        "a schema matches itself"
    );
    let cases = [
        (
            "count",
            SchemaDescriptor::new(&[col(type_code::U64, 0)], &[0]),
            expected,
        ),
        (
            "pk",
            SchemaDescriptor::new(&[col(type_code::U64, 0), col(type_code::I64, 0)], &[1]),
            expected,
        ),
        (
            "type",
            SchemaDescriptor::new(&[col(type_code::U64, 0), col(type_code::F64, 0)], &[0]),
            expected,
        ),
        ("nullable", two_col_schema(0), two_col_schema(1)),
    ];
    let msgs: Vec<String> = cases
        .iter()
        .map(|(what, wire, exp)| validate_schema_match(wire, exp).expect_err(what))
        .collect();
    for i in 0..msgs.len() {
        for j in (i + 1)..msgs.len() {
            assert_ne!(
                msgs[i], msgs[j],
                "{} vs {} report the same message",
                cases[i].0, cases[j].0
            );
        }
    }
}

/// A 4-byte PK and stride-4 payload columns: the wire block pads between
/// regions, so the scatter encoder's destination carve and the block framer's
/// sizing must agree offset for offset. Odd row counts are where they diverge if
/// either walk drops the padding.
#[test]
fn scattered_roundtrips_over_a_padded_schema() {
    let sd = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I16, 0),
        ],
        &[0],
    );
    let blk = sblock(&sd, 3);

    let mut bb = BatchBuilder::new(sd);
    for i in 0..8u32 {
        bb.begin_row(i as u128, (i as i64) + 1);
        bb.put_int(i as i32 as u128 * 10);
        bb.put_int(i as i16 as u128 * 3);
        bb.end_row();
    }
    let batch = bb.finish();

    for count in [1usize, 3, 7] {
        let indices: Vec<u32> = (0..count as u32).map(|i| i * 2 % 8).collect();
        let msg = WireMsg {
            target_id: 3,
            schema_block: Some(&blk),
            data: WireData::Scattered {
                batch: &batch,
                indices: &indices,
                schema: &sd,
            },
            ..Default::default()
        };
        let sz = msg.size();
        let mut buf = vec![0u8; sz];
        assert_eq!(msg.encode(&mut buf, 0), sz, "{count} rows: size must size its encode");

        let decoded = decode_wire(&buf).expect("a scattered block decodes");
        let got = decoded.data_batch.expect("it carries rows");
        assert_eq!(got.len(), count);
        for (j, &src) in indices.iter().enumerate() {
            assert_eq!(got.get_pk(j), src as u128, "{count} rows: pk {j}");
            assert_eq!(got.get_weight(j), src as i64 + 1, "{count} rows: weight {j}");
            assert_eq!(
                i32::from_le_bytes(got.col_data(0)[j * 4..j * 4 + 4].try_into().unwrap()),
                src as i32 * 10,
                "{count} rows: col 0 of row {j}"
            );
            assert_eq!(
                i16::from_le_bytes(got.col_data(1)[j * 2..j * 2 + 2].try_into().unwrap()),
                src as i16 * 3,
                "{count} rows: col 1 of row {j}"
            );
        }
    }
}
