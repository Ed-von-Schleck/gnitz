use crate::runtime::wire::{decode_client_rows, decode_sal_rows, WireMsg, WireSchema};
use crate::test_support::{
    encode_to_wire_vec, make_batch, make_batch_raw, make_schema_u64_i64, make_string_batch, weighted_rows,
};
use gnitz_wire::control::{encode_frame_head, frame_head_size, peek_control_block, ControlHeader};
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_wire::{ClientVerb, TypeCode, WireFlags, WireStatus};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// A SAL slot's rows, its record decoded.
fn sal_rows(wire: &[u8]) -> Result<Option<Batch>, String> {
    decode_sal_rows(wire, &peek_control_block(wire)?, |_, _| None)
}

/// Eight rows of unequal region strides: a 4-byte PK, then stride-4 and
/// stride-2 payloads.
fn padded_batch() -> Batch {
    let sd = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I16, false),
        ],
        &[0],
    );
    let mut bb = BatchBuilder::new(&sd);
    for i in 0..8u32 {
        bb.begin_row(i as u128, i as i64 + 1);
        bb.put_int(i as u128 * 10);
        bb.put_int(i as u128 * 3);
        bb.end_row();
    }
    bb.finish()
}

/// Every frame shape decodes to what was sent, and no proper prefix of it
/// decodes.
#[test]
fn every_frame_shape_round_trips_and_no_prefix_decodes() {
    let fixed = make_schema_u64_i64();
    let consolidated = make_batch(&fixed, &[(1, 2, -5), (2, -1, 7), (9, 3, 0)]);
    let empty = Batch::empty_with_schema(&fixed);
    let raw = make_batch_raw(
        &fixed,
        &(0..8).map(|i| (7 - i, i as i64 + 1, i as i64 * 10)).collect::<Vec<_>>(),
    );
    let long = b"a string long enough to leave the inline prefix".as_slice();
    let strings = make_string_batch(&[(1, 1, b"inline".as_slice()), (2, -2, long), (3, 1, long)]);
    let padded = padded_batch();
    let row_width = (raw.wire_whole().unwrap().byte_size() - WAL_HEADER_SIZE) / raw.len();
    let range = raw.wire_rows_within(2, WAL_HEADER_SIZE + 3 * row_width).unwrap();
    assert_eq!(range.rows(), 3, "a budget of three rows frames three");
    let whole = |b: &Batch| (0..b.len()).collect::<Vec<_>>();

    let fixed_rel = WireSchema::encoded(5, &fixed);
    let string_rel = WireSchema::encoded(6, strings.schema());
    let padded_rel = WireSchema::encoded(7, padded.schema());
    // Each message beside the source batch and the rows of it the message sends,
    // in the order it sends them.
    let shapes = [
        (
            None,
            WireMsg {
                target_id: 0xDEAD,
                flags: WireFlags {
                    verb: ClientVerb::PushTxn,
                    continuation: true,
                    ..Default::default()
                },
                arg0: 0x1111_2222_3333_4444,
                arg1: 0x5555,
                ..Default::default()
            },
            None,
        ),
        (Some(&fixed_rel), WireMsg::default(), None),
        (
            Some(&fixed_rel),
            WireMsg {
                data: consolidated.wire_whole(),
                ..Default::default()
            },
            Some((&consolidated, whole(&consolidated))),
        ),
        (
            Some(&fixed_rel),
            WireMsg {
                data: empty.wire_whole(),
                ..Default::default()
            },
            None,
        ),
        (
            Some(&string_rel),
            WireMsg {
                data: strings.wire_whole(),
                ..Default::default()
            },
            Some((&strings, whole(&strings))),
        ),
        (
            Some(&fixed_rel),
            WireMsg {
                flags: WireFlags::train_frame(true),
                data: Some(range),
                ..Default::default()
            },
            Some((&raw, vec![2, 3, 4])),
        ),
        (
            Some(&padded_rel),
            WireMsg {
                data: padded.wire_listed(&[6, 1, 3]),
                ..Default::default()
            },
            Some((&padded, vec![6, 1, 3])),
        ),
        (
            None,
            WireMsg {
                target_id: 7,
                status: WireStatus::Error,
                blob: b"something went wrong",
                ..Default::default()
            },
            None,
        ),
    ];

    for (shape, (rel, msg, sent)) in shapes.into_iter().enumerate() {
        let msg = rel.map_or(msg, |r| r.frame(msg));
        let wire = msg.encode_to_vec();
        let control = peek_control_block(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        let rows = sal_rows(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        let hdr = ControlHeader {
            status: msg.status,
            target_id: msg.target_id,
            flags: msg.flags,
            arg0: msg.arg0,
            arg1: msg.arg1,
        };
        assert_eq!(control.hdr, hdr, "shape {shape}");
        assert_eq!(&wire[control.blob], msg.blob, "shape {shape}");
        assert_eq!(control.schema.map(|r| &wire[r]), msg.schema_block, "shape {shape}");
        match (sent, rows) {
            (None, None) => {}
            (Some((src, rows)), Some(got)) => {
                let all = weighted_rows(src);
                let want: Vec<_> = rows.iter().map(|&i| all[i].clone()).collect();
                assert_eq!(weighted_rows(&got), want, "shape {shape}");
            }
            (want, got) => panic!(
                "shape {shape}: sent rows {}, decoded rows {}",
                want.is_some(),
                got.is_some()
            ),
        }
        for cut in 0..wire.len() {
            assert!(
                sal_rows(&wire[..cut]).is_err(),
                "shape {shape}: a {cut}/{}-byte prefix decodes",
                wire.len()
            );
        }
    }
}

/// A slot's rows are laid out under the schema `known` answers for its target
/// id and record, else under the record itself — and need a record.
#[test]
fn a_sal_slot_lays_its_rows_out_under_the_known_schema_or_its_record() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let junk = [0xFFu8; 12];
    let wire = WireMsg {
        target_id: 77,
        schema_block: Some(&junk),
        data: batch.wire_whole(),
        ..Default::default()
    }
    .encode_to_vec();
    let control = peek_control_block(&wire).expect("a control block");
    let got = decode_sal_rows(&wire, &control, |tid, record| {
        assert_eq!((tid, record), (77, junk.as_slice()));
        Some(sd)
    })
    .expect("the known schema lays the rows out")
    .expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));
    assert!(sal_rows(&wire).is_err(), "the junk record itself does not decode");

    let framed = WireSchema::encoded(77, &sd)
        .frame(WireMsg {
            data: batch.wire_whole(),
            ..Default::default()
        })
        .encode_to_vec();
    let got = sal_rows(&framed).expect("the record decodes").expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));

    let bare = WireMsg {
        target_id: 77,
        data: batch.wire_whole(),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        sal_rows(&bare).err().as_deref(),
        Some("a data block without a schema block")
    );
}

/// A client frame's rows are decoded as a foreign block under the schema the
/// caller names: no consolidation claim, no null bit on a NOT NULL column, and
/// at least one row.
#[test]
fn a_client_frames_rows_are_a_foreign_block_under_the_callers_schema() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let rel = WireSchema::encoded(3, &sd);
    let decode = |wire: &[u8]| decode_client_rows(wire, &peek_control_block(wire).expect("a control block"), &sd);

    let framed = rel
        .frame(WireMsg {
            data: batch.wire_whole(),
            ..Default::default()
        })
        .encode_to_vec();
    let got = decode(&framed).expect("decodes").expect("rows");
    assert!(!got.is_consolidated(), "a decoded frame carries no claim");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));

    let dataless = rel.frame(WireMsg::default()).encode_to_vec();
    assert!(decode(&dataless).expect("decodes").is_none(), "no data block, no rows");

    let mut bb = BatchBuilder::new(&sd);
    bb.begin_row(1, 1);
    bb.put_null();
    bb.end_row();
    let nulled = rel
        .frame(WireMsg {
            data: bb.finish().wire_whole(),
            ..Default::default()
        })
        .encode_to_vec();
    assert_eq!(decode(&nulled).err(), Some("a null bit on a NOT NULL column"));

    // A well-formed block of zero rows, which `WireMsg` never emits.
    let block = encode_to_wire_vec(&make_batch(&sd, &[]));
    let mut hollow = vec![0; frame_head_size(0, None) + block.len()];
    let pos = encode_frame_head(&mut hollow, &ControlHeader::default(), &[], None, true);
    hollow[pos..].copy_from_slice(&block);
    assert_eq!(decode(&hollow).err(), Some("block holds no rows"));
}
