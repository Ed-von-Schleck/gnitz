use crate::runtime::wire::{decode_client_frame, decode_sal_slot, unknown, WireData, WireMsg, WireSchema};
use crate::test_support::{make_batch, make_batch_raw, make_schema_u64_i64, make_string_batch, weighted_rows};
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder, Layout};
use gnitz_wire::control::{encode_frame_head, frame_head_size, peek_control_block, ControlHeader};
use gnitz_wire::{ClientVerb, TypeCode, WireFlags, WireStatus};

/// The source batch and the rows of it `data` sends, in the order it sends them.
fn sent(data: WireData<'_>) -> Option<(&Batch, Vec<usize>)> {
    match data {
        WireData::None => None,
        WireData::Whole(b) => Some((b, (0..b.len()).collect::<Vec<_>>())),
        WireData::Frame { batch, frame } => {
            Some((batch, (frame.end() - frame.rows()..frame.end()).collect::<Vec<_>>()))
        }
        WireData::Scattered { batch, indices } => Some((batch, indices.iter().map(|&i| i as usize).collect())),
    }
    .filter(|(_, rows)| !rows.is_empty())
}

/// Eight rows whose regions a block pads between: a 4-byte PK, then stride-4 and
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
    let mut bb = BatchBuilder::new(sd);
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
    let frame = raw.wire_frame_within(2, 0, raw.wire_byte_size_range(3));
    assert_eq!(frame.rows(), 3, "a budget of three rows frames three");

    let fixed_rel = WireSchema::encoded(5, fixed);
    let string_rel = WireSchema::encoded(6, *strings.schema());
    let padded_rel = WireSchema::encoded(7, *padded.schema());
    let claims_consolidated = WireFlags {
        batch_consolidated: true,
        ..Default::default()
    };
    let shapes = [
        (
            None,
            WireMsg {
                target_id: 0xDEAD,
                flags: WireFlags {
                    verb: ClientVerb::PushTxn,
                    continuation: true,
                    ..claims_consolidated
                },
                arg0: 0x1111_2222_3333_4444,
                arg1: 0x5555,
                ..Default::default()
            },
        ),
        (Some(&fixed_rel), WireMsg::default()),
        (
            Some(&fixed_rel),
            WireMsg {
                data: WireData::Whole(&consolidated),
                ..Default::default()
            },
        ),
        (
            Some(&fixed_rel),
            WireMsg {
                data: WireData::Whole(&empty),
                ..Default::default()
            },
        ),
        (
            Some(&string_rel),
            WireMsg {
                data: WireData::Whole(&strings),
                ..Default::default()
            },
        ),
        (
            Some(&fixed_rel),
            WireMsg {
                flags: claims_consolidated,
                data: WireData::Frame { batch: &raw, frame },
                ..Default::default()
            },
        ),
        (
            Some(&padded_rel),
            WireMsg {
                data: WireData::Scattered { batch: &padded, indices: &[6, 1, 3] },
                ..Default::default()
            },
        ),
        (
            None,
            WireMsg {
                target_id: 7,
                status: WireStatus::Error,
                blob: b"something went wrong",
                ..Default::default()
            },
        ),
    ];

    for (shape, (rel, msg)) in shapes.into_iter().enumerate() {
        let msg = rel.map_or(msg, |r| r.frame(msg));
        let wire = msg.encode_to_vec();
        let d = decode_sal_slot(&wire, |_, _| None).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        let sent = sent(msg.data);
        let consolidated = sent.as_ref().is_some_and(|(b, _)| b.layout() == Layout::Consolidated);
        let hdr = ControlHeader {
            status: msg.status,
            target_id: msg.target_id,
            flags: WireFlags {
                batch_consolidated: consolidated,
                ..msg.flags
            },
            arg0: msg.arg0,
            arg1: msg.arg1,
        };
        assert_eq!(d.control.hdr, hdr, "shape {shape}");
        assert_eq!(d.blob, msg.blob, "shape {shape}");
        assert_eq!(d.schema, rel.map(|r| *r.descriptor()), "shape {shape}");
        match (sent, d.data_batch) {
            (None, None) => {}
            (Some((src, rows)), Some(got)) => {
                let all = weighted_rows(src);
                let want: Vec<_> = rows.iter().map(|&i| all[i].clone()).collect();
                assert_eq!(weighted_rows(&got), want, "shape {shape}");
                assert_eq!(got.layout(), src.layout(), "shape {shape}");
            }
            (want, got) => panic!(
                "shape {shape}: sent rows {}, decoded rows {}",
                want.is_some(),
                got.is_some()
            ),
        }
        for cut in 0..wire.len() {
            assert!(
                decode_sal_slot(&wire[..cut], |_, _| None).is_err(),
                "shape {shape}: a {cut}/{}-byte prefix decodes",
                wire.len()
            );
        }
    }
}

/// A slot's rows are laid out under the schema `known` answers for its target
/// id and block.
#[test]
fn a_sal_slot_lays_its_rows_out_under_the_known_schema() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let junk = [0xFFu8; 12];
    let wire = WireMsg {
        target_id: 77,
        schema_block: Some(&junk),
        data: WireData::Whole(&batch),
        ..Default::default()
    }
    .encode_to_vec();
    let d = decode_sal_slot(&wire, |tid, record| {
        assert_eq!((tid, record), (77, junk.as_slice()));
        Some(sd)
    })
    .expect("the known schema lays the rows out");
    let got = d.data_batch.expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));
    assert!(
        decode_sal_slot(&wire, |_, _| None).is_err(),
        "the junk block itself does not decode"
    );
}

/// A client frame's consolidation claim is dropped, and its rows need a schema.
#[test]
fn a_client_frame_keeps_no_claim_and_needs_a_schema() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let rel = WireSchema::encoded(3, sd);
    let decode = |wire: &[u8], recordless: Option<&SchemaDescriptor>, known: fn(&[u8]) -> _| {
        decode_client_frame(wire, peek_control_block(wire)?, recordless, known)
    };

    let framed = rel
        .frame(WireMsg {
            data: WireData::Whole(&batch),
            ..Default::default()
        })
        .encode_to_vec();
    let got = decode(&framed, None, unknown)
        .expect("decodes")
        .data_batch
        .expect("rows");
    assert_eq!(
        got.layout(),
        Layout::Raw,
        "a client's consolidation claim is not trusted"
    );
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));
    assert_eq!(
        decode(&framed, None, |_| Err("refused".into())).err().as_deref(),
        Some("refused")
    );

    let bare = WireMsg {
        target_id: 3,
        data: WireData::Whole(&batch),
        ..Default::default()
    }
    .encode_to_vec();
    assert!(
        decode(&bare, None, unknown).is_err(),
        "rows with no schema to lay them out"
    );
    let got = decode(&bare, Some(&sd), unknown)
        .expect("decodes")
        .data_batch
        .expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));

    // A well-formed block of zero rows, which `WireMsg` never emits.
    let empty = make_batch(&sd, &[]);
    let mut hollow = vec![0; frame_head_size(0, None) + empty.wire_byte_size()];
    let pos = encode_frame_head(&mut hollow, &ControlHeader::default(), &[], None, true);
    empty.encode_to_wire(&mut hollow[pos..]);
    assert_eq!(
        decode(&hollow, Some(&sd), unknown).err().as_deref(),
        Some("a data block with no rows")
    );
}
