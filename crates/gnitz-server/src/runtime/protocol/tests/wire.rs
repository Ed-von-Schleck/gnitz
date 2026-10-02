use crate::runtime::wire::{decode_client_rows, WireMsg};
use crate::test_support::{encode_to_wire_vec, make_batch, make_schema_u64_i64, weighted_rows};
use gnitz_wire::control::{encode_frame_head, frame_head_size, peek_control_block, ControlHeader};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::encode_schema_block;

/// A client frame's rows are decoded as a foreign block under the schema the
/// caller names: no consolidation claim, no null bit on a NOT NULL column, and
/// at least one row.
#[test]
fn a_client_frames_rows_are_a_foreign_block_under_the_callers_schema() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let record = encode_schema_block(&sd);
    let frame = |rest: WireMsg<'_>| {
        WireMsg {
            target_id: 3,
            schema_block: Some(&record),
            ..rest
        }
        .encode_to_vec()
    };
    let decode = |wire: &[u8]| decode_client_rows(wire, &peek_control_block(wire).expect("a control block"), &sd);

    let framed = frame(WireMsg {
        data: batch.wire_whole(),
        ..Default::default()
    });
    let got = decode(&framed).expect("decodes").expect("rows");
    assert!(!got.is_consolidated(), "a decoded frame carries no claim");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));

    let dataless = frame(WireMsg::default());
    assert!(decode(&dataless).expect("decodes").is_none(), "no data block, no rows");

    let mut bb = BatchBuilder::new(&sd);
    bb.begin_row(1, 1);
    bb.put_null();
    bb.end_row();
    let nulled = bb.finish();
    let nulled = frame(WireMsg {
        data: nulled.wire_whole(),
        ..Default::default()
    });
    assert_eq!(decode(&nulled).err(), Some("a null bit on a NOT NULL column"));

    // A well-formed block of zero rows, which `WireMsg` never emits.
    let block = encode_to_wire_vec(&make_batch(&sd, &[]));
    let mut hollow = vec![0; frame_head_size(0, None) + block.len()];
    let pos = encode_frame_head(&mut hollow, &ControlHeader::default(), &[], None, true);
    hollow[pos..].copy_from_slice(&block);
    assert_eq!(decode(&hollow).err(), Some("block holds no rows"));
}
