use super::*;
use crate::test_support::{decode_wal_block, kv_rows, kv_schema};
use crate::{retraction_batch, PkColumn, Schema, ZSetBatch};
use gnitz_wire::control::{peek_control_block, DecodedControl};
use gnitz_wire::TypeCode;
use gnitz_wire::{ClientVerb, WireConflictMode};

/// A frame's schema block, decoded.
fn frame_schema(buf: &[u8], ctrl: &DecodedControl) -> Option<Schema> {
    ctrl.schema
        .clone()
        .map(|r| Schema::from_block(&buf[r]).expect("a schema block decodes"))
}

/// A frame's data block, decoded under `schema`.
fn frame_data(buf: &[u8], ctrl: &DecodedControl, schema: &Schema) -> Option<ZSetBatch> {
    ctrl.data
        .clone()
        .map(|r| decode_wal_block(&buf[r], schema).expect("a data block decodes"))
}

/// A multi-family `PUSH_TXN` frame (both modes, a repeated tid, a delete
/// family). The frame layout is `gnitz_wire::txn_frame`'s and is tested there;
/// what this pins is the adapter above it: each family's two blocks are built
/// from *that* family's schema, batch, tid and basis, in that order.
#[test]
fn push_txn_families_carry_their_own_schema_and_batch() {
    let schema = Arc::new(kv_schema(TypeCode::I64));
    let b0 = kv_rows(&[(1, 10, 1), (2, 20, 1)]);
    let b1 = kv_rows(&[(3, 30, 1)]);
    let b2 = retraction_batch(&schema, PkColumn::from_natives(&schema, [4]));

    let family = |tid: u64, batch, mode, basis| PushFamily {
        target: tid.into(),
        schema: Arc::clone(&schema),
        batch,
        mode,
        basis,
    };
    let blind = gnitz_wire::txn_frame::BLIND;
    let families = [
        family(16, b0, WireConflictMode::Update, 42),
        family(17, b1, WireConflictMode::Error, blind),
        family(16, b2, WireConflictMode::Update, blind),
    ];
    let payload = encode_push_txn(&families);

    let ctrl = peek_control_block(&payload).unwrap();
    let decoded = gnitz_wire::txn_frame::decode_items(&payload[ctrl.body], ClientVerb::PushTxn).unwrap();
    assert_eq!(decoded.len(), families.len());
    for ((frame, fam), want) in decoded.iter().zip(&families) {
        let got = (fam.hdr.target_id, fam.hdr.flags.conflict_mode, fam.hdr.arg0);
        assert_eq!(got, (want.target.tid, want.mode, want.basis));
        let block_schema = frame_schema(frame, fam).unwrap();
        assert_eq!(block_schema, *schema);
        assert_eq!(frame_data(frame, fam, &block_schema).as_ref(), Some(&want.batch));
    }
}

/// A frame carries its schema record and its rows; an empty batch still ships
/// the schema, but no data block.
#[test]
fn a_frame_carries_its_schema_and_its_rows_unless_empty() {
    let schema = kv_schema(TypeCode::I64);
    let hdr = ControlHeader { target_id: 42, ..Default::default() };
    for batch in [
        kv_rows(&[(1, 100, 1), (2, 200, 1), (3, 300, 1)]),
        ZSetBatch::new(&schema),
    ] {
        let buf = encode_frame(hdr, &[], Some(&schema.to_block()), Some(&batch));
        let ctrl = peek_control_block(&buf).unwrap();
        assert_eq!(ctrl.hdr, hdr);
        assert_eq!(frame_schema(&buf, &ctrl).as_ref(), Some(&schema));
        let want = (!batch.is_empty()).then_some(&batch);
        assert_eq!(frame_data(&buf, &ctrl, &schema).as_ref(), want);
    }
}
