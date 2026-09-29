use super::*;
use crate::test_support::decode_wal_block;
use crate::{retraction_batch, BatchAppender, PkColumn, Schema, ZSetBatch};
use gnitz_wire::control::{peek_control_block, DecodedControl};
use gnitz_wire::{ClientVerb, WireConflictMode};
use gnitz_wire::{ColumnDef, TypeCode};

fn kv_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("val", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    }
}

/// `(pk, val)` rows at weight 1.
fn kv_batch(schema: &Schema, rows: &[(u128, i64)]) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut a = BatchAppender::new(&mut b, schema);
    for &(pk, v) in rows {
        a.add_row(pk, 1).i64_val(v);
    }
    b
}

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
    let schema = kv_schema();
    let b0 = kv_batch(&schema, &[(1, 10), (2, 20)]);
    let b1 = kv_batch(&schema, &[(3, 30)]);
    let b2 = retraction_batch(&schema, PkColumn::from_natives(&schema, [4]));

    let family = |tid, batch, mode, basis| PushFamily { tid, schema: &schema, batch, mode, basis };
    let blind = gnitz_wire::txn_frame::BLIND;
    let families = [
        family(16, &b0, WireConflictMode::Update, 42),
        family(17, &b1, WireConflictMode::Error, blind),
        family(16, &b2, WireConflictMode::Update, blind),
    ];
    let payload = encode_push_txn(&families);

    let ctrl = peek_control_block(&payload).unwrap();
    let decoded = gnitz_wire::txn_frame::decode_items(&payload[ctrl.body], ClientVerb::PushTxn).unwrap();
    assert_eq!(decoded.len(), families.len());
    for ((frame, fam), want) in decoded.iter().zip(&families) {
        let got = (fam.hdr.target_id, fam.hdr.flags.conflict_mode, fam.hdr.arg0);
        assert_eq!(got, (want.tid, want.mode, want.basis));
        let block_schema = frame_schema(frame, fam).unwrap();
        assert_eq!(block_schema, schema);
        assert_eq!(frame_data(frame, fam, &block_schema).as_ref(), Some(want.batch));
    }
}

/// A frame carries its schema record and its rows; an empty batch still ships
/// the schema, but no data block.
#[test]
fn a_frame_carries_its_schema_and_its_rows_unless_empty() {
    let schema = kv_schema();
    let hdr = ControlHeader { target_id: 42, ..Default::default() };
    for batch in [
        kv_batch(&schema, &[(1, 100), (2, 200), (3, 300)]),
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
