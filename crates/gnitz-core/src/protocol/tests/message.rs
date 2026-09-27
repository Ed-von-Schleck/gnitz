use super::*;
use crate::protocol::codec::{encode_schema_block, schema_from_block};
use crate::protocol::types::{BatchAppender, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch};
use crate::protocol::wal_block::decode_wal_block;
use crate::protocol::{ClientVerb, WireConflictMode, WireFlags, WireStatus};
use crate::test_support::{german_col, payload_of};
use gnitz_wire::control::peek_control_block;

/// A plain push() request's flags.
fn push() -> WireFlags {
    WireFlags {
        verb: ClientVerb::Push,
        ..Default::default()
    }
}

/// A frame's schema block, decoded.
fn frame_schema(buf: &[u8], ctrl: &gnitz_wire::control::DecodedControl) -> Option<Schema> {
    ctrl.schema
        .clone()
        .map(|r| schema_from_block(&buf[r]).expect("a schema block decodes"))
}

/// A frame's data block, decoded under `schema`.
fn frame_data(buf: &[u8], ctrl: &gnitz_wire::control::DecodedControl, schema: &Schema) -> Option<ZSetBatch> {
    ctrl.data
        .clone()
        .map(|r| decode_wal_block(&buf[r], schema).expect("a data block decodes"))
}

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

    let family = |tid, batch, mode, basis| PushFamily { tid, schema: &schema, batch, mode, basis };
    let blind = gnitz_wire::txn_frame::BLIND;
    let families = [
        family(16, &b0, WireConflictMode::Update, 42),
        family(17, &b1, WireConflictMode::Error, blind),
        family(16, &b2, WireConflictMode::Update, blind),
    ];
    let payload = encode_push_txn(&families);

    // The frame layout is `gnitz_wire::txn_frame`'s and is tested there; what
    // this pins is the adapter above it: each family's two blocks are built from
    // *that* family's schema, batch, tid and basis, in that order.
    let ctrl = peek_control_block(&payload).unwrap();
    let decoded = gnitz_wire::txn_frame::decode_items(&payload[ctrl.body], ClientVerb::PushTxn).unwrap();
    let expected = [
        (16u64, WireConflictMode::Update, 42, 2usize),
        (17, WireConflictMode::Error, blind, 1),
        (16, WireConflictMode::Update, blind, 1),
    ];
    assert_eq!(decoded.len(), expected.len());
    for ((frame, fam), (exp_tid, exp_mode, exp_basis, exp_rows)) in decoded.iter().zip(expected) {
        let got = (fam.hdr.target_id, fam.hdr.flags.conflict_mode, fam.hdr.arg0);
        assert_eq!(got, (exp_tid, exp_mode, exp_basis));
        // The schema record is this family's.
        let block_schema = frame_schema(frame, fam).unwrap();
        assert_eq!(block_schema, schema);
        // ... and the data block decodes against it, with this family's rows.
        let batch = frame_data(frame, fam, &block_schema).unwrap();
        assert_eq!(batch.len(), exp_rows);
    }
}

// ── encode_frame round-trips ────────────────────────────────────────────

fn header(target_id: u64, flags: WireFlags, arg0: u64) -> ControlHeader {
    ControlHeader {
        target_id,
        flags,
        arg0,
        ..Default::default()
    }
}

#[test]
fn string_columns_round_trip_through_a_frame() {
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
            vec![
                german_col(&as_cells(&col1), &mut blob),
                german_col(&as_cells(&col2), &mut blob),
            ],
        ),
        blob,
    };

    let buf = encode_frame(
        header(0, push(), 0),
        &[],
        Some(&encode_schema_block(&schema)),
        Some(&batch),
    );
    let ctrl = peek_control_block(&buf).unwrap();
    assert_eq!(frame_schema(&buf, &ctrl).as_ref(), Some(&schema));
    let data = frame_data(&buf, &ctrl, &schema).unwrap();
    assert_eq!(data.nulls, nulls);
    assert_eq!(german_vals(&data, 0), col1);
    assert_eq!(german_vals(&data, 1), col2);
}

/// String values as the byte cells `german_col` takes.
fn as_cells(vals: &[Option<String>]) -> Vec<Option<&[u8]>> {
    vals.iter().map(|v| v.as_deref().map(str::as_bytes)).collect()
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

/// Every control scalar and the blob a reply is read for survive the encode.
#[test]
fn every_control_field_survives_the_encode() {
    let hdr = ControlHeader {
        arg1: 7,
        ..header(0xDEAD_BEEF_1234_5678, push(), 0xAAAA_BBBB_CCCC_DDDD)
    };
    let buf = encode_frame(hdr, b"a blob", None, None);
    let ctrl = peek_control_block(&buf).unwrap();
    assert_eq!(ctrl.hdr, hdr);
    assert_eq!(&buf[ctrl.blob.clone()], b"a blob");
    assert_eq!(ctrl.fault(&buf), None);
}

/// Under a non-`Ok` status the blob is the error text.
#[test]
fn a_non_ok_status_carries_its_text_as_the_blob() {
    let err_hdr = ControlHeader {
        status: WireStatus::Error,
        ..Default::default()
    };
    let buf = encode_frame(err_hdr, b"something broke", None, None);
    let ctrl = peek_control_block(&buf).unwrap();
    assert_eq!(ctrl.hdr.status, WireStatus::Error);
    assert_eq!(ctrl.fault(&buf).map(|f| f.text).as_deref(), Some("something broke"));
}

#[test]
fn a_control_only_frame_carries_no_blocks() {
    let buf = encode_frame(header(0xDEAD, push(), 42), &[], None, None);
    let ctrl = peek_control_block(&buf).unwrap();
    assert_eq!(ctrl.hdr.target_id, 0xDEAD);
    assert_eq!(ctrl.hdr.arg0, 42);
    assert_eq!((ctrl.schema, ctrl.data), (None, None));
}

#[test]
fn a_frame_with_a_schema_and_rows_round_trips() {
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

    let buf = encode_frame(
        header(42, WireFlags::default(), 0),
        &[],
        Some(&encode_schema_block(&schema)),
        Some(&batch),
    );
    let ctrl = peek_control_block(&buf).unwrap();
    assert_eq!(ctrl.hdr.target_id, 42);
    assert_eq!(frame_schema(&buf, &ctrl).as_ref(), Some(&schema));
    let data = frame_data(&buf, &ctrl, &schema).unwrap();
    assert_eq!(data.pks.to_vec_u128(&schema), vec![1u128, 2u128, 3u128]);
    assert_eq!(data.weights, vec![1, 1, 1]);
}

#[test]
fn an_empty_batch_ships_its_schema_and_no_data() {
    let schema = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let empty = ZSetBatch::new(&schema);

    let buf = encode_frame(
        header(10, WireFlags::default(), 0),
        &[],
        Some(&encode_schema_block(&schema)),
        Some(&empty),
    );
    let ctrl = peek_control_block(&buf).unwrap();
    // Schema sent, but no data (empty batch)
    assert!(ctrl.schema.is_some());
    assert!(ctrl.data.is_none());
}
