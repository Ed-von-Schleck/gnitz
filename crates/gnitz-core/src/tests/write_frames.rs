use super::*;
use crate::test_support::{decode_wal_block, kv_rows, kv_schema};
use crate::{Schema, ZSetBatch};
use gnitz_wire::multi_frame::decode_items;
use gnitz_wire::{ColumnDef, DecodedControl, TypeCode, WireConflictMode, peek_control_block};
use std::sync::Arc;

/// `n` rows from `first`, each `(pk, a string past the inline width, pk)`, so a
/// chunk carries a heap of its own.
fn string_rows(first: u64, n: u64) -> ZSetBatch {
    let schema = Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            &[0],
        )
        .unwrap(),
    );
    let mut b = ZSetBatch::new(&schema);
    let mut app = b.builder();
    for pk in first..first + n {
        app.begin_row(pk as u128, 1)
            .put_string(&format!("a string longer than twelve bytes, row {pk}"))
            .put_int(pk as u128);
    }
    b
}

fn family<'a>(verb: ClientVerb, tid: u64, mode: WireConflictMode, basis: u64, batch: &'a ZSetBatch) -> WriteFamily<'a> {
    let mut hdr = ControlHeader::naming(verb, Target { tid, token: tid + 100 }, basis);
    hdr.flags.conflict_mode = mode;
    WriteFamily {
        hdr,
        blob: batch.schema().shape_record(),
        rows: batch.view(),
    }
}

/// Every frame of `write`, encoded.
fn encoded(write: &SpanningWrite<'_>) -> Vec<Vec<u8>> {
    (0..write.frames())
        .map(|at| {
            let mut out = Vec::new();
            write.encode(at, &mut out);
            out
        })
        .collect()
}

/// The rows of one decoded item or frame, under `schema`; empty without a block.
fn rows_of(frame: &[u8], ctrl: &DecodedControl, schema: &Arc<Schema>) -> ZSetBatch {
    match &ctrl.data {
        // An empty family's block declares no rows, as one frame would carry it.
        Some(r) if gnitz_wire::read_u32_le(&frame[r.clone()], 4) == 0 => ZSetBatch::new(schema),
        Some(r) => decode_wal_block(&frame[r.clone()], schema).unwrap(),
        None => ZSetBatch::new(schema),
    }
}

/// A PUSH cut into frames: each within the ceiling, each a whole PUSH frame
/// naming the push's relation, token and mode, all but the last setting
/// `continuation`, and their rows in order the push's rows.
#[test]
fn a_push_is_cut_into_whole_push_frames_at_row_boundaries() {
    for batch in [
        kv_rows(&(0..1000).map(|pk| (pk, pk as i64, 1)).collect::<Vec<_>>()),
        string_rows(0, 300),
    ] {
        let fams = [family(ClientVerb::Push, 7, WireConflictMode::Error, 0, &batch)];
        let ceiling = 4096;
        let write = SpanningWrite::cut(ClientVerb::Push, &fams, ceiling).unwrap();
        let frames = encoded(&write);
        assert!(frames.len() > 2, "{} frames", frames.len());
        let mut got = ZSetBatch::new(batch.schema());
        for (i, f) in frames.iter().enumerate() {
            assert!(f.len() <= ceiling, "frame {i} is {} bytes", f.len());
            let ctrl = peek_control_block(f).unwrap();
            assert_eq!(ctrl.client_verb(), Ok(ClientVerb::Push));
            assert_eq!(ctrl.hdr.target(), Target { tid: 7, token: 107 });
            assert_eq!(ctrl.hdr.flags.conflict_mode, WireConflictMode::Error);
            assert_eq!(ctrl.hdr.flags.continuation, i + 1 < frames.len());
            assert_eq!(&f[ctrl.blob.clone()], batch.schema().shape_record());
            got.extend_from_owned(rows_of(f, &ctrl, batch.schema()));
        }
        assert_eq!(got, batch);
    }
}

/// A PUSH_TXN cut into frames: each a whole PUSH_TXN frame within the ceiling,
/// its prologue setting `continuation` but on the last, its items naming their
/// family's relation, mode and basis; a family's chunks follow one another, and
/// so the items in order hold every family's rows in order, an empty family
/// included.
#[test]
fn a_push_txn_is_cut_into_whole_txn_frames_keeping_family_order() {
    let a = kv_rows(&(0..700).map(|pk| (pk, pk as i64, 1)).collect::<Vec<_>>());
    let b = string_rows(0, 5);
    let empty = ZSetBatch::new(&kv_schema(TypeCode::I64));
    let c = string_rows(10, 400);
    let fams = [
        family(ClientVerb::PushTxn, 1, WireConflictMode::Update, 11, &a),
        family(ClientVerb::PushTxn, 2, WireConflictMode::Error, 12, &b),
        family(ClientVerb::PushTxn, 3, WireConflictMode::Update, 13, &empty),
        family(ClientVerb::PushTxn, 2, WireConflictMode::Update, 14, &c),
    ];
    let ceiling = 8192;
    let write = SpanningWrite::cut(ClientVerb::PushTxn, &fams, ceiling).unwrap();
    let frames = encoded(&write);
    assert!(frames.len() > 3, "{} frames", frames.len());
    // Each item as (family index it names, its rows), folding a family's chunks.
    let mut got: Vec<(u64, ZSetBatch)> = Vec::new();
    for (i, f) in frames.iter().enumerate() {
        assert!(f.len() <= ceiling, "frame {i} is {} bytes", f.len());
        let ctrl = peek_control_block(f).unwrap();
        assert_eq!(ctrl.client_verb(), Ok(ClientVerb::PushTxn));
        assert_eq!(ctrl.hdr.flags.continuation, i + 1 < frames.len());
        for item in decode_items(f, ctrl.body.clone(), ClientVerb::PushTxn).unwrap() {
            let basis = item.hdr.arg0;
            let fam = &fams[(basis - 11) as usize];
            assert_eq!(item.hdr.target(), fam.hdr.target());
            assert_eq!(item.hdr.flags.conflict_mode, fam.hdr.flags.conflict_mode);
            let schema = [&a, &b, &empty, &c][(basis - 11) as usize].schema();
            let rows = rows_of(f, &item, schema);
            match got.last_mut() {
                Some((last, so_far)) if *last == basis => so_far.extend_from_owned(rows),
                _ => got.push((basis, rows)),
            }
        }
    }
    let want: Vec<(u64, ZSetBatch)> = [&a, &b, &empty, &c]
        .into_iter()
        .zip(11..)
        .map(|(batch, basis)| (basis, batch.clone()))
        .collect();
    assert_eq!(got, want);
}

/// A write that fits goes as the frames it is cut into, and one row wider than
/// the ceiling goes in a frame of its own; one past the frame ceiling itself is
/// refused with its frame's size.
#[test]
fn a_row_past_the_ceiling_goes_alone_unless_past_the_frame_cap() {
    let batch = string_rows(0, 3);
    let fams = [family(ClientVerb::Push, 7, WireConflictMode::Update, 0, &batch)];
    let write = SpanningWrite::cut(ClientVerb::Push, &fams, 1).unwrap();
    assert_eq!(write.frames(), 3, "a frame per row");

    let mut wide = ZSetBatch::new(batch.schema());
    wide.builder()
        .begin_row(1, 1)
        .put_string(&"x".repeat(MAX_FRAME_PAYLOAD))
        .put_int(1);
    let fams = [family(ClientVerb::Push, 7, WireConflictMode::Update, 0, &wide)];
    let refused = SpanningWrite::cut(ClientVerb::Push, &fams, 4096).err();
    assert!(refused.is_some_and(|size| size > MAX_FRAME_PAYLOAD), "{refused:?}");
}

/// An empty family counts its block of no rows: at every ceiling, the one
/// that leaves a frame fewer bytes than that block included, each frame stays
/// within the ceiling and the frames carry every family.
#[test]
fn an_empty_family_fits_its_frame_at_every_ceiling() {
    let a = kv_rows(&(0..20).map(|pk| (pk, pk as i64, 1)).collect::<Vec<_>>());
    let empty = ZSetBatch::new(&kv_schema(TypeCode::I64));
    let fams = [
        family(ClientVerb::PushTxn, 1, WireConflictMode::Update, 11, &a),
        family(ClientVerb::PushTxn, 2, WireConflictMode::Update, 12, &empty),
        family(ClientVerb::PushTxn, 1, WireConflictMode::Update, 13, &a),
    ];
    for ceiling in 200..2000 {
        let write = SpanningWrite::cut(ClientVerb::PushTxn, &fams, ceiling).unwrap();
        let mut bases = Vec::new();
        for (i, f) in encoded(&write).iter().enumerate() {
            assert!(f.len() <= ceiling, "ceiling {ceiling}: frame {i} is {} bytes", f.len());
            let ctrl = peek_control_block(f).unwrap();
            let items = decode_items(f, ctrl.body.clone(), ClientVerb::PushTxn).unwrap();
            bases.extend(items.iter().map(|item| item.hdr.arg0));
        }
        bases.dedup();
        assert_eq!(bases, [11, 12, 13], "ceiling {ceiling}");
    }
}

/// An empty PUSH cut into a frame goes as one frame would carry it: with no
/// data block.
#[test]
fn an_empty_push_goes_without_a_data_block() {
    let empty = ZSetBatch::new(&kv_schema(TypeCode::I64));
    let fams = [family(ClientVerb::Push, 7, WireConflictMode::Update, 0, &empty)];
    let write = SpanningWrite::cut(ClientVerb::Push, &fams, 1).unwrap();
    let frames = encoded(&write);
    assert_eq!(frames.len(), 1);
    assert!(peek_control_block(&frames[0]).unwrap().data.is_none());
}
