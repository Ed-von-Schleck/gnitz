//! Unit tests for the CREATE UNIQUE INDEX pre-flight building blocks: the
//! worker's sorted-span frame train (`send_unique_preflight_keys`), the
//! per-row span projection, and the master's per-span merge accounting. The
//! full distributed path (fan-out, k-way merge over live W2M trains, DDL
//! integration) is covered by the multi-worker E2E suite.
//!
//! Every key is the OPK leading-key span (`PkBuf`) — equality-correct and
//! byte-orderable at any width.

use crate::runtime::w2m::fixtures::make_ring;
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::wire::{self, unique_preflight_wire_schema};
use crate::runtime::worker::{preflight_frame_overhead, preflight_keys_per_frame, send_unique_preflight_keys};
use crate::test_support::pk_only_schema;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::make_index_schema;
use gnitz_store::schema::{IndexKeySpec, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder, KeyProducer, SpillSort};
use gnitz_wire::control::peek_control_block_ipc;
use gnitz_wire::type_code;
use gnitz_wire::WireStatus;

// ---------------------------------------------------------------------------
// Span helpers
// ---------------------------------------------------------------------------

/// OPK leading-key span of a single U128 value (U128 OPK == big-endian). The
/// 16-byte span the round-trip tests below ship over the wire.
fn span_u128(v: u128) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
}

/// OPK leading-key span of a single promoted-I64 value (order-preserving
/// big-endian with the sign bit flipped) — what an I64 indexed column projects.
fn span_i64(v: i64) -> PkBuf {
    let mut b = [0u8; 8];
    gnitz_wire::encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut b);
    PkBuf::from_bytes(&b)
}

/// Frame schema of a single-U128-span pre-flight reply (one U128 PK column),
/// so the round-trip tests ship a 16-byte PK span per row. Index columns are
/// all non-nullable, which is what makes it an all-PK schema.
fn u128_frame_schema() -> SchemaDescriptor {
    pk_only_schema(&[type_code::U128])
}

/// A reply budget that admits exactly `n` keys per pre-flight frame: the
/// frame's own overhead — control block, schema block, empty data block — plus
/// `n` rows' worth of data. Written through the production accounting, so a
/// test cannot pin a per-frame key count the emitter would not itself choose.
fn budget_for(frame_schema: &SchemaDescriptor, target_id: u64, n: usize) -> usize {
    let block = crate::catalog::encode_schema_block_ipc(frame_schema, target_id as u32);
    preflight_frame_overhead(frame_schema, &block) + n * per_key(frame_schema)
}

/// The data block's growth for one key — what `preflight_keys_per_frame`
/// divides the row budget by.
fn per_key(frame_schema: &SchemaDescriptor) -> usize {
    gnitz_store::storage::wire_block_size(frame_schema, 1, 0)
        - gnitz_store::storage::wire_block_size(frame_schema, 0, 0)
}

/// Build the real sorted-span producer over pre-sorted `keys` via
/// `SpillSort`'s in-RAM fast path (budget far above the data, so the spill
/// dir is never touched) — the same producer `handle_unique_preflight` feeds
/// to the frame sink.
fn producer_of(keys: &[PkBuf]) -> KeyProducer {
    let stride = keys.first().map_or(16, |k| k.pk_bytes().len());
    let mut s = SpillSort::new("", stride, usize::MAX);
    for k in keys {
        s.push(k.pk_bytes()).unwrap();
    }
    s.finish().unwrap()
}

// ---------------------------------------------------------------------------
// Frame-train round-trip
// ---------------------------------------------------------------------------

fn with_test_ring(f: impl FnOnce(&W2mWriter, &W2mReceiver)) {
    // Room for a whole pre-flight train at once, so a test drains it without
    // ever racing the writer against backpressure.
    let region = unsafe { make_ring(1 << 16, 16, 8) };
    let ptr = region.ptr();
    let writer = W2mWriter::new(ptr);
    let receiver = W2mReceiver::new(vec![ptr]);
    f(&writer, &receiver);
}

/// Drain every frame of one pre-flight train from the ring, asserting the
/// flag/schema discipline the master's merge relies on, and return the spans
/// decoded the way the merge decodes them: the whole PK region of each row
/// (`get_pk_bytes` → `PkBuf`), continuations against the saved schema hint.
fn drain_train(receiver: &W2mReceiver, expected_req_id: u64) -> Vec<PkBuf> {
    let mut keys = Vec::new();
    let mut saved_schema: Option<SchemaDescriptor> = None;
    let mut frames = 0usize;
    loop {
        let slot = receiver.try_read_slot(0).expect("frame missing from train");
        assert_eq!(
            slot.internal_req_id, expected_req_id as u32,
            "ring prefix must carry the request id",
        );
        let ctrl = peek_control_block_ipc(slot.bytes()).expect("ctrl decodes");
        assert_eq!(ctrl.status, WireStatus::Ok);
        assert!(ctrl.flags.continuation, "every pre-flight frame carries continuation");
        let last = ctrl.flags.scan_last;
        if frames == 0 {
            assert!(
                ctrl.flags.has_schema,
                "first frame must carry the synthetic schema block"
            );
        } else {
            assert!(
                !ctrl.flags.has_schema,
                "continuation frames must not re-send the schema"
            );
        }
        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl, saved_schema.as_ref(), &mut offsets)
            .expect("frame decodes");
        if saved_schema.is_none() {
            let s = zc.schema.expect("first frame schema");
            // The reply schema's PK region IS the OPK leading-key span; every
            // column is in the PK, no payload columns.
            assert!(s.num_columns() >= 1);
            assert_eq!(s.pk_indices().len(), s.num_columns());
            saved_schema = Some(s);
        }
        if let Some(ref mb) = zc.data_batch {
            for i in 0..mb.len() {
                keys.push(PkBuf::from_bytes(mb.get_pk_bytes(i)));
            }
        }
        frames += 1;
        drop(zc);
        drop(slot);
        if last {
            break;
        }
    }
    keys
}

/// Multi-frame train: spans split across frames at `keys_per_frame`, the
/// terminal frame tagged `scan_last`, and every span — including extreme
/// u128s — round-trips through the wire to the exact byte span.
#[test]
fn preflight_train_multi_frame_key_roundtrip() {
    let keys: Vec<PkBuf> = [
        0u128,
        1,
        41,
        42,
        (i64::MAX as u64) as u128,
        ((-2i64) as u64) as u128,
        ((-1i64) as u64) as u128,
        u128::MAX - 1,
        u128::MAX,
    ]
    .into_iter()
    .map(span_u128)
    .collect();
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(
            writer,
            77,
            &frame_schema,
            9001,
            budget_for(&frame_schema, 77, 4),
            &mut producer_of(&keys),
        );
        let got = drain_train(receiver, 9001);
        assert_eq!(got, keys);
        assert!(receiver.try_read_slot(0).is_none(), "no frames after terminal");
    });
}

/// A train whose span count is an exact multiple of the frame size must not
/// emit a trailing empty frame: the last full frame is the terminal one.
#[test]
fn preflight_train_exact_frame_boundary() {
    let keys: Vec<PkBuf> = (0..8u128).map(span_u128).collect();
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(
            writer,
            77,
            &frame_schema,
            42,
            budget_for(&frame_schema, 77, 4),
            &mut producer_of(&keys),
        );
        let got = drain_train(receiver, 42);
        assert_eq!(got, keys);
        assert!(receiver.try_read_slot(0).is_none());
    });
}

/// An empty partition still answers with exactly one empty terminal frame so
/// the master's drain sees the train end.
#[test]
fn preflight_train_empty_partition_single_terminal_frame() {
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(
            writer,
            77,
            &frame_schema,
            7,
            budget_for(&frame_schema, 77, 4),
            &mut producer_of(&[]),
        );
        let slot = receiver.try_read_slot(0).expect("terminal frame");
        let ctrl = peek_control_block_ipc(slot.bytes()).expect("ctrl decodes");
        assert_eq!(ctrl.status, WireStatus::Ok);
        assert!(ctrl.flags.scan_last, "single frame must be terminal");
        assert!(!ctrl.flags.has_data, "no data on empty train");
        drop(slot);
        assert!(receiver.try_read_slot(0).is_none());
    });
}

/// The byte clamp, not the key count, is what bounds a pre-flight frame: at a
/// small reply budget `preflight_keys_per_frame` cuts the configured count down
/// to what fits, and the train it produces spans several frames.
///
/// The budget charges the frame's own overhead on top of the eight keys — a
/// budget of eight keys' *rows* alone would leave room for none of them, which
/// is exactly the shortfall this accounting closes.
#[test]
fn preflight_frames_are_cut_by_the_byte_budget() {
    let frame_schema = u128_frame_schema();
    // 16 B span + 8 B weight + 8 B null word per key.
    assert_eq!(per_key(&frame_schema), 32);
    let block = crate::catalog::encode_schema_block_ipc(&frame_schema, 77);
    let overhead = preflight_frame_overhead(&frame_schema, &block);
    let budget = overhead + 8 * 32;
    assert_eq!(
        preflight_keys_per_frame(&frame_schema, budget, overhead),
        8,
        "the budget, not the configured 1<<20 count, must decide the frame's keys"
    );

    let keys: Vec<PkBuf> = (0..24u128).map(span_u128).collect();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 77, &frame_schema, 11, budget, &mut producer_of(&keys));
        let got = drain_train(receiver, 11);
        assert_eq!(got, keys, "the clamped train still carries every span in order");
    });
}

/// The frame overhead must be charged, not assumed away: a budget of exactly
/// eight keys' rows leaves room for none once the control and schema blocks are
/// counted, so the clamp floors at one key rather than emitting an over-budget
/// frame.
#[test]
fn preflight_keys_per_frame_charges_the_frame_overhead() {
    let frame_schema = u128_frame_schema();
    let block = crate::catalog::encode_schema_block_ipc(&frame_schema, 77);
    let overhead = preflight_frame_overhead(&frame_schema, &block);
    assert!(overhead > 0, "a frame's control and schema blocks cost bytes");
    assert_eq!(preflight_keys_per_frame(&frame_schema, 8 * 32, overhead), 1);
}

/// A composite two-column span round-trips through the wire frame: the reply
/// schema's PK region holds the full span verbatim, however many columns the
/// index spans.
#[test]
fn preflight_train_composite_wide_span_roundtrip() {
    // Two U64 index columns → a 16-byte composite leading span, plus a U64
    // source PK; the frame schema is derived exactly as both endpoints do.
    let idx_schema = pk_only_schema(&[type_code::U64; 3]);
    let frame_schema = unique_preflight_wire_schema(&idx_schema, 2);
    // Spans are (a_be ++ b_be), 16 bytes. Two of them share their leading 8
    // bytes and differ only in the trailing column, so the span must carry both
    // columns to keep them distinct.
    let span = |a: u64, b: u64| {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_be_bytes());
        buf[8..].copy_from_slice(&b.to_be_bytes());
        PkBuf::from_bytes(&buf)
    };
    let keys = vec![span(7, 1), span(7, 2), span(9, 1)];
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(
            writer,
            5,
            &frame_schema,
            3,
            budget_for(&frame_schema, 5, 2),
            &mut producer_of(&keys),
        );
        let got = drain_train(receiver, 3);
        assert_eq!(got, keys);
        assert_eq!(got[0].pk_bytes().len(), 16, "composite span is the full 16 bytes");
    });
}

// ---------------------------------------------------------------------------
// Worker projection: IndexKeySpec::key_bytes feeding the train
// ---------------------------------------------------------------------------

/// Mirror of the worker's per-chunk projection loop: build each positive-weight
/// row's OPK span via `IndexKeySpec::key_bytes`, emitting it once per unit of
/// weight (capped at a pair), skipping any-NULL rows. Returns the SORTED spans.
fn project_sorted(batch: &Batch, owner: &SchemaDescriptor, cols: &[u32]) -> Vec<PkBuf> {
    let spec = IndexKeySpec::new(cols, owner).unwrap();
    let mb = batch.as_mem_batch();
    let mut keys: Vec<PkBuf> = Vec::new();
    let mut keybuf = PkBuf::zeroed(0);
    for row in 0..batch.len() {
        let w = batch.get_weight(row);
        if w <= 0 {
            continue;
        }
        if !spec.key_bytes(&mb, row, &mut keybuf) {
            continue;
        }
        keys.push(keybuf);
        if w > 1 {
            keys.push(keybuf);
        }
    }
    keys.sort_unstable();
    keys
}

/// The worker projection (`IndexKeySpec::key_bytes` on the owner schema) and the wire
/// round-trip compose to preserve spans end-to-end for a signed payload column:
/// equal signed values (including negatives) produce equal spans on the master
/// side, and retractions and NULLs never enter the stream.
#[test]
fn preflight_signed_payload_projection_roundtrip() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    // (pk, val, weight): two rows share val=-5 (the duplicate the pre-flight
    // exists to catch), one NULL, one retracted row.
    let rows: [(u128, Option<i64>, i64); 6] = [
        (1, Some(-5), 1),
        (2, Some(300), 1),
        (3, Some(-5), 1),
        (4, None, 1),     // NULL val: skipped
        (5, Some(7), -1), // retracted: skipped
        (6, Some(i64::MIN), 1),
    ];
    let mut bb = BatchBuilder::new(schema);
    for &(pk, val, weight) in &rows {
        bb.begin_row(pk, weight);
        match val {
            Some(v) => bb.put_int(v as u128),
            None => bb.put_null(),
        }
        bb.end_row();
    }
    let batch = bb.finish();

    let keys = project_sorted(&batch, &schema, &[1]);

    // Expected spans: the signed I64-promoted OPK of the four non-NULL,
    // non-retracted values, sorted byte-lex. The signed promotion makes
    // negatives sort below non-negatives — i64::MIN first, 300 last.
    let expected: Vec<PkBuf> = {
        let mut v: Vec<PkBuf> = [-5i64, 300, -5, i64::MIN].iter().map(|&x| span_i64(x)).collect();
        v.sort_unstable();
        v
    };
    assert_eq!(keys, expected, "projection must be the order-preserving signed I64 OPK");

    let frame_schema = unique_preflight_wire_schema(&make_index_schema(&[1], &schema).unwrap(), 1);
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(
            writer,
            5,
            &frame_schema,
            11,
            budget_for(&frame_schema, 5, 3),
            &mut producer_of(&keys),
        );
        let got = drain_train(receiver, 11);
        assert_eq!(got, keys);
        // The duplicate pair is adjacent in the sorted stream — exactly what the
        // master's prev == popped check rejects.
        let dup = span_i64(-5);
        assert_eq!(got.iter().filter(|&&k| k == dup).count(), 2);
    });
}

/// A consolidated row at weight 2 is the same (PK, payload) twice: the worker
/// collection contract (one span per unit of weight, capped at a pair) makes
/// the multiplicity visible to the merge as an adjacent equal pair, and the
/// accumulator's verdict is duplicate.
#[test]
fn preflight_weight2_row_emits_adjacent_pair() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let rows: [(u128, i64, i64); 3] = [
        (1, 7, 1),
        (2, 9, 2), // consolidated duplicate: weight 2
        (3, 11, 1),
    ];
    let mut bb = BatchBuilder::new(schema);
    for &(pk, val, weight) in &rows {
        bb.begin_row(pk, weight);
        bb.put_int(val as u128);
        bb.end_row();
    }
    let batch = bb.finish();

    let keys = project_sorted(&batch, &schema, &[1]);
    assert_eq!(
        keys,
        vec![span_i64(7), span_i64(9), span_i64(9), span_i64(11)],
        "weight-2 row must emit its span twice"
    );
}

/// A composite `UNIQUE (a, b)` span packs both columns, so two rows that share
/// column `a` and differ in `b` produce distinct spans and are both admitted.
#[test]
fn preflight_composite_projection_distinguishes_trailing_column() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // a
            SchemaColumn::new(type_code::U64, 0), // b
        ],
        &[0],
    );
    // (pk, a, b): (10,7,1) and (11,7,2) share a=7 but differ in b → distinct.
    let mut bb = BatchBuilder::new(schema);
    for &(pk, a, b) in &[(10u128, 7u128, 1u128), (11, 7, 2)] {
        bb.begin_row(pk, 1);
        bb.put_int(a);
        bb.put_int(b);
        bb.end_row();
    }
    let batch = bb.finish();
    let keys = project_sorted(&batch, &schema, &[1, 2]);
    assert_eq!(keys.len(), 2);
    assert_ne!(
        keys[0], keys[1],
        "rows differing only in the trailing column are distinct"
    );
    assert_eq!(keys[0].pk_bytes().len(), 16, "composite span spans both columns");
}

// ---------------------------------------------------------------------------
// Merge accounting: verdict + all-or-nothing seed
// ---------------------------------------------------------------------------
