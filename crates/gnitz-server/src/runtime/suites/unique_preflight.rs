//! Unit tests for the CREATE UNIQUE INDEX pre-flight building blocks: the
//! worker's sorted-span frame train (`send_unique_preflight_keys`) and the
//! master's per-span merge accounting.
//!
//! Every key is the OPK leading-key span (`PkBuf`) — equality-correct and
//! byte-orderable at any width.

use crate::runtime::w2m::fixtures::make_ring;
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::wire::{unique_preflight_wire_schema, FRAME_CAP};
use crate::runtime::worker::send_unique_preflight_keys;
use crate::test_support::pk_only_schema;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::{KeyProducer, SpillSort};
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

/// Frame schema of a single-U128-span pre-flight reply (one U128 PK column),
/// so the round-trip tests ship a 16-byte PK span per row. Index columns are
/// all non-nullable, which is what makes it an all-PK schema.
fn u128_frame_schema() -> SchemaDescriptor {
    pk_only_schema(&[type_code::U128])
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
/// (`get_pk_bytes` → `PkBuf`), every frame against `frame_schema`, with the
/// train's frame count.
fn drain_train(receiver: &W2mReceiver, frame_schema: &SchemaDescriptor, expected_req_id: u64) -> (Vec<PkBuf>, usize) {
    let mut keys = Vec::new();
    let mut frames = 0;
    loop {
        let slot = receiver.try_read_slot(0).expect("frame missing from train");
        assert_eq!(
            slot.internal_req_id, expected_req_id as u32,
            "ring prefix must carry the request id",
        );
        let ctrl = slot.control();
        assert_eq!(ctrl.hdr.status, WireStatus::Ok);
        assert!(
            ctrl.hdr.flags.continuation,
            "every pre-flight frame carries continuation"
        );
        let last = ctrl.hdr.flags.scan_last;
        assert!(
            ctrl.schema.is_none(),
            "no pre-flight frame carries a schema block: the master builds it"
        );
        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        if let Some(data) = ctrl.data.clone() {
            let mb =
                gnitz_store::storage::decode_mem_batch_from_wal_block(&slot.bytes()[data], frame_schema, &mut offsets)
                    .expect("frame decodes");
            for i in 0..mb.len() {
                keys.push(PkBuf::from_bytes(mb.get_pk_bytes(i)));
            }
        }
        drop(slot);
        frames += 1;
        if last {
            break;
        }
    }
    (keys, frames)
}

/// Multi-frame train: spans split across frames at `chunk_rows`, the
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
        send_unique_preflight_keys(writer, 77, &frame_schema, 9001, FRAME_CAP, 4, &mut producer_of(&keys));
        let (got, frames) = drain_train(receiver, &frame_schema, 9001);
        assert_eq!(frames, 3, "9 keys at 4 per chunk");
        assert_eq!(got, keys);
        assert!(receiver.try_read_slot(0).is_none(), "no frames after terminal");
    });
}

/// A train whose span count is an exact multiple of `chunk_rows` must not
/// emit a trailing empty frame: the last full frame is the terminal one.
#[test]
fn preflight_train_exact_frame_boundary() {
    let keys: Vec<PkBuf> = (0..8u128).map(span_u128).collect();
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 77, &frame_schema, 42, FRAME_CAP, 4, &mut producer_of(&keys));
        let (got, frames) = drain_train(receiver, &frame_schema, 42);
        assert_eq!(frames, 2, "no trailing empty frame");
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
        send_unique_preflight_keys(writer, 77, &frame_schema, 7, FRAME_CAP, 4, &mut producer_of(&[]));
        let slot = receiver.try_read_slot(0).expect("terminal frame");
        let ctrl = slot.control();
        assert_eq!(ctrl.hdr.status, WireStatus::Ok);
        assert!(ctrl.hdr.flags.scan_last, "single frame must be terminal");
        assert!(ctrl.data.is_none(), "no data on empty train");
        drop(slot);
        assert!(receiver.try_read_slot(0).is_none());
    });
}

/// `budget`, not `chunk_rows`, cuts this train into frames.
#[test]
fn preflight_frames_are_cut_by_the_byte_budget() {
    let frame_schema = u128_frame_schema();
    let keys: Vec<PkBuf> = (0..24u128).map(span_u128).collect();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 77, &frame_schema, 11, 400, 1024, &mut producer_of(&keys));
        let (got, frames) = drain_train(receiver, &frame_schema, 11);
        assert!(frames > 1, "the budget, not the chunk, cut this train");
        assert_eq!(got, keys);
    });
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
        send_unique_preflight_keys(writer, 5, &frame_schema, 3, FRAME_CAP, 2, &mut producer_of(&keys));
        let (got, _) = drain_train(receiver, &frame_schema, 3);
        assert_eq!(got, keys);
        assert_eq!(got[0].pk_bytes().len(), 16, "composite span is the full 16 bytes");
    });
}

// ---------------------------------------------------------------------------
// Merge accounting: verdict + all-or-nothing seed
// ---------------------------------------------------------------------------
