use super::*;
use gnitz_wire::control::DecodedControl;
use gnitz_wire::{FLAG_CONTINUATION, FLAG_EXCHANGE, FLAG_SCAN_LAST};

/// The one-U64-PK, zero-payload fixture every batch here is built over.
fn u64_pk_only() -> SchemaDescriptor {
    crate::test_support::pk_only_schema(&[gnitz_wire::type_code::U64])
}

/// One worker's TERMINAL exchange frame — the whole report when the partition
/// fits one frame. Only the first worker of a round carries a schema here; `pad`
/// is the backfill pad bit (steady-state exchanges leave `seek_col_idx` at 0,
/// which reads as not padded).
fn make_wire(view_id: i64, source_id: i64, with_schema: bool, pad: bool) -> DecodedWire {
    DecodedWire {
        control: DecodedControl {
            target_id: view_id as u64,
            flags: FLAG_EXCHANGE | FLAG_CONTINUATION | FLAG_SCAN_LAST,
            seek_pk: source_id as u128,
            seek_col_idx: if pad { BACKFILL_PAD_BIT } else { 0 },
            ..Default::default()
        },
        schema: with_schema.then(u64_pk_only),
        data_batch: None,
    }
}

/// A consolidated batch of `keys` over [`u64_pk_only`], as one
/// frame of a worker's exchange train carries.
fn chunk(keys: &[u64]) -> Batch {
    let schema = u64_pk_only();
    let mut b = Batch::with_capacity(&schema, keys.len());
    for k in keys {
        b.push_key_row(&k.to_be_bytes(), 1);
    }
    b.certify_layout(Layout::Consolidated);
    b
}

/// One frame of a worker's exchange train, carrying `keys`. Every frame carries
/// the schema block (the ring decode takes no hint) and its own layout claim;
/// only the terminal one carries the round's bookkeeping.
fn make_frame(view_id: i64, source_id: i64, keys: &[u64], last: bool) -> DecodedWire {
    DecodedWire {
        control: DecodedControl {
            target_id: view_id as u64,
            flags: FLAG_EXCHANGE
                | FLAG_CONTINUATION
                | gnitz_wire::FLAG_BATCH_CONSOLIDATED
                | if last { FLAG_SCAN_LAST } else { 0 },
            seek_pk: source_id as u128,
            ..Default::default()
        },
        schema: Some(u64_pk_only()),
        data_batch: Some(chunk(keys)),
    }
}

/// A round completes on the last worker of its `(view, source)` and not before,
/// carrying that round's ids; `all_pad` is the AND of the workers' pad bits, so
/// one unpadded worker keeps the backfill going.
#[test]
fn round_completes_on_the_last_worker_with_all_pad_anded() {
    for (pads, want_all_pad) in [([true, true], true), ([true, false], false), ([false, false], false)] {
        let mut acc = ExchangeAccumulator::new(2);
        assert!(
            acc.process(0, make_wire(7, 3, true, pads[0])).is_none(),
            "one of two workers"
        );
        let relay = acc
            .process(1, make_wire(7, 3, false, pads[1]))
            .expect("the last worker completes the round");
        assert_eq!((relay.view_id, relay.source_id, relay.all_pad), (7, 3, want_all_pad));
    }
}

/// A round nobody sent a schema for is dropped rather than relayed — and dropped
/// whole: the same `(view, source)` opens a fresh round afterwards.
#[test]
fn schema_less_round_is_dropped_and_the_key_reopens() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire(5, 0, false, false)).is_none());
    assert!(
        acc.process(1, make_wire(5, 0, false, false)).is_none(),
        "a schema-less round must not relay"
    );
    assert!(acc.process(0, make_wire(5, 0, true, false)).is_none());
    assert!(
        acc.process(1, make_wire(5, 0, false, false)).is_some(),
        "the dropped round left nothing behind for the next one to complete against"
    );
}

/// A view with two sources opens one round per source: worker 0 reporting for
/// source A and worker 1 for source B completes neither.
#[test]
fn rounds_are_keyed_by_source_id() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire(42, 100, true, false)).is_none());
    assert!(
        acc.process(1, make_wire(42, 200, true, false)).is_none(),
        "a different source's worker must not complete source 100's round"
    );
    assert_eq!(
        acc.process(1, make_wire(42, 100, false, false))
            .expect("source 100's round completes on its second worker")
            .source_id,
        100
    );
    assert_eq!(
        acc.process(0, make_wire(42, 200, false, false))
            .expect("source 200's round was still open")
            .source_id,
        200
    );
}

/// Only a train's terminal frame counts: intermediate frames add payload and
/// leave the round open, the slot accumulates them in frame order, and the
/// concatenation keeps the `Consolidated` claim — losing which would silently
/// drop the round onto the re-sorting repartition.
#[test]
fn a_workers_train_completes_only_on_its_terminal_frame() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(
        acc.process(0, make_frame(7, 3, &[1, 2], false)).is_none(),
        "an intermediate frame must not report the worker"
    );
    assert!(
        acc.process(1, make_frame(7, 3, &[10, 11], true)).is_none(),
        "worker 1 is done, but worker 0's train is still open"
    );
    let relay = acc
        .process(0, make_frame(7, 3, &[5, 6], true))
        .expect("worker 0's terminal frame completes the round");

    let w0 = relay.payloads[0].as_ref().expect("worker 0's accumulated partition");
    assert_eq!(w0.len(), 4, "both frames' rows land in the slot");
    let keys: Vec<u64> = (0..w0.len())
        .map(|i| u64::from_be_bytes(w0.get_pk_bytes(i).try_into().unwrap()))
        .collect();
    assert_eq!(keys, vec![1, 2, 5, 6], "in frame order, which is source-row order");
    assert_eq!(
        w0.layout(),
        Layout::Consolidated,
        "the claim every frame carried must survive the concatenation"
    );
}

/// A worker whose train carries one `Raw` frame must NOT come back certified:
/// the claim is the AND over its frames, not the accumulated batch's own
/// `is_consolidated()`, which any one-row batch answers `true` to.
#[test]
fn one_unconsolidated_frame_clears_the_slots_claim() {
    let mut acc = ExchangeAccumulator::new(1);
    let mut raw = make_frame(7, 3, &[1], false);
    raw.control.flags &= !gnitz_wire::FLAG_BATCH_CONSOLIDATED;
    raw.data_batch = Some({
        let mut b = chunk(&[1]);
        b.certify_layout(Layout::Raw);
        b
    });
    assert!(acc.process(0, raw).is_none());
    let relay = acc
        .process(0, make_frame(7, 3, &[2], true))
        .expect("the terminal frame completes the round");
    assert_eq!(
        relay.payloads[0].as_ref().unwrap().layout(),
        Layout::Raw,
        "one unclaimed frame leaves the whole slot unclaimed"
    );
}
