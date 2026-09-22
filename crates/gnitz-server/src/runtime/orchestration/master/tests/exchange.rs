use super::*;
use gnitz_store::storage::Layout;
use gnitz_wire::control::{ControlHeader, DecodedControl};
use gnitz_wire::WireFlags;

/// The one-U64-PK, zero-payload fixture every batch here is built over.
fn u64_pk_only() -> SchemaDescriptor {
    crate::test_support::pk_only_schema(&[gnitz_wire::type_code::U64])
}

/// An exchange frame's control header, as a worker stamps it.
fn control(view_id: i64, source_id: i64, flags: WireFlags) -> DecodedControl {
    DecodedControl {
        hdr: ControlHeader {
            target_id: view_id as u64,
            flags,
            arg0: source_id as u64,
            ..Default::default()
        },
        blob: Vec::new(),
        body: 0..0,
        schema: None,
        data: None,
    }
}

/// One worker's TERMINAL exchange frame — the whole report when the partition
/// fits one frame. `pad` is the backfill pad bit (steady-state exchanges leave `flags.backfill_pad`
/// clear).
fn make_wire(view_id: i64, source_id: i64, pad: bool) -> DecodedWire {
    DecodedWire {
        control: control(
            view_id,
            source_id,
            WireFlags {
                backfill_pad: pad,
                ..WireFlags::train_frame(0, true)
            },
        ),
        schema: Some(u64_pk_only()),
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
        control: control(
            view_id,
            source_id,
            WireFlags {
                batch_consolidated: true,
                ..WireFlags::train_frame(0, last)
            },
        ),
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
        assert!(acc.process(0, make_wire(7, 3, pads[0])).is_none(), "one of two workers");
        let relay = acc
            .process(1, make_wire(7, 3, pads[1]))
            .expect("the last worker completes the round");
        assert_eq!((relay.view_id, relay.source_id, relay.all_pad), (7, 3, want_all_pad));
    }
}

/// A view with two sources opens one round per source: worker 0 reporting for
/// source A and worker 1 for source B completes neither.
#[test]
fn rounds_are_keyed_by_source_id() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire(42, 100, false)).is_none());
    assert!(
        acc.process(1, make_wire(42, 200, false)).is_none(),
        "a different source's worker must not complete source 100's round"
    );
    assert_eq!(
        acc.process(1, make_wire(42, 100, false))
            .expect("source 100's round completes on its second worker")
            .source_id,
        100
    );
    assert_eq!(
        acc.process(0, make_wire(42, 200, false))
            .expect("source 200's round was still open")
            .source_id,
        200
    );
}

/// Only a train's terminal frame counts: intermediate frames add payload and
/// leave the round open, and the worker's list keeps them in frame order.
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

    let keys: Vec<u64> = relay.payloads[0]
        .iter()
        .flat_map(|f| (0..f.len()).map(|i| u64::from_be_bytes(f.get_pk_bytes(i).try_into().unwrap())))
        .collect();
    assert_eq!(relay.payloads[0].len(), 2, "both frames land in the worker's list");
    assert_eq!(keys, vec![1, 2, 5, 6], "in frame order, which is source-row order");
}
