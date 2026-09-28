use super::*;
use gnitz_store::storage::Layout;
use gnitz_wire::control::{ControlHeader, DecodedControl};
use gnitz_wire::WireFlags;

/// The one-U64-PK, zero-payload fixture every batch here is built over.
fn u64_pk_only() -> SchemaDescriptor {
    crate::test_support::pk_only_schema(&[gnitz_wire::TypeCode::U64])
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
        blob: 0..0,
        body: 0..0,
        schema: None,
        data: None,
    }
}

/// One worker's TERMINAL exchange frame — the whole report when the partition
/// fits one frame. `drained` is the worker's drained bit (steady-state exchanges
/// leave it clear).
fn make_wire(view_id: i64, source_id: i64, drained: bool) -> DecodedWire {
    DecodedWire {
        control: control(
            view_id,
            source_id,
            WireFlags {
                scan_last: true,
                drained,
                ..Default::default()
            },
        ),
        blob: Vec::new(),
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
                scan_last: last,
                batch_consolidated: true,
                ..Default::default()
            },
        ),
        blob: Vec::new(),
        schema: Some(u64_pk_only()),
        data_batch: Some(chunk(keys)),
    }
}

/// A round completes on the last worker and not before, carrying that round's
/// ids; `drained` is the AND of the workers' bits, so one undrained worker keeps
/// the backfill going.
#[test]
fn round_completes_on_the_last_worker_with_drained_anded() {
    for (bits, want_drained) in [([true, true], true), ([true, false], false), ([false, false], false)] {
        let (f0, f1) = (make_wire(7, 3, bits[0]), make_wire(7, 3, bits[1]));
        let mut r = ExchangeRound::open(&f0, 2);
        assert!(!r.accept(0, f0), "one of two workers");
        assert!(r.accept(1, f1), "the last worker completes the round");
        assert_eq!((r.view_id, r.source_id, r.drained), (7, 3, want_drained));
    }
}

/// Only a train's terminal frame counts: intermediate frames add payload and
/// leave the round open, and the worker's list keeps them in frame order.
#[test]
fn a_workers_train_completes_only_on_its_terminal_frame() {
    let first = make_frame(7, 3, &[1, 2], false);
    let mut r = ExchangeRound::open(&first, 2);
    assert!(!r.accept(0, first), "an intermediate frame must not report the worker");
    assert!(
        !r.accept(1, make_frame(7, 3, &[10, 11], true)),
        "worker 1 is done, but worker 0's train is still open"
    );
    assert!(
        r.accept(0, make_frame(7, 3, &[5, 6], true)),
        "worker 0's terminal frame completes the round"
    );

    let keys: Vec<u64> = r.frames[0]
        .iter()
        .flat_map(|f| (0..f.len()).map(|i| u64::from_be_bytes(f.get_pk_bytes(i).try_into().unwrap())))
        .collect();
    assert_eq!(r.frames[0].len(), 2, "both frames land in the worker's list");
    assert_eq!(keys, vec![1, 2, 5, 6], "in frame order, which is source-row order");
}
