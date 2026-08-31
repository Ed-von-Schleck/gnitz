use super::*;
use gnitz_wire::control::DecodedControl;
use gnitz_wire::FLAG_EXCHANGE;

/// One worker's exchange reply. Only the first worker of a round carries a
/// schema; `pad` is the backfill pad bit (steady-state exchanges leave
/// `seek_col_idx` at 0, which reads as not padded).
fn make_wire(view_id: i64, source_id: i64, with_schema: bool, pad: bool) -> DecodedWire {
    DecodedWire {
        control: DecodedControl {
            target_id: view_id as u64,
            flags: FLAG_EXCHANGE,
            seek_pk: source_id as u128,
            seek_col_idx: if pad { BACKFILL_PAD_BIT } else { 0 },
            ..Default::default()
        },
        schema: with_schema.then(SchemaDescriptor::minimal_u64),
        data_batch: None,
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
