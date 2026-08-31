use super::*;
use gnitz_wire::control::DecodedControl;
use gnitz_wire::FLAG_EXCHANGE;

fn make_wire(view_id: i64, source_id: i64, with_schema: bool) -> DecodedWire {
    DecodedWire {
        control: DecodedControl {
            target_id: view_id as u64,
            flags: FLAG_EXCHANGE,
            seek_pk: source_id as u128,
            ..Default::default()
        },
        schema: if with_schema {
            Some(SchemaDescriptor::minimal_u64())
        } else {
            None
        },
        data_batch: None,
    }
}

#[test]
fn partial_round_returns_none() {
    let mut acc = ExchangeAccumulator::new(3);
    // First two of three workers report — round must stay pending.
    assert!(acc.process(0, make_wire(10, 0, true)).is_none());
    assert!(acc.process(1, make_wire(10, 0, false)).is_none());
    assert!(!acc.rounds.is_empty(), "partial round must remain in map");
}

#[test]
fn complete_round_returns_relay_with_correct_ids() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire(7, 3, true)).is_none());
    let relay = acc
        .process(1, make_wire(7, 3, false))
        .expect("complete round must return PendingRelay");
    assert_eq!(relay.view_id, 7);
    assert_eq!(relay.source_id, 3);
    assert_eq!(relay.payloads.len(), 2);
    assert!(acc.rounds.is_empty(), "completed round must be removed from map");
}

#[test]
fn schema_less_round_returns_none_and_cleans_up() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire(5, 0, false)).is_none());
    let result = acc.process(1, make_wire(5, 0, false));
    assert!(result.is_none(), "schema-less round must return None");
    assert!(acc.rounds.is_empty(), "completed schema-less round must not leak");
}

fn make_wire_pad(view_id: i64, source_id: i64, pad: bool, with_schema: bool) -> DecodedWire {
    let mut w = make_wire(view_id, source_id, with_schema);
    w.control.seek_col_idx = if pad { BACKFILL_PAD_BIT } else { 0 };
    w
}

#[test]
fn all_pad_is_and_of_worker_pad_bits() {
    // Every worker padded ⇒ the round is the final all-pad round.
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire_pad(1, 0, true, true)).is_none());
    let relay = acc
        .process(1, make_wire_pad(1, 0, true, false))
        .expect("round completes");
    assert!(relay.all_pad, "all workers padded ⇒ all_pad");

    // A single non-pad worker clears all_pad (backfill must continue).
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire_pad(2, 0, true, true)).is_none());
    let relay = acc
        .process(1, make_wire_pad(2, 0, false, false))
        .expect("round completes");
    assert!(!relay.all_pad, "a non-pad worker clears all_pad");

    // Steady-state exchanges pass seek_col_idx == 0 ⇒ all_pad false.
    let mut acc = ExchangeAccumulator::new(2);
    assert!(acc.process(0, make_wire(3, 0, true)).is_none());
    let relay = acc.process(1, make_wire(3, 0, false)).expect("round completes");
    assert!(!relay.all_pad, "steady-state (seek_col_idx==0) ⇒ all_pad false");
}

fn make_wire_src(view_id: i64, source_id: i64, req_id: u64) -> DecodedWire {
    let mut w = make_wire(view_id, source_id, true);
    w.control.request_id = req_id;
    w
}

/// A view with two sources opens one round per source: worker 0 reporting
/// for source A and worker 1 for source B completes neither.
#[test]
fn accumulator_distinguishes_source_ids() {
    let mut acc = ExchangeAccumulator::new(2);
    assert!(
        acc.process(0, make_wire_src(42, 100, 7)).is_none(),
        "source 100 has heard from worker 0 only"
    );
    assert!(
        acc.process(1, make_wire_src(42, 200, 8)).is_none(),
        "source 200 has heard from worker 1 only — the rounds must not merge"
    );
    let relay = acc
        .process(1, make_wire_src(42, 100, 9))
        .expect("source 100's round completes on its second worker");
    assert_eq!(relay.source_id, 100);
    assert_eq!(acc.rounds.len(), 1, "source 200's round is still open");
}
