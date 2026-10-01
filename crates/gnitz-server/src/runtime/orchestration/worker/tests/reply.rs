use super::send_unique_preflight_keys;
use crate::runtime::test_support::key_producer;
use crate::runtime::w2m::fixtures::make_ring;
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::wire::unique_preflight_wire_schema;
use crate::test_support::pk_only_schema;
use gnitz_wire::{TypeCode, WireStatus};
use gnitz_zset::schema::SchemaDescriptor;

/// Read one pre-flight train off `receiver`, checking the flag and schema
/// discipline the master's merge relies on: the spans, decoded against
/// `frame_schema`, and the train's frame count.
fn drain_train(receiver: &W2mReceiver, frame_schema: &SchemaDescriptor, request_id: u32) -> (Vec<Vec<u8>>, usize) {
    let mut keys = Vec::new();
    let mut frames = 0;
    loop {
        let slot = receiver.try_read_slot(0).expect("the train ends in a terminal frame");
        assert_eq!(slot.internal_req_id, request_id);
        let ctrl = slot.control();
        assert_eq!(ctrl.hdr.status, WireStatus::Ok);
        assert!(
            ctrl.hdr.flags.continuation,
            "every pre-flight frame carries continuation"
        );
        assert!(ctrl.schema.is_none(), "the master builds the frame schema itself");
        if let Some(data) = ctrl.data.clone() {
            let block = gnitz_zset::repr::WalBlock::parse(&slot.bytes()[data], frame_schema).expect("frame decodes");
            let mb = block.view();
            keys.extend((0..mb.len()).map(|i| mb.get_pk_bytes(i).to_vec()));
        }
        frames += 1;
        if ctrl.hdr.flags.scan_last {
            return (keys, frames);
        }
    }
}

/// A train carries every span verbatim, cut into frames by `chunk_rows` or the
/// byte budget, whichever is hit first, and ends at its last frame — one empty
/// terminal frame for an empty partition, and no trailing empty frame after a
/// full one.
#[test]
fn a_preflight_train_carries_every_span_and_ends_once() {
    // Two U64 index columns → a 16-byte composite span, derived as both ends do.
    let frame_schema = unique_preflight_wire_schema(&pk_only_schema(&[TypeCode::U64; 3]), 2);
    let extremes = [
        0u128,
        1,
        (i64::MAX as u64) as u128,
        u64::MAX as u128,
        (7u128 << 64) | 1,
        (7u128 << 64) | 2,
        u128::MAX - 1,
        u128::MAX,
    ];
    let by_budget: Vec<u128> = (0..24).collect();
    let full = gnitz_wire::MAX_FRAME_PAYLOAD;
    let cases: [(&[u128], usize, usize, std::ops::RangeInclusive<usize>); 4] = [
        (&extremes[..7], full, 3, 3..=3),
        (&extremes, full, 4, 2..=2),
        (&[], full, 4, 1..=1),
        (&by_budget, 400, 1024, 2..=24),
    ];
    for (keys, budget, chunk_rows, frames) in cases {
        let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_be_bytes().to_vec()).collect();
        // Room for a whole train, so the writer never parks on backpressure.
        let ptr = make_ring(1 << 16, 16, 8);
        let receiver = W2mReceiver::new(vec![ptr]);
        send_unique_preflight_keys(
            &W2mWriter::new(ptr),
            77,
            &frame_schema,
            9,
            budget,
            chunk_rows,
            &mut key_producer(16, &keys),
        );
        let (got, n) = drain_train(&receiver, &frame_schema, 9);
        assert_eq!(got, keys);
        assert!(frames.contains(&n), "{} spans: {n} frames", keys.len());
        assert!(
            receiver.try_read_slot(0).is_none(),
            "nothing follows the terminal frame"
        );
    }
}
