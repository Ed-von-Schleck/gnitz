use super::*;
use crate::runtime::reactor::reactor_with_rings;
use crate::runtime::test_support::key_producer;
use crate::runtime::worker::{send_span_train, ReplyRoute};
use crate::test_support::pk_only_schema;
use gnitz_wire::TypeCode;

/// Merge the trains the workers send for `partitions[w]`, two spans a frame.
fn merge(partitions: &[&[u64]]) -> Option<UniqueFilter> {
    let frame_schema = pk_only_schema(&[TypeCode::U64]);
    let (reactor, writers) = reactor_with_rings(partitions.len());
    let lease = reactor.lease_train(WorkerSet::ALL, SalMessageKind::KeySpans);
    let route = ReplyRoute {
        target_id: 1,
        request_id: lease.id(),
        fifo: false,
    };
    for (writer, keys) in writers.iter().zip(partitions) {
        let keys: Vec<[u8; 8]> = keys.iter().map(|k| k.to_be_bytes()).collect();
        let mut producer = key_producer(8, &keys);
        send_span_train(writer, route, &frame_schema, gnitz_wire::MAX_FRAME_PAYLOAD, |chunk| {
            producer.fill(chunk, 2);
            producer.remaining() == 0
        });
    }
    reactor.block_on(async move { merge_index_scan(&lease, &frame_schema).await.expect("no fault") })
}

/// An equal pair is a duplicate whether two workers hold it or one worker's
/// frames split it.
#[test]
fn merge_finds_a_duplicate_within_or_across_workers() {
    assert!(merge(&[&[1, 3, 7], &[2, 3]]).is_none(), "across workers");
    assert!(merge(&[&[1, 2, 2, 4], &[3]]).is_none(), "across one worker's frames");
}

/// A duplicate-free merge seeds the filter with exactly the distinct spans,
/// empty partitions included.
#[test]
fn a_duplicate_free_merge_seeds_every_span() {
    let seed = merge(&[&[1, 4, 6, 9, 11], &[], &[2, 3, 10]]).expect("no duplicate");
    let held = |k: u64| seed.may_contain(&k.to_be_bytes());
    for k in [1, 2, 3, 4, 6, 9, 10, 11] {
        assert!(held(k), "{k} is seeded");
    }
    for k in [0, 5, 7, 12] {
        assert!(!held(k), "{k} is not");
    }
}
