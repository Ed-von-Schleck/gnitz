use super::*;
use crate::runtime::reactor::reactor_with_rings;
use crate::runtime::test_support::key_producer;
use crate::runtime::worker::send_unique_preflight_keys;
use crate::test_support::pk_only_schema;
use gnitz_wire::TypeCode;

/// Merge the trains the workers send for `partitions[w]`, two spans a frame.
fn merge(partitions: &[&[u64]]) -> Option<UniqueFilter> {
    let frame_schema = pk_only_schema(&[TypeCode::U64]);
    let (reactor, writers) = reactor_with_rings(partitions.len());
    let lease = reactor.lease_train(WorkerSet::ALL, SalMessageKind::UniquePreflight);
    for (writer, keys) in writers.iter().zip(partitions) {
        let keys: Vec<[u8; 8]> = keys.iter().map(|k| k.to_be_bytes()).collect();
        let mut producer = key_producer(8, &keys);
        send_unique_preflight_keys(
            writer,
            1,
            &frame_schema,
            lease.id(),
            gnitz_wire::MAX_FRAME_PAYLOAD,
            2,
            &mut producer,
        );
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
    let mut seed = merge(&[&[1, 4, 6, 9, 11], &[], &[2, 3, 10]]).expect("no duplicate");
    seed.mark_warm();
    let absent = |k: u64| seed.proves_all_absent([&k.to_be_bytes()[..]].into_iter());
    for k in [1, 2, 3, 4, 6, 9, 10, 11] {
        assert!(!absent(k), "{k} is seeded");
    }
    for k in [0, 5, 7, 12] {
        assert!(absent(k), "{k} is not");
    }
}
