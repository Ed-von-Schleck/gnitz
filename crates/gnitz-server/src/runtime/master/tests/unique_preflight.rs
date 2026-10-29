use super::*;
use crate::runtime::frame::WireMsg;
use crate::runtime::reactor::test_support::reactor_with_rings;
use crate::test_support::pk_only_schema;
use gnitz_wire::TypeCode;

/// Merge the trains the workers send for `partitions[w]`, two spans a frame and
/// a row-less terminal frame.
fn merge(partitions: &[&[u64]]) -> Option<UniqueFilter> {
    let frame_schema = pk_only_schema(&[TypeCode::U64]);
    let (reactor, mut writers) = reactor_with_rings(partitions.len());
    let lease = reactor.lease_train(WorkerSet::ALL, "KeySpans");
    let req = lease.targets().request_id;
    for (writer, keys) in writers.iter_mut().zip(partitions) {
        let mut spans = Batch::empty_with_schema(&frame_schema);
        for pair in keys.chunks(2) {
            spans.clear();
            for key in pair {
                spans.push_key_row(&key.to_be_bytes(), 1);
            }
            writer.send_msg(req, &WireMsg::train_frame(1, false, spans.wire_whole()));
        }
        writer.send_msg(req, &WireMsg::train_frame(1, true, None));
    }
    reactor
        .block_on(merge_index_scan(&lease, &frame_schema))
        .expect("no fault")
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
