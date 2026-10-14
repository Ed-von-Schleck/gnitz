use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, within};

/// Worker `w`'s park word.
pub(super) fn word(parks: WorkerParks, w: usize) -> &'static Park {
    &parks.0[w].0
}

/// An arm taken after a publish sees it, and survives it: only a later publish
/// takes the arm.
#[test]
fn a_publish_before_an_arm_leaves_it_armed() {
    let park = Park(AtomicU64::new(0));
    park.publish(8, "test");
    assert_eq!(park.arm(), 8);
    assert!(park.armed());
    park.publish(16, "test");
    assert!(!park.armed());
    assert_eq!(park.value(), 16);
}

/// A park whose re-test behind the arm finds a group returns without sleeping,
/// and leaves the park disarmed.
#[test]
fn a_park_returns_at_once_when_its_retest_finds_a_group() {
    let parks = WorkerParks::create(1).unwrap();
    let mut armed_when_tested = false;
    parks.park(0).park(|| {
        armed_when_tested = word(parks, 0).armed();
        false
    });
    assert!(armed_when_tested, "the re-test runs behind the arm");
    assert!(!word(parks, 0).armed(), "and the park disarms");
}

/// A worker parked on an empty SAL wakes on another process's wake.
#[test]
fn a_parked_worker_wakes_on_a_forked_masters_wake() {
    let parks = WorkerParks::create(1).unwrap();
    let child = || {
        while !word(parks, 0).armed() {
            std::thread::yield_now();
        }
        parks.wake(0);
    };
    let pid = unsafe { fork_child(child) };
    within(move || {
        // The wake sequence stands in for the SAL the worker re-tests.
        parks.park(0).park(|| parks.wake_seq(0) == 0);
        assert_eq!(parks.wake_seq(0), 1, "the park ended on the wake");
    });
    unsafe { assert_child_exited_ok(pid) };
}

/// Membership, the launched-worker bound and rank, at a small worker count and
/// at the full word.
#[test]
fn a_worker_set_bounds_to_the_launched_workers() {
    let s = WorkerSet::one(1).with(3).with(6);
    assert_eq!(s.len(), 3);
    assert!(s.contains(3) && !s.contains(2));
    assert_eq!(s.without(3), WorkerSet::one(1).with(6));
    assert_eq!(s.within(4), WorkerSet::one(1).with(3));
    assert_eq!(s.iter().collect::<Vec<_>>(), vec![1, 3, 6]);
    assert_eq!([1, 3, 6].map(|w| s.rank(w)), [0, 1, 2]);

    assert_eq!(WorkerSet::ALL.within(4).len(), 4);
    assert_eq!(WorkerSet::ALL.within(MAX_WORKERS), WorkerSet::ALL);
    assert_eq!(WorkerSet::EMPTY.len(), 0);
    assert_eq!(WorkerSet::ALL.rank(MAX_WORKERS - 1), MAX_WORKERS - 1);
    assert_eq!(WorkerSet::EMPTY.rank(7), 0);

    assert_eq!(WorkerSet::one(1).union(WorkerSet::one(3)), WorkerSet::one(1).with(3));
    assert_eq!(s.union(WorkerSet::EMPTY), s);
}
