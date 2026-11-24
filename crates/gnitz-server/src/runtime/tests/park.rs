use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, within};

impl Park {
    /// Whether a sleeper is armed on this park.
    pub(in crate::runtime) fn armed(&self) -> bool {
        self.0.load(Ordering::Acquire) & ARMED != 0
    }
}

/// A park whose re-test behind the arm finds a group returns without sleeping,
/// and leaves the park disarmed.
#[test]
fn a_park_returns_at_once_when_its_retest_finds_a_group() {
    let parks = WorkerParks::create(1).unwrap();
    let mut armed_when_tested = false;
    parks.park(0).park(|| {
        armed_when_tested = parks.park(0).armed();
        false
    });
    assert!(armed_when_tested, "the re-test runs behind the arm");
    assert!(!parks.park(0).armed(), "and the park disarms");
}

/// A bump takes the arm it advances past, so a second bump before the parker
/// re-arms finds none; each still moves the value a futex wait compares.
#[test]
fn a_bump_takes_the_arm_it_wakes() {
    let parks = WorkerParks::create(1).unwrap();
    let park = parks.park(0);
    let (_, at) = park.arm_for(|| true).expect("still waiting: the arm stays");
    park.bump();
    assert!(!park.armed(), "the bump took the arm");
    park.bump();
    assert_eq!(park.value(), u64::from(at) + 2, "both bumps advanced the value");
}

/// A worker parked on an empty SAL wakes on another process's wake.
#[test]
fn a_parked_worker_wakes_on_a_forked_masters_wake() {
    let parks = WorkerParks::create(1).unwrap();
    let child = || {
        while !parks.park(0).armed() {
            std::thread::yield_now();
        }
        parks.wake(WorkerSet::one(0));
    };
    let pid = unsafe { fork_child(child) };
    within(move || {
        // The wake count stands in for the SAL the worker re-tests.
        parks.park(0).park(|| parks.park(0).value() == 0);
        assert_eq!(parks.park(0).value(), 1, "the park ended on the wake");
    });
    assert_child_exited_ok(pid);
}

/// Membership, the launched-worker bound and position, at a small worker count and
/// at the full word.
#[test]
fn a_worker_set_bounds_to_the_launched_workers() {
    let s = WorkerSet::one(1).with(3).with(6);
    assert_eq!(s.len(), 3);
    assert!(s.contains(3) && !s.contains(2));
    assert_eq!(s.without(3), WorkerSet::one(1).with(6));
    assert_eq!(s.within(4), WorkerSet::one(1).with(3));
    assert_eq!(s.iter().collect::<Vec<_>>(), vec![1, 3, 6]);
    assert_eq!([1, 3, 6].map(|w| s.position(w)), [Some(0), Some(1), Some(2)]);
    assert_eq!(s.position(2), None);

    assert_eq!(WorkerSet::ALL.within(4).len(), 4);
    assert_eq!(WorkerSet::ALL.within(MAX_WORKERS), WorkerSet::ALL);
    assert_eq!(WorkerSet::ALL.position(MAX_WORKERS - 1), Some(MAX_WORKERS - 1));
    assert_eq!([1, 3, 6].into_iter().collect::<WorkerSet>(), s);

    assert_eq!(WorkerSet::one(1).union(WorkerSet::one(3)), WorkerSet::one(1).with(3));
}
