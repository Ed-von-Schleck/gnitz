use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, within};

/// A park whose re-test behind the arm finds a group returns without sleeping,
/// and leaves the park disarmed.
#[test]
fn a_park_returns_at_once_when_its_retest_finds_a_group() {
    let parks = WorkerParks::create(1).unwrap();
    let mut armed_when_tested = false;
    parks.park(0).park(|| {
        armed_when_tested = parks.word(0).armed();
        false
    });
    assert!(armed_when_tested, "the re-test runs behind the arm");
    assert!(!parks.word(0).armed(), "and the park disarms");
}

/// A worker parked on an empty SAL wakes on another process's wake.
#[test]
fn a_parked_worker_wakes_on_a_forked_masters_wake() {
    let parks = WorkerParks::create(1).unwrap();
    let child = || {
        while !parks.word(0).armed() {
            std::thread::yield_now();
        }
        parks.wake(0);
    };
    let pid = unsafe { fork_child(child) };
    within(move || {
        // The wake count stands in for the SAL the worker re-tests.
        parks.park(0).park(|| parks.word(0).value() == 0);
        assert_eq!(parks.word(0).value(), 1, "the park ended on the wake");
    });
    unsafe { assert_child_exited_ok(pid) };
}
