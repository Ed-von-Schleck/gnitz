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
