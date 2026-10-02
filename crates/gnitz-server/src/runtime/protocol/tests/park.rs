use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, within};

/// Worker `w`'s park word.
fn word(parks: WorkerParks, w: usize) -> &'static Park {
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

/// What a wake of a parked worker costs its sender, and how often it spends a
/// `FUTEX_WAKE`.
///
/// A forked child parks until it has seen `N` wakes. The parent sends them in two
/// arms: each only once the child has armed, so every wake finds a sleeper, and
/// back to back from its first arm. The armed arm measures each wake on its own and takes the cost of
/// an empty measurement off; the armed share is sampled just before each wake.
///
/// `cd crates && cargo test -p gnitz-server --release park_wake_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn park_wake_bench() {
    const N: u64 = 200_000;

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let empty = (0..N).map(|_| counter.measure(|| ()).1).sum::<u64>();
    for wait_for_arm in [true, false] {
        let parks = WorkerParks::create(1).unwrap();
        // The child's voluntary context switches, written before it exits.
        let switches = map_anon_shared(4096).unwrap().cast::<i64>();
        let armed = || word(parks, 0).armed();
        let child = || {
            let before = gnitz_foundation::perf::voluntary_ctx_switches();
            let mut seen = 0;
            while seen < N {
                parks.park(0).park(|| parks.wake_seq(0) == seen);
                seen = parks.wake_seq(0);
            }
            unsafe { switches.write(gnitz_foundation::perf::voluntary_ctx_switches() - before) };
        };
        let pid = unsafe { fork_child(child) };

        let (mut found_armed, mut instructions) = (0u64, 0u64);
        if wait_for_arm {
            for _ in 0..N {
                while !armed() {
                    std::hint::spin_loop();
                }
                found_armed += 1;
                instructions += counter.measure(|| parks.wake(0)).1;
            }
            instructions -= empty;
        } else {
            // From a sleeping child: sent before its first arm, the wakes would
            // all be over before it ever parked.
            while !armed() {
                std::hint::spin_loop();
            }
            ((), instructions) = counter.measure(|| {
                for _ in 0..N {
                    found_armed += armed() as u64;
                    parks.wake(0);
                }
            });
        }
        unsafe { assert_child_exited_ok(pid) };
        println!(
            "park_wake_bench {:<12} {:>7.1} instr/wake (kernel counted: {}), {:.1}% of wakes found the park armed, \
             {} child context switches",
            if wait_for_arm { "armed" } else { "back_to_back" },
            instructions as f64 / N as f64,
            counter.counts_kernel,
            found_armed as f64 * 100.0 / N as f64,
            unsafe { switches.read() },
        );
    }
}
