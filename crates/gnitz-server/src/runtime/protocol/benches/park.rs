use super::tests::word;
use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use gnitz_foundation::perf::voluntary_ctx_switches;

/// How many of a burst of advances spend a `FUTEX_WAKE`, beside how often the
/// parker slept, for each of the two advances.
///
/// A forked child parks until its word reaches `N`; the parent advances it `N`
/// times back to back. [`Park::publish`] takes the arm, so it spends one syscall
/// per arm; [`Park::bump`] leaves it, so every advance landing before the parker
/// disarms spends one. The arm is sampled just before each advance.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn park_wake_bench() {
    const N: u64 = 200_000;

    for (advance, publish) in [("bump", false), ("publish", true)] {
        let park = word(WorkerParks::create(1).unwrap(), 0);
        // The child's voluntary context switches, written before it exits.
        let sleeps = map_anon_shared(4096).unwrap().cast::<i64>();
        let child = || {
            let before = voluntary_ctx_switches();
            let mut seen = 0;
            while seen < N {
                park.park("bench", || park.value() == seen);
                seen = park.value();
            }
            unsafe { sleeps.write(voluntary_ctx_switches() - before) };
        };
        let pid = unsafe { fork_child(child) };

        // From a parked child: sent before its first arm, the advances would all
        // be over before it ever parked.
        while !park.armed() {
            std::hint::spin_loop();
        }
        let mut spent = 0u64;
        for k in 1..=N {
            spent += park.armed() as u64;
            match publish {
                true => park.publish(k, "bench"),
                false => park.bump("bench"),
            }
        }
        unsafe { assert_child_exited_ok(pid) };
        assert!(spent >= 1, "{advance}: the first advance found the child armed");
        println!(
            "park_wake_bench {advance:<8} {:>5.1}% of advances spent a FUTEX_WAKE, {} child sleeps",
            spent as f64 * 100.0 / N as f64,
            unsafe { sleeps.read() },
        );
    }
}
