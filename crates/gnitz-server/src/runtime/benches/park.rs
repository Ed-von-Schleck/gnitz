use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use gnitz_foundation::perf::voluntary_ctx_switches;

/// How many of a burst of advances spend a `FUTEX_WAKE`, beside how often the
/// parker slept, for each of the two advances.
///
/// A forked child waits until a word reaches `N`; the parent advances it `N`
/// times back to back. A bumped word is the child's own park; a published one
/// is a word apart, which the child arms before parking on its own park, as a
/// worker short of ring room does. [`Park::publish_waking`] takes the arm, so it
/// spends one syscall per arm; [`Park::bump`] leaves it, so every advance landing
/// before the parker disarms spends one. The arm is sampled just before each
/// advance.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn park_wake_bench() {
    const N: u64 = 200_000;

    for (advance, publish) in [("bump", false), ("publish", true)] {
        let parks = WorkerParks::create(2).unwrap();
        // The child's park, and the word it waits on.
        let (park, word) = match publish {
            true => (parks.park(0), parks.park(1)),
            false => (parks.park(0), parks.park(0)),
        };
        // The child's voluntary context switches, written before it exits.
        let sleeps = posix_io::map_shared(None, 4096).unwrap().cast::<i64>();
        let child = || {
            let before = voluntary_ctx_switches();
            let mut seen = 0;
            while seen < N {
                if publish {
                    word.arm();
                }
                park.park(|| word.value() == seen);
                seen = word.value();
            }
            unsafe { sleeps.write(voluntary_ctx_switches() - before) };
        };
        let pid = unsafe { fork_child(child) };

        // From a parked child: sent before its first arm, the advances would all
        // be over before it ever parked.
        while !word.armed() {
            std::hint::spin_loop();
        }
        let mut spent = 0u64;
        for k in 1..=N {
            spent += word.armed() as u64;
            match publish {
                true => word.publish_waking(k, park),
                false => word.bump(),
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
