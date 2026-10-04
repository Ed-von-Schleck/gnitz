use super::super::park::futex_wait_u32;
use super::fixtures::make_ring;
use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use gnitz_wire::control::CTRL_HEADER_SIZE;
use std::time::Instant;

/// Publish/drain throughput over one ring, and how many publishes spend a
/// `FUTEX_WAKE` — the quantity every change to the park protocol moves.
///
/// A forked child publishes `N` control frames as fast as it can while the
/// parent drains them, parking on the ring whenever it runs dry. Only the
/// ratios carry meaning: absolute rates swing with machine load. The master's
/// armed bit is sampled just before each publish's swap, so the wake count
/// approximates the syscalls without `strace`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn w2m_publish_drain_bench() {
    use std::hint::black_box;

    const N: u64 = 200_000;
    const RING_FRAMES: usize = 64;

    let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE, RING_FRAMES, 8);
    // Two u64s the child fills before `_exit`: publishes that found a master
    // park armed, and the child's own elapsed nanos.
    let cptr = map_anon_shared(4096).unwrap().cast::<u64>();

    let child = || {
        let t = Instant::now();
        let mut woke_master = 0u64;
        for req in 1..=N {
            if writer.master_parked() {
                woke_master += 1;
            }
            writer.send_ack(req as u32);
        }
        unsafe {
            cptr.write(woke_master);
            cptr.add(1).write(t.elapsed().as_nanos() as u64);
        }
    };

    let pid = unsafe { fork_child(child) };

    let t = Instant::now();
    let (mut drained, mut parks) = (0u64, 0u64);
    while drained < N {
        match receiver.try_read_slot(0) {
            Some(slot) => {
                black_box(slot.bytes());
                drained += 1;
            }
            None => {
                parks += 1;
                let mut waitv = [FutexWaitV::new(); 1];
                // A successful arm means the armed value equals the read cursor.
                if receiver.arm_waitv(&mut waitv).is_some() {
                    futex_wait_u32(
                        receiver.header(0).write.futex_word(),
                        receiver.read_cursor(0) as u32,
                        "bench",
                    );
                }
                receiver.clear_waitv();
            }
        }
    }
    let drain_ns = t.elapsed().as_nanos() as u64;

    unsafe { assert_child_exited_ok(pid) };
    let (woke_master, publish_ns) = unsafe { (cptr.read(), cptr.add(1).read()) };

    println!(
        "w2m publish/drain N={N} ring={RING_FRAMES} frames: \
         drain {:.2} Mmsg/s, publish {:.2} Mmsg/s, \
         {:.1}% of publishes spent a FUTEX_WAKE, {parks} drain parks ({:.1} per 1k msgs)",
        N as f64 * 1000.0 / drain_ns as f64,
        N as f64 * 1000.0 / publish_ns as f64,
        woke_master as f64 * 100.0 / N as f64,
        parks as f64 * 1000.0 / N as f64,
    );
}
