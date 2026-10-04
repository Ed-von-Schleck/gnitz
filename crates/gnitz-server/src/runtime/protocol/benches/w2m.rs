use super::fixtures::make_ring;
use super::*;
use gnitz_foundation::perf::Counter;
use gnitz_wire::control::CTRL_HEADER_SIZE;
use std::hint::black_box;

/// Instructions per control frame through one ring, publish and drain apart. A
/// lap fills the ring and then empties it, so nobody parks and every lap wraps;
/// a drained frame's cost includes its release.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn w2m_ring_bench() {
    const LAPS: u64 = 2_000;
    const RING_FRAMES: u32 = 64;

    let counter = Counter::instructions();
    let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE, RING_FRAMES as usize, 8);
    let (mut publish, mut drain) = (0, 0);
    for _ in 0..LAPS {
        publish += counter
            .measure(|| {
                for req in 1..=RING_FRAMES {
                    assert!(writer.try_send_msg(req, &WireMsg::default()), "the ring holds a lap");
                }
            })
            .1;
        drain += counter
            .measure(|| {
                for req in 1..=RING_FRAMES {
                    let slot = receiver.try_read_slot(0).expect("a published frame");
                    assert_eq!(slot.internal_req_id, req);
                    black_box(slot.bytes());
                }
            })
            .1;
    }
    assert!(
        receiver.try_read_slot(0).is_none(),
        "every lap drained what it published"
    );
    let frames = (LAPS * RING_FRAMES as u64) as f64;
    println!(
        "w2m_ring_bench publish {:.1} instr/frame, drain {:.1} instr/frame",
        publish as f64 / frames,
        drain as f64 / frames,
    );
}
