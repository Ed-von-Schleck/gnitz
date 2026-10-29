use super::fixtures::{id, make_ring};
use super::*;
use gnitz_foundation::perf::Counter;
use gnitz_wire::CTRL_HEADER_SIZE;
use std::hint::black_box;

/// Instructions per control frame through one ring, publish and drain apart,
/// by how many frames go in before they all come out. Nobody parks; a drained
/// frame's cost includes its release. What a `measure` counts of itself is taken
/// off every lap, so a lap of one is not charged it per frame.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn w2m_ring_bench() {
    const FRAMES: u64 = 128_000;
    const RING_FRAMES: u32 = 64;

    let counter = Counter::instructions();
    let empty = (0..1_000).map(|_| counter.measure(|| ()).1).sum::<u64>() / 1_000;
    for lap in [RING_FRAMES, 1] {
        let laps = FRAMES / lap as u64;
        let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE, RING_FRAMES as usize, 8);
        let (mut publish, mut drain) = (0, 0);
        let reqs: Vec<RequestId> = (1..=lap).map(id).collect();
        for _ in 0..laps {
            publish += counter
                .measure(|| {
                    for &req in &reqs {
                        assert!(writer.try_send_msg(req, &WireMsg::default()), "the ring holds a lap");
                    }
                })
                .1;
            drain += counter
                .measure(|| {
                    for req in 1..=lap {
                        let slot = receiver.try_read_slot(0).expect("a published frame");
                        assert_eq!(slot.request_id, req);
                        black_box(slot.bytes());
                    }
                })
                .1;
        }
        assert!(
            receiver.try_read_slot(0).is_none(),
            "every lap drained what it published"
        );
        println!(
            "w2m_ring_bench lap of {lap}: publish {:.1} instr/frame, drain {:.1} instr/frame",
            (publish - laps * empty) as f64 / FRAMES as f64,
            (drain - laps * empty) as f64 / FRAMES as f64,
        );
    }
}
