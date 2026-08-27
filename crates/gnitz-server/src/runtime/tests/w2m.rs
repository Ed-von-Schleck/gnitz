use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::w2m_ring;
use crate::runtime::wire::STATUS_OK;
use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// One writer thread publishes `n` status frames carrying `pad` while the
/// master drains the ring: every frame must arrive exactly once, in publish
/// order, with its payload intact. The ring holds only `ring_frames` of them,
/// so the run wraps it many times over.
fn concurrent_publish_drains_in_order(case: &str, ring_frames: usize, n: u64, pad: &[u8]) {
    let region = unsafe { w2m_ring::make_ring(CTRL_BLOCK_SIZE_NO_BLOB + pad.len(), ring_frames, 8) };
    let ptr = region.ptr();

    let region_addr = ptr as usize;
    let done = Arc::new(AtomicBool::new(false));
    let done_w = Arc::clone(&done);
    let pad_w = pad.to_vec();
    let writer_thread = std::thread::spawn(move || {
        let writer = W2mWriter::new(region_addr as *mut u8);
        for req_id in 1..=n {
            writer.send_status(0, req_id, STATUS_OK, &pad_w);
        }
        done_w.store(true, Ordering::Release);
    });

    let receiver = W2mReceiver::new(vec![ptr]);
    let drain = || {
        let mut next_expected: u64 = 1;
        let started = Instant::now();
        while next_expected <= n {
            // Read `done` BEFORE the ring. Then an empty ring proves the loss:
            // everything the writer published was already consumed. Reading it
            // after would blame a frame the writer had not published yet.
            let writer_done = done.load(Ordering::Acquire);
            match receiver.try_read(0) {
                Some(decoded) => {
                    assert_eq!(
                        decoded.control.request_id, next_expected,
                        "{case}: frames arrived out of order at req_id={next_expected}"
                    );
                    assert_eq!(
                        decoded.control.error_msg, pad,
                        "{case}: payload corrupted at req_id={next_expected}"
                    );
                    next_expected += 1;
                }
                None if writer_done => panic!(
                    "{case}: writer finished but only {}/{n} frames arrived",
                    next_expected - 1
                ),
                None => {
                    assert!(
                        started.elapsed() < Duration::from_secs(30),
                        "{case}: timed out at req_id={next_expected}"
                    );
                    std::thread::yield_now();
                }
            }
        }
    };

    // The writer must stop touching the region before it is unmapped, and it
    // parks when the ring fills — so on a failed drain, keep consuming until it
    // finishes rather than unwinding straight into `join`.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(drain));
    while !done.load(Ordering::Acquire) {
        while receiver.try_read(0).is_some() {}
        std::thread::yield_now();
    }
    writer_thread.join().expect("writer thread");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[test]
fn w2m_concurrent_small_frames_arrive_in_order() {
    concurrent_publish_drains_in_order("small frames", 8, 2_000, b"");
}

#[test]
fn w2m_concurrent_large_frames_arrive_in_order() {
    // A payload past the German-string inline threshold, so each frame also
    // carries a blob region.
    concurrent_publish_drains_in_order("large frames", 4, 500, &[b'x'; 4000]);
}

#[test]
fn w2m_writer_rejects_oversized() {
    let region = unsafe { w2m_ring::make_ring(CTRL_BLOCK_SIZE_NO_BLOB, 4, 8) };
    let region_addr = region.ptr() as usize;
    let (tx, rx) = std::sync::mpsc::channel::<bool>();
    let writer_thread = std::thread::spawn(move || {
        let writer = W2mWriter::new(region_addr as *mut u8);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.send_encoded((w2m_ring::MAX_W2M_MSG + 1) as usize, 0, |_| {});
        }));
        let _ = tx.send(result.is_err());
    });

    let verdict = rx.recv_timeout(Duration::from_secs(2));
    writer_thread.join().expect("writer thread");
    match verdict {
        Ok(true) => {}
        Ok(false) => panic!("send_encoded returned normally on oversized sz — expected panic"),
        Err(_) => panic!(
            "send_encoded deadlocked on oversized sz — it must panic instead of \
             spinning on Full forever"
        ),
    }
}

#[test]
fn w2m_control_only_reply_has_no_backing() {
    let region = unsafe { w2m_ring::make_ring(CTRL_BLOCK_SIZE_NO_BLOB, 2, 8) };
    let ptr = region.ptr();

    let writer = W2mWriter::new(ptr);
    writer.send_status(0, 42, STATUS_OK, b"");

    let receiver = W2mReceiver::new(vec![ptr]);
    let decoded = receiver.try_read(0).expect("ACK must decode");
    assert_eq!(decoded.control.request_id, 42);
    assert!(decoded.data_batch.is_none(), "control-only ACK must have no data_batch");
}
