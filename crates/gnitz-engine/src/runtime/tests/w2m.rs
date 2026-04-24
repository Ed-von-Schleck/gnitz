use crate::runtime::w2m::{W2mWriter, W2mReceiver};
use crate::runtime::w2m_ring;
use crate::runtime::wire::{encode_wire_into, wire_size, STATUS_OK};

#[test]
fn test_w2m_concurrent_publish_consume_ordered() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    const CAP: usize = 8 * 1024;
    const N: u64 = 2_000;

    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            CAP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1, 0,
        ) as *mut u8
    };
    assert!(!region.is_null());
    unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

    let region_addr = region as usize;
    let done = Arc::new(AtomicBool::new(false));
    let done_w = Arc::clone(&done);
    let writer_thread = std::thread::spawn(move || {
        let writer = W2mWriter::new(region_addr as *mut u8, CAP as u64);
        let sz = wire_size(STATUS_OK, b"", None, None, None, None);
        for req_id in 1..=N {
            writer.send_encoded(sz, |buf| {
                encode_wire_into(
                    buf, 0, 0, 0, 0,
                     0u128, 0, req_id, STATUS_OK, b"", None, None, None, None,
                );
            });
        }
        done_w.store(true, Ordering::Release);
    });

    let receiver = W2mReceiver::new(vec![region]);
    let mut next_expected: u64 = 1;
    let started = std::time::Instant::now();
    while next_expected <= N {
        if let Some(decoded) = receiver.try_read(0) {
            assert_eq!(
                decoded.control.request_id, next_expected,
                "msg ordering broke at req_id={}: writer-cross-reader \
                 or decode-after-release race",
                next_expected,
            );
            next_expected += 1;
        } else {
            if done.load(Ordering::Acquire) && next_expected <= N {
                if receiver.try_read(0).is_none() {
                    panic!(
                        "writer done but only {}/{} msgs received",
                        next_expected - 1, N,
                    );
                }
            }
            std::thread::yield_now();
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "test timed out at req_id={}", next_expected,
        );
    }

    writer_thread.join().expect("writer thread");
    unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
}

#[test]
fn test_w2m_concurrent_large_messages_ordered() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    const CAP: usize = 64 * 1024;
    const N: u64 = 500;
    let pad: Vec<u8> = vec![b'x'; 4000];

    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            CAP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1, 0,
        ) as *mut u8
    };
    assert!(!region.is_null());
    unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

    let region_addr = region as usize;
    let done = Arc::new(AtomicBool::new(false));
    let done_w = Arc::clone(&done);
    let pad_w = pad.clone();
    let writer_thread = std::thread::spawn(move || {
        let writer = W2mWriter::new(region_addr as *mut u8, CAP as u64);
        let sz = wire_size(STATUS_OK, &pad_w, None, None, None, None);
        for req_id in 1..=N {
            writer.send_encoded(sz, |buf| {
                encode_wire_into(
                    buf, 0, 0, 0, 0,
                     0u128, 0, req_id, STATUS_OK, &pad_w, None, None, None, None,
                );
            });
        }
        done_w.store(true, Ordering::Release);
    });

    let receiver = W2mReceiver::new(vec![region]);
    let mut next_expected: u64 = 1;
    let started = std::time::Instant::now();
    while next_expected <= N {
        if let Some(decoded) = receiver.try_read(0) {
            assert_eq!(
                decoded.control.request_id, next_expected,
                "large-msg ordering broke at req_id={}", next_expected,
            );
            assert_eq!(
                decoded.control.error_msg, pad,
                "payload corrupted at req_id={}", next_expected,
            );
            next_expected += 1;
        } else {
            std::thread::yield_now();
        }
        let _ = done.load(Ordering::Acquire);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "test timed out at req_id={}", next_expected,
        );
    }

    writer_thread.join().expect("writer thread");
    unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
}

#[test]
fn test_w2m_writer_rejects_oversized() {
    const CAP: usize = 64 * 1024;
    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            CAP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1, 0,
        ) as *mut u8
    };
    assert!(!region.is_null());
    unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

    let region_addr = region as usize;
    let (tx, rx) = std::sync::mpsc::channel::<bool>();
    std::thread::spawn(move || {
        let writer = W2mWriter::new(region_addr as *mut u8, CAP as u64);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.send_encoded(
                (w2m_ring::MAX_W2M_MSG + 1) as usize,
                |_| {},
            );
        }));
        let _ = tx.send(result.is_err());
    });

    match rx.recv_timeout(std::time::Duration::from_secs(2)) {
        Ok(true) => {}
        Ok(false) => panic!(
            "send_encoded returned normally on oversized sz — expected panic"
        ),
        Err(_) => panic!(
            "send_encoded deadlocked on oversized sz — regression: \
             must panic instead of spinning on Full forever"
        ),
    }

    unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
}

#[test]
fn test_w2m_control_only_reply_has_no_backing() {
    const CAP: usize = 64 * 1024;
    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            CAP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1, 0,
        ) as *mut u8
    };
    assert!(!region.is_null());
    unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

    let writer = W2mWriter::new(region, CAP as u64);
    let sz = wire_size(STATUS_OK, b"", None, None, None, None);
    writer.send_encoded(sz, |buf| {
        encode_wire_into(
            buf, 0, 0, 0, 0,
             0u128, 0, 42, STATUS_OK, b"", None, None, None, None,
        );
    });

    let receiver = W2mReceiver::new(vec![region]);
    let decoded = receiver.try_read(0).expect("ACK must decode");
    assert_eq!(decoded.control.request_id, 42);
    assert!(decoded.data_batch.is_none(),
        "control-only ACK must have no data_batch");
    assert!(decoded.batch_backing.is_none(),
        "control-only ACK must skip the heap-copy path");

    unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
}
