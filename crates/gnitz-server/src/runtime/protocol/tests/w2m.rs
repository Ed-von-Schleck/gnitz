use super::fixtures::make_ring;
use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, SharedRegion};
use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;
use gnitz_wire::WireStatus;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Publish one message directly, without `W2mWriter`'s park loop. Returns the
/// new write cursor and whether the publish SKIP-wrapped — a wrap is exactly a
/// cursor advance longer than the message. `None` when the ring is full.
///
/// # Safety
/// `base` must be a live ring from [`test_ring`], and the caller must be its
/// sole producer.
unsafe fn publish(
    base: *mut u8,
    sz: usize,
    internal_req_id: u32,
    encode: impl FnOnce(&mut [u8]),
) -> Option<(u64, bool)> {
    let mut wc = RingCursor::producer(base);
    let before = wc.virt;
    let mut reservation = try_reserve(&wc, sz, internal_req_id)?;
    encode(reservation.slot());
    commit(&mut wc, reservation);
    Some((wc.virt, wc.virt - before != slot_stride(sz)))
}

// -- futex primitives ----------------------------------------------------

/// Pins the absence of `FUTEX_PRIVATE_FLAG`: a private futex hashes the word
/// to a different key in each process, so the child's wake would never
/// arrive — which no in-process test can catch.
#[test]
fn a_shared_futex_wake_crosses_a_process_boundary() {
    let region = SharedRegion::new(4096);
    let atomic_ptr = region.ptr() as *mut AtomicU32;
    unsafe {
        (*atomic_ptr).store(7, Ordering::Release);
    }

    let pid = unsafe { libc::fork() };
    if pid == 0 {
        // Child: bump the atomic, then wake the parent.
        unsafe {
            (*atomic_ptr).store(8, Ordering::Release);
        }
        futex_wake_u32(atomic_ptr as *const AtomicU32, 1, "test child");
        unsafe {
            libc::_exit(0);
        }
    }

    // `Retry` covers both proofs: woken by the child, or the value had
    // already moved. `TimedOut` is the failure this asserts against.
    let rc = futex_waitv_u32(&[futex_waitv_entry(atomic_ptr as *const AtomicU32, 7)], 5000);
    assert_eq!(rc, Parked::Retry, "the child's wake never reached the parent");
    let final_val = unsafe { (*atomic_ptr).load(Ordering::Acquire) };
    assert_eq!(final_val, 8);

    unsafe { assert_child_exited_ok(pid) };
}

/// The raw multi-word wrapper: a value mismatch fast-returns, no wake times
/// out, and a wake on a NON-FIRST word wakes the multi-word wait. The
/// timeout case bounds wall-clock only from below — an upper bound would be
/// asserting the machine is idle.
#[test]
fn futex_waitv_wakes_on_any_word() {
    use std::time::Instant;
    let region = SharedRegion::new(4096);
    let ptr = region.ptr();
    let w0 = ptr as *const AtomicU32;
    let w1 = unsafe { ptr.add(64) } as *const AtomicU32;
    unsafe {
        (*w0).store(0, Ordering::Release);
        (*w1).store(0, Ordering::Release);
    }
    let word = futex_waitv_entry;

    let rc = futex_waitv_u32(&[word(w0, 0), word(w1, 999)], 2000);
    assert_eq!(rc, Parked::Retry, "value mismatch must fast-return");

    let t = Instant::now();
    let rc = futex_waitv_u32(&[word(w0, 0), word(w1, 0)], 20);
    assert_eq!(rc, Parked::TimedOut, "no wake must time out");
    assert!(t.elapsed().as_millis() >= 15, "timed out before the deadline");

    let pid = unsafe { libc::fork() }; // wake on the NON-FIRST word
    if pid == 0 {
        unsafe {
            libc::usleep(5_000);
            (*w1).fetch_add(1, Ordering::Release);
        }
        futex_wake_u32(w1, 1, "test child");
        unsafe { libc::_exit(0) };
    }
    let rc = futex_waitv_u32(&[word(w0, 0), word(w1, 0)], 5000);
    assert_eq!(rc, Parked::Retry, "a wake on the non-first word must not time out");
    unsafe { assert_child_exited_ok(pid) };
}

// -- ring layout ---------------------------------------------------------

/// Consume one message and release its space, as the master does when it
/// drops the slot. The `swap` — not a plain store — is the barrier the park
/// protocol needs, so these tests model the production retirement.
unsafe fn consume_one(recv: &W2mReceiver) -> Option<&'static [u8]> {
    let slot = recv.try_read_slot(0)?;
    let bytes = &slot.frame[SLOT_LEN_PREFIX_BYTES..];
    drop(slot);
    Some(bytes)
}

/// Round-trip: publish one message, consume it, verify contents and cursor
/// advance.
#[test]
fn a_published_message_round_trips_in_place() {
    unsafe {
        let region = make_ring(128, 4, 8);
        let ptr = region.ptr();
        let recv = W2mReceiver::new(vec![ptr]);

        let payload = [0xAAu8; 128];
        let (new_wc, wrapped) = publish(ptr, payload.len(), 0, |slot| slot.copy_from_slice(&payload))
            .expect("unexpected Full on empty ring");
        assert!(!wrapped);
        assert_eq!(new_wc, W2M_HEADER_SIZE as u64 + slot_stride(128));
        assert_eq!(recv.write_cursor(0), new_wc);

        let data = consume_one(&recv).expect("message must be visible");
        assert_eq!(data, &payload);
        assert_eq!(
            data.as_ptr(),
            ptr.add(W2M_HEADER_SIZE + RING_PREFIX_BYTES as usize) as *const u8,
            "the payload must be mmap-resident — decode_wire reads it in place"
        );
        assert_eq!(recv.read_cursor(0), new_wc);
        assert!(consume_one(&recv).is_none(), "ring must now read empty");
    }
}

/// A SKIP-wrap with the reader still behind: room for 3 messages + 16 bytes
/// of slack, the reader lagging by exactly one.
#[test]
fn a_skip_marker_wraps_strictly() {
    unsafe {
        let big_sz = 1 << 16; // 64 KiB
        let region = make_ring(big_sz, 3, 16);
        let ptr = region.ptr();
        let recv = W2mReceiver::new(vec![ptr]);

        for _ in 0..3 {
            publish(ptr, big_sz, 0, |_| {}).expect("initial big publish");
        }
        for _ in 0..2 {
            consume_one(&recv).expect("consume");
        }

        // The 4th no longer fits before the physical end, and the reader is
        // two messages ahead of the head, so the wrap has somewhere to land.
        let (_, wrapped) = publish(ptr, big_sz, 0, |slot| slot[0] = 0xDE).expect("SKIP wrap must succeed");
        assert!(wrapped, "the 4th publish must SKIP-wrap");

        assert_eq!(
            consume_one(&recv).expect("pre-SKIP big").len(),
            big_sz,
            "the third message still reads contiguously",
        );
        let wrapped_msg = consume_one(&recv).expect("wrapped big via SKIP");
        assert_eq!(wrapped_msg.len(), big_sz);
        assert_eq!(wrapped_msg[0], 0xDE, "the SKIP jump must land on the wrapped payload");
    }
}

/// A message that ends exactly at `capacity` publishes contiguously: the
/// next write position is the header, with no marker needed.
#[test]
fn an_exact_fit_publishes_contiguously() {
    unsafe {
        let msg_sz = 1 << 12;
        const N: usize = 4;
        // Zero slack: DCAP is exactly N messages, so the N-th ends on `cap`.
        let region = make_ring(msg_sz, N, 0);
        let ptr = region.ptr();
        let total = slot_stride(msg_sz);
        let recv = W2mReceiver::new(vec![ptr]);

        for i in 0..N {
            let (new_wc, wrapped) = publish(ptr, msg_sz, 0, |_| {}).unwrap_or_else(|| panic!("publish #{i}"));
            assert!(!wrapped, "publish #{i} must fit contiguously, not wrap");
            assert_eq!(new_wc, W2M_HEADER_SIZE as u64 + (i as u64 + 1) * total);
            consume_one(&recv).expect("consume");
        }
    }
}

/// With the ring exactly full and the reader at the head, the next publish
/// is refused — backpressure, not a wrap over unread data.
#[test]
fn a_full_ring_blocks_the_writer() {
    unsafe {
        let msg_sz = 1 << 16; // 64 KiB
        let region = make_ring(msg_sz, 2, 8);
        let ptr = region.ptr();

        for _ in 0..2 {
            publish(ptr, msg_sz, 0, |_| {}).expect("fill publish");
        }
        assert!(
            publish(ptr, msg_sz, 0, |_| {}).is_none(),
            "an undrained ring must refuse the publish"
        );
    }
}

/// A message this ring cannot hold is a caller bug, and the ring says so
/// rather than reporting a full ring the caller would park on forever.
#[test]
#[should_panic(expected = "exceeds this ring's")]
fn an_oversized_publish_panics() {
    unsafe {
        let region = make_ring(64, 4, 8);
        let dcap = RingCursor::producer(region.ptr()).dcap();
        let _ = publish(region.ptr(), dcap as usize + 1, 0, |_| {});
    }
}

/// Regression: a writer that wraps must never land on a slot the reader has
/// not drained. Publish 4, consume 3, then publish 5..=9 — the sequence that
/// let a physical-cursor predicate overwrite message #4.
#[test]
fn a_writer_never_crosses_the_reader_after_a_wrap() {
    unsafe {
        let msg_sz: usize = 64;
        let region = make_ring(msg_sz, 5, 16);
        let ptr = region.ptr();
        let recv = W2mReceiver::new(vec![ptr]);

        let publish_tag = |tag: u8| publish(ptr, msg_sz, 0, |slot| slot.fill(tag)).is_some();

        for tag in 1u8..=4 {
            assert!(publish_tag(tag), "publish #{tag}");
        }

        let mut received = Vec::new();
        for _ in 0..3 {
            received.push(consume_one(&recv).expect("consume")[0]);
        }
        assert_eq!(received, vec![1, 2, 3]);

        // #5 fits contiguously, #6 forces the wrap, #7..=9 chase the reader.
        for tag in 5u8..=9 {
            if !publish_tag(tag) {
                received.push(consume_one(&recv).expect("drain to make room")[0]);
                assert!(publish_tag(tag), "publish #{tag} after drain");
            }
        }

        while let Some(data) = consume_one(&recv) {
            received.push(data[0]);
        }
        assert_eq!(
            received,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            "the writer must not overwrite unread data after a wrap",
        );
    }
}

/// An unaligned capacity would let the SKIP path's 8-byte writes cross the
/// end of the mapping.
#[test]
#[should_panic(expected = "8-byte aligned")]
fn init_region_rejects_unaligned_capacity() {
    unsafe {
        let cap = W2M_HEADER_SIZE + 17;
        let region = SharedRegion::new(cap);
        init_region(region.ptr(), cap as u64);
    }
}

/// A capacity with no room for a message past the header is refused.
#[test]
#[should_panic(expected = "leaves no room")]
fn init_region_rejects_undersized_capacity() {
    unsafe {
        // Room for a prefix word and nothing after it.
        let cap = W2M_HEADER_SIZE + RING_PREFIX_BYTES as usize;
        let region = SharedRegion::new(cap);
        init_region(region.ptr(), cap as u64);
    }
}

// -- slot retirement -----------------------------------------------------

unsafe fn in_flight_len(recv: &W2mReceiver, w: usize) -> usize {
    (*recv.rings[w].in_flight.get()).queue.len()
}

/// Publish `order.len()` slots, take them all, then release in `order`,
/// asserting `release_cursor` tracks the front-consecutive released prefix
/// after every drop.
fn release_follows_front_consecutive_prefix(order: Vec<usize>) {
    let n = order.len();
    unsafe {
        let region = make_ring(8, n, 8);
        let ptr = region.ptr();
        let receiver = W2mReceiver::new(vec![ptr]);

        for i in 0..n {
            publish(ptr, 8, 0, |s| s[0] = i as u8).unwrap_or_else(|| panic!("publish #{i}"));
        }
        let mut slots: Vec<Option<W2mSlot>> = Vec::with_capacity(n);
        let mut vrcs = Vec::with_capacity(n);
        for _ in 0..n {
            slots.push(Some(receiver.try_read_slot(0).expect("slot")));
            vrcs.push(receiver.read_cursor(0));
        }
        assert_eq!(in_flight_len(&receiver, 0), n, "all {n} slots tracked in-flight");
        assert_eq!(
            receiver.release_cursor(0),
            W2M_HEADER_SIZE as u64,
            "release_cursor must not advance while every slot is in-flight",
        );

        let mut released = vec![false; n];
        for &idx in &order {
            slots[idx] = None; // drop → release(push_idx = idx)
            released[idx] = true;
            let p = released.iter().position(|&r| !r).unwrap_or(n);
            let expected = if p == 0 { W2M_HEADER_SIZE as u64 } else { vrcs[p - 1] };
            assert_eq!(
                receiver.release_cursor(0),
                expected,
                "release_cursor must track the front-consecutive released prefix (p={p})",
            );
        }
        assert_eq!(in_flight_len(&receiver, 0), 0, "queue fully drained");
    }
}

#[test]
fn release_in_push_order() {
    release_follows_front_consecutive_prefix((0..200).collect());
}

/// Reverse order: nothing retires until the head releases last, and that one
/// drop must retire all 200 entries in a single drain.
#[test]
fn release_in_reverse_order() {
    release_follows_front_consecutive_prefix((0..200).rev().collect());
}

/// A deterministic scramble past the queue's initial capacity, exercising
/// partial-prefix retirement at depth.
#[test]
fn release_out_of_order() {
    let mut o: Vec<usize> = (0..200).collect();
    o.sort_by_key(|&i| (i * 73 + 11) % 200); // 73 coprime to 200 → a permutation
    release_follows_front_consecutive_prefix(o);
}

/// Dropping a slot advances release_cursor and unparks a blocked writer.
#[test]
fn a_retired_slot_unparks_the_writer() {
    unsafe {
        // Ring holds exactly 1 status frame, which is what the parked writer
        // below publishes — so the two must be sized the same.
        let region = make_ring(CTRL_BLOCK_SIZE_NO_BLOB, 1, 8);
        let ptr = region.ptr();
        publish(ptr, CTRL_BLOCK_SIZE_NO_BLOB, 0, |s| s[0] = 1).expect("ring should have room for the first message");

        let receiver = W2mReceiver::new(vec![ptr]);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        // This second publish cannot fit; it blocks until a slot retires.
        let region_addr = ptr as usize;
        let handle = std::thread::spawn(move || {
            W2mWriter::new(region_addr as *mut u8).send_status(0, 1, WireStatus::Ok, &[]);
            let _ = done_tx.send(());
        });

        // The writer sets FLAG_WRITER_PARKED before it parks on release_cursor.
        while receiver.header(0).writer_park.flags.load(Ordering::Acquire) & FLAG_WRITER_PARKED == 0 {
            std::hint::spin_loop();
        }

        let slot = receiver.try_read_slot(0).expect("slot");
        drop(slot);

        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("writer thread did not complete within 5 seconds");
        handle.join().expect("writer thread panicked");
    }
}

// -- park / wake ---------------------------------------------------------

/// A publish takes the master gate, so the next publish in the same window
/// finds it clear and spends no syscall.
#[test]
fn publish_takes_the_master_gate() {
    unsafe {
        let region = make_ring(64, 4, 8);
        let ptr = region.ptr();
        let receiver = W2mReceiver::new(vec![ptr]);
        let hdr = receiver.header(0);

        // Arm the reactor's park by hand; `arm_waitv` needs a quiet ring.
        let mut out = [FutexWaitV::new(); 1];
        assert!(receiver.arm_waitv(&mut out).is_some(), "an empty ring must arm");
        assert_ne!(hdr.master_park.flags.load(Ordering::Acquire) & FLAG_MASTER_WAITV, 0);

        publish(ptr, 64, 0, |s| s[0] = 1).expect("an empty ring has room");
        assert_eq!(
            hdr.master_park.flags.load(Ordering::Acquire) & FLAG_MASTER_WAITV,
            0,
            "a publish must clear the master park bit",
        );
    }
}

/// Publish/drain throughput over one ring, and how many publishes spend a
/// `FUTEX_WAKE` — the quantity every change to the park protocol moves.
///
/// A forked child publishes `N` control frames as fast as it can while the
/// parent drains them, parking on the ring whenever it runs dry. Only the
/// ratios carry meaning: absolute rates swing with machine load. The gate is
/// sampled just before each publish, which is the predicate that publish's
/// `wake_master` reads — so the count is a syscall count without `strace`.
///
/// `cd crates && cargo test -p gnitz-server --release w2m_publish_drain_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn w2m_publish_drain_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: u64 = 200_000;
    const RING_FRAMES: usize = 64;

    let region = unsafe { make_ring(CTRL_BLOCK_SIZE_NO_BLOB, RING_FRAMES, 8) };
    let ptr = region.ptr();
    // Two u64s the child fills before `_exit`: publishes that found a master
    // park armed, and the child's own elapsed nanos.
    let counters = SharedRegion::new(4096);
    let cptr = counters.ptr() as *mut u64;
    unsafe { std::ptr::write_bytes(counters.ptr(), 0, 4096) };

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        let writer = W2mWriter::new(ptr);
        let hdr = unsafe { W2mRingHeader::from_raw(ptr) };
        let t = Instant::now();
        let mut woke_master = 0u64;
        for req in 1..=N {
            if hdr.master_park.flags.load(Ordering::Relaxed) & FLAG_MASTER_WAITV != 0 {
                woke_master += 1;
            }
            writer.send_status(0, req, gnitz_wire::WireStatus::Ok, &[]);
        }
        unsafe {
            cptr.write(woke_master);
            cptr.add(1).write(t.elapsed().as_nanos() as u64);
            libc::_exit(0);
        }
    }

    let receiver = W2mReceiver::new(vec![ptr]);
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
                if let Some(armed) = receiver.arm_waitv(&mut waitv) {
                    let _ = futex_waitv_u32(armed, 100);
                }
                receiver.clear_waitv();
            }
        }
    }
    let drain_ns = t.elapsed().as_nanos() as u64;

    let mut status = 0;
    unsafe { libc::waitpid(pid, &mut status, 0) };
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

// -- concurrent publish/drain --------------------------------------------

/// One writer thread publishes `n` status frames carrying `pad` while the
/// master drains the ring: every frame must arrive exactly once, in publish
/// order, with its payload intact. The ring holds only `ring_frames` of them,
/// so the run wraps it many times over.
fn concurrent_publish_drains_in_order(case: &str, ring_frames: usize, n: u64, pad: &[u8]) {
    let region = unsafe { make_ring(CTRL_BLOCK_SIZE_NO_BLOB + pad.len(), ring_frames, 8) };
    let ptr = region.ptr();

    let region_addr = ptr as usize;
    let done = Arc::new(AtomicBool::new(false));
    let done_w = Arc::clone(&done);
    let pad_w = pad.to_vec();
    let writer_thread = std::thread::spawn(move || {
        let writer = W2mWriter::new(region_addr as *mut u8);
        for req_id in 1..=n {
            writer.send_status(0, req_id, WireStatus::Ok, &pad_w);
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
            match receiver.try_read_slot(0).map(|s| s.decode(0)) {
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
        while receiver.try_read_slot(0).map(|s| s.decode(0)).is_some() {}
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
fn w2m_control_only_reply_has_no_backing() {
    let region = unsafe { make_ring(CTRL_BLOCK_SIZE_NO_BLOB, 2, 8) };
    let ptr = region.ptr();

    let writer = W2mWriter::new(ptr);
    writer.send_status(0, 42, WireStatus::Ok, b"");

    let receiver = W2mReceiver::new(vec![ptr]);
    let slot = receiver.try_read_slot(0).expect("an ACK");
    assert_eq!(slot.control(0).request_id, 42);
    assert!(
        slot.decode(0).data_batch.is_none(),
        "control-only ACK must have no data_batch"
    );
}
