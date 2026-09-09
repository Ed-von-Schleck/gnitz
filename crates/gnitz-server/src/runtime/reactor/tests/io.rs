//! Client ingress: the frame deframer over its carry, and the global
//! inbound-memory budget every `RecvBuf` is charged against.

use super::super::test_support::*;
use super::super::*;
use crate::runtime::test_support::try_poll_once;

/// Drive `wire` into a fresh connection capped at `cap` and assert it is
/// refused: the connection reaped, and the global counter back at 0 — a refused
/// frame must never have been allocated, and a reaped one must be refunded.
fn assert_refused(cap: usize, max_payload: Option<usize>, wire: &[u8], why: &str) {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = Reactor::new(16, Limits { inbound_cap: cap, ..Limits::TEST }).expect("reactor");
        r.register_conn(read_fd);
        if let Some(limit) = max_payload {
            r.set_max_payload_len(read_fd, limit);
        }
        gnitz_foundation::posix_io::write_all_fd(write_fd, wire).expect("write");

        assert!(
            poll_until(&r, 20_000, || !r.inner.conns.borrow().contains_key(&read_fd)),
            "{why}"
        );
        assert_eq!(r.inbound().held(), 0, "{why}: budget must be reconciled");
        assert!(
            try_poll_once(r.recv(read_fd)).unwrap().is_none(),
            "{why}: recv after the close must yield None"
        );
        libc::close(write_fd); // read_fd was closed by reap
    }
}

/// The four ways an inbound frame is refused at its header, before any payload
/// byte is allocated.
#[test]
fn inbound_frames_are_refused_at_the_header() {
    let repeat = |payload: &[u8], n: usize| -> Vec<u8> { (0..n).flat_map(|_| framed(payload)).collect() };

    // frame_weight(100) = 100. Two frames = 200 held; the 3rd pushes 300 > 250.
    assert_refused(
        250,
        Some(1 << 20),
        &repeat(&[0xAB; 100], 3),
        "cumulative weight over cap",
    );

    // 1-byte payloads each weigh the 64-byte floor, so 64 frames = 4096 and the
    // 65th breaches. Without the floor 65 frames would weigh 65 B and never trip.
    assert_refused(
        4096,
        None,
        &repeat(&[0xCD], 65),
        "tiny-frame flood via the weight floor",
    );

    // No `set_max_payload_len`: the ceiling is still the 8-byte HELLO payload.
    assert_refused(
        usize::MAX,
        None,
        &framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]),
        "first frame over the pre-handshake ceiling",
    );

    // A zero-length frame is the close sentinel, not a frame.
    assert_refused(usize::MAX, None, &0u32.to_le_bytes(), "the zero-length close sentinel");
}

/// In-flight (partial, un-completed) payloads are accounted, and a second
/// connection whose first frame would breach the full cap is refused — the
/// many-connection uncounted-in-flight OOM vector.
#[test]
fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        // Exactly one 10_000-byte in-flight buffer fits.
        let r = Reactor::new(16, Limits { inbound_cap: 10_000, ..Limits::TEST }).expect("reactor");
        r.register_conn(read_fd);
        r.set_max_payload_len(read_fd, 1 << 20);

        // Header claims 10_000 bytes but only 100 are delivered: the buffer
        // is malloc'd and counted at header-parse time, yet no frame completes.
        let mut hdr_and_part = Vec::new();
        hdr_and_part.extend_from_slice(&10_000u32.to_le_bytes());
        hdr_and_part.extend_from_slice(&[0x11u8; 100]);
        gnitz_foundation::posix_io::write_all_fd(write_fd, &hdr_and_part).expect("write");

        let counted = poll_until(&r, 10_000, || r.inbound().held() == 10_000);
        assert!(counted, "in-flight buffer was not accounted");
        assert!(
            r.inner.conns.borrow().get(&read_fd).is_none_or(|c| c.q.queued() == 0),
            "no frame should have completed from a partial payload"
        );

        // Second connection whose first frame would breach the now-full cap.
        let (read_fd2, write_fd2) = stream_pair();
        r.register_conn(read_fd2);
        r.set_max_payload_len(read_fd2, 1 << 20);
        gnitz_foundation::posix_io::write_all_fd(write_fd2, &framed(&[0x22u8; 100])).expect("write");

        let refused = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd2));
        assert!(refused, "over-cap second connection was not closed");
        // Refused connection allocated nothing; the first buffer is intact.
        assert_eq!(r.inbound().held(), 10_000);

        libc::close(write_fd);
        libc::close(write_fd2); // read_fd2 was closed by reap
        libc::close(read_fd); // conn1 never reaped (in-flight)
    }
}

/// Accounting balances: consumption decrements the counter, so total traffic
/// far above the cap never trips as long as the consumer keeps pace, and the
/// counter returns to 0 once the queue fully drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    let (read_fd, write_fd) = unsafe { stream_pair() };
    // One recv deframes a whole round and charges every frame in it, so the peak
    // is a round rather than a frame — and the cap below admits one.
    let r = Rc::new(Reactor::new(16, Limits { inbound_cap: 15_000, ..Limits::TEST }).expect("reactor"));
    r.register_conn(read_fd);
    r.set_max_payload_len(read_fd, 1 << 20);

    let payload = vec![0x7Eu8; 1_000]; // frame_weight = 1_000
    for _round in 0..2 {
        let mut wire = Vec::new();
        for _ in 0..10 {
            wire.extend_from_slice(&framed(&payload));
        }
        gnitz_foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");
        let r2 = Rc::clone(&r);
        r.block_on(async move {
            for _ in 0..10 {
                let buf = r2.recv(read_fd).await.expect("frame");
                assert_eq!(buf.as_slice().len(), 1_000);
            }
        });
    }
    assert_eq!(
        r.inbound().held(),
        0,
        "counter must return to 0 once every frame is consumed"
    );

    unsafe {
        libc::close(read_fd);
        libc::close(write_fd);
    }
}

// ─────────────────────────────────────────────────────────────────
// The deframer over its carry, driven through `RecvQueue` directly: the
// window is a raw pointer either transport writes into.
// ─────────────────────────────────────────────────────────────────

/// One `RecvQueue` plus the window it last handed out, driven one simulated
/// read at a time. Uncapped: the cap and the payload ceiling have their own
/// tests, through `assert_refused`.
struct Feeder {
    q: io::RecvQueue,
    budget: Rc<io::InboundBudget>,
    window: (*mut u8, u32),
}

impl Feeder {
    fn new() -> Feeder {
        let budget = Rc::new(io::InboundBudget::new(usize::MAX));
        let mut q = io::RecvQueue::new(Rc::clone(&budget));
        q.set_max_payload_len(1 << 20);
        let window = q.remaining();
        Feeder { q, budget, window }
    }

    /// Inbound bytes this connection currently holds charged.
    fn held(&self) -> usize {
        self.budget.held()
    }

    /// One read: hand the window as many of `bytes` as it takes. Returns how
    /// many, or `Err` when the queue closed the connection.
    fn read(&mut self, bytes: &[u8]) -> Result<usize, ()> {
        assert!(self.window.1 > 0, "remaining must never arm a zero-length window");
        let n = bytes.len().min(self.window.1 as usize);
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.window.0, n) };
        self.window = self.q.deliver(n, 7)?;
        Ok(n)
    }

    /// Feed `wire` as whole reads until it is exhausted; returns the read count.
    fn feed(&mut self, mut wire: &[u8]) -> Result<usize, ()> {
        let mut reads = 0;
        while !wire.is_empty() {
            let n = self.read(wire)?;
            wire = &wire[n..];
            reads += 1;
        }
        Ok(reads)
    }

    fn drain(&mut self) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| self.q.try_recv())
            .map(|b| b.as_slice().to_vec())
            .collect()
    }

    /// The window is the whole carry exactly when nothing is left buffered.
    fn carry_is_empty(&self) -> bool {
        self.window.1 as usize == io::CARRY_BYTES
    }
}

/// One read carrying a whole pipelined run queues every frame in it, in order,
/// and leaves nothing behind — the ingress half of the syscall claim.
#[test]
fn one_read_queues_every_frame_it_carries() {
    let mut f = Feeder::new();
    let payloads: Vec<Vec<u8>> = (0..16u8).map(|i| vec![i; 700]).collect();
    let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();

    assert_eq!(
        f.feed(&wire).expect("no refusal"),
        1,
        "a 16-frame run must cost one read"
    );
    assert_eq!(f.drain(), payloads, "every frame, in order");
    assert!(f.carry_is_empty(), "a fully consumed run leaves the carry empty");
}

/// A frame split at every byte boundary — inside the length prefix and inside
/// the payload alike — completes exactly once, and while only a prefix is
/// buffered the connection holds no charged buffer.
#[test]
fn a_frame_split_at_any_boundary_completes_once() {
    let payload = vec![0x5Au8; 40];
    let wire = framed(&payload);
    for split in 1..wire.len() {
        let mut f = Feeder::new();

        assert_eq!(f.read(&wire[..split]).expect("no refusal"), split);
        if split < gnitz_wire::FRAME_LEN_PREFIX_BYTES {
            assert_eq!(
                f.held(),
                0,
                "split={split}: a prefix alone charges nothing — the header is the charge point",
            );
        }
        assert!(
            f.drain().is_empty(),
            "split={split}: no frame completes from a fragment"
        );

        f.feed(&wire[split..]).expect("no refusal");
        assert_eq!(f.drain(), vec![payload.clone()], "split={split}");
        assert!(f.carry_is_empty(), "split={split}: nothing left buffered");
    }
}

/// A frame larger than the carry is charged at its header, absorbs whatever the
/// carry over-read past that header, takes the rest straight into its own
/// buffer, and the connection parses from the carry again afterwards.
#[test]
fn a_frame_larger_than_the_carry_reads_into_its_own_buffer() {
    let big = vec![0xC3u8; 2 * io::CARRY_BYTES + 500];
    let small = vec![0x11u8; 300];
    let mut wire = framed(&big);
    wire.extend_from_slice(&framed(&small));

    let mut f = Feeder::new();
    // Read 1 fills the carry and starts the payload; reads 2..n go straight
    // into it while its remainder is at least a carry long; the short tail
    // rides one last carry read together with the frame behind it.
    let reads = f.feed(&wire).expect("no refusal");
    assert_eq!(reads, 3, "one carry read, one direct read, one carry read for the tail");
    assert_eq!(f.drain(), vec![big, small]);
    assert!(f.carry_is_empty());
}

/// A frame straddling the end of a carry read completes on the **next** read,
/// in the same `deliver` that parses the frames behind it — so reads per run
/// stay `⌈bytes / CARRY_BYTES⌉` rather than twice that minus one.
#[test]
fn a_straddling_frame_rides_the_next_carry_read() {
    const F: usize = 700; // payload; 704 on the wire
    let per_read = io::CARRY_BYTES / (F + gnitz_wire::FRAME_LEN_PREFIX_BYTES);
    let m = 3 * per_read; // spans three carries, straddling both boundaries
    let payloads: Vec<Vec<u8>> = (0..m).map(|i| vec![i as u8; F]).collect();
    let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();

    let mut f = Feeder::new();
    let expected = wire.len().div_ceil(io::CARRY_BYTES);
    assert_eq!(f.feed(&wire).expect("no refusal"), expected, "one read per carry");
    assert_eq!(f.drain(), payloads);
}

/// A read that exactly fills the carry and ends part-way into a length prefix
/// compacts and completes on the next read — the case that would arm a
/// zero-length window if compaction were skipped.
#[test]
fn a_prefix_split_by_a_full_carry_compacts() {
    // Two frames whose wire bytes total `CARRY_BYTES - 2`, so the first read
    // ends two bytes into the third frame's length prefix.
    const P: usize = io::CARRY_BYTES / 2 - gnitz_wire::FRAME_LEN_PREFIX_BYTES - 1;
    let a = vec![0xA1u8; P];
    let b = vec![0xB2u8; P];
    let c = vec![0xC3u8; 64];
    let mut wire = framed(&a);
    wire.extend_from_slice(&framed(&b));
    wire.extend_from_slice(&framed(&c));

    let mut f = Feeder::new();
    let taken = f.read(&wire).expect("no refusal");
    assert_eq!(taken, io::CARRY_BYTES, "the first read fills the carry exactly");
    assert_eq!(
        f.drain(),
        vec![a, b],
        "the two whole frames leave; the split prefix stays"
    );
    assert_eq!(
        f.window.1 as usize,
        io::CARRY_BYTES - 2,
        "the two carried prefix bytes must be compacted to the front",
    );

    f.feed(&wire[taken..]).expect("no refusal");
    assert_eq!(f.drain(), vec![c]);
}

/// The zero-length close sentinel refuses the connection, and the frames parsed
/// ahead of it in the same read are queued before it does.
#[test]
fn frames_ahead_of_the_close_sentinel_are_queued() {
    let payload = vec![0x77u8; 128];
    let mut wire = framed(&payload);
    wire.extend_from_slice(&0u32.to_le_bytes());

    let mut f = Feeder::new();
    assert!(f.read(&wire).is_err(), "the sentinel closes the recv side");
    assert_eq!(f.drain(), vec![payload], "the frame ahead of it was already queued");
}

/// The pre-handshake ceiling adjudicates every frame in the first read, not
/// just the first: HELLO is a synchronous handshake, so a peer that pipelines
/// anything behind it is refused rather than served.
#[test]
fn nothing_may_ride_with_hello() {
    let mut wire = framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN]);
    wire.extend_from_slice(&framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]));
    assert_refused(
        usize::MAX,
        None,
        &wire,
        "a frame pipelined behind HELLO is under the pre-handshake ceiling",
    );
}
