//! Client ingress: `Plain`'s reads over the deframer, and the global
//! inbound-memory budget every `RecvBuf` is charged against.

use std::io::{Read, Write};

use super::super::test_support::*;
use super::super::{Limits, Reactor};
use super::*;
use crate::runtime::test_support::try_poll_once;

/// A reactor with no W2M rings and an inbound cap of `cap`.
fn capped_reactor(cap: usize) -> Reactor {
    make_reactor_with(Limits { inbound_cap: cap, ..Limits::TEST })
}

/// `wire` into a fresh connection capped at `cap` finishes the connection without
/// delivering a frame: the frames ahead of the refusal are discarded and refunded
/// while the connection is still held, and the socket is shut down so the peer —
/// and any task parked in a send to it — sees the end now.
fn assert_refused(cap: usize, wire: &[u8], why: &str) {
    let r = capped_reactor(cap);
    let (conn, mut partner) = registered(&r);
    (&partner).write_all(wire).expect("write");

    assert!(poll_until(&r, || conn.is_gone()), "{why}");
    assert!(
        matches!(try_poll_once(conn.recv()), Some(None)),
        "{why}: a refusal discards the queue, and recv never parks after it"
    );
    assert_eq!(r.inbound.held(), 0, "{why}: budget must be reconciled");
    partner.set_nonblocking(true).expect("nonblocking");
    assert_eq!(
        partner.read(&mut [0u8; 1]).ok(),
        Some(0),
        "{why}: the partner reads EOF"
    );
}

/// The ways an inbound frame is refused at its header, before any payload byte
/// is allocated.
#[test]
fn inbound_frames_are_refused_at_the_header() {
    let repeat = |payload: &[u8], n: usize| -> Vec<u8> { (0..n).flat_map(|_| framed(payload)).collect() };

    // frame_weight(100) = 100. Two frames = 200 held; the 3rd pushes 300 > 250.
    assert_refused(250, &repeat(&[0xAB; 100], 3), "cumulative weight over cap");

    // 1-byte payloads each weigh the 64-byte floor, so 64 frames = 4096 and the
    // 65th breaches. Without the floor 65 frames would weigh 65 B and never trip.
    assert_refused(4096, &repeat(&[0xCD], 65), "tiny-frame flood via the weight floor");

    assert_refused(
        usize::MAX,
        &0u32.to_le_bytes(),
        "a zero-length prefix is a protocol violation",
    );

    let mut oversize = framed(&[0x42u8; 100]);
    oversize.extend_from_slice(&((gnitz_wire::MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes());
    assert_refused(usize::MAX, &oversize, "a prefix past the frame ceiling");
}

/// In-flight (partial, un-completed) payloads are accounted, and a second
/// connection whose first frame would breach the full cap is refused — the
/// many-connection uncounted-in-flight OOM vector.
#[test]
fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
    // Exactly one 10_000-byte in-flight buffer fits.
    let r = capped_reactor(10_000);
    let (conn1, partner1) = registered(&r);

    // Header claims 10_000 bytes but only 100 are delivered: the buffer
    // is malloc'd and counted at header-parse time, yet no frame completes.
    let mut hdr_and_part = Vec::new();
    hdr_and_part.extend_from_slice(&10_000u32.to_le_bytes());
    hdr_and_part.extend_from_slice(&[0x11u8; 100]);
    (&partner1).write_all(&hdr_and_part).expect("write");

    let counted = poll_until(&r, || r.inbound.held() == 10_000);
    assert!(counted, "in-flight buffer was not accounted");
    assert!(
        conn1.try_recv().is_none(),
        "no frame should have completed from a partial payload"
    );

    // Second connection whose first frame would breach the now-full cap.
    let (conn2, partner2) = registered(&r);
    (&partner2).write_all(&framed(&[0x22u8; 100])).expect("write");

    assert!(
        poll_until(&r, || conn2.is_gone()),
        "over-cap second connection was not closed"
    );
    // Refused connection allocated nothing; the first buffer is intact.
    assert_eq!(r.inbound.held(), 10_000);
}

/// Accounting balances: consumption decrements the counter, so total traffic
/// far above the cap never trips as long as the consumer keeps pace, and the
/// counter returns to 0 once the queue fully drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    // One recv deframes a whole round and charges every frame in it, so the peak
    // is a round rather than a frame — and the cap below admits one.
    let r = capped_reactor(15_000);
    let (conn, partner) = registered(&r);

    let payload = vec![0x7Eu8; 1_000]; // frame_weight = 1_000
    for _round in 0..2 {
        let mut wire = Vec::new();
        for _ in 0..10 {
            wire.extend_from_slice(&framed(&payload));
        }
        (&partner).write_all(&wire).expect("write");
        let c = Rc::clone(&conn);
        r.block_on(async move {
            for _ in 0..10 {
                let buf = c.recv().await.expect("frame");
                assert_eq!(buf.as_slice().len(), 1_000);
            }
        });
    }
    assert_eq!(
        r.inbound.held(),
        0,
        "counter must return to 0 once every frame is consumed"
    );
}

// ─────────────────────────────────────────────────────────────────
// `Plain`'s reads over the deframer, driven the way the reactor drives them:
// the window is an address a recv writes into.
// ─────────────────────────────────────────────────────────────────

/// One uncapped `RecvQueue` behind a `Plain`, and the window it last handed out,
/// driven one simulated read at a time.
struct Feeder {
    q: RecvQueue,
    plain: Plain,
    window: (*mut u8, usize),
}

impl Feeder {
    fn new() -> Feeder {
        let mut f = Feeder {
            q: RecvQueue::new(Budget::new(usize::MAX)),
            plain: Plain::new(),
            window: (std::ptr::null_mut(), 0),
        };
        f.take_window();
        f
    }

    /// Keep the next window's address and length, as an armed recv does.
    fn take_window(&mut self) {
        let w = self.plain.window(&mut self.q);
        self.window = (w.as_mut_ptr().cast(), w.len());
    }

    /// One read: hand the window as many of `bytes` as it takes. Returns how
    /// many, or why the queue closed the connection.
    fn read(&mut self, bytes: &[u8]) -> Result<usize, RecvEnd> {
        assert!(self.window.1 > 0, "a window is never zero-length");
        let n = bytes.len().min(self.window.1);
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.window.0, n) };
        self.plain.ingest(n, &mut self.q)?;
        self.take_window();
        Ok(n)
    }

    /// Feed `wire` as whole reads until it is exhausted; returns the read count.
    fn feed(&mut self, mut wire: &[u8]) -> Result<usize, RecvEnd> {
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

    /// Nothing is left buffered between frames.
    fn is_idle(&self) -> bool {
        !self.q.deframer.is_mid_frame()
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
    assert!(f.is_idle(), "a fully consumed run leaves nothing buffered");
}

/// A frame larger than the carry is charged at its header, absorbs whatever the
/// carry over-read past that header, takes the rest straight into its own
/// buffer, and the connection parses from the carry again afterwards.
#[test]
fn a_frame_larger_than_the_carry_reads_into_its_own_buffer() {
    let big = vec![0xC3u8; 2 * CARRY_BYTES + 500];
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
    assert!(f.is_idle());
}

/// A frame straddling the end of a carry read — its cut anywhere in the length
/// prefix, at the payload's start, or in the payload — completes on the **next**
/// read, together with whatever follows it: a run costs one read per carry, not
/// twice that minus one.
#[test]
fn a_frame_straddling_a_full_carry_completes_on_the_next_read() {
    use gnitz_wire::FRAME_LEN_PREFIX_BYTES as P;
    for cut in 1..=P + 1 {
        let payloads = vec![vec![0xA1u8; CARRY_BYTES - P - cut], vec![0xC3u8; 64]];
        let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();
        let mut f = Feeder::new();
        assert_eq!(f.feed(&wire).expect("no refusal"), 2, "cut={cut}");
        assert_eq!(f.drain(), payloads, "cut={cut}");
        assert!(f.is_idle(), "cut={cut}");
    }
}
