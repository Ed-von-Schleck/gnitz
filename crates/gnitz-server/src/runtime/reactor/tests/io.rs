//! Client ingress: `Plain`'s reads over the deframer, and the global
//! inbound-memory budget every `RecvBuf` is charged against.

use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;

use super::super::test_support::*;
use super::super::*;
use crate::runtime::test_support::try_poll_once;

/// A reactor with no W2M rings and an inbound cap of `cap`.
fn capped_reactor(cap: usize) -> Reactor {
    make_reactor_with(Limits { inbound_cap: cap, ..Limits::TEST })
}

/// A registered connection over one end of a fresh socketpair, and the other end.
fn registered(r: &Reactor, established: bool) -> (Rc<ClientConn>, UnixStream) {
    let (local, partner) = UnixStream::pair().expect("socketpair");
    let conn = r.client_conn(OwnedFd::from(local));
    r.register_conn(&conn, Box::new(Plain::new()));
    if established {
        conn.mark_established();
    }
    (conn, partner)
}

/// `wire` into a fresh connection capped at `cap` ends its recv side without
/// parking a reader or delivering a frame, and leaves nothing charged.
fn assert_refused(cap: usize, established: bool, wire: &[u8], why: &str) {
    let r = capped_reactor(cap);
    let (conn, partner) = registered(&r, established);
    let fd = conn.fd();
    (&partner).write_all(wire).expect("write");

    assert!(
        poll_until(&r, 20_000, || !r.inner.conns.borrow().contains_key(&fd)),
        "{why}"
    );
    assert!(
        matches!(try_poll_once(conn.recv()), Some(None)),
        "{why}: a refusal discards the queue, and recv never parks after it"
    );
    assert_eq!(r.inner.inbound.held(), 0, "{why}: budget must be reconciled");
    drop((conn, partner));
}

/// The four ways an inbound frame is refused at its header, before any payload
/// byte is allocated.
#[test]
fn inbound_frames_are_refused_at_the_header() {
    let repeat = |payload: &[u8], n: usize| -> Vec<u8> { (0..n).flat_map(|_| framed(payload)).collect() };

    // frame_weight(100) = 100. Two frames = 200 held; the 3rd pushes 300 > 250.
    assert_refused(250, true, &repeat(&[0xAB; 100], 3), "cumulative weight over cap");

    // 1-byte payloads each weigh the 64-byte floor, so 64 frames = 4096 and the
    // 65th breaches. Without the floor 65 frames would weigh 65 B and never trip.
    assert_refused(
        4096,
        false,
        &repeat(&[0xCD], 65),
        "tiny-frame flood via the weight floor",
    );

    // Not established: the ceiling is still the 8-byte HELLO payload.
    assert_refused(
        usize::MAX,
        false,
        &framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]),
        "first frame over the pre-handshake ceiling",
    );

    assert_refused(
        usize::MAX,
        false,
        &0u32.to_le_bytes(),
        "a zero-length prefix is a protocol violation",
    );
}

/// In-flight (partial, un-completed) payloads are accounted, and a second
/// connection whose first frame would breach the full cap is refused — the
/// many-connection uncounted-in-flight OOM vector.
#[test]
fn inbound_cap_counts_in_flight_and_refuses_new_conn() {
    // Exactly one 10_000-byte in-flight buffer fits.
    let r = capped_reactor(10_000);
    let (conn1, partner1) = registered(&r, true);

    // Header claims 10_000 bytes but only 100 are delivered: the buffer
    // is malloc'd and counted at header-parse time, yet no frame completes.
    let mut hdr_and_part = Vec::new();
    hdr_and_part.extend_from_slice(&10_000u32.to_le_bytes());
    hdr_and_part.extend_from_slice(&[0x11u8; 100]);
    (&partner1).write_all(&hdr_and_part).expect("write");

    let counted = poll_until(&r, 10_000, || r.inner.inbound.held() == 10_000);
    assert!(counted, "in-flight buffer was not accounted");
    assert!(
        conn1.try_recv().is_none(),
        "no frame should have completed from a partial payload"
    );

    // Second connection whose first frame would breach the now-full cap.
    let (conn2, partner2) = registered(&r, true);
    let fd2 = conn2.fd();
    (&partner2).write_all(&framed(&[0x22u8; 100])).expect("write");

    let refused = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&fd2));
    assert!(refused, "over-cap second connection was not closed");
    // Refused connection allocated nothing; the first buffer is intact.
    assert_eq!(r.inner.inbound.held(), 10_000);

    drop((partner1, partner2));
}

/// Accounting balances: consumption decrements the counter, so total traffic
/// far above the cap never trips as long as the consumer keeps pace, and the
/// counter returns to 0 once the queue fully drains.
#[test]
fn inbound_cap_accounting_balances_on_consume() {
    // One recv deframes a whole round and charges every frame in it, so the peak
    // is a round rather than a frame — and the cap below admits one.
    let r = capped_reactor(15_000);
    let (conn, partner) = registered(&r, true);

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
        r.inner.inbound.held(),
        0,
        "counter must return to 0 once every frame is consumed"
    );
    drop(partner);
}

// ─────────────────────────────────────────────────────────────────
// `Plain`'s reads over the deframer, driven the way the reactor drives them:
// the window is a raw pointer a recv writes into.
// ─────────────────────────────────────────────────────────────────

/// One uncapped `RecvQueue` behind a `Plain`, and the window it last handed out,
/// driven one simulated read at a time.
struct Feeder {
    q: io::RecvQueue,
    plain: io::Plain,
    budget: Rc<io::Budget>,
    window: (*mut u8, u32),
}

impl Feeder {
    fn new() -> Feeder {
        let budget = io::Budget::new(usize::MAX);
        let mut q = io::RecvQueue::new(Rc::clone(&budget));
        q.mark_established();
        let mut plain = io::Plain::new();
        let window = plain.window(&mut q);
        Feeder { q, plain, budget, window }
    }

    /// Inbound bytes this connection currently holds charged.
    fn held(&self) -> usize {
        self.budget.held()
    }

    /// One read: hand the window as many of `bytes` as it takes. Returns how
    /// many, or why the queue closed the connection.
    fn read(&mut self, bytes: &[u8]) -> Result<usize, RecvEnd> {
        assert!(self.window.1 > 0, "a window is never zero-length");
        let n = bytes.len().min(self.window.1 as usize);
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.window.0, n) };
        self.plain.ingest(n, &mut self.q)?;
        self.window = self.plain.window(&mut self.q);
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

/// While only part of a length prefix has arrived the connection holds no
/// charged buffer: the whole prefix is the charge point.
#[test]
fn a_split_prefix_charges_nothing() {
    let wire = framed(&[0x5Au8; 40]);
    for split in 1..gnitz_wire::FRAME_LEN_PREFIX_BYTES {
        let mut f = Feeder::new();
        assert_eq!(f.read(&wire[..split]).expect("no refusal"), split);
        assert_eq!(f.held(), 0, "split={split}");
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
    assert!(f.is_idle());
}

/// A frame straddling the end of a carry read completes on the **next** read,
/// in the same `ingest` that parses the frames behind it — so reads per run
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
/// leaves the whole carry as the next window, and the frame behind that prefix
/// completes on the next read.
#[test]
fn a_prefix_split_by_a_full_carry_resumes() {
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
        io::CARRY_BYTES,
        "the next window is the whole carry"
    );

    assert_eq!(f.feed(&wire[taken..]).expect("no refusal"), 1);
    assert_eq!(f.drain(), vec![c]);
}

/// The pre-handshake ceiling adjudicates every frame in the first read, not
/// just the first: a frame over the HELLO size pipelined behind it is refused.
#[test]
fn nothing_may_ride_with_hello() {
    let mut wire = framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN]);
    wire.extend_from_slice(&framed(&[0u8; io::HELLO_PRE_HANDSHAKE_LEN + 1]));
    assert_refused(
        usize::MAX,
        false,
        &wire,
        "a frame pipelined behind HELLO is under the pre-handshake ceiling",
    );
}
