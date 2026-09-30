use super::*;
use crate::test_support::{framed, io_kind, transport_pair, Peer};
use gnitz_foundation::posix_io::set_sockopt_int;
use std::io::{ErrorKind, Read};
use std::time::Duration;

/// Spawn a thread that reads exactly `count` frames off `peer`, then asserts
/// the stream ends there.
fn spawn_reader(peer: Peer, count: usize) -> std::thread::JoinHandle<Vec<Vec<u8>>> {
    std::thread::spawn(move || {
        let frames = (0..count).map(|_| peer.recv()).collect();
        let mut rest = Vec::new();
        (&peer.0).read_to_end(&mut rest).unwrap();
        assert!(rest.is_empty(), "{} bytes past the last frame", rest.len());
        frames
    })
}

#[test]
fn roundtrip_both_directions_and_an_idle_park() {
    let (mut t, peer) = transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    t.send_frame(data.clone(), None).unwrap();
    assert_eq!(peer.recv(), data);
    // Nothing to read: the blocking receive parks in poll until its deadline.
    let d = Duration::from_millis(20);
    let t0 = Instant::now();
    assert_eq!(io_kind(&t.recv_framed(Some(t0 + d))), Some(ErrorKind::WouldBlock));
    assert!(t0.elapsed() >= d);
    peer.send(&data);
    assert_eq!(t.recv_framed(None).unwrap(), data);
}

#[test]
fn recv_refuses_a_prefix_past_the_frame_ceiling() {
    let (mut t, peer) = transport_pair();
    peer.send_bytes(&((gnitz_wire::MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes());
    assert!(matches!(t.recv_framed(None), Err(ProtocolError::DecodeError(_))));
}

#[test]
fn queue_survives_chunked_and_partial_writes() {
    // Far more slices than one chunk, through a 4 KiB send buffer: every
    // writev is clamped or cut short mid-frame, and the cursor resumes each time.
    let (mut t, peer) = transport_pair();
    set_sockopt_int(t.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 4096);
    let n = 3000usize;
    let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("f{i}").repeat(i % 7 + 1).into_bytes()).collect();
    for f in &expected {
        t.enqueue(f.clone());
    }
    assert!(t.flush().unwrap(), "with nobody reading, flush stops at EAGAIN");
    let reader = spawn_reader(peer, n);
    t.flush_blocking(None).unwrap();
    drop(t);
    assert_eq!(reader.join().unwrap(), expected);
}

#[test]
fn send_deadline_keeps_the_tail_queued_and_resumes_mid_frame() {
    // An already-expired deadline: the send writes what the buffer takes, then
    // fails without parking. The remainder is queue state, the next frame
    // queues behind it, and the peer parses both whole and nothing else.
    let (mut t, peer) = transport_pair();
    set_sockopt_int(t.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 4096);
    let big: Vec<u8> = (0u8..=255).cycle().take(300 * 1024).collect();
    let r = t.send_frame(big.clone(), Some(Instant::now()));
    assert_eq!(io_kind(&r), Some(ErrorKind::WouldBlock));
    assert!((1..big.len() + 4).contains(&t.queued_bytes()), "cursor mid-frame");
    let reader = spawn_reader(peer, 2);
    t.send_frame(b"after".to_vec(), None).unwrap();
    drop(t);
    assert_eq!(reader.join().unwrap(), vec![big, b"after".to_vec()]);
}

/// A peer answering `reply` to the client's HELLO, which it reads first.
fn handshake_against(reply: &[u8]) -> Result<(), ClientError> {
    let (mut t, peer) = transport_pair();
    peer.send(reply);
    let r = hello_handshake(&mut t, None);
    assert_eq!(peer.recv(), gnitz_wire::HELLO);
    r
}

#[test]
fn hello_handshake_checks_the_server_hello() {
    handshake_against(&gnitz_wire::HELLO).unwrap();

    let mut other = gnitz_wire::HELLO;
    other[4] ^= 1;
    match handshake_against(&other) {
        Err(ClientError::Refused(f)) => assert!(f.text.contains("version mismatch"), "{}", f.text),
        r => panic!("expected a version refusal, got {r:?}"),
    }

    assert!(matches!(
        handshake_against(b"GNTX\0\0\0\0"),
        Err(ClientError::Protocol(ProtocolError::DecodeError(_)))
    ));
}

#[test]
fn connect_hands_a_tls_prefixed_target_to_the_tls_parser() {
    // Refused by the TLS parser, not tried as an AF_UNIX path (`NotFound`).
    let r = ClientTransport::connect("tls://h", None);
    assert_eq!(io_kind(&r), Some(ErrorKind::InvalidInput));
}

// ── FrameReader ─────────────────────────────────────────────────────────────

#[test]
fn reader_two_frame_stream_split_at_every_offset() {
    // Two frames delivered as two writes cut at every byte offset: both come
    // out intact, and whatever the first write over-read is retained.
    let f1: Vec<u8> = (0u8..200).collect();
    let f2: Vec<u8> = (0u8..=255).cycle().take(700).collect();
    let mut stream = framed(&f1);
    stream.extend(framed(&f2));
    for cut in 0..=stream.len() {
        let (mut t, peer) = transport_pair();
        peer.send_bytes(&stream[..cut]);
        let mut got: Vec<Vec<u8>> = Vec::new();
        let mut may_read = true;
        while let Next::Frame(f) = t.next_frame(&mut may_read).unwrap() {
            got.push(f);
        }
        peer.send_bytes(&stream[cut..]);
        while got.len() < 2 {
            if let Next::Frame(f) = t.next_frame(&mut true).unwrap() {
                got.push(f);
            }
        }
        assert_eq!(got, vec![f1.clone(), f2.clone()], "cut at {cut}");
        assert!(matches!(t.next_frame(&mut false).unwrap(), Next::Pending));
    }
}

#[test]
fn reader_short_read_ends_the_pass_and_carry_serves_without_a_read() {
    let (mut t, peer) = transport_pair();
    // Idle: the read would block, which is `Pending`, not an error.
    assert!(matches!(t.next_frame(&mut true).unwrap(), Next::Pending));
    let mut stream = framed(b"one");
    stream.extend(framed(b"two"));
    peer.send_bytes(&stream);
    let mut may_read = true;
    assert!(matches!(t.next_frame(&mut may_read).unwrap(), Next::Frame(f) if f == b"one"));
    assert!(!may_read, "the short read proved the source drained");
    // No fd access: the second frame is wholly in `carry`.
    assert!(matches!(t.next_frame(&mut may_read).unwrap(), Next::Frame(f) if f == b"two"));
    // The peer's close is not seen until a pass may read again.
    drop(peer);
    assert!(matches!(t.next_frame(&mut may_read).unwrap(), Next::Pending));
    assert_eq!(io_kind(&t.next_frame(&mut true)), Some(ErrorKind::UnexpectedEof));
}

#[test]
fn reader_large_payload_is_one_exact_allocation() {
    // A payload larger than the scratch lands in one Vec of exactly
    // payload_len capacity, filled by reads straight into it.
    let (mut t, peer) = transport_pair();
    let big: Vec<u8> = (0u8..=255).cycle().take(3 * SCRATCH_BYTES + 12345).collect();
    let expect = big.clone();
    let writer = std::thread::spawn(move || peer.send(&big));
    let frame = t.recv_framed(None).unwrap();
    writer.join().unwrap();
    assert_eq!(frame.capacity(), expect.len());
    assert_eq!(frame, expect);
}

#[test]
fn reader_delivers_frame_before_eof_when_read_fills_scratch_exactly() {
    // The peer writes a frame sized to fill the scratch exactly and closes in
    // one breath. The answering read is full — proving nothing — so the
    // reader reads again and sees the 0; the frame must come out first and
    // the EOF on the call after.
    let (mut t, peer) = transport_pair();
    set_sockopt_int(peer.0.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 256 * 1024);
    let payload: Vec<u8> = vec![9u8; SCRATCH_BYTES - 4];
    peer.send(&payload);
    drop(peer);
    assert_eq!(t.recv_framed(None).unwrap(), payload);
    assert_eq!(io_kind(&t.recv_framed(None)), Some(ErrorKind::UnexpectedEof));
}

#[test]
fn reader_eof_mid_payload_is_an_error() {
    let (mut t, peer) = transport_pair();
    peer.send_bytes(&framed(&[1u8; 100])[..50]);
    drop(peer);
    let r = t.recv_framed(None);
    assert!(
        matches!(&r, Err(ProtocolError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof && e.to_string().contains("mid-frame")),
        "{r:?}"
    );
}
