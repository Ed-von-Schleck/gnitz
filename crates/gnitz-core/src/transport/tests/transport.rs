use super::*;
use crate::test_support::{Peer, flush_all, io_kind, pass, recv_frames, transport_pair};
use gnitz_foundation::posix_io::set_sockopt_int;
use gnitz_wire::framed;
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
fn roundtrip_both_directions() {
    let (mut t, peer) = transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    t.enqueue(&data).unwrap();
    t.flush().unwrap();
    assert!(!t.wants_write());
    assert_eq!(peer.recv(), data);
    peer.send(&data);
    assert_eq!(recv_frames(&mut t, 1), [data]);
}

#[test]
fn read_refuses_a_prefix_past_the_frame_ceiling() {
    let (mut t, peer) = transport_pair();
    peer.send_bytes(&((gnitz_wire::MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes());
    assert!(matches!(pass(&mut t), (frames, Err(ProtocolError::DecodeError(_))) if frames.is_empty()));
}

#[test]
fn queue_survives_partial_writes() {
    // Thousands of frames through a 4 KiB send buffer: every write is cut
    // short mid-frame, and the cursor resumes each time.
    let (mut t, peer) = transport_pair();
    set_sockopt_int(t.as_fd().as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 4096).unwrap();
    let n = 3000usize;
    let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("f{i}").repeat(i % 7 + 1).into_bytes()).collect();
    for f in &expected {
        t.enqueue(f).unwrap();
    }
    t.flush().unwrap();
    assert!(t.wants_write(), "with nobody reading, flush stops at EAGAIN");
    let reader = spawn_reader(peer, n);
    flush_all(&mut t);
    drop(t);
    assert_eq!(reader.join().unwrap(), expected);
}

#[test]
fn a_frame_queued_behind_a_partial_write_follows_its_unwritten_bytes() {
    let (mut t, _peer) = transport_pair();
    t.enqueue(b"first").unwrap();
    // Two-thirds written: the next frame drops the written bytes first.
    t.queue.advance(6);
    t.enqueue(b"second").unwrap();
    assert_eq!(t.queue.off, 0, "the written bytes are dropped");
    assert_eq!(
        t.queue.pending(),
        [&framed(b"first")[6..], &framed(b"second")[..]].concat()
    );
}

#[test]
fn an_oversize_frame_is_refused_and_leaves_the_queue_as_it_was() {
    let (mut t, _peer) = transport_pair();
    let len = gnitz_wire::MAX_FRAME_PAYLOAD + 1;
    let oversize = |out: &mut Vec<u8>| out.resize(out.len() + len, 0);
    t.enqueue(b"queued").unwrap();
    assert_eq!(t.frame(oversize), Err(len));
    assert_eq!(t.queue.pending(), framed(b"queued"));
    t.queue.advance(t.queued_bytes());
    // Refused into an empty queue, it takes its allocation with it.
    assert_eq!(t.frame(oversize), Err(len));
    assert!(t.queue.is_empty());
    assert_eq!(t.queue.buf.capacity(), 0);
}

/// A queue that grew past what it retains gives its buffer to the frame-buffer
/// pool once written, which hands it to the next frame of its size.
#[test]
fn a_written_queue_past_its_retained_size_goes_to_the_pool() {
    let (mut t, _peer) = transport_pair();
    // A size no other test of the crate asks the pool for.
    let cap = 17 << 20;
    t.queue.buf = Vec::with_capacity(cap);
    t.queue.buf.extend_from_slice(b"sent");
    let at = t.queue.buf.as_ptr();
    t.queue.advance(4);
    assert_eq!(t.queue.buf.capacity(), 0);
    let again = gnitz_wire::frame_buf::try_alloc(cap).unwrap();
    assert_eq!((again.as_ptr(), again.capacity()), (at, cap), "the buffer given back");
}

/// A peer answering `reply` to the client's HELLO, which carries `token`.
fn hello_against(token: &[u8], reply: &[u8]) -> Result<(), ClientError> {
    let (mut t, peer) = transport_pair();
    peer.send_bytes(reply);
    let r = t.hello(token, Instant::now() + gnitz_wire::CONNECT_TIMEOUT);
    assert_eq!(peer.recv(), gnitz_wire::client_hello(token));
    r
}

/// The server's HELLO carrying `status`, framed.
fn server_hello(status: gnitz_wire::AuthStatus) -> Vec<u8> {
    framed(&gnitz_wire::server_hello(status))
}

/// The HELLO a server of another protocol version refuses ours with.
fn other_version_hello() -> Vec<u8> {
    other_release_hello("")
}

/// The HELLO a server of another protocol version refuses ours with, naming
/// its release `release`.
fn other_release_hello(release: &str) -> Vec<u8> {
    let mut other = gnitz_wire::HELLO.to_vec();
    other[4] ^= 1;
    other.extend_from_slice(release.as_bytes());
    framed(&other)
}

#[track_caller]
fn assert_refusal(r: Result<(), ClientError>, names: &str) {
    match r {
        Err(ClientError::Refused(f)) => assert!(f.text.contains(names), "{}", f.text),
        r => panic!("expected a refusal naming {names:?}, got {r:?}"),
    }
}

#[test]
fn hello_checks_the_server_hello() {
    hello_against(b"", &server_hello(gnitz_wire::AuthStatus::Accepted)).unwrap();
    assert!(matches!(
        hello_against(b"", &framed(b"GNTX\0\0\0\0\0")),
        Err(ClientError::Protocol(ProtocolError::DecodeError(_)))
    ));
}

#[test]
fn hello_names_both_releases_of_a_version_mismatch() {
    let ours = gnitz_wire::RELEASE;
    assert_refusal(
        hello_against(b"", &other_release_hello("9.9.9")),
        &format!("the server is gnitz 9.9.9 and this client gnitz {ours}; use a client of release 9.9.9"),
    );
    // One release, two wire versions: builds of different sources.
    assert_refusal(
        hello_against(b"", &other_release_hello(ours)),
        &format!("both gnitz {ours}, but built from different sources"),
    );
    // A server that names no release is reported by its wire version.
    assert_refusal(
        hello_against(b"", &other_version_hello()),
        &format!("this build (gnitz {ours})"),
    );
}

#[test]
fn hello_sends_the_token_and_reports_the_verdict() {
    hello_against(b"gnitz_ab", &server_hello(gnitz_wire::AuthStatus::Accepted)).unwrap();
    assert_refusal(
        hello_against(b"", &server_hello(gnitz_wire::AuthStatus::Refused)),
        "requires an access token",
    );
    assert_refusal(
        hello_against(b"gnitz_ab", &server_hello(gnitz_wire::AuthStatus::Refused)),
        "does not recognise",
    );
    let not_a_verdict = framed(&[&gnitz_wire::HELLO[..], b"\x09"].concat());
    assert!(matches!(
        hello_against(b"", &not_a_verdict),
        Err(ClientError::Protocol(ProtocolError::DecodeError(_)))
    ));
}

#[test]
fn hello_refuses_anything_behind_the_server_hello() {
    let mut two = server_hello(gnitz_wire::AuthStatus::Accepted);
    two.extend(framed(b"early"));
    let r = hello_against(b"", &two);
    assert!(
        matches!(&r, Err(ClientError::Protocol(ProtocolError::DecodeError(m))) if m.contains("behind the server's HELLO")),
        "{r:?}"
    );

    // A framing error behind a good HELLO is the failure reported.
    let mut torn = server_hello(gnitz_wire::AuthStatus::Accepted);
    torn.extend(0u32.to_le_bytes());
    let r = hello_against(b"", &torn);
    assert!(
        matches!(&r, Err(ClientError::Protocol(ProtocolError::DecodeError(m))) if m.contains("zero-length")),
        "{r:?}"
    );
}

#[test]
fn hello_reports_a_refusal_from_a_peer_that_then_closes() {
    for (reply, names) in [
        (other_version_hello(), "version mismatch"),
        (server_hello(gnitz_wire::AuthStatus::Refused), "does not recognise"),
    ] {
        let (mut t, peer) = transport_pair();
        peer.send_bytes(&reply);
        peer.0.shutdown(std::net::Shutdown::Write).unwrap();
        assert_refusal(
            t.hello(b"gnitz_ab", Instant::now() + gnitz_wire::CONNECT_TIMEOUT),
            names,
        );
    }
}

#[test]
fn a_silent_peer_fails_the_hello_at_the_deadline() {
    let (mut t, _peer) = transport_pair();
    let d = Duration::from_millis(50);
    let t0 = Instant::now();
    let r = t.hello(b"", t0 + d);
    assert!(
        matches!(&r, Err(ClientError::Protocol(ProtocolError::IoError(e))) if e.kind() == ErrorKind::TimedOut),
        "{r:?}"
    );
    assert!(t0.elapsed() >= d);
}

#[test]
fn connect_hands_a_tls_prefixed_target_to_the_tls_parser() {
    // Refused by the TLS parser, not tried as an AF_UNIX path (`NotFound`).
    let r = ClientTransport::connect("tls://h", Instant::now() + gnitz_wire::CONNECT_TIMEOUT);
    assert!(
        matches!(&r, Err(ClientError::Protocol(ProtocolError::IoError(e))) if e.kind() == ErrorKind::InvalidInput),
        "{:?}",
        r.err()
    );
}

// ── The read ────────────────────────────────────────────────────────────────

#[test]
fn two_frame_stream_split_at_every_offset() {
    // Two frames delivered as two writes cut at every byte offset: both come
    // out intact, whatever the first read left mid-frame.
    let f1: Vec<u8> = (0u8..200).collect();
    let f2: Vec<u8> = (0u8..=255).cycle().take(700).collect();
    let mut stream = framed(&f1);
    stream.extend(framed(&f2));
    for cut in 0..=stream.len() {
        let (mut t, peer) = transport_pair();
        peer.send_bytes(&stream[..cut]);
        let (mut got, end) = pass(&mut t);
        end.unwrap();
        peer.send_bytes(&stream[cut..]);
        let (rest, end) = pass(&mut t);
        end.unwrap();
        got.extend(rest);
        assert_eq!(got, [f1.clone(), f2.clone()], "cut at {cut}");
        assert!(!t.deframer.is_mid_frame(), "cut at {cut}");
    }
}

#[test]
fn an_idle_read_would_block_and_a_short_read_ends_the_pass() {
    let (mut t, peer) = transport_pair();
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut read = |t: &mut ClientTransport| {
        t.read(|f| {
            got.push(f.to_vec());
            Ok(())
        })
    };
    // Idle: the read would block, which is no error and ends the pass.
    assert!(!read(&mut t).unwrap());
    let mut stream = framed(b"one");
    stream.extend(framed(b"two"));
    peer.send_bytes(&stream);
    // One read hands out both frames, and being short proves the socket drained.
    assert!(!read(&mut t).unwrap());
    drop(peer);
    assert_eq!(io_kind(&read(&mut t)), Some(ErrorKind::UnexpectedEof));
    assert_eq!(got, [b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn a_read_abandoned_with_frames_unfed_refuses_the_next() {
    let (mut t, peer) = transport_pair();
    let mut stream = framed(b"one");
    stream.extend(framed(b"two"));
    peer.send_bytes(&stream);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        t.read(|_| panic!("the consumer's own"))
    }));
    assert!(unwound.is_err());
    peer.send(b"three");
    let (frames, end) = pass(&mut t);
    assert!(frames.is_empty(), "nothing is framed out of a torn stream");
    assert_eq!(io_kind(&end), Some(ErrorKind::Other));
}

#[test]
fn a_payload_larger_than_the_window_is_intact() {
    // The window over-reads past the prefix, and the rest of the payload is
    // read straight into its own buffer.
    let (mut t, peer) = transport_pair();
    let big: Vec<u8> = (0u8..=255).cycle().take(3 * WINDOW_BYTES + 12345).collect();
    let expect = big.clone();
    let writer = std::thread::spawn(move || {
        peer.send(&big);
        peer
    });
    assert_eq!(recv_frames(&mut t, 1), [expect]);
    writer.join().unwrap();
}

#[test]
fn a_frame_is_delivered_before_eof_when_the_read_fills_the_window_exactly() {
    // The peer writes a frame sized to fill the window exactly and closes in
    // one breath. The answering read is full — proving nothing — so the pass
    // reads again and sees the 0, with the frame already handed out.
    let (mut t, peer) = transport_pair();
    set_sockopt_int(peer.0.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 256 * 1024).unwrap();
    let payload: Vec<u8> = vec![9u8; WINDOW_BYTES - 4];
    peer.send(&payload);
    drop(peer);
    let (frames, end) = pass(&mut t);
    assert_eq!(frames, [payload]);
    assert_eq!(io_kind(&end), Some(ErrorKind::UnexpectedEof));
}

#[test]
fn eof_mid_payload_is_an_error() {
    let (mut t, peer) = transport_pair();
    peer.send_bytes(&framed(&[1u8; 100])[..50]);
    drop(peer);
    // The short read that takes the half frame ends its pass; the close is the
    // next one's.
    assert!(matches!(pass(&mut t), (frames, Ok(())) if frames.is_empty()));
    let (frames, end) = pass(&mut t);
    assert!(frames.is_empty());
    assert!(
        matches!(&end, Err(ProtocolError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof && e.to_string().contains("mid-frame")),
        "{end:?}"
    );
}
