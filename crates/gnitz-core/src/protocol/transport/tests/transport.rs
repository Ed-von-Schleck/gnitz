use super::*;
use crate::test_support::{flush_all, framed, io_kind, pass, recv_frames, transport_pair, Peer};
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
fn roundtrip_both_directions() {
    let (mut t, peer) = transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    t.enqueue(data.clone());
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
fn queue_survives_chunked_and_partial_writes() {
    // Far more slices than one chunk, through a 4 KiB send buffer: every
    // writev is clamped or cut short mid-frame, and the cursor resumes each time.
    let (mut t, peer) = transport_pair();
    set_sockopt_int(t.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 4096).unwrap();
    let n = 3000usize;
    let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("f{i}").repeat(i % 7 + 1).into_bytes()).collect();
    for f in &expected {
        t.enqueue(f.clone());
    }
    t.flush().unwrap();
    assert!(t.wants_write(), "with nobody reading, flush stops at EAGAIN");
    let reader = spawn_reader(peer, n);
    flush_all(&mut t);
    drop(t);
    assert_eq!(reader.join().unwrap(), expected);
}

/// A peer answering `reply` to the client's HELLO, which it reads first.
fn hello_against(reply: &[u8]) -> Result<(), ClientError> {
    let (mut t, peer) = transport_pair();
    peer.send_bytes(reply);
    let r = t.hello(Instant::now() + gnitz_wire::CONNECT_TIMEOUT);
    assert_eq!(peer.recv(), gnitz_wire::HELLO);
    r
}

/// The HELLO of another protocol version.
fn other_version_hello() -> Vec<u8> {
    let mut other = gnitz_wire::HELLO;
    other[4] ^= 1;
    other.to_vec()
}

#[track_caller]
fn assert_version_refusal(r: Result<(), ClientError>) {
    match r {
        Err(ClientError::Refused(f)) => assert!(f.text.contains("version mismatch"), "{}", f.text),
        r => panic!("expected a version refusal, got {r:?}"),
    }
}

#[test]
fn hello_checks_the_server_hello() {
    hello_against(&framed(&gnitz_wire::HELLO)).unwrap();
    assert_version_refusal(hello_against(&framed(&other_version_hello())));
    assert!(matches!(
        hello_against(&framed(b"GNTX\0\0\0\0")),
        Err(ClientError::Protocol(ProtocolError::DecodeError(_)))
    ));
}

#[test]
fn hello_refuses_anything_behind_the_server_hello() {
    let mut two = framed(&gnitz_wire::HELLO);
    two.extend(framed(b"early"));
    let r = hello_against(&two);
    assert!(
        matches!(&r, Err(ClientError::Protocol(ProtocolError::DecodeError(m))) if m.contains("behind the server's HELLO")),
        "{r:?}"
    );

    // A framing error behind a good HELLO is the failure reported.
    let mut torn = framed(&gnitz_wire::HELLO);
    torn.extend(0u32.to_le_bytes());
    let r = hello_against(&torn);
    assert!(
        matches!(&r, Err(ClientError::Protocol(ProtocolError::DecodeError(m))) if m.contains("zero-length")),
        "{r:?}"
    );
}

#[test]
fn hello_reports_another_version_from_a_peer_that_then_closes() {
    let (mut t, peer) = transport_pair();
    peer.send(&other_version_hello());
    peer.0.shutdown(std::net::Shutdown::Write).unwrap();
    assert_version_refusal(t.hello(Instant::now() + gnitz_wire::CONNECT_TIMEOUT));
}

#[test]
fn a_silent_peer_fails_the_hello_at_the_deadline() {
    let (mut t, _peer) = transport_pair();
    let d = Duration::from_millis(50);
    let t0 = Instant::now();
    let r = t.hello(t0 + d);
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
            got.push(f.into_owned());
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

/// The bursts the read benches run, each as its frames' lengths.
pub(super) fn bench_bursts() -> [(&'static str, Vec<usize>); 4] {
    [
        ("1 x 40 B", vec![40]),
        ("3000 x 40 B", vec![40; 3000]),
        ("16 x 8 KiB", vec![8 << 10; 16]),
        ("1 x 100 KiB + 40 B", vec![100 << 10, 40]),
    ]
}

/// `frames` as the bytes a peer writes.
pub(super) fn bench_wire(frames: &[usize]) -> Vec<u8> {
    frames.iter().flat_map(|&len| framed(&vec![0x5Au8; len])).collect()
}

/// One reading pass, counting the frames it hands out.
pub(super) fn bench_pass(t: &mut ClientTransport) -> usize {
    let mut frames = 0;
    let mut more = true;
    while more {
        more = t
            .read(|f| {
                std::hint::black_box(&f);
                frames += 1;
                Ok(())
            })
            .unwrap();
    }
    frames
}

/// Instructions per burst for the one reading pass that takes it, each burst
/// written whole before the pass.
#[test]
#[ignore]
fn read_burst_bench() {
    const ROUNDS: u64 = 200;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let (mut t, peer) = transport_pair();
    for (name, frames) in bench_bursts() {
        let wire = bench_wire(&frames);
        let mut total = 0;
        for _ in 0..ROUNDS {
            peer.send_bytes(&wire);
            let (got, instr) = counter.measure(|| bench_pass(&mut t));
            assert_eq!(got, frames.len(), "{name}: one pass takes the burst");
            total += instr;
        }
        println!("read {name}: {} instr/burst", total / ROUNDS);
    }
}
