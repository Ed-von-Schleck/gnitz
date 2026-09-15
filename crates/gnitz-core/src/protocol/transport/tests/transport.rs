use super::*;
use crate::test_support::{established, framed, make_socketpair, make_transport_pair, raw_send};
use std::os::fd::AsRawFd;

fn set_nonblocking(fd: &OwnedFd) {
    let s = UnixStream::from(fd.try_clone().unwrap());
    s.set_nonblocking(true).unwrap();
}

fn parts(bytes: &[u8]) -> MessageParts {
    MessageParts::single(bytes.to_vec())
}

/// A deadline `d` from now, for the blocking wrappers.
fn deadline(d: Duration) -> Option<Instant> {
    Some(Instant::now() + d)
}

#[test]
fn test_transport_loopback() {
    let (mut a, mut b) = make_transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    a.send_parts(parts(&data), None).unwrap();
    let received = b.recv_framed(None).unwrap();
    assert_eq!(&received[..], &data[..]);
}

#[test]
fn test_transport_medium() {
    let (mut a, mut b) = make_transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
    let reader = std::thread::spawn(move || b.recv_framed(None).unwrap());
    a.send_parts(parts(&data), None).unwrap();
    assert_eq!(reader.join().unwrap(), data);
}

#[test]
fn test_send_parts_multi_segment() {
    let (mut a, mut b) = make_transport_pair();
    let part1 = b"hello ";
    let part2 = b"world";
    let part3: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    a.send_parts(
        MessageParts {
            ctrl: part1.to_vec(),
            schema: part2.to_vec(),
            data: part3.clone(),
        },
        None,
    )
    .unwrap();
    let received = b.recv_framed(None).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(part1);
    expected.extend_from_slice(part2);
    expected.extend_from_slice(&part3);
    assert_eq!(received, expected);
}

#[test]
fn test_send_parts_empty_segments_skipped() {
    let (mut a, mut b) = make_transport_pair();
    let data = b"payload";
    a.send_parts(
        MessageParts {
            ctrl: Vec::new(),
            schema: data.to_vec(),
            data: Vec::new(),
        },
        None,
    )
    .unwrap();
    assert_eq!(&b.recv_framed(None).unwrap()[..], &data[..]);
}

#[test]
fn test_recv_framed_payload_too_large() {
    let (fd_b, a) = make_socketpair();
    let mut b = ClientTransport::from_unix_fd(fd_b);
    let huge: u32 = (gnitz_wire::MAX_FRAME_PAYLOAD_PRE_HANDSHAKE + 1) as u32;
    raw_send(&a, &huge.to_le_bytes());
    let result = b.recv_framed(None);
    assert!(matches!(result, Err(ProtocolError::DecodeError(ref s)) if s.contains("exceeds maximum")));
}

#[test]
fn test_recv_framed_enforces_negotiated_limit() {
    // The negotiated ceiling is what a post-handshake recv honours.
    let (fd_b, a) = make_socketpair();
    let mut b = ClientTransport::from_unix_fd(fd_b);
    let small_limit = 1024usize;
    b.mark_established(small_limit);
    assert_eq!(b.max_payload_len(), small_limit);
    raw_send(&a, &((small_limit + 1) as u32).to_le_bytes());
    // The header is refused before any payload is asked for.
    let result = b.recv_framed(None);
    assert!(matches!(result, Err(ProtocolError::DecodeError(ref s)) if s.contains("exceeds maximum")));
}

#[test]
fn test_pre_handshake_ceiling_admits_status_error_and_refuses_above() {
    // The worst-case pre-ACK reject frame is a `WireStatus::Error` control block with
    // a version text; it must pass the 4 KiB bound, and 4 KiB + 1 must not.
    let (fd_b, a) = make_socketpair();
    let mut b = ClientTransport::from_unix_fd(fd_b);
    let hdr = gnitz_wire::control::ControlHeader {
        status: gnitz_wire::WireStatus::Error,
        ..Default::default()
    };
    let err =
        super::super::message::encode_frame(hdr, b"unsupported wire version: peer=65535, server=65535", None, None)
            .ctrl;
    assert!(err.len() <= gnitz_wire::MAX_FRAME_PAYLOAD_PRE_HANDSHAKE);
    raw_send(&a, &framed(&err));
    assert_eq!(b.recv_framed(None).unwrap(), err);
    raw_send(
        &a,
        &((gnitz_wire::MAX_FRAME_PAYLOAD_PRE_HANDSHAKE + 1) as u32).to_le_bytes(),
    );
    assert!(matches!(b.recv_framed(None), Err(ProtocolError::DecodeError(_))));
}

/// Spawn a reader thread that pulls exactly `count` frames off `t`.
fn spawn_reader(mut t: ClientTransport, count: usize) -> std::thread::JoinHandle<Vec<Vec<u8>>> {
    std::thread::spawn(move || (0..count).map(|_| t.recv_framed(None).unwrap()).collect())
}

#[test]
fn test_send_parts_empty_returns_invalid_input() {
    // Every segment empty sums to 0 → rejected so the close sentinel can never
    // be emitted by this path.
    let (mut a, _b) = make_transport_pair();
    let r = a.send_parts(MessageParts::single(Vec::new()), None);
    assert!(matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput));
    assert!(!a.wants_write(), "a rejected frame leaves nothing queued");
}

#[test]
fn test_flush_with_nothing_queued_touches_no_fd() {
    // An empty queue returns Ok(false) without a syscall: the peer is gone,
    // so any write would surface EPIPE.
    let (mut a, b) = make_transport_pair();
    drop(b);
    assert!(!a.flush().unwrap());
    assert!(!a.wants_write());
    assert!(a.enqueue(parts(b"x")).is_ok());
    assert!(
        a.flush().is_err(),
        "with a frame queued the write does reach the dead peer"
    );
}

#[test]
fn test_frame_len_prefix_rejects_empty_and_oversized() {
    // The two lengths that would corrupt the stream are refused at enqueue,
    // before any queue entry exists — the "no writev" half is structural.
    assert!(
        matches!(frame_len_prefix(0), Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput)
    );
    #[cfg(target_pointer_width = "64")]
    assert!(matches!(
        frame_len_prefix((u32::MAX as usize) + 1),
        Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput
    ));
    assert_eq!(frame_len_prefix(1).unwrap(), 1u32.to_le_bytes());
    let (mut a, _b) = make_transport_pair();
    assert!(a.enqueue(parts(&[])).is_err());
    assert!(!a.wants_write());
}

#[test]
fn test_queue_single_frame() {
    let (mut a, b) = make_transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    let reader = spawn_reader(b, 1);
    a.enqueue(parts(&data)).unwrap();
    a.flush_blocking(None).unwrap();
    drop(a);
    let frames = reader.join().unwrap();
    assert_eq!(frames, vec![data]);
}

#[test]
fn test_queue_many_small_frames_in_order() {
    let (mut a, b) = make_transport_pair();
    let n = 200usize;
    let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("frame-{i}").into_bytes()).collect();
    let reader = spawn_reader(b, n);
    for f in &expected {
        a.enqueue(parts(f)).unwrap();
    }
    a.flush_blocking(None).unwrap();
    drop(a);
    assert_eq!(reader.join().unwrap(), expected);
}

#[test]
fn test_queue_forces_multiple_writev() {
    // Far more slices than IOV_MAX: std clamps each writev and the cursor
    // carries across; every frame intact and in order.
    let (mut a, b) = make_transport_pair();
    let n = 3000usize;
    let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("f{i:04}").into_bytes()).collect();
    let reader = spawn_reader(b, n);
    for f in &expected {
        a.enqueue(parts(f)).unwrap();
    }
    a.flush_blocking(None).unwrap();
    drop(a);
    assert_eq!(reader.join().unwrap(), expected);
}

#[test]
fn test_queue_partial_writes_small_sndbuf_nonblocking() {
    // A 4 KiB send buffer on a non-blocking socket makes flush() return true
    // repeatedly across EAGAIN; the cursor resumes mid-frame each time and
    // every frame arrives intact and in order.
    let (mut a, b) = make_transport_pair();
    set_sockopt_int(a.as_raw_fd(), libc::SO_SNDBUF, 4096);
    let n = 500usize;
    let expected: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut v = vec![(i & 0xff) as u8; 256];
            v[0] = (i >> 8) as u8;
            v
        })
        .collect();
    for f in &expected {
        a.enqueue(parts(f)).unwrap();
    }
    // Drive it by hand: flush → true means park. With nobody reading yet the
    // first flush must stop at EAGAIN with the cursor mid-batch.
    assert!(a.flush().unwrap(), "128 KiB through a 4 KiB buffer must block");
    let reader = spawn_reader(b, n);
    while a.flush().unwrap() {
        poll_fd(a.as_raw_fd(), libc::POLLOUT, None, true).unwrap();
    }
    drop(a);
    assert_eq!(reader.join().unwrap(), expected);
}

#[test]
fn test_send_timeout_leaves_cursor_and_resumes_mid_frame() {
    // A frame far larger than the send buffer under a short deadline to a
    // peer that drains only later: the error the caller sees cannot tell a
    // clean refusal from a torn frame, so the assertion is on what the peer
    // parses — that frame and the one sent after it, both intact.
    let (mut a, b) = make_transport_pair();
    set_sockopt_int(a.as_raw_fd(), libc::SO_SNDBUF, 4096);
    let big: Vec<u8> = (0u8..=255).cycle().take(300 * 1024).collect();
    a.enqueue(parts(&big)).unwrap();
    let r = a.flush_blocking(deadline(Duration::from_millis(100)));
    assert!(
        matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock),
        "expiry must surface as WouldBlock"
    );
    assert!(a.wants_write(), "the remainder stays queued");
    // A later send queues behind the torn frame.
    let small = b"after".to_vec();
    a.enqueue(parts(&small)).unwrap();
    let reader = spawn_reader(b, 2);
    // Now the peer drains; the caller re-enters exactly as
    // `eviction_observed_within` does.
    loop {
        match a.flush_blocking(deadline(Duration::from_millis(100))) {
            Ok(()) => break,
            Err(ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
    drop(a);
    assert_eq!(reader.join().unwrap(), vec![big, small]);
}

#[test]
fn test_send_framed_timeout_keeps_the_tail_queued() {
    // The one-frame blocking send under an expiring deadline: the unwritten
    // tail stays queue state, and the next send finishes it first.
    let (mut a, b) = make_transport_pair();
    set_sockopt_int(a.as_raw_fd(), libc::SO_SNDBUF, 4096);
    let big: Vec<u8> = (0u8..=255).cycle().take(200 * 1024).collect();
    let r = a.send_parts(parts(&big), deadline(Duration::from_millis(100)));
    assert!(matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock));
    assert!(a.wants_write());
    let reader = spawn_reader(b, 2);
    loop {
        match a.send_framed(b"probe", deadline(Duration::from_millis(100))) {
            Ok(()) => break,
            Err(ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
    drop(a);
    assert_eq!(reader.join().unwrap(), vec![big, b"probe".to_vec()]);
}

#[test]
fn test_send_timeout_peer_never_drains_parses_no_torn_frame() {
    // Against a peer that never drains, the bytes on the wire are a prefix
    // of one frame: once it does drain it parses zero complete frames and
    // then EOF, never a frame whose length swallowed a later one.
    let (mut a, b) = make_transport_pair();
    set_sockopt_int(a.as_raw_fd(), libc::SO_SNDBUF, 4096);
    let big: Vec<u8> = vec![7u8; 400 * 1024];
    a.enqueue(parts(&big)).unwrap();
    assert!(a.flush_blocking(deadline(Duration::from_millis(50))).is_err());
    assert!(a.flush_blocking(deadline(Duration::from_millis(50))).is_err());
    drop(a);
    let mut b = b;
    assert!(
        matches!(b.recv_framed(None), Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof)
    );
}

#[test]
fn test_hello_handshake_clamps_server_limit() {
    // A server advertising an oversized limit (u32::MAX) must not raise the
    // client's negotiated ceiling above MAX_FRAME_PAYLOAD_CLIENT.
    let (a, b) = make_socketpair();
    let ack = gnitz_wire::encode_hello_ack(u32::MAX, 0);
    raw_send(&b, &ack);
    let mut t = ClientTransport::from_unix_fd(a);
    assert_eq!(t.max_payload_len(), gnitz_wire::MAX_FRAME_PAYLOAD_PRE_HANDSHAKE);
    hello_handshake(&mut t, None).unwrap();
    assert_eq!(t.max_payload_len(), gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT);
}

#[test]
fn test_hello_handshake_preserves_smaller_limit() {
    // A server limit below the client ceiling passes through unchanged.
    let (a, b) = make_socketpair();
    let small: u32 = 16 * 1024 * 1024;
    let ack = gnitz_wire::encode_hello_ack(small, 7);
    raw_send(&b, &ack);
    let mut t = ClientTransport::from_unix_fd(a);
    let lsn = hello_handshake(&mut t, None).unwrap();
    assert_eq!(t.max_payload_len(), small as usize);
    assert_eq!(lsn, 7, "the ACK's published_lsn seeds the client basis");
}

#[test]
fn test_client_transport_unix_roundtrip() {
    let (mut a, mut b) = make_transport_pair();
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    a.send_framed(&data, None).unwrap();
    assert_eq!(b.recv_framed(None).unwrap(), data);
    b.send_parts(parts(&data), None).unwrap();
    assert_eq!(a.recv_framed(None).unwrap(), data);
}

#[test]
fn test_connect_rejects_malformed_tls_targets() {
    // The tls:// prefix selected TLS: the target is refused by the TLS parser,
    // not tried as an AF_UNIX path (which would fail with `NotFound`).
    let err = ClientTransport::connect("tls://h", None).err();
    assert!(
        matches!(err, Some(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput),
        "got {err:?}",
    );
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
        let (peer, mut t) = make_socketpair_established();
        raw_send(&peer, &stream[..cut]);
        // First half only: at most one frame may be complete.
        let mut got: Vec<Vec<u8>> = Vec::new();
        while let Next::Frame(f) = t.next_frame(true).unwrap() {
            got.push(f);
        }
        raw_send(&peer, &stream[cut..]);
        t.begin_read();
        while got.len() < 2 {
            match t.next_frame(true).unwrap() {
                Next::Frame(f) => got.push(f),
                Next::Pending => t.begin_read(),
            }
        }
        assert_eq!(got, vec![f1.clone(), f2.clone()], "cut at {cut}");
        // Nothing buffered remains.
        assert!(matches!(t.next_frame(false).unwrap(), Next::Pending));
    }
}

/// A peer fd and an established transport over the other end.
fn make_socketpair_established() -> (OwnedFd, ClientTransport) {
    let (a, b) = make_socketpair();
    (b, established(a))
}

#[test]
fn reader_serves_second_frame_from_carry_without_a_read() {
    let (peer, mut t) = make_socketpair_established();
    let mut stream = framed(b"one");
    stream.extend(framed(b"two"));
    raw_send(&peer, &stream);
    assert!(matches!(t.next_frame(true).unwrap(), Next::Frame(f) if f == b"one"));
    // No fd access: the second frame is wholly in `carry`.
    assert!(matches!(t.next_frame(false).unwrap(), Next::Frame(f) if f == b"two"));
    assert!(matches!(t.next_frame(false).unwrap(), Next::Pending));
}

#[test]
fn reader_large_payload_is_one_exact_allocation() {
    // A payload larger than the scratch lands in one Vec of exactly
    // payload_len capacity, filled by reads straight into it.
    let (peer, mut t) = make_socketpair_established();
    set_sockopt_int(peer.as_raw_fd(), libc::SO_SNDBUF, 64 * 1024);
    let big: Vec<u8> = (0u8..=255).cycle().take(3 * SCRATCH_BYTES + 12345).collect();
    let stream = framed(&big);
    let writer = std::thread::spawn(move || raw_send(&peer, &stream));
    let frame = loop {
        match t.next_frame(true).unwrap() {
            Next::Frame(f) => break f,
            Next::Pending => {
                poll_fd(t.as_raw_fd(), libc::POLLIN, None, true).unwrap();
                t.begin_read();
            }
        }
    };
    writer.join().unwrap();
    assert_eq!(frame.capacity(), big.len());
    assert_eq!(frame, big);
}

#[test]
fn reader_distinguishes_eagain_from_eof() {
    let (peer, mut t) = make_socketpair_established();
    // Idle: EAGAIN → Pending, no error.
    assert!(matches!(t.next_frame(true).unwrap(), Next::Pending));
    raw_send(&peer, &framed(b"x"));
    assert!(matches!(t.next_frame(true).unwrap(), Next::Frame(f) if f == b"x"));
    drop(peer);
    // The short read above proved the source drained, so within the same
    // wakeup the reader reports Pending without reading; the next wakeup
    // reads again and sees the EOF.
    assert!(matches!(t.next_frame(true).unwrap(), Next::Pending));
    t.begin_read();
    assert!(
        matches!(t.next_frame(true), Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof)
    );
}

#[test]
fn reader_delivers_frame_before_eof_when_read_fills_scratch_exactly() {
    // The peer writes a frame sized to fill the scratch exactly and closes in
    // one breath. The answering read is full — proving nothing — so the
    // reader reads again and sees the 0; the frame must come out first and
    // the EOF on the call after.
    let (peer, mut t) = make_socketpair_established();
    set_sockopt_int(peer.as_raw_fd(), libc::SO_SNDBUF, 256 * 1024);
    let payload: Vec<u8> = vec![9u8; SCRATCH_BYTES - 4];
    raw_send(&peer, &framed(&payload));
    drop(peer);
    let frame = loop {
        match t.next_frame(true).unwrap() {
            Next::Frame(f) => break f,
            Next::Pending => {
                poll_fd(t.as_raw_fd(), libc::POLLIN, None, true).unwrap();
                t.begin_read();
            }
        }
    };
    assert_eq!(frame, payload);
    assert!(
        matches!(t.next_frame(true), Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof)
    );
}

#[test]
fn reader_eof_mid_payload_is_immediate() {
    let (peer, mut t) = make_socketpair_established();
    let mut stream = framed(&[1u8; 100]);
    stream.truncate(50);
    raw_send(&peer, &stream);
    drop(peer);
    let r = loop {
        match t.next_frame(true) {
            Ok(Next::Pending) => {
                poll_fd(t.as_raw_fd(), libc::POLLIN, None, true).unwrap();
                t.begin_read();
            }
            other => break other,
        }
    };
    assert!(matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof));
}

#[test]
fn test_nonblocking_established_transport_reads_after_idle() {
    // Once established, an idle connection still reads when data arrives:
    // the drained inference parks in poll, never in an EAGAIN error.
    let (peer, mut t) = make_socketpair_established();
    set_nonblocking(&peer);
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        raw_send(&peer, &framed(b"late"));
        peer
    });
    assert_eq!(t.recv_framed(None).unwrap(), b"late");
    drop(h.join().unwrap());
}
